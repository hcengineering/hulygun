//
// Copyright © 2025 Hardcore Engineering Inc.
//
// Licensed under the Eclipse Public License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License. You may
// obtain a copy of the License at https://www.eclipse.org/legal/epl-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//
// See the License for the specific language governing permissions and
// limitations under the License.
//

use std::{sync::Arc, time::Duration};

use anyhow::{Result, bail};
use base64::Engine;
use hulyrs::{
    Error,
    services::{
        account::{SelectWorkspaceParams, WorkspaceKind},
        core::WorkspaceUuid,
        jwt::ClaimsBuilder,
        transactor::{TransactorClient, backend::http::HttpBackend, comm::EventClient},
    },
};
use moka::future::{Cache, CacheBuilder};
use rdkafka::{
    Message,
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::ClientContext,
    config::{ClientConfig, FromClientConfig},
    consumer::{CommitMode, Consumer as _, ConsumerContext, stream_consumer::StreamConsumer},
    message::{BorrowedMessage, Headers},
    types::RDKafkaErrorCode,
};
use serde_json as json;
use tracing::{Span, *};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;

use hulyrs::services::otel;

mod config;
use config::CONFIG;

struct TransactorCache {
    cache: Cache<WorkspaceUuid, Arc<TransactorClient<HttpBackend>>>,
}

impl TransactorCache {
    pub fn new() -> Self {
        let cache = CacheBuilder::new(10_000)
            .time_to_idle(Duration::from_secs(60 * 60))
            .build();

        Self { cache }
    }

    pub async fn get_transactor(
        &self,
        workspace: WorkspaceUuid,
    ) -> std::result::Result<Arc<TransactorClient<HttpBackend>>, Error> {
        self.cache
            .try_get_with(workspace, async {
                let claims = ClaimsBuilder::default()
                    .system_account()
                    .workspace(workspace)
                    .service(&CONFIG.service_id)
                    .build()
                    .unwrap();

                let account = config::hulyrs::SERVICES.new_account_client(&claims)?;

                let workspace_info = account
                    .select_workspace(&SelectWorkspaceParams {
                        workspace_url: String::default(),
                        kind: WorkspaceKind::ByRegion,
                        external_regions: config::hulyrs::CONFIG.external_regions.clone(),
                    })
                    .await?;

                trace!(transactor = %workspace_info.endpoint, "Get transactor for workspace");

                config::hulyrs::SERVICES
                    .new_transactor_client(workspace_info.endpoint, &claims)
                    .map(Arc::new)
            })
            .await
            .map_err(|e| {
                error!(%workspace, error=%e, "Failed to get transactor");
                Error::Other("NoTransactor")
            })
    }
}

struct Context {
    transactors: TransactorCache,
}

impl Context {
    pub fn new() -> Self {
        Self {
            transactors: TransactorCache::new(),
        }
    }
}

impl ClientContext for Context {}
impl ConsumerContext for Context {}

type Consumer = StreamConsumer<Context>;

trait MessageExt {
    fn header(&self, header: &str) -> Option<String>;
}

impl<T: rdkafka::Message> MessageExt for T {
    fn header(&self, header: &str) -> Option<String> {
        self.headers().and_then(|headers| {
            for n in 0..headers.count() {
                let h = headers.get(n);
                if h.key == header {
                    return h.value.map(|v| String::from_utf8_lossy(v).to_string());
                }
            }

            None
        })
    }
}

use opentelemetry::{KeyValue, global, metrics::Counter, trace::Status};
use std::sync::LazyLock;

fn workspace_id(message: &BorrowedMessage<'_>) -> Option<Uuid> {
    message
        .header("WorkspaceUuid")
        .or_else(|| message.header("workspace"))
        .map(|s| Uuid::parse_str(&s))
        .transpose()
        .ok()
        .flatten()
}

#[instrument(level = "trace", skip_all)]
async fn process_event(consumer: &Consumer, message: &BorrowedMessage<'_>) {
    let span = Span::current();

    static EVENTS: LazyLock<Counter<u64>> = LazyLock::new(|| {
        global::meter("hulygun")
            .u64_counter("hulygun_event")
            .build()
    });

    static ERRORS: LazyLock<Counter<u64>> = LazyLock::new(|| {
        global::meter("hulygun")
            .u64_counter("hulygun_error")
            .build()
    });

    let workspace = workspace_id(message)
        .map(|workspace| workspace.to_string())
        .unwrap_or_default();

    let source = message.header("source").unwrap_or_default();

    span.set_attribute("source", source.clone());
    span.set_attribute("workspace", workspace.clone());
    span.set_attribute("topic", message.topic().to_owned());
    span.set_attribute("partition", message.partition().to_string());
    span.set_attribute("offset", message.offset());

    let mattrs = vec![
        KeyValue::new("workspace", workspace),
        KeyValue::new("source", source),
    ];

    EVENTS.add(1, &mattrs);

    if let Err(error) = process_event0(consumer, message).await {
        ERRORS.add(1, &mattrs);

        span.set_status(Status::Error {
            description: error.to_string().into(),
        });
    }
}

async fn process_event0(consumer: &Consumer, message: &BorrowedMessage<'_>) -> Result<()> {
    let context = consumer.context();

    let payload = if let Some(payload) = message.payload() {
        match json::from_slice::<json::Value>(payload) {
            Ok(parsed) => parsed,
            Err(error) => {
                let payload = base64::prelude::BASE64_STANDARD.encode(payload);
                error!(%error, payload, "Invalid payload");
                bail!("InvalidPayload");
            }
        }
    } else {
        bail!("NoPayload");
    };

    let workspace_id = workspace_id(message);

    let workspace = if let Some(workspace) = workspace_id {
        workspace
    } else {
        bail!("InvalidWorkspace");
    };

    let _service = message
        .header("service")
        .unwrap_or_else(|| "unknown".to_string());

    let transactor = context.transactors.get_transactor(workspace).await?;

    if !CONFIG.dry_run {
        let is_transaction = payload["space"] == json::Value::String("core:space:Tx".to_string());

        if is_transaction {
            transactor
                .tx_raw::<_, Option<json::Value>>(&payload)
                .await?;
            trace!("Transaction processed");
        } else {
            warn!(%payload, "Using deprecated communication event processing");

            transactor
                .request_raw::<_, Option<json::Value>>(&payload)
                .await?;
            trace!("Communication event processed");
        }
    }

    Ok(())
}

async fn worker(consumer: Consumer) -> Result<(), anyhow::Error> {
    let topics = CONFIG.topics();
    info!(topics = %format!("[{}]", topics.join(",")), "Starting consumer");

    consumer.subscribe(&topics)?;

    loop {
        let message = consumer.recv().await;

        // error handling is done inside rdkafka, we can safely ignore all errors
        if message.is_err() {
            continue;
        }

        let message = message.unwrap();

        let topic = message.topic();
        let partition = message.partition();
        let offset = message.offset();

        process_event(&consumer, &message).await;

        if !CONFIG.dry_run {
            if let Err(e) = consumer.commit_message(&message, CommitMode::Async) {
                error!(%topic, partition, offset, error=%e, "Failed to commit message");
            }
        }
    }
}

fn initialize_tracing() {
    use opentelemetry::trace::TracerProvider;
    use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
    use tracing::Level;
    use tracing_opentelemetry::OpenTelemetryLayer;
    use tracing_subscriber::{filter::targets::Targets, prelude::*};

    let otel_config = otel::OtelConfig {
        mode: config::hulyrs::CONFIG.otel_mode.clone(),
        service_name: env!("CARGO_PKG_NAME").to_string(),
        service_version: env!("CARGO_PKG_VERSION").to_string(),
    };

    otel::init(&otel_config);

    let filter = Targets::default()
        .with_target(env!("CARGO_BIN_NAME"), config::hulyrs::CONFIG.log)
        .with_target("actix", Level::WARN);
    let format = tracing_subscriber::fmt::layer().compact();

    match &config::hulyrs::CONFIG.otel_mode {
        otel::OtelMode::Off => {
            tracing_subscriber::registry()
                .with(filter)
                .with(format)
                .init();
        }

        _ => {
            tracing_subscriber::registry()
                .with(filter)
                .with(format)
                .with(otel::tracer_provider(&otel_config).map(|provider| {
                    let filter = Targets::default()
                        .with_default(Level::DEBUG)
                        .with_target(env!("CARGO_PKG_NAME"), config::hulyrs::CONFIG.log);

                    OpenTelemetryLayer::new(provider.tracer("hulygun")).with_filter(filter)
                }))
                .with(otel::logger_provider(&otel_config).as_ref().map(|logger| {
                    let filter = Targets::default()
                        .with_default(Level::DEBUG)
                        .with_target(env!("CARGO_PKG_NAME"), Level::DEBUG);

                    OpenTelemetryTracingBridge::new(logger).with_filter(filter)
                }))
                .init();
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    initialize_tracing();

    info!(
        "{}/{} started",
        env!("CARGO_PKG_NAME"),
        env!("CARGO_PKG_VERSION")
    );

    let mut config = ClientConfig::new();
    config.set(
        "bootstrap.servers",
        config::hulyrs::CONFIG.kafka_bootstrap_servers(),
    );

    let admin = AdminClient::from_config(&config)?;

    let topics = CONFIG
        .topics()
        .iter()
        .map(|topic| NewTopic {
            name: topic,
            num_partitions: 4,
            replication: TopicReplication::Fixed(1),
            config: vec![],
        })
        .collect::<Vec<_>>();

    admin
        .create_topics(&topics, &AdminOptions::default())
        .await?
        .iter()
        .for_each(|result| match result {
            Ok(topic) => info!(topic, "Topic created"),
            Err((topic, RDKafkaErrorCode::TopicAlreadyExists)) => {
                trace!(topic, "Topic already exists");
            }

            Err((topic, error)) => error!(topic, ?error, "Failed to create topic"),
        });

    let mut config = ClientConfig::new();

    config
        .set("group.id", &CONFIG.group_id)
        .set(
            "bootstrap.servers",
            config::hulyrs::CONFIG.kafka_bootstrap_servers(),
        )
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "10000")
        .set("heartbeat.interval.ms", "2000")
        .set("auto.offset.reset", "smallest")
        .set("enable.auto.commit", "false");

    if let Some(debug) = config::hulyrs::CONFIG.kafka_rdkafka_debug.as_ref() {
        config.set("debug", debug);
    }

    let consumer = config.create_with_context(Context::new())?;

    worker(consumer).await?;

    Ok(())
}
