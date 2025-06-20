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

use base64::Engine;
use hulyrs::{
    Error,
    services::{
        account::{SelectWorkspaceParams, WorkspaceKind},
        jwt::ClaimsBuilder,
        transactor::{TransactorClient, event::EventClient},
        types::WorkspaceUuid,
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
use tracing::{error, info, trace, warn};
use uuid::Uuid;

mod config;
use config::CONFIG;

struct TransactorCache {
    cache: Cache<WorkspaceUuid, Arc<TransactorClient>>,
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
    ) -> std::result::Result<Arc<TransactorClient>, Error> {
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

                trace!(%workspace, transactor = %workspace_info.endpoint, "Get transactor for workspace");

                config::hulyrs::SERVICES.new_transactor_client(workspace_info.endpoint, &claims).map(Arc::new)
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

async fn process(consumer: &Consumer, message: &BorrowedMessage<'_>) -> Result<(), Error> {
    let context = consumer.context();

    let envelope = if let Some(payload) = message.payload() {
        match json::from_slice::<json::Value>(payload) {
            Ok(parsed) => parsed,
            Err(error) => {
                let payload = base64::prelude::BASE64_STANDARD.encode(payload);
                warn!(%error, payload, "Invalid payload");
                return Err(Error::Other("InvalidPayload"));
            }
        }
    } else {
        return Err(Error::Other("NoPayload"));
    };

    let workspace =
        if let Some(Ok(workspace)) = message.header("WorkspaceUuid").map(|s| Uuid::parse_str(&s)) {
            workspace
        } else {
            return Err(Error::Other("InvalidWorkspace"));
        };

    let transactor = context.transactors.get_transactor(workspace).await?;

    if !CONFIG.dry_run {
        transactor
            .request_raw::<_, Option<json::Value>>(&envelope)
            .await?;
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

        if let Err(e) = process(&consumer, &message).await {
            error!(%topic, partition, offset, error=%e, "Processing error, message discarded");
        } else {
            trace!(%topic, partition, offset, "Message processed");
        }

        if !CONFIG.dry_run {
            if let Err(e) = consumer.commit_message(&message, CommitMode::Async) {
                error!(%topic, partition, offset, error=%e, "Failed to commit message");
            }
        }
    }
}

pub fn initialize_tracing() {
    use tracing::Level;
    use tracing_subscriber::{filter::targets::Targets, prelude::*};

    let filter = Targets::default()
        .with_default(Level::WARN)
        .with_target(env!("CARGO_PKG_NAME"), config::hulyrs::CONFIG.log)
        .with_target("librdkafka", Level::DEBUG);

    let format = tracing_subscriber::fmt::layer().compact();

    tracing_subscriber::registry()
        .with(filter)
        .with(format)
        .init();
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    initialize_tracing();

    info!(
        "{}/{} started",
        env!("CARGO_PKG_NAME"),
        env!("CARGO_PKG_VERSION")
    );

    {
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
    }

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
