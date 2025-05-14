use std::{sync::Arc, time::Duration};

use backoff::{SystemClock, backoff::Backoff, exponential::ExponentialBackoffBuilder};
use governor::{
    Quota, RateLimiter,
    clock::{Clock, MonotonicClock},
    middleware::NoOpMiddleware,
    state::{InMemoryState, direct::NotKeyed},
};
use hulyrs::{
    Error,
    services::{
        account::{AccountClient, SelectWorkspaceParams, WorkspaceKind},
        jwt::ClaimsBuilder,
        transactor::{
            TransactorClient,
            event::{Envelope, EventClient},
        },
        types::WorkspaceUuid,
    },
};
use moka::future::{Cache, CacheBuilder};
use rdkafka::{
    Message,
    client::ClientContext,
    config::ClientConfig,
    consumer::{Consumer as _, ConsumerContext, stream_consumer::StreamConsumer},
    message::{BorrowedMessage, Headers},
};
use serde_json as json;
use tokio::time;
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

                let account = AccountClient::new(&claims)?;

                let workspace_info = account
                    .select_workspace(&SelectWorkspaceParams {
                        workspace_url: String::default(),
                        kind: WorkspaceKind::ByRegion,
                        external_regions: hulyrs::CONFIG.external_regions.clone(),
                    })
                    .await?;

                trace!(%workspace, transactor = %workspace_info.endpoint, "get transactor for workspace");

                TransactorClient::new(workspace_info.endpoint, &claims).map(Arc::new)
            })
            .await
            .map_err(|e| {
                error!(%workspace, error=%e, "Failed to get transactor");
                Error::Other("NoTransactor")
            })
    }
}

#[cfg(test)]
mod test {
    use tracing::debug;

    use hulyrs::services::{
        account::{AccountClient, SelectWorkspaceParams, WorkspaceKind},
        jwt::ClaimsBuilder,
    };

    use crate::config::CONFIG;

    #[tokio::test]
    async fn test_region() {
        super::initialize_tracing();

        let claims = ClaimsBuilder::default()
            .system_account()
            .workspace(uuid::uuid!("80dc97fb-1c3e-4e74-9490-d430f30da740"))
            .service(&CONFIG.service_id)
            .build()
            .unwrap();

        let account = AccountClient::new(&claims).unwrap();

        let select = SelectWorkspaceParams {
            workspace_url: String::default(),
            kind: WorkspaceKind::ByRegion,
            external_regions: Vec::default(),
        };

        let info = account.select_workspace(&select).await.unwrap();

        debug!(?info, "workspace info")
    }
}

pub type Limiter<MW = NoOpMiddleware<<MonotonicClock as Clock>::Instant>> =
    RateLimiter<NotKeyed, InMemoryState, MonotonicClock, MW>;

struct Context {
    transactors: TransactorCache,
    limiter: Limiter,
}

impl Context {
    pub fn new() -> Self {
        let quota = Quota::per_second(CONFIG.rate_limit).allow_burst(1.try_into().unwrap());

        info!(rps = CONFIG.rate_limit, "Rate limiter initialized");

        let limiter = RateLimiter::direct_with_clock(quota, MonotonicClock);

        Self {
            transactors: TransactorCache::new(),
            limiter,
        }
    }
}

trait ErrorExt {
    fn is_transient(&self) -> bool;
}

impl ErrorExt for Error {
    fn is_transient(&self) -> bool {
        match self {
            Error::HttpError(c, _) if c.is_server_error() => true,
            _ => false,
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

    let workspace =
        if let Some(Ok(workspace)) = message.header("WorkspaceUuid").map(|s| Uuid::parse_str(&s)) {
            workspace
        } else {
            return Err(Error::Other("InvalidWorkspace"))?;
        };

    let envelope = if let Some(Ok(payload)) = message
        .payload()
        .map(json::from_slice::<Envelope<json::Value>>)
    {
        payload
    } else {
        return Err(Error::Other("InvalidPayload"))?;
    };

    let transactor = context.transactors.get_transactor(workspace).await?;

    if let Err(delay) = context.limiter.check() {
        time::sleep_until(delay.earliest_possible().into()).await;
    }

    if !CONFIG.dry_run {
        transactor.request_raw(&envelope).await
    } else {
        Ok(())
    }
}

async fn worker(consumer: Consumer) -> Result<(), anyhow::Error> {
    let topics = CONFIG.topics();
    info!(topics = %format!("[{}]", topics.join(",")), "Starting consumer");

    consumer.subscribe(&topics)?;

    let mut backoff = ExponentialBackoffBuilder::<SystemClock>::default()
        .with_initial_interval(Duration::from_secs(1))
        .with_max_interval(Duration::from_secs(30))
        .with_max_elapsed_time(Some(Duration::from_secs(60)))
        .build();

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

        trace!(%topic, partition, offset, "Processing message");

        backoff.reset();

        'retry: loop {
            if let Err(e) = process(&consumer, &message).await {
                if e.is_transient() {
                    if let Some(delay) = backoff.next_backoff() {
                        warn!(%topic, partition, offset, error=%e, "Transient error");

                        time::sleep(delay).await;
                        continue 'retry;
                    } else {
                        error!(%topic, partition, offset, error=%e, "Max elapsed time reached, giving up");
                    }
                } else {
                    error!(%topic, partition, offset, error=%e, "Persistent error, discrading message");
                }
            } else {
                trace!(%topic, partition, offset, "Message processed");
            }

            break;
        }
    }
}

pub fn initialize_tracing() {
    use tracing::Level;
    use tracing_subscriber::{filter::targets::Targets, prelude::*};

    let filter = Targets::default()
        .with_default(Level::WARN)
        .with_target(env!("CARGO_PKG_NAME"), Level::TRACE)
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

    let mut config = ClientConfig::new();

    config
        .set("group.id", &CONFIG.group_id)
        .set(
            "bootstrap.servers",
            hulyrs::CONFIG.kafka_bootstrap_servers(),
        )
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "10000")
        .set("heartbeat.interval.ms", "2000")
        .set(
            "enable.auto.commit",
            if CONFIG.dry_run { "false" } else { "true" },
        )
        .set("auto.offset.reset", "smallest");

    if let Some(debug) = hulyrs::CONFIG.kafka_rdkafka_debug.as_ref() {
        config.set("debug", debug);
    }

    let consumer = config.create_with_context(Context::new())?;

    worker(consumer).await?;

    Ok(())
}
