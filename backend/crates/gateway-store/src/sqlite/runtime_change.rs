//! SQLite 持久 revision 轮询与当前进程内快速通知。

use std::time::Duration;

use async_trait::async_trait;
use futures::{StreamExt, stream};
use gateway_core::{
    routing::ConfigRevision,
    runtime::{SnapshotRevisionStream, SnapshotSubscriptionError, SnapshotSubscriptionPort},
};
use sqlx::SqlitePool;
use tokio::{sync::broadcast, time::MissedTickBehavior};

use crate::{
    Revision, StoreError, StoreResult,
    runtime_change::{RuntimeChange, RuntimeChangeRepository, RuntimeChangeSubscription},
};

const REVISION_POLL_INTERVAL: Duration = Duration::from_secs(1);
const NOTIFICATION_CAPACITY: usize = 128;

#[derive(Clone)]
pub struct SqliteRuntimeChangeRepository {
    pool: SqlitePool,
    notifications: broadcast::Sender<RuntimeChange>,
}

impl SqliteRuntimeChangeRepository {
    #[must_use]
    pub fn new(pool: SqlitePool) -> Self {
        let (notifications, _) = broadcast::channel(NOTIFICATION_CAPACITY);
        Self {
            pool,
            notifications,
        }
    }
}

#[async_trait]
impl RuntimeChangeRepository for SqliteRuntimeChangeRepository {
    async fn publish_runtime_change(&self, change: &RuntimeChange) -> StoreResult<()> {
        let _ = self.notifications.send(change.clone());
        Ok(())
    }

    async fn subscribe_runtime_changes(&self) -> StoreResult<RuntimeChangeSubscription> {
        let last_revision = current_revision(&self.pool).await?;
        let receiver = self.notifications.subscribe();
        let mut interval = tokio::time::interval(REVISION_POLL_INTERVAL);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        // interval 的首个 tick 立即触发；在订阅返回前消费它，使首次数据库对账发生于一秒后。
        interval.tick().await;
        let pool = self.pool.clone();
        let subscription = stream::unfold(
            (pool, receiver, interval, last_revision),
            |(pool, mut receiver, mut interval, mut last_revision)| async move {
                loop {
                    tokio::select! {
                        message = receiver.recv() => {
                            match message {
                                Ok(change) => {
                                    let revision = match &change {
                                        RuntimeChange::SnapshotPublished { config_revision } => *config_revision,
                                    };
                                    if revision != last_revision {
                                        last_revision = revision;
                                        return Some((Ok(change), (pool, receiver, interval, last_revision)));
                                    }
                                }
                                Err(broadcast::error::RecvError::Lagged(_)) => {}
                                Err(broadcast::error::RecvError::Closed) => return None,
                            }
                        }
                        _ = interval.tick() => {
                            match current_revision(&pool).await {
                                Ok(revision) if revision != last_revision => {
                                    last_revision = revision;
                                    let change = RuntimeChange::SnapshotPublished { config_revision: revision };
                                    return Some((Ok(change), (pool, receiver, interval, last_revision)));
                                }
                                Ok(_) => {}
                                Err(error) => return Some((Err(error), (pool, receiver, interval, last_revision))),
                            }
                        }
                    }
                }
            },
        );
        Ok(Box::pin(subscription))
    }
}

impl SnapshotSubscriptionPort for SqliteRuntimeChangeRepository {
    fn publish_snapshot_revision(
        &self,
        revision: ConfigRevision,
    ) -> futures::future::BoxFuture<'_, Result<(), SnapshotSubscriptionError>> {
        Box::pin(async move {
            let config_revision = Revision::new(revision.get())
                .map_err(|_| SnapshotSubscriptionError::unavailable())?;
            self.publish_runtime_change(&RuntimeChange::SnapshotPublished { config_revision })
                .await
                .map_err(|_| SnapshotSubscriptionError::unavailable())
        })
    }

    fn subscribe_snapshot_revisions(
        &self,
    ) -> futures::future::BoxFuture<'_, Result<SnapshotRevisionStream, SnapshotSubscriptionError>>
    {
        Box::pin(async move {
            let revisions = self
                .subscribe_runtime_changes()
                .await
                .map_err(|_| SnapshotSubscriptionError::unavailable())?
                .filter_map(|change| async move {
                    match change {
                        Ok(RuntimeChange::SnapshotPublished { config_revision }) => Some(
                            ConfigRevision::new(config_revision.get())
                                .map_err(|_| SnapshotSubscriptionError::unavailable()),
                        ),
                        Err(_) => Some(Err(SnapshotSubscriptionError::unavailable())),
                    }
                });
            Ok(Box::pin(revisions) as SnapshotRevisionStream)
        })
    }
}

async fn current_revision(pool: &SqlitePool) -> StoreResult<Revision> {
    let revision =
        sqlx::query_scalar::<_, i64>("select config_revision from runtime_settings where id = 1")
            .fetch_optional(pool)
            .await
            .map_err(|_| unavailable("read SQLite runtime revision"))?
            .ok_or_else(|| StoreError::NotFound {
                entity: "runtime settings",
                id: "1".to_owned(),
                source: None,
            })?;
    Revision::new(u64::try_from(revision).map_err(|_| invalid("runtime revision is negative"))?)
}

fn unavailable(operation: &'static str) -> StoreError {
    StoreError::Unavailable {
        backend: crate::StoreBackend::Sqlite,
        message: operation.to_owned(),
        source: None,
    }
}

fn invalid(message: &str) -> StoreError {
    StoreError::InvalidData {
        entity: "runtime change",
        message: message.to_owned(),
        source: None,
    }
}
