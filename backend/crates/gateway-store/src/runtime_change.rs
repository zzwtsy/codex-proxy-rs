//! PostgreSQL、Redis 与 SQLite 共用的运行快照变更通知合同。

use std::pin::Pin;

use async_trait::async_trait;
use futures::Stream;

use crate::{Revision, StoreResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeChange {
    SnapshotPublished { config_revision: Revision },
}

pub type RuntimeChangeSubscription =
    Pin<Box<dyn Stream<Item = StoreResult<RuntimeChange>> + Send + 'static>>;

#[async_trait]
pub trait RuntimeChangeRepository: Send + Sync {
    async fn publish_runtime_change(&self, change: &RuntimeChange) -> StoreResult<()>;
    async fn subscribe_runtime_changes(&self) -> StoreResult<RuntimeChangeSubscription>;
}
