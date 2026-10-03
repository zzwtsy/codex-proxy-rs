//! Client API Key 使用时间的有界批量写回。
//!
//! 业务端口只通知稳定 Key ID；具体存储由 PostgreSQL 或 SQLite adapter 提供。

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use gateway_core::{
    engine::execution::ClientApiKeyUsageSink,
    lifecycle::CancellationToken,
    policy::ClientApiKeyId,
    task::{DaemonTask, WorkerTaskError},
};
use tokio::sync::Notify;

use crate::StoreResult;

const DEFAULT_FLUSH_DELAY: Duration = Duration::from_secs(1);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);

#[async_trait]
pub(crate) trait ClientApiKeyLastUsedRepository: Send + Sync {
    async fn touch_client_api_keys(
        &self,
        touched_at: &BTreeMap<String, DateTime<Utc>>,
    ) -> StoreResult<u64>;
}

#[derive(Clone)]
pub(crate) struct BufferedClientApiKeyUsageSink {
    state: Arc<ClientApiKeyUsageBuffer>,
}

struct ClientApiKeyUsageBuffer {
    pending: Mutex<BTreeMap<String, DateTime<Utc>>>,
    flush_requested: Notify,
}

pub(crate) struct ClientApiKeyUsageWriter {
    repository: Arc<dyn ClientApiKeyLastUsedRepository>,
    state: Arc<ClientApiKeyUsageBuffer>,
    flush_delay: Duration,
}

impl BufferedClientApiKeyUsageSink {
    pub(crate) fn new(
        repository: Arc<dyn ClientApiKeyLastUsedRepository>,
    ) -> (Self, ClientApiKeyUsageWriter) {
        Self::with_flush_delay(repository, DEFAULT_FLUSH_DELAY)
    }

    pub(crate) fn with_flush_delay(
        repository: Arc<dyn ClientApiKeyLastUsedRepository>,
        flush_delay: Duration,
    ) -> (Self, ClientApiKeyUsageWriter) {
        let state = Arc::new(ClientApiKeyUsageBuffer {
            pending: Mutex::new(BTreeMap::new()),
            flush_requested: Notify::new(),
        });
        (
            Self {
                state: Arc::clone(&state),
            },
            ClientApiKeyUsageWriter {
                repository,
                state,
                flush_delay: flush_delay.max(Duration::from_millis(1)),
            },
        )
    }

    fn queue(&self, key_id: &ClientApiKeyId) {
        let used_at = Utc::now();
        let mut pending = lock_unpoisoned(&self.state.pending);
        pending
            .entry(key_id.as_str().to_owned())
            .and_modify(|pending_at| *pending_at = (*pending_at).max(used_at))
            .or_insert(used_at);
        drop(pending);
        self.state.flush_requested.notify_one();
    }
}

impl ClientApiKeyUsageSink for BufferedClientApiKeyUsageSink {
    fn record_used(&self, key_id: &ClientApiKeyId) {
        self.queue(key_id);
    }
}

impl ClientApiKeyUsageWriter {
    async fn flush_pending(&self) -> StoreResult<u64> {
        let updates = std::mem::take(&mut *lock_unpoisoned(&self.state.pending));
        if updates.is_empty() {
            return Ok(0);
        }
        match self.repository.touch_client_api_keys(&updates).await {
            Ok(updated) => Ok(updated),
            Err(error) => {
                let mut pending = lock_unpoisoned(&self.state.pending);
                for (key_id, used_at) in updates {
                    pending
                        .entry(key_id)
                        .and_modify(|pending_at| *pending_at = (*pending_at).max(used_at))
                        .or_insert(used_at);
                }
                drop(pending);
                self.state.flush_requested.notify_one();
                Err(error)
            }
        }
    }

    async fn flush_on_shutdown(&self) {
        match tokio::time::timeout(SHUTDOWN_TIMEOUT, self.flush_pending()).await {
            Ok(Ok(updated)) if updated > 0 => {
                tracing::info!(updated, "Client API Key last-used 已在关闭前写回");
            }
            Ok(Ok(_)) => {}
            Ok(Err(_)) => {
                tracing::warn!("Client API Key last-used 关闭写回失败");
            }
            Err(_) => {
                tracing::warn!(
                    pending = lock_unpoisoned(&self.state.pending).len(),
                    "Client API Key last-used 关闭写回超时"
                );
            }
        }
    }
}

impl DaemonTask for ClientApiKeyUsageWriter {
    fn run(
        &self,
        cancellation: CancellationToken,
    ) -> futures::future::BoxFuture<'_, Result<(), WorkerTaskError>> {
        Box::pin(async move {
            loop {
                tokio::select! {
                    () = cancellation.cancelled() => {
                        self.flush_on_shutdown().await;
                        return Ok(());
                    }
                    () = self.state.flush_requested.notified() => {}
                }
                tokio::select! {
                    () = cancellation.cancelled() => {
                        self.flush_on_shutdown().await;
                        return Ok(());
                    }
                    () = tokio::time::sleep(self.flush_delay) => {}
                }
                if self.flush_pending().await.is_err() {
                    tracing::warn!("Client API Key last-used 批量写回失败");
                    return Err(WorkerTaskError::safe(
                        "client API key last-used flush failed",
                    ));
                }
            }
        })
    }
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
