//! 不依赖会话再次访问的有界 TTL 清理；不参与业务历史保留策略。

use chrono::Utc;
use futures::future::BoxFuture;
use gateway_core::task::{ScheduledTask, WorkerCycleContext, WorkerTaskError};
use sqlx::SqlitePool;

const BATCH_SIZE: i64 = 1_000;

pub(crate) struct SqliteSessionCleanupTask {
    pool: SqlitePool,
}

impl SqliteSessionCleanupTask {
    pub(crate) const fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

impl ScheduledTask for SqliteSessionCleanupTask {
    fn run_cycle(&self, context: WorkerCycleContext) -> BoxFuture<'_, Result<(), WorkerTaskError>> {
        Box::pin(async move {
            let now = Utc::now().timestamp_micros();
            for statement in [
                "delete from provider_session_affinity where rowid in (
                   select rowid from provider_session_affinity where expires_at_us <= ?1
                   order by expires_at_us, rowid limit ?2
                 )",
                "delete from provider_session_exclusions where rowid in (
                   select rowid from provider_session_exclusions where expires_at_us <= ?1
                   order by expires_at_us, rowid limit ?2
                 )",
            ] {
                if context.cancellation().is_cancelled() {
                    return Ok(());
                }
                // 每表独立原子提交，避免持有长写事务；续期与删除由 SQLite 写锁串行化。
                sqlx::query(statement)
                    .bind(now)
                    .bind(BATCH_SIZE)
                    .execute(&self.pool)
                    .await
                    .map_err(|_| WorkerTaskError::safe("SQLite session cleanup failed"))?;
            }
            Ok(())
        })
    }
}
