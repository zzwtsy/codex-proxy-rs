//! SQLite Client API Key 最近使用时间适配。

use std::{collections::BTreeMap, time::Duration};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::future::BoxFuture;
use gateway_core::{
    engine::execution::ClientApiKeyUsageSink,
    lifecycle::CancellationToken,
    policy::ClientApiKeyId,
    task::{DaemonTask, WorkerTaskError},
};
use sqlx::SqlitePool;

use crate::{
    ClientKeyEnabledRepository, StoreResult, admin_store_error,
    client_key_usage::ClientApiKeyLastUsedRepository,
};

use super::sqlite_unavailable;

#[derive(Clone)]
pub struct SqliteClientApiKeyRepository {
    pool: SqlitePool,
}

/// 保留最近使用时间仓储的既有名称，方便现有调用点平滑迁移。
pub type SqliteClientApiKeyLastUsedRepository = SqliteClientApiKeyRepository;

impl SqliteClientApiKeyRepository {
    #[must_use]
    pub const fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// 认证恢复只查询目标 Key 的启用状态，不加载 Key 明文或其他资料。
    pub async fn is_enabled(
        &self,
        id: &ClientApiKeyId,
    ) -> gateway_admin::ports::store::AdminStoreResult<bool> {
        sqlx::query_scalar::<_, i64>(
            "select exists(select 1 from client_api_keys where id = ?1 and enabled = 1)",
        )
        .bind(id.as_str())
        .fetch_one(&self.pool)
        .await
        .map(|enabled| enabled != 0)
        .map_err(|_| {
            admin_store_error(
                "client API key",
                sqlite_unavailable("read SQLite Client API Key status"),
            )
        })
    }
}

#[async_trait]
impl ClientApiKeyLastUsedRepository for SqliteClientApiKeyRepository {
    async fn touch_client_api_keys(
        &self,
        touched_at: &BTreeMap<String, DateTime<Utc>>,
    ) -> StoreResult<u64> {
        if touched_at.is_empty() {
            return Ok(0);
        }
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|_| sqlite_unavailable("begin SQLite Client API Key usage update"))?;
        let mut updated = 0_u64;
        for (id, used_at) in touched_at {
            let result = sqlx::query(
                "update client_api_keys
                 set last_used_at_us = max(coalesce(last_used_at_us, ?2), ?2)
                 where id = ?1",
            )
            .bind(id)
            .bind(used_at.timestamp_micros())
            .execute(&mut *transaction)
            .await
            .map_err(|_| sqlite_unavailable("touch SQLite Client API Keys"))?;
            updated = updated.saturating_add(result.rows_affected());
        }
        transaction
            .commit()
            .await
            .map_err(|_| sqlite_unavailable("commit SQLite Client API Key usage update"))?;
        Ok(updated)
    }
}

#[async_trait]
impl ClientKeyEnabledRepository for SqliteClientApiKeyRepository {
    async fn is_enabled(
        &self,
        id: &ClientApiKeyId,
    ) -> gateway_admin::ports::store::AdminStoreResult<bool> {
        SqliteClientApiKeyRepository::is_enabled(self, id).await
    }
}

#[derive(Clone)]
pub struct SqliteClientApiKeyUsageSink {
    inner: crate::client_key_usage::BufferedClientApiKeyUsageSink,
}

pub struct SqliteClientApiKeyUsageWriter {
    inner: crate::client_key_usage::ClientApiKeyUsageWriter,
}

impl SqliteClientApiKeyUsageSink {
    #[must_use]
    pub fn new(pool: SqlitePool) -> (Self, SqliteClientApiKeyUsageWriter) {
        let repository: std::sync::Arc<dyn ClientApiKeyLastUsedRepository> =
            std::sync::Arc::new(SqliteClientApiKeyRepository::new(pool));
        let (inner, writer) =
            crate::client_key_usage::BufferedClientApiKeyUsageSink::new(repository);
        (
            Self { inner },
            SqliteClientApiKeyUsageWriter { inner: writer },
        )
    }

    #[must_use]
    pub fn with_flush_delay(
        pool: SqlitePool,
        flush_delay: Duration,
    ) -> (Self, SqliteClientApiKeyUsageWriter) {
        let repository: std::sync::Arc<dyn ClientApiKeyLastUsedRepository> =
            std::sync::Arc::new(SqliteClientApiKeyRepository::new(pool));
        let (inner, writer) =
            crate::client_key_usage::BufferedClientApiKeyUsageSink::with_flush_delay(
                repository,
                flush_delay,
            );
        (
            Self { inner },
            SqliteClientApiKeyUsageWriter { inner: writer },
        )
    }
}

impl ClientApiKeyUsageSink for SqliteClientApiKeyUsageSink {
    fn record_used(&self, key_id: &ClientApiKeyId) {
        self.inner.record_used(key_id);
    }
}

impl DaemonTask for SqliteClientApiKeyUsageWriter {
    fn run(&self, cancellation: CancellationToken) -> BoxFuture<'_, Result<(), WorkerTaskError>> {
        self.inner.run(cancellation)
    }
}
