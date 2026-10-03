//! SQLite 分批保留期删除适配。

use std::num::NonZeroU32;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use gateway_admin::{
    model::retention::{RetentionPolicy, RetentionTarget},
    ports::{
        retention::RetentionStore,
        store::{AdminStoreError, AdminStoreErrorKind, AdminStoreResult},
    },
};
use sqlx::SqlitePool;

use crate::admin_store_error;

use super::sqlite_unavailable;

#[derive(Clone)]
pub struct SqliteRetentionRepository {
    pool: SqlitePool,
}

impl SqliteRetentionRepository {
    #[must_use]
    pub const fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl RetentionStore for SqliteRetentionRepository {
    async fn load_policy(&self) -> AdminStoreResult<RetentionPolicy> {
        let row = sqlx::query_as::<_, (i64, i64, i64)>(
            "select usage_retention_days, ops_event_retention_days, audit_retention_days
             from runtime_settings where id = 1",
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| unavailable("load SQLite retention settings"))?
        .ok_or_else(|| {
            AdminStoreError::new(
                AdminStoreErrorKind::NotFound,
                "retention settings",
                "runtime settings not found",
            )
        })?;
        let to_days = |value| u32::try_from(value).map_err(|_| invalid_policy());
        RetentionPolicy::try_new(to_days(row.0)?, to_days(row.1)?, to_days(row.2)?)
            .map_err(|_| invalid_policy())
    }

    async fn purge_batch(
        &self,
        target: RetentionTarget,
        now: DateTime<Utc>,
        policy: RetentionPolicy,
        limit: NonZeroU32,
    ) -> AdminStoreResult<u64> {
        let days_micros = i64::from(policy.days(target))
            .checked_mul(86_400_000_000)
            .ok_or_else(invalid_policy)?;
        let cutoff_us = now
            .timestamp_micros()
            .checked_sub(days_micros)
            .ok_or_else(invalid_policy)?;
        let sql = match target {
            RetentionTarget::ModelRequests => {
                "delete from model_requests where id in (
                   select id from model_requests
                   where outcome <> 'running' and completed_at_us < ?1
                   order by completed_at_us, id limit ?2
                 )"
            }
            RetentionTarget::OpsEvents => {
                "delete from ops_events where id in (
                   select id from ops_events
                   where model_request_id is null and created_at_us < ?1
                   order by created_at_us, id limit ?2
                 )"
            }
            RetentionTarget::AdminAuditEvents => {
                "delete from admin_audit_events where id in (
                   select id from admin_audit_events
                   where created_at_us < ?1
                   order by created_at_us, id limit ?2
                 )"
            }
        };
        sqlx::query(sql)
            .bind(cutoff_us)
            .bind(i64::from(limit.get()))
            .execute(&self.pool)
            .await
            .map(|result| result.rows_affected())
            .map_err(|_| unavailable("delete expired SQLite records"))
    }
}

fn unavailable(operation: &'static str) -> AdminStoreError {
    admin_store_error("retention", sqlite_unavailable(operation))
}

fn invalid_policy() -> AdminStoreError {
    AdminStoreError::new(
        AdminStoreErrorKind::Invalid,
        "retention settings",
        "invalid retention policy",
    )
}
