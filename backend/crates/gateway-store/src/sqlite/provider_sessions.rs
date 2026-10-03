//! SQLite Provider 会话亲和与失败账号排除状态。

use std::time::Duration;

use chrono::Utc;
use futures::future::BoxFuture;
use gateway_core::{
    account::ProviderAccountId,
    provider_ports::{
        ProviderSessionAffinityKey, ProviderSessionAffinityPort, ProviderSessionExclusionPort,
        ProviderSessionExclusions, ProviderStoreError, ProviderStoreErrorKind,
    },
    routing::ProviderKind,
};
use sqlx::SqlitePool;
use uuid::Uuid;

use crate::coordination::resource_fingerprint;

use super::value::{datetime_to_micros, duration_micros};

const MAX_SESSION_STATE_TTL: Duration = Duration::from_secs(24 * 60 * 60);

#[derive(Clone)]
pub struct SqliteProviderSessionAffinityRepository {
    pool: SqlitePool,
}

impl SqliteProviderSessionAffinityRepository {
    #[must_use]
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    fn key(
        provider_kind: &ProviderKind,
        affinity_key: &ProviderSessionAffinityKey,
    ) -> Result<String, ProviderStoreError> {
        let scope = format!(
            "{}\0{}",
            provider_kind.as_str(),
            affinity_key.expose_to_store()
        );
        resource_fingerprint("provider session affinity", &scope)
            .map_err(|_| provider_invalid("encode provider session affinity key"))
    }

    fn ttl(ttl: Duration) -> Result<i64, ProviderStoreError> {
        if ttl.is_zero() || ttl > MAX_SESSION_STATE_TTL {
            return Err(provider_invalid("validate provider session affinity TTL"));
        }
        duration_micros(ttl).map_err(|_| provider_invalid("validate provider session affinity TTL"))
    }
}

impl ProviderSessionAffinityPort for SqliteProviderSessionAffinityRepository {
    fn load<'a>(
        &'a self,
        provider_kind: &'a ProviderKind,
        key: &'a ProviderSessionAffinityKey,
    ) -> BoxFuture<'a, Result<Option<ProviderAccountId>, ProviderStoreError>> {
        Box::pin(async move {
            let fingerprint = Self::key(provider_kind, key)?;
            let now = datetime_to_micros(Utc::now());
            let account_id = sqlx::query_scalar::<_, String>(
                "SELECT account_id FROM provider_session_affinity                  WHERE session_fingerprint = ? AND expires_at_us > ?",
            )
            .bind(&fingerprint)
            .bind(now)
            .fetch_optional(&self.pool)
            .await
            .map_err(|_| provider_unavailable("load provider session affinity"))?;
            if account_id.is_none() {
                sqlx::query(
                    "DELETE FROM provider_session_affinity                      WHERE session_fingerprint = ? AND expires_at_us <= ?",
                )
                .bind(fingerprint)
                .bind(now)
                .execute(&self.pool)
                .await
                .map_err(|_| provider_unavailable("clean expired provider session affinity"))?;
            }
            account_id
                .map(|account_id| {
                    ProviderAccountId::new(account_id)
                        .map_err(|_| provider_invalid("decode provider session affinity"))
                })
                .transpose()
        })
    }

    fn bind<'a>(
        &'a self,
        provider_kind: &'a ProviderKind,
        key: &'a ProviderSessionAffinityKey,
        account_id: &'a ProviderAccountId,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<(), ProviderStoreError>> {
        Box::pin(async move {
            let fingerprint = Self::key(provider_kind, key)?;
            let ttl = Self::ttl(ttl)?;
            let mut transaction = self
                .pool
                .begin()
                .await
                .map_err(|_| provider_unavailable("bind provider session affinity"))?;
            super::acquire_write_lock(&mut transaction)
                .await
                .map_err(|_| provider_unavailable("bind provider session affinity"))?;
            let expires_at = datetime_to_micros(Utc::now())
                .checked_add(ttl)
                .ok_or_else(|| provider_invalid("validate provider session affinity expiry"))?;
            sqlx::query(
                "INSERT INTO provider_session_affinity                  (session_fingerprint, account_id, revision, expires_at_us) VALUES (?, ?, 1, ?)                  ON CONFLICT (session_fingerprint) DO UPDATE SET                    account_id = excluded.account_id, revision = provider_session_affinity.revision + 1,                    expires_at_us = excluded.expires_at_us",
            )
            .bind(fingerprint)
            .bind(account_id.as_str())
            .bind(expires_at)
            .execute(&mut *transaction)
            .await
            .map_err(|_| provider_unavailable("bind provider session affinity"))?;
            transaction
                .commit()
                .await
                .map_err(|_| provider_unavailable("bind provider session affinity"))
        })
    }

    fn claim_or_load<'a>(
        &'a self,
        provider_kind: &'a ProviderKind,
        key: &'a ProviderSessionAffinityKey,
        candidate_account_id: &'a ProviderAccountId,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<ProviderAccountId, ProviderStoreError>> {
        Box::pin(async move {
            let fingerprint = Self::key(provider_kind, key)?;
            let candidate = candidate_account_id.as_str();
            let ttl = Self::ttl(ttl)?;
            let mut transaction = self
                .pool
                .begin()
                .await
                .map_err(|_| provider_unavailable("claim provider session affinity"))?;
            super::acquire_write_lock(&mut transaction)
                .await
                .map_err(|_| provider_unavailable("claim provider session affinity"))?;
            let now = datetime_to_micros(Utc::now());
            let expires_at = now
                .checked_add(ttl)
                .ok_or_else(|| provider_invalid("validate provider session affinity expiry"))?;
            sqlx::query(
                "INSERT INTO provider_session_affinity                  (session_fingerprint, account_id, revision, expires_at_us) VALUES (?, ?, 1, ?)                  ON CONFLICT (session_fingerprint) DO NOTHING",
            )
            .bind(&fingerprint)
            .bind(candidate)
            .bind(expires_at)
            .execute(&mut *transaction)
            .await
            .map_err(|_| provider_unavailable("claim provider session affinity"))?;
            let mut account_id: Option<String> = sqlx::query_scalar(
                "UPDATE provider_session_affinity                  SET account_id = ?, revision = revision + 1, expires_at_us = ?                  WHERE session_fingerprint = ? AND expires_at_us <= ?                  RETURNING account_id",
            )
            .bind(candidate)
            .bind(expires_at)
            .bind(&fingerprint)
            .bind(now)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|_| provider_unavailable("claim provider session affinity"))?;
            if account_id.is_none() {
                account_id = sqlx::query_scalar(
                    "SELECT account_id FROM provider_session_affinity WHERE session_fingerprint = ?",
                )
                .bind(&fingerprint)
                .fetch_optional(&mut *transaction)
                .await
                .map_err(|_| provider_unavailable("load provider session affinity"))?;
            }
            if account_id.as_deref() == Some(candidate) {
                sqlx::query(
                    "UPDATE provider_session_affinity SET expires_at_us = ?, revision = revision + 1                      WHERE session_fingerprint = ? AND expires_at_us > ? AND account_id = ?",
                )
                .bind(expires_at)
                .bind(&fingerprint)
                .bind(now)
                .bind(candidate)
                .execute(&mut *transaction)
                .await
                .map_err(|_| provider_unavailable("refresh provider session affinity"))?;
            }
            transaction
                .commit()
                .await
                .map_err(|_| provider_unavailable("claim provider session affinity"))?;
            ProviderAccountId::new(
                account_id
                    .ok_or_else(|| provider_unavailable("claim provider session affinity"))?,
            )
            .map_err(|_| provider_invalid("decode provider session affinity"))
        })
    }

    fn compare_and_bind<'a>(
        &'a self,
        provider_kind: &'a ProviderKind,
        key: &'a ProviderSessionAffinityKey,
        expected_account_id: &'a ProviderAccountId,
        replacement_account_id: &'a ProviderAccountId,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<ProviderAccountId, ProviderStoreError>> {
        Box::pin(async move {
            let fingerprint = Self::key(provider_kind, key)?;
            let ttl = Self::ttl(ttl)?;
            let mut transaction = self
                .pool
                .begin()
                .await
                .map_err(|_| provider_unavailable("compare provider session affinity"))?;
            super::acquire_write_lock(&mut transaction)
                .await
                .map_err(|_| provider_unavailable("compare provider session affinity"))?;
            let now = datetime_to_micros(Utc::now());
            let expires_at = now
                .checked_add(ttl)
                .ok_or_else(|| provider_invalid("validate provider session affinity expiry"))?;
            sqlx::query(
                "INSERT INTO provider_session_affinity                  (session_fingerprint, account_id, revision, expires_at_us) VALUES (?, ?, 1, ?)                  ON CONFLICT (session_fingerprint) DO NOTHING",
            )
            .bind(&fingerprint)
            .bind(replacement_account_id.as_str())
            .bind(expires_at)
            .execute(&mut *transaction)
            .await
            .map_err(|_| provider_unavailable("compare provider session affinity"))?;
            let mut account_id: Option<String> = sqlx::query_scalar(
                "UPDATE provider_session_affinity                  SET account_id = ?, revision = revision + 1, expires_at_us = ?                  WHERE session_fingerprint = ? AND (expires_at_us <= ? OR account_id = ?)                  RETURNING account_id",
            )
            .bind(replacement_account_id.as_str())
            .bind(expires_at)
            .bind(&fingerprint)
            .bind(now)
            .bind(expected_account_id.as_str())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|_| provider_unavailable("compare provider session affinity"))?;
            if account_id.is_none() {
                account_id = sqlx::query_scalar(
                    "SELECT account_id FROM provider_session_affinity WHERE session_fingerprint = ?",
                )
                .bind(&fingerprint)
                .fetch_optional(&mut *transaction)
                .await
                .map_err(|_| provider_unavailable("load provider session affinity"))?;
            }
            transaction
                .commit()
                .await
                .map_err(|_| provider_unavailable("compare provider session affinity"))?;
            ProviderAccountId::new(
                account_id
                    .ok_or_else(|| provider_unavailable("compare provider session affinity"))?,
            )
            .map_err(|_| provider_invalid("decode provider session affinity"))
        })
    }

    fn clear<'a>(
        &'a self,
        provider_kind: &'a ProviderKind,
        key: &'a ProviderSessionAffinityKey,
    ) -> BoxFuture<'a, Result<bool, ProviderStoreError>> {
        Box::pin(async move {
            let fingerprint = Self::key(provider_kind, key)?;
            let result =
                sqlx::query("DELETE FROM provider_session_affinity WHERE session_fingerprint = ?")
                    .bind(fingerprint)
                    .execute(&self.pool)
                    .await
                    .map_err(|_| provider_unavailable("clear provider session affinity"))?;
            Ok(result.rows_affected() > 0)
        })
    }
}

#[derive(Clone)]
pub struct SqliteProviderSessionExclusionRepository {
    pool: SqlitePool,
}

impl SqliteProviderSessionExclusionRepository {
    #[must_use]
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    fn key(
        provider_kind: &ProviderKind,
        affinity_key: &ProviderSessionAffinityKey,
    ) -> Result<String, ProviderStoreError> {
        let scope = format!(
            "{}\0{}",
            provider_kind.as_str(),
            affinity_key.expose_to_store()
        );
        resource_fingerprint("provider session exclusion", &scope)
            .map_err(|_| provider_invalid("encode provider session exclusion key"))
    }

    fn ttl(ttl: Duration) -> Result<i64, ProviderStoreError> {
        if ttl.is_zero() || ttl > MAX_SESSION_STATE_TTL {
            return Err(provider_invalid("validate provider session exclusion TTL"));
        }
        duration_micros(ttl)
            .map_err(|_| provider_invalid("validate provider session exclusion TTL"))
    }

    async fn load_active(
        &self,
        fingerprint: &str,
    ) -> Result<Option<ProviderSessionExclusions>, ProviderStoreError> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|_| provider_unavailable("load provider session exclusion"))?;
        let cleanup_before = datetime_to_micros(Utc::now());
        sqlx::query(
            "DELETE FROM provider_session_exclusions              WHERE session_fingerprint = ? AND expires_at_us <= ?",
        )
        .bind(fingerprint)
        .bind(cleanup_before)
        .execute(&mut *transaction)
        .await
        .map_err(|_| provider_unavailable("clean provider session exclusion"))?;
        let now = datetime_to_micros(Utc::now());
        let rows = sqlx::query(
            "SELECT account_id, revision FROM provider_session_exclusions              WHERE session_fingerprint = ? AND expires_at_us > ? ORDER BY account_id",
        )
        .bind(fingerprint)
        .bind(now)
        .fetch_all(&mut *transaction)
        .await
        .map_err(|_| provider_unavailable("load provider session exclusion"))?;
        transaction
            .commit()
            .await
            .map_err(|_| provider_unavailable("load provider session exclusion"))?;
        if rows.is_empty() {
            return Ok(None);
        }
        let mut accounts = std::collections::BTreeSet::new();
        let mut revision = None;
        for row in rows {
            let account_id: String = sqlx::Row::try_get(&row, "account_id")
                .map_err(|_| provider_unavailable("decode provider session exclusion"))?;
            let row_revision: String = sqlx::Row::try_get(&row, "revision")
                .map_err(|_| provider_unavailable("decode provider session exclusion"))?;
            if revision
                .as_ref()
                .is_some_and(|current| current != &row_revision)
            {
                return Err(provider_unavailable(
                    "provider session exclusion revision mismatch",
                ));
            }
            revision = Some(row_revision);
            accounts.insert(
                ProviderAccountId::new(account_id)
                    .map_err(|_| provider_invalid("decode provider session exclusion"))?,
            );
        }
        Ok(Some(ProviderSessionExclusions::new(
            accounts,
            revision.unwrap_or_default(),
        )))
    }
}

impl ProviderSessionExclusionPort for SqliteProviderSessionExclusionRepository {
    fn load<'a>(
        &'a self,
        provider_kind: &'a ProviderKind,
        key: &'a ProviderSessionAffinityKey,
    ) -> BoxFuture<'a, Result<Option<ProviderSessionExclusions>, ProviderStoreError>> {
        Box::pin(async move {
            let fingerprint = SqliteProviderSessionExclusionRepository::key(provider_kind, key)?;
            self.load_active(&fingerprint).await
        })
    }

    fn record_failure<'a>(
        &'a self,
        provider_kind: &'a ProviderKind,
        key: &'a ProviderSessionAffinityKey,
        account_id: &'a ProviderAccountId,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<ProviderSessionExclusions, ProviderStoreError>> {
        Box::pin(async move {
            let fingerprint = SqliteProviderSessionExclusionRepository::key(provider_kind, key)?;
            let ttl = Self::ttl(ttl)?;
            let mut transaction = self
                .pool
                .begin()
                .await
                .map_err(|_| provider_unavailable("record provider session exclusion"))?;
            super::acquire_write_lock(&mut transaction)
                .await
                .map_err(|_| provider_unavailable("record provider session exclusion"))?;
            let now = datetime_to_micros(Utc::now());
            let expires_at = now
                .checked_add(ttl)
                .ok_or_else(|| provider_invalid("validate provider session exclusion expiry"))?;
            let revision = Uuid::now_v7().to_string();
            sqlx::query(
                "DELETE FROM provider_session_exclusions                  WHERE session_fingerprint = ? AND expires_at_us <= ?",
            )
            .bind(&fingerprint)
            .bind(now)
            .execute(&mut *transaction)
            .await
            .map_err(|_| provider_unavailable("clean provider session exclusion"))?;
            sqlx::query(
                "UPDATE provider_session_exclusions SET revision = ?                  WHERE session_fingerprint = ? AND expires_at_us > ?",
            )
            .bind(&revision)
            .bind(&fingerprint)
            .bind(now)
            .execute(&mut *transaction)
            .await
            .map_err(|_| provider_unavailable("record provider session exclusion"))?;
            sqlx::query(
                "INSERT INTO provider_session_exclusions                  (session_fingerprint, account_id, revision, expires_at_us) VALUES (?, ?, ?, ?)                  ON CONFLICT (session_fingerprint, account_id) DO UPDATE SET                    revision = excluded.revision, expires_at_us = excluded.expires_at_us",
            )
            .bind(&fingerprint)
            .bind(account_id.as_str())
            .bind(&revision)
            .bind(expires_at)
            .execute(&mut *transaction)
            .await
            .map_err(|_| provider_unavailable("record provider session exclusion"))?;
            transaction
                .commit()
                .await
                .map_err(|_| provider_unavailable("record provider session exclusion"))?;
            self.load_active(&fingerprint)
                .await?
                .ok_or_else(|| provider_unavailable("record provider session exclusion"))
        })
    }

    fn clear<'a>(
        &'a self,
        provider_kind: &'a ProviderKind,
        key: &'a ProviderSessionAffinityKey,
        expected_revision: &'a str,
    ) -> BoxFuture<'a, Result<bool, ProviderStoreError>> {
        Box::pin(async move {
            let fingerprint = SqliteProviderSessionExclusionRepository::key(provider_kind, key)?;
            let result = sqlx::query(
                "DELETE FROM provider_session_exclusions                  WHERE session_fingerprint = ? AND revision = ?",
            )
            .bind(fingerprint)
            .bind(expected_revision)
            .execute(&self.pool)
            .await
            .map_err(|_| provider_unavailable("clear provider session exclusion"))?;
            Ok(result.rows_affected() > 0)
        })
    }
}

fn provider_unavailable(_operation: &'static str) -> ProviderStoreError {
    ProviderStoreError::new(
        ProviderStoreErrorKind::Unavailable,
        "SQLite Provider state failed",
    )
}

fn provider_invalid(operation: &'static str) -> ProviderStoreError {
    ProviderStoreError::new(ProviderStoreErrorKind::InvalidData, operation)
}
