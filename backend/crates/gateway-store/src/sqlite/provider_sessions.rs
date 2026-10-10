//! SQLite Provider 会话亲和与失败账号排除状态

use std::time::Duration;

use chrono::Utc;
use futures::future::BoxFuture;
use gateway_core::{
    account::ProviderAccountId,
    provider_ports::{
        ProviderSessionAffinityKey, ProviderSessionAffinityPort, ProviderSessionAlias,
        ProviderSessionBinding, ProviderSessionExclusionPort, ProviderSessionExclusions,
        ProviderStoreError, ProviderStoreErrorKind,
    },
    routing::ProviderKind,
};
use sqlx::SqlitePool;
use uuid::Uuid;

use crate::coordination::resource_fingerprint;

use super::value::{datetime_to_micros, duration_micros};

const MAX_SESSION_AFFINITY_TTL: Duration =
    Duration::from_secs(gateway_core::account::MAX_SESSION_AFFINITY_TTL_HOURS as u64 * 60 * 60);
const MAX_SESSION_EXCLUSION_TTL: Duration = Duration::from_secs(24 * 60 * 60);

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

    fn alias_key(
        provider_kind: &ProviderKind,
        alias: &ProviderSessionAffinityKey,
    ) -> Result<String, ProviderStoreError> {
        let scope = format!("{}\0{}", provider_kind.as_str(), alias.expose_to_store());
        resource_fingerprint("provider session alias", &scope)
            .map_err(|_| provider_invalid("encode provider session alias key"))
    }

    fn ttl(ttl: Duration) -> Result<i64, ProviderStoreError> {
        if ttl.is_zero() || ttl > MAX_SESSION_AFFINITY_TTL {
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
    ) -> BoxFuture<'a, Result<Option<ProviderSessionBinding>, ProviderStoreError>> {
        Box::pin(async move {
            let fingerprint = Self::key(provider_kind, key)?;
            let now = datetime_to_micros(Utc::now());
            let binding = sqlx::query_as::<_, (String, String)>(
                "SELECT account_id, revision FROM provider_session_affinity
                 WHERE session_fingerprint = ? AND expires_at_us > ?",
            )
            .bind(&fingerprint)
            .bind(now)
            .fetch_optional(&self.pool)
            .await
            .map_err(|source| {
                super::provider_query_error("load provider session affinity", source)
            })?;
            binding
                .map(|(account_id, revision)| {
                    let account_id = ProviderAccountId::new(account_id)
                        .map_err(|_| provider_invalid("decode provider session affinity"))?;
                    ProviderSessionBinding::new(account_id, revision)
                })
                .transpose()
        })
    }

    fn compare_and_bind<'a>(
        &'a self,
        provider_kind: &'a ProviderKind,
        key: &'a ProviderSessionAffinityKey,
        expected: Option<&'a ProviderSessionBinding>,
        account_id: &'a ProviderAccountId,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<Option<ProviderSessionBinding>, ProviderStoreError>> {
        Box::pin(async move {
            let fingerprint = Self::key(provider_kind, key)?;
            let ttl = Self::ttl(ttl)?;
            let mut transaction = self.pool.begin().await.map_err(|source| {
                crate::provider_unavailable("admit provider session affinity", source)
            })?;
            super::acquire_write_lock(&mut transaction)
                .await
                .map_err(|source| {
                    crate::provider_unavailable("admit provider session affinity", source)
                })?;
            let now = datetime_to_micros(Utc::now());
            let current = sqlx::query_as::<_, (String, String)>(
                "SELECT account_id, revision FROM provider_session_affinity
                 WHERE session_fingerprint = ? AND expires_at_us > ?",
            )
            .bind(&fingerprint)
            .bind(now)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|source| {
                super::provider_query_error("load provider session affinity", source)
            })?
            .map(|(current_account, revision)| {
                let current_account = ProviderAccountId::new(current_account)
                    .map_err(|_| provider_invalid("decode provider session affinity"))?;
                ProviderSessionBinding::new(current_account, revision)
            })
            .transpose()?;

            if current.as_ref() != expected {
                return Ok(None);
            }

            let binding = match expected.filter(|binding| binding.account_id() == account_id) {
                Some(binding) => binding.clone(),
                None => ProviderSessionBinding::new(
                    account_id.clone(),
                    format!("a{}", &Uuid::new_v4().simple().to_string()[1..]),
                )?,
            };
            let expires_at = now
                .checked_add(ttl)
                .ok_or_else(|| provider_invalid("validate provider session affinity expiry"))?;
            sqlx::query(
                "INSERT INTO provider_session_affinity
                 (session_fingerprint, account_id, revision, expires_at_us)
                 VALUES (?, ?, ?, ?)
                 ON CONFLICT (session_fingerprint) DO UPDATE SET
                   account_id = excluded.account_id,
                   revision = excluded.revision,
                   expires_at_us = excluded.expires_at_us",
            )
            .bind(fingerprint)
            .bind(binding.account_id().as_str())
            .bind(binding.revision())
            .bind(expires_at)
            .execute(&mut *transaction)
            .await
            .map_err(|source| {
                crate::provider_unavailable("admit provider session affinity", source)
            })?;
            transaction.commit().await.map_err(|source| {
                crate::provider_unavailable("admit provider session affinity", source)
            })?;
            Ok(Some(binding))
        })
    }

    fn load_alias<'a>(
        &'a self,
        provider: &'a ProviderKind,
        alias: &'a ProviderSessionAffinityKey,
    ) -> BoxFuture<'a, Result<Option<ProviderSessionAlias>, ProviderStoreError>> {
        Box::pin(async move {
            let fingerprint = Self::alias_key(provider, alias)?;
            let now = datetime_to_micros(Utc::now());
            let value = sqlx::query_as::<_, (String, i64, Option<String>)>(
                "SELECT session_key, follow_only, root_session_key FROM provider_session_aliases
                 WHERE alias_fingerprint = ? AND expires_at_us > ?",
            )
            .bind(&fingerprint)
            .bind(now)
            .fetch_optional(&self.pool)
            .await
            .map_err(|source| super::provider_query_error("load provider session alias", source))?;
            value
                .map(|(session_key, follow_only, root_session_key)| {
                    Ok(ProviderSessionAlias {
                        session_key: ProviderSessionAffinityKey::try_new(session_key)?,
                        root_session_key: root_session_key
                            .map(ProviderSessionAffinityKey::try_new)
                            .transpose()?,
                        follow_only: follow_only != 0,
                    })
                })
                .transpose()
        })
    }

    fn bind_alias<'a>(
        &'a self,
        provider: &'a ProviderKind,
        alias: &'a ProviderSessionAffinityKey,
        session: &'a ProviderSessionAlias,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<bool, ProviderStoreError>> {
        Box::pin(async move {
            let fingerprint = Self::alias_key(provider, alias)?;
            let ttl = Self::ttl(ttl)?;
            let mut transaction = self.pool.begin().await.map_err(|source| {
                crate::provider_unavailable("bind provider session alias", source)
            })?;
            super::acquire_write_lock(&mut transaction)
                .await
                .map_err(|source| {
                    crate::provider_unavailable("bind provider session alias", source)
                })?;
            let now = datetime_to_micros(Utc::now());
            let expires_at = now
                .checked_add(ttl)
                .ok_or_else(|| provider_invalid("validate provider session alias expiry"))?;
            let result = sqlx::query(
                "INSERT INTO provider_session_aliases
                 (alias_fingerprint, session_key, follow_only, expires_at_us, root_session_key)
                 VALUES (?, ?, ?, ?, ?)
                 ON CONFLICT (alias_fingerprint) DO UPDATE SET
                   session_key = excluded.session_key,
                   follow_only = excluded.follow_only,
                   expires_at_us = excluded.expires_at_us,
                   root_session_key = excluded.root_session_key
                 WHERE provider_session_aliases.expires_at_us <= ?
                    OR (provider_session_aliases.session_key = excluded.session_key
                        AND provider_session_aliases.follow_only = excluded.follow_only
                        AND provider_session_aliases.root_session_key IS excluded.root_session_key)",
            )
            .bind(fingerprint)
            .bind(session.session_key.expose_to_store())
            .bind(i64::from(session.follow_only))
            .bind(expires_at)
            .bind(
                session
                    .root_session_key
                    .as_ref()
                    .map(ProviderSessionAffinityKey::expose_to_store),
            )
            .bind(now)
            .execute(&mut *transaction)
            .await
            .map_err(|source| crate::provider_unavailable("bind provider session alias", source))?;
            transaction.commit().await.map_err(|source| {
                crate::provider_unavailable("bind provider session alias", source)
            })?;
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
        if ttl.is_zero() || ttl > MAX_SESSION_EXCLUSION_TTL {
            return Err(provider_invalid("validate provider session exclusion TTL"));
        }
        duration_micros(ttl)
            .map_err(|_| provider_invalid("validate provider session exclusion TTL"))
    }

    async fn load_active<'e>(
        executor: impl sqlx::Executor<'e, Database = sqlx::Sqlite>,
        fingerprint: &str,
        now: i64,
    ) -> Result<Option<ProviderSessionExclusions>, ProviderStoreError> {
        // 单条查询使用同一快照，过期记录由写入路径和后台任务清理
        let rows = sqlx::query_as::<_, (String, String)>(
            "SELECT account_id, revision FROM provider_session_exclusions
             WHERE session_fingerprint = ? AND expires_at_us > ? ORDER BY account_id",
        )
        .bind(fingerprint)
        .bind(now)
        .fetch_all(executor)
        .await
        .map_err(|source| super::provider_query_error("load provider session exclusion", source))?;
        if rows.is_empty() {
            return Ok(None);
        }
        let mut accounts = std::collections::BTreeSet::new();
        let mut revision = None;
        for (account_id, row_revision) in rows {
            if revision
                .as_ref()
                .is_some_and(|current| current != &row_revision)
            {
                return Err(ProviderStoreError::new(
                    ProviderStoreErrorKind::InvalidData,
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
            Self::load_active(&self.pool, &fingerprint, datetime_to_micros(Utc::now())).await
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
            let mut transaction = self.pool.begin().await.map_err(|source| {
                crate::provider_unavailable("record provider session exclusion", source)
            })?;
            super::acquire_write_lock(&mut transaction)
                .await
                .map_err(|source| {
                    crate::provider_unavailable("record provider session exclusion", source)
                })?;
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
            .map_err(|source| crate::provider_unavailable("clean provider session exclusion", source))?;
            sqlx::query(
                "UPDATE provider_session_exclusions SET revision = ?, expires_at_us = ?
                 WHERE session_fingerprint = ? AND expires_at_us > ?",
            )
            .bind(&revision)
            .bind(expires_at)
            .bind(&fingerprint)
            .bind(now)
            .execute(&mut *transaction)
            .await
            .map_err(|source| {
                crate::provider_unavailable("record provider session exclusion", source)
            })?;
            sqlx::query(
                "INSERT INTO provider_session_exclusions                  (session_fingerprint, account_id, revision, expires_at_us) VALUES (?, ?, ?, ?)                  ON CONFLICT (session_fingerprint, account_id) DO UPDATE SET                    revision = excluded.revision, expires_at_us = excluded.expires_at_us",
            )
            .bind(&fingerprint)
            .bind(account_id.as_str())
            .bind(&revision)
            .bind(expires_at)
            .execute(&mut *transaction)
            .await
            .map_err(|source| crate::provider_unavailable("record provider session exclusion", source))?;
            let state = Self::load_active(&mut *transaction, &fingerprint, now)
                .await?
                .ok_or_else(|| provider_invalid("record provider session exclusion snapshot"))?;
            transaction.commit().await.map_err(|source| {
                crate::provider_unavailable("record provider session exclusion", source)
            })?;
            Ok(state)
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
            .map_err(|source| crate::provider_unavailable("clear provider session exclusion", source))?;
            Ok(result.rows_affected() > 0)
        })
    }
}

fn provider_invalid(operation: &'static str) -> ProviderStoreError {
    ProviderStoreError::new(ProviderStoreErrorKind::InvalidData, operation)
}
