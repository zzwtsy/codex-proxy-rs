//! SQLite 共享账号与模型 cooldown、容量失败计数及峰值。

use std::{collections::BTreeMap, time::Duration};

use chrono::{DateTime, Utc};
use futures::future::BoxFuture;
use gateway_admin::model::accounts::{AccountFreeze, AccountRuntimeSnapshot};
use gateway_core::{
    account::{AccountCooldownKind, CredentialRevision, ProviderAccountId},
    provider_ports::{
        ProviderCooldown, ProviderCooldownKind, ProviderCooldownPort, ProviderCooldownScope,
        ProviderScopedCooldown, ProviderStoreError, ProviderStoreErrorKind,
    },
};
use sqlx::{QueryBuilder, Row, Sqlite, SqlitePool, Transaction};
use uuid::Uuid;

use super::value::{datetime_from_micros, datetime_to_micros};
use crate::AccountRuntimeStateRepository;

#[derive(Clone)]
pub struct SqliteProviderCooldownRepository {
    pool: SqlitePool,
}

impl SqliteProviderCooldownRepository {
    #[must_use]
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    async fn read_account(
        &self,
        account_id: &ProviderAccountId,
    ) -> Result<Option<ProviderCooldown>, ProviderStoreError> {
        let row = sqlx::query(
            "SELECT credential_revision, until_us, kind FROM provider_cooldowns              WHERE account_id = ?",
        )
        .bind(account_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| provider_unavailable("read provider cooldown"))?;
        let Some(row) = row else {
            return Ok(None);
        };
        let revision: i64 = sqlx::Row::try_get(&row, "credential_revision")
            .map_err(|_| provider_unavailable("decode provider cooldown"))?;
        let until: i64 = sqlx::Row::try_get(&row, "until_us")
            .map_err(|_| provider_unavailable("decode provider cooldown"))?;
        let kind_value: String = sqlx::Row::try_get(&row, "kind")
            .map_err(|_| provider_unavailable("decode provider cooldown"))?;
        let kind = AccountCooldownKind::parse(&kind_value)
            .ok_or_else(|| provider_invalid("decode provider cooldown kind"))?;
        if kind != AccountCooldownKind::CapacityFreezeProbe
            && until <= datetime_to_micros(Utc::now())
        {
            sqlx::query(
                "DELETE FROM provider_cooldowns WHERE account_id = ? AND credential_revision = ? AND until_us <= ? AND kind = ?",
            )
            .bind(account_id.as_str())
            .bind(revision)
            .bind(datetime_to_micros(Utc::now()))
            .bind(&kind_value)
            .execute(&self.pool)
            .await
            .map_err(|_| provider_unavailable("clean expired provider cooldown"))?;
            return Ok(None);
        }
        let revision = CredentialRevision::new(
            u64::try_from(revision).map_err(|_| provider_invalid("decode cooldown revision"))?,
        )
        .map_err(|_| provider_invalid("decode cooldown revision"))?;
        let until = datetime_from_micros(until)
            .map_err(|_| provider_invalid("decode provider cooldown timestamp"))?;
        Ok(Some(ProviderCooldown::new_with_kind(
            account_id.clone(),
            revision,
            until.into(),
            kind,
        )))
    }

    async fn read_scoped_value(
        &self,
        account_id: &ProviderAccountId,
        scope: &ProviderCooldownScope,
    ) -> Result<Option<ProviderScopedCooldown>, ProviderStoreError> {
        let row = sqlx::query(
            "SELECT credential_revision, until_us FROM provider_scoped_cooldowns              WHERE account_id = ? AND scope_kind = ? AND scope_value = ?",
        )
        .bind(account_id.as_str())
        .bind(scope.kind())
        .bind(scope.value())
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| provider_unavailable("read scoped provider cooldown"))?;
        let Some(row) = row else {
            return Ok(None);
        };
        let revision: i64 = sqlx::Row::try_get(&row, "credential_revision")
            .map_err(|_| provider_unavailable("decode scoped provider cooldown"))?;
        let until: i64 = sqlx::Row::try_get(&row, "until_us")
            .map_err(|_| provider_unavailable("decode scoped provider cooldown"))?;
        let now = datetime_to_micros(Utc::now());
        if until <= now {
            sqlx::query(
                "DELETE FROM provider_scoped_cooldowns                  WHERE account_id = ? AND scope_kind = ? AND scope_value = ?                    AND credential_revision = ? AND until_us <= ?",
            )
            .bind(account_id.as_str())
            .bind(scope.kind())
            .bind(scope.value())
            .bind(revision)
            .bind(now)
            .execute(&self.pool)
            .await
            .map_err(|_| provider_unavailable("clean expired scoped provider cooldown"))?;
            return Ok(None);
        }
        let revision = CredentialRevision::new(
            u64::try_from(revision).map_err(|_| provider_invalid("decode cooldown revision"))?,
        )
        .map_err(|_| provider_invalid("decode cooldown revision"))?;
        let until = datetime_from_micros(until)
            .map_err(|_| provider_invalid("decode scoped provider cooldown timestamp"))?;
        Ok(Some(ProviderScopedCooldown::new(
            account_id.clone(),
            revision,
            scope.clone(),
            until.into(),
        )))
    }

    async fn acquire_write_lock(
        transaction: &mut Transaction<'_, sqlx::Sqlite>,
    ) -> Result<(), ProviderStoreError> {
        super::acquire_write_lock(transaction)
            .await
            .map_err(|_| provider_unavailable("acquire SQLite write lock"))
    }

    async fn put_account(
        &self,
        account_id: &ProviderAccountId,
        revision: CredentialRevision,
        until: DateTime<Utc>,
        kind: ProviderCooldownKind,
    ) -> Result<bool, ProviderStoreError> {
        let revision = i64::try_from(revision.get())
            .map_err(|_| provider_invalid("encode provider cooldown revision"))?;
        let incoming_until = datetime_to_micros(until);
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|_| provider_unavailable("write provider cooldown"))?;
        Self::acquire_write_lock(&mut transaction).await?;
        let now = datetime_to_micros(Utc::now());
        let current = sqlx::query(
            "SELECT credential_revision, until_us, kind FROM provider_cooldowns WHERE account_id = ?",
        )
        .bind(account_id.as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| provider_unavailable("read provider cooldown"))?;
        let mut until = incoming_until;
        if let Some(row) = current {
            let current_revision: i64 = sqlx::Row::try_get(&row, "credential_revision")
                .map_err(|_| provider_unavailable("decode provider cooldown"))?;
            let current_until: i64 = sqlx::Row::try_get(&row, "until_us")
                .map_err(|_| provider_unavailable("decode provider cooldown"))?;
            let current_kind: String = sqlx::Row::try_get(&row, "kind")
                .map_err(|_| provider_unavailable("decode provider cooldown"))?;
            if current_revision > revision {
                transaction
                    .commit()
                    .await
                    .map_err(|_| provider_unavailable("write provider cooldown"))?;
                return Ok(false);
            }
            if current_revision == revision {
                if current_kind != AccountCooldownKind::RateLimit.as_str()
                    && kind == AccountCooldownKind::RateLimit
                {
                    transaction
                        .commit()
                        .await
                        .map_err(|_| provider_unavailable("write provider cooldown"))?;
                    return Ok(false);
                }
                if current_kind == kind.as_str() && current_until >= incoming_until {
                    transaction
                        .commit()
                        .await
                        .map_err(|_| provider_unavailable("write provider cooldown"))?;
                    return Ok(false);
                }
                until = current_until.max(incoming_until);
            }
        }
        if until <= now && kind != AccountCooldownKind::CapacityFreezeProbe {
            transaction
                .commit()
                .await
                .map_err(|_| provider_unavailable("write provider cooldown"))?;
            return Ok(false);
        }
        let generation = Uuid::now_v7().to_string();
        sqlx::query(
            "INSERT INTO provider_cooldowns (account_id, credential_revision, until_us, kind, generation)              VALUES (?, ?, ?, ?, ?) ON CONFLICT (account_id) DO UPDATE SET                credential_revision = excluded.credential_revision, until_us = excluded.until_us,                kind = excluded.kind, generation = excluded.generation",
        )
        .bind(account_id.as_str())
        .bind(revision)
        .bind(until)
        .bind(kind.as_str())
        .bind(generation)
        .execute(&mut *transaction)
        .await
        .map_err(|_| provider_unavailable("write provider cooldown"))?;
        transaction
            .commit()
            .await
            .map_err(|_| provider_unavailable("write provider cooldown"))?;
        Ok(true)
    }
}

impl ProviderCooldownPort for SqliteProviderCooldownRepository {
    fn put_if_later(
        &self,
        cooldown: ProviderCooldown,
    ) -> BoxFuture<'_, Result<bool, ProviderStoreError>> {
        Box::pin(async move {
            self.put_account(
                cooldown.account_id(),
                cooldown.credential_revision(),
                cooldown.until().into(),
                cooldown.kind(),
            )
            .await
        })
    }

    fn read<'a>(
        &'a self,
        account_id: &'a ProviderAccountId,
    ) -> BoxFuture<'a, Result<Option<ProviderCooldown>, ProviderStoreError>> {
        Box::pin(async move { self.read_account(account_id).await })
    }

    fn clear<'a>(
        &'a self,
        account_id: &'a ProviderAccountId,
        through_revision: CredentialRevision,
    ) -> BoxFuture<'a, Result<bool, ProviderStoreError>> {
        Box::pin(async move {
            let revision = i64::try_from(through_revision.get())
                .map_err(|_| provider_invalid("encode provider cooldown revision"))?;
            let result = sqlx::query(
                "DELETE FROM provider_cooldowns WHERE account_id = ? AND credential_revision <= ?",
            )
            .bind(account_id.as_str())
            .bind(revision)
            .execute(&self.pool)
            .await
            .map_err(|_| provider_unavailable("clear provider cooldown"))?;
            Ok(result.rows_affected() > 0)
        })
    }

    fn put_scoped_if_later(
        &self,
        cooldown: ProviderScopedCooldown,
    ) -> BoxFuture<'_, Result<bool, ProviderStoreError>> {
        Box::pin(async move {
            let revision = i64::try_from(cooldown.credential_revision().get())
                .map_err(|_| provider_invalid("encode scoped cooldown revision"))?;
            let until = datetime_to_micros(cooldown.until().into());
            let now = datetime_to_micros(Utc::now());
            if until <= now {
                return Ok(false);
            }
            let mut transaction = self
                .pool
                .begin()
                .await
                .map_err(|_| provider_unavailable("write scoped provider cooldown"))?;
            Self::acquire_write_lock(&mut transaction).await?;
            let current: Option<(i64, i64)> = sqlx::query_as(
                "SELECT credential_revision, until_us FROM provider_scoped_cooldowns                  WHERE account_id = ? AND scope_kind = ? AND scope_value = ?",
            )
            .bind(cooldown.account_id().as_str())
            .bind(cooldown.scope().kind())
            .bind(cooldown.scope().value())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|_| provider_unavailable("read scoped provider cooldown"))?;
            if current.is_some_and(|(current_revision, current_until)| {
                current_revision > revision
                    || (current_revision == revision && current_until >= until)
            }) {
                transaction
                    .commit()
                    .await
                    .map_err(|_| provider_unavailable("write scoped provider cooldown"))?;
                return Ok(false);
            }
            sqlx::query(
                "INSERT INTO provider_scoped_cooldowns                  (account_id, scope_kind, scope_value, credential_revision, until_us) VALUES (?, ?, ?, ?, ?)                  ON CONFLICT (account_id, scope_kind, scope_value) DO UPDATE SET                    credential_revision = excluded.credential_revision, until_us = excluded.until_us",
            )
            .bind(cooldown.account_id().as_str())
            .bind(cooldown.scope().kind())
            .bind(cooldown.scope().value())
            .bind(revision)
            .bind(until)
            .execute(&mut *transaction)
            .await
            .map_err(|_| provider_unavailable("write scoped provider cooldown"))?;
            transaction
                .commit()
                .await
                .map_err(|_| provider_unavailable("write scoped provider cooldown"))?;
            Ok(true)
        })
    }

    fn read_scoped<'a>(
        &'a self,
        account_id: &'a ProviderAccountId,
        scope: &'a ProviderCooldownScope,
    ) -> BoxFuture<'a, Result<Option<ProviderScopedCooldown>, ProviderStoreError>> {
        Box::pin(async move { self.read_scoped_value(account_id, scope).await })
    }

    fn clear_scoped<'a>(
        &'a self,
        account_id: &'a ProviderAccountId,
        scope: &'a ProviderCooldownScope,
        through_revision: CredentialRevision,
    ) -> BoxFuture<'a, Result<bool, ProviderStoreError>> {
        Box::pin(async move {
            let revision = i64::try_from(through_revision.get())
                .map_err(|_| provider_invalid("encode scoped cooldown revision"))?;
            let result = sqlx::query(
                "DELETE FROM provider_scoped_cooldowns                  WHERE account_id = ? AND scope_kind = ? AND scope_value = ?                    AND credential_revision <= ?",
            )
            .bind(account_id.as_str())
            .bind(scope.kind())
            .bind(scope.value())
            .bind(revision)
            .execute(&self.pool)
            .await
            .map_err(|_| provider_unavailable("clear scoped provider cooldown"))?;
            Ok(result.rows_affected() > 0)
        })
    }

    fn clear_all<'a>(
        &'a self,
        account_id: &'a ProviderAccountId,
    ) -> BoxFuture<'a, Result<bool, ProviderStoreError>> {
        Box::pin(async move {
            let mut transaction = self
                .pool
                .begin()
                .await
                .map_err(|_| provider_unavailable("clear provider runtime state"))?;
            Self::acquire_write_lock(&mut transaction).await?;
            let mut removed = 0_u64;
            for query in [
                "DELETE FROM provider_cooldowns WHERE account_id = ?",
                "DELETE FROM provider_scoped_cooldowns WHERE account_id = ?",
                "DELETE FROM provider_capacity_failures WHERE account_id = ?",
            ] {
                removed = removed.saturating_add(
                    sqlx::query(query)
                        .bind(account_id.as_str())
                        .execute(&mut *transaction)
                        .await
                        .map_err(|_| provider_unavailable("clear provider runtime state"))?
                        .rows_affected(),
                );
            }
            transaction
                .commit()
                .await
                .map_err(|_| provider_unavailable("clear provider runtime state"))?;
            Ok(removed > 0)
        })
    }

    fn record_capacity_failure<'a>(
        &'a self,
        account_id: &'a ProviderAccountId,
        window: Duration,
        in_flight: u32,
    ) -> BoxFuture<'a, Result<u32, ProviderStoreError>> {
        Box::pin(async move {
            let window_micros = i64::try_from(window.as_micros())
                .ok()
                .filter(|value| *value > 0)
                .ok_or_else(|| provider_invalid("encode capacity failure window"))?;
            let now = datetime_to_micros(Utc::now());
            let expires = now
                .checked_add(window_micros)
                .ok_or_else(|| provider_invalid("encode capacity failure expiry"))?;
            let mut transaction = self
                .pool
                .begin()
                .await
                .map_err(|_| provider_unavailable("record capacity failure"))?;
            Self::acquire_write_lock(&mut transaction).await?;
            let current: Option<(i64, i64, Option<i64>, Option<i64>)> = sqlx::query_as(
                "SELECT failure_count, failures_expires_at_us, peak_in_flight, peak_expires_at_us                  FROM provider_capacity_failures WHERE account_id = ?",
            )
            .bind(account_id.as_str())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|_| provider_unavailable("read capacity failure state"))?;
            let (count, previous_peak, previous_peak_expiry) = match current {
                Some((count, failure_expiry, peak, peak_expiry)) if failure_expiry > now => (
                    count
                        .checked_add(1)
                        .ok_or_else(|| provider_invalid("capacity failure count overflow"))?,
                    if peak_expiry.is_some_and(|expiry| expiry > now) {
                        peak.unwrap_or(0)
                    } else {
                        0
                    },
                    peak_expiry,
                ),
                _ => (1, 0, None),
            };
            let (peak, peak_expiry) = if in_flight > 0 {
                (Some(previous_peak.max(i64::from(in_flight))), Some(expires))
            } else if previous_peak > 0 {
                (Some(previous_peak), previous_peak_expiry)
            } else {
                (None, None)
            };
            sqlx::query(
                "INSERT INTO provider_capacity_failures                  (account_id, failure_count, failures_expires_at_us, peak_in_flight, peak_expires_at_us)                  VALUES (?, ?, ?, ?, ?) ON CONFLICT (account_id) DO UPDATE SET                    failure_count = excluded.failure_count, failures_expires_at_us = excluded.failures_expires_at_us,                    peak_in_flight = excluded.peak_in_flight, peak_expires_at_us = excluded.peak_expires_at_us",
            )
            .bind(account_id.as_str())
            .bind(count)
            .bind(expires)
            .bind(peak)
            .bind(peak_expiry)
            .execute(&mut *transaction)
            .await
            .map_err(|_| provider_unavailable("record capacity failure"))?;
            transaction
                .commit()
                .await
                .map_err(|_| provider_unavailable("record capacity failure"))?;
            u32::try_from(count).map_err(|_| provider_invalid("capacity failure count exceeds u32"))
        })
    }

    fn clear_after_success<'a>(
        &'a self,
        account_id: &'a ProviderAccountId,
        through_revision: CredentialRevision,
    ) -> BoxFuture<'a, Result<(), ProviderStoreError>> {
        Box::pin(async move {
            let revision = i64::try_from(through_revision.get())
                .map_err(|_| provider_invalid("encode cooldown revision"))?;
            let mut transaction = self
                .pool
                .begin()
                .await
                .map_err(|_| provider_unavailable("clear cooldown after success"))?;
            Self::acquire_write_lock(&mut transaction).await?;
            sqlx::query(
                "DELETE FROM provider_cooldowns WHERE account_id = ?                  AND credential_revision <= ? AND kind = 'rate_limit'",
            )
            .bind(account_id.as_str())
            .bind(revision)
            .execute(&mut *transaction)
            .await
            .map_err(|_| provider_unavailable("clear cooldown after success"))?;
            sqlx::query("DELETE FROM provider_capacity_failures WHERE account_id = ?")
                .bind(account_id.as_str())
                .execute(&mut *transaction)
                .await
                .map_err(|_| provider_unavailable("clear capacity failure evidence"))?;
            transaction
                .commit()
                .await
                .map_err(|_| provider_unavailable("clear cooldown after success"))
        })
    }

    fn capacity_peak_in_flight<'a>(
        &'a self,
        account_id: &'a ProviderAccountId,
    ) -> BoxFuture<'a, Result<Option<u32>, ProviderStoreError>> {
        Box::pin(async move {
            let now = datetime_to_micros(Utc::now());
            let peak: Option<i64> = sqlx::query_scalar(
                "SELECT peak_in_flight FROM provider_capacity_failures                  WHERE account_id = ? AND peak_expires_at_us > ?",
            )
            .bind(account_id.as_str())
            .bind(now)
            .fetch_optional(&self.pool)
            .await
            .map_err(|_| provider_unavailable("read capacity peak"))?
            .flatten();
            peak.map(|value| {
                u32::try_from(value).map_err(|_| provider_invalid("capacity peak exceeds u32"))
            })
            .transpose()
        })
    }
}

#[async_trait::async_trait]
impl AccountRuntimeStateRepository for SqliteProviderCooldownRepository {
    async fn active_rate_limits(&self) -> crate::StoreResult<AccountRuntimeSnapshot> {
        let now = datetime_to_micros(Utc::now());
        sqlx::query(
            "DELETE FROM provider_cooldowns WHERE account_id IN (                SELECT account_id FROM provider_cooldowns                WHERE kind <> 'capacity_freeze_probe' AND until_us <= ? LIMIT 256            ) AND kind <> 'capacity_freeze_probe' AND until_us <= ?",
        )
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(|_| sqlite_store_unavailable("clean expired provider cooldowns"))?;
        let rows = sqlx::query(
            "SELECT account_id, until_us, kind FROM provider_cooldowns             WHERE kind = 'capacity_freeze_probe' OR until_us > ?",
        )
        .bind(now)
        .fetch_all(&self.pool)
        .await
        .map_err(|_| sqlite_store_unavailable("list active provider cooldowns"))?;
        let mut cooldown = BTreeMap::new();
        for row in rows {
            let account_id: String = row
                .try_get("account_id")
                .map_err(|_| sqlite_store_invalid("decode provider cooldown account"))?;
            let until: i64 = row
                .try_get("until_us")
                .map_err(|_| sqlite_store_invalid("decode provider cooldown expiry"))?;
            let kind_value: String = row
                .try_get("kind")
                .map_err(|_| sqlite_store_invalid("decode provider cooldown kind"))?;
            let kind = AccountCooldownKind::parse(&kind_value)
                .ok_or_else(|| sqlite_store_invalid("decode provider cooldown kind"))?;
            let until = datetime_from_micros(until)
                .map_err(|_| sqlite_store_invalid("decode provider cooldown expiry"))?;
            cooldown.insert(
                account_id,
                gateway_core::account::AccountCooldown {
                    until: until.into(),
                    kind,
                },
            );
        }
        Ok(AccountRuntimeSnapshot {
            cooldown,
            in_flight: None,
        })
    }

    async fn account_cooldowns(
        &self,
        account_ids: &[String],
    ) -> crate::StoreResult<BTreeMap<String, gateway_core::account::AccountCooldown>> {
        let mut cooldowns = BTreeMap::new();
        for account_id in account_ids {
            crate::require_nonempty("account runtime", "account_id", account_id)?;
            let account_id_value = ProviderAccountId::new(account_id.clone())
                .map_err(|_| sqlite_store_invalid("decode provider account ID"))?;
            if let Some(cooldown) = self
                .read_account(&account_id_value)
                .await
                .map_err(provider_store_error)?
            {
                cooldowns.insert(
                    account_id.clone(),
                    gateway_core::account::AccountCooldown {
                        until: cooldown.until(),
                        kind: cooldown.kind(),
                    },
                );
            }
        }
        Ok(cooldowns)
    }

    async fn active_freezes(&self) -> crate::StoreResult<BTreeMap<String, AccountFreeze>> {
        let now = datetime_to_micros(Utc::now());
        let rows = sqlx::query(
            "SELECT account_id, credential_revision, until_us, kind, generation             FROM provider_cooldowns             WHERE kind IN ('capacity_freeze', 'capacity_freeze_probe')               AND (kind = 'capacity_freeze_probe' OR until_us > ?)",
        )
        .bind(now)
        .fetch_all(&self.pool)
        .await
        .map_err(|_| sqlite_store_unavailable("list active provider freezes"))?;
        let mut freezes = BTreeMap::new();
        for row in rows {
            let account_id: String = row
                .try_get("account_id")
                .map_err(|_| sqlite_store_invalid("decode provider freeze account"))?;
            let revision: i64 = row
                .try_get("credential_revision")
                .map_err(|_| sqlite_store_invalid("decode provider freeze revision"))?;
            let until: i64 = row
                .try_get("until_us")
                .map_err(|_| sqlite_store_invalid("decode provider freeze expiry"))?;
            let kind_value: String = row
                .try_get("kind")
                .map_err(|_| sqlite_store_invalid("decode provider freeze kind"))?;
            let kind = AccountCooldownKind::parse(&kind_value)
                .ok_or_else(|| sqlite_store_invalid("decode provider freeze kind"))?;
            if !kind.is_capacity_freeze() {
                return Err(sqlite_store_invalid("decode provider freeze kind"));
            }
            let generation: String = row
                .try_get("generation")
                .map_err(|_| sqlite_store_invalid("decode provider freeze generation"))?;
            let revision = u64::try_from(revision)
                .ok()
                .and_then(|revision| gateway_admin::model::Revision::new(revision).ok())
                .ok_or_else(|| sqlite_store_invalid("decode provider freeze revision"))?;
            let until = datetime_from_micros(until)
                .map_err(|_| sqlite_store_invalid("decode provider freeze expiry"))?;
            freezes.insert(
                account_id,
                AccountFreeze {
                    credential_revision: revision,
                    until,
                    generation,
                    requires_probe: kind.requires_probe(),
                },
            );
        }
        Ok(freezes)
    }

    async fn capacity_peaks(
        &self,
        account_ids: &[String],
    ) -> crate::StoreResult<BTreeMap<String, u32>> {
        for account_id in account_ids {
            crate::require_nonempty("account runtime", "account_id", account_id)?;
        }
        if account_ids.is_empty() {
            return Ok(BTreeMap::new());
        }
        let now = datetime_to_micros(Utc::now());
        let mut query = QueryBuilder::<Sqlite>::new(
            "SELECT account_id, peak_in_flight FROM provider_capacity_failures             WHERE peak_in_flight IS NOT NULL AND peak_expires_at_us > ",
        );
        query.push_bind(now).push(" AND account_id IN (");
        {
            let mut separated = query.separated(", ");
            for account_id in account_ids {
                separated.push_bind(account_id);
            }
        }
        query.push(")");
        let rows = query
            .build()
            .fetch_all(&self.pool)
            .await
            .map_err(|_| sqlite_store_unavailable("read provider capacity peaks"))?;
        let mut peaks = BTreeMap::new();
        for row in rows {
            let account_id: String = row
                .try_get("account_id")
                .map_err(|_| sqlite_store_invalid("decode provider capacity peak account"))?;
            let peak: i64 = row
                .try_get("peak_in_flight")
                .map_err(|_| sqlite_store_invalid("decode provider capacity peak"))?;
            peaks.insert(
                account_id,
                u32::try_from(peak)
                    .map_err(|_| sqlite_store_invalid("decode provider capacity peak"))?,
            );
        }
        Ok(peaks)
    }

    async fn finish_freeze(
        &self,
        account_id: &str,
        expected: &AccountFreeze,
        postpone_until: Option<DateTime<Utc>>,
    ) -> crate::StoreResult<bool> {
        crate::require_nonempty("account runtime", "account_id", account_id)?;
        let revision = i64::try_from(expected.credential_revision.get())
            .map_err(|_| sqlite_store_invalid("encode provider freeze revision"))?;
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|_| sqlite_store_unavailable("finish provider freeze"))?;
        Self::acquire_write_lock(&mut transaction)
            .await
            .map_err(provider_store_error)?;
        let changed = if let Some(postpone_until) = postpone_until {
            let postpone_until = datetime_to_micros(postpone_until);
            sqlx::query(
                "UPDATE provider_cooldowns                 SET until_us = max(until_us, ?), generation = ?                 WHERE account_id = ? AND credential_revision = ? AND generation = ?                   AND kind IN ('capacity_freeze', 'capacity_freeze_probe')",
            )
            .bind(postpone_until)
            .bind(Uuid::now_v7().to_string())
            .bind(account_id)
            .bind(revision)
            .bind(&expected.generation)
            .execute(&mut *transaction)
            .await
            .map_err(|_| sqlite_store_unavailable("postpone provider freeze"))?
            .rows_affected()
                == 1
        } else {
            let deleted = sqlx::query(
                "DELETE FROM provider_cooldowns                 WHERE account_id = ? AND credential_revision = ? AND generation = ?                   AND kind IN ('capacity_freeze', 'capacity_freeze_probe')",
            )
            .bind(account_id)
            .bind(revision)
            .bind(&expected.generation)
            .execute(&mut *transaction)
            .await
            .map_err(|_| sqlite_store_unavailable("finish provider freeze"))?
            .rows_affected()
                == 1;
            if deleted {
                sqlx::query("DELETE FROM provider_capacity_failures WHERE account_id = ?")
                    .bind(account_id)
                    .execute(&mut *transaction)
                    .await
                    .map_err(|_| sqlite_store_unavailable("clear provider freeze evidence"))?;
            }
            deleted
        };
        transaction
            .commit()
            .await
            .map_err(|_| sqlite_store_unavailable("finish provider freeze"))?;
        Ok(changed)
    }
}

fn provider_store_error(error: ProviderStoreError) -> crate::StoreError {
    match error.kind() {
        ProviderStoreErrorKind::InvalidData => crate::StoreError::InvalidData {
            entity: "SQLite provider runtime state",
            message: error.to_string(),
            source: None,
        },
        ProviderStoreErrorKind::Conflict => crate::StoreError::Conflict {
            entity: "provider runtime state",
            id: "account".to_owned(),
            kind: crate::ConflictKind::InvalidTransition,
            source: None,
        },
        ProviderStoreErrorKind::Unavailable => sqlite_store_unavailable("provider runtime state"),
    }
}

fn sqlite_store_unavailable(operation: &'static str) -> crate::StoreError {
    crate::StoreError::Unavailable {
        backend: crate::StoreBackend::Sqlite,
        message: operation.to_owned(),
        source: None,
    }
}

fn sqlite_store_invalid(message: &'static str) -> crate::StoreError {
    crate::StoreError::InvalidData {
        entity: "SQLite provider runtime state",
        message: message.to_owned(),
        source: None,
    }
}

fn provider_unavailable(operation: &'static str) -> ProviderStoreError {
    ProviderStoreError::new(ProviderStoreErrorKind::Unavailable, operation)
}

fn provider_invalid(operation: &'static str) -> ProviderStoreError {
    ProviderStoreError::new(ProviderStoreErrorKind::InvalidData, operation)
}
