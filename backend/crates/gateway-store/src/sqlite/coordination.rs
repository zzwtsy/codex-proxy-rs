//! SQLite 中跨进程共享的 Provider 租约与 fencing 状态。

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use chrono::Utc;
use sqlx::{QueryBuilder, Row, Sqlite, SqlitePool};
use uuid::Uuid;

use super::value::{datetime_from_micros, datetime_to_micros, duration_micros};

use crate::{
    CredentialBoundedLeaseAcquisition, CredentialBoundedLeaseRequest, CredentialLeaseGrant,
    CredentialLeaseRepository, CredentialLeaseRequest, CredentialLeaseScope,
    CredentialRuntimeSignal, Revision, StoreBackend, StoreError, StoreResult,
    coordination::resource_fingerprint,
};

#[derive(Clone)]
pub struct SqliteCredentialLeaseRepository {
    pool: SqlitePool,
}

impl SqliteCredentialLeaseRepository {
    #[must_use]
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub(crate) async fn runtime_signals(
        &self,
        resource_ids: &[String],
        scope: CredentialLeaseScope,
    ) -> StoreResult<Vec<CredentialRuntimeSignal>> {
        for resource_id in resource_ids {
            crate::require_nonempty("credential runtime signal", "resource_id", resource_id)?;
        }
        if resource_ids.is_empty() {
            return Ok(Vec::new());
        }
        let fingerprints = resource_ids
            .iter()
            .map(|resource_id| resource_fingerprint("credential lease", resource_id))
            .collect::<StoreResult<Vec<_>>>()?;
        let now = datetime_to_micros(Utc::now());
        let mut cleanup =
            QueryBuilder::<Sqlite>::new("DELETE FROM credential_leases WHERE scope = ");
        cleanup.push_bind(scope.as_str());
        cleanup.push(" AND expires_at_us <= ");
        cleanup.push_bind(now);
        cleanup.push(" AND resource_fingerprint IN (");
        {
            let mut separated = cleanup.separated(", ");
            for fingerprint in &fingerprints {
                separated.push_bind(fingerprint);
            }
        }
        cleanup.push(")");
        cleanup
            .build()
            .execute(&self.pool)
            .await
            .map_err(sqlite_unavailable)?;

        // 请求间隔来自共享账号记录，容量计数只读取当前调度池
        let mut query = QueryBuilder::<Sqlite>::new(
            "SELECT c.resource_fingerprint, count(l.lease_id) AS in_flight,
                    c.last_started_at_us FROM credential_lease_counters c
             LEFT JOIN credential_leases l ON l.scope = ",
        );
        query.push_bind(scope.as_str());
        query.push(" AND l.resource_fingerprint = c.resource_fingerprint AND l.expires_at_us > ");
        query.push_bind(now);
        query.push(" WHERE c.scope = 'account'");
        query.push(" AND c.resource_fingerprint IN (");
        {
            let mut separated = query.separated(", ");
            for fingerprint in &fingerprints {
                separated.push_bind(fingerprint);
            }
        }
        query.push(") GROUP BY c.resource_fingerprint, c.last_started_at_us");
        let rows = query
            .build()
            .fetch_all(&self.pool)
            .await
            .map_err(sqlite_unavailable)?;
        let mut signals = BTreeMap::new();
        for row in rows {
            let fingerprint: String = row
                .try_get("resource_fingerprint")
                .map_err(sqlite_unavailable)?;
            let in_flight: i64 = row.try_get("in_flight").map_err(sqlite_unavailable)?;
            let last_started: Option<i64> = row
                .try_get("last_started_at_us")
                .map_err(sqlite_unavailable)?;
            signals.insert(
                fingerprint,
                (
                    u32::try_from(in_flight)
                        .map_err(|_| invalid_lease("in-flight count exceeds u32"))?,
                    last_started.map(datetime_from_micros).transpose()?,
                ),
            );
        }
        resource_ids
            .iter()
            .zip(fingerprints)
            .map(|(resource_id, fingerprint)| {
                let (in_flight, last_started_at) =
                    signals.get(&fingerprint).cloned().unwrap_or((0, None));
                Ok(CredentialRuntimeSignal {
                    resource_id: resource_id.clone(),
                    in_flight,
                    last_started_at,
                })
            })
            .collect()
    }

    async fn acquire_with_limits(
        &self,
        request: &CredentialLeaseRequest,
        max_concurrent: u32,
        request_interval: Duration,
    ) -> StoreResult<LeaseAttempt> {
        request.validate()?;
        if max_concurrent == 0
            && !matches!(
                request.scope,
                CredentialLeaseScope::ProviderAccount
                    | CredentialLeaseScope::ProviderAccountReserved
            )
        {
            return Err(invalid_lease("max_concurrent must be positive"));
        }
        let scope = request.scope.as_str();
        let interval_scope = if request.scope == CredentialLeaseScope::ProviderAccountReserved {
            CredentialLeaseScope::ProviderAccount.as_str()
        } else {
            scope
        };
        let resource = resource_fingerprint("credential lease", &request.resource_id)?;
        let owner = resource_fingerprint("credential lease owner", &request.owner_id)?;
        let ttl = duration_micros(request.ttl)?;
        let interval = duration_micros(request_interval)?;
        let lease_id = Uuid::now_v7().to_string();
        let mut transaction = self.pool.begin().await.map_err(sqlite_unavailable)?;
        super::acquire_write_lock(&mut transaction).await?;
        let now = datetime_to_micros(Utc::now());
        let expires_at = now
            .checked_add(ttl)
            .ok_or_else(|| invalid_lease("lease expiry is outside SQLite timestamp range"))?;

        sqlx::query(
            "INSERT INTO credential_lease_counters (scope, resource_fingerprint) VALUES (?, ?)              ON CONFLICT (scope, resource_fingerprint) DO NOTHING",
        )
        .bind(scope)
        .bind(&resource)
        .execute(&mut *transaction)
        .await
        .map_err(sqlite_unavailable)?;
        if interval_scope != scope {
            sqlx::query("INSERT INTO credential_lease_counters (scope, resource_fingerprint) VALUES (?, ?) ON CONFLICT (scope, resource_fingerprint) DO NOTHING")
                .bind(interval_scope).bind(&resource).execute(&mut *transaction).await.map_err(sqlite_unavailable)?;
        }
        sqlx::query(
            "DELETE FROM credential_leases              WHERE scope = ? AND resource_fingerprint = ? AND expires_at_us <= ?",
        )
        .bind(scope)
        .bind(&resource)
        .bind(now)
        .execute(&mut *transaction)
        .await
        .map_err(sqlite_unavailable)?;

        let (in_flight, earliest_expiry): (i64, Option<i64>) = sqlx::query_as(
            "SELECT count(*), min(expires_at_us) FROM credential_leases              WHERE scope = ? AND resource_fingerprint = ?",
        )
        .bind(scope)
        .bind(&resource)
        .fetch_one(&mut *transaction)
        .await
        .map_err(sqlite_unavailable)?;
        let last_started: Option<i64> = sqlx::query_scalar(
            "SELECT last_started_at_us FROM credential_lease_counters              WHERE scope = ? AND resource_fingerprint = ?",
        )
        .bind(interval_scope)
        .bind(&resource)
        .fetch_one(&mut *transaction)
        .await
        .map_err(sqlite_unavailable)?;

        let capacity_wait = if max_concurrent > 0 && in_flight >= i64::from(max_concurrent) {
            earliest_expiry.unwrap_or(now).saturating_sub(now).max(1)
        } else {
            0
        };
        let interval_wait = last_started
            .map(|last| interval.saturating_sub(now.saturating_sub(last)))
            .unwrap_or(0);
        let retry_after = capacity_wait.max(interval_wait);
        if retry_after > 0 {
            transaction.commit().await.map_err(sqlite_unavailable)?;
            return Ok(LeaseAttempt {
                grant: None,
                retry_after: Some(Duration::from_micros(retry_after as u64)),
            });
        }

        let next_fence: i64 = sqlx::query_scalar(
            "UPDATE credential_lease_counters              SET fencing_token = fencing_token + 1, last_started_at_us = ?              WHERE scope = ? AND resource_fingerprint = ?                AND fencing_token < 9223372036854775807              RETURNING fencing_token",
        )
        .bind(now)
        .bind(scope)
        .bind(&resource)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(sqlite_unavailable)?
        .ok_or_else(|| invalid_lease("fencing token reached the SQLite integer limit"))?;
        if interval_scope != scope {
            sqlx::query("UPDATE credential_lease_counters SET last_started_at_us = ? WHERE scope = ? AND resource_fingerprint = ?")
                .bind(now).bind(interval_scope).bind(&resource).execute(&mut *transaction).await.map_err(sqlite_unavailable)?;
        }
        sqlx::query(
            "INSERT INTO credential_leases              (scope, resource_fingerprint, lease_id, owner_fingerprint, fencing_token, expires_at_us)              VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(scope)
        .bind(&resource)
        .bind(&lease_id)
        .bind(owner)
        .bind(next_fence)
        .bind(expires_at)
        .execute(&mut *transaction)
        .await
        .map_err(sqlite_unavailable)?;
        transaction.commit().await.map_err(sqlite_unavailable)?;

        let fencing_token = u64::try_from(next_fence)
            .map_err(|_| invalid_lease("SQLite returned an invalid fencing token"))?;
        Ok(LeaseAttempt {
            grant: Some(CredentialLeaseGrant {
                lease_id,
                fencing_token: Revision::new(fencing_token)?,
                expires_at: datetime_from_micros(expires_at)?,
            }),
            retry_after: None,
        })
    }
}

#[async_trait]
impl CredentialLeaseRepository for SqliteCredentialLeaseRepository {
    async fn acquire_credential_lease(
        &self,
        request: &CredentialLeaseRequest,
    ) -> StoreResult<Option<CredentialLeaseGrant>> {
        self.acquire_with_limits(request, 1, Duration::ZERO)
            .await
            .map(|attempt| attempt.grant)
    }

    async fn renew_credential_lease(
        &self,
        request: &CredentialLeaseRequest,
        grant: &CredentialLeaseGrant,
    ) -> StoreResult<Option<CredentialLeaseGrant>> {
        request.validate()?;
        let scope = request.scope.as_str();
        let resource = resource_fingerprint("credential lease", &request.resource_id)?;
        let owner = resource_fingerprint("credential lease owner", &request.owner_id)?;
        let ttl = duration_micros(request.ttl)?;
        let fencing_token = i64::try_from(grant.fencing_token.get())
            .map_err(|_| invalid_lease("fencing token is outside SQLite integer range"))?;
        let mut transaction = self.pool.begin().await.map_err(sqlite_unavailable)?;
        super::acquire_write_lock(&mut transaction).await?;
        let now = datetime_to_micros(Utc::now());
        let expires_at = now
            .checked_add(ttl)
            .ok_or_else(|| invalid_lease("lease expiry is outside SQLite timestamp range"))?;
        let renewed: Option<i64> = sqlx::query_scalar(
            "UPDATE credential_leases SET expires_at_us = ?              WHERE scope = ? AND resource_fingerprint = ? AND lease_id = ?                AND owner_fingerprint = ? AND fencing_token = ? AND expires_at_us > ?              RETURNING expires_at_us",
        )
        .bind(expires_at)
        .bind(scope)
        .bind(resource)
        .bind(&grant.lease_id)
        .bind(owner)
        .bind(fencing_token)
        .bind(now)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(sqlite_unavailable)?;
        transaction.commit().await.map_err(sqlite_unavailable)?;
        renewed
            .map(|expires_at| {
                Ok(CredentialLeaseGrant {
                    lease_id: grant.lease_id.clone(),
                    fencing_token: grant.fencing_token,
                    expires_at: datetime_from_micros(expires_at)?,
                })
            })
            .transpose()
    }

    async fn release_credential_lease(
        &self,
        request: &CredentialLeaseRequest,
        grant: &CredentialLeaseGrant,
    ) -> StoreResult<bool> {
        request.validate()?;
        let scope = request.scope.as_str();
        let resource = resource_fingerprint("credential lease", &request.resource_id)?;
        let owner = resource_fingerprint("credential lease owner", &request.owner_id)?;
        let fencing_token = i64::try_from(grant.fencing_token.get())
            .map_err(|_| invalid_lease("fencing token is outside SQLite integer range"))?;
        let result = sqlx::query(
            "DELETE FROM credential_leases              WHERE scope = ? AND resource_fingerprint = ? AND lease_id = ?                AND owner_fingerprint = ? AND fencing_token = ?",
        )
        .bind(scope)
        .bind(resource)
        .bind(&grant.lease_id)
        .bind(owner)
        .bind(fencing_token)
        .execute(&self.pool)
        .await
        .map_err(sqlite_unavailable)?;
        Ok(result.rows_affected() == 1)
    }

    async fn credential_runtime_signals(
        &self,
        resource_ids: &[String],
    ) -> StoreResult<Vec<CredentialRuntimeSignal>> {
        self.runtime_signals(resource_ids, CredentialLeaseScope::ProviderAccount)
            .await
    }

    async fn try_acquire_bounded_lease(
        &self,
        request: &CredentialBoundedLeaseRequest,
    ) -> StoreResult<CredentialBoundedLeaseAcquisition> {
        request.validate()?;
        let lease_request = request.lease_request();
        let attempt = self
            .acquire_with_limits(
                &lease_request,
                request.max_concurrent,
                request.request_interval,
            )
            .await?;
        match attempt.grant {
            Some(grant) => Ok(CredentialBoundedLeaseAcquisition::Acquired(
                crate::CredentialLeaseGuard::new(Arc::new(self.clone()), lease_request, grant),
            )),
            None => Ok(CredentialBoundedLeaseAcquisition::Busy {
                retry_after: attempt.retry_after,
            }),
        }
    }
}

struct LeaseAttempt {
    grant: Option<CredentialLeaseGrant>,
    retry_after: Option<Duration>,
}

fn invalid_lease(message: &str) -> StoreError {
    StoreError::InvalidData {
        entity: "credential lease",
        message: message.to_owned(),
        source: None,
    }
}

fn sqlite_unavailable(_error: impl std::fmt::Display) -> StoreError {
    StoreError::Unavailable {
        backend: StoreBackend::Sqlite,
        message: "credential lease operation failed".to_owned(),
        source: None,
    }
}
