//! SQLite Provider 调度 lease 与进程内轮询游标。

use std::{
    collections::BTreeMap,
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime},
};

use gateway_core::{
    account::{AccountRuntimeSignals, ProviderAccountId},
    policy::ClientApiKeyId,
    provider_ports::{
        ProviderLeaseAcquisition, ProviderLeasePort, ProviderLeaseRequest,
        ProviderRefreshCapacityRequest, ProviderSchedulingLeaseRequest, ProviderSchedulingState,
        ProviderStoreError, ProviderStoreErrorKind,
    },
    routing::ProviderKind,
};

use crate::{
    CredentialBoundedLeaseAcquisition, CredentialBoundedLeaseRequest, CredentialLeaseGuard,
    CredentialLeaseRepository, CredentialLeaseRequest, CredentialLeaseScope,
    sqlite::SqliteCredentialLeaseRepository,
};

const PROVIDER_ACCOUNT_LEASE_TTL: Duration = Duration::from_secs(10 * 60);
const OAUTH_REFRESH_LEASE_TTL: Duration = Duration::from_secs(5 * 60);
const OAUTH_REFRESH_CAPACITY_RESOURCE: &str = "oauth-refresh-global";

#[derive(Clone)]
pub struct SqliteProviderLeaseCoordinator {
    repository: SqliteCredentialLeaseRepository,
    process_id: String,
    sequence: std::sync::Arc<AtomicU64>,
    cursors: std::sync::Arc<Mutex<BTreeMap<String, u64>>>,
}

impl SqliteProviderLeaseCoordinator {
    #[must_use]
    pub fn new(repository: SqliteCredentialLeaseRepository) -> Self {
        Self {
            repository,
            process_id: format!("gateway_{}", uuid::Uuid::now_v7().simple()),
            sequence: std::sync::Arc::new(AtomicU64::new(0)),
            cursors: std::sync::Arc::default(),
        }
    }

    fn owner_id(&self, operation: &str) -> String {
        let sequence = self.sequence.fetch_add(1, Ordering::Relaxed);
        format!("{}:{operation}:{sequence}", self.process_id)
    }

    fn next_cursor(
        &self,
        client_api_key_id: &ClientApiKeyId,
        provider_kind: &ProviderKind,
    ) -> Result<u64, ProviderStoreError> {
        let key = format!("{}\0{}", client_api_key_id.as_str(), provider_kind.as_str());
        let mut cursors = self
            .cursors
            .lock()
            .map_err(|_| unavailable("advance scheduling cursor"))?;
        let cursor = cursors.entry(key).or_default();
        let result = *cursor;
        *cursor = cursor.saturating_add(1);
        Ok(result)
    }

    async fn load_signals(
        &self,
        accounts: &[ProviderAccountId],
    ) -> Result<BTreeMap<ProviderAccountId, AccountRuntimeSignals>, ProviderStoreError> {
        let ids = accounts
            .iter()
            .map(|account| account.as_str().to_owned())
            .collect::<Vec<_>>();
        let signals = self
            .repository
            .credential_runtime_signals(&ids)
            .await
            .map_err(|_| unavailable("load scheduling signals"))?;
        signals
            .into_iter()
            .map(|signal| {
                let account = ProviderAccountId::new(signal.resource_id)
                    .map_err(|_| invalid("decode scheduling signals"))?;
                Ok((
                    account,
                    AccountRuntimeSignals {
                        in_flight: signal.in_flight,
                        last_started_at: signal.last_started_at.map(Into::into),
                        quota_reset_at: None,
                        quota_remaining_rank: None,
                        cooldown: None,
                        failure_rate_basis_points: None,
                        first_output_latency_ms: None,
                    },
                ))
            })
            .collect()
    }

    async fn acquire_scheduling(
        &self,
        request: &ProviderSchedulingLeaseRequest,
    ) -> Result<ProviderLeaseAcquisition, ProviderStoreError> {
        let ttl = request
            .deadline()
            .duration_since(SystemTime::now())
            .ok()
            .filter(|remaining| !remaining.is_zero())
            .map(|remaining| remaining.min(PROVIDER_ACCOUNT_LEASE_TTL))
            .ok_or_else(|| unavailable("acquire expired scheduling lease"))?;
        let acquisition = self
            .repository
            .try_acquire_bounded_lease(&CredentialBoundedLeaseRequest {
                scope: CredentialLeaseScope::ProviderAccount,
                resource_id: request.account_id().as_str().to_owned(),
                owner_id: self.owner_id("request"),
                max_concurrent: request.max_concurrent().get(),
                request_interval: request.request_interval(),
                ttl,
            })
            .await
            .map_err(|_| unavailable("acquire scheduling lease"))?;
        Ok(map_acquisition(acquisition))
    }

    async fn acquire_refresh_capacity(
        &self,
        request: ProviderRefreshCapacityRequest,
    ) -> Result<ProviderLeaseAcquisition, ProviderStoreError> {
        let acquisition = self
            .repository
            .try_acquire_bounded_lease(&CredentialBoundedLeaseRequest {
                scope: CredentialLeaseScope::OAuthRefreshCapacity,
                resource_id: OAUTH_REFRESH_CAPACITY_RESOURCE.to_owned(),
                owner_id: self.owner_id("refresh-capacity"),
                max_concurrent: request.max_concurrent().get(),
                request_interval: Duration::ZERO,
                ttl: OAUTH_REFRESH_LEASE_TTL,
            })
            .await
            .map_err(|_| unavailable("acquire refresh capacity"))?;
        Ok(map_acquisition(acquisition))
    }
}

impl ProviderLeasePort for SqliteProviderLeaseCoordinator {
    fn load_state<'a>(
        &'a self,
        client_api_key_id: &'a ClientApiKeyId,
        provider_kind: &'a ProviderKind,
        accounts: &'a [ProviderAccountId],
    ) -> futures::future::BoxFuture<'a, Result<ProviderSchedulingState, ProviderStoreError>> {
        Box::pin(async move {
            let signals = self.load_signals(accounts).await?;
            let cursor = self.next_cursor(client_api_key_id, provider_kind)?;
            Ok(ProviderSchedulingState::new(signals, cursor))
        })
    }

    fn try_acquire(
        &self,
        request: ProviderLeaseRequest,
    ) -> futures::future::BoxFuture<'_, Result<ProviderLeaseAcquisition, ProviderStoreError>> {
        Box::pin(async move {
            match request {
                ProviderLeaseRequest::Scheduling(request) => {
                    self.acquire_scheduling(&request).await
                }
                ProviderLeaseRequest::RefreshCapacity(request) => {
                    self.acquire_refresh_capacity(request).await
                }
                ProviderLeaseRequest::Refresh(request) => {
                    let lease = CredentialLeaseRequest {
                        scope: CredentialLeaseScope::OAuthRefresh,
                        resource_id: request.account_id().as_str().to_owned(),
                        owner_id: self.owner_id("refresh"),
                        ttl: OAUTH_REFRESH_LEASE_TTL,
                    };
                    let grant = self
                        .repository
                        .acquire_credential_lease(&lease)
                        .await
                        .map_err(|_| unavailable("acquire refresh lease"))?;
                    Ok(match grant {
                        Some(grant) => {
                            ProviderLeaseAcquisition::Acquired(Box::new(CredentialLeaseGuard::new(
                                std::sync::Arc::new(self.repository.clone())
                                    as std::sync::Arc<dyn CredentialLeaseRepository>,
                                lease,
                                grant,
                            )))
                        }
                        None => ProviderLeaseAcquisition::Busy { retry_after: None },
                    })
                }
            }
        })
    }

    fn account_in_flight<'a>(
        &'a self,
        account_ids: &'a [ProviderAccountId],
    ) -> futures::future::BoxFuture<'a, Result<BTreeMap<ProviderAccountId, u32>, ProviderStoreError>>
    {
        Box::pin(async move {
            if account_ids.is_empty() {
                return Ok(BTreeMap::new());
            }
            let ids = account_ids
                .iter()
                .map(|account_id| account_id.as_str().to_owned())
                .collect::<Vec<_>>();
            let signals = self
                .repository
                .credential_runtime_signals(&ids)
                .await
                .map_err(|_| unavailable("load account in-flight signals"))?;
            signals
                .into_iter()
                .map(|signal| {
                    let account_id = ProviderAccountId::new(signal.resource_id)
                        .map_err(|_| invalid("decode account in-flight signals"))?;
                    Ok((account_id, signal.in_flight))
                })
                .collect()
        })
    }
}

fn map_acquisition(acquisition: CredentialBoundedLeaseAcquisition) -> ProviderLeaseAcquisition {
    match acquisition {
        CredentialBoundedLeaseAcquisition::Acquired(guard) => {
            ProviderLeaseAcquisition::Acquired(Box::new(guard))
        }
        CredentialBoundedLeaseAcquisition::Busy { retry_after } => {
            ProviderLeaseAcquisition::Busy { retry_after }
        }
    }
}

fn invalid(operation: &'static str) -> ProviderStoreError {
    ProviderStoreError::new(ProviderStoreErrorKind::InvalidData, operation)
}

fn unavailable(operation: &'static str) -> ProviderStoreError {
    ProviderStoreError::new(ProviderStoreErrorKind::Unavailable, operation)
}
