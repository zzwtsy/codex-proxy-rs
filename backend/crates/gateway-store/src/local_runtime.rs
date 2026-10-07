//! 只服务当前进程的临时运行态适配器。
//!
//! SQLite 模式不依赖 Redis 保存准入、Responses 亲和与单进程 Worker leader 状态。

use std::{
    collections::{HashMap, HashSet},
    num::NonZeroU64,
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

use futures::future::BoxFuture;
use gateway_core::{
    engine::{
        ModelRequestId,
        admission::{
            ClientAdmissionDecision, ClientAdmissionError, ClientAdmissionPort,
            ClientAdmissionRecovery, ClientAdmissionRejection, ClientAdmissionRequest,
            ClientAdmissionRestoreResult,
        },
        continuation::{
            NativeContinuationPin, NativeContinuationPort, NativeContinuationStoreError,
            PreviousResponseId,
        },
    },
    policy::ClientApiKeyId,
    task::{
        WorkerFencingToken, WorkerLeaderLeaseGuard, WorkerLeaderLeasePort, WorkerLeaseAcquisition,
        WorkerLeaseError, WorkerLeaseRequest,
    },
};
use serde::Serialize;
use tokio::sync::Mutex;

use crate::coordination::resource_fingerprint;

const CONTINUATION_TTL: Duration = Duration::from_secs(4 * 60 * 60);
const MAX_CONTINUATIONS: usize = 65_536;
const MAX_CONTINUATION_BYTES: usize = 64 * 1024;
const CLIENT_ADMISSION_WINDOW: Duration = Duration::from_secs(60);

#[derive(Clone, Default)]
pub struct LocalClientAdmissionPort {
    state: Arc<Mutex<ClientAdmissionState>>,
}

#[derive(Default)]
struct ClientAdmissionState {
    clients: HashMap<String, ClientAdmissionClientState>,
}

#[derive(Default)]
struct ClientAdmissionClientState {
    recent_requests: HashMap<String, SystemTime>,
    running_requests: HashMap<String, SystemTime>,
}

impl ClientAdmissionClientState {
    fn prune(&mut self, now: SystemTime, cutoff: SystemTime) {
        self.recent_requests
            .retain(|_, started_at| *started_at > cutoff);
        self.running_requests
            .retain(|_, expires_at| *expires_at > now);
    }
}

impl ClientAdmissionState {
    fn prune_client(&mut self, client_id: &str, now: SystemTime, cutoff: SystemTime) {
        if let Some(client) = self.clients.get_mut(client_id) {
            client.prune(now, cutoff);
            if client.recent_requests.is_empty() && client.running_requests.is_empty() {
                self.clients.remove(client_id);
            }
        }
    }
}

impl ClientAdmissionPort for LocalClientAdmissionPort {
    fn abandon(&self, client_api_key_id: &ClientApiKeyId, model_request_id: &ModelRequestId) {
        let port = self.clone();
        let client_api_key_id = client_api_key_id.clone();
        let model_request_id = model_request_id.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            drop(runtime.spawn(async move {
                let _ = port.release(&client_api_key_id, &model_request_id).await;
            }));
        }
    }

    fn admit(
        &self,
        request: ClientAdmissionRequest,
    ) -> BoxFuture<'_, Result<ClientAdmissionDecision, ClientAdmissionError>> {
        Box::pin(async move {
            if request.lease_ttl.is_zero() {
                return Err(ClientAdmissionError(None));
            }
            let now = SystemTime::now();
            let expires_at = now
                .checked_add(request.lease_ttl)
                .ok_or(ClientAdmissionError(None))?;
            let cutoff = now
                .checked_sub(CLIENT_ADMISSION_WINDOW)
                .ok_or(ClientAdmissionError(None))?;
            let mut state = self.state.lock().await;
            let client_id = request.client_api_key_id.as_str();
            state.prune_client(client_id, now, cutoff);
            let client = state.clients.entry(client_id.to_owned()).or_default();
            if request.limits.requests_per_minute > 0
                && u64::try_from(client.recent_requests.len()).unwrap_or(u64::MAX)
                    >= request.limits.requests_per_minute
            {
                return Ok(ClientAdmissionDecision::Rejected(
                    ClientAdmissionRejection::RateLimited,
                ));
            }
            if !request.allow_concurrency_acquire
                || (request.limits.max_concurrency > 0
                    && u64::try_from(client.running_requests.len()).unwrap_or(u64::MAX)
                        >= request.limits.max_concurrency)
            {
                return Ok(ClientAdmissionDecision::Rejected(
                    ClientAdmissionRejection::ConcurrencyLimited,
                ));
            }
            client
                .running_requests
                .insert(request.model_request_id.as_str().to_owned(), expires_at);
            client
                .recent_requests
                .insert(request.model_request_id.as_str().to_owned(), now);
            Ok(ClientAdmissionDecision::Granted)
        })
    }

    fn release<'a>(
        &'a self,
        client_api_key_id: &'a ClientApiKeyId,
        model_request_id: &'a ModelRequestId,
    ) -> BoxFuture<'a, Result<bool, ClientAdmissionError>> {
        Box::pin(async move {
            let now = SystemTime::now();
            let cutoff = now
                .checked_sub(CLIENT_ADMISSION_WINDOW)
                .ok_or(ClientAdmissionError(None))?;
            let mut state = self.state.lock().await;
            let client_id = client_api_key_id.as_str();
            state.prune_client(client_id, now, cutoff);
            let removed = state.clients.get_mut(client_id).is_some_and(|client| {
                client
                    .running_requests
                    .remove(model_request_id.as_str())
                    .is_some()
            });
            state.prune_client(client_id, now, cutoff);
            Ok(removed)
        })
    }

    fn restore(
        &self,
        recovery: ClientAdmissionRecovery,
    ) -> BoxFuture<'_, Result<ClientAdmissionRestoreResult, ClientAdmissionError>> {
        Box::pin(async move {
            let now = SystemTime::now();
            let cutoff = now
                .checked_sub(CLIENT_ADMISSION_WINDOW)
                .ok_or(ClientAdmissionError(None))?;
            let mut recent_ids = HashSet::with_capacity(recovery.recent_requests.len());
            for request in &recovery.recent_requests {
                if request.started_at > now || !recent_ids.insert(request.model_request_id.as_str())
                {
                    return Err(ClientAdmissionError(None));
                }
            }
            let mut running_ids = HashSet::with_capacity(recovery.running_requests.len());
            if recovery
                .running_requests
                .iter()
                .any(|request| !running_ids.insert(request.model_request_id.as_str()))
            {
                return Err(ClientAdmissionError(None));
            }

            let mut state = self.state.lock().await;
            let client_id = recovery.client_api_key_id.as_str();
            state.prune_client(client_id, now, cutoff);
            let client = state.clients.entry(client_id.to_owned()).or_default();
            let mut restored_recent_requests = 0_u64;
            for request in recovery.recent_requests {
                if request.started_at > cutoff {
                    let is_new = !client
                        .recent_requests
                        .contains_key(request.model_request_id.as_str());
                    client
                        .recent_requests
                        .entry(request.model_request_id.as_str().to_owned())
                        .or_insert(request.started_at);
                    if is_new {
                        restored_recent_requests = restored_recent_requests.saturating_add(1);
                    }
                }
            }
            let mut restored_running_requests = 0_u64;
            for request in recovery.running_requests {
                if request.expires_at > now {
                    let is_new = !client
                        .running_requests
                        .contains_key(request.model_request_id.as_str());
                    client
                        .running_requests
                        .entry(request.model_request_id.as_str().to_owned())
                        .or_insert(request.expires_at);
                    if is_new {
                        restored_running_requests = restored_running_requests.saturating_add(1);
                    }
                }
            }
            Ok(ClientAdmissionRestoreResult {
                restored_recent_requests,
                restored_running_requests,
            })
        })
    }
}

#[derive(Clone, Default)]
pub struct LocalNativeContinuationRepository {
    entries: Arc<Mutex<HashMap<String, ContinuationEntry>>>,
}

struct ContinuationEntry {
    pin: NativeContinuationPin,
    stored_at: Instant,
    expires_at: Instant,
}

#[derive(Serialize)]
struct ContinuationSize<'a> {
    client_api_key_id: &'a str,
    upstream_response_id: &'a str,
    provider: &'a str,
    account: &'a str,
    scope: &'static str,
    session_state: Option<&'a gateway_core::operation::ProviderSessionState>,
}

impl NativeContinuationPort for LocalNativeContinuationRepository {
    fn resolve<'a>(
        &'a self,
        client_api_key_id: &'a ClientApiKeyId,
        previous_response_id: &'a PreviousResponseId,
    ) -> BoxFuture<'a, Result<Option<NativeContinuationPin>, NativeContinuationStoreError>> {
        Box::pin(async move {
            if previous_response_id.as_str().is_empty() {
                return Ok(None);
            }
            let key = continuation_key(previous_response_id.as_str())?;
            let now = Instant::now();
            let mut entries = self.entries.lock().await;
            entries.retain(|_, entry| entry.expires_at > now);
            let Some(entry) = entries.get(&key) else {
                return Ok(None);
            };
            if !entry.pin.matches_client(client_api_key_id) {
                return Err(NativeContinuationStoreError::ownership_mismatch());
            }
            Ok(Some(entry.pin.clone()))
        })
    }

    fn record<'a>(
        &'a self,
        pin: NativeContinuationPin,
    ) -> BoxFuture<'a, Result<(), NativeContinuationStoreError>> {
        Box::pin(async move {
            if pin.previous_response_id().as_str().is_empty() {
                return Ok(());
            }
            let key = continuation_key(pin.previous_response_id().as_str())?;
            if pin
                .session_state()
                .is_some_and(|state| state.provider() != pin.provider().as_str())
            {
                return Err(NativeContinuationStoreError::invalid_data(
                    "session state provider does not match pin provider",
                ));
            }
            let scope = match pin.scope() {
                gateway_core::engine::continuation::NativeContinuationScope::Persisted => {
                    "persisted"
                }
                gateway_core::engine::continuation::NativeContinuationScope::ConnectionLocal => {
                    "connection_local"
                }
            };
            let size = serde_json::to_vec(&ContinuationSize {
                client_api_key_id: pin.client_api_key_id().as_str(),
                upstream_response_id: pin.upstream_response_id().as_str(),
                provider: pin.provider().as_str(),
                account: pin.account().as_str(),
                scope,
                session_state: pin.session_state(),
            })
            .map_err(|_| {
                NativeContinuationStoreError::invalid_data("continuation encoding failed")
            })?
            .len();
            if size > MAX_CONTINUATION_BYTES {
                return Err(NativeContinuationStoreError::invalid_data(
                    "continuation record exceeds the configured size limit",
                ));
            }
            let now = Instant::now();
            let expires_at = now.checked_add(CONTINUATION_TTL).ok_or_else(|| {
                NativeContinuationStoreError::invalid_data("continuation expiry is out of range")
            })?;
            let mut entries = self.entries.lock().await;
            entries.retain(|_, entry| entry.expires_at > now);
            if !entries.contains_key(&key)
                && entries.len() >= MAX_CONTINUATIONS
                && let Some(oldest_key) = entries
                    .iter()
                    .min_by_key(|(_, entry)| entry.stored_at)
                    .map(|(key, _)| key.clone())
            {
                entries.remove(&oldest_key);
            }
            entries.insert(
                key,
                ContinuationEntry {
                    pin,
                    stored_at: now,
                    expires_at,
                },
            );
            Ok(())
        })
    }
}

fn continuation_key(response_id: &str) -> Result<String, NativeContinuationStoreError> {
    resource_fingerprint("native continuation", response_id)
        .map_err(|_| NativeContinuationStoreError::invalid_data("invalid continuation handle"))
}

#[derive(Clone, Default)]
pub struct LocalWorkerLeaderLeasePort {
    state: Arc<Mutex<WorkerLeaseState>>,
}

#[derive(Default)]
struct WorkerLeaseState {
    fencing_counters: HashMap<gateway_core::task::WorkerId, u64>,
    active: HashMap<gateway_core::task::WorkerId, ActiveWorkerLease>,
}

struct ActiveWorkerLease {
    fencing_token: u64,
    expires_at: Instant,
}

struct LocalWorkerLeaderLeaseGuard {
    state: Arc<Mutex<WorkerLeaseState>>,
    worker: gateway_core::task::WorkerId,
    fencing_token: u64,
    ttl: Duration,
}

impl WorkerLeaderLeaseGuard for LocalWorkerLeaderLeaseGuard {
    fn fencing_token(&self) -> WorkerFencingToken {
        WorkerFencingToken::new(
            NonZeroU64::new(self.fencing_token).expect("worker fencing token is nonzero"),
        )
    }

    fn renew(&mut self) -> BoxFuture<'_, Result<(), WorkerLeaseError>> {
        Box::pin(async move {
            let now = Instant::now();
            let expires_at = now
                .checked_add(self.ttl)
                .ok_or_else(|| WorkerLeaseError::safe("worker lease expiry is out of range"))?;
            let mut state = self.state.lock().await;
            let Some(active) = state.active.get_mut(&self.worker) else {
                return Err(WorkerLeaseError::safe("worker lease was lost"));
            };
            if active.fencing_token != self.fencing_token || active.expires_at <= now {
                return Err(WorkerLeaseError::safe("worker lease was lost"));
            }
            active.expires_at = expires_at;
            Ok(())
        })
    }

    fn release(self: Box<Self>) -> BoxFuture<'static, Result<(), WorkerLeaseError>> {
        let state = Arc::clone(&self.state);
        let worker = self.worker.clone();
        let fencing_token = self.fencing_token;
        Box::pin(async move {
            let mut state = state.lock().await;
            if state
                .active
                .get(&worker)
                .is_some_and(|active| active.fencing_token == fencing_token)
            {
                state.active.remove(&worker);
            }
            Ok(())
        })
    }
}

impl WorkerLeaderLeasePort for LocalWorkerLeaderLeasePort {
    fn try_acquire(
        &self,
        request: WorkerLeaseRequest,
    ) -> BoxFuture<'_, Result<WorkerLeaseAcquisition, WorkerLeaseError>> {
        Box::pin(async move {
            let now = Instant::now();
            let expires_at = now
                .checked_add(request.ttl())
                .ok_or_else(|| WorkerLeaseError::safe("worker lease expiry is out of range"))?;
            let worker = request.worker().clone();
            let mut state = self.state.lock().await;
            if let Some(active) = state.active.get(&worker) {
                if active.expires_at > now {
                    return Ok(WorkerLeaseAcquisition::Busy {
                        retry_after: Some(active.expires_at.saturating_duration_since(now)),
                    });
                }
                state.active.remove(&worker);
            }
            let fencing_token = state
                .fencing_counters
                .get(&worker)
                .copied()
                .unwrap_or_default()
                .checked_add(1)
                .and_then(NonZeroU64::new)
                .ok_or_else(|| WorkerLeaseError::safe("worker fencing token is exhausted"))?;
            state
                .fencing_counters
                .insert(worker.clone(), fencing_token.get());
            state.active.insert(
                worker.clone(),
                ActiveWorkerLease {
                    fencing_token: fencing_token.get(),
                    expires_at,
                },
            );
            Ok(WorkerLeaseAcquisition::Acquired(Box::new(
                LocalWorkerLeaderLeaseGuard {
                    state: Arc::clone(&self.state),
                    worker,
                    fencing_token: fencing_token.get(),
                    ttl: request.ttl(),
                },
            )))
        })
    }
}
