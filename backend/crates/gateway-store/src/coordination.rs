//! 跨后端共享的运行态协调合同与服务进程本地适配器。

use std::{
    collections::HashMap,
    fmt,
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::future::BoxFuture;
use gateway_core::{
    account::OpaqueProviderData,
    provider_ports::{
        NewOAuthPendingFlow, OAuthPendingBinding, OAuthPendingClaimOutcome,
        OAuthPendingConsumeOutcome, OAuthPendingFlowPort, OAuthPendingPutOutcome,
        OAuthPendingReleaseOutcome, ProviderStoreError, ProviderStoreErrorKind,
    },
    routing::ProviderKind,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use crate::{Revision, StoreError, StoreResult, require_nonempty};

const MAX_LOCAL_OAUTH_PENDING_FLOWS: usize = 4096;

/// 认证状态后端存储的会话主体；不含密码或原始 API Key。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum SessionSubjectRecord {
    Admin {
        admin_user_id: String,
        // 旧会话没有指纹，按未认证处理并要求重新登录。
        #[serde(default)]
        credential_fingerprint: String,
    },
    Key {
        client_key_id: String,
    },
}

impl SessionSubjectRecord {
    pub(crate) fn validate(&self) -> StoreResult<()> {
        match self {
            Self::Admin { admin_user_id, .. } => {
                require_nonempty("authentication session", "admin_user_id", admin_user_id)
            }
            Self::Key { client_key_id } => {
                require_nonempty("authentication session", "client_key_id", client_key_id)
            }
        }
    }
}

/// 可丢失的认证会话事实。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthSessionRecord {
    pub subject: SessionSubjectRecord,
    pub expires_at: DateTime<Utc>,
    pub absolute_expires_at: Option<DateTime<Utc>>,
}

impl AuthSessionRecord {
    pub(crate) fn validate(&self) -> StoreResult<()> {
        self.subject.validate()?;
        if self.expires_at <= Utc::now() {
            return Err(StoreError::InvalidData {
                entity: "authentication state",
                message: "session expiry must be in the future".to_owned(),
                source: None,
            });
        }
        if self
            .absolute_expires_at
            .is_some_and(|limit| self.expires_at > limit)
        {
            return Err(auth_state_invalid(
                "session expiry is outside its absolute lifetime",
            ));
        }
        Ok(())
    }
}

/// 管理认证用例依赖的临时状态端口；Redis 和进程内实现共用该合同。
#[async_trait]
pub trait AuthStateRepository: Send + Sync {
    async fn load_session(&self, session_id: &str) -> StoreResult<Option<AuthSessionRecord>>;
    async fn store_session(&self, session_id: &str, session: &AuthSessionRecord)
    -> StoreResult<()>;
    async fn delete_session(&self, session_id: &str) -> StoreResult<Option<AuthSessionRecord>>;
    async fn renew_session(
        &self,
        session_id: &str,
        expected: &AuthSessionRecord,
        expires_at: DateTime<Utc>,
    ) -> StoreResult<Option<AuthSessionRecord>>;
    async fn consume_login_attempt(
        &self,
        source: &str,
        source_limit: u32,
        global_limit: u32,
        window: Duration,
    ) -> StoreResult<Option<Duration>>;
}

/// 服务进程专属认证状态；进程重启会使会话和登录限流窗口失效。
#[derive(Clone, Default)]
pub struct LocalAuthStateRepository {
    state: Arc<Mutex<LocalAuthState>>,
}

#[derive(Default)]
struct LocalAuthState {
    sessions: HashMap<String, AuthSessionRecord>,
    source_attempts: HashMap<String, LoginAttemptWindow>,
    global_attempts: Option<LoginAttemptWindow>,
}

struct LoginAttemptWindow {
    started_at: Instant,
    attempts: u32,
}

#[async_trait]
impl AuthStateRepository for LocalAuthStateRepository {
    async fn load_session(&self, session_id: &str) -> StoreResult<Option<AuthSessionRecord>> {
        let key = resource_fingerprint("authentication session", session_id)?;
        let now = Utc::now();
        let mut state = self.state.lock().await;
        state.sessions.retain(|_, session| session.expires_at > now);
        Ok(state.sessions.get(&key).cloned())
    }

    async fn store_session(
        &self,
        session_id: &str,
        session: &AuthSessionRecord,
    ) -> StoreResult<()> {
        session.validate()?;
        let key = resource_fingerprint("authentication session", session_id)?;
        let mut state = self.state.lock().await;
        let now = Utc::now();
        state.sessions.retain(|_, session| session.expires_at > now);
        state.sessions.insert(key, session.clone());
        Ok(())
    }

    async fn delete_session(&self, session_id: &str) -> StoreResult<Option<AuthSessionRecord>> {
        let key = resource_fingerprint("authentication session", session_id)?;
        let mut state = self.state.lock().await;
        let session = state.sessions.remove(&key);
        Ok(session.filter(|session| session.expires_at > Utc::now()))
    }

    async fn renew_session(
        &self,
        session_id: &str,
        expected: &AuthSessionRecord,
        expires_at: DateTime<Utc>,
    ) -> StoreResult<Option<AuthSessionRecord>> {
        let Some(absolute_expires_at) = expected.absolute_expires_at else {
            return Err(auth_state_invalid("session has no renewable lifetime"));
        };
        if expires_at < expected.expires_at || expires_at > absolute_expires_at {
            return Err(auth_state_invalid(
                "session renewal is outside its lifetime",
            ));
        }

        let mut renewed = expected.clone();
        renewed.expires_at = expires_at;
        renewed.validate()?;

        let key = resource_fingerprint("authentication session", session_id)?;
        let now = Utc::now();
        let mut state = self.state.lock().await;
        state.sessions.retain(|_, session| session.expires_at > now);
        match state.sessions.get(&key) {
            Some(current) if current == expected => {
                state.sessions.insert(key, renewed.clone());
                Ok(Some(renewed))
            }
            Some(current) => Ok(Some(current.clone())),
            None => Ok(None),
        }
    }

    async fn consume_login_attempt(
        &self,
        source: &str,
        source_limit: u32,
        global_limit: u32,
        window: Duration,
    ) -> StoreResult<Option<Duration>> {
        if source_limit == 0 || global_limit == 0 || window.is_zero() {
            return Err(auth_state_invalid("login rate limit policy is invalid"));
        }
        let source_key = resource_fingerprint("login source", source)?;
        let now = Instant::now();
        now.checked_add(window)
            .ok_or_else(|| auth_state_invalid("login rate limit window is too large"))?;
        let mut state = self.state.lock().await;
        state
            .source_attempts
            .retain(|_, attempt| now.saturating_duration_since(attempt.started_at) < window);
        {
            let attempt = state.global_attempts.get_or_insert(LoginAttemptWindow {
                started_at: now,
                attempts: 0,
            });
            reset_login_window(attempt, now, window);
            if attempt.attempts >= global_limit {
                let retry_after = (attempt.started_at + window).saturating_duration_since(now);
                return Ok(Some(retry_after.max(Duration::from_millis(1))));
            }
            attempt.attempts = attempt.attempts.saturating_add(1);
        }
        let (source_attempts, source_window_end) = {
            let attempt = state
                .source_attempts
                .entry(source_key)
                .or_insert(LoginAttemptWindow {
                    started_at: now,
                    attempts: 0,
                });
            let count = record_login_attempt(attempt, now, window);
            (count, attempt.started_at + window)
        };
        let source_retry = (source_attempts > source_limit)
            .then(|| source_window_end.saturating_duration_since(now));
        Ok(source_retry.map(|duration| duration.max(Duration::from_millis(1))))
    }
}

fn reset_login_window(attempt: &mut LoginAttemptWindow, now: Instant, window: Duration) {
    if now.saturating_duration_since(attempt.started_at) >= window {
        attempt.started_at = now;
        attempt.attempts = 0;
    }
}

fn record_login_attempt(attempt: &mut LoginAttemptWindow, now: Instant, window: Duration) -> u32 {
    reset_login_window(attempt, now, window);
    attempt.attempts = attempt.attempts.saturating_add(1);
    attempt.attempts
}

fn auth_state_invalid(message: &'static str) -> StoreError {
    StoreError::InvalidData {
        entity: "authentication state",
        message: message.to_owned(),
        source: None,
    }
}

/// 服务进程专属 OAuth pending flow；重启后未完成的授权流程会失效。
#[derive(Clone, Default)]
pub struct LocalOAuthPendingFlowRepository {
    flows: Arc<Mutex<HashMap<String, LocalOAuthPendingFlow>>>,
}

struct LocalOAuthPendingFlow {
    owner_fingerprint: String,
    payload: OpaqueProviderData,
    expires_at: Instant,
    claim: Option<LocalOAuthPendingClaim>,
}

struct LocalOAuthPendingClaim {
    fingerprint: String,
    expires_at: Instant,
}

impl OAuthPendingFlowPort for LocalOAuthPendingFlowRepository {
    fn put_if_absent(
        &self,
        flow: NewOAuthPendingFlow,
    ) -> BoxFuture<'_, Result<OAuthPendingPutOutcome, ProviderStoreError>> {
        Box::pin(async move {
            let key = oauth_pending_key(flow.provider_kind(), flow.flow());
            let owner_fingerprint = oauth_pending_fingerprint(flow.provider_kind(), flow.owner());
            let encoded = serde_json::to_vec(flow.payload().expose_to_provider())
                .map_err(|_| oauth_pending_invalid("encode OAuth pending payload"))?;
            if encoded.len() > 1024 * 1024 {
                return Err(oauth_pending_invalid("validate OAuth pending payload"));
            }
            let expires_at = oauth_pending_expiry(flow.ttl())?;
            let mut flows = self.flows.lock().await;
            let now = Instant::now();
            flows.retain(|_, pending| pending.expires_at > now);
            if flows.contains_key(&key) {
                return Ok(OAuthPendingPutOutcome::AlreadyExists);
            }
            if flows.len() >= MAX_LOCAL_OAUTH_PENDING_FLOWS {
                return Err(ProviderStoreError::new(
                    ProviderStoreErrorKind::Unavailable,
                    "OAuth pending flow capacity is full",
                ));
            }
            flows.insert(
                key,
                LocalOAuthPendingFlow {
                    owner_fingerprint,
                    payload: flow.payload().clone(),
                    expires_at,
                    claim: None,
                },
            );
            Ok(OAuthPendingPutOutcome::Stored)
        })
    }

    fn claim_if_owner<'a>(
        &'a self,
        provider_kind: &'a ProviderKind,
        flow: &'a OAuthPendingBinding,
        owner: &'a OAuthPendingBinding,
        claim: &'a OAuthPendingBinding,
        claim_ttl: Duration,
    ) -> BoxFuture<'a, Result<OAuthPendingClaimOutcome, ProviderStoreError>> {
        Box::pin(async move {
            let key = oauth_pending_key(provider_kind, flow);
            let owner_fingerprint = oauth_pending_fingerprint(provider_kind, owner);
            let claim_fingerprint = oauth_pending_fingerprint(provider_kind, claim);
            let expires_at = oauth_pending_expiry(claim_ttl)?;
            let now = Instant::now();
            let mut flows = self.flows.lock().await;
            let Some(pending) = flows.get_mut(&key) else {
                return Ok(OAuthPendingClaimOutcome::NotFound);
            };
            if pending.expires_at <= now {
                flows.remove(&key);
                return Ok(OAuthPendingClaimOutcome::NotFound);
            }
            if pending.owner_fingerprint != owner_fingerprint {
                return Ok(OAuthPendingClaimOutcome::OwnerMismatch);
            }
            if pending
                .claim
                .as_ref()
                .is_some_and(|current| current.expires_at > now)
            {
                return Ok(OAuthPendingClaimOutcome::InProgress);
            }
            pending.claim = Some(LocalOAuthPendingClaim {
                fingerprint: claim_fingerprint,
                expires_at,
            });
            Ok(OAuthPendingClaimOutcome::Claimed(pending.payload.clone()))
        })
    }

    fn release_claim<'a>(
        &'a self,
        provider_kind: &'a ProviderKind,
        flow: &'a OAuthPendingBinding,
        owner: &'a OAuthPendingBinding,
        claim: &'a OAuthPendingBinding,
    ) -> BoxFuture<'a, Result<OAuthPendingReleaseOutcome, ProviderStoreError>> {
        Box::pin(async move {
            let key = oauth_pending_key(provider_kind, flow);
            let owner_fingerprint = oauth_pending_fingerprint(provider_kind, owner);
            let claim_fingerprint = oauth_pending_fingerprint(provider_kind, claim);
            let now = Instant::now();
            let mut flows = self.flows.lock().await;
            let Some(pending) = flows.get_mut(&key) else {
                return Ok(OAuthPendingReleaseOutcome::NotFound);
            };
            if pending.expires_at <= now {
                flows.remove(&key);
                return Ok(OAuthPendingReleaseOutcome::NotFound);
            }
            if pending.owner_fingerprint != owner_fingerprint {
                return Ok(OAuthPendingReleaseOutcome::OwnerMismatch);
            }
            if pending
                .claim
                .as_ref()
                .is_none_or(|current| current.fingerprint != claim_fingerprint)
            {
                return Ok(OAuthPendingReleaseOutcome::ClaimMismatch);
            }
            pending.claim = None;
            Ok(OAuthPendingReleaseOutcome::Released)
        })
    }

    fn consume_claim<'a>(
        &'a self,
        provider_kind: &'a ProviderKind,
        flow: &'a OAuthPendingBinding,
        owner: &'a OAuthPendingBinding,
        claim: &'a OAuthPendingBinding,
    ) -> BoxFuture<'a, Result<OAuthPendingConsumeOutcome, ProviderStoreError>> {
        Box::pin(async move {
            let key = oauth_pending_key(provider_kind, flow);
            let owner_fingerprint = oauth_pending_fingerprint(provider_kind, owner);
            let claim_fingerprint = oauth_pending_fingerprint(provider_kind, claim);
            let now = Instant::now();
            let mut flows = self.flows.lock().await;
            let Some(pending) = flows.get(&key) else {
                return Ok(OAuthPendingConsumeOutcome::NotFound);
            };
            if pending.expires_at <= now {
                flows.remove(&key);
                return Ok(OAuthPendingConsumeOutcome::NotFound);
            }
            if pending.owner_fingerprint != owner_fingerprint {
                return Ok(OAuthPendingConsumeOutcome::OwnerMismatch);
            }
            if pending
                .claim
                .as_ref()
                .is_none_or(|current| current.fingerprint != claim_fingerprint)
            {
                return Ok(OAuthPendingConsumeOutcome::ClaimMismatch);
            }
            flows.remove(&key);
            Ok(OAuthPendingConsumeOutcome::Consumed)
        })
    }
}

fn oauth_pending_key(provider_kind: &ProviderKind, flow: &OAuthPendingBinding) -> String {
    format!(
        "{}:{}",
        provider_kind.as_str(),
        oauth_pending_fingerprint(provider_kind, flow)
    )
}

fn oauth_pending_fingerprint(
    provider_kind: &ProviderKind,
    binding: &OAuthPendingBinding,
) -> String {
    let mut digest = Sha256::new();
    digest.update(provider_kind.as_str().as_bytes());
    digest.update([0]);
    digest.update(binding.expose_to_store().as_bytes());
    hex::encode(digest.finalize())
}

fn oauth_pending_expiry(ttl: Duration) -> Result<Instant, ProviderStoreError> {
    if ttl.is_zero() {
        return Err(oauth_pending_invalid("validate OAuth pending TTL"));
    }
    Instant::now()
        .checked_add(ttl)
        .ok_or_else(|| oauth_pending_invalid("validate OAuth pending TTL"))
}

fn oauth_pending_invalid(operation: &'static str) -> ProviderStoreError {
    ProviderStoreError::new(ProviderStoreErrorKind::InvalidData, operation)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CredentialLeaseScope {
    Provider,
    ProviderAccount,
    OAuthRefreshCapacity,
    OAuthRefresh,
    ProviderTask,
}

impl CredentialLeaseScope {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Provider => "provider",
            Self::ProviderAccount => "account",
            Self::OAuthRefreshCapacity => "refresh-capacity",
            Self::OAuthRefresh => "refresh",
            Self::ProviderTask => "task",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CredentialLeaseRequest {
    pub scope: CredentialLeaseScope,
    pub resource_id: String,
    pub owner_id: String,
    pub ttl: Duration,
}

impl CredentialLeaseRequest {
    pub fn validate(&self) -> StoreResult<()> {
        require_nonempty("credential lease", "resource_id", &self.resource_id)?;
        require_nonempty("credential lease", "owner_id", &self.owner_id)?;
        supported_duration(self.ttl, false, "lease TTL")
    }
}

/// 对明确资源施加并发数和启动间隔约束的 lease 请求。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CredentialBoundedLeaseRequest {
    pub scope: CredentialLeaseScope,
    pub resource_id: String,
    pub owner_id: String,
    pub max_concurrent: u32,
    pub request_interval: Duration,
    pub ttl: Duration,
}

impl CredentialBoundedLeaseRequest {
    pub fn validate(&self) -> StoreResult<()> {
        require_nonempty("credential bounded lease", "resource_id", &self.resource_id)?;
        require_nonempty("credential bounded lease", "owner_id", &self.owner_id)?;
        if self.max_concurrent == 0 && self.scope != CredentialLeaseScope::ProviderAccount {
            return Err(invalid("max_concurrent must be positive"));
        }
        supported_duration(self.request_interval, true, "request interval")?;
        supported_duration(self.ttl, false, "lease TTL")
    }

    pub(crate) fn lease_request(&self) -> CredentialLeaseRequest {
        CredentialLeaseRequest {
            scope: self.scope,
            resource_id: self.resource_id.clone(),
            owner_id: self.owner_id.clone(),
            ttl: self.ttl,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CredentialLeaseGrant {
    pub lease_id: String,
    pub fencing_token: Revision,
    pub expires_at: DateTime<Utc>,
}

/// 请求排程所需的账号运行信号。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CredentialRuntimeSignal {
    pub resource_id: String,
    pub in_flight: u32,
    pub last_started_at: Option<DateTime<Utc>>,
}

pub enum CredentialBoundedLeaseAcquisition {
    Acquired(CredentialLeaseGuard),
    Busy { retry_after: Option<Duration> },
}

impl fmt::Debug for CredentialBoundedLeaseAcquisition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Acquired(_) => formatter.write_str("Acquired([LEASE_GUARD])"),
            Self::Busy { retry_after } => formatter
                .debug_struct("Busy")
                .field("retry_after", retry_after)
                .finish(),
        }
    }
}

/// Drop 时尽力释放；进程崩溃由后端设置的 TTL 回收。
pub struct CredentialLeaseGuard {
    repository: Arc<dyn CredentialLeaseRepository>,
    request: CredentialLeaseRequest,
    grant: Option<CredentialLeaseGrant>,
}

impl CredentialLeaseGuard {
    pub(crate) fn new(
        repository: Arc<dyn CredentialLeaseRepository>,
        request: CredentialLeaseRequest,
        grant: CredentialLeaseGrant,
    ) -> Self {
        Self {
            repository,
            request,
            grant: Some(grant),
        }
    }

    /// 租约丢失时取消依赖该账号槽位的请求。
    pub fn maintain(
        self,
        deadline: gateway_core::lifecycle::Deadline,
        cancellation: gateway_core::lifecycle::CancellationToken,
    ) -> StoreResult<impl gateway_core::provider_ports::ProviderLeaseGuard> {
        let repository = Arc::clone(&self.repository);
        let lease_request = self.request.clone();
        let grant = self.grant.clone().ok_or_else(|| StoreError::InvalidData {
            entity: "credential lease",
            message: "lease has already been released".to_owned(),
            source: None,
        })?;
        let renewal =
            crate::lease_renewal::LeaseRenewal::spawn(deadline, Some(cancellation), move |ttl| {
                let repository = Arc::clone(&repository);
                let mut lease_request = lease_request.clone();
                lease_request.ttl = ttl;
                let grant = grant.clone();
                Box::pin(async move {
                    repository
                        .renew_credential_lease(&lease_request, &grant)
                        .await
                        .map(|grant| grant.is_some())
                })
            });
        Ok(RenewingCredentialLease {
            // 先停止续期，再由 guard 释放存储端租约。
            _renewal: renewal,
            _guard: self,
        })
    }

    #[must_use]
    pub fn grant(&self) -> Option<&CredentialLeaseGrant> {
        self.grant.as_ref()
    }

    pub async fn release(mut self) -> StoreResult<bool> {
        let Some(grant) = self.grant.take() else {
            return Ok(false);
        };
        self.repository
            .release_credential_lease(&self.request, &grant)
            .await
    }
}

struct RenewingCredentialLease {
    _renewal: crate::lease_renewal::LeaseRenewal,
    _guard: CredentialLeaseGuard,
}

impl fmt::Debug for CredentialLeaseGuard {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CredentialLeaseGuard")
            .field("scope", &self.request.scope)
            .field("resource_id", &"[FINGERPRINTED]")
            .field("owner_id", &"[FINGERPRINTED]")
            .field("grant", &self.grant)
            .finish()
    }
}

impl Drop for CredentialLeaseGuard {
    fn drop(&mut self) {
        let Some(grant) = self.grant.take() else {
            return;
        };
        let repository = Arc::clone(&self.repository);
        let request = self.request.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            drop(runtime.spawn(async move {
                let _ = repository.release_credential_lease(&request, &grant).await;
            }));
        }
    }
}

#[async_trait]
pub trait CredentialLeaseRepository: Send + Sync {
    async fn acquire_credential_lease(
        &self,
        request: &CredentialLeaseRequest,
    ) -> StoreResult<Option<CredentialLeaseGrant>>;
    async fn renew_credential_lease(
        &self,
        request: &CredentialLeaseRequest,
        grant: &CredentialLeaseGrant,
    ) -> StoreResult<Option<CredentialLeaseGrant>>;
    async fn release_credential_lease(
        &self,
        request: &CredentialLeaseRequest,
        grant: &CredentialLeaseGrant,
    ) -> StoreResult<bool>;
    async fn credential_runtime_signals(
        &self,
        resource_ids: &[String],
    ) -> StoreResult<Vec<CredentialRuntimeSignal>>;
    async fn try_acquire_bounded_lease(
        &self,
        request: &CredentialBoundedLeaseRequest,
    ) -> StoreResult<CredentialBoundedLeaseAcquisition>;
}

fn supported_duration(value: Duration, allow_zero: bool, field: &'static str) -> StoreResult<()> {
    let milliseconds = value.as_millis();
    if (!allow_zero && milliseconds == 0) || milliseconds > i64::MAX as u128 {
        return Err(invalid(&format!("{field} is outside the supported range")));
    }
    Ok(())
}

fn invalid(message: &str) -> StoreError {
    StoreError::InvalidData {
        entity: "credential lease",
        message: message.to_owned(),
        source: None,
    }
}

pub(crate) fn resource_fingerprint(entity: &'static str, value: &str) -> StoreResult<String> {
    require_nonempty(entity, "resource ID", value)?;
    Ok(hex::encode(Sha256::digest(value.as_bytes())))
}
