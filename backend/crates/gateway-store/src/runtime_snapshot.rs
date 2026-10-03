//! 两种数据库共用的运行快照合同与纯领域映射。

use std::collections::BTreeMap;

use async_trait::async_trait;

use gateway_core::{
    account::ProviderAccountId,
    routing::snapshot::{
        SnapshotAccountGroupFacts, SnapshotAccountGroupMemberFacts, SnapshotClientPolicyFacts,
        SnapshotFacts, SnapshotProviderAccountFacts, SnapshotStoreError,
    },
    routing::{AccountGroupId, ConfigRevision},
    settings::SettingsValues,
};

use crate::{Revision, StoreError, StoreResult};

#[async_trait]
pub trait RuntimeSnapshotRepository: Send + Sync {
    async fn load_runtime_snapshot(&self) -> StoreResult<RuntimeSnapshotData>;
    async fn current_config_revision(&self) -> StoreResult<Revision>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientApiKeySnapshot {
    pub request_profiles:
        BTreeMap<gateway_core::routing::ProviderKind, gateway_core::account::OpaqueProviderData>,
    pub id: gateway_core::policy::ClientApiKeyId,
    pub plaintext_key: gateway_core::policy::PlaintextClientApiKey,
    pub group_ids: Vec<AccountGroupId>,
    pub limits: gateway_core::policy::RateLimits,
}

impl ClientApiKeySnapshot {
    pub(crate) fn from_persisted(
        id: String,
        key: String,
        group_ids: Vec<String>,
        max_concurrency: i64,
        requests_per_minute: i64,
    ) -> StoreResult<Self> {
        Ok(Self {
            request_profiles: BTreeMap::new(),
            id: gateway_core::policy::ClientApiKeyId::new(id)
                .map_err(|_| invalid("persisted key ID is invalid"))?,
            plaintext_key: gateway_core::policy::PlaintextClientApiKey::new(key)
                .map_err(|_| invalid("persisted plaintext key is invalid"))?,
            group_ids: group_ids
                .into_iter()
                .map(|id| {
                    AccountGroupId::new(id).map_err(|_| invalid("persisted group ID is invalid"))
                })
                .collect::<StoreResult<Vec<_>>>()?,
            limits: gateway_core::policy::RateLimits {
                max_concurrency: to_u64(max_concurrency)?,
                requests_per_minute: to_u64(requests_per_minute)?,
            },
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotRuntimeSettings {
    pub pricing: gateway_core::metering::PricingOverrides,
    pub request_profiles:
        BTreeMap<gateway_core::routing::ProviderKind, gateway_core::account::OpaqueProviderData>,
    pub request_location_enabled: bool,
    pub request_location: gateway_core::account::RequestLocation,
    pub refresh_margin_seconds: u64,
    pub refresh_concurrency: u32,
    pub max_concurrent_per_account: u32,
    pub request_interval_ms: u64,
    pub max_waiting_per_key: u32,
    pub max_waiting_per_account: u32,
    pub concurrency_wait_timeout_seconds: u32,
    pub openai_guardian_reserved_concurrency: u32,
    pub responses_max_decompressed_body_bytes: u64,
    pub smart_scheduling: gateway_core::account::SmartSchedulingConfig,
    pub rotation_strategy: String,
    pub model_mappings: BTreeMap<String, String>,
    pub min_codex_desktop_version: Option<String>,
    pub min_codex_cli_version: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeSnapshotData {
    pub config_revision: Revision,
    pub observed_current_revision: Revision,
    pub settings: SnapshotRuntimeSettings,
    pub client_api_keys: Vec<ClientApiKeySnapshot>,
    pub account_groups: Vec<SnapshotAccountGroupData>,
    pub provider_accounts: Vec<SnapshotProviderAccountData>,
    pub group_memberships: Vec<SnapshotGroupMembershipData>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotAccountGroupData {
    pub disable_fast: bool,
    pub id: AccountGroupId,
    pub name: String,
    pub enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotProviderAccountData {
    pub id: String,
    pub provider_kind: String,
    pub model_access: gateway_core::account::AccountModelAccess,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotGroupMembershipData {
    pub group_id: AccountGroupId,
    pub account_id: String,
}

/// 将持久层快照合同映射为 Core 使用的不可变运行事实。
pub(crate) fn snapshot_data_into_facts(
    data: RuntimeSnapshotData,
) -> Result<SnapshotFacts, SnapshotStoreError> {
    let config_revision = core_revision(data.config_revision)?;
    let observed_current_revision = core_revision(data.observed_current_revision)?;
    let settings = SettingsValues::new(
        data.settings.max_concurrent_per_account,
        data.settings.request_interval_ms,
        data.settings.rotation_strategy,
        data.settings.model_mappings,
        data.settings.min_codex_desktop_version,
        data.settings.min_codex_cli_version,
    )
    .with_responses_max_decompressed_body_bytes(data.settings.responses_max_decompressed_body_bytes)
    .with_openai_guardian_reserved_concurrency(data.settings.openai_guardian_reserved_concurrency)
    .with_smart_scheduling(data.settings.smart_scheduling)
    .with_request_profiles(data.settings.request_profiles)
    .with_pricing(data.settings.pricing)
    .with_request_location(
        data.settings.request_location,
        data.settings.request_location_enabled,
    )
    .with_concurrency_queues(
        data.settings.max_waiting_per_key,
        data.settings.max_waiting_per_account,
        data.settings.concurrency_wait_timeout_seconds,
    );
    let client_policies = data
        .client_api_keys
        .into_iter()
        .map(|key| {
            SnapshotClientPolicyFacts::new(key.id, key.plaintext_key, key.group_ids, key.limits)
                .with_request_profiles(key.request_profiles)
        })
        .collect();
    let account_groups = data
        .account_groups
        .into_iter()
        .map(|group| {
            SnapshotAccountGroupFacts::new(group.id, group.name, group.enabled)
                .with_disable_fast(group.disable_fast)
        })
        .collect();
    let provider_accounts = data
        .provider_accounts
        .into_iter()
        .map(|account| {
            ProviderAccountId::new(account.id)
                .map(|id| {
                    SnapshotProviderAccountFacts::new(id, account.provider_kind)
                        .with_model_access(account.model_access)
                })
                .map_err(|_| SnapshotStoreError::unavailable())
        })
        .collect::<Result<Vec<_>, _>>()?;
    let group_memberships = data
        .group_memberships
        .into_iter()
        .map(|membership| {
            ProviderAccountId::new(membership.account_id)
                .map(|account_id| {
                    SnapshotAccountGroupMemberFacts::new(membership.group_id, account_id)
                })
                .map_err(|_| SnapshotStoreError::unavailable())
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(SnapshotFacts::new(
        config_revision,
        observed_current_revision,
        settings,
        client_policies,
        account_groups,
        provider_accounts,
        group_memberships,
    ))
}

pub(crate) fn core_revision(revision: Revision) -> Result<ConfigRevision, SnapshotStoreError> {
    ConfigRevision::new(revision.get()).map_err(|_| SnapshotStoreError::unavailable())
}

pub(crate) fn revision_from_i64(value: i64) -> StoreResult<Revision> {
    Revision::new(to_u64(value)?)
}

pub(crate) fn to_u64(value: i64) -> StoreResult<u64> {
    u64::try_from(value).map_err(|_| invalid("numeric snapshot field is negative"))
}

pub(crate) fn to_u32(value: i64) -> StoreResult<u32> {
    u32::try_from(value).map_err(|_| invalid("numeric snapshot field is outside u32"))
}

pub(crate) fn decode_request_profiles(
    profiles: BTreeMap<String, serde_json::Map<String, serde_json::Value>>,
) -> StoreResult<
    BTreeMap<gateway_core::routing::ProviderKind, gateway_core::account::OpaqueProviderData>,
> {
    profiles
        .into_iter()
        .map(|(kind, document)| {
            Ok((
                gateway_core::routing::ProviderKind::new(kind)
                    .map_err(|_| invalid("invalid request profile provider"))?,
                gateway_core::account::OpaqueProviderData::new(document),
            ))
        })
        .collect()
}

fn invalid(message: &str) -> StoreError {
    StoreError::InvalidData {
        entity: "runtime snapshot",
        message: message.to_owned(),
    }
}
