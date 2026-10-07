//! Provider 与 Core 共用的运行设置模型、校验和存储端口。

use std::{collections::BTreeMap, fmt};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use gateway_core::{account::RotationStrategy, policy::CodexClientVersion};

use crate::{Revision, StoreError, StoreResult};

#[derive(Clone, PartialEq, Eq)]
pub struct RuntimeSettings {
    pub request_profiles:
        BTreeMap<gateway_core::routing::ProviderKind, gateway_core::account::OpaqueProviderData>,
    pub config_revision: Revision,
    pub admin_api_key: Option<String>,
    pub refresh_margin_seconds: u64,
    pub refresh_concurrency: u32,
    pub max_concurrent_per_account: u32,
    pub request_interval_ms: u64,
    pub max_waiting_per_key: u32,
    pub max_waiting_per_account: u32,
    pub concurrency_wait_timeout_seconds: u32,
    pub openai_guardian_reserved_concurrency: u32,
    pub openai_account_affinity: gateway_core::account::AccountAffinity,
    pub max_account_rotations: u32,
    pub openai_session_affinity_ttl_hours: u32,
    pub responses_max_decompressed_body_bytes: u64,
    pub smart_scheduling: gateway_core::account::SmartSchedulingConfig,
    pub rotation_strategy: String,
    pub request_location_enabled: bool,
    pub request_location: gateway_core::account::RequestLocation,
    pub model_mappings: BTreeMap<String, String>,
    pub min_codex_desktop_version: Option<String>,
    pub min_codex_cli_version: Option<String>,
    pub usage_retention_days: u32,
    pub ops_event_retention_days: u32,
    pub audit_retention_days: u32,
    pub account_auto_freeze_enabled: bool,
    pub account_auto_freeze_threshold: u32,
    pub account_auto_freeze_window_seconds: u64,
    pub account_auto_freeze_duration_seconds: u64,
    pub account_auto_freeze_probe_enabled: bool,
    pub account_auto_freeze_probe_model: Option<String>,
    pub account_auto_freeze_adaptive_concurrency: bool,
    pub account_warmup_enabled: bool,
    pub account_warmup_schedule_time: String,
    pub account_warmup_model: Option<String>,
    pub updated_at: DateTime<Utc>,
}

impl fmt::Debug for RuntimeSettings {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RuntimeSettings")
            .field("request_profile_count", &self.request_profiles.len())
            .field("config_revision", &self.config_revision)
            .field(
                "admin_api_key",
                &self.admin_api_key.as_ref().map(|_| "[REDACTED]"),
            )
            .field("refresh_margin_seconds", &self.refresh_margin_seconds)
            .field("refresh_concurrency", &self.refresh_concurrency)
            .field(
                "max_concurrent_per_account",
                &self.max_concurrent_per_account,
            )
            .field("request_interval_ms", &self.request_interval_ms)
            .field("openai_account_affinity", &self.openai_account_affinity)
            .field("max_account_rotations", &self.max_account_rotations)
            .field(
                "openai_session_affinity_ttl_hours",
                &self.openai_session_affinity_ttl_hours,
            )
            .field("rotation_strategy", &self.rotation_strategy)
            .field("request_location_enabled", &self.request_location_enabled)
            .field("request_location", &self.request_location)
            .field("model_mappings", &self.model_mappings)
            .field("min_codex_desktop_version", &self.min_codex_desktop_version)
            .field("min_codex_cli_version", &self.min_codex_cli_version)
            .field("usage_retention_days", &self.usage_retention_days)
            .field("ops_event_retention_days", &self.ops_event_retention_days)
            .field("audit_retention_days", &self.audit_retention_days)
            .field(
                "account_auto_freeze_enabled",
                &self.account_auto_freeze_enabled,
            )
            .field(
                "account_auto_freeze_threshold",
                &self.account_auto_freeze_threshold,
            )
            .field(
                "account_auto_freeze_window_seconds",
                &self.account_auto_freeze_window_seconds,
            )
            .field(
                "account_auto_freeze_duration_seconds",
                &self.account_auto_freeze_duration_seconds,
            )
            .field(
                "account_auto_freeze_probe_enabled",
                &self.account_auto_freeze_probe_enabled,
            )
            .field(
                "account_auto_freeze_probe_model",
                &self.account_auto_freeze_probe_model,
            )
            .field(
                "account_auto_freeze_adaptive_concurrency",
                &self.account_auto_freeze_adaptive_concurrency,
            )
            .field("account_warmup_enabled", &self.account_warmup_enabled)
            .field(
                "account_warmup_schedule_time",
                &self.account_warmup_schedule_time,
            )
            .field("account_warmup_model", &self.account_warmup_model)
            .field("updated_at", &self.updated_at)
            .finish()
    }
}

#[derive(Clone)]
pub struct RuntimeSettingsUpdate {
    pub request_profile_updates: BTreeMap<
        gateway_core::routing::ProviderKind,
        Option<gateway_core::account::OpaqueProviderData>,
    >,
    pub refresh_margin_seconds: u64,
    pub refresh_concurrency: u32,
    pub max_concurrent_per_account: u32,
    pub request_interval_ms: u64,
    pub max_waiting_per_key: u32,
    pub max_waiting_per_account: u32,
    pub concurrency_wait_timeout_seconds: u32,
    pub openai_guardian_reserved_concurrency: u32,
    pub openai_account_affinity: gateway_core::account::AccountAffinity,
    pub max_account_rotations: u32,
    pub openai_session_affinity_ttl_hours: u32,
    pub responses_max_decompressed_body_bytes: u64,
    pub smart_scheduling: gateway_core::account::SmartSchedulingConfig,
    pub rotation_strategy: String,
    pub request_location_enabled: bool,
    pub request_location: gateway_core::account::RequestLocation,
    pub model_mappings: BTreeMap<String, String>,
    pub min_codex_desktop_version: Option<String>,
    pub min_codex_cli_version: Option<String>,
    pub usage_retention_days: u32,
    pub ops_event_retention_days: u32,
    pub audit_retention_days: u32,
    pub account_auto_freeze_enabled: bool,
    pub account_auto_freeze_threshold: u32,
    pub account_auto_freeze_window_seconds: u64,
    pub account_auto_freeze_duration_seconds: u64,
    pub account_auto_freeze_probe_enabled: bool,
    pub account_auto_freeze_probe_model: Option<String>,
    pub account_auto_freeze_adaptive_concurrency: bool,
    pub account_warmup_enabled: bool,
    pub account_warmup_schedule_time: String,
    pub account_warmup_model: Option<String>,
}

impl fmt::Debug for RuntimeSettingsUpdate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RuntimeSettingsUpdate")
            .field("rotation_strategy", &self.rotation_strategy)
            .field("request_location_enabled", &self.request_location_enabled)
            .field("request_location", &self.request_location)
            .field("model_mappings", &self.model_mappings)
            .finish_non_exhaustive()
    }
}

impl RuntimeSettingsUpdate {
    pub fn validate(&self) -> StoreResult<()> {
        if self.request_location.validate().is_err()
            || self.responses_max_decompressed_body_bytes == 0
            || isize::try_from(self.responses_max_decompressed_body_bytes).is_err()
            || self.refresh_margin_seconds == 0
            || self.refresh_concurrency == 0
            || self.max_waiting_per_key > 1_000
            || self.max_waiting_per_account > 1_000
            || !(1..=120).contains(&self.concurrency_wait_timeout_seconds)
            || self.max_account_rotations > gateway_core::account::MAX_ACCOUNT_ROTATIONS
            || !(1..=gateway_core::account::MAX_SESSION_AFFINITY_TTL_HOURS)
                .contains(&self.openai_session_affinity_ttl_hours)
            || self.usage_retention_days < 31
            || self.ops_event_retention_days == 0
            || self.audit_retention_days == 0
            || !(2..=1_000).contains(&self.account_auto_freeze_threshold)
            || !(60..=3_600).contains(&self.account_auto_freeze_window_seconds)
            || !(300..=604_800).contains(&self.account_auto_freeze_duration_seconds)
            || !valid_model_mappings(&self.model_mappings)
            || !valid_client_version(self.min_codex_desktop_version.as_deref())
            || !valid_client_version(self.min_codex_cli_version.as_deref())
            || !valid_probe_model(self.account_auto_freeze_probe_model.as_deref())
            || !gateway_core::provider_ports::valid_warmup_schedule_time(
                &self.account_warmup_schedule_time,
            )
            || !valid_probe_model(self.account_warmup_model.as_deref())
            || (self.account_warmup_enabled && self.account_warmup_model.is_none())
            || RotationStrategy::parse(&self.rotation_strategy).is_none()
            || self.request_profile_updates.len() > 256
            || self
                .request_profile_updates
                .values()
                .flatten()
                .any(|profile| {
                    serde_json::to_vec(profile.expose_to_provider())
                        .map_or(true, |encoded| encoded.len() > 64 * 1024)
                })
        {
            return Err(StoreError::InvalidData {
                source: None,
                entity: "runtime settings",
                message: "settings violate the frozen runtime constraints".to_owned(),
            });
        }
        Ok(())
    }
}

#[async_trait]
pub trait RuntimeSettingsRepository: Send + Sync {
    async fn load_runtime_settings(&self) -> StoreResult<RuntimeSettings>;

    async fn update_runtime_settings(&self, update: RuntimeSettingsUpdate)
    -> StoreResult<Revision>;
}

fn valid_model_mappings(mappings: &BTreeMap<String, String>) -> bool {
    mappings.len() <= 512
        && mappings.iter().all(|(requested, upstream)| {
            valid_model_name(requested, 256) && valid_model_name(upstream, 256)
        })
}

fn valid_model_name(value: &str, max_len: usize) -> bool {
    !value.is_empty()
        && value.len() <= max_len
        && !value.bytes().any(|byte| byte.is_ascii_control())
}

fn valid_client_version(value: Option<&str>) -> bool {
    value.is_none_or(|value| CodexClientVersion::parse(value).is_ok())
}

fn valid_probe_model(value: Option<&str>) -> bool {
    value.is_none_or(|value| {
        !value.is_empty()
            && value.len() <= 128
            && value == value.trim()
            && !value.bytes().any(|byte| byte.is_ascii_control())
    })
}
