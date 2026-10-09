//! 多 Provider 网关的 PostgreSQL 持久化与 Redis 协调 adapter
//!
//! 业务规则与 port 由 `gateway-core` / `gateway-admin` 拥有
//! 本 crate 只负责把 PostgreSQL 业务表
//! 和可丢失 Redis 状态映射为明确的基础设施操作

use std::sync::Arc;
use std::time::{Duration, SystemTime};
use std::{fmt, num::NonZeroU64};

use gateway_admin::model::auth::{AdminAuditEvent as AdminAuditModel, AuthSession, SessionSubject};
use gateway_admin::model::settings::{
    AdminApiKey, AdminApiKeyMutation, ModelMappings, ReplaceRuntimeSettings,
    RotationStrategy as AdminRotationStrategy, RuntimeSettings as AdminRuntimeSettings,
};
use gateway_admin::model::{MutationContext, Revision as AdminRevision};
use gateway_admin::ports::backup::BackupStorePorts;
use gateway_admin::ports::store::{
    AdminAccountStorePorts, AdminStoreError, AdminStoreErrorKind, AdminStorePorts,
    AdminStoreResult, AuthStore, SettingsStore,
};
use gateway_core::CoreStorePorts;
use gateway_core::health::{HealthProbe, HealthState};
use gateway_core::provider_ports::ProviderStorePorts;
use gateway_core::task::{
    DaemonRestartPolicy, ScheduledTask, WorkerContribution, WorkerCycleContext, WorkerId,
    WorkerKind, WorkerLeaderLeasePort, WorkerLeaseRequest, WorkerRegistration, WorkerRunnable,
    WorkerSchedule, WorkerTaskError,
};
use serde::Deserialize;
use serde_json::{Map, Value};

mod admin_adapter;
mod admin_audit;
mod admission_recovery;
mod billing;
pub use admission_recovery::{
    ClientAdmissionRecentRequest, ClientAdmissionRecovery, ClientAdmissionRecoveryRepository,
    ClientAdmissionRunningRequest,
};
mod bundle;
mod client_key_usage;
mod config;
mod coordination;
mod lease_renewal;
mod local_runtime;
pub use local_runtime::{
    LocalClientAdmissionPort, LocalNativeContinuationRepository, LocalWorkerLeaderLeasePort,
};
mod value;
mod workers;

pub mod backup;
pub(crate) mod execution;
mod plugin_state_rules;
pub mod postgres;
mod pricing_validation;
pub mod redis;
mod request_observation;
pub(crate) mod runtime_change;
pub(crate) mod runtime_settings;
pub use runtime_settings::{RuntimeSettingsRepository, RuntimeSettingsUpdate};
pub(crate) mod runtime_snapshot;
pub mod sqlite;
pub use runtime_snapshot::RuntimeSnapshotRepository;

pub(crate) use admin_adapter::*;
pub(crate) use admin_audit::*;
pub use bundle::*;
pub use config::*;
pub use coordination::*;
pub use value::*;
pub(crate) use workers::*;
pub use workers::{CommandStoreDrainError, PostgresHealthProbe, SqliteHealthProbe};
