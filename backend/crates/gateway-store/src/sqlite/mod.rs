//! SQLite 文件数据库的连接、迁移与 SQLite 专属协调表。

use std::{path::Path, time::Duration};

mod account_groups;
mod admin_accounts;
mod admin_client_keys;
mod admin_observability;
mod admin_security_audit;
mod admin_settings;
pub(crate) use admin_security_audit::append_admin_audit_event_in_transaction;
mod admission_recovery;
mod backup;
mod client_budgets;
mod client_keys;
pub mod coordination;
mod execution;
#[cfg(unix)]
mod file_permissions;
mod name_key;
mod name_key_migration;
mod observability;
mod plugin_artifacts;
mod plugin_distribution;
mod plugin_instances;
mod plugin_resources;
mod plugin_state;
mod plugin_store;
mod provider_accounts;
mod provider_cooldowns;
mod provider_leases;
mod provider_sessions;
mod proxies;
mod retention;
mod runtime_cache;
mod runtime_change;
mod runtime_settings;
pub(crate) mod session_cleanup;
mod snapshot;
pub use account_groups::SqliteAccountGroupRepository;
pub use admin_accounts::SqliteAdminAccountStore;
pub use admin_client_keys::SqliteAdminClientKeyStore;
pub use admin_observability::SqliteAdminObservabilityStore;
pub use admin_security_audit::SqliteAdminSecurityAuditRepository;
pub use admin_settings::SqliteAdminSettingsRepository;
pub use admission_recovery::SqliteClientAdmissionRecoveryRepository;
pub use backup::SqliteBackupRepository;
pub use client_budgets::SqliteClientBudgetStore;
pub use client_keys::{
    SqliteClientApiKeyLastUsedRepository, SqliteClientApiKeyRepository,
    SqliteClientApiKeyUsageSink, SqliteClientApiKeyUsageWriter,
};
pub use coordination::SqliteCredentialLeaseRepository;
pub use execution::SqliteExecutionStore;
pub use plugin_artifacts::SqlitePluginArtifactStore;
pub use plugin_distribution::SqlitePluginDistributionStore;
pub use plugin_instances::SqlitePluginInstanceStore;
pub use plugin_resources::SqlitePluginResourceStore;
pub use plugin_state::SqlitePluginStateStore;
pub use plugin_store::SqlitePluginStore;
pub use provider_accounts::SqliteProviderAccountRepository;
pub use provider_cooldowns::SqliteProviderCooldownRepository;
pub use provider_leases::SqliteProviderLeaseCoordinator;
pub use provider_sessions::{
    SqliteProviderSessionAffinityRepository, SqliteProviderSessionExclusionRepository,
};
pub use proxies::SqliteProxyRepository;
pub use retention::SqliteRetentionRepository;
pub use runtime_cache::SqliteProviderRuntimeCache;
pub use runtime_change::SqliteRuntimeChangeRepository;
pub use runtime_settings::SqliteRuntimeSettingsRepository;
pub use snapshot::SqliteRuntimeSnapshotRepository;
pub mod value;

use sqlx::{
    SqlitePool, Transaction,
    migrate::Migrator,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
};

use crate::{SqliteStoreConfig, StoreBackend, StoreError, StoreResult};

/// 构造 SQLite 管理设置端口；业务代码只接收 gateway-admin 领域接口。
#[must_use]
pub fn admin_settings_store(
    pool: SqlitePool,
) -> std::sync::Arc<dyn gateway_admin::ports::store::SettingsStore> {
    std::sync::Arc::new(crate::AdminSettingsStoreAdapter {
        control_plane: std::sync::Arc::new(SqliteAdminSettingsRepository::new(pool)),
    })
}

/// 构造使用 SQLite 安全审计和进程内会话状态的 Admin 认证端口。
#[must_use]
pub fn admin_auth_store(
    pool: SqlitePool,
) -> std::sync::Arc<dyn gateway_admin::ports::store::AuthStore> {
    let keys: std::sync::Arc<dyn crate::ClientKeyEnabledRepository> =
        std::sync::Arc::new(SqliteClientApiKeyRepository::new(pool.clone()));
    std::sync::Arc::new(crate::AuthStoreAdapter {
        security: std::sync::Arc::new(SqliteAdminSecurityAuditRepository::new(pool.clone())),
        settings: std::sync::Arc::new(SqliteRuntimeSettingsRepository::new(pool)),
        state: std::sync::Arc::new(crate::LocalAuthStateRepository::default()),
        keys,
    })
}

/// 构造由 SQLite 共享 cooldown 与 Provider 租约支持的 Admin 账号运行态端口。
#[must_use]
pub fn account_runtime_store(
    pool: SqlitePool,
) -> std::sync::Arc<dyn gateway_admin::ports::store::AccountRuntimeStore> {
    let state: std::sync::Arc<dyn crate::AccountRuntimeStateRepository> =
        std::sync::Arc::new(SqliteProviderCooldownRepository::new(pool.clone()));
    let leases: std::sync::Arc<dyn crate::AccountRuntimeSignalRepository> =
        std::sync::Arc::new(SqliteCredentialLeaseRepository::new(pool));
    std::sync::Arc::new(crate::AccountRuntimeStoreAdapter::new(state, leases))
}

static MIGRATOR: Migrator = sqlx::migrate!("../../migrations/sqlite");

/// 创建或打开 SQLite 数据库并应用独立迁移集。
pub async fn connect_and_migrate(
    path: &Path,
    config: &SqliteStoreConfig,
) -> StoreResult<SqlitePool> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|_| sqlite_unavailable("create SQLite data directory"))?;
    }
    #[cfg(unix)]
    let prepared_path = {
        let path = path.to_path_buf();
        tokio::task::spawn_blocking(move || file_permissions::prepare(&path))
            .await
            .map_err(|_| sqlite_unavailable("prepare SQLite file permissions task"))??
    };
    #[cfg(unix)]
    let path = prepared_path.as_path();
    let pool = connect_pool(path, config, false).await?;
    let migration_result = async {
        if name_key_migration::is_pending(&pool).await? {
            name_key_migration::preflight(&pool).await?;
            MIGRATOR
                .run_to(12, &pool)
                .await
                .map_err(|error| StoreError::Unavailable {
                    backend: StoreBackend::Sqlite,
                    message: format!("apply SQLite migrations before name-key backfill: {error}"),
                    source: None,
                })?;
            name_key_migration::backfill(&pool).await?;
        }
        MIGRATOR
            .run(&pool)
            .await
            .map_err(|error| StoreError::Unavailable {
                backend: StoreBackend::Sqlite,
                message: format!("apply SQLite migrations: {error}"),
                source: None,
            })?;
        Ok::<(), StoreError>(())
    }
    .await;
    if let Err(error) = migration_result {
        pool.close().await;
        return Err(error);
    }
    Ok(pool)
}

/// 只读打开已有文件；不创建文件、不运行迁移，也不启用 WAL 写入。
pub async fn connect_read_only(path: &Path, config: &SqliteStoreConfig) -> StoreResult<SqlitePool> {
    if !path.is_file() {
        return Err(sqlite_unavailable(
            "open existing SQLite database read-only",
        ));
    }
    connect_pool(path, config, true).await
}

async fn connect_pool(
    path: &Path,
    config: &SqliteStoreConfig,
    read_only: bool,
) -> StoreResult<SqlitePool> {
    if config.max_connections == 0
        || config.acquire_timeout_seconds == 0
        || config.busy_timeout_ms == 0
    {
        return Err(StoreError::InvalidData {
            entity: "SQLite pool config",
            message: "connection limits and timeouts must be positive".to_owned(),
            source: None,
        });
    }
    let options = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(!read_only && !cfg!(unix))
        .read_only(read_only)
        .foreign_keys(true)
        .busy_timeout(Duration::from_millis(config.busy_timeout_ms));
    let options = if read_only {
        options
    } else {
        options
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
    };
    SqlitePoolOptions::new()
        .max_connections(config.max_connections)
        .acquire_timeout(Duration::from_secs(config.acquire_timeout_seconds))
        .connect_with(options)
        .await
        .map_err(|_| sqlite_unavailable("connect SQLite"))
}

pub(crate) async fn acquire_write_lock(
    transaction: &mut Transaction<'_, sqlx::Sqlite>,
) -> StoreResult<()> {
    sqlx::query("UPDATE cpr_store_write_lock SET revision = revision + 1 WHERE id = 1")
        .execute(&mut **transaction)
        .await
        .map(|_| ())
        .map_err(|error| sqlite_unavailable("acquire SQLite write lock").with_source(error))
}

pub(crate) fn sqlite_unavailable(operation: &'static str) -> StoreError {
    StoreError::Unavailable {
        backend: StoreBackend::Sqlite,
        message: operation.to_owned(),
        source: None,
    }
}

/// 在写事务中推进运行配置 revision，并防止 SQLite INTEGER 溢出。
pub(crate) async fn bump_config_revision(
    transaction: &mut Transaction<'_, sqlx::Sqlite>,
    updated_at_us: i64,
) -> StoreResult<crate::Revision> {
    let revision = sqlx::query_scalar::<_, i64>(
        "update runtime_settings
         set config_revision = config_revision + 1,
             updated_at_us = max(updated_at_us, ?1)
         where id = 1 and config_revision < 9223372036854775807
         returning config_revision",
    )
    .bind(updated_at_us)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|error| sqlite_unavailable("advance SQLite config revision").with_source(error))?
    .ok_or_else(|| StoreError::InvalidData {
        entity: "runtime settings",
        message: "config revision is missing or cannot be advanced".to_owned(),
        source: None,
    })?;
    crate::Revision::new(
        u64::try_from(revision).map_err(|_| StoreError::InvalidData {
            entity: "runtime settings",
            message: "config revision is outside the supported range".to_owned(),
            source: None,
        })?,
    )
}
