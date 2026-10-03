//! 完成连接、迁移与 hydration 的 Store 能力集合与启动屏障。

use gateway_core::account::ProviderAccountStore;
use gateway_core::provider_ports::ProviderCooldownPort;

use super::*;

/// 已完成连接、迁移与 hydration 的 Store 能力集合。
pub struct StoreBundle {
    admin_ports: AdminStorePorts,
    core_ports: CoreStorePorts,
    provider_ports: ProviderStorePorts,
    retention: Arc<dyn gateway_admin::ports::retention::RetentionStore>,
    worker_leader_lease: Arc<dyn WorkerLeaderLeasePort>,
    health_probes: Vec<Arc<dyn HealthProbe>>,
    worker_contributions: Vec<WorkerContribution>,
    command_writers: Option<CommandStoreWriters>,
    command_drain: Option<CommandStoreDrain>,
}

impl StoreBundle {
    #[must_use]
    pub fn admin_ports(&self) -> AdminStorePorts {
        self.admin_ports.clone()
    }

    #[must_use]
    pub fn core_ports(&self) -> CoreStorePorts {
        self.core_ports.clone()
    }

    #[must_use]
    pub fn provider_ports(&self) -> ProviderStorePorts {
        self.provider_ports.clone()
    }

    #[must_use]
    pub fn retention(&self) -> Arc<dyn gateway_admin::ports::retention::RetentionStore> {
        Arc::clone(&self.retention)
    }

    #[must_use]
    pub fn worker_leader_lease(&self) -> Arc<dyn WorkerLeaderLeasePort> {
        Arc::clone(&self.worker_leader_lease)
    }

    #[must_use]
    pub fn health_probes(&self) -> Vec<Arc<dyn HealthProbe>> {
        self.health_probes.clone()
    }

    pub fn take_worker_contributions(&mut self) -> Vec<WorkerContribution> {
        std::mem::take(&mut self.worker_contributions)
    }

    /// 在插件命令真正执行前启动四个必要写泵；不注册任何后台业务 Worker。
    pub fn start_command_line_writes(&mut self) -> Result<(), CommandStoreDrainError> {
        if self.command_drain.is_some() {
            return Err(CommandStoreDrainError);
        }
        let writers = self.command_writers.take().ok_or(CommandStoreDrainError)?;
        self.command_drain = Some(writers.start());
        Ok(())
    }

    /// 命令成功、失败或取消后有界排空账本、Key 使用与准入释放。
    pub async fn shutdown_command_line_writes(&mut self) -> Result<(), CommandStoreDrainError> {
        match self.command_drain.take() {
            Some(drain) => drain.shutdown().await,
            None if self.command_writers.is_some() => Ok(()),
            None => Err(CommandStoreDrainError),
        }
    }
}

#[derive(Clone, Copy)]
enum StoreMode {
    Runtime,
    CommandLine,
}

/// 在返回 Bundle 前完成全部 Store 启动屏障。
pub async fn initialize(config: StoreConfig) -> StoreResult<StoreBundle> {
    connect(config, false, StoreMode::Runtime).await
}

/// CLI 命令使用同一有界写队列，但不贡献恢复、保留、leader 或维护 Worker。
pub async fn initialize_command_line(config: StoreConfig) -> StoreResult<StoreBundle> {
    connect(config, false, StoreMode::CommandLine).await
}

/// 读取现有安装描述，不迁移数据库；数据库连接和备份暂存区都不会被写入。
pub async fn initialize_read_only(config: StoreConfig) -> StoreResult<StoreBundle> {
    connect(config, true, StoreMode::Runtime).await
}

async fn connect(
    mut config: StoreConfig,
    read_only: bool,
    mode: StoreMode,
) -> StoreResult<StoreBundle> {
    config.validate_resolved()?;
    match config.resolved_backend()? {
        ResolvedStoreBackend::PostgresRedis {
            database_url,
            redis_url,
        } => connect_postgres_redis(config, read_only, mode, database_url, redis_url).await,
        ResolvedStoreBackend::Sqlite { path, options } => {
            connect_sqlite(&config, read_only, mode, &path, &options).await
        }
    }
}

async fn connect_sqlite(
    config: &StoreConfig,
    read_only: bool,
    mode: StoreMode,
    path: &std::path::Path,
    options: &SqliteStoreConfig,
) -> StoreResult<StoreBundle> {
    let pool = if read_only {
        sqlite::connect_read_only(path, options).await?
    } else {
        sqlite::connect_and_migrate(path, options).await?
    };

    let provider_accounts = Arc::new(sqlite::SqliteProviderAccountRepository::new(pool.clone()));
    let cooldowns = Arc::new(sqlite::SqliteProviderCooldownRepository::new(pool.clone()));
    let credential_leases = sqlite::SqliteCredentialLeaseRepository::new(pool.clone());
    let admin_account_runtime = Arc::new(AccountRuntimeStoreAdapter::new(
        Arc::clone(&cooldowns) as Arc<dyn AccountRuntimeStateRepository>,
        Arc::new(credential_leases.clone()) as Arc<dyn CredentialLeaseRepository>,
    ));
    let provider_leases = Arc::new(sqlite::SqliteProviderLeaseCoordinator::new(
        credential_leases,
    ));
    let session_affinity: Arc<dyn gateway_core::provider_ports::ProviderSessionAffinityPort> =
        Arc::new(sqlite::SqliteProviderSessionAffinityRepository::new(
            pool.clone(),
        ));
    let runtime_cache = Arc::new(sqlite::SqliteProviderRuntimeCache::new(pool.clone()));
    let runtime_policy = Arc::new(sqlite::SqliteRuntimeSettingsRepository::new(pool.clone()));
    let oauth_pending = Arc::new(LocalOAuthPendingFlowRepository::default());

    let plugins = Arc::new(sqlite::SqlitePluginStore::new(pool.clone()));
    let admin_ports = AdminStorePorts::new(
        AdminAccountStorePorts::new(
            Arc::new(sqlite::SqliteAdminAccountStore::new(pool.clone())),
            admin_account_runtime,
            Arc::new(sqlite::SqliteAccountGroupRepository::new(pool.clone())),
            Arc::new(sqlite::SqliteProxyRepository::new(pool.clone())),
        ),
        sqlite::admin_auth_store(pool.clone()),
        Arc::new(sqlite::SqliteAdminClientKeyStore::new(pool.clone())),
        Arc::new(
            sqlite::SqliteAdminObservabilityStore::new(
                pool.clone(),
                Arc::clone(&cooldowns) as Arc<dyn ProviderCooldownPort>,
            )
            .with_timezone(config.timezone),
        ),
        Arc::new(AdminSettingsStoreAdapter {
            control_plane: Arc::new(sqlite::SqliteAdminSettingsRepository::new(pool.clone())),
        }),
        sqlite_backup_ports(pool.clone(), config, read_only)?,
        plugins.clone(),
        plugins,
        Arc::new(sqlite::SqlitePluginResourceStore::new(pool.clone())),
    );

    let execution_repository = Arc::new(sqlite::SqliteExecutionStore::new(pool.clone()));
    let (execution, execution_writer) =
        postgres::BufferedExecutionStore::new(Arc::clone(&execution_repository));
    let execution = Arc::new(execution);
    let (client_key_usage, client_key_usage_writer) =
        sqlite::SqliteClientApiKeyUsageSink::new(pool.clone());
    let retention = Arc::new(sqlite::SqliteRetentionRepository::new(pool.clone()));
    let admissions = Arc::new(LocalClientAdmissionPort::default());
    let continuation: Arc<dyn gateway_core::engine::continuation::NativeContinuationPort> =
        Arc::new(LocalNativeContinuationRepository::default());
    let snapshots = Arc::new(sqlite::SqliteRuntimeSnapshotRepository::new(pool.clone()));
    let snapshot_changes = Arc::new(sqlite::SqliteRuntimeChangeRepository::new(pool.clone()));
    let core_ports = CoreStorePorts::new(
        execution,
        (
            admissions,
            Arc::new(sqlite::SqliteClientAdmissionRecoveryRepository::new(
                pool.clone(),
            )),
        ),
        continuation,
        (snapshots, snapshot_changes),
        Arc::new(client_key_usage),
    )
    .with_budget(Arc::new(sqlite::SqliteClientBudgetStore::new(
        pool.clone(),
        config.timezone,
    )))
    .with_session_affinity(Arc::clone(&session_affinity));

    let provider_ports = ProviderStorePorts::new(
        provider_accounts,
        provider_leases,
        session_affinity,
        Arc::new(sqlite::SqliteProviderSessionExclusionRepository::new(
            pool.clone(),
        )),
        runtime_cache.clone(),
        runtime_cache.clone(),
        runtime_cache,
        cooldowns,
        runtime_policy,
        oauth_pending,
    );
    let worker_leader_lease = Arc::new(LocalWorkerLeaderLeasePort::default());
    let health_probes: Vec<Arc<dyn HealthProbe>> = vec![Arc::new(SqliteHealthProbe::new(
        pool.clone(),
        options.max_connections,
    ))];
    let (worker_contributions, command_writers) = match mode {
        StoreMode::Runtime if read_only => (Vec::new(), None),
        StoreMode::Runtime => (
            sqlite_store_worker_contributions(
                pool,
                execution_repository,
                Box::new(execution_writer),
                Box::new(client_key_usage_writer),
            )?,
            None,
        ),
        StoreMode::CommandLine => {
            let execution_idle = execution_writer.idle();
            (
                Vec::new(),
                Some(CommandStoreWriters {
                    writers: vec![
                        Box::new(execution_writer) as Box<dyn gateway_core::task::DaemonTask>,
                        Box::new(client_key_usage_writer),
                    ],
                    execution_idle: Some(execution_idle),
                }),
            )
        }
    };
    Ok(StoreBundle {
        admin_ports,
        core_ports,
        provider_ports,
        retention,
        worker_leader_lease,
        health_probes,
        worker_contributions,
        command_writers,
        command_drain: None,
    })
}

async fn connect_postgres_redis(
    config: StoreConfig,
    read_only: bool,
    mode: StoreMode,
    database_url: String,
    redis_url: String,
) -> StoreResult<StoreBundle> {
    const REDIS_NAMESPACE: &str = "codex-proxy-rs";

    let pool = if read_only {
        postgres::connect_read_only(&database_url, config.pool).await?
    } else {
        postgres::connect_and_migrate(&database_url, config.pool).await?
    };
    let observability_query_budget = postgres::ObservabilityQueryBudget::try_new(
        config.pool.observability_max_connections(),
        config.pool.acquire_timeout(),
    )?;
    let redis_client =
        ::redis::Client::open(redis_url).map_err(|_| redis_unavailable("create Redis client"))?;
    let redis_connection = redis_client
        .get_connection_manager()
        .await
        .map_err(|_| redis_unavailable("connect Redis manager"))?;

    let provider_accounts = Arc::new(postgres::PgProviderAccountRepository::new(pool.clone()));
    let cooldowns = Arc::new(redis::RedisCredentialCooldownRepository::new(
        redis_connection.clone(),
        REDIS_NAMESPACE,
    )?);
    let account_store: Arc<dyn ProviderAccountStore> = provider_accounts;

    let credential_leases =
        redis::RedisCredentialLeaseRepository::new(redis_connection.clone(), REDIS_NAMESPACE)?;
    let admin_account_runtime = Arc::new(redis::RedisAdminAccountRuntimeStore::new(
        cooldowns.as_ref().clone(),
        credential_leases.clone(),
    ));
    let provider_leases = Arc::new(redis::RedisProviderLeaseCoordinator::new(
        credential_leases.clone(),
    ));
    let provider_session_affinity: Arc<
        dyn gateway_core::provider_ports::ProviderSessionAffinityPort,
    > = Arc::new(redis::RedisProviderSessionAffinityRepository::new(
        redis_connection.clone(),
        REDIS_NAMESPACE,
    )?);
    let credential_state = Arc::new(redis::RedisCredentialStateRepository::new(
        redis_connection.clone(),
        REDIS_NAMESPACE,
    )?);
    let artifact_profiles = Arc::new(redis::RedisProviderArtifactProfileRepository::new(
        redis_connection.clone(),
        REDIS_NAMESPACE,
    )?);
    let runtime_policy = Arc::new(postgres::PgRuntimeSettingsRepository::new(pool.clone()));
    let oauth_pending = Arc::new(redis::RedisOAuthPendingFlowRepository::new(
        redis_connection.clone(),
        REDIS_NAMESPACE,
    )?);

    let plugins = Arc::new(postgres::PgPluginStore::new(pool.clone()));
    let admin_ports = AdminStorePorts::new(
        AdminAccountStorePorts::new(
            Arc::new(postgres::PgAdminAccountStore::new(
                pool.clone(),
                Some(Arc::clone(&cooldowns) as Arc<dyn ProviderCooldownPort>),
                observability_query_budget.clone(),
            )),
            admin_account_runtime,
            Arc::new(
                postgres::PgAccountGroupRepository::new(pool.clone())
                    .with_timezone(config.timezone),
            ),
            Arc::new(postgres::PgProxyRepository::new(pool.clone())),
        ),
        Arc::new(AuthStoreAdapter {
            keys: Arc::new(postgres::PgAdminClientKeyStore::new(pool.clone())),
            security: Arc::new(postgres::PgAdminSecurityAuditRepository::new(pool.clone())),
            settings: Arc::new(postgres::PgRuntimeSettingsRepository::new(pool.clone())),
            state: Arc::new(redis::RedisAuthStateRepository::new(
                redis_connection.clone(),
                REDIS_NAMESPACE,
            )?),
        }),
        Arc::new(postgres::PgAdminClientKeyStore::new(pool.clone())),
        Arc::new(
            postgres::PgAdminObservabilityStore::new(
                pool.clone(),
                Some(Arc::new(credential_leases.clone()) as Arc<dyn CredentialLeaseRepository>),
                Some(Arc::clone(&cooldowns) as Arc<dyn ProviderCooldownPort>),
                observability_query_budget,
            )
            .with_timezone(config.timezone),
        ),
        Arc::new(AdminSettingsStoreAdapter {
            control_plane: Arc::new(postgres::PgControlPlaneRepository::new(pool.clone())),
        }),
        backup_ports(pool.clone(), &config, read_only)?,
        plugins.clone(),
        plugins.clone(),
        plugins,
    );

    let execution_repository = Arc::new(postgres::PgExecutionStore::new(pool.clone()));
    let (execution, execution_writer) =
        postgres::BufferedExecutionStore::new(Arc::clone(&execution_repository));
    let execution = Arc::new(execution);
    let (client_key_usage, client_key_usage_writer) =
        postgres::PgClientApiKeyUsageSink::new(pool.clone());
    let retention = Arc::new(postgres::PgRetentionRepository::new(pool.clone()));
    let admissions: Arc<dyn gateway_core::engine::admission::ClientAdmissionPort> = Arc::new(
        redis::RedisClientAdmissionRepository::new(redis_connection.clone(), REDIS_NAMESPACE)?,
    );
    // Continuation affinity 是下一轮请求的路由事实，Core 必须直接等待 Redis 确认。
    let continuation: Arc<dyn gateway_core::engine::continuation::NativeContinuationPort> =
        Arc::new(redis::RedisNativeContinuationRepository::new(
            redis_connection.clone(),
            REDIS_NAMESPACE,
        )?);
    let (admissions, admission_release_writer) =
        redis::BufferedClientAdmissionPort::new(admissions);
    let core_ports = CoreStorePorts::new(
        execution,
        (
            Arc::new(admissions),
            Arc::new(postgres::PgClientAdmissionRecoveryRepository::new(
                pool.clone(),
            )),
        ),
        continuation,
        (
            Arc::new(postgres::PgRuntimeSnapshotRepository::new(pool.clone())),
            Arc::new(redis::RedisRuntimeChangeRepository::new(
                redis_client,
                REDIS_NAMESPACE,
            )?),
        ),
        Arc::new(client_key_usage),
    )
    .with_budget(Arc::new(
        postgres::PgClientBudgetStore::new(pool.clone()).with_timezone(config.timezone),
    ))
    .with_session_affinity(Arc::clone(&provider_session_affinity));

    let provider_ports = ProviderStorePorts::new(
        account_store,
        provider_leases,
        provider_session_affinity,
        Arc::new(redis::RedisProviderSessionExclusionRepository::new(
            redis_connection.clone(),
            REDIS_NAMESPACE,
        )?),
        credential_state.clone(),
        artifact_profiles,
        credential_state,
        cooldowns,
        runtime_policy,
        oauth_pending,
    );
    let worker_leader_lease = Arc::new(redis::worker_lease::RedisWorkerLeaderLeasePort::new(
        credential_leases,
    ));
    let health_probes: Vec<Arc<dyn HealthProbe>> = vec![
        Arc::new(PostgresHealthProbe::new(
            pool.clone(),
            config.pool.max_connections,
        )),
        Arc::new(RedisHealthProbe {
            connection: redis_connection,
        }),
    ];
    let (worker_contributions, command_writers) = match mode {
        StoreMode::Runtime => (
            store_worker_contributions(
                execution_repository,
                execution_writer,
                client_key_usage_writer,
                admission_release_writer,
            )?,
            None,
        ),
        StoreMode::CommandLine => {
            let execution_idle = execution_writer.idle();
            (
                Vec::new(),
                Some(CommandStoreWriters {
                    writers: vec![
                        Box::new(execution_writer) as Box<dyn gateway_core::task::DaemonTask>,
                        Box::new(client_key_usage_writer),
                        Box::new(admission_release_writer),
                    ],
                    execution_idle: Some(execution_idle),
                }),
            )
        }
    };
    Ok(StoreBundle {
        admin_ports,
        core_ports,
        provider_ports,
        retention,
        worker_leader_lease,
        health_probes,
        worker_contributions,
        command_writers,
        command_drain: None,
    })
}

/// 构造 SQLite 备份控制面的仓储、VACUUM INTO 导出器与对象存储适配器。
pub(crate) fn sqlite_backup_ports(
    pool: sqlx::SqlitePool,
    config: &StoreConfig,
    read_only: bool,
) -> StoreResult<BackupStorePorts> {
    let staging = Arc::new(if read_only {
        backup::staging::StagingArea::open_read_only(
            config.backup_staging_dir().to_path_buf(),
            backup::staging::DEFAULT_MAX_ARCHIVE_BYTES,
            StoreBackend::Sqlite,
        )?
    } else {
        backup::staging::StagingArea::open_with_backend(
            config.backup_staging_dir().to_path_buf(),
            backup::staging::DEFAULT_MAX_ARCHIVE_BYTES,
            StoreBackend::Sqlite,
        )?
    });
    let repository = Arc::new(sqlite::SqliteBackupRepository::new(pool.clone()));
    let dump = Arc::new(backup::sqlite_dump::SqliteDumpAdapter::new(pool, staging));
    let object_store = Arc::new(backup::s3::S3ObjectStoreAdapter::new());
    Ok(BackupStorePorts::new(repository, dump, object_store))
}

/// 构造 PostgreSQL 备份控制面的仓储、pg_dump 导出器与对象存储适配器。
pub(crate) fn backup_ports(
    pool: sqlx::PgPool,
    config: &StoreConfig,
    read_only: bool,
) -> StoreResult<BackupStorePorts> {
    let staging = Arc::new(if read_only {
        backup::staging::StagingArea::open_read_only(
            config.backup_staging_dir().to_path_buf(),
            backup::staging::DEFAULT_MAX_ARCHIVE_BYTES,
            StoreBackend::PostgreSql,
        )?
    } else {
        backup::staging::StagingArea::open_with_backend(
            config.backup_staging_dir().to_path_buf(),
            backup::staging::DEFAULT_MAX_ARCHIVE_BYTES,
            StoreBackend::PostgreSql,
        )?
    });
    let repository = Arc::new(postgres::PgBackupRepository::new(pool));
    let database = config.database_config()?;
    let dump = Arc::new(backup::pg_dump::PgDumpAdapter::new(
        staging,
        database.url.clone(),
        database.password.clone(),
    ));
    let object_store = Arc::new(backup::s3::S3ObjectStoreAdapter::new());
    Ok(BackupStorePorts::new(repository, dump, object_store))
}
