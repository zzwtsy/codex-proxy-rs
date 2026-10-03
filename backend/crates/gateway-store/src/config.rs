//! Store 启动配置、环境变量解析与校验。

use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use super::*;

pub(crate) const DATABASE_URL_ENV: &str = "CPR_DATABASE_URL";
pub(crate) const REDIS_URL_ENV: &str = "CPR_REDIS_URL";
pub(crate) const DATABASE_PASSWORD_ENV: &str = "CPR_DATABASE_PASSWORD";
pub(crate) const REDIS_PASSWORD_ENV: &str = "CPR_REDIS_PASSWORD";
pub(crate) const SQLITE_PATH_ENV: &str = "CPR_SQLITE_PATH";
pub(crate) const POSTGRES_STATEMENT_TIMEOUT: Duration = Duration::from_secs(30);
pub(crate) const POSTGRES_LOCK_TIMEOUT: Duration = Duration::from_secs(5);
pub(crate) const POSTGRES_IDLE_TRANSACTION_TIMEOUT: Duration = Duration::from_secs(30);
pub(crate) const POSTGRES_HEALTH_ATTEMPT_TIMEOUT: Duration = Duration::from_millis(450);
pub(crate) const POSTGRES_HEALTH_RETRY_DELAY: Duration = Duration::from_millis(50);

/// Store 自己拥有并校验的启动配置。
#[derive(Clone, Deserialize)]
pub struct StoreConfig {
    #[serde(skip)]
    pub(crate) timezone: gateway_core::time::DeploymentTimeZone,
    #[serde(default)]
    pub backend: StoreBackendKind,
    #[serde(default)]
    pub(crate) database: Option<StoreConnectionConfig>,
    #[serde(default)]
    pub(crate) redis: Option<StoreConnectionConfig>,
    #[serde(default)]
    pub(crate) sqlite: SqliteStoreConfig,
    #[serde(default)]
    pub(crate) pool: StorePoolConfig,
    #[serde(skip)]
    backup_staging_dir: PathBuf,
    #[serde(skip)]
    resolved_sqlite_path: Option<PathBuf>,
}

/// 后端选择使用封闭组合，避免组装出 PostgreSQL 无 Redis 或 SQLite 配 Redis 的模式。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StoreBackendKind {
    #[default]
    Postgres,
    Sqlite,
}

/// SQLite 连接预算。数据库文件路径缺省为运行数据目录下的 `codex-proxy.sqlite3`。
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct SqliteStoreConfig {
    pub path: Option<PathBuf>,
    pub max_connections: u32,
    pub acquire_timeout_seconds: u64,
    pub busy_timeout_ms: u64,
}

impl Default for SqliteStoreConfig {
    fn default() -> Self {
        Self {
            path: None,
            max_connections: 5,
            acquire_timeout_seconds: 5,
            busy_timeout_ms: 5_000,
        }
    }
}

/// PostgreSQL 连接池预算；acquire 超时决定池耗尽时快速失败而非排队积压。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct StorePoolConfig {
    pub max_connections: u32,
    pub acquire_timeout_seconds: u64,
}

impl Default for StorePoolConfig {
    fn default() -> Self {
        Self {
            max_connections: 20,
            acquire_timeout_seconds: 5,
        }
    }
}

impl StorePoolConfig {
    pub(crate) fn validate(&self) -> StoreResult<()> {
        if self.max_connections < 2 || self.acquire_timeout_seconds == 0 {
            return Err(StoreError::InvalidData {
                entity: "store config",
                message:
                    "pool.max_connections must be at least 2 and acquire timeout must be positive"
                        .to_owned(),
            });
        }
        Ok(())
    }

    /// 管理观测查询可并发占用的连接数；始终为数据面保留约 20% 的池容量。
    #[must_use]
    pub const fn observability_max_connections(self) -> u32 {
        self.max_connections - self.max_connections.div_ceil(5)
    }

    pub(crate) const fn acquire_timeout(self) -> Duration {
        Duration::from_secs(self.acquire_timeout_seconds)
    }
}

pub(crate) enum ResolvedStoreBackend {
    PostgresRedis {
        database_url: String,
        redis_url: String,
    },
    Sqlite {
        path: PathBuf,
        options: SqliteStoreConfig,
    },
}

impl StoreConfig {
    #[must_use]
    pub fn with_timezone(mut self, timezone: gateway_core::time::DeploymentTimeZone) -> Self {
        self.timezone = timezone;
        self
    }

    pub fn resolve_and_validate(&mut self, runtime_data_dir: &Path) -> StoreResult<()> {
        if runtime_data_dir.as_os_str().is_empty() {
            return Err(StoreError::InvalidData {
                entity: "store config",
                message: "runtime_data_dir must not be empty".to_owned(),
            });
        }
        self.backup_staging_dir = runtime_data_dir.join("backup-staging");
        self.validate_resolved()
    }

    pub(crate) fn validate_resolved(&mut self) -> StoreResult<()> {
        let runtime_data_dir = self
            .backup_staging_dir
            .parent()
            .ok_or_else(|| invalid_store_config("runtime_data_dir was not resolved"))?
            .to_path_buf();
        if self.backup_staging_dir.as_os_str().is_empty() {
            return Err(StoreError::InvalidData {
                entity: "store config",
                message: "runtime_data_dir was not resolved".to_owned(),
            });
        }
        match self.backend {
            StoreBackendKind::Postgres => self.resolve_postgres_redis()?,
            StoreBackendKind::Sqlite => self.resolve_sqlite(&runtime_data_dir)?,
        }
        Ok(())
    }

    fn resolve_postgres_redis(&mut self) -> StoreResult<()> {
        if self.sqlite != SqliteStoreConfig::default() {
            return Err(invalid_store_config(
                "SQLite options cannot be combined with the postgres backend",
            ));
        }
        let database = self.database.as_mut().ok_or_else(|| {
            invalid_store_config("database configuration is required for postgres")
        })?;
        let redis = self
            .redis
            .as_mut()
            .ok_or_else(|| invalid_store_config("redis configuration is required for postgres"))?;
        if let Some(url) = optional_environment_value(DATABASE_URL_ENV)? {
            database.url = url;
        }
        if let Some(url) = optional_environment_value(REDIS_URL_ENV)? {
            redis.url = url;
        }
        if let Some(password) = optional_environment_value_allow_empty(DATABASE_PASSWORD_ENV)? {
            database.password = password;
        }
        if let Some(password) = optional_environment_value_allow_empty(REDIS_PASSWORD_ENV)? {
            redis.password = password;
        }
        database.validate("database", true)?;
        redis.validate("redis", false)?;
        self.pool.validate()
    }

    fn resolve_sqlite(&mut self, runtime_data_dir: &Path) -> StoreResult<()> {
        if self.database.is_some() || self.redis.is_some() {
            return Err(invalid_store_config(
                "sqlite cannot be combined with database or redis configuration",
            ));
        }
        if self.sqlite.max_connections == 0
            || self.sqlite.acquire_timeout_seconds == 0
            || self.sqlite.busy_timeout_ms == 0
        {
            return Err(invalid_store_config(
                "sqlite connection limits and timeouts must be positive",
            ));
        }
        self.resolved_sqlite_path = Some(sqlite_database_path(
            runtime_data_dir,
            self.sqlite.path.as_deref(),
            optional_environment_value(SQLITE_PATH_ENV)?.as_deref(),
        )?);
        Ok(())
    }

    pub(crate) fn database_url(&self) -> StoreResult<String> {
        self.database
            .as_ref()
            .ok_or_else(|| invalid_store_config("database configuration is missing"))?
            .connection_url("database")
    }

    pub(crate) fn redis_url(&self) -> StoreResult<String> {
        self.redis
            .as_ref()
            .ok_or_else(|| invalid_store_config("redis configuration is missing"))?
            .connection_url("redis")
    }

    pub(crate) fn database_config(&self) -> StoreResult<&StoreConnectionConfig> {
        self.database
            .as_ref()
            .ok_or_else(|| invalid_store_config("database configuration is missing"))
    }

    pub(crate) fn resolved_backend(&self) -> StoreResult<ResolvedStoreBackend> {
        match self.backend {
            StoreBackendKind::Postgres => Ok(ResolvedStoreBackend::PostgresRedis {
                database_url: self.database_url()?,
                redis_url: self.redis_url()?,
            }),
            StoreBackendKind::Sqlite => Ok(ResolvedStoreBackend::Sqlite {
                path: self.sqlite_path()?.to_path_buf(),
                options: self.sqlite.clone(),
            }),
        }
    }

    pub fn sqlite_path(&self) -> StoreResult<&Path> {
        self.resolved_sqlite_path
            .as_deref()
            .ok_or_else(|| invalid_store_config("SQLite path was not resolved"))
    }

    /// 返回由统一运行数据根目录派生的备份暂存目录。
    #[must_use]
    pub fn backup_staging_dir(&self) -> &Path {
        &self.backup_staging_dir
    }
}

pub(crate) fn optional_environment_value(name: &'static str) -> StoreResult<Option<String>> {
    match optional_environment_value_allow_empty(name)? {
        Some(value) if value.trim().is_empty() => Err(StoreError::InvalidData {
            entity: "store config",
            message: format!("environment variable {name} is empty"),
        }),
        value => Ok(value),
    }
}

fn optional_environment_value_allow_empty(name: &'static str) -> StoreResult<Option<String>> {
    match std::env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => Err(StoreError::InvalidData {
            entity: "store config",
            message: format!("environment variable {name} is not Unicode"),
        }),
    }
}

fn sqlite_database_path(
    runtime_data_dir: &Path,
    configured_path: Option<&Path>,
    environment_path: Option<&str>,
) -> StoreResult<PathBuf> {
    if let Some(environment_path) = environment_path {
        let environment_path = Path::new(environment_path);
        if environment_path.is_absolute()
            || environment_path
                .components()
                .any(|component| matches!(component, std::path::Component::ParentDir))
        {
            return Err(invalid_store_config(
                "CPR_SQLITE_PATH must stay inside runtime_data_dir",
            ));
        }
    }
    let path = environment_path
        .map(PathBuf::from)
        .or_else(|| configured_path.map(Path::to_path_buf))
        .unwrap_or_else(|| PathBuf::from("codex-proxy.sqlite3"));
    if path.as_os_str().is_empty() || path == Path::new(":memory:") {
        return Err(invalid_store_config(
            "sqlite.path must identify a persistent database file",
        ));
    }
    Ok(if path.is_absolute() {
        path
    } else {
        runtime_data_dir.join(path)
    })
}

fn invalid_store_config(message: &'static str) -> StoreError {
    StoreError::InvalidData {
        entity: "store config",
        message: message.to_owned(),
    }
}

impl fmt::Debug for StoreConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StoreConfig")
            .field("backend", &self.backend)
            .field("database", &"[REDACTED]")
            .field("redis", &"[REDACTED]")
            .field("sqlite_path", &self.resolved_sqlite_path)
            .field("pool", &self.pool)
            .finish()
    }
}

#[derive(Clone, Deserialize)]
pub(crate) struct StoreConnectionConfig {
    pub(crate) url: String,
    #[serde(default)]
    pub(crate) password: String,
}

impl StoreConnectionConfig {
    fn validate(&self, field: &'static str, password_required: bool) -> StoreResult<()> {
        require_nonempty("store config", field, &self.url)?;
        if password_required && self.password.is_empty() {
            return Err(StoreError::InvalidData {
                entity: "store config",
                message: format!("{field}.password must not be empty"),
            });
        }
        self.connection_url(field).map(|_| ())
    }

    fn connection_url(&self, field: &'static str) -> StoreResult<String> {
        let mut url = url::Url::parse(&self.url).map_err(|_| StoreError::InvalidData {
            entity: "store config",
            message: format!("{field}.url is invalid"),
        })?;
        if url.password().is_some() {
            return Err(StoreError::InvalidData {
                entity: "store config",
                message: format!("{field}.url must not contain a password"),
            });
        }
        if !self.password.is_empty() {
            url.set_password(Some(&self.password))
                .map_err(|()| StoreError::InvalidData {
                    entity: "store config",
                    message: format!("{field}.url cannot carry credentials"),
                })?;
        }
        Ok(url.to_string())
    }
}
