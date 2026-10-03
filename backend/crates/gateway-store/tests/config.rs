use std::process::Command;

use gateway_store::{StoreConfig, StoreError, StorePoolConfig};

const PASSWORD: &str = "111111111111111111111111111111111111111111111111";

#[test]
fn store_config_should_derive_backup_staging_from_runtime_data_dir() {
    let root = tempfile::tempdir().expect("runtime data root");
    let mut config = valid_config();

    config
        .resolve_and_validate(root.path())
        .expect("valid Store configuration");

    assert_eq!(
        config.backup_staging_dir(),
        root.path().join("backup-staging")
    );
}

#[test]
fn store_pool_config_ignores_unknown_options_and_keeps_connection_pool_limits() {
    let pool: StorePoolConfig =
        serde_json::from_value(serde_json::json!({})).expect("default pool configuration");

    assert_eq!(pool, StorePoolConfig::default());
    assert_eq!(pool.max_connections, 20);
    assert_eq!(pool.acquire_timeout_seconds, 5);
    assert_eq!(pool.observability_max_connections(), 16);
    assert_eq!(
        StorePoolConfig {
            max_connections: 50,
            ..pool
        }
        .observability_max_connections(),
        40
    );
    assert_eq!(
        serde_json::from_value::<StorePoolConfig>(serde_json::json!({
            "statement_timeout_seconds": 60,
        }))
        .unwrap(),
        pool,
    );
}

#[test]
fn store_config_rejects_a_pool_that_cannot_reserve_both_traffic_classes() {
    let root = tempfile::tempdir().expect("runtime data root");
    let mut config: StoreConfig = serde_json::from_value(serde_json::json!({
        "database": {
            "url": "postgres://codex_proxy@127.0.0.1:5432/codex_proxy",
            "password": PASSWORD,
        },
        "redis": {
            "url": "redis://127.0.0.1:6379/",
            "password": PASSWORD,
        },
        "pool": {
            "max_connections": 1,
        },
    }))
    .expect("syntactically valid Store configuration");

    let error = config
        .resolve_and_validate(root.path())
        .expect_err("one connection cannot serve both traffic classes");

    assert!(matches!(
        error,
        StoreError::InvalidData { message, .. }
            if message.contains("max_connections must be at least 2")
    ));
}

#[test]
fn sqlite_path_environment_variable_overrides_the_configured_path() {
    if std::env::var_os("CPR_SQLITE_PATH_TEST_CHILD").is_none() {
        let status = Command::new(std::env::current_exe().expect("current test executable"))
            .args([
                "--exact",
                "config::sqlite_path_environment_variable_overrides_the_configured_path",
            ])
            .env("CPR_SQLITE_PATH_TEST_CHILD", "1")
            .env("CPR_SQLITE_PATH", "override/gateway.db")
            .status()
            .expect("run isolated environment override test");
        assert!(status.success(), "child configuration test failed");
        return;
    }

    let root = tempfile::tempdir().expect("runtime data directory");
    let mut config: StoreConfig = serde_json::from_value(serde_json::json!({
        "backend": "sqlite",
        "sqlite": { "path": "configured/gateway.db" },
    }))
    .expect("SQLite configuration");

    config
        .resolve_and_validate(root.path())
        .expect("valid SQLite config");

    assert_eq!(
        config.sqlite_path().unwrap(),
        root.path().join("override/gateway.db")
    );
}

#[test]
fn sqlite_path_environment_variable_rejects_paths_outside_runtime_data_dir() {
    if std::env::var_os("CPR_SQLITE_PATH_ESCAPE_TEST_CHILD").is_none() {
        for path in ["../outside.sqlite3", "/tmp/outside.sqlite3"] {
            let status = Command::new(std::env::current_exe().expect("current test executable"))
                .args([
                    "--exact",
                    "config::sqlite_path_environment_variable_rejects_paths_outside_runtime_data_dir",
                ])
                .env("CPR_SQLITE_PATH_ESCAPE_TEST_CHILD", "1")
                .env("CPR_SQLITE_PATH", path)
                .status()
                .expect("run isolated environment path validation test");
            assert!(status.success(), "child path validation failed for {path}");
        }
        return;
    }

    let root = tempfile::tempdir().expect("runtime data root");
    let mut config: StoreConfig = serde_json::from_value(serde_json::json!({
        "backend": "sqlite",
        "sqlite": {},
    }))
    .expect("SQLite configuration");
    assert!(matches!(
        config.resolve_and_validate(root.path()),
        Err(StoreError::InvalidData { .. })
    ));
}

fn valid_config() -> StoreConfig {
    serde_json::from_value(serde_json::json!({
        "database": {
            "url": "postgres://codex_proxy@127.0.0.1:5432/codex_proxy",
            "password": PASSWORD,
        },
        "redis": {
            "url": "redis://127.0.0.1:6379/",
            "password": PASSWORD,
        },
    }))
    .expect("test Store configuration")
}

#[test]
fn legacy_store_config_defaults_to_postgres_and_keeps_the_existing_connection_contract() {
    let root = tempfile::tempdir().expect("runtime data root");
    let mut config = valid_config();

    assert_eq!(config.backend, gateway_store::StoreBackendKind::Postgres);
    config
        .resolve_and_validate(root.path())
        .expect("legacy config remains valid");
}

#[test]
fn sqlite_config_resolves_relative_paths_without_postgres_or_redis() {
    let root = tempfile::tempdir().expect("runtime data root");
    let mut config: StoreConfig = serde_json::from_value(serde_json::json!({
        "backend": "sqlite",
        "sqlite": { "path": "state/gateway.db" },
    }))
    .expect("SQLite configuration");
    config
        .resolve_and_validate(root.path())
        .expect("valid SQLite config");
    assert_eq!(
        config.sqlite_path().unwrap(),
        root.path().join("state/gateway.db")
    );
}

#[test]
fn postgres_requires_redis_and_sqlite_rejects_postgres_or_redis_configuration() {
    let root = tempfile::tempdir().expect("runtime data root");
    let mut postgres: StoreConfig = serde_json::from_value(serde_json::json!({
        "database": { "url": "postgres://codex_proxy@127.0.0.1:5432/codex_proxy", "password": PASSWORD },
    })).unwrap();
    assert!(matches!(
        postgres.resolve_and_validate(root.path()),
        Err(StoreError::InvalidData { .. })
    ));

    let mut sqlite_with_redis: StoreConfig = serde_json::from_value(serde_json::json!({
        "backend": "sqlite",
        "redis": { "url": "redis://127.0.0.1:6379/" },
    }))
    .unwrap();
    assert!(matches!(
        sqlite_with_redis.resolve_and_validate(root.path()),
        Err(StoreError::InvalidData { .. })
    ));
}

#[test]
fn sqlite_rejects_non_file_path_and_unknown_backend() {
    let root = tempfile::tempdir().expect("runtime data root");
    let mut sqlite: StoreConfig = serde_json::from_value(serde_json::json!({
        "backend": "sqlite",
        "sqlite": { "path": ":memory:" },
    }))
    .unwrap();
    assert!(matches!(
        sqlite.resolve_and_validate(root.path()),
        Err(StoreError::InvalidData { .. })
    ));
    assert!(
        serde_json::from_value::<StoreConfig>(serde_json::json!({ "backend": "mysql" })).is_err()
    );
}
