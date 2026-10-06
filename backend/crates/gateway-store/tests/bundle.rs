//! 验证只读检查 Bundle 不执行迁移且拒绝业务写入

use gateway_core::{
    account::{
        CredentialRevision, NewProviderAccount, PlaintextCredential, ProviderAccount,
        ProviderAccountId,
    },
    routing::ProviderKind,
};
use gateway_store::StoreConfig;
use serde_json::json;
use sqlx::postgres::PgPoolOptions;

#[tokio::test]
async fn inspection_bundle_does_not_migrate_and_rejects_business_writes() {
    let (Some(database), Some(redis)) = (
        crate::support::test_env("CPR_TEST_DATABASE_URL"),
        crate::support::test_env("CPR_TEST_REDIS_URL"),
    ) else {
        return;
    };
    let schema = format!("cpr_inspection_{}", uuid::Uuid::new_v4().simple());
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database)
        .await
        .unwrap();
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!("create schema {schema}")))
        .execute(&admin)
        .await
        .unwrap();
    let mut database = url::Url::parse(&database).unwrap();
    database
        .query_pairs_mut()
        .append_pair("options", &format!("-csearch_path={schema}"));
    let config_connection = |mut url: url::Url| {
        let password = url.password().unwrap().to_owned();
        url.set_password(None).unwrap();
        json!({"url":url.as_str(),"password":password})
    };
    let mut config: StoreConfig = serde_json::from_value(json!({
        "database":config_connection(database), "redis":config_connection(url::Url::parse(&redis).unwrap()),
    })).unwrap();
    let directory = tempfile::tempdir().unwrap();
    config.resolve_and_validate(directory.path()).unwrap();
    let inspection = gateway_store::initialize_read_only(config.clone())
        .await
        .unwrap();
    assert!(
        inspection
            .admin_ports()
            .plugins()
            .load_instances()
            .await
            .is_err()
    );
    let tables: i64 =
        sqlx::query_scalar("select count(*) from information_schema.tables where table_schema=$1")
            .bind(&schema)
            .fetch_one(&admin)
            .await
            .unwrap();
    assert_eq!(tables, 0, "只读帮助不能在空库创建迁移表或业务表");
    drop(inspection);
    let writable = gateway_store::initialize(config.clone()).await.unwrap();
    let inspection = gateway_store::initialize_read_only(config).await.unwrap();
    let create = |id: &str| NewProviderAccount {
        account: ProviderAccount::new(
            ProviderAccountId::new(id.to_owned()).unwrap(),
            ProviderKind::new("example").unwrap(),
            "inspection test".into(),
            None,
            "api_key".into(),
            CredentialRevision::new(1).unwrap(),
            None,
        ),
        credential: PlaintextCredential::new(
            json!({"key":"test-only"}).as_object().unwrap().clone(),
        ),
        model_access: None,
    };
    writable
        .provider_ports()
        .accounts()
        .create_account(create("acct_writable"))
        .await
        .unwrap();
    assert!(
        inspection
            .provider_ports()
            .accounts()
            .create_account(create("acct_readonly"))
            .await
            .is_err()
    );
    let accounts = inspection
        .provider_ports()
        .accounts()
        .list_accounts()
        .await
        .unwrap();
    assert_eq!(accounts.len(), 1);
    assert_eq!(accounts[0].id().as_str(), "acct_writable");
    drop(inspection);
    drop(writable);
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!("drop schema {schema} cascade")))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}

#[tokio::test]
async fn sqlite_runtime_bundle_exposes_health_and_worker_contributions() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("runtime.sqlite3");
    let mut config: StoreConfig = serde_json::from_value(json!({
        "backend": "sqlite",
        "sqlite": { "path": path },
    }))
    .unwrap();
    config.resolve_and_validate(root.path()).unwrap();

    let mut bundle = gateway_store::initialize(config).await.unwrap();
    assert!(root.path().join("backup-staging").is_dir());
    let probes = bundle.health_probes();
    assert_eq!(probes.len(), 1);
    assert_eq!(probes[0].name(), "sqlite");
    assert_eq!(
        probes[0].check().await,
        gateway_core::health::HealthState::Healthy
    );
    let workers = bundle.take_worker_contributions();
    assert_eq!(workers.len(), 4);
    assert!(workers.iter().any(|worker| matches!(worker,
        gateway_core::task::WorkerContribution::Registration(registration)
        if registration.id.kind() == gateway_core::task::WorkerKind::Retention
            && registration.id.owner() == "sqlite_sessions"
    )));
    assert!(bundle.take_worker_contributions().is_empty());
}

#[tokio::test]
async fn sqlite_bundle_migrates_writable_cli_and_opens_existing_file_read_only() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("gateway.sqlite3");
    let mut config: StoreConfig = serde_json::from_value(json!({
        "backend": "sqlite",
        "sqlite": { "path": path },
    }))
    .unwrap();
    config.resolve_and_validate(root.path()).unwrap();

    assert!(
        gateway_store::initialize_read_only(config.clone())
            .await
            .is_err()
    );
    assert!(!path.exists(), "只读帮助不得创建 SQLite 文件");

    std::fs::File::create(&path).unwrap();
    let empty_inspection = gateway_store::initialize_read_only(config.clone())
        .await
        .unwrap();
    assert!(
        empty_inspection
            .admin_ports()
            .plugins()
            .load_instances()
            .await
            .is_err(),
        "只读帮助不得迁移一个已有空数据库"
    );
    drop(empty_inspection);
    let empty_pool = gateway_store::sqlite::connect_read_only(
        &path,
        &gateway_store::SqliteStoreConfig::default(),
    )
    .await
    .unwrap();
    let table_count: i64 =
        sqlx::query_scalar("select count(*) from sqlite_master where type = 'table'")
            .fetch_one(&empty_pool)
            .await
            .unwrap();
    assert_eq!(table_count, 0);
    empty_pool.close().await;

    let mut command = gateway_store::initialize_command_line(config.clone())
        .await
        .unwrap();
    assert!(path.is_file());
    assert_eq!(command.health_probes().len(), 1);
    assert_eq!(command.health_probes()[0].name(), "sqlite");
    assert_eq!(
        command.health_probes()[0].check().await,
        gateway_core::health::HealthState::Healthy
    );
    assert!(
        command
            .admin_ports()
            .plugins()
            .load_instances()
            .await
            .is_ok()
    );
    assert!(
        command
            .provider_ports()
            .accounts()
            .list_accounts()
            .await
            .is_ok()
    );
    assert!(command.take_worker_contributions().is_empty());
    command.start_command_line_writes().unwrap();
    command.shutdown_command_line_writes().await.unwrap();
    drop(command);

    let staging = root.path().join("backup-staging");
    std::fs::remove_dir_all(&staging).unwrap();
    let mut inspection = gateway_store::initialize_read_only(config).await.unwrap();
    assert!(!staging.exists(), "只读帮助不得创建备份暂存目录");
    assert!(inspection.take_worker_contributions().is_empty());
    assert_eq!(inspection.health_probes().len(), 1);
    assert!(
        inspection
            .admin_ports()
            .plugins()
            .load_instances()
            .await
            .is_ok()
    );
}
