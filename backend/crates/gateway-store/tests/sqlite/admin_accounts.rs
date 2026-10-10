//! 验证 SQLite 账号查询、导入、设置更新与失败事务的原子性

use gateway_admin::{
    model::{
        PageSize,
        accounts::{AccountListQuery, AccountRuntimeSnapshot},
        provider_credentials::PluginAccountListQuery,
    },
    ports::store::AccountStore,
};
use gateway_core::account::{
    CredentialRevision, NewProviderAccount, PlaintextCredential, ProviderAccount,
    ProviderAccountId, ProviderAccountStore,
};
use gateway_core::routing::ProviderKind;
use gateway_store::{
    SqliteStoreConfig, sqlite,
    sqlite::{SqliteAdminAccountStore, SqliteProviderAccountRepository},
};

#[tokio::test]
async fn sqlite_admin_account_views_read_core_accounts_and_credentials() {
    let root = tempfile::tempdir().expect("SQLite directory");
    let pool = sqlite::connect_and_migrate(
        &root.path().join("admin-accounts.sqlite3"),
        &SqliteStoreConfig::default(),
    )
    .await
    .expect("migrate SQLite");
    let provider = ProviderKind::new("example").unwrap();
    let id = ProviderAccountId::new("acct_admin_view").unwrap();
    let provider_accounts = SqliteAdminAccountStore::new(pool.clone());
    let core_accounts = SqliteProviderAccountRepository::new(pool.clone());
    ProviderAccountStore::create_account(
        &core_accounts,
        NewProviderAccount {
            account: ProviderAccount::new(
                id.clone(),
                provider.clone(),
                "admin account".to_owned(),
                Some("upstream-user".to_owned()),
                "api_key".to_owned(),
                CredentialRevision::new(1).unwrap(),
                None,
            )
            .with_profile(
                Some("test@example.com".to_owned()),
                Some("remote-id".to_owned()),
                Some("pro".to_owned()),
            ),
            credential: PlaintextCredential::new(
                serde_json::json!({"token":"test-only"})
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
            model_access: None,
        },
    )
    .await
    .unwrap();

    let page = provider_accounts
        .list_accounts(
            AccountListQuery {
                page: 1,
                page_size: PageSize::new(20).unwrap(),
                provider_kind: Some(provider.clone()),
                group_filter: None,
                search: Some("example.com".to_owned()),
                status: None,
                sort: None,
            },
            AccountRuntimeSnapshot::default(),
        )
        .await
        .unwrap();
    assert_eq!(page.config_revision.get(), 1);
    assert_eq!(page.total, 1);
    assert_eq!(page.summary.total, 1);
    assert_eq!(page.items[0].account.id, id.as_str());
    assert_eq!(
        page.items[0].account.email.as_deref(),
        Some("test@example.com")
    );
    assert_eq!(page.items[0].account.plan_type.as_deref(), Some("pro"));

    let plugin_page = provider_accounts
        .list_plugin_accounts(PluginAccountListQuery {
            provider_kind: Some(provider.clone()),
            cursor: None,
            limit: PageSize::new(1).unwrap(),
        })
        .await
        .unwrap();
    assert_eq!(plugin_page.accounts.len(), 1);
    assert_eq!(plugin_page.accounts[0].id, id.as_str());

    let details = provider_accounts
        .credential_details_by_id(&id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(details.credential.provider_kind, provider);
    assert_eq!(details.credential.name, "admin account");
    let exported = provider_accounts
        .load_credentials_for_export(&ProviderKind::new("example").unwrap(), &[id])
        .await
        .unwrap();
    assert_eq!(exported.len(), 1);
    assert_eq!(
        exported[0]
            .provider_material
            .expose_to_provider()
            .expose_to_provider()
            .get("token")
            .unwrap(),
        "test-only"
    );

    pool.close().await;
}

async fn account_write_fixture() -> (tempfile::TempDir, sqlx::SqlitePool, SqliteAdminAccountStore) {
    let root = tempfile::tempdir().unwrap();
    let pool = sqlite::connect_and_migrate(
        &root.path().join("account-writes.sqlite3"),
        &SqliteStoreConfig::default(),
    )
    .await
    .unwrap();
    let store = SqliteAdminAccountStore::new(pool.clone());
    (root, pool, store)
}

fn write_context() -> gateway_admin::model::MutationContext {
    gateway_admin::model::MutationContext {
        actor: gateway_admin::model::MutationActor::System,
        request_id: "sqlite-account-write-test".to_owned(),
    }
}

fn imported_credential(
    id: &str,
    user: &str,
) -> gateway_admin::model::provider_credentials::PreparedCredentialCreate {
    use gateway_admin::model::provider_credentials::{PreparedCredentialCreate, ProviderDocument};
    use gateway_core::account::{CredentialState, OpaqueProviderData};
    PreparedCredentialCreate {
        model_access: None,
        outbound_proxy: None,
        account_id: ProviderAccountId::new(id).unwrap(),
        provider_kind: ProviderKind::new("example").unwrap(),
        name: id.to_owned(),
        email: None,
        upstream_user_id: Some(user.to_owned()),
        upstream_account_id: None,
        plan_type: None,
        authentication_kind: "oauth".to_owned(),
        provider_material: ProviderDocument::new(OpaqueProviderData::new(
            serde_json::json!({"access_token": "synthetic-import-secret"})
                .as_object()
                .unwrap()
                .clone(),
        )),
        has_refresh_token: false,
        access_token_expires_at: None,
        next_refresh_at: None,
        enabled: true,
        credential_state: CredentialState::Ready,
        credential_observed_at: chrono::Utc::now(),
    }
}

fn import_command(
    identities: &[(&str, &str)],
    settings: Option<gateway_admin::model::accounts::AccountImportSettings>,
) -> gateway_admin::model::provider_credentials::CredentialImportCommit {
    use gateway_admin::model::provider_credentials::{
        CredentialImportCommit, PreparedCredentialImport,
    };
    CredentialImportCommit {
        outbound_proxy: None,
        settings,
        prepared: PreparedCredentialImport {
            provider_kind: ProviderKind::new("example").unwrap(),
            credentials: identities
                .iter()
                .map(|(id, user)| imported_credential(id, user))
                .collect(),
        },
    }
}

fn import_settings() -> gateway_admin::model::accounts::AccountImportSettings {
    use gateway_core::account::{AccountConcurrencyLimit, AccountWeight};
    gateway_admin::model::accounts::AccountImportSettings {
        enabled: false,
        concurrency_limit: AccountConcurrencyLimit::new(3),
        weight: AccountWeight::new(7).unwrap(),
        notes: Some("  团队备用  ".to_owned()),
        model_access: None,
        group_ids: vec![
            gateway_core::routing::AccountGroupId::new("grp_0123456789abcdef0123456789abcdef")
                .unwrap(),
        ],
    }
}

async fn seed_import_group(pool: &sqlx::SqlitePool) {
    sqlx::query("insert into account_groups (id, name, created_at_us, updated_at_us) values (?1, 'Import group', 1, 1)")
        .bind(import_settings().group_ids[0].as_str()).execute(pool).await.unwrap();
}

async fn write_snapshot(pool: &sqlx::SqlitePool) -> (Vec<String>, Vec<(String, String)>, i64, i64) {
    let accounts = sqlx::query_scalar("select json_object('id', id, 'credentials', provider_credentials_json, 'credential_revision', credential_revision, 'enabled', enabled, 'limit', concurrency_limit, 'weight', weight, 'notes', notes, 'updated', updated_at_us) from provider_accounts order by id")
        .fetch_all(pool).await.unwrap();
    let groups = sqlx::query_as("select provider_account_id, account_group_id from account_group_accounts order by provider_account_id, account_group_id")
        .fetch_all(pool).await.unwrap();
    let revision = sqlx::query_scalar("select config_revision from runtime_settings where id = 1")
        .fetch_one(pool)
        .await
        .unwrap();
    let audits = sqlx::query_scalar("select count(*) from admin_audit_events")
        .fetch_one(pool)
        .await
        .unwrap();
    (accounts, groups, revision, audits)
}

#[tokio::test]
async fn sqlite_import_settings_apply_to_new_and_existing_identities_and_preserve_memberships() {
    let (_root, pool, store) = account_write_fixture().await;
    seed_import_group(&pool).await;
    store
        .commit_credential_import(
            import_command(&[("acct_existing", "existing-user")], None),
            &write_context(),
        )
        .await
        .unwrap();
    let result = store
        .commit_credential_import(
            import_command(
                &[
                    ("acct_new", "new-user"),
                    ("acct_candidate", "existing-user"),
                    ("acct_duplicate", "existing-user"),
                ],
                Some(import_settings()),
            ),
            &write_context(),
        )
        .await
        .expect("import settings must execute valid SQLite SQL");
    assert_eq!(
        result
            .credential_ids
            .iter()
            .map(ProviderAccountId::as_str)
            .collect::<Vec<_>>(),
        ["acct_new", "acct_existing", "acct_existing"]
    );
    type SavedAccountSettings = (String, i64, Option<i64>, i64, Option<String>);
    let saved: Vec<SavedAccountSettings> = sqlx::query_as(
        "select id, enabled, concurrency_limit, weight, notes from provider_accounts order by id",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        saved,
        [
            (
                "acct_existing".to_owned(),
                0,
                Some(3),
                7,
                Some("团队备用".to_owned())
            ),
            (
                "acct_new".to_owned(),
                0,
                Some(3),
                7,
                Some("团队备用".to_owned())
            )
        ]
    );
    let before = write_snapshot(&pool).await;
    assert_eq!(before.1.len(), 2);
    store
        .commit_credential_import(
            import_command(&[("acct_reimported", "existing-user")], None),
            &write_context(),
        )
        .await
        .unwrap();
    let after = write_snapshot(&pool).await;
    assert_eq!(after.1, before.1);
    let notes: Option<String> =
        sqlx::query_scalar("select notes from provider_accounts where id = 'acct_existing'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(notes.as_deref(), Some("团队备用"));
    pool.close().await;
}

#[tokio::test]
async fn sqlite_single_account_settings_replace_and_clear_notes_and_groups() {
    use gateway_admin::model::accounts::UpdateAccount;
    let (_root, pool, store) = account_write_fixture().await;
    seed_import_group(&pool).await;
    store
        .commit_credential_import(
            import_command(&[("acct_single", "single-user")], None),
            &write_context(),
        )
        .await
        .unwrap();
    let settings = import_settings();
    let mut command = UpdateAccount {
        account_id: "acct_single".to_owned(),
        enabled: settings.enabled,
        concurrency_limit: settings.concurrency_limit,
        weight: settings.weight,
        notes: settings.notes,
        model_access: None,
        group_ids: settings.group_ids,
        outbound_proxy: None,
    };
    store
        .update_account(command.clone(), &write_context())
        .await
        .unwrap();
    let before = write_snapshot(&pool).await;
    assert_eq!(before.1.len(), 1);
    command.notes = None;
    store
        .update_account(command.clone(), &write_context())
        .await
        .unwrap();
    let notes: Option<String> =
        sqlx::query_scalar("select notes from provider_accounts where id = 'acct_single'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(notes.as_deref(), Some("团队备用"));
    command.notes = Some(" \n\t ".to_owned());
    command.group_ids.clear();
    store
        .update_account(command, &write_context())
        .await
        .unwrap();
    let notes: Option<String> =
        sqlx::query_scalar("select notes from provider_accounts where id = 'acct_single'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(notes, None);
    assert!(write_snapshot(&pool).await.1.is_empty());
    pool.close().await;
}

#[tokio::test]
async fn sqlite_batch_settings_update_one_or_many_accounts_without_overwriting_omitted_fields() {
    use gateway_admin::model::accounts::BatchUpdateAccounts;
    use gateway_core::account::AccountWeight;
    let (_root, pool, store) = account_write_fixture().await;
    seed_import_group(&pool).await;
    store
        .commit_credential_import(
            import_command(
                &[("acct_first", "first-user"), ("acct_second", "second-user")],
                Some(import_settings()),
            ),
            &write_context(),
        )
        .await
        .unwrap();
    for ids in [vec!["acct_first"], vec!["acct_first", "acct_second"]] {
        store
            .batch_update_accounts(
                BatchUpdateAccounts {
                    account_ids: ids.iter().map(|id| (*id).to_owned()).collect(),
                    enabled: None,
                    concurrency_limit: None,
                    weight: Some(AccountWeight::new(9).unwrap()),
                    model_access: None,
                    group_ids: None,
                    outbound_proxy: None,
                },
                &write_context(),
            )
            .await
            .unwrap();
        for id in ids {
            let saved: (i64, Option<i64>, i64, Option<String>) = sqlx::query_as("select enabled, concurrency_limit, weight, notes from provider_accounts where id = ?1")
                .bind(id).fetch_one(&pool).await.unwrap();
            assert_eq!(saved, (0, Some(3), 9, Some("团队备用".to_owned())));
        }
        assert_eq!(write_snapshot(&pool).await.1.len(), 2);
    }
    store
        .batch_update_accounts(
            BatchUpdateAccounts {
                account_ids: vec!["acct_first".to_owned(), "acct_second".to_owned()],
                enabled: None,
                concurrency_limit: Some(None),
                weight: None,
                model_access: None,
                group_ids: Some(Vec::new()),
                outbound_proxy: None,
            },
            &write_context(),
        )
        .await
        .unwrap();
    assert!(write_snapshot(&pool).await.1.is_empty());
    let limits: Vec<Option<i64>> =
        sqlx::query_scalar("select concurrency_limit from provider_accounts")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(limits, [None, None]);
    pool.close().await;
}

#[tokio::test]
async fn sqlite_import_write_failure_rolls_back_credentials_settings_revision_and_audit() {
    let (_root, pool, store) = account_write_fixture().await;
    seed_import_group(&pool).await;
    store
        .commit_credential_import(
            import_command(&[("acct_existing", "existing-user")], None),
            &write_context(),
        )
        .await
        .unwrap();
    let before = write_snapshot(&pool).await;
    sqlx::query("create trigger reject_import_notes before update of notes on provider_accounts begin select raise(ABORT, 'PRIVATE_SQLITE_WRITE_CAUSE'); end")
        .execute(&pool).await.unwrap();
    let error = store
        .commit_credential_import(
            import_command(
                &[
                    ("acct_new", "new-user"),
                    ("acct_replacement", "existing-user"),
                ],
                Some(import_settings()),
            ),
            &write_context(),
        )
        .await
        .unwrap_err();
    let error = gateway_admin::model::AdminError::new(
        gateway_admin::model::AdminErrorKind::Unavailable,
        "依赖服务暂不可用",
    )
    .with_source(error);
    assert!(
        error
            .error_details()
            .unwrap()
            .as_str()
            .contains("PRIVATE_SQLITE_WRITE_CAUSE")
    );
    assert!(!error.to_string().contains("PRIVATE_SQLITE_WRITE_CAUSE"));
    assert_eq!(write_snapshot(&pool).await, before);
    pool.close().await;
}

#[tokio::test]
async fn sqlite_import_rollback_failure_keeps_the_primary_database_cause() {
    let (_root, pool, store) = account_write_fixture().await;
    seed_import_group(&pool).await;
    let before = write_snapshot(&pool).await;
    sqlx::query("create trigger rollback_import_notes before update of notes on provider_accounts begin select raise(ROLLBACK, 'PRIVATE_SQLITE_PRIMARY_CAUSE'); end")
        .execute(&pool).await.unwrap();
    let error = store
        .commit_credential_import(
            import_command(&[("acct_new", "new-user")], Some(import_settings())),
            &write_context(),
        )
        .await
        .unwrap_err();
    let error = gateway_admin::model::AdminError::new(
        gateway_admin::model::AdminErrorKind::Unavailable,
        "依赖服务暂不可用",
    )
    .with_source(error);
    let details: serde_json::Value =
        serde_json::from_str(error.error_details().unwrap().as_str()).unwrap();
    assert!(
        details["causes"]["messages"]
            .to_string()
            .contains("PRIVATE_SQLITE_PRIMARY_CAUSE")
    );
    assert!(
        details["causes"]["cleanup"]
            .to_string()
            .contains("no transaction is active")
    );
    assert_eq!(write_snapshot(&pool).await, before);
    pool.close().await;
}

#[tokio::test]
async fn sqlite_account_update_failures_preserve_primary_and_cleanup_causes_and_roll_back_state() {
    use gateway_admin::model::accounts::UpdateAccount;
    for action in ["ABORT", "ROLLBACK"] {
        let (_root, pool, store) = account_write_fixture().await;
        seed_import_group(&pool).await;
        store
            .commit_credential_import(
                import_command(&[("acct_existing", "existing-user")], None),
                &write_context(),
            )
            .await
            .unwrap();
        let before = write_snapshot(&pool).await;
        // 两种固定触发器分别验证仍可回滚和数据库已自行回滚的清理出口
        let trigger = match action {
            "ABORT" => {
                "create trigger reject_update_notes before update of notes on provider_accounts begin select raise(ABORT, 'PRIVATE_UPDATE_PRIMARY_CAUSE'); end"
            }
            _ => {
                "create trigger reject_update_notes before update of notes on provider_accounts begin select raise(ROLLBACK, 'PRIVATE_UPDATE_PRIMARY_CAUSE'); end"
            }
        };
        sqlx::query(trigger).execute(&pool).await.unwrap();
        let settings = import_settings();
        let error = store
            .update_account(
                UpdateAccount {
                    account_id: "acct_existing".to_owned(),
                    enabled: settings.enabled,
                    concurrency_limit: settings.concurrency_limit,
                    weight: settings.weight,
                    notes: settings.notes,
                    model_access: None,
                    group_ids: settings.group_ids,
                    outbound_proxy: None,
                },
                &write_context(),
            )
            .await
            .unwrap_err();
        assert_eq!(
            error.kind(),
            gateway_admin::ports::store::AdminStoreErrorKind::Unavailable
        );
        let error = gateway_admin::model::AdminError::new(
            gateway_admin::model::AdminErrorKind::Unavailable,
            "依赖服务暂不可用",
        )
        .with_source(error);
        let details: serde_json::Value =
            serde_json::from_str(error.error_details().unwrap().as_str()).unwrap();
        assert!(
            details["causes"]["messages"]
                .to_string()
                .contains("PRIVATE_UPDATE_PRIMARY_CAUSE")
        );
        assert_eq!(
            details["causes"]["cleanup"].is_array(),
            action == "ROLLBACK"
        );
        assert_eq!(write_snapshot(&pool).await, before);
        pool.close().await;
    }
}

#[tokio::test]
async fn sqlite_authorization_commit_is_atomic_and_idempotent() {
    use chrono::Utc;
    use gateway_admin::model::{
        MutationActor, MutationContext,
        provider_credentials::{
            AuthorizationCommit, AuthorizationCredentialCommit, AuthorizationMutationTarget,
            AuthorizationOwnerBinding, AuthorizationReceiptKey, PreparedCredentialCreate,
        },
    };
    use gateway_core::account::{CredentialState, OpaqueProviderData};

    let root = tempfile::tempdir().expect("SQLite directory");
    let pool = sqlite::connect_and_migrate(
        &root.path().join("authorization.sqlite3"),
        &SqliteStoreConfig::default(),
    )
    .await
    .expect("migrate SQLite");
    let store = SqliteAdminAccountStore::new(pool.clone());
    let context = MutationContext {
        actor: MutationActor::System,
        request_id: "sqlite-authorization-test".to_owned(),
    };
    let provider = ProviderKind::new("example").unwrap();
    let account_id = ProviderAccountId::new("acct_authorized").unwrap();
    let key =
        AuthorizationReceiptKey::new(provider.clone(), "oauth-flow-sqlite-test", &context).unwrap();
    let credential = PreparedCredentialCreate {
        model_access: None,
        outbound_proxy: None,
        account_id: account_id.clone(),
        provider_kind: provider.clone(),
        name: "authorized account".to_owned(),
        email: Some("oauth@example.com".to_owned()),
        upstream_user_id: Some("oauth-user".to_owned()),
        upstream_account_id: None,
        plan_type: Some("pro".to_owned()),
        authentication_kind: "oauth".to_owned(),
        provider_material: gateway_admin::model::provider_credentials::ProviderDocument::new(
            OpaqueProviderData::new(
                serde_json::json!({"access_token":"test-token"})
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        ),
        has_refresh_token: false,
        access_token_expires_at: None,
        next_refresh_at: None,
        enabled: true,
        credential_state: CredentialState::Ready,
        credential_observed_at: Utc::now(),
    };
    let command = AuthorizationCommit {
        key: key.clone(),
        settings: None,
        pending: gateway_admin::model::provider_credentials::PendingAuthorizationMutation::new(
            provider.clone(),
            AuthorizationMutationTarget::Create {
                name: "login".to_owned(),
            },
            AuthorizationOwnerBinding::from_context(&context),
        ),
        credential: AuthorizationCredentialCommit::Create(Box::new(credential)),
    };

    let first = store
        .commit_authorization(command.clone(), &context)
        .await
        .unwrap();
    assert!(first.newly_committed);
    assert_eq!(first.result.account_id, account_id);
    assert_eq!(first.result.credential_revision.unwrap().get(), 1);

    let replay = store.commit_authorization(command, &context).await.unwrap();
    assert!(!replay.newly_committed);
    assert_eq!(replay.result, first.result);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("select count(*) from provider_accounts")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1,
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("select count(*) from authorization_receipts")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1,
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "select count(*) from admin_audit_events where action = 'authorize'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        1,
    );
    pool.close().await;
}

#[tokio::test]
async fn sqlite_account_usage_aggregates_exact_costs_and_sorts_accounts() {
    use chrono::{Duration, Utc};
    use gateway_admin::model::{
        PageSize,
        accounts::{AccountSort, AccountSortField, SortDirection},
    };
    use gateway_core::account::{
        CredentialRevision, NewProviderAccount, PlaintextCredential, ProviderAccount,
        ProviderAccountId, ProviderAccountStore,
    };

    let root = tempfile::tempdir().expect("SQLite directory");
    let pool = sqlite::connect_and_migrate(
        &root.path().join("account-usage.sqlite3"),
        &SqliteStoreConfig::default(),
    )
    .await
    .expect("migrate SQLite");
    let provider = ProviderKind::new("example").unwrap();
    let core = SqliteProviderAccountRepository::new(pool.clone());
    for (id, user) in [("acct_usage_a", "user-a"), ("acct_usage_b", "user-b")] {
        ProviderAccountStore::create_account(
            &core,
            NewProviderAccount {
                account: ProviderAccount::new(
                    ProviderAccountId::new(id).unwrap(),
                    provider.clone(),
                    id.to_owned(),
                    Some(user.to_owned()),
                    "api_key".to_owned(),
                    CredentialRevision::new(1).unwrap(),
                    None,
                ),
                credential: PlaintextCredential::new(
                    serde_json::json!({"token":"test"})
                        .as_object()
                        .unwrap()
                        .clone(),
                ),
                model_access: None,
            },
        )
        .await
        .unwrap();
    }

    let now = Utc::now().timestamp_micros();
    for (request_id, account_id, tokens, amount) in [
        ("req_usage_a1", "acct_usage_a", 10_i64, 100_i64),
        ("req_usage_a2", "acct_usage_a", 20_i64, 100_i64),
        ("req_usage_b1", "acct_usage_b", 20_i64, 100_i64),
    ] {
        let start = now - 60_000_000;
        sqlx::query(
            "insert into model_requests (
               id, client_api_key_ref, operation, client_transport, requested_model_id, provider_kind, upstream_model_id, provider_account_ref, upstream_transport, attempt_count, upstream_send_state, downstream_committed_at_us, outcome, client_status_code, input_tokens, output_tokens, total_tokens, cost_source, cost_amount, cost_currency, started_at_us, deadline_at_us, completed_at_us, request_observation_json
             ) values (
               ?1, 'key_test', 'generate', 'http_sse', 'public-model', 'example', 'upstream-model', ?2, 'websocket', 1, 'sent', ?3, 'succeeded', 200, ?4, 0, ?4, 'provider_reported', ?5, 'USD', ?6, ?7, ?7,
               json_object('request', json_object('configRevision', 1, 'protocol', 'openai', 'endpoint', '/v1/responses', 'compact', json('false')), 'routing', json_object('scope', 'all', 'groupRefs', json('[]'), 'groupNamesSnapshot', json('[]')))
             )",
        )
        .bind(request_id)
        .bind(account_id)
        .bind(start + 500_000)
        .bind(tokens)
        .bind(format!("{amount:020}"))
        .bind(start)
        .bind(start + 1_000_000)
        .execute(&pool)
        .await
        .unwrap();
    }

    let store = SqliteAdminAccountStore::new(pool.clone());
    let range = gateway_admin::model::observability::TimeRange::new(
        Utc::now() - Duration::hours(1),
        Utc::now() + Duration::hours(1),
    )
    .unwrap();
    let ids = vec!["acct_usage_a".to_owned(), "acct_usage_b".to_owned()];
    let usage = store.load_account_usage(range, &ids).await.unwrap();
    assert_eq!(usage[0].request_count, 2);
    assert_eq!(usage[0].total_tokens, Some(30));
    assert_eq!(usage[0].cost_coverage.provider_reported_count, 2);
    assert_eq!(usage[0].costs[0].amount.as_str(), "0.00000002");
    assert_eq!(usage[0].models[0].request_count, 2);
    assert_eq!(
        usage[0]
            .request_buckets
            .iter()
            .map(|bucket| bucket.request_count)
            .sum::<u64>(),
        2
    );

    let page = store
        .list_accounts(
            AccountListQuery {
                page: 1,
                page_size: PageSize::new(20).unwrap(),
                provider_kind: Some(provider),
                group_filter: None,
                search: None,
                status: None,
                sort: Some(AccountSort {
                    field: AccountSortField::Usage,
                    direction: SortDirection::Desc,
                }),
            },
            AccountRuntimeSnapshot::default(),
        )
        .await
        .unwrap();
    assert_eq!(page.items[0].account.id, "acct_usage_a");
    pool.close().await;
}

#[tokio::test]
async fn sqlite_usage_facts_agree_across_account_group_and_forecast_queries() {
    use chrono::{Duration, Utc};
    use gateway_admin::model::{
        PageSize,
        account_groups::{AccountGroupColor, AccountGroupListQuery, NewAccountGroup},
        accounts::{AccountSort, AccountSortField, AccountUsageWindowQuery, SortDirection},
        observability::TimeRange,
    };
    use gateway_admin::ports::store::AccountGroupStore;
    use gateway_core::account::{
        CredentialRevision, NewProviderAccount, PlaintextCredential, ProviderAccount,
        ProviderAccountId, ProviderAccountStore,
    };
    use gateway_store::sqlite::SqliteAccountGroupRepository;

    let root = tempfile::tempdir().expect("SQLite directory");
    let pool = sqlite::connect_and_migrate(
        &root.path().join("usage-facts-matrix.sqlite3"),
        &SqliteStoreConfig::default(),
    )
    .await
    .expect("migrate SQLite");
    let provider = ProviderKind::new("example").unwrap();
    let accounts = SqliteProviderAccountRepository::new(pool.clone());
    for (id, name) in [
        ("acct_usage_matrix_a", "matrix-a"),
        ("acct_usage_matrix_b", "matrix-b"),
    ] {
        ProviderAccountStore::create_account(
            &accounts,
            NewProviderAccount {
                account: ProviderAccount::new(
                    ProviderAccountId::new(id).unwrap(),
                    provider.clone(),
                    name.to_owned(),
                    None,
                    "api_key".to_owned(),
                    CredentialRevision::new(1).unwrap(),
                    None,
                ),
                credential: PlaintextCredential::new(
                    serde_json::json!({"token":"test"})
                        .as_object()
                        .unwrap()
                        .clone(),
                ),
                model_access: None,
            },
        )
        .await
        .unwrap();
    }

    let group =
        gateway_core::routing::AccountGroupId::new("grp_0123456789abcdef0123456789abcdef").unwrap();
    let groups = SqliteAccountGroupRepository::new(pool.clone());
    groups
        .create_account_group(
            NewAccountGroup {
                id: group.clone(),
                name: "Usage facts".to_owned(),
                description: None,
                color: AccountGroupColor::parse("#2563EBFF").unwrap(),
                fast_mode: gateway_core::account::FastMode::Default,
            },
            &gateway_admin::model::MutationContext {
                actor: gateway_admin::model::MutationActor::System,
                request_id: "usage-facts-group-create".to_owned(),
            },
        )
        .await
        .unwrap();
    let now = Utc::now();
    let now_us = now.timestamp_micros();
    sqlx::query(
        "insert into account_group_accounts (account_group_id, provider_account_id, created_at_us)
         values (?1, 'acct_usage_matrix_a', ?2)",
    )
    .bind(group.as_str())
    .bind(now_us)
    .execute(&pool)
    .await
    .unwrap();

    let start_us = now_us - 15 * 60 * 1_000_000;
    let facts = [
        UsageMatrixFact {
            id: "matrix_http_success",
            account_id: "acct_usage_matrix_a",
            provider_kind: "example",
            model: Some("gpt-matrix"),
            transport: "http_sse",
            status: Some(200),
            outcome: "succeeded",
            request_kind: None,
            input_tokens: Some(100),
            output_tokens: Some(0),
            total_tokens: Some(100),
            image_generation_succeeded: None,
            amount: Some("0.12500001"),
            recovered: false,
            pending: false,
        },
        UsageMatrixFact {
            id: "matrix_websocket_success",
            account_id: "acct_usage_matrix_a",
            provider_kind: "example",
            model: Some("gpt-matrix"),
            transport: "websocket",
            status: None,
            outcome: "succeeded",
            request_kind: None,
            input_tokens: Some(50),
            output_tokens: Some(0),
            total_tokens: Some(50),
            image_generation_succeeded: None,
            amount: Some("0.00000002"),
            recovered: false,
            pending: false,
        },
        UsageMatrixFact {
            id: "matrix_image_intent",
            account_id: "acct_usage_matrix_a",
            provider_kind: "example",
            model: None,
            transport: "http_sse",
            status: Some(201),
            outcome: "succeeded",
            request_kind: None,
            input_tokens: None,
            output_tokens: None,
            total_tokens: None,
            image_generation_succeeded: Some(1),
            amount: Some("0.00000003"),
            recovered: false,
            pending: false,
        },
        UsageMatrixFact {
            id: "matrix_no_usage_evidence",
            account_id: "acct_usage_matrix_a",
            provider_kind: "example",
            model: None,
            transport: "http_sse",
            status: Some(200),
            outcome: "succeeded",
            request_kind: None,
            input_tokens: None,
            output_tokens: None,
            total_tokens: None,
            image_generation_succeeded: None,
            amount: None,
            recovered: false,
            pending: false,
        },
        UsageMatrixFact {
            id: "matrix_http_failure_status",
            account_id: "acct_usage_matrix_a",
            provider_kind: "example",
            model: Some("gpt-matrix"),
            transport: "http_sse",
            status: Some(500),
            outcome: "succeeded",
            request_kind: None,
            input_tokens: Some(8),
            output_tokens: Some(0),
            total_tokens: Some(8),
            image_generation_succeeded: None,
            amount: Some("0.00000005"),
            recovered: false,
            pending: false,
        },
        UsageMatrixFact {
            id: "matrix_failed_outcome",
            account_id: "acct_usage_matrix_a",
            provider_kind: "example",
            model: Some("gpt-matrix"),
            transport: "http_sse",
            status: Some(200),
            outcome: "failed",
            request_kind: None,
            input_tokens: Some(7),
            output_tokens: Some(0),
            total_tokens: Some(7),
            image_generation_succeeded: None,
            amount: Some("0.00000006"),
            recovered: false,
            pending: false,
        },
        UsageMatrixFact {
            id: "matrix_openai_prewarm",
            account_id: "acct_usage_matrix_a",
            provider_kind: "openai",
            model: Some("gpt-matrix"),
            transport: "http_sse",
            status: Some(200),
            outcome: "succeeded",
            request_kind: Some("prewarm"),
            input_tokens: Some(9),
            output_tokens: Some(0),
            total_tokens: Some(9),
            image_generation_succeeded: None,
            amount: Some("0.00000007"),
            recovered: false,
            pending: false,
        },
        UsageMatrixFact {
            id: "matrix_recovered",
            account_id: "acct_usage_matrix_a",
            provider_kind: "example",
            model: Some("gpt-matrix"),
            transport: "http_sse",
            status: Some(200),
            outcome: "succeeded",
            request_kind: None,
            input_tokens: Some(11),
            output_tokens: Some(0),
            total_tokens: Some(11),
            image_generation_succeeded: None,
            amount: Some("0.00000004"),
            recovered: true,
            pending: false,
        },
        UsageMatrixFact {
            id: "matrix_missing_tokens_and_cost",
            account_id: "acct_usage_matrix_a",
            provider_kind: "example",
            model: Some("gpt-matrix"),
            transport: "http_sse",
            status: Some(200),
            outcome: "succeeded",
            request_kind: None,
            input_tokens: None,
            output_tokens: None,
            total_tokens: None,
            image_generation_succeeded: None,
            amount: None,
            recovered: false,
            pending: false,
        },
        UsageMatrixFact {
            id: "matrix_pending",
            account_id: "acct_usage_matrix_a",
            provider_kind: "example",
            model: Some("gpt-matrix"),
            transport: "http_sse",
            status: None,
            outcome: "running",
            request_kind: None,
            input_tokens: None,
            output_tokens: None,
            total_tokens: None,
            image_generation_succeeded: None,
            amount: None,
            recovered: false,
            pending: true,
        },
        UsageMatrixFact {
            id: "matrix_other_account",
            account_id: "acct_usage_matrix_b",
            provider_kind: "example",
            model: Some("gpt-matrix"),
            transport: "http_sse",
            status: Some(200),
            outcome: "succeeded",
            request_kind: None,
            input_tokens: Some(155),
            output_tokens: Some(0),
            total_tokens: Some(155),
            image_generation_succeeded: None,
            amount: Some("0.00000001"),
            recovered: false,
            pending: false,
        },
    ];
    for (index, fact) in facts.into_iter().enumerate() {
        insert_usage_matrix_fact(&pool, fact, start_us + index as i64 * 10_000_000).await;
    }

    let store = SqliteAdminAccountStore::new(pool.clone());
    let range = TimeRange::new(now - Duration::hours(1), now + Duration::hours(1)).unwrap();
    let account_usage = store
        .load_account_usage(range, &["acct_usage_matrix_a".to_owned()])
        .await
        .unwrap();
    assert_eq!(account_usage[0].request_count, 4);
    assert_eq!(account_usage[0].total_tokens, Some(150));
    assert_eq!(account_usage[0].costs[0].amount.as_str(), "0.12500006");
    assert_eq!(account_usage[0].models[0].request_count, 3);
    assert_eq!(account_usage[0].models[0].input_tokens, Some(150));

    let page = store
        .list_accounts(
            AccountListQuery {
                page: 1,
                page_size: PageSize::new(20).unwrap(),
                provider_kind: Some(provider),
                group_filter: None,
                search: None,
                status: None,
                sort: Some(AccountSort {
                    field: AccountSortField::Usage,
                    direction: SortDirection::Desc,
                }),
            },
            AccountRuntimeSnapshot::default(),
        )
        .await
        .unwrap();
    assert_eq!(page.items[0].account.id, "acct_usage_matrix_b");
    assert_eq!(page.items[1].account.id, "acct_usage_matrix_a");

    let group_page = groups
        .list_account_groups(AccountGroupListQuery {
            page: 1,
            page_size: PageSize::new(20).unwrap(),
            search: None,
            enabled: None,
        })
        .await
        .unwrap();
    // 分组成本查询沿用现有资格合同，其中恢复请求仍计入保留成本
    assert_eq!(
        group_page.items[0].usage.retained_total_usd.as_str(),
        "0.1250001"
    );

    let history = store
        .load_quota_forecast_history(&AccountUsageWindowQuery {
            account_id: "acct_usage_matrix_a".to_owned(),
            key: "matrix".to_owned(),
            range,
        })
        .await
        .unwrap();
    assert_eq!(history.usage.request_count, 4);
    assert_eq!(history.usage.tokens, 150);
    assert_eq!(history.usage.missing_token_count, 2);
    assert_eq!(history.usage.known_cost_count, 3);
    assert_eq!(history.usage.unavailable_cost_count, 1);
    assert!((history.usage.usd - 0.12500006).abs() < f64::EPSILON);
    assert_eq!(history.usage.excluded_request_count, 5);
    assert_eq!(history.pending_request_count, 1);
    pool.close().await;
}

#[derive(Clone, Copy)]
struct UsageMatrixFact<'a> {
    id: &'a str,
    account_id: &'a str,
    provider_kind: &'a str,
    model: Option<&'a str>,
    transport: &'a str,
    status: Option<i64>,
    outcome: &'a str,
    request_kind: Option<&'a str>,
    input_tokens: Option<i64>,
    output_tokens: Option<i64>,
    total_tokens: Option<i64>,
    image_generation_succeeded: Option<i64>,
    amount: Option<&'a str>,
    recovered: bool,
    pending: bool,
}

async fn insert_usage_matrix_fact(
    pool: &sqlx::SqlitePool,
    fact: UsageMatrixFact<'_>,
    started_at_us: i64,
) {
    use std::str::FromStr as _;

    let completed_at_us = (!fact.pending).then_some(started_at_us + 1_000_000);
    let committed_at_us = (!fact.pending).then_some(started_at_us + 500_000);
    let recovered_at_us = fact
        .recovered
        .then(|| completed_at_us.expect("recovered fact is complete") + 1_000_000);
    let recovery_request_id = fact.recovered.then_some("matrix-recovery");
    let recovery_attempt_count = i64::from(fact.recovered);
    let amount = fact.amount.map(|amount| {
        gateway_store::sqlite::value::encode_amount(
            gateway_core::metering::Decimal::from_str(amount).unwrap(),
        )
    });
    let cost_source = if amount.is_some() {
        "provider_reported"
    } else {
        "unavailable"
    };
    let currency = fact.amount.map(|_| "USD");
    let image_requested = i64::from(fact.image_generation_succeeded.is_some());
    sqlx::query(
        "insert into model_requests (
           id, client_api_key_ref, operation, client_transport, requested_model_id,
           provider_kind, upstream_model_id, provider_account_ref, upstream_transport,
           attempt_count, upstream_send_state, downstream_committed_at_us, outcome,
           client_status_code, input_tokens, output_tokens, total_tokens, cost_source,
           cost_amount, cost_currency, started_at_us, deadline_at_us, completed_at_us,
           request_kind, image_generation_requested, image_generation_succeeded,
           recovery_request_id, recovered_at_us, recovery_attempt_count, provider_observation_json,
           request_observation_json
         ) values (
           ?1, 'key_usage_matrix', 'generate', ?2, ?3, ?4, ?3, ?5, ?2, 1, 'sent',
           ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20,
           ?21, ?22, ?23, '{}',
           json_object('request', json_object('configRevision', 1, 'protocol', 'openai',
             'endpoint', '/v1/responses', 'compact', json('false')),
             'routing', json_object('scope', 'all', 'groupRefs', json('[]'),
             'groupNamesSnapshot', json('[]')))
         )",
    )
    .bind(fact.id)
    .bind(fact.transport)
    .bind(fact.model)
    .bind(fact.provider_kind)
    .bind(fact.account_id)
    .bind(committed_at_us)
    .bind(fact.outcome)
    .bind(fact.status)
    .bind(fact.input_tokens)
    .bind(fact.output_tokens)
    .bind(fact.total_tokens)
    .bind(cost_source)
    .bind(amount)
    .bind(currency)
    .bind(started_at_us)
    .bind(started_at_us + 60_000_000)
    .bind(completed_at_us)
    .bind(fact.request_kind)
    .bind(image_requested)
    .bind(fact.image_generation_succeeded)
    .bind(recovery_request_id)
    .bind(recovered_at_us)
    .bind(recovery_attempt_count)
    .execute(pool)
    .await
    .unwrap();
    if fact.recovered {
        sqlx::query(
            "update model_requests set request_observation_json = json_set(
               request_observation_json, '$.recovery.retryDelayMs', 0,
               '$.recovery.totalLatencyMs', 0
             ) where id = ?1",
        )
        .bind(fact.id)
        .execute(pool)
        .await
        .unwrap();
    }
}
