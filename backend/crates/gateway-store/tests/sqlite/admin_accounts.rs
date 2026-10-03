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
               id, client_api_key_ref, config_revision, protocol, operation, endpoint,
               client_transport, requested_model_id, provider_kind, upstream_model_id,
               provider_account_ref, upstream_transport, attempt_count, upstream_send_state,
               downstream_committed_at_us, outcome, client_status_code, input_tokens,
               output_tokens, total_tokens, cost_source, cost_amount, cost_currency,
               started_at_us, deadline_at_us, completed_at_us, routing_scope
             ) values (
               ?1, 'key_test', 1, 'openai', 'generate', '/v1/responses',
               'http_sse', 'public-model', 'example', 'upstream-model',
               ?2, 'websocket', 1, 'sent', ?3, 'succeeded', 200, ?4,
               0, ?4, 'provider_reported', ?5, 'USD', ?6, ?7, ?7, 'all'
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
