use gateway_admin::{
    model::{
        MutationActor, MutationContext, PageSize,
        account_groups::{
            AccountGroupColor, AccountGroupListQuery, DeleteAccountGroup, NewAccountGroup,
            SetAccountGroupEnabled, UpdateAccountGroup,
        },
    },
    ports::store::AccountGroupStore,
};
use gateway_core::routing::AccountGroupId;
use gateway_store::{SqliteStoreConfig, sqlite, sqlite::SqliteAccountGroupRepository};

#[tokio::test]
async fn sqlite_account_groups_preserve_cas_audit_membership_and_list_contracts() {
    let root = tempfile::tempdir().expect("SQLite directory");
    let pool = sqlite::connect_and_migrate(
        &root.path().join("account-groups.sqlite3"),
        &SqliteStoreConfig::default(),
    )
    .await
    .expect("migrate SQLite");
    let repository = SqliteAccountGroupRepository::new(pool.clone());
    let id = AccountGroupId::new("grp_0123456789abcdef0123456789abcdef").unwrap();
    let color = AccountGroupColor::parse("#2563ebff").unwrap();
    let create = repository
        .create_account_group(
            NewAccountGroup {
                disable_fast: true,
                id: id.clone(),
                name: "Production".to_owned(),
                description: Some("primary account pool".to_owned()),
                color: color.clone(),
            },
            &context("group-create"),
        )
        .await
        .unwrap();
    assert_eq!(create.config_revision.get(), 2);
    assert!(create.record.as_ref().unwrap().disable_fast);

    let update = repository
        .update_account_group(
            UpdateAccountGroup {
                disable_fast: None,
                id: id.clone(),
                name: "Production Renamed".to_owned(),
                description: None,
                color,
            },
            &context("group-update"),
        )
        .await
        .unwrap();
    assert!(update.record.as_ref().unwrap().disable_fast);
    assert_eq!(update.record.as_ref().unwrap().name, "Production Renamed");

    let disabled = repository
        .set_account_group_enabled(
            SetAccountGroupEnabled {
                id: id.clone(),
                enabled: false,
            },
            &context("group-disable"),
        )
        .await
        .unwrap();
    assert!(!disabled.record.as_ref().unwrap().enabled);

    let page = repository
        .list_account_groups(AccountGroupListQuery {
            page: 1,
            page_size: PageSize::new(20).unwrap(),
            search: Some("renamed".to_owned()),
            enabled: Some(false),
        })
        .await
        .unwrap();
    assert_eq!(page.config_revision.get(), 4);
    assert_eq!(page.total, 1);
    assert_eq!(page.items[0].id, id);
    assert_eq!(page.items[0].member_count, 0);
    assert!(page.items[0].provider_counts.is_empty());
    assert_eq!(page.items[0].usage.today_usd.as_str(), "0");
    assert_eq!(page.items[0].usage.retained_total_usd.as_str(), "0");

    let deleted = repository
        .delete_account_group(
            DeleteAccountGroup { id: id.clone() },
            &context("group-delete"),
        )
        .await
        .unwrap();
    assert_eq!(deleted.config_revision.get(), 5);
    assert!(deleted.record.is_none());

    let audit_count: i64 = sqlx::query_scalar(
        "select count(*) from admin_audit_events where entity_kind = 'account_group'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(audit_count, 4);
    pool.close().await;
}

#[tokio::test]
async fn sqlite_account_group_member_facts_use_persisted_status_and_default_slots() {
    let root = tempfile::tempdir().expect("SQLite directory");
    let pool = sqlite::connect_and_migrate(
        &root.path().join("account-group-members.sqlite3"),
        &SqliteStoreConfig::default(),
    )
    .await
    .expect("migrate SQLite");
    let repository = SqliteAccountGroupRepository::new(pool.clone());
    let group = AccountGroupId::new("grp_abcdef0123456789abcdef0123456789").unwrap();
    repository
        .create_account_group(
            NewAccountGroup {
                disable_fast: false,
                id: group.clone(),
                name: "Member facts".to_owned(),
                description: None,
                color: AccountGroupColor::parse("#2563EBFF").unwrap(),
            },
            &context("member-group"),
        )
        .await
        .unwrap();

    let now = chrono::Utc::now().timestamp_micros();
    sqlx::query(
        "insert into provider_accounts (
           id, provider_kind, name, authentication_kind, provider_credentials_json,
           credential_revision, has_refresh_token, enabled, weight, model_access_json,
           credential_state, credential_observed_at_us, quota_access_state,
           quota_access_observed_at_us, created_at_us, updated_at_us
         ) values (
           'account-group-member', 'openai', 'Member', 'oauth', '{}',
           1, 0, 1, 1, '{\"mode\":\"all\",\"models\":[]}',
           'ready', ?1, 'allowed', ?1, ?1, ?1
         )",
    )
    .bind(now)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "insert into account_group_accounts (account_group_id, provider_account_id, created_at_us)
         values (?1, 'account-group-member', ?2)",
    )
    .bind(group.as_str())
    .bind(now)
    .execute(&pool)
    .await
    .unwrap();

    let members = repository
        .load_account_group_members(std::slice::from_ref(&group))
        .await
        .unwrap();
    assert_eq!(members.len(), 1);
    assert_eq!(members[0].group_id, group);
    assert_eq!(members[0].account_id, "account-group-member");
    assert!(members[0].status.enabled);
    assert_eq!(
        members[0].status.credential_state,
        gateway_core::account::CredentialState::Ready
    );
    assert_eq!(members[0].total_slots, Some(3));

    let page = repository
        .list_account_groups(AccountGroupListQuery {
            page: 1,
            page_size: PageSize::new(20).unwrap(),
            search: None,
            enabled: None,
        })
        .await
        .unwrap();
    assert_eq!(page.items[0].member_count, 1);
    assert_eq!(page.items[0].provider_counts.get("openai"), Some(&1));
    pool.close().await;
}

#[tokio::test]
async fn sqlite_account_group_usage_includes_retained_costs_and_separates_today() {
    use std::str::FromStr as _;

    let root = tempfile::tempdir().expect("SQLite directory");
    let pool = sqlite::connect_and_migrate(
        &root.path().join("account-group-usage.sqlite3"),
        &SqliteStoreConfig::default(),
    )
    .await
    .expect("migrate SQLite");
    let repository = SqliteAccountGroupRepository::new(pool.clone());
    let group = AccountGroupId::new("grp_0123456789abcdef0123456789abcdef").unwrap();
    repository
        .create_account_group(
            NewAccountGroup {
                disable_fast: false,
                id: group.clone(),
                name: "Usage facts".to_owned(),
                description: None,
                color: AccountGroupColor::parse("#2563EBFF").unwrap(),
            },
            &context("usage-group"),
        )
        .await
        .unwrap();

    let now = chrono::Utc::now();
    let now_us = now.timestamp_micros();
    sqlx::query(
        "insert into provider_accounts (
           id, provider_kind, name, authentication_kind, provider_credentials_json,
           credential_revision, has_refresh_token, enabled, weight, model_access_json,
           credential_state, credential_observed_at_us, quota_access_state,
           quota_access_observed_at_us, created_at_us, updated_at_us
         ) values (
           'account-group-usage', 'openai', 'Usage', 'oauth', '{}',
           1, 0, 1, 1, '{\"mode\":\"all\",\"models\":[]}',
           'ready', ?1, 'allowed', ?1, ?1, ?1
         )",
    )
    .bind(now_us)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "insert into account_group_accounts (account_group_id, provider_account_id, created_at_us)
         values (?1, 'account-group-usage', ?2)",
    )
    .bind(group.as_str())
    .bind(now_us)
    .execute(&pool)
    .await
    .unwrap();

    let today_start = gateway_core::time::DeploymentTimeZone::default()
        .day_start(now)
        .expect("business day start")
        .timestamp_micros();
    let historical_start = today_start - chrono::Duration::days(10).num_microseconds().unwrap();
    for (request_id, amount, started_at_us) in [
        ("req_group_usage_retained", "1.25", historical_start),
        ("req_group_usage_today", "2.5", today_start),
    ] {
        let amount = gateway_store::sqlite::value::encode_amount(
            gateway_core::metering::Decimal::from_str(amount).unwrap(),
        );
        sqlx::query(
            "insert into model_requests (
               id, client_api_key_ref, config_revision, protocol, operation, endpoint,
               client_transport, requested_model_id, outcome, client_status_code,
               downstream_committed_at_us, cost_source, cost_amount, cost_currency,
               started_at_us, deadline_at_us, completed_at_us, routing_scope,
               provider_account_ref, provider_kind
             ) values (
               ?1, 'key-group-usage', 1, 'openai', 'responses.create', '/v1/responses',
               'http', 'gpt-5', 'succeeded', 200, ?2, 'calculated', ?3, 'USD',
               ?4, ?5, ?6, 'legacy_provider', 'account-group-usage', 'openai'
             )",
        )
        .bind(request_id)
        .bind(started_at_us + 1_000_000)
        .bind(amount)
        .bind(started_at_us)
        .bind(started_at_us + 60_000_000)
        .bind(started_at_us + 1_000_000)
        .execute(&pool)
        .await
        .unwrap();
    }

    let page = repository
        .list_account_groups(AccountGroupListQuery {
            page: 1,
            page_size: PageSize::new(20).unwrap(),
            search: None,
            enabled: None,
        })
        .await
        .unwrap();
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].usage.today_usd.as_str(), "2.5");
    assert_eq!(page.items[0].usage.retained_total_usd.as_str(), "3.75");
    pool.close().await;
}

fn context(request_id: &str) -> MutationContext {
    MutationContext {
        actor: MutationActor::System,
        request_id: request_id.to_owned(),
    }
}
