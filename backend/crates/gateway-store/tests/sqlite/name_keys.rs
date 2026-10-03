use gateway_admin::{
    model::{
        MutationActor, MutationContext, PageSize,
        account_groups::{
            AccountGroupColor, AccountGroupListQuery, NewAccountGroup, UpdateAccountGroup,
        },
        client_keys::{
            ClientKeyListQuery, ClientKeyPageSize, ClientKeySort, ClientKeySortField, NewClientKey,
            SortDirection, UpdateClientKey,
        },
    },
    ports::store::{AccountGroupStore, ClientKeyStore},
};
use gateway_core::{
    policy::{ClientApiKeyId, RateLimits},
    routing::AccountGroupId,
};
use gateway_store::{
    SqliteStoreConfig, sqlite,
    sqlite::{SqliteAccountGroupRepository, SqliteAdminClientKeyStore},
};
use sqlx::{SqlitePool, sqlite::SqliteConnectOptions, sqlite::SqlitePoolOptions};

static TEST_MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../migrations/sqlite");

fn context(request_id: &str) -> MutationContext {
    MutationContext {
        actor: MutationActor::System,
        request_id: request_id.to_owned(),
    }
}

fn new_key(id: &str, name: &str, plaintext: &str) -> NewClientKey {
    NewClientKey {
        request_profile_overrides: Default::default(),
        id: ClientApiKeyId::new(id).expect("Client API Key ID"),
        name: name.to_owned(),
        label: None,
        group_ids: Vec::new(),
        limits: RateLimits {
            max_concurrency: 4,
            requests_per_minute: 30,
        },
        budget: Default::default(),
        plaintext: plaintext.to_owned(),
    }
}

async fn legacy_database(path: &std::path::Path) -> SqlitePool {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(path)
                .create_if_missing(true),
        )
        .await
        .expect("create legacy SQLite database");
    TEST_MIGRATOR
        .run_to(11, &pool)
        .await
        .expect("migrate legacy SQLite database to version 11");
    pool
}

#[tokio::test]
async fn sqlite_name_keys_normalize_duplicates_searches_updates_and_cursors() {
    let root = tempfile::tempdir().expect("SQLite data directory");
    let pool = sqlite::connect_and_migrate(
        &root.path().join("name-keys.sqlite3"),
        &SqliteStoreConfig::default(),
    )
    .await
    .expect("create SQLite database");
    let groups = SqliteAccountGroupRepository::new(pool.clone());
    let keys = SqliteAdminClientKeyStore::new(pool.clone());
    let color = AccountGroupColor::parse("#2563ebff").expect("group color");
    let first_group = AccountGroupId::new("grp_0123456789abcdef0123456789abcdef").unwrap();
    let second_group = AccountGroupId::new("grp_1123456789abcdef0123456789abcdef").unwrap();
    let sigma_group = AccountGroupId::new("grp_4123456789abcdef0123456789abcdef").unwrap();

    let created_group = groups
        .create_account_group(
            NewAccountGroup {
                disable_fast: false,
                id: first_group.clone(),
                name: "Straße".to_owned(),
                description: None,
                color: color.clone(),
            },
            &context("unicode-group-create-1"),
        )
        .await
        .expect("create Unicode account group");
    assert_eq!(created_group.record.as_ref().unwrap().name, "Straße");
    groups
        .create_account_group(
            NewAccountGroup {
                disable_fast: false,
                id: second_group.clone(),
                name: "CAFÉ".to_owned(),
                description: None,
                color: color.clone(),
            },
            &context("unicode-group-create-2"),
        )
        .await
        .expect("create accented account group");
    groups
        .create_account_group(
            NewAccountGroup {
                disable_fast: false,
                id: sigma_group,
                name: "ΟΣ".to_owned(),
                description: None,
                color: color.clone(),
            },
            &context("unicode-group-create-sigma"),
        )
        .await
        .expect("create Greek account group");
    for (id, name, request_id) in [
        (
            "grp_2123456789abcdef0123456789abcdef",
            "STRASSE",
            "unicode-group-duplicate-fold",
        ),
        (
            "grp_3123456789abcdef0123456789abcdef",
            "cafe\u{301}",
            "unicode-group-duplicate-normalization",
        ),
        (
            "grp_5123456789abcdef0123456789abcdef",
            "ος",
            "unicode-group-duplicate-sigma",
        ),
    ] {
        assert!(
            groups
                .create_account_group(
                    NewAccountGroup {
                        disable_fast: false,
                        id: AccountGroupId::new(id).unwrap(),
                        name: name.to_owned(),
                        description: None,
                        color: color.clone(),
                    },
                    &context(request_id),
                )
                .await
                .is_err(),
            "normalized duplicate group name must be rejected"
        );
    }
    let group_search = groups
        .list_account_groups(AccountGroupListQuery {
            page: 1,
            page_size: PageSize::new(20).unwrap(),
            search: Some("cafe\u{301}".to_owned()),
            enabled: None,
        })
        .await
        .expect("search account group by normalized name");
    assert_eq!(group_search.total, 1);
    assert_eq!(group_search.items[0].name, "CAFÉ");
    let updated_group = groups
        .update_account_group(
            UpdateAccountGroup {
                disable_fast: None,
                id: second_group.clone(),
                name: "Cafe\u{301}".to_owned(),
                description: None,
                color,
            },
            &context("unicode-group-update"),
        )
        .await
        .expect("update account group name");
    assert_eq!(updated_group.record.as_ref().unwrap().name, "Cafe\u{301}");

    let first_key_id = ClientApiKeyId::new("unicode_key_one").unwrap();
    let second_key_id = ClientApiKeyId::new("unicode_key_two").unwrap();
    let first_key = keys
        .create_client_key(
            new_key("unicode_key_one", "Straße Key", "sk_unicode_one"),
            &context("unicode-key-create-1"),
        )
        .await
        .expect("create Unicode Client API Key")
        .1;
    assert_eq!(first_key.name, "Straße Key");
    keys.create_client_key(
        new_key("unicode_key_two", "CAFÉ Key", "sk_unicode_two"),
        &context("unicode-key-create-2"),
    )
    .await
    .expect("create accented Client API Key");
    assert!(
        keys.create_client_key(
            new_key("unicode_key_three", "STRASSE KEY", "sk_unicode_three"),
            &context("unicode-key-duplicate-fold"),
        )
        .await
        .is_err(),
        "full case-fold duplicate Client API Key name must be rejected"
    );

    let updated_key = keys
        .update_client_key(
            UpdateClientKey {
                request_profile_override_updates: Default::default(),
                id: second_key_id.clone(),
                name: "Cafe\u{301} Key".to_owned(),
                label: None,
                group_ids: Vec::new(),
                limits: RateLimits {
                    max_concurrency: 4,
                    requests_per_minute: 30,
                },
                daily_limit_usd: None,
                weekly_limit_usd: None,
            },
            &context("unicode-key-update"),
        )
        .await
        .expect("update Client API Key name")
        .1;
    assert_eq!(updated_key.name, "Cafe\u{301} Key");

    let key_search = keys
        .list_client_keys(ClientKeyListQuery {
            cursor: None,
            page_size: ClientKeyPageSize::new(10).unwrap(),
            search: Some("café".to_owned()),
            sort: ClientKeySort {
                field: ClientKeySortField::Name,
                direction: SortDirection::Asc,
            },
        })
        .await
        .expect("search Client API Key by normalized name");
    assert_eq!(key_search.total, 1);
    assert_eq!(key_search.items[0].id, second_key_id);
    assert_eq!(key_search.items[0].name, "Cafe\u{301} Key");

    let first_page = keys
        .list_client_keys(ClientKeyListQuery {
            cursor: None,
            page_size: ClientKeyPageSize::new(1).unwrap(),
            search: None,
            sort: ClientKeySort {
                field: ClientKeySortField::Name,
                direction: SortDirection::Asc,
            },
        })
        .await
        .expect("first name-sorted Client API Key page");
    assert_eq!(first_page.items[0].id, second_key_id);
    let next_cursor = first_page.next_cursor.expect("next name cursor");
    let second_page = keys
        .list_client_keys(ClientKeyListQuery {
            cursor: Some(next_cursor),
            page_size: ClientKeyPageSize::new(1).unwrap(),
            search: None,
            sort: ClientKeySort {
                field: ClientKeySortField::Name,
                direction: SortDirection::Asc,
            },
        })
        .await
        .expect("second name-sorted Client API Key page");
    assert_eq!(second_page.items[0].id, first_key_id);
    pool.close().await;
}

#[tokio::test]
async fn sqlite_name_key_backfill_reports_conflicts_before_changing_schema() {
    let root = tempfile::tempdir().expect("SQLite data directory");
    let path = root.path().join("legacy-conflicts.sqlite3");
    let pool = legacy_database(&path).await;
    for (id, name) in [
        ("grp_0123456789abcdef0123456789abcdef", "Straße"),
        ("grp_1123456789abcdef0123456789abcdef", "STRASSE"),
    ] {
        sqlx::query(
            "insert into account_groups (id, name, created_at_us, updated_at_us)
             values (?1, ?2, 1, 1)",
        )
        .bind(id)
        .bind(name)
        .execute(&pool)
        .await
        .expect("seed conflicting legacy account groups");
    }
    for (id, name, key) in [
        ("legacy_key_one", "CAFÉ", "sk_legacy_one"),
        ("legacy_key_two", "cafe\u{301}", "sk_legacy_two"),
    ] {
        sqlx::query(
            "insert into client_api_keys (id, name, key, created_at_us, updated_at_us)
             values (?1, ?2, ?3, 1, 1)",
        )
        .bind(id)
        .bind(name)
        .bind(key)
        .execute(&pool)
        .await
        .expect("seed conflicting legacy Client API Keys");
    }
    pool.close().await;

    let error = sqlite::connect_and_migrate(&path, &SqliteStoreConfig::default())
        .await
        .expect_err("conflicting legacy names must stop migration");
    let message = error.to_string();
    for id in [
        "grp_0123456789abcdef0123456789abcdef",
        "grp_1123456789abcdef0123456789abcdef",
        "legacy_key_one",
        "legacy_key_two",
    ] {
        assert!(
            message.contains(id),
            "migration error should report {id}: {message}"
        );
    }

    let pool = sqlite::connect_read_only(&path, &SqliteStoreConfig::default())
        .await
        .expect("read legacy database after rejected migration");
    for table in ["account_groups", "client_api_keys"] {
        let has_name_key: i64 = sqlx::query_scalar(
            "select count(*) from pragma_table_info(?1) where name = 'name_key'",
        )
        .bind(table)
        .fetch_one(&pool)
        .await
        .expect("inspect legacy table");
        assert_eq!(
            has_name_key, 0,
            "migration must leave {table} schema unchanged"
        );
    }
    let applied_version: i64 =
        sqlx::query_scalar("select max(version) from _sqlx_migrations where success = 1")
            .fetch_one(&pool)
            .await
            .expect("read applied migration version");
    assert_eq!(applied_version, 11);
    let original_group_name: String = sqlx::query_scalar(
        "select name from account_groups where id = 'grp_0123456789abcdef0123456789abcdef'",
    )
    .fetch_one(&pool)
    .await
    .expect("read unchanged legacy group name");
    assert_eq!(original_group_name, "Straße");
    pool.close().await;
}

#[tokio::test]
async fn sqlite_name_key_backfill_preserves_display_names_and_adds_unique_indexes() {
    let root = tempfile::tempdir().expect("SQLite data directory");
    let path = root.path().join("legacy-name-keys.sqlite3");
    let pool = legacy_database(&path).await;
    sqlx::query(
        "insert into account_groups (id, name, created_at_us, updated_at_us)
         values ('grp_0123456789abcdef0123456789abcdef', 'Straße', 1, 1)",
    )
    .execute(&pool)
    .await
    .expect("seed legacy account group");
    sqlx::query(
        "insert into client_api_keys (id, name, key, created_at_us, updated_at_us)
         values ('legacy_key', 'CAFÉ', 'sk_legacy', 1, 1)",
    )
    .execute(&pool)
    .await
    .expect("seed legacy Client API Key");
    pool.close().await;

    let pool = sqlite::connect_and_migrate(&path, &SqliteStoreConfig::default())
        .await
        .expect("backfill legacy names and complete migrations");
    let (group_name, group_key): (String, String) = sqlx::query_as(
        "select name, name_key from account_groups where id = 'grp_0123456789abcdef0123456789abcdef'",
    )
    .fetch_one(&pool)
    .await
    .expect("read migrated group name");
    assert_eq!(group_name, "Straße");
    assert_eq!(group_key, "strasse");
    let (client_name, client_key): (String, String) =
        sqlx::query_as("select name, name_key from client_api_keys where id = 'legacy_key'")
            .fetch_one(&pool)
            .await
            .expect("read migrated Client API Key name");
    assert_eq!(client_name, "CAFÉ");
    assert_eq!(client_key, "café");

    let indexes: i64 = sqlx::query_scalar(
        "select count(*) from sqlite_master where type = 'index' and name in
         ('account_groups_name_key_uq', 'client_api_keys_name_key_uq')",
    )
    .fetch_one(&pool)
    .await
    .expect("read normalized name indexes");
    assert_eq!(indexes, 2);
    assert!(
        sqlx::query(
            "insert into account_groups (id, name, created_at_us, updated_at_us, name_key)
             values ('group_duplicate', 'STRASSE', 2, 2, 'strasse')",
        )
        .execute(&pool)
        .await
        .is_err(),
        "normalized account group index must reject duplicates"
    );
    assert!(
        sqlx::query(
            "insert into client_api_keys (id, name, key, created_at_us, updated_at_us, name_key)
             values ('key_duplicate', 'cafe', 'sk_duplicate', 2, 2, 'café')",
        )
        .execute(&pool)
        .await
        .is_err(),
        "normalized Client API Key index must reject duplicates"
    );
    pool.close().await;
}
