use chrono::Utc;
use gateway_store::{
    SqliteStoreConfig,
    postgres::{AdminAuditActorKind, AdminAuditEvent, AdminSecurityAuditRepository},
    sqlite,
    sqlite::SqliteAdminSecurityAuditRepository,
};

#[tokio::test]
async fn sqlite_password_changes_and_audit_events_commit_atomically() {
    let root = tempfile::tempdir().expect("SQLite data directory");
    let pool = sqlite::connect_and_migrate(
        &root.path().join("admin-security.sqlite3"),
        &SqliteStoreConfig::default(),
    )
    .await
    .expect("create SQLite database");
    let repository = SqliteAdminSecurityAuditRepository::new(pool.clone());

    assert!(
        repository
            .create_password_hash_if_absent("admin_one", "hash_one")
            .await
            .expect("create initial password")
    );
    assert!(
        !repository
            .create_password_hash_if_absent("admin_one", "ignored_hash")
            .await
            .expect("keep existing password")
    );
    assert!(
        repository
            .change_password(
                "admin_one",
                "hash_one",
                "hash_two",
                audit("audit_password_changed", "admin_one"),
            )
            .await
            .expect("change password and append audit")
    );
    assert!(
        !repository
            .change_password(
                "admin_one",
                "hash_one",
                "hash_three",
                audit("audit_stale_password", "admin_one"),
            )
            .await
            .expect("reject stale expected password hash")
    );
    assert_eq!(
        repository
            .password_hash("admin_one")
            .await
            .expect("load current password")
            .as_deref(),
        Some("hash_two")
    );

    assert!(
        repository
            .create_password_hash_if_absent("admin_two", "second_hash")
            .await
            .expect("create second admin")
    );
    let mut invalid_identity = audit("audit_invalid_identity", "admin_two");
    invalid_identity.actor_kind = AdminAuditActorKind::AdminApiKey;
    assert!(
        repository
            .change_password("admin_two", "second_hash", "changed_hash", invalid_identity,)
            .await
            .is_err()
    );
    assert_eq!(
        repository
            .password_hash("admin_two")
            .await
            .expect("load password after rolled-back audit")
            .as_deref(),
        Some("second_hash")
    );
    let audit_count: i64 = sqlx::query_scalar(
        "select count(*) from admin_audit_events where actor_admin_user_id = 'admin_two'",
    )
    .fetch_one(&pool)
    .await
    .expect("count audits for second admin");
    assert_eq!(audit_count, 0);
    pool.close().await;
}

fn audit(id: &str, admin_user_id: &str) -> AdminAuditEvent {
    AdminAuditEvent {
        id: id.to_owned(),
        actor_kind: AdminAuditActorKind::AdminSession,
        actor_admin_user_id: Some(admin_user_id.to_owned()),
        actor_ref: format!("admin:{admin_user_id}"),
        admin_request_id: Some("req_admin_password".to_owned()),
        action: "auth.password_changed".to_owned(),
        entity_kind: "admin_user".to_owned(),
        entity_ref: admin_user_id.to_owned(),
        config_revision: None,
        changed_fields: vec!["password_hash".to_owned()],
        created_at: Utc::now(),
    }
}

#[tokio::test]
async fn sqlite_auth_store_keeps_sessions_process_local_and_limits_login_attempts() {
    use std::time::Duration;

    use chrono::Duration as ChronoDuration;
    use gateway_admin::model::auth::{AuthSession, SessionSubject};

    let root = tempfile::tempdir().expect("SQLite data directory");
    let pool = sqlite::connect_and_migrate(
        &root.path().join("admin-auth.sqlite3"),
        &SqliteStoreConfig::default(),
    )
    .await
    .expect("create SQLite database");
    let first_process = sqlite::admin_auth_store(pool.clone());
    let second_process = sqlite::admin_auth_store(pool.clone());
    let session = AuthSession {
        subject: SessionSubject::Admin {
            admin_user_id: "admin_local".to_owned(),
            credential_fingerprint: "credential-fingerprint".to_owned(),
        },
        expires_at: Utc::now() + ChronoDuration::minutes(10),
    };
    first_process
        .store_session("session_local", &session)
        .await
        .expect("store process-local session");
    assert_eq!(
        first_process
            .load_session("session_local")
            .await
            .expect("load session"),
        Some(session),
    );
    assert!(
        second_process
            .load_session("session_local")
            .await
            .expect("other process does not load local session")
            .is_none()
    );

    let source = "127.0.0.1".parse().expect("source IP");
    assert!(
        first_process
            .consume_login_attempt(source, 1, 10, Duration::from_secs(60))
            .await
            .expect("first login attempt")
            .is_none()
    );
    assert!(
        first_process
            .consume_login_attempt(source, 1, 10, Duration::from_secs(60))
            .await
            .expect("second login attempt is rate limited")
            .is_some()
    );
    pool.close().await;
}
