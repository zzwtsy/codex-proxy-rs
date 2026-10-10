//! SQLite 会话续期、冷却证据、无写锁读取与错误来源回归

use gateway_core::{
    account::{CredentialRevision, ProviderAccountId},
    provider_ports::{
        ProviderCooldown, ProviderCooldownKind, ProviderCooldownPort, ProviderSessionAffinityKey,
        ProviderSessionAffinityPort, ProviderSessionAlias, ProviderSessionExclusionPort,
        ProviderStoreErrorKind,
    },
    routing::ProviderKind,
};
use gateway_store::{
    SqliteStoreConfig,
    sqlite::{
        self, SqliteProviderCooldownRepository, SqliteProviderSessionAffinityRepository,
        SqliteProviderSessionExclusionRepository,
    },
};
use sqlx::SqlitePool;
use std::{
    error::Error,
    time::{Duration, SystemTime},
};

async fn fixture() -> (tempfile::TempDir, SqlitePool) {
    let root = tempfile::tempdir().unwrap();
    let pool = sqlite::connect_and_migrate(
        &root.path().join("state.sqlite3"),
        &SqliteStoreConfig::default(),
    )
    .await
    .unwrap();
    (root, pool)
}

#[tokio::test]
async fn successful_requests_preserve_frozen_and_newer_capacity_evidence() {
    let (_root, pool) = fixture().await;
    crate::support::provider_state::success_cleanup_contract(
        &SqliteProviderCooldownRepository::new(pool.clone()),
    )
    .await;
    pool.close().await;
}

#[tokio::test]
async fn expired_freeze_does_not_reject_new_rate_limit_but_probe_freeze_does() {
    let (_root, pool) = fixture().await;
    let repository = SqliteProviderCooldownRepository::new(pool.clone());
    let revision = CredentialRevision::new(1).unwrap();
    for (name, kind, expired, accepted) in [
        ("expired", ProviderCooldownKind::CapacityFreeze, true, true),
        ("active", ProviderCooldownKind::CapacityFreeze, false, false),
        (
            "probe",
            ProviderCooldownKind::CapacityFreezeProbe,
            true,
            false,
        ),
    ] {
        let account = ProviderAccountId::new(format!("acct_{name}")).unwrap();
        repository
            .put_if_later(ProviderCooldown::new_with_kind(
                account.clone(),
                revision,
                SystemTime::now() + Duration::from_secs(600),
                kind,
            ))
            .await
            .unwrap();
        if expired {
            sqlx::query("update provider_cooldowns set until_us = 0 where account_id = ?")
                .bind(account.as_str())
                .execute(&pool)
                .await
                .unwrap();
        }
        assert_eq!(
            repository
                .put_if_later(ProviderCooldown::new(
                    account.clone(),
                    revision,
                    SystemTime::now() + Duration::from_secs(60)
                ))
                .await
                .unwrap(),
            accepted,
            "{name}"
        );
        assert_eq!(
            repository.read(&account).await.unwrap().unwrap().kind(),
            if accepted {
                ProviderCooldownKind::RateLimit
            } else {
                kind
            }
        );
    }
    pool.close().await;
}

#[tokio::test]
async fn aliases_renew_nullable_roots_without_relaxing_identity() {
    let (_root, pool) = fixture().await;
    let repository = SqliteProviderSessionAffinityRepository::new(pool.clone());
    let provider = ProviderKind::new("openai").unwrap();
    for root in [
        None,
        Some(ProviderSessionAffinityKey::try_new("root").unwrap()),
    ] {
        let alias = ProviderSessionAffinityKey::try_new(if root.is_some() {
            "child-turn"
        } else {
            "root-turn"
        })
        .unwrap();
        let identity = ProviderSessionAlias {
            session_key: ProviderSessionAffinityKey::try_new("session").unwrap(),
            root_session_key: root,
            follow_only: false,
        };
        assert!(
            repository
                .bind_alias(&provider, &alias, &identity, Duration::from_secs(1))
                .await
                .unwrap()
        );
        assert!(
            repository
                .bind_alias(&provider, &alias, &identity, Duration::from_secs(600))
                .await
                .unwrap()
        );
        let remaining: i64 =
            sqlx::query_scalar("select min(expires_at_us) from provider_session_aliases")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(remaining > chrono::Utc::now().timestamp_micros() + 500_000_000);
        for field in 0..3 {
            let mut conflict = identity.clone();
            match field {
                0 => conflict.session_key = ProviderSessionAffinityKey::try_new("other").unwrap(),
                1 => {
                    conflict.root_session_key = if conflict.root_session_key.is_some() {
                        None
                    } else {
                        Some(ProviderSessionAffinityKey::try_new("other-root").unwrap())
                    }
                }
                _ => conflict.follow_only = true,
            }
            assert!(
                !repository
                    .bind_alias(&provider, &alias, &conflict, Duration::from_secs(600))
                    .await
                    .unwrap()
            );
        }
        sqlx::query("update provider_session_aliases set expires_at_us=0")
            .execute(&pool)
            .await
            .unwrap();
        let mut replacement = identity.clone();
        replacement.follow_only = true;
        assert!(
            repository
                .bind_alias(&provider, &alias, &replacement, Duration::from_secs(600))
                .await
                .unwrap()
        );
    }
    pool.close().await;
}

#[tokio::test]
async fn exclusion_failures_renew_the_whole_live_set_and_fence_clear() {
    let (_root, pool) = fixture().await;
    let repository = SqliteProviderSessionExclusionRepository::new(pool.clone());
    let provider = ProviderKind::new("openai").unwrap();
    let key = ProviderSessionAffinityKey::try_new("session").unwrap();
    let a = ProviderAccountId::new("acct_a").unwrap();
    let b = ProviderAccountId::new("acct_b").unwrap();
    let expired = ProviderAccountId::new("acct_expired").unwrap();
    repository
        .record_failure(&provider, &key, &expired, Duration::from_secs(60))
        .await
        .unwrap();
    sqlx::query("update provider_session_exclusions set expires_at_us=0")
        .execute(&pool)
        .await
        .unwrap();
    let previous = repository
        .record_failure(&provider, &key, &a, Duration::from_secs(60))
        .await
        .unwrap();
    let current = repository
        .record_failure(&provider, &key, &b, Duration::from_secs(600))
        .await
        .unwrap();
    assert_eq!(current.excluded_accounts().len(), 2);
    assert!(!current.excluded_accounts().contains(&expired));
    let expiries: Vec<i64> =
        sqlx::query_scalar("select expires_at_us from provider_session_exclusions")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert!(expiries.iter().all(|expiry| *expiry == expiries[0]
        && *expiry > chrono::Utc::now().timestamp_micros() + 500_000_000));
    assert!(
        !repository
            .clear(&provider, &key, previous.revision())
            .await
            .unwrap()
    );
    let repeated = repository
        .record_failure(&provider, &key, &b, Duration::from_secs(900))
        .await
        .unwrap();
    assert_ne!(repeated.revision(), current.revision());
    assert!(
        !repository
            .clear(&provider, &key, current.revision())
            .await
            .unwrap()
    );
    let min_expiry: i64 =
        sqlx::query_scalar("select min(expires_at_us) from provider_session_exclusions")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(min_expiry > chrono::Utc::now().timestamp_micros() + 800_000_000);
    let c = ProviderAccountId::new("acct_c").unwrap();
    let d = ProviderAccountId::new("acct_d").unwrap();
    let (left, right) = tokio::join!(
        repository.record_failure(&provider, &key, &c, Duration::from_secs(900)),
        repository.record_failure(&provider, &key, &d, Duration::from_secs(900))
    );
    assert!(left.is_ok() && right.is_ok());
    assert_eq!(
        repository
            .load(&provider, &key)
            .await
            .unwrap()
            .unwrap()
            .excluded_accounts()
            .len(),
        4
    );
    pool.close().await;
}

#[tokio::test]
async fn session_reads_do_not_wait_for_an_unrelated_writer() {
    let (_root, pool) = fixture().await;
    let affinity = SqliteProviderSessionAffinityRepository::new(pool.clone());
    let exclusions = SqliteProviderSessionExclusionRepository::new(pool.clone());
    let provider = ProviderKind::new("openai").unwrap();
    let account = ProviderAccountId::new("acct_read").unwrap();
    for name in ["expired", "live"] {
        let key = ProviderSessionAffinityKey::try_new(name).unwrap();
        affinity
            .compare_and_bind(&provider, &key, None, &account, Duration::from_secs(600))
            .await
            .unwrap();
        affinity
            .bind_alias(
                &provider,
                &key,
                &ProviderSessionAlias {
                    session_key: key.clone(),
                    root_session_key: None,
                    follow_only: false,
                },
                Duration::from_secs(600),
            )
            .await
            .unwrap();
        exclusions
            .record_failure(&provider, &key, &account, Duration::from_secs(600))
            .await
            .unwrap();
        if name == "expired" {
            for table in [
                "provider_session_affinity",
                "provider_session_aliases",
                "provider_session_exclusions",
            ] {
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "update {table} set expires_at_us=0"
                )))
                .execute(&pool)
                .await
                .unwrap();
            }
        }
    }
    let mut writer = pool.begin().await.unwrap();
    sqlx::query("update cpr_store_write_lock set revision=revision+1")
        .execute(&mut *writer)
        .await
        .unwrap();
    let reads = tokio::time::timeout(Duration::from_millis(500), async {
        for name in ["live", "missing", "expired"] {
            let key = ProviderSessionAffinityKey::try_new(name).unwrap();
            let expected = name == "live";
            assert_eq!(
                affinity.load(&provider, &key).await.unwrap().is_some(),
                expected
            );
            assert_eq!(
                affinity
                    .load_alias(&provider, &key)
                    .await
                    .unwrap()
                    .is_some(),
                expected
            );
            assert_eq!(
                exclusions.load(&provider, &key).await.unwrap().is_some(),
                expected
            );
        }
    })
    .await;
    writer.rollback().await.unwrap();
    assert!(reads.is_ok(), "reads must not acquire the writer lock");
    pool.close().await;
}

#[tokio::test]
async fn session_database_errors_keep_operation_and_source() {
    let (_root, pool) = fixture().await;
    let repository = SqliteProviderSessionAffinityRepository::new(pool.clone());
    sqlx::query("drop table provider_session_aliases")
        .execute(&pool)
        .await
        .unwrap();
    let error = repository
        .load_alias(
            &ProviderKind::new("openai").unwrap(),
            &ProviderSessionAffinityKey::try_new("private-session-marker").unwrap(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ProviderStoreErrorKind::Unavailable);
    assert!(error.to_string().contains("load provider session alias"));
    assert!(!error.to_string().contains("private-session-marker"));
    assert!(!error.to_string().contains("no such table"));
    assert!(error.source().is_some());
    pool.close().await;
}

#[tokio::test]
async fn malformed_state_is_invalid_data_and_cooldown_errors_keep_sources() {
    let (_root, pool) = fixture().await;
    let provider = ProviderKind::new("openai").unwrap();
    let key = ProviderSessionAffinityKey::try_new("session").unwrap();
    let account = ProviderAccountId::new("acct_decode").unwrap();
    let exclusions = SqliteProviderSessionExclusionRepository::new(pool.clone());
    exclusions
        .record_failure(&provider, &key, &account, Duration::from_secs(60))
        .await
        .unwrap();
    sqlx::query("update provider_session_exclusions set account_id = x'ff'")
        .execute(&pool)
        .await
        .unwrap();
    let error = exclusions.load(&provider, &key).await.unwrap_err();
    assert_eq!(error.kind(), ProviderStoreErrorKind::InvalidData);
    assert!(error.source().is_some());
    sqlx::query("drop table provider_cooldowns")
        .execute(&pool)
        .await
        .unwrap();
    let cooldowns = SqliteProviderCooldownRepository::new(pool.clone());
    let error = cooldowns.read(&account).await.unwrap_err();
    assert_eq!(error.kind(), ProviderStoreErrorKind::Unavailable);
    assert!(error.to_string().contains("read provider cooldown"));
    assert!(!error.to_string().contains("no such table"));
    assert!(
        error
            .source()
            .unwrap()
            .to_string()
            .contains("no such table")
    );
    pool.close().await;
}
