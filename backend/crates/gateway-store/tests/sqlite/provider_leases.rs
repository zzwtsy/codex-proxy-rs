//! 验证 Guardian 容量隔离、跨池请求间隔与调度信号

use std::{num::NonZeroU32, time::Duration};

use gateway_core::{
    account::{CredentialRevision, ProviderAccountId},
    policy::ClientApiKeyId,
    provider_ports::{
        ProviderConcurrencyPool, ProviderLeaseAcquisition, ProviderLeasePort, ProviderLeaseRequest,
        ProviderSchedulingLeaseRequest,
    },
    routing::ProviderKind,
};
use gateway_store::{SqliteStoreConfig, sqlite};

fn request(pool: ProviderConcurrencyPool, interval: Duration) -> ProviderLeaseRequest {
    ProviderLeaseRequest::Scheduling(
        ProviderSchedulingLeaseRequest::new(
            ProviderKind::new("openai").unwrap(),
            ProviderAccountId::new("acct_guardian").unwrap(),
            CredentialRevision::new(1).unwrap(),
            NonZeroU32::new(1).unwrap(),
            interval,
            gateway_core::lifecycle::Deadline::default(),
        )
        .with_concurrency_pool(pool),
    )
}

#[tokio::test]
async fn reserved_and_shared_capacity_are_independent_across_connections() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("guardian.sqlite3");
    let pool = sqlite::connect_and_migrate(&path, &SqliteStoreConfig::default())
        .await
        .unwrap();
    let other_pool = sqlite::connect_and_migrate(&path, &SqliteStoreConfig::default())
        .await
        .unwrap();
    let coordinator = sqlite::SqliteProviderLeaseCoordinator::new(
        sqlite::SqliteCredentialLeaseRepository::new(pool.clone()),
    );
    let other = sqlite::SqliteProviderLeaseCoordinator::new(
        sqlite::SqliteCredentialLeaseRepository::new(other_pool.clone()),
    );
    let shared = coordinator
        .try_acquire(request(ProviderConcurrencyPool::Shared, Duration::ZERO))
        .await
        .unwrap();
    assert!(matches!(shared, ProviderLeaseAcquisition::Acquired(_)));
    let reserved = other
        .try_acquire(request(ProviderConcurrencyPool::Reserved, Duration::ZERO))
        .await
        .unwrap();
    assert!(matches!(reserved, ProviderLeaseAcquisition::Acquired(_)));
    for concurrency_pool in [
        ProviderConcurrencyPool::Shared,
        ProviderConcurrencyPool::Reserved,
    ] {
        assert!(matches!(
            other
                .try_acquire(request(concurrency_pool, Duration::ZERO))
                .await
                .unwrap(),
            ProviderLeaseAcquisition::Busy { .. }
        ));
        let account = ProviderAccountId::new("acct_guardian").unwrap();
        let signals = other
            .load_state(
                &ClientApiKeyId::new("key_guardian").unwrap(),
                &ProviderKind::new("openai").unwrap(),
                std::slice::from_ref(&account),
                concurrency_pool,
            )
            .await
            .unwrap();
        assert_eq!(signals.signals()[&account].in_flight, 1);
        assert!(signals.signals()[&account].last_started_at.is_some());
    }
    drop(shared);
    drop(reserved);
    pool.close().await;
    other_pool.close().await;
}

#[tokio::test]
async fn account_request_interval_is_shared_in_both_pool_directions() {
    for first_pool in [
        ProviderConcurrencyPool::Shared,
        ProviderConcurrencyPool::Reserved,
    ] {
        let root = tempfile::tempdir().unwrap();
        let pool = sqlite::connect_and_migrate(
            &root.path().join("interval.sqlite3"),
            &SqliteStoreConfig::default(),
        )
        .await
        .unwrap();
        let coordinator = sqlite::SqliteProviderLeaseCoordinator::new(
            sqlite::SqliteCredentialLeaseRepository::new(pool.clone()),
        );
        let first = coordinator
            .try_acquire(request(first_pool, Duration::from_secs(60)))
            .await
            .unwrap();
        assert!(matches!(first, ProviderLeaseAcquisition::Acquired(_)));
        let other_pool = match first_pool {
            ProviderConcurrencyPool::Shared => ProviderConcurrencyPool::Reserved,
            ProviderConcurrencyPool::Reserved => ProviderConcurrencyPool::Shared,
        };
        let account = ProviderAccountId::new("acct_guardian").unwrap();
        let signals = coordinator
            .load_state(
                &ClientApiKeyId::new("key_guardian").unwrap(),
                &ProviderKind::new("openai").unwrap(),
                std::slice::from_ref(&account),
                other_pool,
            )
            .await
            .unwrap();
        assert_eq!(signals.signals()[&account].in_flight, 0);
        assert!(signals.signals()[&account].last_started_at.is_some());
        let blocked = coordinator
            .try_acquire(request(other_pool, Duration::from_secs(60)))
            .await
            .unwrap();
        assert!(matches!(
            blocked,
            ProviderLeaseAcquisition::Busy {
                retry_after: Some(_)
            }
        ));
        drop(first);
        pool.close().await;
    }
}
