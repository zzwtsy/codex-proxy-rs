use gateway_admin::{
    model::{
        MutationActor, MutationContext, PageSize,
        proxies::{NewProxy, ProxyListQuery, ProxyLocationDetection, ProxyTestResult, UpdateProxy},
    },
    ports::{proxy::ProxyStore, store::AdminStoreErrorKind},
};
use gateway_core::account::{OutboundProxy, RequestLocation};
use gateway_store::{SqliteStoreConfig, sqlite, sqlite::SqliteProxyRepository};

#[tokio::test]
async fn sqlite_managed_proxy_mutations_are_revisioned_and_imports_are_reserved() {
    let root = tempfile::tempdir().expect("SQLite directory");
    let pool = sqlite::connect_and_migrate(
        &root.path().join("proxies.sqlite3"),
        &SqliteStoreConfig::default(),
    )
    .await
    .expect("migrate SQLite");
    let repository = SqliteProxyRepository::new(pool.clone());
    let created = repository
        .create(
            NewProxy {
                auto_location: true,
                test: Some(ProxyTestResult {
                    location: ProxyLocationDetection::Detected {
                        location: RequestLocation::default(),
                    },
                    success: true,
                    latency_ms: 42,
                    exit_ip: Some("192.0.2.7".parse().unwrap()),
                    exit_ipv4: Some("192.0.2.7".parse().unwrap()),
                    exit_ipv6: None,
                    message: "connected".to_owned(),
                }),
                location: None,
                name: "Primary".to_owned(),
                proxy: OutboundProxy::parse("http://user:pass@proxy.example:8080").unwrap(),
            },
            &context("proxy-create"),
        )
        .await
        .unwrap();
    assert_eq!(created.config_revision.get(), 2);
    assert!(created.record.auto_location);
    assert!(created.record.detected_location.is_some());
    assert_eq!(created.record.last_test.as_ref().unwrap().latency_ms, 42);

    let reservation = repository.reserve_import(&created.record.id).await.unwrap();
    assert_eq!(reservation.binding.id, created.record.id);
    let duplicate_reservation = repository.reserve_import(&created.record.id).await;
    assert!(matches!(
        duplicate_reservation,
        Err(ref error) if error.kind() == AdminStoreErrorKind::Conflict
    ));
    drop(reservation);
    assert!(repository.reserve_import(&created.record.id).await.is_ok());

    let renamed = repository
        .update(
            UpdateProxy {
                auto_location: None,
                test: None,
                location: None,
                id: created.record.id.clone(),
                revision: created.record.revision,
                name: "Primary renamed".to_owned(),
                proxy: Some(OutboundProxy::parse("http://user:pass@proxy2.example:8080").unwrap()),
            },
            &context("proxy-update"),
        )
        .await
        .unwrap();
    assert_eq!(renamed.config_revision.get(), 3);
    assert_eq!(renamed.record.revision.get(), 2);
    assert_eq!(renamed.record.name, "Primary renamed");
    assert!(renamed.record.detected_location.is_none());

    let stale = repository
        .record_test(
            &renamed.record.id,
            created.record.revision,
            ProxyTestResult {
                location: ProxyLocationDetection::NotRequested,
                success: true,
                latency_ms: 1,
                exit_ip: None,
                exit_ipv4: None,
                exit_ipv6: None,
                message: String::new(),
            },
            &context("proxy-stale-test"),
        )
        .await
        .unwrap_err();
    assert_eq!(stale.kind(), AdminStoreErrorKind::Conflict);

    let page = repository
        .list(ProxyListQuery {
            page: 1,
            page_size: PageSize::new(20).unwrap(),
            search: "renamed".to_owned(),
        })
        .await
        .unwrap();
    assert_eq!(page.total, 1);
    assert_eq!(page.items[0].id, renamed.record.id);

    let deleted = repository
        .delete(
            &renamed.record.id,
            renamed.record.revision,
            &context("proxy-delete"),
        )
        .await
        .unwrap();
    assert_eq!(deleted.get(), 4);
    pool.close().await;
}

fn context(request_id: &str) -> MutationContext {
    MutationContext {
        actor: MutationActor::System,
        request_id: request_id.to_owned(),
    }
}
