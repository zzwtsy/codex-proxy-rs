use std::time::Duration;

use chrono::Utc;
use gateway_core::{
    lifecycle::CancellationToken,
    task::{
        ScheduledTask, WorkerContribution, WorkerCycleContext, WorkerId, WorkerKind, WorkerRunnable,
    },
};
use gateway_store::{SqliteStoreConfig, StoreConfig, sqlite};
use sqlx::SqlitePool;

struct Fixture {
    _root: tempfile::TempDir,
    pool: SqlitePool,
    task: Box<dyn ScheduledTask>,
    id: WorkerId,
}

impl Fixture {
    async fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("sessions.sqlite3");
        let mut config: StoreConfig = serde_json::from_value(serde_json::json!({
            "backend": "sqlite", "sqlite": { "path": path },
        }))
        .unwrap();
        config.resolve_and_validate(root.path()).unwrap();
        let mut bundle = gateway_store::initialize(config).await.unwrap();
        let worker = bundle
            .take_worker_contributions()
            .into_iter()
            .find_map(|worker| match worker {
                WorkerContribution::Registration(registration)
                    if registration.id.kind() == WorkerKind::Retention
                        && registration.id.owner() == "sqlite_sessions" =>
                {
                    Some(registration)
                }
                _ => None,
            })
            .expect("SQLite session cleanup worker");
        let WorkerRunnable::Scheduled { schedule, task, .. } = worker.runnable else {
            panic!("cleanup must use Host scheduling");
        };
        assert_eq!(schedule.interval(), Duration::from_secs(30));
        let pool = sqlite::connect_and_migrate(&path, &SqliteStoreConfig::default())
            .await
            .unwrap();
        Self {
            _root: root,
            pool,
            task,
            id: worker.id,
        }
    }

    async fn cycle(&self) {
        self.task
            .run_cycle(WorkerCycleContext::new(
                self.id.clone(),
                None,
                CancellationToken::new(),
            ))
            .await
            .unwrap();
    }

    async fn counts(&self) -> (i64, i64, i64) {
        sqlx::query_as("select (select count(*) from provider_session_affinity), (select count(*) from provider_session_exclusions), (select count(*) from provider_session_aliases)")
            .fetch_one(&self.pool).await.unwrap()
    }

    async fn seed(&self, count: i64, expiry: i64) {
        sqlx::query("with recursive numbers(n) as (values (1) union all select n + 1 from numbers where n < ?1)
            insert into provider_session_affinity (session_fingerprint, account_id, revision, expires_at_us)
            select 'session-' || n, 'acct_test', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', ?2 from numbers")
            .bind(count).bind(expiry).execute(&self.pool).await.unwrap();
        sqlx::query("insert into provider_session_exclusions (session_fingerprint, account_id, revision, expires_at_us)
            select session_fingerprint, account_id, 'generation-1', expires_at_us from provider_session_affinity")
            .execute(&self.pool).await.unwrap();
        sqlx::query("insert into provider_session_aliases (alias_fingerprint, session_key, follow_only, expires_at_us)
            select 'alias-' || substr(session_fingerprint, 9), 'opaque-session', 0, expires_at_us from provider_session_affinity")
            .execute(&self.pool).await.unwrap();
    }
}

#[tokio::test]
async fn sqlite_session_cleanup_bounds_each_table_and_preserves_live_sessions() {
    let fixture = Fixture::new().await;
    // 到期即应被清理，不要求原会话再次调用 load。
    fixture.seed(1006, Utc::now().timestamp_micros()).await;
    let future = Utc::now().timestamp_micros() + 86_400_000_000;
    sqlx::query("update provider_session_affinity set expires_at_us = ?1 where session_fingerprint = 'session-1006'")
        .bind(future).execute(&fixture.pool).await.unwrap();
    sqlx::query("update provider_session_exclusions set expires_at_us = ?1 where session_fingerprint = 'session-1006'")
        .bind(future).execute(&fixture.pool).await.unwrap();
    sqlx::query("update provider_session_aliases set expires_at_us = ?1 where alias_fingerprint = 'alias-1006'")
        .bind(future).execute(&fixture.pool).await.unwrap();
    fixture.cycle().await;
    assert_eq!(fixture.counts().await, (6, 6, 6));
    fixture.cycle().await;
    assert_eq!(fixture.counts().await, (1, 1, 1));
    fixture.cycle().await;
    assert_eq!(fixture.counts().await, (1, 1, 1));
    let survivor: String =
        sqlx::query_scalar("select session_fingerprint from provider_session_affinity")
            .fetch_one(&fixture.pool)
            .await
            .unwrap();
    assert_eq!(survivor, "session-1006");
    fixture.pool.close().await;
}

#[tokio::test]
async fn sqlite_session_cleanup_observes_renewal_committed_while_waiting_for_writer() {
    let fixture = Fixture::new().await;
    fixture.seed(1, 0).await;
    let mut renewal = fixture.pool.begin().await.unwrap();
    let future = Utc::now().timestamp_micros() + 86_400_000_000;
    sqlx::query("update provider_session_affinity set expires_at_us = ?1")
        .bind(future)
        .execute(&mut *renewal)
        .await
        .unwrap();
    sqlx::query("update provider_session_exclusions set expires_at_us = ?1")
        .bind(future)
        .execute(&mut *renewal)
        .await
        .unwrap();
    sqlx::query("update provider_session_aliases set expires_at_us = ?1")
        .bind(future)
        .execute(&mut *renewal)
        .await
        .unwrap();
    let pool = fixture.pool.clone();
    let mut cleanup = tokio::spawn(async move {
        fixture.cycle().await;
        fixture
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut cleanup)
            .await
            .is_err()
    );
    renewal.commit().await.unwrap();
    let fixture = cleanup.await.unwrap();
    assert_eq!(fixture.counts().await, (1, 1, 1));
    pool.close().await;
}

#[tokio::test]
async fn sqlite_session_cleanup_retries_after_partial_failure_and_honors_cancellation() {
    let fixture = Fixture::new().await;
    fixture.seed(1, 0).await;
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    fixture
        .task
        .run_cycle(WorkerCycleContext::new(
            fixture.id.clone(),
            None,
            cancellation,
        ))
        .await
        .unwrap();
    assert_eq!(fixture.counts().await, (1, 1, 1));
    sqlx::query("alter table provider_session_exclusions rename to saved_exclusions")
        .execute(&fixture.pool)
        .await
        .unwrap();
    assert!(
        fixture
            .task
            .run_cycle(WorkerCycleContext::new(
                fixture.id.clone(),
                None,
                CancellationToken::new()
            ))
            .await
            .is_err()
    );
    let affinity_count: i64 = sqlx::query_scalar("select count(*) from provider_session_affinity")
        .fetch_one(&fixture.pool)
        .await
        .unwrap();
    assert_eq!(affinity_count, 0);
    sqlx::query("alter table saved_exclusions rename to provider_session_exclusions")
        .execute(&fixture.pool)
        .await
        .unwrap();
    fixture.cycle().await;
    assert_eq!(fixture.counts().await, (0, 0, 0));
    fixture.pool.close().await;
}
