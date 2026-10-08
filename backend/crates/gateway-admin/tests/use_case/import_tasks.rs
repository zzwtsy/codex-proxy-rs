//! 验证后台导入任务的调度、真实 SQLite 写入与受控失败诊断

use std::{sync::Arc, time::Duration};

use gateway_admin::{
    AdminBundle, AdminServices,
    model::{
        AdminErrorKind, MutationActor, MutationContext,
        import_tasks::{ImportTaskInput, SubmitImportTask},
        provider_credentials::ImportCredentials,
    },
    ports::provider::ProviderAdminErrorKind,
};
use gateway_core::{
    lifecycle::CancellationToken,
    routing::ProviderKind,
    task::{WorkerContribution, WorkerKind, WorkerRunnable},
};
use uuid::Uuid;

use super::{
    AdminHarness, RecordingDiagnostics,
    accounts::{EventLog, FakeAccountStore, FakeProviderAdmin, context, events, recorded},
};

fn command(id: Uuid, count: usize) -> SubmitImportTask {
    let context = context("task-submit");
    SubmitImportTask {
        submission_id: id,
        fingerprint: [1; 32],
        context: context.clone(),
        items: (0..count)
            .map(|_| ImportTaskInput {
                provider: ProviderKind::new("openai").unwrap(),
                command: ImportCredentials {
                    outbound_proxy_id: None,
                    settings: None,
                    context: context.clone(),
                    document: gateway_admin::model::provider_credentials::ProviderDocument::new(
                        gateway_core::account::OpaqueProviderData::new(
                            serde_json::json!({"token": "synthetic-import-input-secret"})
                                .as_object()
                                .unwrap()
                                .clone(),
                        ),
                    ),
                },
            })
            .collect(),
    }
}

async fn wait_diagnostics(diagnostics: &RecordingDiagnostics, count: usize) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while diagnostics.0.lock().unwrap().len() < count {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("import diagnostics recorded");
}

#[tokio::test]
async fn real_sqlite_import_worker_persists_settings_and_emits_no_failure() {
    use gateway_admin::{model::accounts::AccountImportSettings, ports::store::AccountStore};
    use gateway_store::{
        SqliteStoreConfig,
        sqlite::{self, SqliteAdminAccountStore},
    };
    let root = tempfile::tempdir().unwrap();
    let pool = sqlite::connect_and_migrate(
        &root.path().join("worker.sqlite3"),
        &SqliteStoreConfig::default(),
    )
    .await
    .unwrap();
    // 真实审计写入要求提交者对应的管理员已存在
    sqlite::admin_auth_store(pool.clone())
        .create_password_hash_if_absent("admin-test", "synthetic-password-hash")
        .await
        .unwrap();
    let accounts = Arc::new(SqliteAdminAccountStore::new(pool.clone()));
    let log = events();
    let diagnostics = Arc::new(RecordingDiagnostics::default());
    let mut bundle = AdminHarness::new()
        .accounts(accounts.clone())
        .provider(FakeProviderAdmin::new("openai", log.clone()))
        .diagnostics(diagnostics.clone())
        .build_bundle()
        .await;
    let services = bundle.services();
    let mut input = command(Uuid::now_v7(), 1);
    input.items[0].command.settings = Some(AccountImportSettings {
        enabled: false,
        concurrency_limit: gateway_core::account::AccountConcurrencyLimit::new(3),
        weight: gateway_core::account::AccountWeight::new(7).unwrap(),
        notes: Some("  worker notes  ".to_owned()),
        model_access: None,
        group_ids: Vec::new(),
    });
    let task = services.import_tasks().submit(input).unwrap();
    let (cancel, worker) = start(&mut bundle);
    wait_finished(&services, task.task_id).await;
    cancel.cancel();
    worker.await.unwrap();
    let result = services
        .import_tasks()
        .detail(&context("poll"), task.task_id)
        .unwrap();
    assert_eq!(
        (
            result.summary.counts.succeeded,
            result.summary.counts.imported_accounts
        ),
        (1, 1)
    );
    assert_eq!(result.items[0].account_ids[0].as_str(), "acct_prepared");
    let saved = accounts
        .load_account("acct_prepared", Default::default())
        .await
        .unwrap()
        .unwrap();
    assert!(!saved.account.enabled);
    assert_eq!(saved.account.concurrency_limit.unwrap().get(), 3);
    assert_eq!(saved.account.weight.get(), 7);
    assert_eq!(saved.account.notes.as_deref(), Some("worker notes"));
    assert!(saved.account.groups.is_empty());
    assert!(diagnostics.0.lock().unwrap().is_empty());
    pool.close().await;
}

#[tokio::test]
async fn closed_sqlite_import_worker_records_one_safe_failure_with_task_context() {
    use gateway_store::{
        SqliteStoreConfig,
        sqlite::{self, SqliteAdminAccountStore},
    };
    let root = tempfile::tempdir().unwrap();
    let pool = sqlite::connect_and_migrate(
        &root.path().join("closed-worker.sqlite3"),
        &SqliteStoreConfig::default(),
    )
    .await
    .unwrap();
    let log = events();
    let diagnostics = Arc::new(RecordingDiagnostics::default());
    let mut bundle = AdminHarness::new()
        .accounts(Arc::new(SqliteAdminAccountStore::new(pool.clone())))
        .provider(FakeProviderAdmin::new("openai", log.clone()))
        .diagnostics(diagnostics.clone())
        .build_bundle()
        .await;
    pool.close().await;
    let logs = Arc::new(ImportLogs::default());
    let _subscriber = tracing::subscriber::set_default(logs.clone());
    let services = bundle.services();
    let task = services
        .import_tasks()
        .submit(command(Uuid::now_v7(), 1))
        .unwrap();
    let (cancel, worker) = start(&mut bundle);
    wait_finished(&services, task.task_id).await;
    wait_diagnostics(&diagnostics, 1).await;
    for _ in 0..3 {
        let result = services
            .import_tasks()
            .detail(&context("poll"), task.task_id)
            .unwrap();
        assert_eq!(result.summary.counts.unknown, 1);
        assert_eq!(result.items[0].message.as_deref(), Some("依赖服务暂不可用"));
        assert!(result.items[0].account_ids.is_empty());
        assert!(!format!("{result:?}").contains("closed pool"));
        assert!(!format!("{result:?}").contains("synthetic-import-input-secret"));
    }
    cancel.cancel();
    worker.await.unwrap();
    let failures = diagnostics.0.lock().unwrap();
    assert_eq!(failures.len(), 1);
    let failure = &failures[0];
    assert_eq!(
        (failure.component, failure.operation, failure.kind),
        ("admin", "account_import", "unavailable")
    );
    assert_eq!(
        failure.correlation_id.as_deref(),
        Some(task.task_id.to_string().as_str())
    );
    assert_eq!(failure.provider_kind.as_ref().unwrap().as_str(), "openai");
    let details = failure.details.as_ref().unwrap().as_str();
    assert!(details.contains(&task.task_id.to_string()));
    assert!(details.contains("item 1"));
    assert!(details.contains("task-submit"));
    assert!(details.contains("closed pool"), "{details}");
    assert!(!details.contains("synthetic-import-input-secret"));
    assert!(!format!("{failure:?}").contains("closed pool"));
    assert_eq!(
        recorded(&log)
            .iter()
            .filter(|event| **event == "provider.prepare_import")
            .count(),
        1
    );
    let logs = logs.0.lock().unwrap();
    assert!(
        logs.iter()
            .any(|line| line.contains("account_import.item_failed")
                && line.contains(&task.task_id.to_string())
                && line.contains("item_index=1")
                && line.contains("task-submit"))
    );
    assert!(logs.iter().all(
        |line| !line.contains("closed pool") && !line.contains("synthetic-import-input-secret")
    ));
}

// 只捕获当前测试线程的结构化事件，避免设置全局订阅器影响并行测试
#[derive(Default)]
struct ImportLogs(std::sync::Mutex<Vec<String>>);

impl tracing::Subscriber for ImportLogs {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        let mut fields = ImportLogFields(String::new());
        event.record(&mut fields);
        self.0.lock().unwrap().push(fields.0);
    }
}

struct ImportLogFields(String);

impl tracing::field::Visit for ImportLogFields {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        use std::fmt::Write as _;
        write!(self.0, "{}={value:?} ", field.name()).unwrap();
    }
}

struct DelayedDiagnostics {
    recording: Arc<RecordingDiagnostics>,
    release: tokio::sync::Semaphore,
}

#[async_trait::async_trait]
impl gateway_core::diagnostics::OperationalDiagnostics for DelayedDiagnostics {
    async fn record_failure(
        &self,
        failure: gateway_core::diagnostics::OperationalFailure,
    ) -> Result<(), gateway_core::error::StoreError> {
        self.recording.record_failure(failure).await?;
        let _permit = self.release.acquire().await.unwrap();
        Err(gateway_core::error::StoreError::new(
            gateway_core::error::StoreErrorKind::Unavailable,
        ))
    }
}

#[tokio::test]
async fn blocked_or_failed_diagnostics_leave_terminal_results_queryable_without_replaying_import() {
    let log = events();
    let accounts = FakeAccountStore::new("openai", log.clone());
    accounts.fail_next_commit();
    let recording = Arc::new(RecordingDiagnostics::default());
    let diagnostics = Arc::new(DelayedDiagnostics {
        recording: recording.clone(),
        release: tokio::sync::Semaphore::new(0),
    });
    let mut bundle = AdminHarness::new()
        .accounts(accounts)
        .provider(FakeProviderAdmin::new("openai", log.clone()))
        .diagnostics(diagnostics.clone())
        .build_bundle()
        .await;
    let services = bundle.services();
    let task = services
        .import_tasks()
        .submit(command(Uuid::now_v7(), 1))
        .unwrap();
    let (cancel, worker) = start(&mut bundle);
    wait_diagnostics(&recording, 1).await;
    let result = services
        .import_tasks()
        .detail(&context("poll"), task.task_id)
        .unwrap();
    assert_eq!(result.summary.counts.unknown, 1);
    assert!(result.summary.finished_at.is_some());
    assert_eq!(services.import_tasks().list(&context("poll")).len(), 1);
    // 诊断仍在等待，另一个提交也必须可以取得注册表锁
    let pending = services
        .import_tasks()
        .submit(command(Uuid::now_v7(), 1))
        .unwrap();
    services
        .import_tasks()
        .stop(&context("stop"), pending.task_id)
        .unwrap();
    cancel.cancel();
    diagnostics.release.add_permits(1);
    worker.await.unwrap();
    assert_eq!(recording.0.lock().unwrap().len(), 1);
    assert_eq!(
        services
            .import_tasks()
            .detail(&context("poll"), task.task_id)
            .unwrap()
            .items[0]
            .status,
        result.items[0].status
    );
    assert_eq!(
        recorded(&log)
            .iter()
            .filter(|event| **event == "provider.prepare_import")
            .count(),
        1
    );
}

#[tokio::test]
async fn import_panic_is_recorded_as_interruption_without_capturing_the_payload() {
    let log = events();
    let provider = FakeProviderAdmin::new("openai", log.clone());
    provider.block_imports().close();
    let diagnostics = Arc::new(RecordingDiagnostics::default());
    let mut bundle = AdminHarness::new()
        .accounts(FakeAccountStore::new("openai", log))
        .provider(provider)
        .diagnostics(diagnostics.clone())
        .build_bundle()
        .await;
    let services = bundle.services();
    let task = services
        .import_tasks()
        .submit(command(Uuid::now_v7(), 1))
        .unwrap();
    let (cancel, worker) = start(&mut bundle);
    wait_diagnostics(&diagnostics, 1).await;
    cancel.cancel();
    worker.await.unwrap();
    let result = services
        .import_tasks()
        .detail(&context("poll"), task.task_id)
        .unwrap();
    assert_eq!(result.summary.counts.unknown, 1);
    assert_eq!(
        result.items[0].message.as_deref(),
        Some("执行中断，请先核对账号列表再决定是否重新导入")
    );
    let failures = diagnostics.0.lock().unwrap();
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0].kind, "internal");
    assert!(
        !failures[0]
            .details
            .as_ref()
            .unwrap()
            .as_str()
            .contains("import permit")
    );
}

async fn harness() -> (AdminBundle, Arc<FakeProviderAdmin>, EventLog) {
    let log = events();
    let provider = FakeProviderAdmin::new("openai", log.clone());
    let bundle = AdminHarness::new()
        .provider(provider.clone())
        .accounts(FakeAccountStore::new("openai", log.clone()))
        .build_bundle()
        .await;
    (bundle, provider, log)
}

fn start(bundle: &mut AdminBundle) -> (CancellationToken, tokio::task::JoinHandle<()>) {
    let registration = bundle
        .take_worker_contributions()
        .into_iter()
        .find_map(|contribution| match contribution {
            WorkerContribution::Registration(registration)
                if registration.id.kind() == WorkerKind::AccountImport =>
            {
                Some(registration)
            }
            _ => None,
        })
        .expect("import registration");
    let WorkerRunnable::Daemon { task, .. } = registration.runnable else {
        panic!("import daemon")
    };
    let cancellation = CancellationToken::new();
    let token = cancellation.clone();
    let handle = tokio::spawn(async move {
        task.run(token).await.expect("import daemon exits cleanly");
    });
    (cancellation, handle)
}

async fn wait_started(log: &EventLog, count: usize) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while recorded(log)
            .iter()
            .filter(|event| **event == "provider.prepare_import")
            .count()
            < count
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("imports started");
}

async fn wait_finished(services: &AdminServices, id: Uuid) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while services
            .import_tasks()
            .detail(&context("poll"), id)
            .unwrap()
            .summary
            .finished_at
            .is_none()
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("imports finished");
}

#[tokio::test]
async fn retry_should_return_the_same_task_but_reject_changed_content() {
    let (bundle, _, _) = harness().await;
    let services = bundle.services();
    let id = Uuid::now_v7();
    let first = services.import_tasks().submit(command(id, 2)).unwrap();
    let retry = services.import_tasks().submit(command(id, 2)).unwrap();
    assert_eq!(first.task_id, retry.task_id);
    assert_eq!(
        services
            .import_tasks()
            .list(&context("reopened-page"))
            .len(),
        1
    );
    let mut changed = command(id, 2);
    changed.fingerprint = [2; 32];
    assert_eq!(
        services.import_tasks().submit(changed).unwrap_err().kind(),
        AdminErrorKind::Conflict
    );
}

#[tokio::test]
async fn task_reads_and_stop_should_be_isolated_by_owner() {
    let (bundle, _, _) = harness().await;
    let services = bundle.services();
    let task = services
        .import_tasks()
        .submit(command(Uuid::now_v7(), 1))
        .unwrap();
    let other = MutationContext {
        actor: MutationActor::AdminSession {
            admin_user_id: "another-admin".to_owned(),
        },
        request_id: "other-session".to_owned(),
    };
    assert!(services.import_tasks().list(&other).is_empty());
    assert_eq!(
        services
            .import_tasks()
            .detail(&other, task.task_id)
            .unwrap_err()
            .kind(),
        AdminErrorKind::NotFound
    );
    assert_eq!(
        services
            .import_tasks()
            .stop(&other, task.task_id)
            .unwrap_err()
            .kind(),
        AdminErrorKind::NotFound
    );
}

#[tokio::test]
async fn stopping_should_skip_pending_items_and_allow_three_in_flight_to_commit() {
    let (mut bundle, provider, log) = harness().await;
    let gate = provider.block_imports();
    let services = bundle.services();
    let task = services
        .import_tasks()
        .submit(command(Uuid::now_v7(), 8))
        .unwrap();
    let (cancel, worker) = start(&mut bundle);
    wait_started(&log, 3).await;
    let stopped = services
        .import_tasks()
        .stop(&context("stop"), task.task_id)
        .unwrap();
    assert_eq!(stopped.summary.counts.running, 3);
    assert_eq!(stopped.summary.counts.skipped, 5);
    gate.add_permits(3);
    wait_finished(&services, task.task_id).await;
    let finished = services
        .import_tasks()
        .detail(&context("new-page"), task.task_id)
        .unwrap();
    assert_eq!(finished.summary.counts.imported_accounts, 3);
    assert_eq!(
        recorded(&log)
            .iter()
            .filter(|event| **event == "provider.prepare_import")
            .count(),
        3
    );
    cancel.cancel();
    worker.await.unwrap();
}

#[tokio::test]
async fn concurrency_limit_should_be_shared_across_tasks() {
    let (mut bundle, provider, log) = harness().await;
    let gate = provider.block_imports();
    let services = bundle.services();
    let first = services
        .import_tasks()
        .submit(command(Uuid::now_v7(), 6))
        .unwrap();
    let second = services
        .import_tasks()
        .submit(command(Uuid::now_v7(), 6))
        .unwrap();
    let (cancel, worker) = start(&mut bundle);
    wait_started(&log, 3).await;
    let summaries = services.import_tasks().list(&context("poll"));
    assert_eq!(
        summaries
            .iter()
            .map(|task| task.counts.running)
            .sum::<usize>(),
        3
    );
    assert!(summaries.iter().all(|task| task.counts.running > 0));
    gate.add_permits(12);
    wait_finished(&services, first.task_id).await;
    wait_finished(&services, second.task_id).await;
    cancel.cancel();
    worker.await.unwrap();
}

#[tokio::test]
async fn failed_and_unknown_items_should_not_abort_the_batch_or_retry_credentials() {
    for (kind, failed, unknown) in [
        (ProviderAdminErrorKind::Invalid, 1, 0),
        (ProviderAdminErrorKind::Ambiguous, 0, 1),
    ] {
        let (mut bundle, provider, log) = harness().await;
        provider.fail_next(kind);
        let services = bundle.services();
        let task = services
            .import_tasks()
            .submit(command(Uuid::now_v7(), 4))
            .unwrap();
        let (cancel, worker) = start(&mut bundle);
        wait_finished(&services, task.task_id).await;
        let result = services
            .import_tasks()
            .detail(&context("poll"), task.task_id)
            .unwrap();
        assert_eq!(
            (
                result.summary.counts.succeeded,
                result.summary.counts.failed,
                result.summary.counts.unknown
            ),
            (3, failed, unknown)
        );
        assert_eq!(
            recorded(&log)
                .iter()
                .filter(|event| **event == "provider.prepare_import")
                .count(),
            4
        );
        cancel.cancel();
        worker.await.unwrap();
    }
}

#[tokio::test]
async fn shutdown_should_drain_running_items_and_reject_new_work() {
    let (mut bundle, provider, log) = harness().await;
    let gate = provider.block_imports();
    let services = bundle.services();
    let task = services
        .import_tasks()
        .submit(command(Uuid::now_v7(), 7))
        .unwrap();
    let (cancel, worker) = start(&mut bundle);
    wait_started(&log, 3).await;
    cancel.cancel();
    tokio::task::yield_now().await;
    assert_eq!(
        services
            .import_tasks()
            .submit(command(Uuid::now_v7(), 1))
            .unwrap_err()
            .kind(),
        AdminErrorKind::Unavailable
    );
    assert!(!worker.is_finished());
    gate.add_permits(3);
    worker.await.unwrap();
    let result = services
        .import_tasks()
        .detail(&context("poll"), task.task_id)
        .unwrap();
    assert_eq!(
        (
            result.summary.counts.succeeded,
            result.summary.counts.skipped
        ),
        (3, 4)
    );
}

#[tokio::test(start_paused = true)]
async fn completed_records_should_expire_and_new_process_should_start_empty() {
    let (bundle, _, _) = harness().await;
    let services = bundle.services();
    let task = services
        .import_tasks()
        .submit(command(Uuid::now_v7(), 2))
        .unwrap();
    services
        .import_tasks()
        .stop(&context("stop"), task.task_id)
        .unwrap();
    tokio::time::advance(Duration::from_secs(3601)).await;
    assert!(services.import_tasks().list(&context("poll")).is_empty());
    assert_eq!(
        services
            .import_tasks()
            .detail(&context("poll"), task.task_id)
            .unwrap_err()
            .kind(),
        AdminErrorKind::NotFound
    );
    let (fresh, _, _) = harness().await;
    assert!(
        fresh
            .services()
            .import_tasks()
            .list(&context("poll"))
            .is_empty()
    );
}

#[tokio::test]
async fn queue_should_reject_excess_work_before_starting_any_imports() {
    let (bundle, _, log) = harness().await;
    let services = bundle.services();
    assert_eq!(
        services
            .import_tasks()
            .submit(command(Uuid::now_v7(), 201))
            .unwrap_err()
            .kind(),
        AdminErrorKind::Invalid
    );
    for _ in 0..8 {
        services
            .import_tasks()
            .submit(command(Uuid::now_v7(), 1))
            .unwrap();
    }
    assert_eq!(
        services
            .import_tasks()
            .submit(command(Uuid::now_v7(), 1))
            .unwrap_err()
            .kind(),
        AdminErrorKind::RateLimited
    );
    assert!(recorded(&log).is_empty());
}
