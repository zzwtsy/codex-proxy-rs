use chrono::{Duration, Utc};
use gateway_admin::{
    model::backup::{
        BackupRecordListQuery, BackupRecordSeed, BackupStatus, BackupStatusTransition,
        BackupTriggerKind, UpdateBackupScheduleCommand, UpdateBackupStorageCommand,
    },
    model::{MutationActor, MutationContext, PageSize},
    ports::backup::{BackupRepository, StatusTransitionUpdate},
    ports::store::AdminStoreErrorKind,
};
use gateway_store::{SqliteStoreConfig, sqlite, sqlite::SqliteBackupRepository};
use secrecy::SecretString;

#[tokio::test]
async fn sqlite_backup_repository_matches_status_and_revision_contracts() {
    let root = tempfile::tempdir().expect("SQLite data directory");
    let pool = sqlite::connect_and_migrate(
        &root.path().join("backup.sqlite3"),
        &SqliteStoreConfig::default(),
    )
    .await
    .expect("create SQLite database");
    let repository = SqliteBackupRepository::new(pool.clone());
    let context = MutationContext {
        actor: MutationActor::System,
        request_id: "sqlite-backup-test".to_owned(),
    };

    let initial = repository
        .load_settings()
        .await
        .expect("load default settings");
    assert_eq!(initial.storage_revision, 1);
    let (configured, revision) = repository
        .update_storage_settings(storage("https://s3.example.com"), &context)
        .await
        .expect("configure backup storage");
    assert_eq!(configured.storage_revision, 2);
    assert_eq!(revision.get(), 2);
    assert!(
        !repository
            .record_verification(configured.storage_revision - 1, Utc::now())
            .await
            .expect("reject stale verification")
    );
    assert!(
        repository
            .record_verification(configured.storage_revision, Utc::now())
            .await
            .expect("record current verification")
    );

    let run_at = Utc::now() + Duration::hours(1);
    let scheduled = repository
        .update_schedule_settings(
            UpdateBackupScheduleCommand {
                schedule_enabled: true,
                cron_expression: "0 2 * * *".to_owned(),
                retention_days: 7,
                retention_count: 5,
            },
            Some(run_at),
            &context,
            Default::default(),
        )
        .await
        .expect("enable backup schedule");
    let seed = scheduled_seed(run_at);
    assert!(
        repository
            .insert_scheduled_record(
                seed.clone(),
                Some(run_at + Duration::hours(1)),
                "0 2 * * *",
                scheduled
                    .schedule_timezone
                    .as_deref()
                    .expect("schedule timezone"),
                Some(run_at),
            )
            .await
            .expect("insert scheduled backup")
    );
    assert!(
        !repository
            .insert_scheduled_record(
                scheduled_seed(run_at),
                Some(run_at + Duration::hours(2)),
                "0 2 * * *",
                scheduled
                    .schedule_timezone
                    .as_deref()
                    .expect("schedule timezone"),
                Some(run_at + Duration::hours(1)),
            )
            .await
            .expect("advance cursor while skipping duplicate scheduled task")
    );

    let claimed = repository
        .claim_next_queued(Utc::now())
        .await
        .expect("claim next queued backup")
        .expect("scheduled backup is queued");
    assert_eq!(claimed.status, BackupStatus::Dumping);
    assert_eq!(claimed.attempt_count, 1);
    assert_eq!(
        repository
            .list_intermediate_records()
            .await
            .expect("list intermediate records")
            .len(),
        1
    );

    let uploading = repository
        .transition_status(
            &claimed.id,
            transition(BackupStatus::Dumping, BackupStatus::Uploading),
            StatusTransitionUpdate {
                size_bytes: Some(42),
                sha256: Some("a".repeat(64)),
                ..StatusTransitionUpdate::default()
            },
            Utc::now(),
        )
        .await
        .expect("transition backup to uploading")
        .expect("record transitioned to uploading");
    assert_eq!(uploading.size_bytes, Some(42));
    assert!(
        repository
            .transition_status(
                &claimed.id,
                transition(BackupStatus::Dumping, BackupStatus::Uploading),
                StatusTransitionUpdate::default(),
                Utc::now(),
            )
            .await
            .expect("reject stale state transition")
            .is_none()
    );
    let completed_at = Utc::now();
    let completed = repository
        .transition_status(
            &claimed.id,
            transition(BackupStatus::Uploading, BackupStatus::Completed),
            StatusTransitionUpdate {
                completed_at: Some(completed_at),
                ..StatusTransitionUpdate::default()
            },
            completed_at,
        )
        .await
        .expect("transition backup to completed")
        .expect("completed backup record");
    assert_eq!(
        completed.completed_at.map(|at| at.timestamp_micros()),
        Some(completed_at.timestamp_micros())
    );
    assert_eq!(
        repository
            .list_scheduled_completed_desc(10)
            .await
            .expect("list completed scheduled backups")
            .len(),
        1
    );
    let page = repository
        .list_backup_records(BackupRecordListQuery {
            page: 1,
            page_size: PageSize::new(10).expect("valid page size"),
            status: Some(BackupStatus::Completed),
            trigger: Some(BackupTriggerKind::Scheduled),
        })
        .await
        .expect("page completed backups");
    assert_eq!(page.total, 1);
    assert_eq!(page.items, vec![completed]);

    let deleting = repository
        .transition_to_deleting(&claimed.id, Utc::now())
        .await
        .expect("begin remote deletion")
        .expect("deleting backup record");
    assert_eq!(deleting.status, BackupStatus::Deleting);
    assert_eq!(
        repository
            .list_pending_deletions(10)
            .await
            .expect("list pending deletions")
            .len(),
        1
    );
    repository
        .delete_record(&claimed.id)
        .await
        .expect("delete backup record after object cleanup");
    assert!(
        repository
            .load_backup_record(&claimed.id)
            .await
            .expect("load deleted backup")
            .is_none()
    );

    let audit_count: i64 = sqlx::query_scalar("select count(*) from admin_audit_events")
        .fetch_one(&pool)
        .await
        .expect("count backup audit events");
    assert_eq!(audit_count, 2);
    pool.close().await;
}

#[tokio::test]
async fn sqlite_backup_repository_rejects_active_task_conflicts() {
    let root = tempfile::tempdir().expect("SQLite data directory");
    let pool = sqlite::connect_and_migrate(
        &root.path().join("backup-conflict.sqlite3"),
        &SqliteStoreConfig::default(),
    )
    .await
    .expect("create SQLite database");
    let repository = SqliteBackupRepository::new(pool.clone());
    repository
        .insert_backup_record(manual_seed("1"))
        .await
        .expect("insert first manual backup");
    let error = repository
        .insert_backup_record(manual_seed("2"))
        .await
        .expect_err("a second active backup is rejected");
    assert_eq!(error.kind(), AdminStoreErrorKind::Conflict);
    pool.close().await;
}

fn storage(endpoint: &str) -> UpdateBackupStorageCommand {
    UpdateBackupStorageCommand {
        endpoint: endpoint.to_owned(),
        region: "auto".to_owned(),
        bucket: "backup-bucket".to_owned(),
        access_key_id: "access-key".to_owned(),
        secret_access_key: Some(SecretString::from("secret-value")),
        prefix: "codex".to_owned(),
        force_path_style: false,
    }
}

fn manual_seed(suffix: &str) -> BackupRecordSeed {
    let id = format!("backup_{}", suffix.repeat(32 / suffix.len()));
    BackupRecordSeed {
        id: id.clone(),
        trigger_kind: BackupTriggerKind::Manual,
        scheduled_at: None,
        object_key: format!("codex/manual/{id}.sqlite3"),
        expires_at: None,
    }
}

fn scheduled_seed(at: chrono::DateTime<Utc>) -> BackupRecordSeed {
    let id = format!("backup_{}", "c".repeat(32));
    BackupRecordSeed {
        id,
        trigger_kind: BackupTriggerKind::Scheduled,
        scheduled_at: Some(at),
        object_key: format!("codex/scheduled/{}.sqlite3", at.timestamp()),
        expires_at: None,
    }
}

fn transition(from: BackupStatus, to: BackupStatus) -> BackupStatusTransition {
    BackupStatusTransition::try_new(from, to).expect("legal backup status transition")
}
