//! SQLite `backup_settings` 与 `backup_records` 仓储。
//!
//! 写操作先取得短事务写锁，再执行条件迁移，保持任务领取、配置修订和审计原子性。

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use gateway_admin::{
    model::{
        MutationContext, Revision as AdminRevision,
        audit::MutationAuditOperation,
        backup::{
            BackupRecord, BackupRecordListQuery, BackupRecordPage, BackupRecordSeed,
            BackupSettings, BackupStatus, BackupStatusTransition, BackupTriggerKind,
            UpdateBackupScheduleCommand, UpdateBackupStorageCommand,
        },
    },
    ports::{
        backup::{BackupRepository, StatusTransitionUpdate},
        store::{AdminStoreError, AdminStoreErrorKind, AdminStoreResult},
    },
};
use secrecy::{ExposeSecret as _, SecretString};
use sqlx::{Row, Sqlite, SqlitePool, Transaction};

use crate::{Revision, StoreError, mutation_audit};

use super::{acquire_write_lock, value::datetime_from_micros};

const RECORD_COLUMNS: &str =
    "id, trigger_kind, status, scheduled_at_us, object_key, size_bytes, sha256,
    attempt_count, error_code, error_message, started_at_us, completed_at_us, expires_at_us,
    created_at_us, updated_at_us";
const SETTINGS_COLUMNS: &str = "storage_revision, endpoint, region, bucket, access_key_id,
    secret_access_key, prefix, force_path_style, schedule_enabled, cron_expression,
    schedule_timezone, retention_days, retention_count, next_run_at_us, last_verified_at_us,
    updated_at_us";

#[derive(Clone)]
pub struct SqliteBackupRepository {
    pool: SqlitePool,
}

impl SqliteBackupRepository {
    #[must_use]
    pub const fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl BackupRepository for SqliteBackupRepository {
    async fn load_settings(&self) -> AdminStoreResult<BackupSettings> {
        load_settings(&self.pool).await
    }

    async fn update_storage_settings(
        &self,
        command: UpdateBackupStorageCommand,
        context: &MutationContext,
    ) -> AdminStoreResult<(BackupSettings, AdminRevision)> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|_| unavailable("begin backup storage update"))?;
        acquire_write_lock(&mut transaction)
            .await
            .map_err(map_store_error)?;
        let result = async {
            let current = load_settings_in_transaction(&mut transaction).await?;
            let mut changed = storage_changed_fields(&current, &command);
            if changed.is_empty() {
                let revision = load_config_revision(&mut transaction).await?;
                return Ok((current, admin_revision(revision)?));
            }
            ensure_storage_identity_stable(&current, &command, &mut transaction).await?;
            let now = Utc::now().timestamp_micros();
            sqlx::query(
                "update backup_settings set endpoint = ?1, region = ?2, bucket = ?3,
                    access_key_id = ?4, secret_access_key = coalesce(?5, secret_access_key),
                    prefix = ?6, force_path_style = ?7, storage_revision = storage_revision + 1,
                    last_verified_at_us = null, schedule_enabled = 0, next_run_at_us = null,
                    updated_at_us = max(updated_at_us, ?8) where id = 1",
            )
            .bind(&command.endpoint)
            .bind(&command.region)
            .bind(&command.bucket)
            .bind(&command.access_key_id)
            .bind(
                command
                    .secret_access_key
                    .as_ref()
                    .map(|secret| secret.expose_secret()),
            )
            .bind(&command.prefix)
            .bind(i64::from(command.force_path_style))
            .bind(now)
            .execute(&mut *transaction)
            .await
            .map_err(|_| unavailable("update SQLite backup storage"))?;

            let revision = super::bump_config_revision(&mut transaction, now)
                .await
                .map_err(map_store_error)?;
            let revision = admin_revision(revision)?;
            if current.last_verified_at.is_some() {
                changed.push("last_verified_at".to_owned());
            }
            if current.schedule_enabled {
                changed.push("schedule_enabled".to_owned());
                changed.push("next_run_at".to_owned());
            }
            let mut audit = mutation_audit(
                context,
                MutationAuditOperation::BackupStorageUpdate,
                "1",
                changed,
            );
            audit.config_revision =
                Some(i64::try_from(revision.get()).map_err(|_| invalid("config revision"))?);
            crate::sqlite::append_admin_audit_event_in_transaction(&mut transaction, audit)
                .await
                .map_err(map_store_error)?;
            let settings = load_settings_in_transaction(&mut transaction).await?;
            Ok((settings, revision))
        }
        .await;
        match result {
            Ok(value) => {
                transaction
                    .commit()
                    .await
                    .map_err(|_| unavailable("commit SQLite backup storage update"))?;
                Ok(value)
            }
            Err(error) => {
                transaction
                    .rollback()
                    .await
                    .map_err(|_| unavailable("rollback SQLite backup storage update"))?;
                Err(error)
            }
        }
    }

    async fn update_schedule_settings(
        &self,
        command: UpdateBackupScheduleCommand,
        next_run_at: Option<DateTime<Utc>>,
        context: &MutationContext,
        timezone: gateway_core::time::DeploymentTimeZone,
    ) -> AdminStoreResult<BackupSettings> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|_| unavailable("begin backup schedule update"))?;
        acquire_write_lock(&mut transaction)
            .await
            .map_err(map_store_error)?;
        let result = async {
            sqlx::query(
                "update backup_settings set schedule_enabled = ?1, cron_expression = ?2,
                    schedule_timezone = ?3, retention_days = ?4, retention_count = ?5,
                    next_run_at_us = ?6, updated_at_us = max(updated_at_us, ?7) where id = 1",
            )
            .bind(i64::from(command.schedule_enabled))
            .bind(&command.cron_expression)
            .bind(timezone.name())
            .bind(i64::from(command.retention_days))
            .bind(i64::from(command.retention_count))
            .bind(next_run_at.map(|at| at.timestamp_micros()))
            .bind(Utc::now().timestamp_micros())
            .execute(&mut *transaction)
            .await
            .map_err(|_| unavailable("update SQLite backup schedule"))?;
            let audit = mutation_audit(
                context,
                MutationAuditOperation::BackupScheduleUpdate,
                "1",
                vec![
                    "schedule_enabled".to_owned(),
                    "cron_expression".to_owned(),
                    "schedule_timezone".to_owned(),
                    "retention_days".to_owned(),
                    "retention_count".to_owned(),
                ],
            );
            crate::sqlite::append_admin_audit_event_in_transaction(&mut transaction, audit)
                .await
                .map_err(map_store_error)?;
            load_settings_in_transaction(&mut transaction).await
        }
        .await;
        match result {
            Ok(settings) => {
                transaction
                    .commit()
                    .await
                    .map_err(|_| unavailable("commit SQLite backup schedule update"))?;
                Ok(settings)
            }
            Err(error) => {
                transaction
                    .rollback()
                    .await
                    .map_err(|_| unavailable("rollback SQLite backup schedule update"))?;
                Err(error)
            }
        }
    }

    async fn record_verification(
        &self,
        storage_revision: u64,
        at: DateTime<Utc>,
    ) -> AdminStoreResult<bool> {
        let revision = i64::try_from(storage_revision).map_err(|_| invalid("storage revision"))?;
        let result = sqlx::query(
            "update backup_settings set last_verified_at_us = ?2,
                updated_at_us = max(updated_at_us, ?2)
             where id = 1 and storage_revision = ?1",
        )
        .bind(revision)
        .bind(at.timestamp_micros())
        .execute(&self.pool)
        .await
        .map_err(|_| unavailable("record SQLite backup verification"))?;
        Ok(result.rows_affected() == 1)
    }

    async fn insert_backup_record(&self, seed: BackupRecordSeed) -> AdminStoreResult<BackupRecord> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|_| unavailable("begin backup record insert"))?;
        acquire_write_lock(&mut transaction)
            .await
            .map_err(map_store_error)?;
        let result = async {
            if has_active_record(&mut transaction).await? {
                return Err(conflict("an active backup task already exists"));
            }
            if let Some(scheduled_at) = seed.scheduled_at
                && has_scheduled_record(&mut transaction, scheduled_at).await?
            {
                return Err(conflict("a scheduled backup already exists at this time"));
            }
            insert_queued(&mut transaction, &seed).await
        }
        .await;
        match result {
            Ok(record) => {
                transaction
                    .commit()
                    .await
                    .map_err(|_| unavailable("commit backup record insert"))?;
                Ok(record)
            }
            Err(error) => {
                transaction
                    .rollback()
                    .await
                    .map_err(|_| unavailable("rollback backup record insert"))?;
                Err(error)
            }
        }
    }

    async fn insert_scheduled_record(
        &self,
        seed: BackupRecordSeed,
        next_run_at: Option<DateTime<Utc>>,
        expected_cron: &str,
        expected_timezone: &str,
        expected_next_run_at: Option<DateTime<Utc>>,
    ) -> AdminStoreResult<bool> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|_| unavailable("begin scheduled backup insert"))?;
        acquire_write_lock(&mut transaction)
            .await
            .map_err(map_store_error)?;
        let changed = sqlx::query(
            "update backup_settings set next_run_at_us = ?1
             where id = 1 and schedule_enabled = 1 and cron_expression = ?2
               and schedule_timezone = ?3 and next_run_at_us is ?4",
        )
        .bind(next_run_at.map(|at| at.timestamp_micros()))
        .bind(expected_cron)
        .bind(expected_timezone)
        .bind(expected_next_run_at.map(|at| at.timestamp_micros()))
        .execute(&mut *transaction)
        .await
        .map_err(|_| unavailable("guard SQLite scheduled backup insert"))?
        .rows_affected()
            == 1;
        if !changed {
            transaction
                .rollback()
                .await
                .map_err(|_| unavailable("rollback stale backup schedule"))?;
            return Ok(false);
        }
        if has_active_record(&mut transaction).await? {
            transaction
                .commit()
                .await
                .map_err(|_| unavailable("commit skipped scheduled backup"))?;
            return Ok(false);
        }
        if let Some(scheduled_at) = seed.scheduled_at
            && has_scheduled_record(&mut transaction, scheduled_at).await?
        {
            transaction
                .commit()
                .await
                .map_err(|_| unavailable("commit duplicate scheduled backup cursor"))?;
            return Ok(false);
        }
        insert_queued(&mut transaction, &seed).await?;
        transaction
            .commit()
            .await
            .map_err(|_| unavailable("commit scheduled backup insert"))?;
        Ok(true)
    }

    async fn list_backup_records(
        &self,
        query: BackupRecordListQuery,
    ) -> AdminStoreResult<BackupRecordPage> {
        let limit = u32::from(query.page_size.get()).min(200);
        let offset = u64::from(query.page.saturating_sub(1)) * u64::from(limit);
        let status = query.status.map(BackupStatus::as_str);
        let trigger = query.trigger.map(BackupTriggerKind::as_str);
        let total: i64 = sqlx::query_scalar(
            "select count(*) from backup_records
             where (?1 is null or status = ?1) and (?2 is null or trigger_kind = ?2)",
        )
        .bind(status)
        .bind(trigger)
        .fetch_one(&self.pool)
        .await
        .map_err(|_| unavailable("count SQLite backup records"))?;
        let offset = i64::try_from(offset).map_err(|_| invalid("page offset"))?;
        let sql = format!(
            "select {RECORD_COLUMNS} from backup_records
             where (?1 is null or status = ?1) and (?2 is null or trigger_kind = ?2)
             order by created_at_us desc, id desc limit ?3 offset ?4"
        );
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(status)
            .bind(trigger)
            .bind(i64::from(limit))
            .bind(offset)
            .fetch_all(&self.pool)
            .await
            .map_err(|_| unavailable("list SQLite backup records"))?;
        Ok(BackupRecordPage {
            items: rows
                .iter()
                .map(record_from_row)
                .collect::<AdminStoreResult<Vec<_>>>()?,
            total: u64::try_from(total).map_err(|_| invalid("record count"))?,
            page: query.page,
            page_size: query.page_size,
        })
    }

    async fn load_backup_record(&self, id: &str) -> AdminStoreResult<Option<BackupRecord>> {
        let sql = format!("select {RECORD_COLUMNS} from backup_records where id = ?1");
        sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|_| unavailable("load SQLite backup record"))?
            .as_ref()
            .map(record_from_row)
            .transpose()
    }

    async fn list_intermediate_records(&self) -> AdminStoreResult<Vec<BackupRecord>> {
        list_records(
            &self.pool,
            "where status in ('dumping', 'uploading') order by created_at_us, id",
        )
        .await
    }

    async fn list_pending_deletions(&self, limit: u32) -> AdminStoreResult<Vec<BackupRecord>> {
        let limit = i64::from(limit.clamp(1, 100));
        list_records_with_limit(
            &self.pool,
            "where status = 'deleting' order by created_at_us, id",
            limit,
        )
        .await
    }

    async fn list_expired_records(&self, limit: u32) -> AdminStoreResult<Vec<BackupRecord>> {
        let limit = i64::from(limit.clamp(1, 1000));
        let now = Utc::now().timestamp_micros();
        let sql = format!(
            "select {RECORD_COLUMNS} from backup_records
             where expires_at_us is not null and expires_at_us <= ?1 and status in ('completed', 'failed')
             order by created_at_us, id limit ?2"
        );
        fetch_records(&self.pool, &sql, Some(now), limit).await
    }

    async fn claim_next_queued(
        &self,
        now: DateTime<Utc>,
    ) -> AdminStoreResult<Option<BackupRecord>> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|_| unavailable("begin backup claim"))?;
        acquire_write_lock(&mut transaction)
            .await
            .map_err(map_store_error)?;
        let id = sqlx::query_scalar::<_, String>(
            "select id from backup_records where status = 'queued' order by created_at_us, id limit 1",
        )
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| unavailable("select queued SQLite backup"))?;
        let record = if let Some(id) = id {
            sqlx::query(
                "update backup_records set status = 'dumping', started_at_us = ?1,
                            completed_at_us = null, attempt_count = attempt_count + 1,
                            updated_at_us = max(updated_at_us, ?1)
                         where id = ?2 and status = 'queued'",
            )
            .bind(now.timestamp_micros())
            .bind(&id)
            .execute(&mut *transaction)
            .await
            .map_err(|_| unavailable("claim queued SQLite backup"))?;
            let sql = format!("select {RECORD_COLUMNS} from backup_records where id = ?1");
            sqlx::query(sqlx::AssertSqlSafe(sql))
                .bind(id)
                .fetch_optional(&mut *transaction)
                .await
                .map_err(|_| unavailable("load claimed SQLite backup"))?
                .as_ref()
                .map(record_from_row)
                .transpose()?
        } else {
            None
        };
        transaction
            .commit()
            .await
            .map_err(|_| unavailable("commit SQLite backup claim"))?;
        Ok(record)
    }

    async fn transition_status(
        &self,
        id: &str,
        transition: BackupStatusTransition,
        update: StatusTransitionUpdate,
        now: DateTime<Utc>,
    ) -> AdminStoreResult<Option<BackupRecord>> {
        let size = update
            .size_bytes
            .map(i64::try_from)
            .transpose()
            .map_err(|_| invalid("size bytes"))?;
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|_| unavailable("begin backup transition"))?;
        acquire_write_lock(&mut transaction)
            .await
            .map_err(map_store_error)?;
        let changed = sqlx::query(
            "update backup_records set status = ?1, size_bytes = coalesce(?2, size_bytes),
                sha256 = coalesce(?3, sha256), error_code = coalesce(?4, error_code),
                error_message = coalesce(?5, error_message),
                completed_at_us = coalesce(?6,
                  case when ?1 in ('completed', 'failed') then ?7 else completed_at_us end),
                updated_at_us = max(updated_at_us, ?7)
             where id = ?8 and status = ?9",
        )
        .bind(transition.to().as_str())
        .bind(size)
        .bind(update.sha256.as_deref())
        .bind(update.error_code.as_deref())
        .bind(update.error_message.as_deref())
        .bind(update.completed_at.map(|at| at.timestamp_micros()))
        .bind(now.timestamp_micros())
        .bind(id)
        .bind(transition.from().as_str())
        .execute(&mut *transaction)
        .await
        .map_err(|_| unavailable("transition SQLite backup record"))?;
        if changed.rows_affected() == 0 {
            transaction
                .commit()
                .await
                .map_err(|_| unavailable("commit unmatched SQLite backup transition"))?;
            return Ok(None);
        }
        let sql =
            format!("select {RECORD_COLUMNS} from backup_records where id = ?1 and status = ?2");
        let row = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(id)
            .bind(transition.to().as_str())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|_| unavailable("load transitioned SQLite backup"))?;
        transaction
            .commit()
            .await
            .map_err(|_| unavailable("commit SQLite backup transition"))?;
        row.as_ref().map(record_from_row).transpose()
    }

    async fn transition_to_deleting(
        &self,
        id: &str,
        now: DateTime<Utc>,
    ) -> AdminStoreResult<Option<BackupRecord>> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|_| unavailable("begin backup deletion transition"))?;
        acquire_write_lock(&mut transaction)
            .await
            .map_err(map_store_error)?;
        let changed = sqlx::query(
            "update backup_records set status = 'deleting', updated_at_us = max(updated_at_us, ?1)
             where id = ?2 and status in ('completed', 'failed')",
        )
        .bind(now.timestamp_micros())
        .bind(id)
        .execute(&mut *transaction)
        .await
        .map_err(|_| unavailable("transition SQLite backup to deleting"))?;
        if changed.rows_affected() == 0 {
            transaction
                .commit()
                .await
                .map_err(|_| unavailable("commit unmatched SQLite backup deletion transition"))?;
            return Ok(None);
        }
        let sql = format!(
            "select {RECORD_COLUMNS} from backup_records where id = ?1 and status = 'deleting'"
        );
        let row = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(id)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|_| unavailable("load SQLite backup deletion transition"))?;
        transaction
            .commit()
            .await
            .map_err(|_| unavailable("commit SQLite backup deletion transition"))?;
        row.as_ref().map(record_from_row).transpose()
    }

    async fn delete_record(&self, id: &str) -> AdminStoreResult<()> {
        sqlx::query("delete from backup_records where id = ?1")
            .bind(id)
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(|_| unavailable("delete SQLite backup record"))
    }

    async fn advance_schedule_cursor(
        &self,
        next_run_at: DateTime<Utc>,
        expected_cron: &str,
        expected_timezone: Option<&str>,
        expected_next_run_at: Option<DateTime<Utc>>,
        timezone: &str,
    ) -> AdminStoreResult<bool> {
        let result = sqlx::query(
            "update backup_settings set next_run_at_us = ?1, schedule_timezone = ?5
             where id = 1 and schedule_enabled = 1 and cron_expression = ?2
               and schedule_timezone is ?3 and next_run_at_us is ?4",
        )
        .bind(next_run_at.timestamp_micros())
        .bind(expected_cron)
        .bind(expected_timezone)
        .bind(expected_next_run_at.map(|at| at.timestamp_micros()))
        .bind(timezone)
        .execute(&self.pool)
        .await
        .map_err(|_| unavailable("advance SQLite backup schedule cursor"))?;
        Ok(result.rows_affected() == 1)
    }

    async fn list_scheduled_completed_desc(
        &self,
        limit: u32,
    ) -> AdminStoreResult<Vec<BackupRecord>> {
        let limit = i64::from(limit.clamp(1, 10000));
        list_records_with_limit(
            &self.pool,
            "where trigger_kind = 'scheduled' and status = 'completed' order by completed_at_us desc, id desc",
            limit,
        )
        .await
    }
}

async fn load_settings(pool: &SqlitePool) -> AdminStoreResult<BackupSettings> {
    let sql = format!("select {SETTINGS_COLUMNS} from backup_settings where id = 1");
    sqlx::query(sqlx::AssertSqlSafe(sql))
        .fetch_optional(pool)
        .await
        .map_err(|_| unavailable("load SQLite backup settings"))?
        .as_ref()
        .map(settings_from_row)
        .transpose()?
        .ok_or_else(|| not_found("backup settings"))
}

async fn load_settings_in_transaction(
    transaction: &mut Transaction<'_, Sqlite>,
) -> AdminStoreResult<BackupSettings> {
    let sql = format!("select {SETTINGS_COLUMNS} from backup_settings where id = 1");
    sqlx::query(sqlx::AssertSqlSafe(sql))
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|_| unavailable("load SQLite backup settings in transaction"))?
        .as_ref()
        .map(settings_from_row)
        .transpose()?
        .ok_or_else(|| not_found("backup settings"))
}

async fn load_config_revision(
    transaction: &mut Transaction<'_, Sqlite>,
) -> AdminStoreResult<Revision> {
    let value =
        sqlx::query_scalar::<_, i64>("select config_revision from runtime_settings where id = 1")
            .fetch_optional(&mut **transaction)
            .await
            .map_err(|_| unavailable("load SQLite config revision"))?
            .ok_or_else(|| not_found("runtime settings"))?;
    Revision::new(u64::try_from(value).map_err(|_| invalid("config revision"))?)
        .map_err(|_| invalid("config revision"))
}

async fn insert_queued(
    transaction: &mut Transaction<'_, Sqlite>,
    seed: &BackupRecordSeed,
) -> AdminStoreResult<BackupRecord> {
    let now = Utc::now().timestamp_micros();
    sqlx::query(
        "insert into backup_records (id, trigger_kind, status, scheduled_at_us, object_key,
             expires_at_us, created_at_us, updated_at_us)
         values (?1, ?2, 'queued', ?3, ?4, ?5, ?6, ?6)",
    )
    .bind(&seed.id)
    .bind(seed.trigger_kind.as_str())
    .bind(seed.scheduled_at.map(|at| at.timestamp_micros()))
    .bind(&seed.object_key)
    .bind(seed.expires_at.map(|at| at.timestamp_micros()))
    .bind(now)
    .execute(&mut **transaction)
    .await
    .map_err(|error| match error {
        sqlx::Error::Database(database_error) if database_error.is_unique_violation() => {
            conflict("backup record already exists")
        }
        _ => unavailable("insert SQLite backup record"),
    })?;
    let sql = format!("select {RECORD_COLUMNS} from backup_records where id = ?1");
    let row = sqlx::query(sqlx::AssertSqlSafe(sql))
        .bind(&seed.id)
        .fetch_one(&mut **transaction)
        .await
        .map_err(|_| unavailable("load inserted SQLite backup record"))?;
    record_from_row(&row)
}

async fn has_active_record(transaction: &mut Transaction<'_, Sqlite>) -> AdminStoreResult<bool> {
    sqlx::query_scalar::<_, i64>(
        "select exists(select 1 from backup_records where status in ('queued', 'dumping', 'uploading'))",
    )
    .fetch_one(&mut **transaction)
    .await
    .map(|value| value != 0)
    .map_err(|_| unavailable("check active SQLite backup record"))
}

async fn has_scheduled_record(
    transaction: &mut Transaction<'_, Sqlite>,
    at: DateTime<Utc>,
) -> AdminStoreResult<bool> {
    sqlx::query_scalar::<_, i64>(
        "select exists(select 1 from backup_records where scheduled_at_us = ?1)",
    )
    .bind(at.timestamp_micros())
    .fetch_one(&mut **transaction)
    .await
    .map(|value| value != 0)
    .map_err(|_| unavailable("check duplicate scheduled SQLite backup"))
}

async fn ensure_storage_identity_stable(
    current: &BackupSettings,
    command: &UpdateBackupStorageCommand,
    transaction: &mut Transaction<'_, Sqlite>,
) -> AdminStoreResult<()> {
    let identity_changed = current.endpoint.as_deref() != Some(command.endpoint.as_str())
        || current.region.as_deref() != Some(command.region.as_str())
        || current.bucket.as_deref() != Some(command.bucket.as_str())
        || current.force_path_style != command.force_path_style;
    if !identity_changed {
        return Ok(());
    }
    let exists = sqlx::query_scalar::<_, i64>("select exists(select 1 from backup_records)")
        .fetch_one(&mut **transaction)
        .await
        .map_err(|_| unavailable("check SQLite backup storage identity"))?;
    if exists != 0 {
        return Err(conflict(
            "storage identity cannot change while backup records exist",
        ));
    }
    Ok(())
}

async fn list_records(pool: &SqlitePool, clause: &str) -> AdminStoreResult<Vec<BackupRecord>> {
    let sql = format!("select {RECORD_COLUMNS} from backup_records {clause}");
    let rows = sqlx::query(sqlx::AssertSqlSafe(sql))
        .fetch_all(pool)
        .await
        .map_err(|_| unavailable("list SQLite backup records"))?;
    rows.iter().map(record_from_row).collect()
}

async fn list_records_with_limit(
    pool: &SqlitePool,
    clause: &str,
    limit: i64,
) -> AdminStoreResult<Vec<BackupRecord>> {
    let sql = format!("select {RECORD_COLUMNS} from backup_records {clause} limit ?1");
    let rows = sqlx::query(sqlx::AssertSqlSafe(sql))
        .bind(limit)
        .fetch_all(pool)
        .await
        .map_err(|_| unavailable("list bounded SQLite backup records"))?;
    rows.iter().map(record_from_row).collect()
}

async fn fetch_records(
    pool: &SqlitePool,
    sql: &str,
    at: Option<i64>,
    limit: i64,
) -> AdminStoreResult<Vec<BackupRecord>> {
    let rows = sqlx::query(sqlx::AssertSqlSafe(sql.to_owned()))
        .bind(at)
        .bind(limit)
        .fetch_all(pool)
        .await
        .map_err(|_| unavailable("fetch SQLite backup records"))?;
    rows.iter().map(record_from_row).collect()
}

fn record_from_row(row: &sqlx::sqlite::SqliteRow) -> AdminStoreResult<BackupRecord> {
    let trigger = read_string(row, "trigger_kind")?;
    let status = read_string(row, "status")?;
    let size = read_optional_i64(row, "size_bytes")?
        .map(u64::try_from)
        .transpose()
        .map_err(|_| invalid("size_bytes"))?;
    let attempt_count =
        u32::try_from(read_i64(row, "attempt_count")?).map_err(|_| invalid("attempt_count"))?;
    Ok(BackupRecord {
        id: read_string(row, "id")?,
        trigger_kind: BackupTriggerKind::parse(&trigger).ok_or_else(|| invalid("trigger_kind"))?,
        status: BackupStatus::parse(&status).ok_or_else(|| invalid("status"))?,
        scheduled_at: read_optional_datetime(row, "scheduled_at_us")?,
        object_key: read_string(row, "object_key")?,
        size_bytes: size,
        sha256: read_optional_string(row, "sha256")?,
        attempt_count,
        error_code: read_optional_string(row, "error_code")?,
        error_message: read_optional_string(row, "error_message")?,
        started_at: read_optional_datetime(row, "started_at_us")?,
        completed_at: read_optional_datetime(row, "completed_at_us")?,
        expires_at: read_optional_datetime(row, "expires_at_us")?,
        created_at: read_datetime(row, "created_at_us")?,
        updated_at: read_datetime(row, "updated_at_us")?,
    })
}

fn settings_from_row(row: &sqlx::sqlite::SqliteRow) -> AdminStoreResult<BackupSettings> {
    let revision = u64::try_from(read_i64(row, "storage_revision")?)
        .map_err(|_| invalid("storage_revision"))?;
    let secret = read_optional_string(row, "secret_access_key")?;
    Ok(BackupSettings {
        storage_revision: revision,
        endpoint: read_optional_string(row, "endpoint")?,
        region: read_optional_string(row, "region")?,
        bucket: read_optional_string(row, "bucket")?,
        access_key_id: read_optional_string(row, "access_key_id")?,
        secret_access_key: secret.map(SecretString::from),
        prefix: read_optional_string(row, "prefix")?,
        force_path_style: read_bool(row, "force_path_style")?,
        schedule_enabled: read_bool(row, "schedule_enabled")?,
        cron_expression: read_optional_string(row, "cron_expression")?,
        schedule_timezone: read_optional_string(row, "schedule_timezone")?,
        retention_days: u32::try_from(read_i64(row, "retention_days")?)
            .map_err(|_| invalid("retention_days"))?,
        retention_count: u32::try_from(read_i64(row, "retention_count")?)
            .map_err(|_| invalid("retention_count"))?,
        next_run_at: read_optional_datetime(row, "next_run_at_us")?,
        last_verified_at: read_optional_datetime(row, "last_verified_at_us")?,
        updated_at: read_datetime(row, "updated_at_us")?,
    })
}

fn storage_changed_fields(
    current: &BackupSettings,
    command: &UpdateBackupStorageCommand,
) -> Vec<String> {
    let mut fields = Vec::new();
    if current.endpoint.as_deref() != Some(command.endpoint.as_str()) {
        fields.push("endpoint".to_owned());
    }
    if current.region.as_deref() != Some(command.region.as_str()) {
        fields.push("region".to_owned());
    }
    if current.bucket.as_deref() != Some(command.bucket.as_str()) {
        fields.push("bucket".to_owned());
    }
    if current.access_key_id.as_deref() != Some(command.access_key_id.as_str()) {
        fields.push("access_key_id".to_owned());
    }
    if command.secret_access_key.as_ref().is_some_and(|secret| {
        current
            .secret_access_key
            .as_ref()
            .map(|value| value.expose_secret())
            != Some(secret.expose_secret())
    }) {
        fields.push("secret_access_key".to_owned());
    }
    if current.prefix.as_deref() != Some(command.prefix.as_str()) {
        fields.push("prefix".to_owned());
    }
    if current.force_path_style != command.force_path_style {
        fields.push("force_path_style".to_owned());
    }
    fields
}

fn read_i64(row: &sqlx::sqlite::SqliteRow, field: &'static str) -> AdminStoreResult<i64> {
    row.try_get(field)
        .map_err(|_| invalid("persisted integer field"))
}

fn read_optional_i64(
    row: &sqlx::sqlite::SqliteRow,
    field: &'static str,
) -> AdminStoreResult<Option<i64>> {
    row.try_get(field)
        .map_err(|_| invalid("persisted nullable integer field"))
}

fn read_string(row: &sqlx::sqlite::SqliteRow, field: &'static str) -> AdminStoreResult<String> {
    row.try_get(field)
        .map_err(|_| invalid("persisted text field"))
}

fn read_optional_string(
    row: &sqlx::sqlite::SqliteRow,
    field: &'static str,
) -> AdminStoreResult<Option<String>> {
    row.try_get(field)
        .map_err(|_| invalid("persisted nullable text field"))
}

fn read_bool(row: &sqlx::sqlite::SqliteRow, field: &'static str) -> AdminStoreResult<bool> {
    Ok(read_i64(row, field)? != 0)
}

fn read_datetime(
    row: &sqlx::sqlite::SqliteRow,
    field: &'static str,
) -> AdminStoreResult<DateTime<Utc>> {
    datetime_from_micros(read_i64(row, field)?).map_err(map_store_error)
}

fn read_optional_datetime(
    row: &sqlx::sqlite::SqliteRow,
    field: &'static str,
) -> AdminStoreResult<Option<DateTime<Utc>>> {
    read_optional_i64(row, field)?
        .map(datetime_from_micros)
        .transpose()
        .map_err(map_store_error)
}

fn admin_revision(revision: Revision) -> AdminStoreResult<AdminRevision> {
    AdminRevision::new(revision.get()).map_err(|_| invalid("config revision"))
}

fn map_store_error(error: StoreError) -> AdminStoreError {
    match error {
        StoreError::Unavailable { .. } => unavailable("SQLite store operation failed"),
        StoreError::NotFound { entity, .. } => not_found(entity),
        StoreError::Conflict { entity, .. } => conflict(entity),
        StoreError::InvalidData { message, .. } => invalid(&message),
    }
}

fn unavailable(message: &'static str) -> AdminStoreError {
    AdminStoreError::new(AdminStoreErrorKind::Unavailable, "backup", message)
}

fn not_found(entity: &'static str) -> AdminStoreError {
    AdminStoreError::new(AdminStoreErrorKind::NotFound, entity, "not found")
}

fn invalid(message: impl Into<String>) -> AdminStoreError {
    AdminStoreError::new(AdminStoreErrorKind::Invalid, "backup", message)
}

fn conflict(message: &'static str) -> AdminStoreError {
    AdminStoreError::new(AdminStoreErrorKind::Conflict, "backup record", message)
}
