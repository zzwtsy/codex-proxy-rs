//! SQLite 管理员密码摘要与安全审计 adapter。

use async_trait::async_trait;
use chrono::Utc;
use sqlx::{Sqlite, SqlitePool, Transaction};

use crate::{
    AdminAuditEvent, AdminSecurityAuditRepository, StoreError, StoreResult, require_nonempty,
};

use super::sqlite_unavailable;

const ENTITY: &str = "admin audit event";

#[derive(Clone)]
pub struct SqliteAdminSecurityAuditRepository {
    pool: SqlitePool,
}

impl SqliteAdminSecurityAuditRepository {
    #[must_use]
    pub const fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl AdminSecurityAuditRepository for SqliteAdminSecurityAuditRepository {
    async fn password_hash(&self, admin_user_id: &str) -> StoreResult<Option<String>> {
        require_nonempty("admin user", "id", admin_user_id)?;
        sqlx::query_scalar("select password_hash from admin_users where id = ?1")
            .bind(admin_user_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|_| sqlite_unavailable("read SQLite admin password hash"))
    }

    async fn change_password(
        &self,
        admin_user_id: &str,
        expected_hash: &str,
        password_hash: &str,
        audit: AdminAuditEvent,
    ) -> StoreResult<bool> {
        require_nonempty("admin user", "id", admin_user_id)?;
        require_nonempty("admin user", "password_hash", password_hash)?;
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|_| sqlite_unavailable("begin SQLite password change"))?;
        let changed = sqlx::query(
            "update admin_users
             set password_hash = ?3, updated_at_us = max(updated_at_us, ?4)
             where id = ?1 and password_hash = ?2",
        )
        .bind(admin_user_id)
        .bind(expected_hash)
        .bind(password_hash)
        .bind(Utc::now().timestamp_micros())
        .execute(&mut *transaction)
        .await
        .map_err(|_| sqlite_unavailable("change SQLite admin password"))?
        .rows_affected()
            == 1;
        if !changed {
            return Ok(false);
        }
        append_admin_audit_event_in_transaction(&mut transaction, audit).await?;
        transaction
            .commit()
            .await
            .map_err(|_| sqlite_unavailable("commit SQLite password change"))?;
        Ok(true)
    }

    async fn create_password_hash_if_absent(
        &self,
        admin_user_id: &str,
        password_hash: &str,
    ) -> StoreResult<bool> {
        require_nonempty("admin user", "id", admin_user_id)?;
        require_nonempty("admin user", "password_hash", password_hash)?;
        let now = Utc::now().timestamp_micros();
        let result = sqlx::query(
            "insert into admin_users (id, password_hash, created_at_us, updated_at_us)
             values (?1, ?2, ?3, ?3) on conflict (id) do nothing",
        )
        .bind(admin_user_id)
        .bind(password_hash)
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(|_| sqlite_unavailable("create SQLite admin password hash"))?;
        Ok(result.rows_affected() == 1)
    }

    async fn append_admin_audit_event(&self, event: AdminAuditEvent) -> StoreResult<()> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|_| sqlite_unavailable("begin SQLite admin audit insert"))?;
        append_admin_audit_event_in_transaction(&mut transaction, event).await?;
        transaction
            .commit()
            .await
            .map_err(|_| sqlite_unavailable("commit SQLite admin audit insert"))
    }
}

pub(crate) async fn append_admin_audit_event_in_transaction(
    transaction: &mut Transaction<'_, Sqlite>,
    event: AdminAuditEvent,
) -> StoreResult<()> {
    event.validate()?;
    let changed_fields_json =
        serde_json::to_string(&event.changed_fields).map_err(|_| StoreError::InvalidData {
            entity: ENTITY,
            message: "changed fields could not be encoded".to_owned(),
            source: None,
        })?;
    sqlx::query(
        "insert into admin_audit_events (
           id, actor_kind, actor_admin_user_id, actor_ref, admin_request_id,
           action, entity_kind, entity_ref, config_revision, changed_fields_json, created_at_us
         ) values (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
    )
    .bind(event.id)
    .bind(event.actor_kind.as_str())
    .bind(event.actor_admin_user_id)
    .bind(event.actor_ref)
    .bind(event.admin_request_id)
    .bind(event.action)
    .bind(event.entity_kind)
    .bind(event.entity_ref)
    .bind(event.config_revision)
    .bind(changed_fields_json)
    .bind(event.created_at.timestamp_micros())
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        sqlite_unavailable("append SQLite admin audit event in transaction").with_source(error)
    })?;
    Ok(())
}
