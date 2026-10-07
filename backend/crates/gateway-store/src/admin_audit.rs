//! 管理员持久审计合同；数据库编码由各 Store adapter 拥有。

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::{StoreError, StoreResult, require_nonempty};

const ENTITY: &str = "admin audit event";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdminAuditActorKind {
    AdminSession,
    AdminApiKey,
    System,
    Anonymous,
}

impl AdminAuditActorKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AdminSession => "admin_session",
            Self::AdminApiKey => "admin_api_key",
            Self::System => "system",
            Self::Anonymous => "anonymous",
        }
    }
}

impl From<gateway_admin::model::auth::AuditActorKind> for AdminAuditActorKind {
    fn from(value: gateway_admin::model::auth::AuditActorKind) -> Self {
        use gateway_admin::model::auth::AuditActorKind;
        match value {
            AuditActorKind::AdminSession => Self::AdminSession,
            AuditActorKind::AdminApiKey => Self::AdminApiKey,
            AuditActorKind::System => Self::System,
            AuditActorKind::Anonymous => Self::Anonymous,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminAuditEvent {
    pub id: String,
    pub actor_kind: AdminAuditActorKind,
    pub actor_admin_user_id: Option<String>,
    pub actor_ref: String,
    pub admin_request_id: Option<String>,
    pub action: String,
    pub entity_kind: String,
    pub entity_ref: String,
    pub config_revision: Option<i64>,
    pub changed_fields: Vec<String>,
    pub created_at: DateTime<Utc>,
}

impl AdminAuditEvent {
    pub fn validate(&self) -> StoreResult<()> {
        require_nonempty(ENTITY, "id", &self.id)?;
        require_nonempty(ENTITY, "actor_ref", &self.actor_ref)?;
        require_nonempty(ENTITY, "action", &self.action)?;
        require_nonempty(ENTITY, "entity_kind", &self.entity_kind)?;
        require_nonempty(ENTITY, "entity_ref", &self.entity_ref)?;
        if self.config_revision.is_some_and(|revision| revision <= 0) {
            return Err(invalid("config_revision must be positive"));
        }
        if self.changed_fields.len() > 64
            || self
                .changed_fields
                .iter()
                .any(|field| field.trim().is_empty())
        {
            return Err(invalid(
                "changed_fields must contain at most 64 non-empty names",
            ));
        }
        Ok(())
    }
}

#[async_trait]
pub trait AdminSecurityAuditRepository: Send + Sync {
    async fn password_hash(&self, admin_user_id: &str) -> StoreResult<Option<String>>;

    async fn change_password(
        &self,
        admin_user_id: &str,
        expected_hash: &str,
        password_hash: &str,
        audit: AdminAuditEvent,
    ) -> StoreResult<bool>;

    async fn create_password_hash_if_absent(
        &self,
        admin_user_id: &str,
        password_hash: &str,
    ) -> StoreResult<bool>;

    async fn append_admin_audit_event(&self, event: AdminAuditEvent) -> StoreResult<()>;
}

fn invalid(message: &str) -> StoreError {
    StoreError::InvalidData {
        entity: ENTITY,
        message: message.to_owned(),
        source: None,
    }
}
