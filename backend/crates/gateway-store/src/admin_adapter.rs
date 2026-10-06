//! Admin 认证与设置 adapter

use super::*;
use std::{collections::BTreeMap, sync::Arc};

use chrono::{DateTime, Utc};
use gateway_admin::model::audit::MutationAuditOperation;
use gateway_admin::{model::accounts::AccountRuntimeSnapshot, ports::store::AccountRuntimeStore};

/// Admin 账号运行态查询所需的后端中立 cooldown 能力。
#[async_trait::async_trait]
pub(crate) trait AccountRuntimeStateRepository: Send + Sync {
    async fn active_rate_limits(&self) -> StoreResult<AccountRuntimeSnapshot>;
    async fn account_cooldowns(
        &self,
        account_ids: &[String],
    ) -> StoreResult<BTreeMap<String, gateway_core::account::AccountCooldown>>;
    async fn active_freezes(
        &self,
    ) -> StoreResult<BTreeMap<String, gateway_admin::model::accounts::AccountFreeze>>;
    async fn capacity_peaks(&self, account_ids: &[String]) -> StoreResult<BTreeMap<String, u32>>;
    async fn finish_freeze(
        &self,
        account_id: &str,
        expected: &gateway_admin::model::accounts::AccountFreeze,
        postpone_until: Option<DateTime<Utc>>,
    ) -> StoreResult<bool>;
}

/// 将账号 cooldown 状态与凭据租约信号组合为 Admin 运行态端口。
#[derive(Clone)]
pub(crate) struct AccountRuntimeStoreAdapter {
    state: Arc<dyn AccountRuntimeStateRepository>,
    leases: Arc<dyn CredentialLeaseRepository>,
}

impl AccountRuntimeStoreAdapter {
    #[must_use]
    pub(crate) fn new(
        state: Arc<dyn AccountRuntimeStateRepository>,
        leases: Arc<dyn CredentialLeaseRepository>,
    ) -> Self {
        Self { state, leases }
    }
}

#[async_trait::async_trait]
impl AccountRuntimeStore for AccountRuntimeStoreAdapter {
    async fn active_rate_limits(&self) -> AdminStoreResult<AccountRuntimeSnapshot> {
        self.state
            .active_rate_limits()
            .await
            .map_err(|error| admin_store_error("account runtime", error))
    }

    async fn account_runtime(
        &self,
        account_ids: &[String],
    ) -> AdminStoreResult<AccountRuntimeSnapshot> {
        let cooldown = self
            .state
            .account_cooldowns(account_ids)
            .await
            .map_err(|error| admin_store_error("account runtime", error))?;
        let in_flight = self
            .leases
            .credential_runtime_signals(account_ids)
            .await
            .ok()
            .map(|signals| {
                signals
                    .into_iter()
                    .map(|signal| (signal.resource_id, u64::from(signal.in_flight)))
                    .collect()
            });
        Ok(AccountRuntimeSnapshot {
            cooldown,
            in_flight,
        })
    }

    async fn active_freezes(
        &self,
    ) -> AdminStoreResult<BTreeMap<String, gateway_admin::model::accounts::AccountFreeze>> {
        self.state
            .active_freezes()
            .await
            .map_err(|error| admin_store_error("account runtime", error))
    }

    async fn capacity_peaks(
        &self,
        account_ids: &[String],
    ) -> AdminStoreResult<BTreeMap<String, u32>> {
        self.state
            .capacity_peaks(account_ids)
            .await
            .map_err(|error| admin_store_error("account runtime", error))
    }

    async fn finish_freeze(
        &self,
        account_id: &str,
        expected: &gateway_admin::model::accounts::AccountFreeze,
        postpone_until: Option<DateTime<Utc>>,
    ) -> AdminStoreResult<bool> {
        self.state
            .finish_freeze(account_id, expected, postpone_until)
            .await
            .map_err(|error| admin_store_error("account runtime", error))
    }
}

pub(crate) struct AuthStoreAdapter {
    pub(crate) security: Arc<dyn AdminSecurityAuditRepository>,
    pub(crate) settings: Arc<dyn RuntimeSettingsRepository>,
    pub(crate) state: Arc<dyn AuthStateRepository>,
    pub(crate) keys: Arc<dyn ClientKeyEnabledRepository>,
}

/// AuthStore 只需查询 Key 是否启用，不依赖完整管理端 Key 仓储。
#[async_trait::async_trait]
pub(crate) trait ClientKeyEnabledRepository: Send + Sync {
    async fn is_enabled(&self, id: &gateway_core::policy::ClientApiKeyId)
    -> AdminStoreResult<bool>;
}

#[async_trait::async_trait]
impl ClientKeyEnabledRepository for postgres::PgAdminClientKeyStore {
    async fn is_enabled(
        &self,
        id: &gateway_core::policy::ClientApiKeyId,
    ) -> AdminStoreResult<bool> {
        postgres::PgAdminClientKeyStore::is_enabled(self, id).await
    }
}

#[async_trait::async_trait]
pub(crate) trait AdminSettingsRepository: Send + Sync {
    async fn load_runtime_settings(&self) -> StoreResult<crate::runtime_settings::RuntimeSettings>;
    async fn load_pricing(&self) -> StoreResult<gateway_admin::model::pricing::StoredPricing>;
    async fn sync_pricing(
        &self,
        changes: gateway_admin::model::pricing::PricingSyncChanges,
        audit: AdminAuditEvent,
    ) -> StoreResult<Revision>;
    async fn update_pricing(
        &self,
        command: gateway_admin::model::pricing::UpdatePricing,
        audit: AdminAuditEvent,
    ) -> StoreResult<Revision>;
    async fn replace_runtime_settings(
        &self,
        expected_revision: Revision,
        settings: RuntimeSettingsUpdate,
        audit: AdminAuditEvent,
    ) -> StoreResult<crate::runtime_settings::RuntimeSettings>;
    async fn replace_admin_api_key(
        &self,
        admin_api_key: Option<String>,
        audit: AdminAuditEvent,
    ) -> StoreResult<Revision>;
}

#[async_trait::async_trait]
impl AdminSettingsRepository for sqlite::SqliteAdminSettingsRepository {
    async fn load_runtime_settings(&self) -> StoreResult<crate::runtime_settings::RuntimeSettings> {
        sqlite::SqliteAdminSettingsRepository::load_runtime_settings(self).await
    }

    async fn load_pricing(&self) -> StoreResult<gateway_admin::model::pricing::StoredPricing> {
        sqlite::SqliteAdminSettingsRepository::load_pricing(self).await
    }

    async fn sync_pricing(
        &self,
        changes: gateway_admin::model::pricing::PricingSyncChanges,
        audit: AdminAuditEvent,
    ) -> StoreResult<Revision> {
        sqlite::SqliteAdminSettingsRepository::sync_pricing(self, changes, audit).await
    }

    async fn update_pricing(
        &self,
        command: gateway_admin::model::pricing::UpdatePricing,
        audit: AdminAuditEvent,
    ) -> StoreResult<Revision> {
        sqlite::SqliteAdminSettingsRepository::update_pricing(self, command, audit).await
    }

    async fn replace_runtime_settings(
        &self,
        expected_revision: Revision,
        settings: RuntimeSettingsUpdate,
        audit: AdminAuditEvent,
    ) -> StoreResult<crate::runtime_settings::RuntimeSettings> {
        sqlite::SqliteAdminSettingsRepository::replace_runtime_settings(
            self,
            expected_revision,
            settings,
            audit,
        )
        .await
    }

    async fn replace_admin_api_key(
        &self,
        admin_api_key: Option<String>,
        audit: AdminAuditEvent,
    ) -> StoreResult<Revision> {
        sqlite::SqliteAdminSettingsRepository::replace_admin_api_key(self, admin_api_key, audit)
            .await
    }
}

#[async_trait::async_trait]
impl AdminSettingsRepository for postgres::PgControlPlaneRepository {
    async fn load_runtime_settings(&self) -> StoreResult<crate::runtime_settings::RuntimeSettings> {
        postgres::ControlPlaneRepository::load_control_plane(self)
            .await
            .map(|snapshot| snapshot.settings)
    }

    async fn load_pricing(&self) -> StoreResult<gateway_admin::model::pricing::StoredPricing> {
        postgres::PgControlPlaneRepository::load_pricing(self).await
    }

    async fn sync_pricing(
        &self,
        changes: gateway_admin::model::pricing::PricingSyncChanges,
        audit: AdminAuditEvent,
    ) -> StoreResult<Revision> {
        postgres::PgControlPlaneRepository::sync_pricing(self, changes, audit).await
    }

    async fn update_pricing(
        &self,
        command: gateway_admin::model::pricing::UpdatePricing,
        audit: AdminAuditEvent,
    ) -> StoreResult<Revision> {
        postgres::PgControlPlaneRepository::update_pricing(self, command, audit).await
    }

    async fn replace_runtime_settings(
        &self,
        expected_revision: Revision,
        settings: RuntimeSettingsUpdate,
        audit: AdminAuditEvent,
    ) -> StoreResult<crate::runtime_settings::RuntimeSettings> {
        postgres::ControlPlaneRepository::replace_control_plane(
            self,
            postgres::ControlPlaneReplacement {
                expected_revision,
                settings,
                audit,
            },
        )
        .await
        .map(|snapshot| snapshot.settings)
    }

    async fn replace_admin_api_key(
        &self,
        admin_api_key: Option<String>,
        audit: AdminAuditEvent,
    ) -> StoreResult<Revision> {
        postgres::ControlPlaneRepository::replace_admin_api_key(self, admin_api_key, audit).await
    }
}

pub(crate) struct AdminSettingsStoreAdapter {
    pub(crate) control_plane: Arc<dyn AdminSettingsRepository>,
}

#[async_trait::async_trait]
impl SettingsStore for AdminSettingsStoreAdapter {
    async fn load_pricing(&self) -> AdminStoreResult<gateway_admin::model::pricing::StoredPricing> {
        self.control_plane
            .load_pricing()
            .await
            .map_err(|error| admin_store_error("model pricing", error))
    }

    async fn sync_pricing(
        &self,
        changes: gateway_admin::model::pricing::PricingSyncChanges,
        context: &MutationContext,
    ) -> AdminStoreResult<gateway_admin::model::Revision> {
        let audit = mutation_audit(
            context,
            MutationAuditOperation::ModelPricingSync,
            "models.dev",
            vec!["synced".to_owned()],
        );
        let revision = self
            .control_plane
            .sync_pricing(changes, audit)
            .await
            .map_err(|error| admin_store_error("model pricing sync", error))?;
        admin_revision(revision)
    }

    async fn update_pricing(
        &self,
        command: gateway_admin::model::pricing::UpdatePricing,
        context: &MutationContext,
    ) -> AdminStoreResult<gateway_admin::model::Revision> {
        let audit = mutation_audit(
            context,
            MutationAuditOperation::ModelPricingUpdate,
            &command.provider,
            command.models.clone(),
        );
        let revision = self
            .control_plane
            .update_pricing(command, audit)
            .await
            .map_err(|error| admin_store_error("model pricing", error))?;
        admin_revision(revision)
    }

    async fn load_runtime_settings(&self) -> AdminStoreResult<AdminRuntimeSettings> {
        let settings = self
            .control_plane
            .load_runtime_settings()
            .await
            .map_err(|error| admin_store_error("runtime settings", error))?;
        admin_runtime_settings(settings)
    }

    async fn admin_api_key_exists(&self) -> AdminStoreResult<bool> {
        self.control_plane
            .load_runtime_settings()
            .await
            .map(|settings| settings.admin_api_key.is_some())
            .map_err(|error| admin_store_error("admin API key", error))
    }

    async fn replace_runtime_settings(
        &self,
        command: ReplaceRuntimeSettings,
        context: &MutationContext,
    ) -> AdminStoreResult<AdminRuntimeSettings> {
        let expected_revision = store_revision(command.expected_revision)?;
        let settings = RuntimeSettingsUpdate {
            request_profile_updates: command.request_profile_updates,
            refresh_margin_seconds: command.refresh_margin_seconds,
            refresh_concurrency: command.refresh_concurrency,
            max_concurrent_per_account: command.max_concurrent_per_account,
            request_location_enabled: command.request_location_enabled,
            request_location: command.request_location,
            request_interval_ms: command.request_interval_ms,
            max_waiting_per_key: command.max_waiting_per_key,
            max_waiting_per_account: command.max_waiting_per_account,
            concurrency_wait_timeout_seconds: command.concurrency_wait_timeout_seconds,
            openai_guardian_reserved_concurrency: command.openai_guardian_reserved_concurrency,
            responses_max_decompressed_body_bytes: command.responses_max_decompressed_body_bytes,
            smart_scheduling: command.smart_scheduling,
            rotation_strategy: command.rotation_strategy.as_str().to_owned(),
            model_mappings: store_model_mappings(command.model_mappings),
            min_codex_desktop_version: command.min_codex_desktop_version,
            min_codex_cli_version: command.min_codex_cli_version,
            usage_retention_days: command.usage_retention_days,
            ops_event_retention_days: command.ops_event_retention_days,
            audit_retention_days: command.audit_retention_days,
            account_auto_freeze_enabled: command.account_auto_freeze_enabled,
            account_auto_freeze_threshold: command.account_auto_freeze_threshold,
            account_auto_freeze_window_seconds: command.account_auto_freeze_window_seconds,
            account_auto_freeze_duration_seconds: command.account_auto_freeze_duration_seconds,
            account_auto_freeze_probe_enabled: command.account_auto_freeze_probe_enabled,
            account_auto_freeze_probe_model: command.account_auto_freeze_probe_model,
            account_auto_freeze_adaptive_concurrency: command
                .account_auto_freeze_adaptive_concurrency,
            account_warmup_enabled: command.account_warmup_enabled,
            account_warmup_schedule_time: command.account_warmup_schedule_time,
            account_warmup_model: command.account_warmup_model,
        };
        let audit = mutation_audit(
            context,
            MutationAuditOperation::RuntimeSettingsReplace,
            "1",
            vec![
                "provider_request_profiles_json".to_owned(),
                "request_location_enabled".to_owned(),
                "request_location_json".to_owned(),
                "model_mappings_json".to_owned(),
                "refresh_margin_seconds".to_owned(),
                "refresh_concurrency".to_owned(),
                "max_concurrent_per_account".to_owned(),
                "request_interval_ms".to_owned(),
                "max_waiting_per_key".to_owned(),
                "max_waiting_per_account".to_owned(),
                "concurrency_wait_timeout_seconds".to_owned(),
                "openai_guardian_reserved_concurrency".to_owned(),
                "responses_max_decompressed_body_bytes".to_owned(),
                "rotation_strategy".to_owned(),
                "smart_scheduling_json".to_owned(),
                "min_codex_desktop_version".to_owned(),
                "min_codex_cli_version".to_owned(),
                "retention".to_owned(),
                "account_auto_freeze".to_owned(),
            ],
        );
        let settings = self
            .control_plane
            .replace_runtime_settings(expected_revision, settings, audit)
            .await
            .map_err(|error| admin_store_error("runtime settings", error))?;
        admin_runtime_settings(settings)
    }

    async fn replace_admin_api_key(
        &self,
        key: AdminApiKey,
        context: &MutationContext,
    ) -> AdminStoreResult<AdminApiKeyMutation> {
        self.replace_admin_api_key_value(Some(key.expose_for_auth().to_owned()), context)
            .await
    }

    async fn delete_admin_api_key(
        &self,
        context: &MutationContext,
    ) -> AdminStoreResult<AdminApiKeyMutation> {
        self.replace_admin_api_key_value(None, context).await
    }
}

impl AdminSettingsStoreAdapter {
    async fn replace_admin_api_key_value(
        &self,
        admin_api_key: Option<String>,
        context: &MutationContext,
    ) -> AdminStoreResult<AdminApiKeyMutation> {
        let exists = admin_api_key.is_some();
        let revision = self
            .control_plane
            .replace_admin_api_key(
                admin_api_key,
                mutation_audit(
                    context,
                    MutationAuditOperation::AdminApiKeyChanged { exists },
                    "1",
                    vec!["admin_api_key".to_owned()],
                ),
            )
            .await
            .map_err(|error| admin_store_error("admin API key", error))?;
        Ok(AdminApiKeyMutation {
            config_revision: admin_revision(revision)?,
            exists,
        })
    }
}

pub(crate) fn admin_runtime_settings(
    settings: crate::runtime_settings::RuntimeSettings,
) -> AdminStoreResult<AdminRuntimeSettings> {
    let rotation_strategy = AdminRotationStrategy::parse(settings.rotation_strategy.as_str())
        .ok_or_else(|| {
            AdminStoreError::new(
                AdminStoreErrorKind::Invalid,
                "runtime settings",
                "rotation strategy is invalid",
            )
        })?;
    let model_mappings = settings
        .model_mappings
        .into_iter()
        .map(|(public, upstream)| {
            let public = gateway_core::routing::PublicModelId::new(public).map_err(|_| {
                AdminStoreError::new(
                    AdminStoreErrorKind::Invalid,
                    "runtime settings",
                    "public model mapping is invalid",
                )
            })?;
            let upstream = gateway_core::routing::UpstreamModelId::new(upstream).map_err(|_| {
                AdminStoreError::new(
                    AdminStoreErrorKind::Invalid,
                    "runtime settings",
                    "upstream model mapping is invalid",
                )
            })?;
            Ok((public, upstream))
        })
        .collect::<AdminStoreResult<ModelMappings>>()?;
    Ok(AdminRuntimeSettings {
        request_profiles: settings.request_profiles,
        config_revision: admin_revision(settings.config_revision)?,
        request_location_enabled: settings.request_location_enabled,
        request_location: settings.request_location,
        model_mappings,
        refresh_margin_seconds: settings.refresh_margin_seconds,
        refresh_concurrency: settings.refresh_concurrency,
        max_concurrent_per_account: settings.max_concurrent_per_account,
        request_interval_ms: settings.request_interval_ms,
        max_waiting_per_key: settings.max_waiting_per_key,
        max_waiting_per_account: settings.max_waiting_per_account,
        concurrency_wait_timeout_seconds: settings.concurrency_wait_timeout_seconds,
        openai_guardian_reserved_concurrency: settings.openai_guardian_reserved_concurrency,
        responses_max_decompressed_body_bytes: settings.responses_max_decompressed_body_bytes,
        smart_scheduling: settings.smart_scheduling,
        rotation_strategy,
        min_codex_desktop_version: settings.min_codex_desktop_version,
        min_codex_cli_version: settings.min_codex_cli_version,
        usage_retention_days: settings.usage_retention_days,
        ops_event_retention_days: settings.ops_event_retention_days,
        audit_retention_days: settings.audit_retention_days,
        account_auto_freeze_enabled: settings.account_auto_freeze_enabled,
        account_auto_freeze_threshold: settings.account_auto_freeze_threshold,
        account_auto_freeze_window_seconds: settings.account_auto_freeze_window_seconds,
        account_auto_freeze_duration_seconds: settings.account_auto_freeze_duration_seconds,
        account_auto_freeze_probe_enabled: settings.account_auto_freeze_probe_enabled,
        account_auto_freeze_probe_model: settings.account_auto_freeze_probe_model,
        account_auto_freeze_adaptive_concurrency: settings.account_auto_freeze_adaptive_concurrency,
        account_warmup_enabled: settings.account_warmup_enabled,
        account_warmup_schedule_time: settings.account_warmup_schedule_time,
        account_warmup_model: settings.account_warmup_model,
        updated_at: settings.updated_at,
    })
}

pub(crate) fn store_model_mappings(
    mappings: ModelMappings,
) -> std::collections::BTreeMap<String, String> {
    mappings
        .into_iter()
        .map(|(public, upstream)| (public.as_str().to_owned(), upstream.as_str().to_owned()))
        .collect()
}

#[async_trait::async_trait]
impl AuthStore for AuthStoreAdapter {
    async fn load_password_hash(&self, admin_user_id: &str) -> AdminStoreResult<Option<String>> {
        self.security
            .password_hash(admin_user_id)
            .await
            .map_err(|error| admin_store_error("admin authentication", error))
    }

    async fn change_password(
        &self,
        admin_user_id: &str,
        expected_hash: &str,
        password_hash: &str,
        audit: AdminAuditModel,
    ) -> AdminStoreResult<bool> {
        self.security
            .change_password(
                admin_user_id,
                expected_hash,
                password_hash,
                auth_audit_record(audit)?,
            )
            .await
            .map_err(|error| admin_store_error("administrator password", error))
    }

    async fn create_password_hash_if_absent(
        &self,
        admin_user_id: &str,
        password_hash: &str,
    ) -> AdminStoreResult<bool> {
        self.security
            .create_password_hash_if_absent(admin_user_id, password_hash)
            .await
            .map_err(|error| admin_store_error("admin authentication", error))
    }

    async fn load_admin_api_key(&self) -> AdminStoreResult<Option<AdminApiKey>> {
        self.settings
            .load_runtime_settings()
            .await
            .map(|settings| settings.admin_api_key.map(AdminApiKey::new))
            .map_err(|error| admin_store_error("admin API key", error))
    }

    async fn load_session(&self, session_id: &str) -> AdminStoreResult<Option<AuthSession>> {
        self.state
            .load_session(session_id)
            .await
            .map_err(|error| admin_store_error("authentication session", error))?
            .map(auth_session)
            .transpose()
    }

    async fn store_session(&self, session_id: &str, session: &AuthSession) -> AdminStoreResult<()> {
        self.state
            .store_session(session_id, &auth_session_record(session))
            .await
            .map_err(|error| admin_store_error("authentication session", error))
    }

    async fn renew_session(
        &self,
        session_id: &str,
        expected: &AuthSession,
        expires_at: chrono::DateTime<chrono::Utc>,
    ) -> AdminStoreResult<Option<AuthSession>> {
        self.state
            .renew_session(session_id, &auth_session_record(expected), expires_at)
            .await
            .map_err(|error| admin_store_error("authentication session renewal", error))?
            .map(auth_session)
            .transpose()
    }

    async fn delete_session(&self, session_id: &str) -> AdminStoreResult<Option<AuthSession>> {
        self.state
            .delete_session(session_id)
            .await
            .map_err(|error| admin_store_error("authentication session", error))?
            .map(auth_session)
            .transpose()
    }

    async fn client_key_enabled(
        &self,
        id: &gateway_core::policy::ClientApiKeyId,
    ) -> AdminStoreResult<bool> {
        self.keys.is_enabled(id).await
    }

    async fn consume_login_attempt(
        &self,
        source_ip: std::net::IpAddr,
        source_limit: u32,
        global_limit: u32,
        window: std::time::Duration,
    ) -> AdminStoreResult<Option<std::time::Duration>> {
        self.state
            .consume_login_attempt(&source_ip.to_string(), source_limit, global_limit, window)
            .await
            .map_err(|error| admin_store_error("login limit", error))
    }

    async fn append_audit_event(&self, event: AdminAuditModel) -> AdminStoreResult<()> {
        self.security
            .append_admin_audit_event(auth_audit_record(event)?)
            .await
            .map_err(|error| admin_store_error("admin audit", error))
    }
}

fn auth_audit_record(event: AdminAuditModel) -> AdminStoreResult<AdminAuditEvent> {
    let config_revision = event
        .config_revision
        .map(|revision| i64::try_from(revision.get()))
        .transpose()
        .map_err(|_| {
            AdminStoreError::new(
                AdminStoreErrorKind::Invalid,
                "admin audit",
                "config revision is outside the supported range",
            )
        })?;
    Ok(AdminAuditEvent {
        id: event.id,
        actor_kind: AdminAuditActorKind::from(event.actor_kind),
        actor_admin_user_id: event.actor_admin_user_id,
        actor_ref: event.actor_ref,
        admin_request_id: event.request_id,
        action: event.action,
        entity_kind: event.entity_kind,
        entity_ref: event.entity_ref,
        config_revision,
        changed_fields: event.changed_fields,
        created_at: event.occurred_at,
    })
}

fn auth_session(record: AuthSessionRecord) -> AdminStoreResult<AuthSession> {
    let subject = match record.subject {
        SessionSubjectRecord::Admin {
            admin_user_id,
            credential_fingerprint,
        } => SessionSubject::Admin {
            admin_user_id,
            credential_fingerprint,
        },
        SessionSubjectRecord::Key { client_key_id } => SessionSubject::Key {
            client_key_id: gateway_core::policy::ClientApiKeyId::new(client_key_id).map_err(
                |_| {
                    AdminStoreError::new(
                        AdminStoreErrorKind::Invalid,
                        "authentication session",
                        "client key ID is invalid",
                    )
                },
            )?,
        },
    };
    Ok(AuthSession {
        subject,
        expires_at: record.expires_at,
        absolute_expires_at: record.absolute_expires_at,
    })
}

fn auth_session_record(session: &AuthSession) -> redis::AuthSessionRecord {
    let subject = match &session.subject {
        SessionSubject::Admin {
            admin_user_id,
            credential_fingerprint,
        } => redis::SessionSubjectRecord::Admin {
            admin_user_id: admin_user_id.clone(),
            credential_fingerprint: credential_fingerprint.clone(),
        },
        SessionSubject::Key { client_key_id } => redis::SessionSubjectRecord::Key {
            client_key_id: client_key_id.as_str().to_owned(),
        },
    };
    redis::AuthSessionRecord {
        subject,
        expires_at: session.expires_at,
        absolute_expires_at: session.absolute_expires_at,
    }
}
