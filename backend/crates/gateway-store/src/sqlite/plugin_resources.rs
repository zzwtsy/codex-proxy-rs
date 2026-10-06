//! 插件资源写入在同一 SQLite 事务中验证实例 owner、归属和配置版本。

use std::collections::BTreeSet;

use async_trait::async_trait;
use chrono::Utc;
use gateway_admin::{
    model::{
        MutationContext,
        account_groups::NewAccountGroup,
        audit::MutationAuditOperation,
        client_keys::NewClientKey,
        plugin_resources::{
            GroupMembersChange, GroupMembersChanged, ManagedResource, PluginResourceOwner,
            ResourceMutation,
        },
    },
    ports::{
        plugin_resources::PluginResourceStore,
        store::{AdminStoreError, AdminStoreErrorKind, AdminStoreResult},
    },
};
use gateway_core::routing::AccountGroupId;
use sqlx::{Row, Sqlite, SqlitePool, Transaction};

use crate::{admin_revision, admin_store_error, mutation_audit};

use super::{
    acquire_write_lock, admin_client_keys, append_admin_audit_event_in_transaction,
    bump_config_revision, name_key::normalize_name_key, sqlite_unavailable,
};

const ENTITY: &str = "plugin resource";

#[derive(Clone)]
pub struct SqlitePluginResourceStore {
    pool: SqlitePool,
}

impl SqlitePluginResourceStore {
    #[must_use]
    pub const fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    async fn begin(
        &self,
        owner: &PluginResourceOwner,
    ) -> AdminStoreResult<Transaction<'_, Sqlite>> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|_| unavailable("begin plugin resource mutation"))?;
        acquire_write_lock(&mut transaction)
            .await
            .map_err(|error| admin_store_error(ENTITY, error))?;
        admin_client_keys::verify_plugin_owner(&mut transaction, owner)
            .await
            .map_err(|error| admin_store_error(ENTITY, error))?;
        Ok(transaction)
    }
}

#[async_trait]
impl PluginResourceStore for SqlitePluginResourceStore {
    async fn ensure_group(
        &self,
        owner: &PluginResourceOwner,
        resource_key: String,
        command: NewAccountGroup,
        context: &MutationContext,
    ) -> AdminStoreResult<ResourceMutation<ManagedResource>> {
        validate_resource_key(&resource_key)?;
        super::account_groups::validate_group_fields(
            &command.name,
            command.description.as_deref(),
        )?;
        let mut transaction = self.begin(owner).await?;
        if let Some(row) = sqlx::query(
            "select g.id, g.name, g.enabled
             from plugin_group_resources r
             join account_groups g on g.id = r.group_id
             where r.instance_id = ?1 and r.resource_key = ?2",
        )
        .bind(&owner.instance_id)
        .bind(&resource_key)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| unavailable("load plugin-owned account group"))?
        {
            return finish(
                transaction,
                context,
                MutationAuditOperation::AccountGroupPluginReconcile,
                "",
                false,
                resource(&row)?,
            )
            .await;
        }

        let now = Utc::now().timestamp_micros();
        let name_key = normalize_name_key(&command.name);
        sqlx::query(
            "insert into account_groups
             (id, name, description, color, fast_mode, enabled, created_at_us, updated_at_us, name_key)
             values (?1, ?2, ?3, ?4, ?5, 1, ?6, ?6, ?7)",
        )
        .bind(command.id.as_str())
        .bind(&command.name)
        .bind(&command.description)
        .bind(command.color.as_str())
        .bind(command.fast_mode.as_str())
        .bind(now)
        .bind(name_key)
        .execute(&mut *transaction)
        .await
        .map_err(|error| {
            admin_store_error(
                "account group",
                super::account_groups::map_write_error(error),
            )
        })?;

        sqlx::query(
            "insert into plugin_group_resources (instance_id, resource_key, group_id)
             values (?1, ?2, ?3)",
        )
        .bind(&owner.instance_id)
        .bind(&resource_key)
        .bind(command.id.as_str())
        .execute(&mut *transaction)
        .await
        .map_err(|_| unavailable("link plugin-owned account group"))?;

        finish(
            transaction,
            context,
            MutationAuditOperation::AccountGroupPluginReconcile,
            command.id.as_str(),
            true,
            ManagedResource {
                id: command.id.as_str().to_owned(),
                name: command.name,
                enabled: true,
            },
        )
        .await
    }

    async fn ensure_key(
        &self,
        owner: &PluginResourceOwner,
        resource_key: String,
        groups: Vec<String>,
        command: NewClientKey,
        context: &MutationContext,
    ) -> AdminStoreResult<ResourceMutation<ManagedResource>> {
        validate_resource_key(&resource_key)?;
        validate_resource_keys(&groups)?;
        let mut transaction = self.begin(owner).await?;
        if let Some(row) = sqlx::query(
            "select k.id, k.name, k.enabled
             from plugin_key_resources r
             join client_api_keys k on k.id = r.key_id
             where r.instance_id = ?1 and r.resource_key = ?2",
        )
        .bind(&owner.instance_id)
        .bind(&resource_key)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| unavailable("load plugin-owned Client API Key"))?
        {
            return finish(
                transaction,
                context,
                MutationAuditOperation::ClientApiKeyPluginReconcile,
                "",
                false,
                resource(&row)?,
            )
            .await;
        }

        if groups.is_empty() {
            return Err(invalid(
                "a plugin-owned key must reference at least one owned group",
            ));
        }
        let encoded_groups = serde_json::to_string(&groups)
            .map_err(|_| invalid("plugin group keys could not be encoded"))?;
        let group_ids = sqlx::query_scalar::<_, String>(
            "select group_id
             from plugin_group_resources
             where instance_id = ?1 and resource_key in (select value from json_each(?2))
             order by resource_key",
        )
        .bind(&owner.instance_id)
        .bind(encoded_groups)
        .fetch_all(&mut *transaction)
        .await
        .map_err(|_| unavailable("resolve plugin-owned key groups"))?;
        if group_ids.len() != groups.len() {
            return Err(conflict("plugin-owned account group is unavailable"));
        }
        let account_group_ids = group_ids
            .iter()
            .map(|id| {
                AccountGroupId::new(id.clone()).map_err(|_| invalid("invalid account group id"))
            })
            .collect::<AdminStoreResult<Vec<_>>>()?;
        let key = NewClientKey {
            request_profile_overrides: command.request_profile_overrides,
            id: command.id,
            name: command.name,
            label: command.label,
            group_ids: account_group_ids,
            limits: command.limits,
            budget: command.budget,
            plaintext: command.plaintext,
        };
        admin_client_keys::validate_new_key(&key)?;
        let profile_json = encode_profiles(&key)?;
        let group_ids = key
            .group_ids
            .iter()
            .map(|group| group.as_str().to_owned())
            .collect::<Vec<_>>();
        let now = Utc::now().timestamp_micros();
        admin_client_keys::insert_client_key_in_transaction(
            &mut transaction,
            &key,
            &profile_json,
            &group_ids,
            now,
        )
        .await
        .map_err(|error| admin_store_error("client API key", error))?;
        sqlx::query(
            "insert into plugin_key_resources (instance_id, resource_key, key_id)
             values (?1, ?2, ?3)",
        )
        .bind(&owner.instance_id)
        .bind(&resource_key)
        .bind(key.id.as_str())
        .execute(&mut *transaction)
        .await
        .map_err(|_| unavailable("link plugin-owned Client API Key"))?;

        finish(
            transaction,
            context,
            MutationAuditOperation::ClientApiKeyPluginReconcile,
            key.id.as_str(),
            true,
            ManagedResource {
                id: key.id.as_str().to_owned(),
                name: key.name.trim().to_owned(),
                enabled: true,
            },
        )
        .await
    }

    async fn change_members(
        &self,
        owner: &PluginResourceOwner,
        command: GroupMembersChange,
        context: &MutationContext,
    ) -> AdminStoreResult<ResourceMutation<GroupMembersChanged>> {
        validate_resource_key(&command.resource_key)?;
        let add: BTreeSet<_> = command.add.iter().collect();
        let remove: BTreeSet<_> = command.remove.iter().collect();
        if command.add.len().saturating_add(command.remove.len()) > 200
            || !add.is_disjoint(&remove)
            || add.len() != command.add.len()
            || remove.len() != command.remove.len()
        {
            return Err(invalid("invalid account membership change"));
        }
        let mut transaction = self.begin(owner).await?;
        let group_id = sqlx::query_scalar::<_, String>(
            "select group_id from plugin_group_resources
             where instance_id = ?1 and resource_key = ?2",
        )
        .bind(&owner.instance_id)
        .bind(&command.resource_key)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| unavailable("load plugin-owned account group"))?
        .ok_or_else(|| conflict("plugin-owned account group is unavailable"))?;

        let add_json = serde_json::to_string(&command.add)
            .map_err(|_| invalid("account IDs could not be encoded"))?;
        let added = sqlx::query(
            "insert into account_group_accounts (account_group_id, provider_account_id, created_at_us)
             select ?1, a.id, ?3 from provider_accounts a
             where a.id in (select value from json_each(?2))
             on conflict do nothing",
        )
        .bind(&group_id)
        .bind(add_json)
        .bind(Utc::now().timestamp_micros())
        .execute(&mut *transaction)
        .await
        .map_err(|_| unavailable("add plugin-owned account group members"))?
        .rows_affected();

        let remove_json = serde_json::to_string(&command.remove)
            .map_err(|_| invalid("account IDs could not be encoded"))?;
        let removed = sqlx::query(
            "delete from account_group_accounts
             where account_group_id = ?1
               and provider_account_id in (select value from json_each(?2))",
        )
        .bind(&group_id)
        .bind(remove_json)
        .execute(&mut *transaction)
        .await
        .map_err(|_| unavailable("remove plugin-owned account group members"))?
        .rows_affected();

        finish(
            transaction,
            context,
            MutationAuditOperation::AccountGroupPluginReconcile,
            &group_id,
            added + removed > 0,
            GroupMembersChanged { added, removed },
        )
        .await
    }
}

async fn finish<T>(
    mut transaction: Transaction<'_, Sqlite>,
    context: &MutationContext,
    operation: MutationAuditOperation,
    id: &str,
    changed: bool,
    value: T,
) -> AdminStoreResult<ResourceMutation<T>> {
    let revision = if changed {
        let revision = bump_config_revision(&mut transaction, Utc::now().timestamp_micros())
            .await
            .map_err(|error| admin_store_error(ENTITY, error))?;
        let mut audit = mutation_audit(
            context,
            operation,
            id,
            vec!["plugin_owned_resource".to_owned()],
        );
        audit.config_revision =
            Some(i64::try_from(revision.get()).map_err(|_| invalid("revision overflow"))?);
        append_admin_audit_event_in_transaction(&mut transaction, audit)
            .await
            .map_err(|error| admin_store_error(ENTITY, error))?;
        Some(admin_revision(revision)?)
    } else {
        None
    };
    transaction
        .commit()
        .await
        .map_err(|_| unavailable("commit plugin resource mutation"))?;
    Ok(ResourceMutation { revision, value })
}

fn resource(row: &sqlx::sqlite::SqliteRow) -> AdminStoreResult<ManagedResource> {
    let enabled = row
        .try_get::<i64, _>("enabled")
        .map_err(|_| unavailable("decode plugin resource enabled state"))?
        != 0;
    Ok(ManagedResource {
        id: row
            .try_get("id")
            .map_err(|_| unavailable("decode plugin resource ID"))?,
        name: row
            .try_get("name")
            .map_err(|_| unavailable("decode plugin resource name"))?,
        enabled,
    })
}

fn validate_resource_key(key: &str) -> AdminStoreResult<()> {
    if key.is_empty()
        || key.len() > 64
        || !key.as_bytes()[0].is_ascii_alphanumeric()
        || !key.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"_.-".contains(&byte)
        })
    {
        return Err(invalid("invalid plugin resource key"));
    }
    Ok(())
}

fn validate_resource_keys(keys: &[String]) -> AdminStoreResult<()> {
    let unique: BTreeSet<_> = keys.iter().collect();
    if keys.is_empty() || keys.len() > 64 || unique.len() != keys.len() {
        return Err(invalid("invalid plugin account group selection"));
    }
    for key in keys {
        validate_resource_key(key)?;
    }
    Ok(())
}

fn encode_profiles(command: &NewClientKey) -> AdminStoreResult<String> {
    let values = command
        .request_profile_overrides
        .iter()
        .map(|(provider, profile)| (provider.as_str(), profile.expose_to_provider()))
        .collect::<std::collections::BTreeMap<_, _>>();
    serde_json::to_string(&values)
        .map_err(|_| invalid("Client API Key profile could not be encoded"))
}

fn invalid(message: &'static str) -> AdminStoreError {
    AdminStoreError::new(AdminStoreErrorKind::Invalid, ENTITY, message)
}

fn conflict(message: &'static str) -> AdminStoreError {
    AdminStoreError::new(AdminStoreErrorKind::Conflict, ENTITY, message)
}

fn unavailable(operation: &'static str) -> AdminStoreError {
    admin_store_error(ENTITY, sqlite_unavailable(operation))
}
