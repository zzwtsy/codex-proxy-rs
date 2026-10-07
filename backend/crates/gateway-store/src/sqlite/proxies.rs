//! SQLite 受管代理目录、账号绑定和探测结果适配。

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use chrono::Utc;
use gateway_admin::{
    model::{
        MutationContext, Revision as AdminRevision,
        account_groups::{AccountGroupColor, AccountGroupRef},
        audit::MutationAuditOperation,
        proxies::*,
    },
    ports::{
        proxy::{ProxyImportGuard, ProxyImportReservation, ProxyStore},
        store::{AdminStoreError, AdminStoreResult},
    },
};
use gateway_core::{
    account::{OutboundProxy, ProviderAccountId, RequestLocation},
    routing::AccountGroupId,
};
use sqlx::{Row, SqlitePool};

use crate::{
    ConflictKind, Revision, StoreError, StoreResult, admin_revision, admin_store_error,
    mutation_audit,
};

use super::{
    acquire_write_lock, append_admin_audit_event_in_transaction, sqlite_unavailable,
    value::datetime_from_micros,
};

const ENTITY: &str = "outbound proxy";

#[derive(Clone)]
pub struct SqliteProxyRepository {
    pool: SqlitePool,
    active_imports: Arc<Mutex<std::collections::HashSet<String>>>,
}

struct SqliteProxyImportGuard {
    id: String,
    active_imports: Arc<Mutex<std::collections::HashSet<String>>>,
}

impl ProxyImportGuard for SqliteProxyImportGuard {}

impl Drop for SqliteProxyImportGuard {
    fn drop(&mut self) {
        self.active_imports
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.id);
    }
}

impl SqliteProxyRepository {
    #[must_use]
    pub fn new(pool: SqlitePool) -> Self {
        Self {
            pool,
            active_imports: Arc::new(Mutex::new(std::collections::HashSet::new())),
        }
    }

    async fn get_record(&self, id: &str) -> AdminStoreResult<ProxyRecord> {
        let row = sqlx::query(
            "select p.*,
                    (select count(*) from provider_accounts a where a.outbound_proxy_id = p.id)
                      as account_count
             from outbound_proxies p where p.id = ?1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| unavailable_admin("read proxy"))?
        .ok_or_else(|| admin_store_error(ENTITY, not_found(id)))?;
        proxy_record(row).map_err(store_error)
    }

    async fn record_in_transaction(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        id: &str,
    ) -> StoreResult<ProxyRecord> {
        let row = sqlx::query(
            "select p.*,
                    (select count(*) from provider_accounts a where a.outbound_proxy_id = p.id)
                      as account_count
             from outbound_proxies p where p.id = ?1",
        )
        .bind(id)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|_| unavailable("read proxy in transaction"))?
        .ok_or_else(|| conflict(id))?;
        proxy_record(row)
    }

    async fn save_test(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        current: &ProxyRecord,
        result: ProxyTestResult,
    ) -> StoreResult<()> {
        let detected = current.detected_location_after_test(&result);
        if let Some(location) = &detected {
            location.location.validate().map_err(|_| invalid())?;
        }
        let encoded_detection = serde_json::to_string(&result.location).map_err(|_| invalid())?;
        let encoded_detected = detected
            .map(|value| serde_json::to_string(&value).map_err(|_| invalid()))
            .transpose()?;
        sqlx::query(
            "update outbound_proxies set last_test_at_us = ?2, last_test_success = ?3,
               last_test_latency_ms = ?4, last_test_ip = ?5, last_test_ipv4 = ?6,
               last_test_ipv6 = ?7, last_test_message = ?8,
               last_location_detection_json = ?9, detected_location_json = ?10
             where id = ?1",
        )
        .bind(&current.id)
        .bind(Utc::now().timestamp_micros())
        .bind(i64::from(result.success))
        .bind(i64::try_from(result.latency_ms).map_err(|_| invalid())?)
        .bind(result.exit_ip.map(|ip| ip.to_string()))
        .bind(result.exit_ipv4.map(|ip| ip.to_string()))
        .bind(result.exit_ipv6.map(|ip| ip.to_string()))
        .bind(result.message)
        .bind(encoded_detection)
        .bind(encoded_detected)
        .execute(&mut **transaction)
        .await
        .map_err(|_| unavailable("save proxy test result"))?;
        Ok(())
    }

    async fn save_location(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        id: &str,
        location: Option<&RequestLocation>,
    ) -> StoreResult<()> {
        let normalized = location
            .cloned()
            .map(RequestLocation::normalized)
            .transpose()
            .map_err(|_| invalid())?;
        sqlx::query(
            "update outbound_proxies set location_country = ?2, location_region = ?3,
               location_city = ?4, location_timezone = ?5 where id = ?1",
        )
        .bind(id)
        .bind(normalized.as_ref().map(|value| value.country.as_str()))
        .bind(normalized.as_ref().map(|value| value.region.as_str()))
        .bind(normalized.as_ref().map(|value| value.city.as_str()))
        .bind(normalized.as_ref().map(|value| value.timezone.name()))
        .execute(&mut **transaction)
        .await
        .map_err(|_| unavailable("save proxy location"))?;
        Ok(())
    }

    async fn audit(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        context: &MutationContext,
        operation: MutationAuditOperation,
        id: &str,
        changed_fields: &[&str],
        revision: Revision,
    ) -> StoreResult<()> {
        let mut audit = mutation_audit(
            context,
            operation,
            id,
            changed_fields
                .iter()
                .map(|field| (*field).to_owned())
                .collect(),
        );
        audit.config_revision = Some(i64::try_from(revision.get()).map_err(|_| invalid())?);
        append_admin_audit_event_in_transaction(transaction, audit).await
    }

    async fn bump_revision(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    ) -> StoreResult<Revision> {
        super::bump_config_revision(transaction, Utc::now().timestamp_micros()).await
    }
}

#[async_trait]
impl ProxyStore for SqliteProxyRepository {
    async fn reserve_import(&self, id: &str) -> AdminStoreResult<ProxyImportReservation> {
        {
            let mut active = self
                .active_imports
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !active.insert(id.to_owned()) {
                return Err(admin_store_error(ENTITY, conflict(id)));
            }
        }
        let record = match self.get_record(id).await {
            Ok(record) => record,
            Err(error) => {
                self.active_imports
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(id);
                return Err(error);
            }
        };
        Ok(ProxyImportReservation {
            binding: ImportProxyBinding {
                id: record.id,
                proxy: record.proxy,
            },
            guard: Box::new(SqliteProxyImportGuard {
                id: id.to_owned(),
                active_imports: Arc::clone(&self.active_imports),
            }),
        })
    }

    async fn list(&self, query: ProxyListQuery) -> AdminStoreResult<ProxyPage> {
        if query.page == 0 {
            return Err(invalid_admin());
        }
        let search = query.search.to_lowercase();
        let total = sqlx::query_scalar::<_, i64>(
            "select count(*) from outbound_proxies where instr(lower(name), ?1) > 0",
        )
        .bind(&search)
        .fetch_one(&self.pool)
        .await
        .map_err(|_| unavailable_admin("count proxies"))?;
        let offset = i64::from(query.page - 1) * i64::from(query.page_size.get());
        let rows = sqlx::query(
            "select p.*,
                    (select count(*) from provider_accounts a where a.outbound_proxy_id = p.id)
                      as account_count
             from outbound_proxies p
             where instr(lower(p.name), ?1) > 0
             order by p.created_at_us desc, p.id
             limit ?2 offset ?3",
        )
        .bind(search)
        .bind(i64::from(query.page_size.get()))
        .bind(offset)
        .fetch_all(&self.pool)
        .await
        .map_err(|_| unavailable_admin("list proxies"))?;
        let items = rows
            .into_iter()
            .map(proxy_record)
            .collect::<StoreResult<Vec<_>>>()
            .map_err(store_error)?;
        Ok(ProxyPage {
            items,
            total: u64::try_from(total).map_err(|_| admin_store_error(ENTITY, invalid()))?,
            page: query.page,
            page_size: query.page_size.get(),
        })
    }

    async fn list_accounts(
        &self,
        query: ProxyAccountListQuery,
    ) -> AdminStoreResult<ProxyAccountPage> {
        if query.page == 0 {
            return Err(invalid_admin());
        }
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|_| unavailable_admin("begin proxy account page"))?;
        let search = query.search.to_lowercase();
        let total = sqlx::query_scalar::<_, i64>(
            "select count(a.id)
             from outbound_proxies p
             left join provider_accounts a on a.outbound_proxy_id = p.id
               and (instr(lower(a.name), ?2) > 0 or instr(lower(coalesce(a.email, '')), ?2) > 0)
             where p.id = ?1
             group by p.id",
        )
        .bind(&query.proxy_id)
        .bind(&search)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| unavailable_admin("count proxy accounts"))?
        .ok_or_else(|| admin_store_error(ENTITY, not_found(&query.proxy_id)))?;
        let offset = i64::from(query.page - 1) * i64::from(query.page_size.get());
        let rows = sqlx::query(
            "select id, name, email, provider_kind, authentication_kind, plan_type, enabled
             from provider_accounts
             where outbound_proxy_id = ?1
               and (instr(lower(name), ?2) > 0
                 or instr(lower(coalesce(email, '')), ?2) > 0)
             order by name, id limit ?3 offset ?4",
        )
        .bind(&query.proxy_id)
        .bind(search)
        .bind(i64::from(query.page_size.get()))
        .bind(offset)
        .fetch_all(&mut *transaction)
        .await
        .map_err(|_| unavailable_admin("list proxy accounts"))?;
        let mut items = rows
            .into_iter()
            .map(|row| {
                Ok(ProxyAccountRef {
                    id: text(&row, "id")?,
                    name: text(&row, "name")?,
                    email: optional_text(&row, "email")?,
                    provider_kind: text(&row, "provider_kind")?,
                    authentication_kind: text(&row, "authentication_kind")?,
                    plan_type: optional_text(&row, "plan_type")?,
                    plan_type_display: None,
                    groups: Vec::new(),
                    enabled: integer(&row, "enabled")? != 0,
                })
            })
            .collect::<StoreResult<Vec<_>>>()
            .map_err(store_error)?;
        if !items.is_empty() {
            let account_ids = serde_json::to_string(
                &items
                    .iter()
                    .map(|item| item.id.as_str())
                    .collect::<Vec<_>>(),
            )
            .map_err(|_| store_error(invalid()))?;
            let rows = sqlx::query(
                "select membership.provider_account_id, g.id, g.name, g.color, g.enabled
                 from account_group_accounts membership
                 join account_groups g on g.id = membership.account_group_id
                 where membership.provider_account_id in
                   (select value from json_each(?1))
                 order by membership.provider_account_id, g.id",
            )
            .bind(account_ids)
            .fetch_all(&mut *transaction)
            .await
            .map_err(|_| unavailable_admin("load proxy account groups"))?;
            let mut groups = BTreeMap::<String, Vec<AccountGroupRef>>::new();
            for row in rows {
                let account_id = text(&row, "provider_account_id").map_err(store_error)?;
                groups.entry(account_id).or_default().push(AccountGroupRef {
                    id: AccountGroupId::new(text(&row, "id").map_err(store_error)?)
                        .map_err(|_| invalid_admin())?,
                    name: text(&row, "name").map_err(store_error)?,
                    color: AccountGroupColor::parse(&text(&row, "color").map_err(store_error)?)
                        .ok_or_else(invalid_admin)?,
                    enabled: integer(&row, "enabled").map_err(store_error)? != 0,
                });
            }
            for item in &mut items {
                item.groups = groups.remove(&item.id).unwrap_or_default();
            }
        }
        transaction
            .commit()
            .await
            .map_err(|_| unavailable_admin("commit proxy account page"))?;
        Ok(ProxyAccountPage {
            items,
            total: u64::try_from(total).map_err(|_| invalid_admin())?,
            page: query.page,
            page_size: query.page_size.get(),
        })
    }

    async fn get(&self, id: &str) -> AdminStoreResult<ProxyRecord> {
        self.get_record(id).await
    }

    async fn remove_account(
        &self,
        proxy_id: &str,
        account_id: &ProviderAccountId,
        context: &MutationContext,
    ) -> AdminStoreResult<AdminRevision> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|_| unavailable_admin("begin proxy account removal"))?;
        acquire_write_lock(&mut transaction)
            .await
            .map_err(store_error)?;
        let changed = sqlx::query(
            "update provider_accounts set outbound_proxy_id = null, outbound_proxy_url = null,
               updated_at_us = max(updated_at_us, ?3)
             where id = ?1 and outbound_proxy_id = ?2",
        )
        .bind(account_id.as_str())
        .bind(proxy_id)
        .bind(Utc::now().timestamp_micros())
        .execute(&mut *transaction)
        .await
        .map_err(|_| unavailable_admin("remove account proxy"))?
        .rows_affected();
        if changed != 1 {
            return Err(admin_store_error(ENTITY, conflict(proxy_id)));
        }
        let revision = self
            .bump_revision(&mut transaction)
            .await
            .map_err(store_error)?;
        let audit = mutation_audit(
            context,
            MutationAuditOperation::ProviderAccountUpdate,
            account_id.as_str(),
            vec!["outbound_proxy".to_owned()],
        );
        let mut audit = audit;
        audit.config_revision = Some(i64::try_from(revision.get()).map_err(|_| invalid_admin())?);
        append_admin_audit_event_in_transaction(&mut transaction, audit)
            .await
            .map_err(store_error)?;
        transaction
            .commit()
            .await
            .map_err(|_| unavailable_admin("commit proxy account removal"))?;
        admin_revision(revision)
    }

    async fn create(
        &self,
        command: NewProxy,
        context: &MutationContext,
    ) -> AdminStoreResult<ProxyMutation> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|_| unavailable_admin("begin proxy create"))?;
        acquire_write_lock(&mut transaction)
            .await
            .map_err(store_error)?;
        let duplicate = sqlx::query_scalar::<_, i64>(
            "select exists(select 1 from outbound_proxies where proxy_url = ?1)",
        )
        .bind(command.proxy.expose_url())
        .fetch_one(&mut *transaction)
        .await
        .map_err(|_| unavailable_admin("check duplicate proxy URL"))?;
        if duplicate != 0 {
            return Err(admin_store_error(
                ENTITY,
                conflict("proxy URL already exists"),
            ));
        }
        let id = format!("proxy_{}", uuid::Uuid::now_v7().simple());
        let now = Utc::now().timestamp_micros();
        sqlx::query(
            "insert into outbound_proxies (id, name, proxy_url, auto_location, created_at_us, updated_at_us)
             values (?1, ?2, ?3, ?4, ?5, ?5)",
        )
        .bind(&id)
        .bind(&command.name)
        .bind(command.proxy.expose_url())
        .bind(i64::from(command.auto_location))
        .bind(now)
        .execute(&mut *transaction)
        .await
        .map_err(|_| unavailable_admin("create proxy"))?;
        self.save_location(&mut transaction, &id, command.location.as_ref())
            .await
            .map_err(store_error)?;
        if let Some(test) = command.test {
            let current = self
                .record_in_transaction(&mut transaction, &id)
                .await
                .map_err(store_error)?;
            self.save_test(&mut transaction, &current, test)
                .await
                .map_err(store_error)?;
        }
        let revision = self
            .bump_revision(&mut transaction)
            .await
            .map_err(store_error)?;
        self.audit(
            &mut transaction,
            context,
            MutationAuditOperation::OutboundProxyCreate,
            &id,
            &["name", "proxy_url", "location", "auto_location"],
            revision,
        )
        .await
        .map_err(store_error)?;
        transaction
            .commit()
            .await
            .map_err(|_| unavailable_admin("commit proxy create"))?;
        Ok(ProxyMutation {
            config_revision: admin_revision(revision)?,
            record: self.get_record(&id).await?,
        })
    }

    async fn update(
        &self,
        command: UpdateProxy,
        context: &MutationContext,
    ) -> AdminStoreResult<ProxyMutation> {
        if self
            .active_imports
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(&command.id)
        {
            return Err(admin_store_error(ENTITY, conflict(&command.id)));
        }
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|_| unavailable_admin("begin proxy update"))?;
        acquire_write_lock(&mut transaction)
            .await
            .map_err(store_error)?;
        let current = self
            .record_in_transaction(&mut transaction, &command.id)
            .await
            .map_err(store_error)?;
        if current.revision.get() != command.revision.get() {
            return Err(admin_store_error(ENTITY, conflict(&command.id)));
        }
        if let Some(proxy) = &command.proxy {
            let duplicate = sqlx::query_scalar::<_, i64>(
                "select exists(select 1 from outbound_proxies where proxy_url = ?1 and id <> ?2)",
            )
            .bind(proxy.expose_url())
            .bind(&command.id)
            .fetch_one(&mut *transaction)
            .await
            .map_err(|_| unavailable_admin("check duplicate proxy URL"))?;
            if duplicate != 0 {
                return Err(admin_store_error(ENTITY, conflict(&command.id)));
            }
        }
        let url_changed = command
            .proxy
            .as_ref()
            .is_some_and(|proxy| proxy.expose_url() != current.proxy.expose_url());
        let auto_changed = command
            .auto_location
            .is_some_and(|value| value != current.auto_location);
        let now = Utc::now().timestamp_micros();
        let changed = sqlx::query(
            "update outbound_proxies set name = ?2, proxy_url = coalesce(?3, proxy_url),
               auto_location = coalesce(?4, auto_location), revision = revision + 1,
               updated_at_us = max(updated_at_us, ?5),
               detected_location_json = case when ?6 then null else detected_location_json end,
               last_location_detection_json = case when ?6 then '{\"status\":\"notRequested\"}'
                 else last_location_detection_json end,
               last_test_at_us = case when ?7 then null else last_test_at_us end,
               last_test_success = case when ?7 then null else last_test_success end,
               last_test_latency_ms = case when ?7 then null else last_test_latency_ms end,
               last_test_ip = case when ?7 then null else last_test_ip end,
               last_test_ipv4 = case when ?7 then null else last_test_ipv4 end,
               last_test_ipv6 = case when ?7 then null else last_test_ipv6 end,
               last_test_message = case when ?7 then null else last_test_message end
             where id = ?1 and revision = ?8",
        )
        .bind(&command.id)
        .bind(&command.name)
        .bind(command.proxy.as_ref().map(OutboundProxy::expose_url))
        .bind(command.auto_location.map(i64::from))
        .bind(now)
        .bind(i64::from(url_changed || auto_changed))
        .bind(i64::from(url_changed))
        .bind(i64::try_from(command.revision.get()).map_err(|_| invalid_admin())?)
        .execute(&mut *transaction)
        .await
        .map_err(|_| unavailable_admin("update proxy"))?
        .rows_affected();
        if changed != 1 {
            return Err(admin_store_error(ENTITY, conflict(&command.id)));
        }
        if let Some(location) = &command.location {
            self.save_location(&mut transaction, &command.id, location.as_ref())
                .await
                .map_err(store_error)?;
        }
        if let Some(proxy) = &command.proxy {
            sqlx::query(
                "update provider_accounts set outbound_proxy_url = ?2,
                   updated_at_us = max(updated_at_us, ?3)
                 where outbound_proxy_id = ?1 and outbound_proxy_url is not ?2",
            )
            .bind(&command.id)
            .bind(proxy.expose_url())
            .bind(now)
            .execute(&mut *transaction)
            .await
            .map_err(|_| unavailable_admin("update linked account proxy URLs"))?;
        }
        if let Some(test) = command.test {
            let current = self
                .record_in_transaction(&mut transaction, &command.id)
                .await
                .map_err(store_error)?;
            self.save_test(&mut transaction, &current, test)
                .await
                .map_err(store_error)?;
        }
        let revision = self
            .bump_revision(&mut transaction)
            .await
            .map_err(store_error)?;
        self.audit(
            &mut transaction,
            context,
            MutationAuditOperation::OutboundProxyUpdate,
            &command.id,
            &["name", "proxy_url", "location", "auto_location"],
            revision,
        )
        .await
        .map_err(store_error)?;
        transaction
            .commit()
            .await
            .map_err(|_| unavailable_admin("commit proxy update"))?;
        let record = self.get_record(&command.id).await?;
        Ok(ProxyMutation {
            config_revision: admin_revision(revision)?,
            record,
        })
    }

    async fn delete(
        &self,
        id: &str,
        expected_revision: AdminRevision,
        context: &MutationContext,
    ) -> AdminStoreResult<AdminRevision> {
        if self
            .active_imports
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(id)
        {
            return Err(admin_store_error(ENTITY, conflict(id)));
        }
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|_| unavailable_admin("begin proxy delete"))?;
        acquire_write_lock(&mut transaction)
            .await
            .map_err(store_error)?;
        let deleted = sqlx::query("delete from outbound_proxies where id = ?1 and revision = ?2")
            .bind(id)
            .bind(i64::try_from(expected_revision.get()).map_err(|_| invalid_admin())?)
            .execute(&mut *transaction)
            .await;
        let rows = match deleted {
            Ok(result) => result.rows_affected(),
            Err(error)
                if error
                    .as_database_error()
                    .is_some_and(|db| db.is_foreign_key_violation()) =>
            {
                return Err(admin_store_error(ENTITY, conflict(id)));
            }
            Err(_) => return Err(unavailable_admin("delete proxy")),
        };
        if rows != 1 {
            return Err(admin_store_error(ENTITY, conflict(id)));
        }
        let revision = self
            .bump_revision(&mut transaction)
            .await
            .map_err(store_error)?;
        self.audit(
            &mut transaction,
            context,
            MutationAuditOperation::OutboundProxyDelete,
            id,
            &[],
            revision,
        )
        .await
        .map_err(store_error)?;
        transaction
            .commit()
            .await
            .map_err(|_| unavailable_admin("commit proxy delete"))?;
        admin_revision(revision)
    }

    async fn record_test(
        &self,
        id: &str,
        expected_revision: AdminRevision,
        result: ProxyTestResult,
        context: &MutationContext,
    ) -> AdminStoreResult<ProxyMutation> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|_| unavailable_admin("begin proxy test update"))?;
        acquire_write_lock(&mut transaction)
            .await
            .map_err(store_error)?;
        let current = self
            .record_in_transaction(&mut transaction, id)
            .await
            .map_err(store_error)?;
        if current.revision.get() != expected_revision.get() {
            return Err(admin_store_error(ENTITY, conflict(id)));
        }
        let current_revision = sqlx::query_scalar::<_, i64>(
            "select config_revision from runtime_settings where id = 1",
        )
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| unavailable_admin("read config revision"))?
        .ok_or_else(|| not_found_admin("runtime settings", "1"))?;
        self.save_test(&mut transaction, &current, result)
            .await
            .map_err(store_error)?;
        if current.auto_location {
            sqlx::query(
                "update outbound_proxies set revision = revision + 1,
                   updated_at_us = max(updated_at_us, ?2) where id = ?1",
            )
            .bind(id)
            .bind(Utc::now().timestamp_micros())
            .execute(&mut *transaction)
            .await
            .map_err(|_| unavailable_admin("advance tested proxy revision"))?;
        }
        let updated = self
            .record_in_transaction(&mut transaction, id)
            .await
            .map_err(store_error)?;
        let revision = if current.effective_location() != updated.effective_location() {
            self.bump_revision(&mut transaction)
                .await
                .map_err(store_error)?
        } else {
            Revision::new(u64::try_from(current_revision).map_err(|_| invalid_admin())?)
                .map_err(store_error)?
        };
        self.audit(
            &mut transaction,
            context,
            MutationAuditOperation::OutboundProxyTest,
            id,
            &["last_test", "detected_location"],
            revision,
        )
        .await
        .map_err(store_error)?;
        transaction
            .commit()
            .await
            .map_err(|_| unavailable_admin("commit proxy test update"))?;
        Ok(ProxyMutation {
            config_revision: admin_revision(revision)?,
            record: updated,
        })
    }
}

fn proxy_record(row: sqlx::sqlite::SqliteRow) -> StoreResult<ProxyRecord> {
    let success = optional_integer(&row, "last_test_success")?.map(|value| value != 0);
    let encoded_detected: Option<String> = row
        .try_get("detected_location_json")
        .map_err(|_| invalid())?;
    let detected_location: Option<DetectedProxyLocation> = encoded_detected
        .as_deref()
        .map(serde_json::from_str)
        .transpose()
        .map_err(|_| invalid())?;
    if let Some(value) = &detected_location {
        value.location.validate().map_err(|_| invalid())?;
    }
    let detection: ProxyLocationDetection =
        serde_json::from_str(&text(&row, "last_location_detection_json")?)
            .map_err(|_| invalid())?;
    let location_country = optional_text(&row, "location_country")?;
    let location = location_country
        .map(|country| {
            let location = RequestLocation {
                country,
                region: text(&row, "location_region")?,
                city: text(&row, "location_city")?,
                timezone: text(&row, "location_timezone")?
                    .parse()
                    .map_err(|_| invalid())?,
            };
            location.normalized().map_err(|_| invalid())
        })
        .transpose()?;
    let last_test = success
        .map(|success| {
            let latency =
                u64::try_from(integer(&row, "last_test_latency_ms")?).map_err(|_| invalid())?;
            Ok(ProxyTestResult {
                location: detection,
                success,
                latency_ms: latency,
                exit_ip: optional_text(&row, "last_test_ip")?
                    .map(|value| value.parse().map_err(|_| invalid()))
                    .transpose()?,
                exit_ipv4: optional_text(&row, "last_test_ipv4")?
                    .map(|value| value.parse().map_err(|_| invalid()))
                    .transpose()?,
                exit_ipv6: optional_text(&row, "last_test_ipv6")?
                    .map(|value| value.parse().map_err(|_| invalid()))
                    .transpose()?,
                message: optional_text(&row, "last_test_message")?.unwrap_or_default(),
            })
        })
        .transpose()?;
    Ok(ProxyRecord {
        auto_location: integer(&row, "auto_location")? != 0,
        detected_location,
        location,
        id: text(&row, "id")?,
        name: text(&row, "name")?,
        proxy: OutboundProxy::parse(&text(&row, "proxy_url")?).map_err(|_| invalid())?,
        revision: AdminRevision::new(
            u64::try_from(integer(&row, "revision")?).map_err(|_| invalid())?,
        )
        .map_err(|_| invalid())?,
        account_count: u64::try_from(integer(&row, "account_count")?).map_err(|_| invalid())?,
        last_test_at: optional_integer(&row, "last_test_at_us")?
            .map(datetime_from_micros)
            .transpose()?,
        last_test,
        created_at: datetime_from_micros(integer(&row, "created_at_us")?)?,
        updated_at: datetime_from_micros(integer(&row, "updated_at_us")?)?,
    })
}

fn text(row: &sqlx::sqlite::SqliteRow, name: &'static str) -> StoreResult<String> {
    row.try_get(name).map_err(|_| invalid())
}

fn optional_text(row: &sqlx::sqlite::SqliteRow, name: &'static str) -> StoreResult<Option<String>> {
    row.try_get(name).map_err(|_| invalid())
}

fn integer(row: &sqlx::sqlite::SqliteRow, name: &'static str) -> StoreResult<i64> {
    row.try_get(name).map_err(|_| invalid())
}

fn optional_integer(row: &sqlx::sqlite::SqliteRow, name: &'static str) -> StoreResult<Option<i64>> {
    row.try_get(name).map_err(|_| invalid())
}

fn invalid() -> StoreError {
    StoreError::InvalidData {
        entity: ENTITY,
        message: "invalid proxy record".to_owned(),
        source: None,
    }
}

fn unavailable(message: &'static str) -> StoreError {
    sqlite_unavailable(message)
}

fn conflict(id: &str) -> StoreError {
    StoreError::Conflict {
        entity: ENTITY,
        id: id.to_owned(),
        kind: ConflictKind::InvalidTransition,
        source: None,
    }
}

fn not_found(id: &str) -> StoreError {
    StoreError::NotFound {
        entity: ENTITY,
        id: id.to_owned(),
        source: None,
    }
}

fn store_error(error: StoreError) -> AdminStoreError {
    admin_store_error(ENTITY, error)
}

fn invalid_admin() -> AdminStoreError {
    store_error(invalid())
}

fn unavailable_admin(operation: &'static str) -> AdminStoreError {
    store_error(unavailable(operation))
}

fn not_found_admin(entity: &'static str, id: &str) -> AdminStoreError {
    admin_store_error(
        entity,
        StoreError::NotFound {
            entity,
            id: id.to_owned(),
            source: None,
        },
    )
}
