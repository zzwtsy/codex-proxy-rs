//! 将 Redis cooldown 与凭据租约信号组合为 Admin 账号运行态端口。

use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::future::join_all;
use gateway_admin::{
    model::accounts::AccountRuntimeSnapshot,
    ports::store::{AccountRuntimeStore, AdminStoreResult},
};

use crate::{
    AccountRuntimeSignalRepository, AccountRuntimeStateRepository, AccountRuntimeStoreAdapter,
    StoreResult,
};

use super::{
    CredentialCooldownRepository as _, RedisCredentialCooldownRepository,
    RedisCredentialLeaseRepository,
};

/// 只组合可丢失 Redis 事实；不持有 PostgreSQL 或执行状态投影。
#[derive(Clone)]
pub struct RedisAdminAccountRuntimeStore {
    inner: AccountRuntimeStoreAdapter,
}

impl RedisAdminAccountRuntimeStore {
    #[must_use]
    pub fn new(
        cooldowns: RedisCredentialCooldownRepository,
        leases: RedisCredentialLeaseRepository,
    ) -> Self {
        let state: Arc<dyn AccountRuntimeStateRepository> = Arc::new(cooldowns);
        let leases: Arc<dyn AccountRuntimeSignalRepository> = Arc::new(leases);
        Self {
            inner: AccountRuntimeStoreAdapter::new(state, leases),
        }
    }
}

#[async_trait]
impl AccountRuntimeSignalRepository for RedisCredentialLeaseRepository {
    async fn credential_runtime_signals(
        &self,
        account_ids: &[String],
    ) -> StoreResult<Vec<(String, u32)>> {
        super::CredentialLeaseRepository::credential_runtime_signals(self, account_ids)
            .await
            .map(|signals| {
                signals
                    .into_iter()
                    .map(|signal| (signal.resource_id, signal.in_flight))
                    .collect()
            })
    }
}

#[async_trait]
impl AccountRuntimeStore for RedisAdminAccountRuntimeStore {
    async fn active_rate_limits(&self) -> AdminStoreResult<AccountRuntimeSnapshot> {
        self.inner.active_rate_limits().await
    }

    async fn account_runtime(
        &self,
        account_ids: &[String],
    ) -> AdminStoreResult<AccountRuntimeSnapshot> {
        self.inner.account_runtime(account_ids).await
    }

    async fn active_freezes(
        &self,
    ) -> AdminStoreResult<BTreeMap<String, gateway_admin::model::accounts::AccountFreeze>> {
        self.inner.active_freezes().await
    }

    async fn capacity_peaks(
        &self,
        account_ids: &[String],
    ) -> AdminStoreResult<BTreeMap<String, u32>> {
        self.inner.capacity_peaks(account_ids).await
    }

    async fn finish_freeze(
        &self,
        account_id: &str,
        expected: &gateway_admin::model::accounts::AccountFreeze,
        postpone_until: Option<DateTime<Utc>>,
    ) -> AdminStoreResult<bool> {
        self.inner
            .finish_freeze(account_id, expected, postpone_until)
            .await
    }
}

#[async_trait]
impl AccountRuntimeStateRepository for RedisCredentialCooldownRepository {
    async fn active_rate_limits(&self) -> StoreResult<AccountRuntimeSnapshot> {
        self.active_cooldowns().await
    }

    async fn account_cooldowns(
        &self,
        account_ids: &[String],
    ) -> StoreResult<BTreeMap<String, gateway_core::account::AccountCooldown>> {
        let reads = account_ids.iter().map(|account_id| async move {
            self.read_credential_cooldown(account_id)
                .await
                .map(|cooldown| {
                    cooldown.map(|cooldown| {
                        (
                            account_id.clone(),
                            gateway_core::account::AccountCooldown {
                                until: cooldown.cooldown_until.into(),
                                kind: cooldown.kind,
                            },
                        )
                    })
                })
        });
        let mut cooldowns = BTreeMap::new();
        for result in join_all(reads).await {
            if let Some((account_id, cooldown)) = result? {
                cooldowns.insert(account_id, cooldown);
            }
        }
        Ok(cooldowns)
    }

    async fn active_freezes(
        &self,
    ) -> StoreResult<BTreeMap<String, gateway_admin::model::accounts::AccountFreeze>> {
        RedisCredentialCooldownRepository::active_freezes(self).await
    }

    async fn capacity_peaks(&self, account_ids: &[String]) -> StoreResult<BTreeMap<String, u32>> {
        let reads = account_ids.iter().map(|account_id| async move {
            self.read_capacity_peak(account_id)
                .await
                .map(|peak| peak.map(|value| (account_id.clone(), value)))
        });
        let mut peaks = BTreeMap::new();
        for result in join_all(reads).await {
            if let Some((account_id, peak)) = result? {
                peaks.insert(account_id, peak);
            }
        }
        Ok(peaks)
    }

    async fn finish_freeze(
        &self,
        account_id: &str,
        expected: &gateway_admin::model::accounts::AccountFreeze,
        postpone_until: Option<DateTime<Utc>>,
    ) -> StoreResult<bool> {
        RedisCredentialCooldownRepository::finish_freeze(self, account_id, expected, postpone_until)
            .await
    }
}
