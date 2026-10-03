//! SQLite 模式的进程内 Provider 缓存与跨进程刷新退避。

use std::{
    collections::HashMap,
    time::{Duration, Instant, SystemTime},
};

use gateway_core::{
    account::{OpaqueProviderData, ProviderAccountId},
    provider_ports::{
        ProviderArtifactProfile, ProviderArtifactProfileCachePort, ProviderCatalogCacheKey,
        ProviderCatalogCachePort, ProviderCredentialState, ProviderCredentialStatePort,
        ProviderStoreError, ProviderStoreErrorKind,
    },
};
use sqlx::SqlitePool;
use tokio::sync::Mutex;

use super::value::datetime_to_micros;

const CREDENTIAL_STATE_TTL: Duration = Duration::from_secs(24 * 60 * 60);
const MAX_CATALOG_BYTES: usize = 1024 * 1024;
const MAX_ARTIFACT_PROFILE_BYTES: usize = 16 * 1024;
const MAX_REDIS_EXACT_INTEGER: u64 = (1_u64 << 53) - 1;
const MAX_ARTIFACT_KEY_BYTES: usize = 128;
const MAX_REFRESH_FAILURE_COUNT: i64 = u32::MAX as i64;

#[derive(Clone)]
pub struct SqliteProviderRuntimeCache {
    pool: SqlitePool,
    credential_states: std::sync::Arc<Mutex<HashMap<String, Expiring<ProviderCredentialState>>>>,
    catalogs: std::sync::Arc<Mutex<HashMap<String, Expiring<OpaqueProviderData>>>>,
    artifact_profiles: std::sync::Arc<Mutex<HashMap<String, Expiring<ProviderArtifactProfile>>>>,
}

struct Expiring<T> {
    value: T,
    expires_at: Instant,
}

impl SqliteProviderRuntimeCache {
    #[must_use]
    pub fn new(pool: SqlitePool) -> Self {
        Self {
            pool,
            credential_states: std::sync::Arc::default(),
            catalogs: std::sync::Arc::default(),
            artifact_profiles: std::sync::Arc::default(),
        }
    }
}

impl ProviderCredentialStatePort for SqliteProviderRuntimeCache {
    fn replace(
        &self,
        state: ProviderCredentialState,
    ) -> futures::future::BoxFuture<'_, Result<(), ProviderStoreError>> {
        Box::pin(async move {
            let key = state.account_id().as_str().to_owned();
            let expires_at = Instant::now()
                .checked_add(CREDENTIAL_STATE_TTL)
                .ok_or_else(|| invalid("replace credential state"))?;
            let now = Instant::now();
            let mut cache = self.credential_states.lock().await;
            cache.retain(|_, entry| entry.expires_at > now);
            if cache.get(&key).is_some_and(|current| {
                current.value.credential_revision() >= state.credential_revision()
            }) {
                return Ok(());
            }
            cache.insert(
                key,
                Expiring {
                    value: state,
                    expires_at,
                },
            );
            Ok(())
        })
    }

    fn read<'a>(
        &'a self,
        account_id: &'a ProviderAccountId,
    ) -> futures::future::BoxFuture<'a, Result<Option<ProviderCredentialState>, ProviderStoreError>>
    {
        Box::pin(async move {
            let key = account_id.as_str();
            let now = Instant::now();
            let mut cache = self.credential_states.lock().await;
            cache.retain(|_, entry| entry.expires_at > now);
            Ok(cache.get(key).map(|entry| entry.value.clone()))
        })
    }

    fn clear<'a>(
        &'a self,
        account_id: &'a ProviderAccountId,
    ) -> futures::future::BoxFuture<'a, Result<bool, ProviderStoreError>> {
        Box::pin(async move {
            Ok(self
                .credential_states
                .lock()
                .await
                .remove(account_id.as_str())
                .is_some())
        })
    }

    fn record_refresh_backoff<'a>(
        &'a self,
        account_id: &'a ProviderAccountId,
        window: Duration,
    ) -> futures::future::BoxFuture<'a, Result<u32, ProviderStoreError>> {
        Box::pin(async move {
            let window_micros = i64::try_from(window.as_micros())
                .ok()
                .filter(|micros| *micros > 0)
                .ok_or_else(|| invalid("record refresh backoff"))?;
            let mut transaction = self
                .pool
                .begin()
                .await
                .map_err(|_| unavailable("record refresh backoff"))?;
            super::acquire_write_lock(&mut transaction)
                .await
                .map_err(|_| unavailable("acquire SQLite refresh backoff lock"))?;
            let now = datetime_to_micros(chrono::Utc::now());
            let retry_at = now
                .checked_add(window_micros)
                .ok_or_else(|| invalid("record refresh backoff"))?;
            let current = sqlx::query_as::<_, (i64, i64)>(
                "SELECT failure_count, retry_at_us FROM credential_refresh_backoff WHERE account_id = ?",
            )
            .bind(account_id.as_str())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|_| unavailable("read refresh backoff"))?;
            let failure_count = match current {
                Some((count, current_retry_at)) if current_retry_at > now => count
                    .checked_add(1)
                    .unwrap_or(MAX_REFRESH_FAILURE_COUNT)
                    .min(MAX_REFRESH_FAILURE_COUNT),
                _ => 1,
            };
            sqlx::query(concat!(
                "INSERT INTO credential_refresh_backoff ",
                "(account_id, retry_at_us, failure_count, updated_at_us) ",
                "VALUES (?, ?, ?, ?) ",
                "ON CONFLICT (account_id) DO UPDATE SET ",
                "retry_at_us = excluded.retry_at_us, ",
                "failure_count = excluded.failure_count, ",
                "updated_at_us = excluded.updated_at_us",
            ))
            .bind(account_id.as_str())
            .bind(retry_at)
            .bind(failure_count)
            .bind(now)
            .execute(&mut *transaction)
            .await
            .map_err(|_| unavailable("write refresh backoff"))?;
            transaction
                .commit()
                .await
                .map_err(|_| unavailable("commit refresh backoff"))?;
            Ok(u32::try_from(failure_count).unwrap_or(u32::MAX))
        })
    }

    fn clear_refresh_backoff<'a>(
        &'a self,
        account_id: &'a ProviderAccountId,
    ) -> futures::future::BoxFuture<'a, Result<(), ProviderStoreError>> {
        Box::pin(async move {
            sqlx::query("DELETE FROM credential_refresh_backoff WHERE account_id = ?")
                .bind(account_id.as_str())
                .execute(&self.pool)
                .await
                .map_err(|_| unavailable("clear refresh backoff"))?;
            Ok(())
        })
    }
}

impl ProviderCatalogCachePort for SqliteProviderRuntimeCache {
    fn replace<'a>(
        &'a self,
        key: &'a ProviderCatalogCacheKey,
        catalog: &'a OpaqueProviderData,
        ttl: Duration,
    ) -> futures::future::BoxFuture<'a, Result<(), ProviderStoreError>> {
        Box::pin(async move {
            if ttl.is_zero() || ttl.subsec_nanos() != 0 {
                return Err(invalid("validate catalog cache TTL"));
            }
            let encoded = serde_json::to_vec(catalog.expose_to_provider())
                .map_err(|_| invalid("encode provider catalog"))?;
            if encoded.len() > MAX_CATALOG_BYTES {
                return Err(invalid("validate provider catalog size"));
            }
            let expires_at = Instant::now()
                .checked_add(ttl)
                .ok_or_else(|| invalid("validate catalog cache TTL"))?;
            let cache_key = catalog_key(key);
            let now = Instant::now();
            let mut cache = self.catalogs.lock().await;
            cache.retain(|_, entry| entry.expires_at > now);
            cache.insert(
                cache_key,
                Expiring {
                    value: catalog.clone(),
                    expires_at,
                },
            );
            Ok(())
        })
    }

    fn read<'a>(
        &'a self,
        key: &'a ProviderCatalogCacheKey,
    ) -> futures::future::BoxFuture<'a, Result<Option<OpaqueProviderData>, ProviderStoreError>>
    {
        Box::pin(async move {
            let cache_key = catalog_key(key);
            let now = Instant::now();
            let mut cache = self.catalogs.lock().await;
            cache.retain(|_, entry| entry.expires_at > now);
            Ok(cache.get(&cache_key).map(|entry| entry.value.clone()))
        })
    }
}

impl ProviderArtifactProfileCachePort for SqliteProviderRuntimeCache {
    fn replace_if_newer(
        &self,
        profile: ProviderArtifactProfile,
        ttl: Duration,
    ) -> futures::future::BoxFuture<'_, Result<bool, ProviderStoreError>> {
        Box::pin(async move {
            validate_artifact_key(profile.artifact_key())?;
            if profile.artifact_sequence() == 0
                || profile.artifact_sequence() > MAX_REDIS_EXACT_INTEGER
            {
                return Err(invalid("validate artifact profile sequence"));
            }
            let ttl_ms = u64::try_from(ttl.as_millis())
                .ok()
                .filter(|millis| *millis > 0 && *millis <= MAX_REDIS_EXACT_INTEGER)
                .ok_or_else(|| invalid("validate artifact profile TTL"))?;
            let _verified_at_ms = profile
                .verified_at()
                .duration_since(SystemTime::UNIX_EPOCH)
                .ok()
                .and_then(|duration| u64::try_from(duration.as_millis()).ok())
                .filter(|millis| *millis <= MAX_REDIS_EXACT_INTEGER)
                .ok_or_else(|| invalid("validate artifact profile timestamp"))?;
            let encoded = serde_json::to_vec(profile.profile().expose_to_provider())
                .map_err(|_| invalid("encode artifact profile"))?;
            if encoded.len() > MAX_ARTIFACT_PROFILE_BYTES {
                return Err(invalid("validate artifact profile size"));
            }
            let expires_at = Instant::now()
                .checked_add(Duration::from_millis(ttl_ms))
                .ok_or_else(|| invalid("validate artifact profile TTL"))?;
            let key = artifact_profile_key(profile.provider_kind(), profile.artifact_key());
            let now = Instant::now();
            let mut cache = self.artifact_profiles.lock().await;
            cache.retain(|_, entry| entry.expires_at > now);
            if let Some(current) = cache.get(&key) {
                if current.value.artifact_sequence() > profile.artifact_sequence() {
                    return Ok(false);
                }
                if current.value.artifact_sequence() == profile.artifact_sequence()
                    && current.value.profile() != profile.profile()
                {
                    return Err(ProviderStoreError::new(
                        ProviderStoreErrorKind::Conflict,
                        "replace artifact profile",
                    ));
                }
            }
            cache.insert(
                key,
                Expiring {
                    value: profile,
                    expires_at,
                },
            );
            Ok(true)
        })
    }

    fn read<'a>(
        &'a self,
        provider_kind: &'a gateway_core::routing::ProviderKind,
        artifact_key: &'a str,
    ) -> futures::future::BoxFuture<'a, Result<Option<ProviderArtifactProfile>, ProviderStoreError>>
    {
        Box::pin(async move {
            validate_artifact_key(artifact_key)?;
            let key = artifact_profile_key(provider_kind, artifact_key);
            let now = Instant::now();
            let mut cache = self.artifact_profiles.lock().await;
            cache.retain(|_, entry| entry.expires_at > now);
            Ok(cache.get(&key).map(|entry| entry.value.clone()))
        })
    }
}

fn catalog_key(key: &ProviderCatalogCacheKey) -> String {
    format!("{}\0{}", key.provider_kind().as_str(), key.scope().as_str())
}

fn artifact_profile_key(
    provider_kind: &gateway_core::routing::ProviderKind,
    artifact_key: &str,
) -> String {
    format!("{}\0{artifact_key}", provider_kind.as_str())
}

fn validate_artifact_key(key: &str) -> Result<(), ProviderStoreError> {
    if key.is_empty()
        || key.len() > MAX_ARTIFACT_KEY_BYTES
        || !key
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(invalid("validate artifact profile key"));
    }
    Ok(())
}

fn invalid(operation: &'static str) -> ProviderStoreError {
    ProviderStoreError::new(ProviderStoreErrorKind::InvalidData, operation)
}

fn unavailable(operation: &'static str) -> ProviderStoreError {
    ProviderStoreError::new(ProviderStoreErrorKind::Unavailable, operation)
}
