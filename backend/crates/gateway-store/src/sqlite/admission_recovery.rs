//! SQLite 执行账本到进程内准入缓存的崩溃恢复适配。

use std::collections::BTreeMap;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use gateway_core::{
    engine::{
        ModelRequestId,
        admission::{
            ClientAdmissionError, ClientAdmissionRecovery as CoreAdmissionRecovery,
            ClientAdmissionRecoveryPort, RecentAdmissionFact, RunningAdmissionFact,
        },
    },
    policy::ClientApiKeyId,
};
use sqlx::{Row, SqlitePool};

use crate::{
    StoreError, StoreResult,
    admission_recovery::{
        ClientAdmissionRecentRequest, ClientAdmissionRecovery, ClientAdmissionRecoveryRepository,
        ClientAdmissionRunningRequest,
    },
};

use super::{sqlite_unavailable, value::datetime_from_micros};

#[derive(Clone)]
pub struct SqliteClientAdmissionRecoveryRepository {
    pool: SqlitePool,
}

impl SqliteClientAdmissionRecoveryRepository {
    #[must_use]
    pub const fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl ClientAdmissionRecoveryRepository for SqliteClientAdmissionRecoveryRepository {
    async fn load_client_admission_recovery(
        &self,
        window_started_at: DateTime<Utc>,
    ) -> StoreResult<Vec<ClientAdmissionRecovery>> {
        let rows = sqlx::query(
            "select client_api_key_ref, id, started_at_us, deadline_at_us, outcome
             from model_requests
             where started_at_us >= ?1 or outcome = 'running'
             order by client_api_key_ref, started_at_us, id",
        )
        .bind(window_started_at.timestamp_micros())
        .fetch_all(&self.pool)
        .await
        .map_err(|_| sqlite_unavailable("load SQLite client admission recovery"))?;

        let mut recoveries = BTreeMap::<String, ClientAdmissionRecovery>::new();
        for row in rows {
            let client_api_key_ref = read_string(&row, "client_api_key_ref")?;
            let model_request_id = read_string(&row, "id")?;
            let started_at = datetime_from_micros(read_i64(&row, "started_at_us")?)?;
            let deadline_at = datetime_from_micros(read_i64(&row, "deadline_at_us")?)?;
            let outcome = read_string(&row, "outcome")?;
            let recovery = recoveries
                .entry(client_api_key_ref.clone())
                .or_insert_with(|| ClientAdmissionRecovery {
                    client_api_key_ref,
                    recent_requests: Vec::new(),
                    running_requests: Vec::new(),
                });
            if started_at >= window_started_at {
                recovery.recent_requests.push(ClientAdmissionRecentRequest {
                    model_request_id: model_request_id.clone(),
                    started_at,
                });
            }
            if outcome == "running" {
                recovery
                    .running_requests
                    .push(ClientAdmissionRunningRequest {
                        model_request_id,
                        deadline_at,
                    });
            }
        }
        Ok(recoveries.into_values().collect())
    }
}

impl ClientAdmissionRecoveryPort for SqliteClientAdmissionRecoveryRepository {
    fn load_recovery(
        &self,
        since: std::time::SystemTime,
    ) -> futures::future::BoxFuture<'_, Result<Vec<CoreAdmissionRecovery>, ClientAdmissionError>>
    {
        Box::pin(async move {
            self.load_client_admission_recovery(DateTime::<Utc>::from(since))
                .await
                .map_err(|source| ClientAdmissionError(Some(source.into())))?
                .into_iter()
                .map(|recovery| {
                    let client_api_key_id = ClientApiKeyId::new(recovery.client_api_key_ref)
                        .map_err(|source| ClientAdmissionError(Some(source.into())))?;
                    let recent_requests = recovery
                        .recent_requests
                        .into_iter()
                        .map(|request| {
                            Ok(RecentAdmissionFact {
                                model_request_id: ModelRequestId::new(request.model_request_id)
                                    .map_err(|source| ClientAdmissionError(Some(source.into())))?,
                                started_at: request.started_at.into(),
                            })
                        })
                        .collect::<Result<Vec<_>, ClientAdmissionError>>()?;
                    let running_requests = recovery
                        .running_requests
                        .into_iter()
                        .map(|request| {
                            Ok(RunningAdmissionFact {
                                model_request_id: ModelRequestId::new(request.model_request_id)
                                    .map_err(|source| ClientAdmissionError(Some(source.into())))?,
                                expires_at: request.deadline_at.into(),
                            })
                        })
                        .collect::<Result<Vec<_>, ClientAdmissionError>>()?;
                    Ok(CoreAdmissionRecovery {
                        client_api_key_id,
                        recent_requests,
                        running_requests,
                    })
                })
                .collect()
        })
    }
}

fn read_string(row: &sqlx::sqlite::SqliteRow, field: &'static str) -> StoreResult<String> {
    row.try_get(field).map_err(|_| StoreError::InvalidData {
        entity: "SQLite client admission recovery",
        message: "persisted text field is invalid".to_owned(),
        source: None,
    })
}

fn read_i64(row: &sqlx::sqlite::SqliteRow, field: &'static str) -> StoreResult<i64> {
    row.try_get(field).map_err(|_| StoreError::InvalidData {
        entity: "SQLite client admission recovery",
        message: "persisted integer field is invalid".to_owned(),
        source: None,
    })
}
