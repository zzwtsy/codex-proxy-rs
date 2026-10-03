//! 从持久执行账本重建客户端准入热状态的中立合同。

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::StoreResult;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientAdmissionRecentRequest {
    pub model_request_id: String,
    pub started_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientAdmissionRunningRequest {
    pub model_request_id: String,
    pub deadline_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientAdmissionRecovery {
    pub client_api_key_ref: String,
    pub recent_requests: Vec<ClientAdmissionRecentRequest>,
    pub running_requests: Vec<ClientAdmissionRunningRequest>,
}

#[async_trait]
pub trait ClientAdmissionRecoveryRepository: Send + Sync {
    async fn load_client_admission_recovery(
        &self,
        window_started_at: DateTime<Utc>,
    ) -> StoreResult<Vec<ClientAdmissionRecovery>>;
}
