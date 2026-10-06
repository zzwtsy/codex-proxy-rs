use std::time::Duration;

use chrono::{TimeDelta, Utc};
use gateway_core::{
    account::{OpaqueProviderData, ProviderAccountId},
    engine::{
        ModelRequestId,
        admission::{
            ClientAdmissionDecision, ClientAdmissionPort, ClientAdmissionRejection,
            ClientAdmissionRequest,
        },
        continuation::{
            NativeContinuationPin, NativeContinuationPort, NativeContinuationStoreErrorKind,
            PreviousResponseId,
        },
    },
    policy::{ClientApiKeyId, RateLimits},
    provider_ports::{
        NewOAuthPendingFlow, OAuthPendingBinding, OAuthPendingClaimOutcome,
        OAuthPendingConsumeOutcome, OAuthPendingFlowPort, OAuthPendingPutOutcome,
        OAuthPendingReleaseOutcome,
    },
    routing::ProviderKind,
    task::{
        WorkerId, WorkerKind, WorkerLeaderLeasePort, WorkerLeaseAcquisition, WorkerLeaseRequest,
    },
};
use gateway_store::{
    AuthSessionRecord, AuthStateRepository, LocalAuthStateRepository, LocalClientAdmissionPort,
    LocalNativeContinuationRepository, LocalOAuthPendingFlowRepository, LocalWorkerLeaderLeasePort,
    SessionSubjectRecord,
};

#[tokio::test]
async fn local_auth_state_stores_expires_and_deletes_process_sessions() {
    let repository = LocalAuthStateRepository::default();
    let session = AuthSessionRecord {
        subject: SessionSubjectRecord::Admin {
            admin_user_id: "admin".to_owned(),
            credential_fingerprint: "fingerprint".to_owned(),
        },
        expires_at: Utc::now() + TimeDelta::minutes(1),
        absolute_expires_at: None,
    };

    repository
        .store_session("session-id", &session)
        .await
        .expect("store local authentication session");
    assert_eq!(
        repository
            .load_session("session-id")
            .await
            .expect("load local authentication session"),
        Some(session.clone())
    );
    assert_eq!(
        repository
            .delete_session("session-id")
            .await
            .expect("delete local authentication session"),
        Some(session)
    );
    assert!(
        repository
            .load_session("session-id")
            .await
            .expect("load deleted session")
            .is_none()
    );
}

#[tokio::test]
async fn local_auth_state_enforces_source_and_global_fixed_windows() {
    let repository = LocalAuthStateRepository::default();
    let window = Duration::from_millis(100);

    for _ in 0..2 {
        assert_eq!(
            repository
                .consume_login_attempt("source-a", 2, 5, window)
                .await
                .expect("source attempt"),
            None
        );
    }
    assert!(
        repository
            .consume_login_attempt("source-a", 2, 5, window)
            .await
            .expect("source limit")
            .is_some()
    );
    assert_eq!(
        repository
            .consume_login_attempt("source-b", 2, 5, window)
            .await
            .expect("global limit not yet reached"),
        None
    );
    assert_eq!(
        repository
            .consume_login_attempt("source-c", 2, 5, window)
            .await
            .expect("global limit not yet reached"),
        None
    );
    assert!(
        repository
            .consume_login_attempt("source-d", 2, 5, window)
            .await
            .expect("global limit")
            .is_some()
    );
    for attempt in 0..100 {
        assert!(
            repository
                .consume_login_attempt(&format!("new-source-{attempt}"), 2, 5, window)
                .await
                .expect("reject requests after global limit")
                .is_some()
        );
    }

    tokio::time::sleep(window + Duration::from_millis(10)).await;
    assert_eq!(
        repository
            .consume_login_attempt("source-a", 2, 5, window)
            .await
            .expect("new fixed window"),
        None
    );
}

#[tokio::test]
async fn local_oauth_pending_flow_claim_release_and_consume_are_owner_fenced() {
    let repository = LocalOAuthPendingFlowRepository::default();
    let provider = ProviderKind::new("openai").expect("provider kind");
    let flow = OAuthPendingBinding::try_new("flow").expect("flow binding");
    let owner = OAuthPendingBinding::try_new("owner").expect("owner binding");
    let other_owner = OAuthPendingBinding::try_new("other-owner").expect("other owner");
    let claim = OAuthPendingBinding::try_new("claim").expect("claim binding");
    let other_claim = OAuthPendingBinding::try_new("other-claim").expect("other claim");
    let payload = OpaqueProviderData::new(serde_json::Map::from_iter([(
        "state".to_owned(),
        serde_json::Value::String("opaque".to_owned()),
    )]));

    assert_eq!(
        repository
            .put_if_absent(
                NewOAuthPendingFlow::try_new(
                    provider.clone(),
                    flow.clone(),
                    owner.clone(),
                    Duration::from_secs(1),
                    payload.clone(),
                )
                .expect("new pending flow")
            )
            .await
            .expect("store OAuth pending flow"),
        OAuthPendingPutOutcome::Stored
    );
    assert_eq!(
        repository
            .put_if_absent(
                NewOAuthPendingFlow::try_new(
                    provider.clone(),
                    flow.clone(),
                    owner.clone(),
                    Duration::from_secs(1),
                    payload.clone(),
                )
                .expect("duplicate pending flow")
            )
            .await
            .expect("detect duplicate OAuth pending flow"),
        OAuthPendingPutOutcome::AlreadyExists
    );
    assert_eq!(
        repository
            .claim_if_owner(
                &provider,
                &flow,
                &other_owner,
                &claim,
                Duration::from_secs(1),
            )
            .await
            .expect("reject a different owner"),
        OAuthPendingClaimOutcome::OwnerMismatch
    );
    assert_eq!(
        repository
            .claim_if_owner(&provider, &flow, &owner, &claim, Duration::from_secs(1),)
            .await
            .expect("claim pending flow"),
        OAuthPendingClaimOutcome::Claimed(payload)
    );
    assert_eq!(
        repository
            .claim_if_owner(
                &provider,
                &flow,
                &owner,
                &other_claim,
                Duration::from_secs(1),
            )
            .await
            .expect("reject competing claim"),
        OAuthPendingClaimOutcome::InProgress
    );
    assert_eq!(
        repository
            .release_claim(&provider, &flow, &owner, &other_claim)
            .await
            .expect("reject claim mismatch"),
        OAuthPendingReleaseOutcome::ClaimMismatch
    );
    assert_eq!(
        repository
            .release_claim(&provider, &flow, &owner, &claim)
            .await
            .expect("release pending flow claim"),
        OAuthPendingReleaseOutcome::Released
    );
    assert_eq!(
        repository
            .claim_if_owner(
                &provider,
                &flow,
                &owner,
                &other_claim,
                Duration::from_secs(1),
            )
            .await
            .expect("reclaim released flow"),
        OAuthPendingClaimOutcome::Claimed(OpaqueProviderData::new(serde_json::Map::from_iter([(
            "state".to_owned(),
            serde_json::Value::String("opaque".to_owned()),
        )])))
    );
    assert_eq!(
        repository
            .consume_claim(&provider, &flow, &owner, &other_claim)
            .await
            .expect("consume claimed flow"),
        OAuthPendingConsumeOutcome::Consumed
    );
    assert_eq!(
        repository
            .consume_claim(&provider, &flow, &owner, &other_claim)
            .await
            .expect("do not consume a flow twice"),
        OAuthPendingConsumeOutcome::NotFound
    );
}

#[tokio::test]
async fn local_oauth_pending_flow_reclaims_expired_claim_and_expires_flow() {
    let repository = LocalOAuthPendingFlowRepository::default();
    let provider = ProviderKind::new("openai").expect("provider kind");
    let flow = OAuthPendingBinding::try_new("short-flow").expect("flow binding");
    let owner = OAuthPendingBinding::try_new("owner").expect("owner binding");
    let first_claim = OAuthPendingBinding::try_new("first-claim").expect("claim binding");
    let second_claim = OAuthPendingBinding::try_new("second-claim").expect("claim binding");
    let payload = OpaqueProviderData::new(serde_json::Map::new());

    repository
        .put_if_absent(
            NewOAuthPendingFlow::try_new(
                provider.clone(),
                flow.clone(),
                owner.clone(),
                Duration::from_millis(120),
                payload.clone(),
            )
            .expect("short-lived pending flow"),
        )
        .await
        .expect("store short-lived flow");
    repository
        .claim_if_owner(
            &provider,
            &flow,
            &owner,
            &first_claim,
            Duration::from_millis(20),
        )
        .await
        .expect("claim flow");
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(
        repository
            .claim_if_owner(
                &provider,
                &flow,
                &owner,
                &second_claim,
                Duration::from_millis(20),
            )
            .await
            .expect("reclaim expired claim"),
        OAuthPendingClaimOutcome::Claimed(payload)
    );
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        repository
            .claim_if_owner(
                &provider,
                &flow,
                &owner,
                &first_claim,
                Duration::from_millis(20),
            )
            .await
            .expect("flow expiration"),
        OAuthPendingClaimOutcome::NotFound
    );
}

#[tokio::test]
async fn local_native_continuations_are_process_local_and_client_fenced() {
    let repository = LocalNativeContinuationRepository::default();
    let separate_process = LocalNativeContinuationRepository::default();
    let provider = ProviderKind::new("openai").expect("provider");
    let response_id = PreviousResponseId::new("resp_sensitive\0handle");
    let client = ClientApiKeyId::new("key_primary").expect("client API key ID");
    let pin = NativeContinuationPin::new(
        response_id.clone(),
        PreviousResponseId::new("upstream-id"),
        client.clone(),
        provider.clone(),
        ProviderAccountId::new("acct_primary").expect("account"),
    );
    repository.record(pin.clone()).await.expect("record pin");

    assert_eq!(
        repository
            .resolve(&client, &response_id)
            .await
            .expect("resolve pin"),
        Some(pin)
    );
    assert!(
        separate_process
            .resolve(&client, &response_id)
            .await
            .expect("separate process cache miss")
            .is_none()
    );
    assert_eq!(
        repository
            .resolve(
                &ClientApiKeyId::new("key_secondary").expect("second client ID"),
                &response_id,
            )
            .await
            .expect_err("another client must be fenced")
            .kind(),
        NativeContinuationStoreErrorKind::OwnershipMismatch
    );
}

#[tokio::test]
async fn local_worker_leases_expire_and_fence_stale_guards() {
    let first_port = LocalWorkerLeaderLeasePort::default();
    let second_port = first_port.clone();
    let worker = WorkerId::try_new(WorkerKind::Retention, "sqlite-runtime").expect("worker ID");
    let request = WorkerLeaseRequest::try_new(worker, Duration::from_millis(30))
        .expect("worker lease request");
    let mut first_guard = match first_port
        .try_acquire(request.clone())
        .await
        .expect("first lease acquisition")
    {
        WorkerLeaseAcquisition::Acquired(guard) => guard,
        WorkerLeaseAcquisition::Busy { .. } => panic!("initial lease must be available"),
    };
    let first_token = first_guard.fencing_token();
    assert!(matches!(
        second_port
            .try_acquire(request.clone())
            .await
            .expect("contended acquisition"),
        WorkerLeaseAcquisition::Busy { .. }
    ));
    tokio::time::sleep(Duration::from_millis(40)).await;
    let second_guard = match second_port
        .try_acquire(request.clone())
        .await
        .expect("lease acquisition after expiry")
    {
        WorkerLeaseAcquisition::Acquired(guard) => guard,
        WorkerLeaseAcquisition::Busy { .. } => panic!("expired lease must be available"),
    };
    assert!(second_guard.fencing_token() > first_token);
    assert!(first_guard.renew().await.is_err());
    first_guard.release().await.expect("release stale guard");
    assert!(matches!(
        first_port
            .try_acquire(request.clone())
            .await
            .expect("new lease remains active"),
        WorkerLeaseAcquisition::Busy { .. }
    ));
    second_guard.release().await.expect("release current lease");
    let third_guard = match first_port
        .try_acquire(request)
        .await
        .expect("lease acquisition after release")
    {
        WorkerLeaseAcquisition::Acquired(guard) => guard,
        WorkerLeaseAcquisition::Busy { .. } => panic!("released lease must be reusable"),
    };
    third_guard.release().await.expect("release final lease");
}

#[tokio::test]
async fn local_client_admission_enforces_concurrency_and_keeps_rpm_after_release() {
    let repository = LocalClientAdmissionPort::default();
    let separate_process = LocalClientAdmissionPort::default();
    let client_id = ClientApiKeyId::new("key_primary").expect("client API key ID");
    let limits = RateLimits {
        max_concurrency: 1,
        requests_per_minute: 2,
    };
    let request = |id: &str| ClientAdmissionRequest {
        model_request_id: ModelRequestId::new(id).expect("model request ID"),
        client_api_key_id: client_id.clone(),
        lease_ttl: Duration::from_secs(1),
        allow_concurrency_acquire: true,
        limits,
    };

    let first = request("req_local_first");
    assert_eq!(
        repository
            .admit(first.clone())
            .await
            .expect("first admission"),
        ClientAdmissionDecision::Granted
    );
    assert_eq!(
        repository
            .admit(request("req_local_blocked"))
            .await
            .expect("concurrency check"),
        ClientAdmissionDecision::Rejected(ClientAdmissionRejection::ConcurrencyLimited)
    );
    assert!(
        repository
            .release(&client_id, &first.model_request_id)
            .await
            .expect("release first admission")
    );
    assert_eq!(
        repository
            .admit(request("req_local_second"))
            .await
            .expect("second admission"),
        ClientAdmissionDecision::Granted
    );
    assert_eq!(
        repository
            .admit(request("req_local_rate_limited"))
            .await
            .expect("RPM check"),
        ClientAdmissionDecision::Rejected(ClientAdmissionRejection::RateLimited)
    );
    assert_eq!(
        separate_process
            .admit(first)
            .await
            .expect("separate process admission"),
        ClientAdmissionDecision::Granted
    );
}
