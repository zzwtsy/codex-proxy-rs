//! 验证跨集合复用实例时旧请求和宿主回调仍持有各自的冻结计划

use super::*;
use gateway_admin::ports::plugins::PluginPreparation;

#[tokio::test]
async fn partial_update_preserves_inflight_middleware_and_uses_new_configuration_for_new_calls() {
    let (cache, store, runtime) = super::super::setup_with_contributions(Contributions::from([
        crate::support::contribution(Capability::Middleware, vec![Stage::Request], vec![], vec![]),
    ]))
    .await;
    let markers = tempfile::tempdir().unwrap();
    let mut snapshot = store.snapshot.lock().unwrap().clone();
    let template = snapshot.instances[0].clone();
    snapshot.instances = ["a", "b", "c"]
        .into_iter()
        .enumerate()
        .map(|(order, id)| {
            let mut instance = template.clone();
            instance.id = id.into();
            instance.configuration = serde_json::json!({"startup_marker": markers.path().join(id)});
            instance.bindings = vec![binding(
                MIDDLEWARE_CONTRIBUTION,
                "request",
                order as i32,
                PluginFailurePolicy::Reject,
            )];
            instance
        })
        .collect();
    let old = PluginPreparation::prepare(&runtime, snapshot.clone())
        .await
        .unwrap();
    let old_plan = runtime.execution_registry().middleware(&old).unwrap();
    assert!(runtime.policy_registry().resolve(&old).is_none());
    let (started, waiting) = tokio::sync::oneshot::channel();
    let (resume, blocked) = tokio::sync::oneshot::channel();
    let inflight = tokio::spawn(async move {
        old_plan
            .handle(
                middleware_context(ClientTransport::HttpJson),
                MiddlewareRequest::new(
                    "openai",
                    middleware_headers(),
                    Bytes::from_static(br#"{"input":"hello"}"#),
                ),
                gateway_core::middleware::compose(Vec::new(), move |request| {
                    Box::pin(async move {
                        started.send(()).unwrap();
                        blocked.await.unwrap();
                        Downstream {
                            calls: Arc::default(),
                            closes: Arc::default(),
                            reads: Arc::default(),
                            error: false,
                        }
                        .into_next()
                        .run(request)
                        .await
                    })
                }),
            )
            .await
    });
    waiting.await.unwrap();
    snapshot.config_revision = Revision::new(2).unwrap();
    snapshot.instances[1].revision = snapshot.config_revision;
    snapshot.instances[1].configuration["middleware_map_body"] = true.into();
    let new = PluginPreparation::prepare(&runtime, snapshot)
        .await
        .unwrap();
    drop(old);
    let new_plan = runtime.execution_registry().middleware(&new).unwrap();
    let changed = new_plan
        .handle(
            middleware_context(ClientTransport::HttpJson),
            MiddlewareRequest::new(
                "openai",
                middleware_headers(),
                Bytes::from_static(br#"{"input":"hello"}"#),
            ),
            Downstream {
                calls: Arc::default(),
                closes: Arc::default(),
                reads: Arc::default(),
                error: false,
            }
            .into_next(),
        )
        .await;
    let (_, _, _, mut changed_body, _) = changed.unwrap().into_parts();
    assert_eq!(
        changed_body
            .next_frame()
            .await
            .unwrap()
            .unwrap()
            .into_bytes(),
        Bytes::from_static(b"{\"downstream\":true} ")
    );
    changed_body.close().await;
    resume.send(()).unwrap();
    let response = inflight
        .await
        .unwrap()
        .expect("the old request completes through A/B/C callbacks");
    let (_, _, _, mut body, _) = response.into_parts();
    assert_eq!(
        body.next_frame().await.unwrap().unwrap().into_bytes(),
        Bytes::from_static(br#"{"downstream":true}"#)
    );
    body.close().await;
    assert!(new.is_ready());
    for (id, count) in [("a", 1), ("b", 2), ("c", 1)] {
        assert_eq!(
            std::fs::read_to_string(markers.path().join(id))
                .unwrap()
                .lines()
                .count(),
            count
        );
    }
    drop(new_plan);
    drop(new);
    super::super::wait_until_empty(cache.path()).await;
}
