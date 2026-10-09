//! 验证 xAI Provider 独立采集输出时间，结构帧与空增量不启动首字

use std::time::Instant;

use super::*;

#[tokio::test]
async fn provider_observes_semantic_output_on_the_request_clock() {
    for (event_type, item_type, delta, has_text, has_reasoning) in [
        (
            "response.output_text.delta",
            "message",
            "hello",
            true,
            false,
        ),
        ("response.output_text.delta", "message", "", false, false),
        (
            "response.reasoning_summary_text.delta",
            "reasoning",
            "plan",
            false,
            true,
        ),
        (
            "response.reasoning_summary_text.delta",
            "reasoning",
            "",
            false,
            false,
        ),
        (
            "response.function_call_arguments.delta",
            "function_call",
            "{}",
            false,
            false,
        ),
        (
            "response.function_call_arguments.delta",
            "function_call",
            "",
            false,
            false,
        ),
    ] {
        let body = [
            json!({"type":"response.created","response":{"id":"resp_timing","model":MODEL}}),
            json!({"type":"response.output_item.added","output_index":0,"item":{"id":"item_timing","type":item_type,"call_id":"call_timing","name":"lookup","content":[]}}),
            json!({"type":event_type,"item_id":"item_timing","call_id":"call_timing","output_index":0,"content_index":0,"summary_index":0,"delta":delta}),
            json!({"type":"response.completed","response":{"id":"resp_timing","status":"completed","output":[],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}),
        ]
        .into_iter()
        .map(|event| format!("data: {event}\n\n"))
        .collect::<String>()
        .into_bytes();
        let transport = StubInferenceTransport::sequence([InferenceMode::SuccessBody(body)]);
        let provider = provider(StubSelector::success(), transport).await;
        let context = AttemptContext::new(
            gateway_core::engine::RequestAttemptContext::new(
                ModelRequestId::new("req_xai_timing").unwrap(),
                ClientApiKeyId::new("key_xai_timing").unwrap(),
            )
            .with_timing_started_at(Instant::now() - Duration::from_secs(3)),
            NonZeroU32::new(2).unwrap(),
            SystemTime::now() + Duration::from_secs(30),
            selection_policy(),
            AccountAttemptContext::new(BTreeSet::new(), None, None),
            None,
            CancellationToken::new(),
        );
        let mut stream = provider
            .execute(provider_request("xai"), context)
            .await
            .unwrap();
        let mut timings = gateway_core::event::ProviderResponseTimings::default();
        let mut completed = false;
        while let Some(event) = stream.next().await {
            let event = event.unwrap();
            if let Some(observation) = event.response_observation() {
                timings = observation.timings();
            }
            completed |= event
                .canonical_facts()
                .iter()
                .any(|fact| matches!(fact, GatewayEvent::Completed(_)));
        }
        assert!(completed);
        assert!(timings.first_event_ms.is_some_and(|value| value >= 3_000));
        assert_eq!(
            timings.first_token_ms.is_some(),
            !delta.is_empty(),
            "{event_type}"
        );
        assert_eq!(timings.first_text_ms.is_some(), has_text);
        assert_eq!(timings.first_reasoning_ms.is_some(), has_reasoning);
        if let Some(first_token_ms) = timings.first_token_ms {
            assert!(first_token_ms >= 3_000);
        }
        assert_eq!(timings.upstream_response_ms, None);
    }
}
