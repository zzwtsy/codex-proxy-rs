//! 验证 SSE 与 WebSocket 的首字边界、请求级计时原点与独立上游耗时

use std::time::Instant;

use gateway_core::engine::provider::ProviderStream;
use gateway_core::event::ProviderResponseTimings;

use super::*;

fn timed_context(started_at: Instant, attempt: u32) -> AttemptContext {
    AttemptContext::new(
        RequestAttemptContext::new(
            ModelRequestId::new("req_output_timing").unwrap(),
            ClientApiKeyId::new("key_openai_contract").unwrap(),
        )
        .with_timing_started_at(started_at),
        NonZeroU32::new(attempt).unwrap(),
        SystemTime::now() + Duration::from_secs(30),
        account_policy(),
        AccountAttemptContext::new(BTreeSet::new(), None, None)
            .with_account_scope(contract_account_scope()),
        None,
        CancellationToken::new(),
    )
}

async fn first_upstream_timings(stream: &mut ProviderStream) -> ProviderResponseTimings {
    timeout(Duration::from_secs(5), async {
        while let Some(event) = stream.next().await {
            if let Some(observation) = event.unwrap().response_observation()
                && observation.timings().first_event_ms.is_some()
            {
                return observation.timings();
            }
        }
        panic!("missing upstream timing observation");
    })
    .await
    .expect("upstream timing before stream completion")
}

async fn finish_timings(
    stream: &mut ProviderStream,
    mut timings: ProviderResponseTimings,
) -> ProviderResponseTimings {
    timeout(Duration::from_secs(5), async {
        while let Some(event) = stream.next().await {
            if let Some(observation) = event.unwrap().response_observation() {
                timings = observation.timings();
            }
        }
        timings
    })
    .await
    .expect("complete upstream stream")
}

async fn observe_timing_events(websocket: bool, events: Vec<Value>) -> ProviderResponseTimings {
    let store = Arc::new(MemoryAccountStore::default());
    create_account(&store, "acct_provider_contract").await;
    let (base_url, server) = if websocket {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut ws = crate::transport::accept_codex_test_websocket_with(
                socket,
                |request: &tokio_tungstenite::tungstenite::handshake::server::Request, response| {
                    assert_eq!(
                        request.headers()["x-responsesapi-include-timing-metrics"],
                        "true"
                    );
                    response.headers_mut().insert(
                        "sec-websocket-extensions",
                        "permessage-deflate".parse().unwrap(),
                    );
                },
            )
            .await;
            ws.next().await.unwrap().unwrap();
            for event in events {
                ws.send(Message::Text(event.to_string().into()))
                    .await
                    .unwrap();
            }
        });
        (base_url, server)
    } else {
        let (base_url, release, _, server) = paused_chunked_sse_server(
            events[..1]
                .iter()
                .map(|event| format!("data: {event}\n\n"))
                .collect(),
            events[1..]
                .iter()
                .map(|event| format!("data: {event}\n\n"))
                .collect(),
        )
        .await;
        release.send(()).unwrap();
        (base_url, server)
    };
    let operation = if websocket {
        generate_operation()
    } else {
        http_generate_operation()
    };
    let mut stream = provider_with_base_url(&store, base_url)
        .execute(
            planned_request("openai", operation),
            timed_context(Instant::now() - Duration::from_secs(20), 2),
        )
        .await
        .unwrap();
    let timings = finish_timings(&mut stream, ProviderResponseTimings::default()).await;
    server.await.unwrap();
    timings
}

#[tokio::test]
async fn official_response_duration_is_observed_independently_of_the_request_clock() {
    for websocket in [false, true] {
        let created = json!({"type":"response.created","response":{"id":"resp_official_time","created_at":100}});
        let completed = json!({"type":"response.completed","response":{"id":"resp_official_time","status":"completed","created_at":100,"completed_at":107,"output":[],"usage":{"input_tokens":1,"output_tokens":112,"total_tokens":113}}});
        let metrics = json!({"type":"responsesapi.websocket_timing","timing_metrics":{
            "responses_duration_excl_engine_and_client_tool_time_ms":120.25,
            "engine_service_total_ms":6400.0,
            "engine_iapi_ttft_total_ms":650.5,
            "engine_service_ttft_total_ms":720.25,
            "engine_iapi_tbt_across_engine_calls_ms":18.45,
            "engine_service_tbt_across_engine_calls_ms":20.12
        }});
        let wrong_id = json!({"type":"responsesapi.websocket_timing","response_id":"resp_other","timing_metrics":{"engine_service_total_ms":999.0}});
        let invalid = json!({"type":"responsesapi.websocket_timing","timing_metrics":{
            "responses_duration_excl_engine_and_client_tool_time_ms":null,
            "engine_service_total_ms":"100",
            "engine_iapi_ttft_total_ms":-1,
            "engine_service_ttft_total_ms":1e99,
            "engine_iapi_tbt_across_engine_calls_ms":{},
            "engine_service_tbt_across_engine_calls_ms":[]
        }});
        let stale = json!({"type":"responsesapi.websocket_timing","timing_metrics":{"engine_service_total_ms":999.0}});
        let events = [stale, created, metrics, wrong_id, invalid, completed];
        let timings = observe_timing_events(websocket, events.to_vec()).await;
        assert_eq!(timings.upstream_response_ms, Some(7_000));
        assert_eq!(timings.upstream_api_overhead_ms, Some(120.25));
        assert_eq!(timings.upstream_engine_ms, Some(6400.0));
        assert_eq!(timings.upstream_engine_iapi_ttft_ms, Some(650.5));
        assert_eq!(timings.upstream_engine_service_ttft_ms, Some(720.25));
        assert_eq!(timings.upstream_engine_iapi_tbt_ms, Some(18.45));
        assert_eq!(timings.upstream_engine_service_tbt_ms, Some(20.12));
        assert_eq!(timings.first_token_ms, None);
        assert!(timings.first_event_ms.is_some_and(|value| value >= 20_000));
    }
}

// 保留实测计时帧的层级和小数，响应身份使用虚构值，不包含请求正文
fn response_critical_path_event() -> Value {
    json!({"type":"responsesapi.websocket_timing","timing_metrics":{
        "timing_scope":"logical_turn",
        "response_id":"resp_critical_path",
        "pre_inference_ms":211.691468,
        "client_tool_pause_total_ms":3730.088369,
        "total_turn_time_s":25.017284596,
        "num_engine_calls":4,
        "first_sampled_message_ttft_ms":null,
        "critical_path":{
            "scope":"response",
            "coverage":"complete",
            "boundary_type":"actionable_output_item_done",
            "num_engine_calls":1,
            "responses_harness_unblocked_ms":6792.094664,
            "responses_pre_inference_ms":35.747883,
            "engine_wall_ms":6710.68507,
            "client_tool_pause_ms":0.0,
            "responses_other_ms":45.66171099999974,
            "engine_iapi_total_ms":null,
            "taas_request_setup_total_ms":null,
            "taas_provider_wait_total_ms":null,
            "sampling_and_stream_total_ms":null,
            "inference_residual_total_ms":null
        }
    }})
}

fn critical_path_response_events(metrics: Value) -> Vec<Value> {
    vec![
        json!({"type":"response.created","response":{"id":"resp_critical_path"}}),
        metrics,
        json!({"type":"response.completed","response":{"id":"resp_critical_path","status":"completed","created_at":100,"completed_at":107,"output":[],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}),
    ]
}

#[tokio::test]
async fn response_critical_path_is_observed_without_logical_turn_totals_on_both_transports() {
    for websocket in [false, true] {
        let mut metrics = response_critical_path_event();
        // 同帧即使携带整轮专项值，也不能与响应级 critical path 混算
        metrics["timing_metrics"]["engine_service_total_ms"] = json!(25_000);
        metrics["timing_metrics"]["engine_iapi_ttft_total_ms"] = json!(500);
        let mut events = critical_path_response_events(metrics);
        let mut wrong_id = response_critical_path_event();
        wrong_id["timing_metrics"]["response_id"] = json!("resp_other");
        wrong_id["timing_metrics"]["critical_path"]["engine_wall_ms"] = json!(999);
        events.insert(2, wrong_id);
        let timings = observe_timing_events(websocket, events).await;
        assert_eq!(timings.upstream_engine_ms, Some(6710.68507));
        assert!((timings.upstream_api_overhead_ms.unwrap() - 81.409594).abs() < 0.000001);
        assert_eq!(timings.upstream_response_ms, Some(7000));
        assert_eq!(timings.first_token_ms, None);
        assert_eq!(timings.upstream_engine_iapi_ttft_ms, None);
        assert_eq!(timings.upstream_engine_service_ttft_ms, None);
        assert_eq!(timings.upstream_engine_iapi_tbt_ms, None);
        assert_eq!(timings.upstream_engine_service_tbt_ms, None);
    }
}

#[tokio::test]
async fn critical_path_rejects_mismatched_identity_and_incomplete_or_cumulative_scope() {
    for (pointer, value) in [
        ("/timing_metrics/response_id", json!("resp_other")),
        ("/timing_metrics/response_id", Value::Null),
        ("/timing_metrics/critical_path/scope", json!("logical_turn")),
        ("/timing_metrics/critical_path/scope", Value::Null),
        ("/timing_metrics/critical_path/coverage", json!("partial")),
        ("/timing_metrics/critical_path/coverage", Value::Null),
        (
            "/timing_metrics/critical_path/boundary_type",
            json!("unknown"),
        ),
        ("/timing_metrics/critical_path", Value::Null),
    ] {
        let mut metrics = response_critical_path_event();
        // 正确的顶层 ID 不能掩盖嵌套 ID 的冲突
        metrics["response_id"] = json!("resp_critical_path");
        *metrics.pointer_mut(pointer).unwrap() = value;
        let timings = observe_timing_events(false, critical_path_response_events(metrics)).await;
        assert_eq!(timings.upstream_engine_ms, None, "{pointer}");
        assert_eq!(timings.upstream_api_overhead_ms, None, "{pointer}");
    }
    for identity in ["response_id", "response"] {
        let mut metrics = response_critical_path_event();
        metrics[identity] = if identity == "response" {
            json!({"id":"resp_other"})
        } else {
            json!("resp_other")
        };
        let timings = observe_timing_events(false, critical_path_response_events(metrics)).await;
        assert_eq!(timings.upstream_engine_ms, None);
    }
    let metrics = json!({"type":"responsesapi.websocket_timing","timing_metrics":{
        "timing_scope":"logical_turn",
        "engine_service_total_ms":25_000,
        "responses_duration_excl_engine_and_client_tool_time_ms":500,
        "engine_iapi_ttft_total_ms":200
    }});
    let timings = observe_timing_events(false, critical_path_response_events(metrics)).await;
    assert_eq!(timings.upstream_engine_ms, None);
    assert_eq!(timings.upstream_api_overhead_ms, None);
    assert_eq!(timings.upstream_engine_iapi_ttft_ms, None);
}

#[tokio::test]
async fn critical_path_keeps_missing_or_invalid_components_unknown_and_preserves_zero() {
    for value in [Value::Null, json!(-1), json!("45"), json!(1e99)] {
        let mut metrics = response_critical_path_event();
        metrics["timing_metrics"]["critical_path"]["responses_other_ms"] = value;
        let timings = observe_timing_events(false, critical_path_response_events(metrics)).await;
        assert_eq!(timings.upstream_api_overhead_ms, None);
        assert_eq!(timings.upstream_engine_ms, Some(6710.68507));
    }
    for (component, expected) in [(0.0, Some(0.0)), (5e18, None)] {
        let mut metrics = response_critical_path_event();
        let path = &mut metrics["timing_metrics"]["critical_path"];
        path["responses_other_ms"] = json!(component);
        path["responses_pre_inference_ms"] = json!(component);
        path["engine_wall_ms"] = json!(0);
        let timings = observe_timing_events(false, critical_path_response_events(metrics)).await;
        assert_eq!(timings.upstream_api_overhead_ms, expected);
        assert_eq!(timings.upstream_engine_ms, Some(0.0));
    }
}

#[tokio::test]
async fn timing_frames_outside_the_active_response_do_not_enter_request_metrics() {
    let store = Arc::new(MemoryAccountStore::default());
    create_account(&store, "acct_provider_contract").await;
    let timing = json!({"type":"responsesapi.websocket_timing","timing_metrics":{
        "responses_duration_excl_engine_and_client_tool_time_ms":0,
        "engine_service_total_ms":450,
        "engine_iapi_ttft_total_ms":211,
        "engine_service_ttft_total_ms":233,
        "engine_iapi_tbt_across_engine_calls_ms":2.450638,
        "engine_service_tbt_across_engine_calls_ms":5.267279
    }});
    let created = json!({"type":"response.created","response":{"id":"resp_boundary"}});
    let completed = json!({"type":"response.completed","response":{"id":"resp_boundary","status":"completed","output":[],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}});
    let (base_url, release, _, server) = paused_chunked_sse_server(
        format!("data: {timing}\n\ndata: {created}\n\n"),
        format!("data: {completed}\n\ndata: {timing}\n\n"),
    )
    .await;
    release.send(()).unwrap();
    let mut stream = provider_with_base_url(&store, base_url)
        .execute(
            planned_request("openai", http_generate_operation()),
            timed_context(Instant::now(), 1),
        )
        .await
        .unwrap();
    let timings = finish_timings(&mut stream, ProviderResponseTimings::default()).await;
    assert_eq!(timings.upstream_api_overhead_ms, None);
    assert_eq!(timings.upstream_engine_ms, None);
    assert_eq!(timings.upstream_engine_iapi_ttft_ms, None);
    assert_eq!(timings.upstream_engine_service_ttft_ms, None);
    assert_eq!(timings.upstream_engine_iapi_tbt_ms, None);
    assert_eq!(timings.upstream_engine_service_tbt_ms, None);
    assert_eq!(timings.first_token_ms, None);
    server.await.unwrap();
}

#[tokio::test]
async fn content_without_output_item_start_keeps_ttft_unknown_on_every_attempt() {
    for attempt in [1, 2] {
        let store = Arc::new(MemoryAccountStore::default());
        create_account(&store, "acct_provider_contract").await;
        let first_chunk = concat!(
            r#"data: {"type":"response.created","response":{"id":"resp_scope_capture","model":"gpt-5.4"}}"#,
            "\n\n",
            r#"data: {"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":"hello"}"#,
            "\n\n",
        )
        .to_owned();
        let (base_url, release, _, server) =
            paused_chunked_sse_server(first_chunk, CAPTURE_COMPLETED_SSE.to_owned()).await;
        // 用已有请求原点模拟选号或之前尝试的等待，不依赖 sleep 的调度精度
        let started_at = Instant::now() - Duration::from_secs(3);
        let mut stream = provider_with_base_url(&store, base_url)
            .execute(
                planned_request("openai", http_generate_operation()),
                timed_context(started_at, attempt),
            )
            .await
            .unwrap();
        let first = first_upstream_timings(&mut stream).await;
        assert!(first.first_event_ms.is_some_and(|value| value >= 3_000));
        assert_eq!(first.first_token_ms, None);
        assert!(first.first_text_ms.is_some_and(|value| value >= 3_000));
        release.send(()).unwrap();
        let final_timings = finish_timings(&mut stream, first).await;
        assert_eq!(final_timings.first_token_ms, first.first_token_ms);
        server.await.unwrap();
    }
}

#[tokio::test]
async fn structural_frames_start_ttft_before_content_on_http_and_websocket() {
    for websocket in [false, true] {
        for (output, has_content) in [
            (
                json!({"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":"hello"}),
                true,
            ),
            (
                json!({"type":"response.reasoning_summary_text.delta","output_index":0,"summary_index":0,"delta":"thinking"}),
                true,
            ),
            (
                json!({"type":"response.function_call_arguments.delta","output_index":0,"delta":"{}"}),
                true,
            ),
            (
                json!({"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":""}),
                false,
            ),
            (
                json!({"type":"response.reasoning_summary_text.delta","output_index":0,"summary_index":0,"delta":""}),
                false,
            ),
        ] {
            let store = Arc::new(MemoryAccountStore::default());
            create_account(&store, "acct_provider_contract").await;
            let is_text = output["type"] == "response.output_text.delta";
            let is_reasoning = output["type"] == "response.reasoning_summary_text.delta";
            let created = json!({"type":"response.created","response":{"id":"resp_timing","model":"gpt-5.4"}});
            let added = json!({"type":"response.output_item.added","output_index":0,"item":{"type":"message","content":[]}});
            let completed = json!({"type":"response.completed","response":{"id":"resp_timing","status":"completed","output":[],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}});
            let (base_url, release, server) = if websocket {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let base_url = format!("http://{}", listener.local_addr().unwrap());
                let (release, released) = oneshot::channel();
                let server = tokio::spawn(async move {
                    let (socket, _) = listener.accept().await.unwrap();
                    let mut ws = crate::transport::accept_codex_test_websocket_with(
                        socket,
                        |request: &tokio_tungstenite::tungstenite::handshake::server::Request,
                         response| {
                            assert_eq!(
                                request.headers()["x-responsesapi-include-timing-metrics"],
                                "true"
                            );
                            response.headers_mut().insert(
                                "sec-websocket-extensions",
                                "permessage-deflate".parse().unwrap(),
                            );
                        },
                    )
                    .await;
                    ws.next().await.unwrap().unwrap();
                    for event in [created, added] {
                        ws.send(Message::Text(event.to_string().into()))
                            .await
                            .unwrap();
                    }
                    released.await.unwrap();
                    for event in [output, completed] {
                        ws.send(Message::Text(event.to_string().into()))
                            .await
                            .unwrap();
                    }
                });
                (base_url, release, server)
            } else {
                let (base_url, release, _, server) = paused_chunked_sse_server(
                    format!("data: {created}\n\ndata: {added}\n\n"),
                    format!("data: {output}\n\ndata: {completed}\n\n"),
                )
                .await;
                (base_url, release, server)
            };
            let operation = if websocket {
                generate_operation()
            } else {
                http_generate_operation()
            };
            let mut stream = provider_with_base_url(&store, base_url)
                .execute(
                    planned_request("openai", operation),
                    timed_context(Instant::now() - Duration::from_secs(3), 2),
                )
                .await
                .unwrap();
            let first = timeout(Duration::from_secs(5), async {
                while let Some(event) = stream.next().await {
                    if let Some(observation) = event.unwrap().response_observation()
                        && observation.timings().first_token_ms.is_some()
                    {
                        return observation.timings();
                    }
                }
                panic!("missing structural first-token observation");
            })
            .await
            .expect("structural first token before releasing content");
            assert!(first.first_token_ms.is_some_and(|value| value >= 3_000));
            assert_eq!(first.first_text_ms, None);
            assert_eq!(first.first_reasoning_ms, None);
            release.send(()).unwrap();
            let final_timings = finish_timings(&mut stream, first).await;
            assert_eq!(final_timings.first_token_ms, first.first_token_ms);
            assert_eq!(
                final_timings.first_text_ms.is_some(),
                has_content && is_text
            );
            assert_eq!(
                final_timings.first_reasoning_ms.is_some(),
                has_content && is_reasoning
            );
            server.await.unwrap();
        }
    }
}
