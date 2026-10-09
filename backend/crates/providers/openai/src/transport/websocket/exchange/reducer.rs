//! WebSocket aggregate/stream 共用事件归约器

use gateway_protocol::openai::events;
use serde_json::Value;

use crate::transport::protocol::websocket::{
    websocket_event_type, websocket_metadata_headers, websocket_metadata_turn_state,
    websocket_response_completed_id, websocket_response_is_interrupted,
};
use crate::transport::response_meta;

use super::super::pool::{CodexWebSocketConnectionMetadata, WebSocketContinuationState};
use super::CodexWebSocketExchangeError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::transport::websocket) enum WebSocketTerminalKind {
    Completed,
    Interrupted,
    Incomplete,
    Failed,
}

pub(super) enum ExchangeAction {
    RateLimits(events::ParsedRateLimits),
    Forward {
        terminal: Option<WebSocketTerminalKind>,
    },
    Ignore,
}

pub(super) struct ReducedWebSocketEvent {
    pub(super) action: ExchangeAction,
    pub(super) diagnostic_event_type: Option<String>,
    pub(super) turn_state_update: Option<String>,
}

pub(super) fn reduce_websocket_event(
    raw: &str,
    metadata: &mut CodexWebSocketConnectionMetadata,
    continuation: &mut WebSocketContinuationState,
) -> Result<ReducedWebSocketEvent, CodexWebSocketExchangeError> {
    // 解析只服务连接状态和观测，未知或不可解析文本仍交付下游
    let Ok(value) = serde_json::from_str::<Value>(raw) else {
        return Ok(ReducedWebSocketEvent {
            action: ExchangeAction::Forward { terminal: None },
            diagnostic_event_type: None,
            turn_state_update: None,
        });
    };
    let diagnostic_event_type = diagnostic_event_type(websocket_event_type(&value));
    if let Some(parsed) = events::parse_rate_limits_event(&value) {
        let headers = events::rate_limits_to_header_pairs(&parsed);
        metadata.rate_limit_headers.extend(headers);
        return Ok(ReducedWebSocketEvent {
            action: ExchangeAction::RateLimits(parsed),
            diagnostic_event_type,
            turn_state_update: None,
        });
    }

    response_meta::merge_response_metadata(
        &mut metadata.response_metadata,
        websocket_metadata_headers(&value),
    );
    if let Some(model) = response_meta::reported_model_from_event(&value) {
        metadata.response_metadata.effective_model = Some(model.to_owned());
    }
    let turn_state_update = websocket_metadata_turn_state(&value).and_then(|turn_state| {
        if metadata.turn_state.is_some() {
            return None;
        }
        metadata.turn_state = Some(turn_state.clone());
        Some(turn_state)
    });

    let event = websocket_event_type(&value);
    if let Some(response_id) = websocket_response_completed_id(&value) {
        continuation.record_completed(response_id);
    }

    let terminal = match event {
        Some("response.completed") => Some(WebSocketTerminalKind::Completed),
        Some("response.incomplete") if websocket_response_is_interrupted(&value) => {
            Some(WebSocketTerminalKind::Interrupted)
        }
        Some("response.incomplete") => Some(WebSocketTerminalKind::Incomplete),
        Some("response.failed" | "error") => Some(WebSocketTerminalKind::Failed),
        _ => None,
    };
    let action = if event == Some("codex.rate_limits") {
        ExchangeAction::Ignore
    } else {
        ExchangeAction::Forward { terminal }
    };
    Ok(ReducedWebSocketEvent {
        action,
        diagnostic_event_type,
        turn_state_update,
    })
}

fn diagnostic_event_type(event_type: Option<&str>) -> Option<String> {
    const MAX_EVENT_TYPE_BYTES: usize = 128;

    let event_type = event_type?;
    (!event_type.is_empty()
        && event_type.len() <= MAX_EVENT_TYPE_BYTES
        && event_type.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
        }))
    .then(|| event_type.to_owned())
}
