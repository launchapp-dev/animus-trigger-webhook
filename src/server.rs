//! Axum HTTP server that turns inbound POST requests into [`TriggerEvent`]s.
//!
//! The router is intentionally minimal: one handler for `POST /` and one for
//! `POST /webhooks/*segment`. Both share the same body-parsing path and event
//! construction; the only difference is the resulting [`TriggerEvent::kind`].

use std::net::SocketAddr;
use std::sync::Arc;

use animus_trigger_protocol::TriggerEvent;
use axum::body::{to_bytes, Body};
use axum::extract::{Path, State};
use axum::http::{HeaderMap, Request, StatusCode};
use axum::response::IntoResponse;
use axum::routing::post;
use axum::Router;
use chrono::Utc;
use serde_json::{json, Value};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use uuid::Uuid;

/// Header consulted to populate [`TriggerEvent::subject_id`].
pub const HEADER_SUBJECT_ID: &str = "X-Animus-Subject-Id";

/// Header consulted to populate [`TriggerEvent::action_hint`].
pub const HEADER_ACTION_HINT: &str = "X-Animus-Action-Hint";

/// Maximum POST body size accepted by the server. 1 MiB is more than enough
/// for typical webhook payloads (GitHub maxes out at ~25 MB but most are
/// well under 1 MB); anything larger is rejected with `413 Payload Too Large`.
pub const MAX_BODY_BYTES: usize = 1024 * 1024;

/// Shared state handed to each axum handler.
#[derive(Clone)]
pub struct AppState {
    /// Sender side of the in-memory event channel. The watch stream owns the
    /// receiver; handlers `try_send` into this and drop events on overflow
    /// (the host is expected to drain promptly).
    pub event_tx: mpsc::Sender<TriggerEvent>,

    /// Optional bearer token. When `Some`, requests without a matching
    /// `Authorization: Bearer <token>` header are rejected with `401`.
    pub auth_token: Option<Arc<String>>,
}

/// Build the axum [`Router`] used by the webhook server.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/", post(root_handler))
        .route("/webhooks/:segment", post(typed_handler))
        .with_state(state)
}

/// Bind a TCP listener to `addr` and return both the listener and the
/// resolved [`SocketAddr`] (useful when callers pass `:0` to let the OS
/// pick a port).
pub async fn bind(addr: SocketAddr) -> anyhow::Result<(TcpListener, SocketAddr)> {
    let listener = TcpListener::bind(addr).await?;
    let local = listener.local_addr()?;
    Ok((listener, local))
}

/// Run the axum server on `listener` until `shutdown` resolves.
pub async fn serve(
    listener: TcpListener,
    state: AppState,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()> {
    let app = router(state);
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await?;
    Ok(())
}

async fn root_handler(State(state): State<AppState>, request: Request<Body>) -> impl IntoResponse {
    handle(state, "webhook".to_string(), request).await
}

async fn typed_handler(
    State(state): State<AppState>,
    Path(segment): Path<String>,
    request: Request<Body>,
) -> impl IntoResponse {
    let segment = sanitize_segment(&segment);
    let kind = if segment.is_empty() {
        "webhook".to_string()
    } else {
        format!("webhook.{segment}")
    };
    handle(state, kind, request).await
}

async fn handle(state: AppState, kind: String, request: Request<Body>) -> impl IntoResponse {
    let (parts, body) = request.into_parts();
    let headers = parts.headers;

    if let Err(status) = check_auth(&state, &headers) {
        return status;
    }

    let bytes = match to_bytes(body, MAX_BODY_BYTES).await {
        Ok(bytes) => bytes,
        Err(_) => return StatusCode::PAYLOAD_TOO_LARGE,
    };

    let payload = parse_payload(&bytes);
    let subject_id = header_str(&headers, HEADER_SUBJECT_ID);
    let action_hint = header_str(&headers, HEADER_ACTION_HINT);

    let event = TriggerEvent {
        id: Uuid::new_v4().to_string(),
        occurred_at: Utc::now(),
        kind,
        payload,
        subject_id,
        action_hint,
    };

    // Best-effort send. If the channel is full, drop the event — the host is
    // expected to drain promptly and we'd rather fail-fast than block the
    // axum worker. Errors here are not retryable from inside the handler.
    if state.event_tx.try_send(event).is_err() {
        tracing::warn!("event channel full or closed; dropping inbound webhook");
        return StatusCode::SERVICE_UNAVAILABLE;
    }

    StatusCode::ACCEPTED
}

fn check_auth(state: &AppState, headers: &HeaderMap) -> Result<(), StatusCode> {
    let Some(expected) = state.auth_token.as_ref() else {
        return Ok(());
    };
    let Some(raw) = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
    else {
        return Err(StatusCode::UNAUTHORIZED);
    };
    let presented = raw.strip_prefix("Bearer ").unwrap_or("");
    if presented == expected.as_str() {
        Ok(())
    } else {
        Err(StatusCode::UNAUTHORIZED)
    }
}

fn parse_payload(bytes: &[u8]) -> Value {
    if bytes.is_empty() {
        return Value::Object(serde_json::Map::new());
    }
    match serde_json::from_slice::<Value>(bytes) {
        Ok(value) => value,
        Err(_) => {
            let raw = String::from_utf8_lossy(bytes).into_owned();
            json!({ "raw": raw })
        }
    }
}

fn header_str(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|h| h.to_str().ok())
        .map(|s| s.to_string())
        .filter(|s| !s.is_empty())
}

/// Conservative path-segment sanitizer. Drops anything that isn't an ASCII
/// alphanumeric, dot, dash, or underscore so the resulting `kind` string is
/// safe to log and YAML-match against without exotic escaping.
fn sanitize_segment(raw: &str) -> String {
    raw.chars()
        .filter(|c| c.is_ascii_alphanumeric() || matches!(*c, '.' | '-' | '_'))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_payload_handles_json() {
        let v = parse_payload(br#"{"a":1}"#);
        assert_eq!(v, json!({"a": 1}));
    }

    #[test]
    fn parse_payload_wraps_non_json_bytes() {
        let v = parse_payload(b"not json");
        assert_eq!(v, json!({"raw": "not json"}));
    }

    #[test]
    fn parse_payload_handles_empty() {
        let v = parse_payload(b"");
        assert_eq!(v, json!({}));
    }

    #[test]
    fn sanitize_segment_strips_unsafe_chars() {
        assert_eq!(sanitize_segment("github.push"), "github.push");
        assert_eq!(sanitize_segment("path/with/slashes"), "pathwithslashes");
        assert_eq!(sanitize_segment("good_name-1"), "good_name-1");
    }
}
