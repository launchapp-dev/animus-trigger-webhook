//! Contract tests for the webhook `TriggerBackend` implementation.
//!
//! Each test spins up a `WebhookBackend` bound to `127.0.0.1:0` (so the OS
//! picks a free port), drives `watch()` to start the HTTP listener, makes
//! real HTTP requests with `reqwest`, and drains the resulting trigger
//! stream with a timeout to assert on the emitted events.

use std::net::SocketAddr;
use std::time::Duration;

use animus_plugin_protocol::HealthStatus;
use animus_trigger_protocol::{TriggerBackend, TriggerEvent};
use animus_trigger_webhook::backend::WebhookBackend;
use animus_trigger_webhook::config::WebhookConfig;
use animus_trigger_webhook::server;
use futures::StreamExt;
use tokio::net::TcpListener;

const RECV_TIMEOUT: Duration = Duration::from_secs(2);

/// Bind a TCP listener to `127.0.0.1:0`, return the resolved addr, and drop
/// the listener immediately so the port is free for the backend to re-bind.
/// Race-y in theory; fine in practice for these tests because nothing else
/// is racing for the port on a developer machine or CI VM.
async fn pick_free_port() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral");
    let addr = listener.local_addr().expect("local_addr");
    drop(listener);
    addr
}

async fn start_backend(
    config: WebhookConfig,
) -> (WebhookBackend, animus_trigger_protocol::TriggerStream) {
    let backend = WebhookBackend::new(config);
    let stream = backend.watch().await.expect("watch should succeed");
    // Give axum::serve a moment to actually accept the bound socket.
    tokio::time::sleep(Duration::from_millis(50)).await;
    (backend, stream)
}

async fn next_event(stream: &mut animus_trigger_protocol::TriggerStream) -> TriggerEvent {
    let item = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("timed out waiting for trigger event")
        .expect("stream ended unexpectedly")
        .expect("backend yielded an error instead of an event");
    item
}

#[tokio::test]
async fn schema_advertises_webhook_kind() {
    let addr = pick_free_port().await;
    let backend = WebhookBackend::new(WebhookConfig::for_testing(addr));
    let schema = backend.schema();
    assert_eq!(schema.kinds, vec!["webhook".to_string()]);
    assert!(!schema.supports_resume);
    assert!(!schema.supports_dedup);
    assert!(!schema.supports_ack);
}

#[tokio::test]
async fn accepts_json_post_and_emits_event() {
    let addr = pick_free_port().await;
    let (_backend, mut stream) = start_backend(WebhookConfig::for_testing(addr)).await;

    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://{addr}/"))
        .json(&serde_json::json!({"hello": "world"}))
        .send()
        .await
        .expect("POST should succeed");
    assert_eq!(response.status(), reqwest::StatusCode::ACCEPTED);

    let event = next_event(&mut stream).await;
    assert_eq!(event.kind, "webhook");
    assert_eq!(event.payload, serde_json::json!({"hello": "world"}));
    assert!(event.subject_id.is_none());
    assert!(event.action_hint.is_none());
    assert!(!event.id.is_empty());
}

#[tokio::test]
async fn typed_route_emits_namespaced_kind() {
    let addr = pick_free_port().await;
    let (_backend, mut stream) = start_backend(WebhookConfig::for_testing(addr)).await;

    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://{addr}/webhooks/github.push"))
        .json(&serde_json::json!({"ref": "refs/heads/main"}))
        .send()
        .await
        .expect("POST should succeed");
    assert_eq!(response.status(), reqwest::StatusCode::ACCEPTED);

    let event = next_event(&mut stream).await;
    assert_eq!(event.kind, "webhook.github.push");
    assert_eq!(event.payload["ref"], "refs/heads/main");
}

#[tokio::test]
async fn rejects_post_without_bearer_when_auth_configured() {
    let addr = pick_free_port().await;
    let config = WebhookConfig::for_testing_with_auth(addr, "s3cret");
    let (_backend, _stream) = start_backend(config).await;

    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://{addr}/"))
        .json(&serde_json::json!({"a": 1}))
        .send()
        .await
        .expect("POST should connect");
    assert_eq!(response.status(), reqwest::StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn accepts_post_with_correct_bearer() {
    let addr = pick_free_port().await;
    let config = WebhookConfig::for_testing_with_auth(addr, "s3cret");
    let (_backend, mut stream) = start_backend(config).await;

    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://{addr}/"))
        .bearer_auth("s3cret")
        .json(&serde_json::json!({"a": 1}))
        .send()
        .await
        .expect("POST should connect");
    assert_eq!(response.status(), reqwest::StatusCode::ACCEPTED);

    let event = next_event(&mut stream).await;
    assert_eq!(event.payload["a"], 1);
}

#[tokio::test]
async fn rejects_post_with_wrong_bearer() {
    let addr = pick_free_port().await;
    let config = WebhookConfig::for_testing_with_auth(addr, "s3cret");
    let (_backend, _stream) = start_backend(config).await;

    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://{addr}/"))
        .bearer_auth("wrong")
        .json(&serde_json::json!({"a": 1}))
        .send()
        .await
        .expect("POST should connect");
    assert_eq!(response.status(), reqwest::StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn extracts_subject_id_and_action_hint_from_headers() {
    let addr = pick_free_port().await;
    let (_backend, mut stream) = start_backend(WebhookConfig::for_testing(addr)).await;

    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://{addr}/"))
        .header(server::HEADER_SUBJECT_ID, "linear:ENG-1")
        .header(server::HEADER_ACTION_HINT, "run-workflow:review")
        .json(&serde_json::json!({"payload": "thing"}))
        .send()
        .await
        .expect("POST should succeed");
    assert_eq!(response.status(), reqwest::StatusCode::ACCEPTED);

    let event = next_event(&mut stream).await;
    assert_eq!(event.subject_id.as_deref(), Some("linear:ENG-1"));
    assert_eq!(event.action_hint.as_deref(), Some("run-workflow:review"));
}

#[tokio::test]
async fn non_json_body_wrapped_as_raw() {
    let addr = pick_free_port().await;
    let (_backend, mut stream) = start_backend(WebhookConfig::for_testing(addr)).await;

    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://{addr}/"))
        .header("content-type", "text/plain")
        .body("not-a-json-body")
        .send()
        .await
        .expect("POST should succeed");
    assert_eq!(response.status(), reqwest::StatusCode::ACCEPTED);

    let event = next_event(&mut stream).await;
    assert_eq!(event.payload, serde_json::json!({"raw": "not-a-json-body"}));
}

#[tokio::test]
async fn health_healthy_when_server_running() {
    let addr = pick_free_port().await;
    let (backend, _stream) = start_backend(WebhookConfig::for_testing(addr)).await;
    let health = backend.health().await.expect("health");
    assert_eq!(health.status, HealthStatus::Healthy);
}

#[tokio::test]
async fn watch_twice_returns_unavailable() {
    let addr = pick_free_port().await;
    let backend = WebhookBackend::new(WebhookConfig::for_testing(addr));
    let _first = backend.watch().await.expect("first watch ok");
    let second = backend.watch().await;
    assert!(matches!(
        second,
        Err(animus_trigger_protocol::BackendError::Unavailable(_))
    ));
}
