//! [`WebhookBackend`] - the `TriggerBackend` implementation.
//!
//! `WebhookBackend::watch` is the single entrypoint the runtime calls. It
//! binds the axum HTTP server (if not already bound), takes ownership of the
//! event-channel receiver, and returns a `TriggerStream` that drains that
//! receiver. The server runs in a `tokio::spawn`ed task until `shutdown_tx`
//! fires (on drop or future cancellation).

use std::sync::Arc;

use animus_plugin_protocol::{HealthCheckResult, HealthStatus};
use animus_trigger_protocol::{
    BackendError, TriggerBackend, TriggerEvent, TriggerSchema, TriggerStream,
};
use async_trait::async_trait;
use futures::stream::unfold;
use futures::StreamExt;
use tokio::sync::{mpsc, oneshot, Mutex};

use crate::config::WebhookConfig;
use crate::server::{self, AppState};

/// Trigger kind emitted for the catch-all `POST /` route.
pub const KIND_DEFAULT: &str = "webhook";

/// Single-watcher webhook trigger backend.
pub struct WebhookBackend {
    config: WebhookConfig,
    event_rx: Mutex<Option<mpsc::Receiver<TriggerEvent>>>,
    event_tx: mpsc::Sender<TriggerEvent>,
    shutdown_tx: Mutex<Option<oneshot::Sender<()>>>,
    server_started: Mutex<bool>,
}

impl WebhookBackend {
    /// Construct a backend from the supplied configuration. Does NOT bind
    /// the listener — that happens lazily inside [`Self::watch`] so that
    /// `--manifest` invocations and credential-free lifecycle calls don't
    /// require an open port.
    pub fn new(config: WebhookConfig) -> Self {
        let (event_tx, event_rx) = mpsc::channel::<TriggerEvent>(config.channel_buffer);
        Self {
            config,
            event_rx: Mutex::new(Some(event_rx)),
            event_tx,
            shutdown_tx: Mutex::new(None),
            server_started: Mutex::new(false),
        }
    }

    /// Borrow the configuration this backend was built with.
    pub fn config(&self) -> &WebhookConfig {
        &self.config
    }

    /// Clone of the event-channel sender. Exposed for in-process tests that
    /// want to push synthetic events without going through HTTP.
    #[doc(hidden)]
    pub fn event_sender(&self) -> mpsc::Sender<TriggerEvent> {
        self.event_tx.clone()
    }
}

#[async_trait]
impl TriggerBackend for WebhookBackend {
    fn schema(&self) -> TriggerSchema {
        TriggerSchema {
            kinds: vec![KIND_DEFAULT.to_string()],
            // The HTTP listener has no persistent journal. Restarting the
            // plugin re-binds the port; in-flight POSTs in the channel are
            // lost. Dedup and ack are therefore meaningless here.
            supports_resume: false,
            supports_dedup: false,
            supports_ack: false,
        }
    }

    async fn watch(&self) -> Result<TriggerStream, BackendError> {
        let receiver = {
            let mut guard = self.event_rx.lock().await;
            guard.take()
        };
        let receiver = receiver.ok_or_else(|| {
            BackendError::Unavailable(
                "watch already called; webhook backend supports a single watcher".into(),
            )
        })?;

        // Lazy server start so that --manifest and health calls work without
        // binding the port.
        {
            let mut started = self.server_started.lock().await;
            if !*started {
                let auth_token = self.config.auth_token.clone().map(Arc::new);
                let state = AppState {
                    event_tx: self.event_tx.clone(),
                    auth_token,
                };
                let (listener, local_addr) =
                    server::bind(self.config.listen_addr)
                        .await
                        .map_err(|error| {
                            BackendError::Unavailable(format!(
                                "failed to bind {}: {error}",
                                self.config.listen_addr
                            ))
                        })?;
                tracing::info!(addr = %local_addr, "animus-trigger-webhook listening");

                let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
                *self.shutdown_tx.lock().await = Some(shutdown_tx);

                tokio::spawn(async move {
                    let shutdown = async {
                        let _ = shutdown_rx.await;
                    };
                    if let Err(error) = server::serve(listener, state, shutdown).await {
                        tracing::error!(?error, "axum server exited with error");
                    }
                });

                *started = true;
            }
        }

        let stream = unfold(receiver, |mut rx| async move {
            rx.recv().await.map(|event| (Ok(event), rx))
        })
        .boxed();
        Ok(stream)
    }

    async fn ack(&self, _event_id: &str) -> Result<(), BackendError> {
        // Fire-and-forget — schema advertises supports_ack = false.
        Ok(())
    }

    async fn health(&self) -> Result<HealthCheckResult, BackendError> {
        let started = *self.server_started.lock().await;
        let channel_open = !self.event_tx.is_closed();
        let status = if !started {
            // Pre-watch state: config loaded, no listener yet. Considered
            // healthy because the runtime polls health before `watch`.
            HealthStatus::Healthy
        } else if channel_open {
            HealthStatus::Healthy
        } else {
            HealthStatus::Unhealthy
        };
        Ok(HealthCheckResult {
            status,
            uptime_ms: None,
            memory_usage_bytes: None,
            last_error: None,
        })
    }
}

impl Drop for WebhookBackend {
    fn drop(&mut self) {
        if let Ok(mut guard) = self.shutdown_tx.try_lock() {
            if let Some(tx) = guard.take() {
                let _ = tx.send(());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_advertises_webhook_kind() {
        let backend =
            WebhookBackend::new(WebhookConfig::for_testing("127.0.0.1:0".parse().unwrap()));
        let schema = backend.schema();
        assert_eq!(schema.kinds, vec!["webhook".to_string()]);
        assert!(!schema.supports_resume);
        assert!(!schema.supports_dedup);
        assert!(!schema.supports_ack);
    }

    #[tokio::test]
    async fn ack_is_noop() {
        let backend =
            WebhookBackend::new(WebhookConfig::for_testing("127.0.0.1:0".parse().unwrap()));
        backend.ack("anything").await.expect("ack is a no-op");
    }

    #[tokio::test]
    async fn health_is_healthy_before_watch() {
        let backend =
            WebhookBackend::new(WebhookConfig::for_testing("127.0.0.1:0".parse().unwrap()));
        let health = backend.health().await.expect("health");
        assert_eq!(health.status, HealthStatus::Healthy);
    }
}
