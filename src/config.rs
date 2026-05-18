//! Environment-driven configuration for the webhook trigger backend.

use std::net::SocketAddr;

use anyhow::{Context, Result};

/// Default listener address used when `ANIMUS_WEBHOOK_LISTEN_ADDR` is unset.
pub const DEFAULT_LISTEN_ADDR: &str = "127.0.0.1:7878";

/// Default in-memory event channel buffer.
pub const DEFAULT_CHANNEL_BUFFER: usize = 256;

/// Runtime configuration loaded from environment variables.
#[derive(Debug, Clone)]
pub struct WebhookConfig {
    /// `ip:port` the axum HTTP server binds to.
    pub listen_addr: SocketAddr,

    /// Optional bearer token. When `Some`, every POST must carry
    /// `Authorization: Bearer <token>` or the server returns `401`.
    pub auth_token: Option<String>,

    /// Capacity of the in-memory `tokio::sync::mpsc` channel that buffers
    /// events between the HTTP handlers and the watch stream consumer.
    /// Events that overflow the buffer are dropped — the host is expected
    /// to drain the watch stream promptly.
    pub channel_buffer: usize,
}

impl WebhookConfig {
    /// Load configuration from environment variables.
    ///
    /// All env vars are optional: a default config produces a localhost
    /// listener on `127.0.0.1:7878` with no auth and a 256-event buffer.
    pub fn from_env() -> Result<Self> {
        let listen_addr_str = std::env::var("ANIMUS_WEBHOOK_LISTEN_ADDR")
            .unwrap_or_else(|_| DEFAULT_LISTEN_ADDR.to_string());
        let listen_addr: SocketAddr = listen_addr_str
            .parse()
            .with_context(|| format!("invalid ANIMUS_WEBHOOK_LISTEN_ADDR: {listen_addr_str:?}"))?;

        let auth_token = std::env::var("ANIMUS_WEBHOOK_AUTH_TOKEN")
            .ok()
            .filter(|s| !s.is_empty());

        let channel_buffer = match std::env::var("ANIMUS_WEBHOOK_CHANNEL_BUFFER") {
            Ok(raw) => raw
                .parse::<usize>()
                .with_context(|| format!("invalid ANIMUS_WEBHOOK_CHANNEL_BUFFER: {raw:?}"))?
                .max(1),
            Err(_) => DEFAULT_CHANNEL_BUFFER,
        };

        Ok(Self {
            listen_addr,
            auth_token,
            channel_buffer,
        })
    }

    /// Helper for integration tests / embedders that want to construct a
    /// config without going through env vars.
    pub fn for_testing(listen_addr: SocketAddr) -> Self {
        Self {
            listen_addr,
            auth_token: None,
            channel_buffer: 32,
        }
    }

    /// Variant of [`Self::for_testing`] that pre-configures bearer auth.
    pub fn for_testing_with_auth(listen_addr: SocketAddr, token: impl Into<String>) -> Self {
        Self {
            listen_addr,
            auth_token: Some(token.into()),
            channel_buffer: 32,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_when_env_unset() {
        // Pre-clear in case something leaked in from the harness.
        std::env::remove_var("ANIMUS_WEBHOOK_LISTEN_ADDR");
        std::env::remove_var("ANIMUS_WEBHOOK_AUTH_TOKEN");
        std::env::remove_var("ANIMUS_WEBHOOK_CHANNEL_BUFFER");
        let config = WebhookConfig::from_env().expect("default config should parse");
        assert_eq!(config.listen_addr.to_string(), DEFAULT_LISTEN_ADDR);
        assert_eq!(config.auth_token, None);
        assert_eq!(config.channel_buffer, DEFAULT_CHANNEL_BUFFER);
    }
}
