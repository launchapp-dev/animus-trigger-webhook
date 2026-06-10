use animus_plugin_protocol::{PluginInfo, PLUGIN_KIND_TRIGGER_BACKEND};
use animus_plugin_runtime::trigger_backend_main;
use animus_trigger_webhook::backend::WebhookBackend;
use animus_trigger_webhook::config::WebhookConfig;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    emit_manifest_if_requested();

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();

    let config = WebhookConfig::from_env()?;
    let backend = WebhookBackend::new(config);

    let info = PluginInfo {
        name: env!("CARGO_PKG_NAME").into(),
        version: env!("CARGO_PKG_VERSION").into(),
        plugin_kind: PLUGIN_KIND_TRIGGER_BACKEND.into(),
        description: Some(env!("CARGO_PKG_DESCRIPTION").into()),
    };

    trigger_backend_main(info, backend).await
}

fn emit_manifest_if_requested() {
    if !std::env::args()
        .skip(1)
        .any(|arg| arg == "--manifest" || arg == "-m")
    {
        return;
    }

    let manifest = serde_json::json!({
        "name": env!("CARGO_PKG_NAME"),
        "version": env!("CARGO_PKG_VERSION"),
        "plugin_kind": "trigger_backend",
        "description": env!("CARGO_PKG_DESCRIPTION"),
        "protocol_version": animus_plugin_protocol::PROTOCOL_VERSION,
        "capabilities": [
            "trigger/watch",
            "trigger/schema",
            "health/check"
        ],
        "env_required": [
            {
                "name": "ANIMUS_WEBHOOK_LISTEN_ADDR",
                "description": "IP address and port for the HTTP listener.",
                "required": false
            },
            {
                "name": "ANIMUS_WEBHOOK_AUTH_TOKEN",
                "description": "Bearer token for incoming POST authentication.",
                "sensitive": true,
                "required": false
            },
            {
                "name": "ANIMUS_WEBHOOK_CHANNEL_BUFFER",
                "description": "In-memory event channel buffer size.",
                "required": false
            }
        ]
    });
    println!(
        "{}",
        serde_json::to_string(&manifest).expect("serialize manifest")
    );
    std::process::exit(0);
}
