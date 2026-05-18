# animus-trigger-webhook

Generic HTTP webhook receiver — the first reference [`TriggerBackend`](https://github.com/launchapp-dev/animus-protocol) plugin for [Animus](https://github.com/launchapp-dev/animus-cli).

## What this is

`animus-trigger-webhook` opens a single HTTP port, accepts POST requests, and emits one [`TriggerEvent`](https://github.com/launchapp-dev/animus-protocol/blob/main/animus-trigger-protocol/src/lib.rs) per request. The Animus daemon consumes that stream and turns each event into work (queueing a workflow, creating a task, kicking off a review, ...).

Use it as a generic shim in front of anything that can POST JSON:

- GitHub webhooks
- Linear webhooks
- Stripe events
- a `curl` from a cron job, a CI step, or an ops runbook
- a sibling daemon you want to wire into Animus without writing a dedicated plugin

For source-specific signature verification, idempotency, and rich routing — write a dedicated trigger plugin. This is the lowest-common-denominator HTTP fan-in.

## Endpoints

| Route | Behavior |
|---|---|
| `POST /` | Emits an event with `kind = "webhook"`. |
| `POST /webhooks/<segment>` | Emits an event with `kind = "webhook.<segment>"`. |

The request body is parsed as JSON when possible and stored in `event.payload`. If the body is not valid JSON, it is wrapped as `{"raw": "<body>"}`.

The server responds `202 Accepted` once the event is queued. Responses do not wait for downstream processing.

### Optional headers

| Header | Effect |
|---|---|
| `Authorization: Bearer <token>` | Required if `ANIMUS_WEBHOOK_AUTH_TOKEN` is set. Returns `401` on mismatch. |
| `X-Animus-Subject-Id: <id>` | Populates `event.subject_id` (e.g. `linear:ENG-1`). |
| `X-Animus-Action-Hint: <hint>` | Populates `event.action_hint` (e.g. `run-workflow:review`). |

## Configuration

| Env var | Default | Purpose |
|---|---|---|
| `ANIMUS_WEBHOOK_LISTEN_ADDR` | `127.0.0.1:7878` | `ip:port` for the listener. |
| `ANIMUS_WEBHOOK_AUTH_TOKEN` | _unset_ | Optional bearer token. Auth is disabled when unset. |
| `ANIMUS_WEBHOOK_CHANNEL_BUFFER` | `256` | In-memory event channel buffer. Events overflowing the buffer are dropped — the host applies its own backpressure. |
| `RUST_LOG` | _unset_ | Standard `tracing-subscriber` env filter. |

## Install (planned)

```bash
animus plugin install animus-trigger-webhook
```

## Workflow YAML usage (sketch)

```yaml
triggers:
  webhook:
    backend: animus-trigger-webhook
    env:
      ANIMUS_WEBHOOK_LISTEN_ADDR: "0.0.0.0:7878"
      ANIMUS_WEBHOOK_AUTH_TOKEN: "${WEBHOOK_TOKEN}"
    on:
      kind: webhook
      enqueue: review
```

## Smoke test

```bash
cargo build --release
ANIMUS_WEBHOOK_LISTEN_ADDR=127.0.0.1:7878 \
  ./target/release/animus-trigger-webhook \
  --manifest
```

Manifest output is a single line of JSON describing the plugin (name, version, kind, capabilities, protocol version).

To exercise the wire protocol end-to-end, pipe a JSON-RPC frame for `trigger/watch` into the binary and POST to the listener; events arrive as `trigger/event` notifications on stdout. The contract tests in `tests/contract.rs` cover the in-process path.

## Design

- **Protocol:** [`animus-trigger-protocol`](https://github.com/launchapp-dev/animus-protocol/tree/main/animus-trigger-protocol)
- **Wire layer:** `trigger_backend_main` from [`animus-plugin-runtime`](https://github.com/launchapp-dev/animus-protocol/tree/main/animus-plugin-runtime)
- **Naming:** repo, crate, and binary all named `animus-trigger-webhook`
- **Single-watcher constraint:** the in-memory event channel has a single receiver; calling `trigger/watch` twice on the same process returns `Unavailable`. Animus only watches each plugin once per lifecycle.

## License

MIT — see [LICENSE](LICENSE).
