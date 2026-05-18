//! Library surface for the `animus-trigger-webhook` plugin.
//!
//! The binary entrypoint lives in `src/main.rs`. The modules below are
//! exposed so integration tests (and downstream embedders that want to wire
//! the webhook backend without spawning a subprocess) can reach the
//! `TriggerBackend` implementation directly.

pub mod backend;
pub mod config;
pub mod server;
