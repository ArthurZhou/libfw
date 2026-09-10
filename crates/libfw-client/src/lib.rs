//! libfw-client: reusable client-side transfer logic and browser glue.
//!
//! The crate is intentionally split into a reusable Rust library core and a
//! thin `wasm` bridge. This allows the client logic to be consumed by Rust
//! code directly, while the JS/browser-facing API remains in the dedicated
//! WASM module.
//!
//! # Two client forms, one protocol
//!
//! * **Native Rust** ([`native::NativeClient`], any non-`wasm32` target):
//!   an async `tokio` + `reqwest` transport implementing the same wire
//!   protocol — resumable `Range` downloads, the session upload protocol,
//!   zrip compression and the same adaptive tuning engine. Enable it by
//!   depending on this crate from a normal Rust binary/library.
//! * **Browser/WASM** ([`LibfwClient`], `wasm32` only): the engine behind the
//!   `sdk/` npm package, which additionally supports the File System Access
//!   API and IndexedDB resume state.
#![allow(dead_code)]
#![recursion_limit = "512"]

mod config;
mod download;
mod error;
mod http;
mod js;
mod plan;
mod state;
mod tune;
mod upload;

pub use config::{backoff_ms, ClientConfig};
pub use error::LibfwError;
pub use plan::FileEntry;
pub use tune::{
    CompressLevel, TuneEvent, TuneHandle, TuneParams, TunePhase, TuneStats, TransferKind,
};
// Re-exported so consumers can name the capability/limit types returned by
// `native::NativeClient::{capabilities, fetch_capabilities}` without having to
// depend on `libfw-core` directly.
pub use libfw_core::{Capabilities, CompressionFormat, IntRange, Limits, ZripLevels};

/// Native (non-WASM) Rust client: async `reqwest` transport speaking the same
/// wire protocol as the browser engine.
#[cfg(not(target_arch = "wasm32"))]
pub mod native;

#[cfg(target_arch = "wasm32")]
pub mod wasm;
#[cfg(target_arch = "wasm32")]
pub use wasm::{js_option_string, LibfwClient};
