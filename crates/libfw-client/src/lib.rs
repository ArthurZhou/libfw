//! libfw-client: reusable client-side transfer logic and browser glue.
//!
//! The crate is intentionally split into a reusable Rust library core and a
//! thin `wasm` bridge. This allows the client logic to be consumed by Rust
//! code directly, while the JS/browser-facing API remains in the dedicated
//! WASM module.
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
    CompressLevel, LocalStore, TuneCache, TuneEvent, TuneHandle, TuneParams, TunePhase,
    TuneStats, TuneStore, TransferKind, tune_key,
};

#[cfg(target_arch = "wasm32")]
pub mod wasm;
#[cfg(target_arch = "wasm32")]
pub use wasm::{js_option_string, LibfwClient};
