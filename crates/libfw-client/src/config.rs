//! Client configuration, parsed from the JS-side options object.
//!
//! All knobs are optional; defaults follow the protocol constants from
//! `libfw-core` (2 MiB chunks, 4 concurrent connections, 3 retries).

use js_sys::Reflect;
use wasm_bindgen::prelude::*;

use libfw_core::{CHUNK_SIZE, DEFAULT_CONCURRENCY, DEFAULT_UPLOAD_WINDOW, MAX_RETRIES};

/// Default delay before the first retry (milliseconds).
pub const DEFAULT_BASE_RETRY_MS: u32 = 500;
/// Upper bound for exponential backoff (milliseconds).
pub const DEFAULT_MAX_RETRY_MS: u32 = 30_000;
/// Default per-request timeout (milliseconds).
pub const DEFAULT_TIMEOUT_MS: u32 = 60_000;

/// Runtime configuration of the WASM engine.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// Maximum number of concurrently-transferring files (default 4).
    pub concurrency: usize,
    /// In-flight chunk window for a single file's upload (default 8).
    ///
    /// Independent of `concurrency`: one file keeps up to `upload_window`
    /// chunks in flight, so a high-latency link stays saturated (throughput
    /// is bounded by bandwidth, not `chunk_size / RTT`).
    pub upload_window: usize,
    /// Request `zrip` compression from the server / compress uploads.
    pub compress: bool,
    /// Fixed chunk size used to slice files (default 2 MiB).
    pub chunk_size: u64,
    /// Maximum retries per chunk/file (default 3).
    pub max_retries: u32,
    /// Initial backoff delay in ms (default 500).
    pub base_retry_delay_ms: u32,
    /// Backoff ceiling in ms (default 30s).
    pub max_retry_delay_ms: u32,
    /// Per-request timeout in ms (default 60s).
    pub timeout_ms: u32,
}

impl Default for ClientConfig {
    fn default() -> Self {
        ClientConfig {
            concurrency: DEFAULT_CONCURRENCY,
            upload_window: DEFAULT_UPLOAD_WINDOW,
            compress: true,
            chunk_size: CHUNK_SIZE,
            max_retries: MAX_RETRIES,
            base_retry_delay_ms: DEFAULT_BASE_RETRY_MS,
            max_retry_delay_ms: DEFAULT_MAX_RETRY_MS,
            timeout_ms: DEFAULT_TIMEOUT_MS,
        }
    }
}

/// Read an optional `u64`/`number` field from a JS object.
fn opt_u64(obj: &JsValue, key: &str) -> Option<u64> {
    Reflect::get(obj, &JsValue::from_str(key))
        .ok()
        .and_then(|v| v.as_f64())
        .map(|f| f as u64)
}

/// Read an optional `usize` field from a JS object.
fn opt_usize(obj: &JsValue, key: &str) -> Option<usize> {
    Reflect::get(obj, &JsValue::from_str(key))
        .ok()
        .and_then(|v| v.as_f64())
        .map(|f| f as usize)
}

/// Read an optional `u32` field from a JS object.
fn opt_u32(obj: &JsValue, key: &str) -> Option<u32> {
    Reflect::get(obj, &JsValue::from_str(key))
        .ok()
        .and_then(|v| v.as_f64())
        .map(|f| f as u32)
}

/// Read an optional boolean field from a JS object.
fn opt_bool(obj: &JsValue, key: &str) -> Option<bool> {
    Reflect::get(obj, &JsValue::from_str(key))
        .ok()
        .and_then(|v| v.as_bool())
}

impl ClientConfig {
    /// Parse configuration from a JS object literal, e.g.
    /// `{ concurrency: 4, compress: true, chunkSize: 2097152 }`.
    ///
    /// Safe to call with `null`/`undefined` (returns defaults) — this also
    /// keeps native (non-wasm) unit tests runnable.
    pub fn from_js(opts: &JsValue) -> ClientConfig {
        let mut cfg = ClientConfig::default();
        if !opts.is_object() {
            return cfg;
        }
        if let Some(v) = opt_usize(opts, "concurrency") {
            if v > 0 {
                cfg.concurrency = v;
            }
        }
        if let Some(v) = opt_usize(opts, "uploadWindow") {
            if v > 0 {
                cfg.upload_window = v;
            }
        }
        if let Some(v) = opt_bool(opts, "compress") {
            cfg.compress = v;
        }
        if let Some(v) = opt_u64(opts, "chunkSize") {
            if v > 0 {
                cfg.chunk_size = v;
            }
        }
        if let Some(v) = opt_u32(opts, "maxRetries") {
            cfg.max_retries = v;
        }
        if let Some(v) = opt_u32(opts, "baseRetryDelayMs") {
            cfg.base_retry_delay_ms = v;
        }
        if let Some(v) = opt_u32(opts, "maxRetryDelayMs") {
            cfg.max_retry_delay_ms = v;
        }
        if let Some(v) = opt_u32(opts, "timeoutMs") {
            cfg.timeout_ms = v;
        }
        cfg
    }

    /// Exponential backoff delay for a failed attempt (0-based).
    ///
    /// `2^attempt * base`, clamped to `max`. Bounded so a hung peer cannot
    /// grow the delay without limit.
    pub fn backoff_ms(&self, attempt: u32) -> u32 {
        backoff_ms(attempt, self.base_retry_delay_ms, self.max_retry_delay_ms)
    }
}

/// Pure exponential backoff: `min(max, base << attempt)` with saturation.
pub fn backoff_ms(attempt: u32, base: u32, max: u32) -> u32 {
    if attempt == 0 {
        return base.min(max);
    }
    let shift = attempt.min(31);
    let doubled = (base as u64) << shift;
    doubled.min(max as u64) as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_sane() {
        let cfg = ClientConfig::default();
        assert_eq!(cfg.concurrency, 4);
        assert_eq!(cfg.upload_window, DEFAULT_UPLOAD_WINDOW);
        assert_eq!(cfg.chunk_size, CHUNK_SIZE);
        assert_eq!(cfg.max_retries, MAX_RETRIES);
        assert!(cfg.compress);
    }

    #[test]
    fn backoff_grows_exponentially_then_clamps() {
        let cfg = ClientConfig {
            base_retry_delay_ms: 100,
            max_retry_delay_ms: 1000,
            ..ClientConfig::default()
        };
        assert_eq!(cfg.backoff_ms(0), 100);
        assert_eq!(cfg.backoff_ms(1), 200);
        assert_eq!(cfg.backoff_ms(2), 400);
        assert_eq!(cfg.backoff_ms(3), 800);
        assert_eq!(cfg.backoff_ms(4), 1000); // clamped
        assert_eq!(cfg.backoff_ms(99), 1000);
    }

    #[test]
    fn pure_backoff_saturates() {
        assert_eq!(backoff_ms(0, 500, 30_000), 500);
        assert_eq!(backoff_ms(6, 500, 30_000), 30_000);
    }

    #[test]
    #[cfg(target_arch = "wasm32")]
    fn parses_js_options() {
        let obj = js_sys::Object::new();
        js_sys::Reflect::set(&obj, &JsValue::from_str("concurrency"), &JsValue::from_f64(8.0))
            .unwrap();
        js_sys::Reflect::set(
            &obj,
            &JsValue::from_str("uploadWindow"),
            &JsValue::from_f64(16.0),
        )
        .unwrap();
        js_sys::Reflect::set(&obj, &JsValue::from_str("compress"), &JsValue::FALSE).unwrap();
        js_sys::Reflect::set(
            &obj,
            &JsValue::from_str("chunkSize"),
            &JsValue::from_f64(1024.0),
        )
        .unwrap();
        let cfg = ClientConfig::from_js(&obj);
        assert_eq!(cfg.concurrency, 8);
        assert_eq!(cfg.upload_window, 16);
        assert!(!cfg.compress);
        assert_eq!(cfg.chunk_size, 1024);
    }

    #[test]
    #[cfg(target_arch = "wasm32")]
    fn ignores_non_object_options() {
        let cfg = ClientConfig::from_js(&JsValue::NULL);
        assert_eq!(cfg.concurrency, DEFAULT_CONCURRENCY);
        let cfg = ClientConfig::from_js(&JsValue::UNDEFINED);
        assert_eq!(cfg.chunk_size, CHUNK_SIZE);
    }
}
