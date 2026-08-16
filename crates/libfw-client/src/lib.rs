//! libfw-client: WASM engine for browser file & folder transfers.
//!
//! This crate ships the *engine* that runs inside the browser: it performs
//! the HTTP transfer (via `fetch`), slices files into chunks, keeps memory
//! constant, retries with exponential backoff, and drives the task state
//! machine (`idle → downloading/uploading → paused → resumed →
//! completed/failed`).
#![recursion_limit = "512"]
//!
//! The [`LibfwClient`] WASM class is exported through `wasm-bindgen` and is
//! intended to be wrapped by the accompanying JS SDK (`sdk/`). The SDK owns
//! WASM instantiation, the File System Access API, IndexedDB persistence and
//! the `createWritable` byte sink — all data crosses the boundary through
//! the callbacks installed via [`LibfwClient::set_callbacks`].
//!
//! # Callbacks object
//!
//! ```js
//! engine.set_callbacks({
//!   onFileStart(path, size) {},
//!   onWriteChunk(path, offset, data) {},   // Uint8Array
//!   onFileCompleted(path) {},
//!   onProgress(done, total) {},
//!   loadState(direction, path) { return Promise.resolve(null); }, // IndexedDB
//!   saveState(direction, path, state) { return Promise.resolve(); },// IndexedDB
//!   getFileList() { return Promise.resolve([]); },       // uploads
//!   readFile(path, offset, length) { return Promise.resolve(new Uint8Array(0)); },
//!   log(msg) {},
//! });
//! ```

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

use js_sys::Reflect;
use wasm_bindgen::prelude::*;

use crate::js::Callbacks;
use crate::state::{TaskControl, TaskState};
use crate::tune::{TuningEngine, now_ms};

/// WASM engine facade. Construct via `new LibfwClient(options)`.
#[wasm_bindgen]
pub struct LibfwClient {
    config: ClientConfig,
    callbacks: Callbacks,
    control: TaskControl,
    tune: TuneHandle,
}

#[wasm_bindgen]
impl LibfwClient {
    /// Create an engine. `options` may include:
    /// `{ concurrency, uploadWindow, downloadWindow, downloadChunkSize,
    /// compress, compressLevel, chunkSize, maxRetries, baseRetryDelayMs,
    /// maxRetryDelayMs, timeoutMs, autoTune, tuneTtlMs }`.
    #[wasm_bindgen(constructor)]
    pub fn new(opts: JsValue) -> LibfwClient {
        let config = ClientConfig::from_js(&opts);
        LibfwClient {
            config: config.clone(),
            callbacks: Callbacks::new(),
            // The global in-flight HTTP pool is sized by `concurrency`, so it
            // bounds total network parallelism (not just concurrent files).
            control: TaskControl::with_max_parallel(config.concurrency.max(1)),
            tune: std::rc::Rc::new(std::cell::RefCell::new(TuningEngine::new(
                config.auto_tune,
                config.tune_ttl_ms,
                config.compress_level,
            ))),
        }
    }

    /// Install the JS callbacks object (required before any transfer).
    pub fn set_callbacks(&self, callbacks: JsValue) {
        self.callbacks.set(callbacks);
    }

    /// Install an `onTuning(phase, params, stats)` callback, invoked on
    /// every tuning state change (phase transitions and window evaluations).
    pub fn set_tune_callback(&self, cb: JsValue) {
        let f = cb.dyn_ref::<js_sys::Function>().cloned();
        self.tune.borrow_mut().set_on_tuning(f);
    }

    /// Snapshot the tuning engine: `{ phase, params, stats, capsHash }`.
    pub fn tune_state(&self) -> JsValue {
        self.tune.borrow().state_js()
    }

    /// Download every file under the virtual `dirPath` (empty = root).
    ///
    /// Resolves with the number of bytes written.
    pub fn download_folder(&self, base_url: &str, token: &str, dir_path: &str) -> js_sys::Promise {
        let base_url = base_url.to_string();
        let token = token.to_string();
        let dir_path = dir_path.to_string();
        let config = self.config.clone();
        let callbacks = self.callbacks.clone();
        let control = self.control.clone();
        let tune = self.tune.clone();

        wasm_bindgen_futures::future_to_promise(async move {
            control.reset();
            control.begin(TaskState::Downloading);
            let level = match prepare_transfer(
                &base_url, &config, &tune, &control, TransferKind::Download,
            )
            .await
            {
                Ok(l) => l,
                Err(e) => {
                    control.fail();
                    return Err(e.to_js());
                }
            };
            match download::download_folder(
                &base_url,
                &token,
                &dir_path,
                &callbacks,
                &control,
                &config,
                &tune,
                level,
            )
            .await
            {
                Ok(total) => {
                    tune.borrow_mut().transfer_end(&base_url, &LocalStore, now_ms(), true);
                    control.complete();
                    Ok(JsValue::from_f64(total as f64))
                }
                Err(e) => {
                    tune.borrow_mut().transfer_end(&base_url, &LocalStore, now_ms(), false);
                    control.fail();
                    Err(e.to_js())
                }
            }
        })
    }

    /// Download a single file at `file_path` into the chosen local directory.
    ///
    /// Resolves with the number of bytes written.
    pub fn download_file(&self, base_url: &str, token: &str, file_path: &str) -> js_sys::Promise {
        let base_url = base_url.to_string();
        let token = token.to_string();
        let file_path = file_path.to_string();
        let config = self.config.clone();
        let callbacks = self.callbacks.clone();
        let control = self.control.clone();
        let tune = self.tune.clone();

        wasm_bindgen_futures::future_to_promise(async move {
            control.reset();
            control.begin(TaskState::Downloading);
            let level = match prepare_transfer(
                &base_url, &config, &tune, &control, TransferKind::Download,
            )
            .await
            {
                Ok(l) => l,
                Err(e) => {
                    control.fail();
                    return Err(e.to_js());
                }
            };
            match download::download_single(
                &base_url,
                &token,
                &file_path,
                &callbacks,
                &control,
                &config,
                &tune,
                level,
            )
            .await
            {
                Ok(total) => {
                    tune.borrow_mut().transfer_end(&base_url, &LocalStore, now_ms(), true);
                    control.complete();
                    Ok(JsValue::from_f64(total as f64))
                }
                Err(e) => {
                    tune.borrow_mut().transfer_end(&base_url, &LocalStore, now_ms(), false);
                    control.fail();
                    Err(e.to_js())
                }
            }
        })
    }

    /// Upload the files reported by the JS `getFileList` callback.
    ///
    /// Resolves with the number of bytes uploaded.
    pub fn upload(&self, base_url: &str, token: &str) -> js_sys::Promise {
        let base_url = base_url.to_string();
        let token = token.to_string();
        let config = self.config.clone();
        let callbacks = self.callbacks.clone();
        let control = self.control.clone();
        let tune = self.tune.clone();

        wasm_bindgen_futures::future_to_promise(async move {
            control.reset();
            control.begin(TaskState::Uploading);
            let _level = match prepare_transfer(
                &base_url, &config, &tune, &control, TransferKind::Upload,
            )
            .await
            {
                Ok(l) => l,
                Err(e) => {
                    control.fail();
                    return Err(e.to_js());
                }
            };
            match upload::upload(&base_url, &token, &callbacks, &control, &config, &tune).await {
                Ok(total) => {
                    tune.borrow_mut().transfer_end(&base_url, &LocalStore, now_ms(), true);
                    control.complete();
                    Ok(JsValue::from_f64(total as f64))
                }
                Err(e) => {
                    tune.borrow_mut().transfer_end(&base_url, &LocalStore, now_ms(), false);
                    control.fail();
                    Err(e.to_js())
                }
            }
        })
    }

    /// Pause the active transfer (state → `paused`).
    pub fn pause(&self) {
        self.control.pause();
    }

    /// Resume a paused transfer.
    pub fn resume(&self) {
        self.control.resume();
    }

    /// Cancel the active transfer (state → `failed`).
    pub fn cancel(&self) {
        self.control.cancel();
    }

    /// Current state: `idle | downloading | uploading | paused | completed |
    /// failed`.
    pub fn state(&self) -> String {
        self.control.state().as_str().to_string()
    }

    /// Progress in `[0, 1]`.
    pub fn progress(&self) -> f64 {
        self.control.progress()
    }

    /// Bytes transferred so far.
    pub fn done_bytes(&self) -> f64 {
        self.control.done_bytes() as f64
    }

    /// Total bytes to transfer.
    pub fn total_bytes(&self) -> f64 {
        self.control.total_bytes() as f64
    }

    /// Whether callbacks have been installed.
    pub fn has_callbacks(&self) -> bool {
        self.callbacks.is_set()
    }
}

/// Read an optional string field from a JS object (helper for the SDK).
#[wasm_bindgen]
pub fn js_option_string(obj: &JsValue, key: &str) -> Option<String> {
    Reflect::get(obj, &JsValue::from_str(key))
        .ok()
        .and_then(|v| v.as_string())
}

/// Shared transfer preamble.
///
/// 1. Fetch `/capabilities` when auto-tuning (a 404 means a legacy server —
///    tuning is disabled for this client's lifetime and defaults are used).
/// 2. Seed the tuning engine (cache reuse → Settled, else Ramping from the
///    advertised minimums), returning the starting parameter table.
/// 3. Apply the starting concurrency to the global permit pool.
///
/// Returns the resolved download compression level.
async fn prepare_transfer(
    base_url: &str,
    config: &ClientConfig,
    tune: &TuneHandle,
    control: &TaskControl,
    direction: TransferKind,
) -> Result<i32, LibfwError> {
    let caps = if config.auto_tune {
        match http::fetch_capabilities(base_url, config.timeout_ms).await {
            Ok(c) => c,
            Err(LibfwError::Http { status: 404, .. }) => {
                // Legacy server without the /capabilities route: no tuning.
                tune.borrow_mut().set_enabled(false);
                libfw_core::Capabilities::default()
            }
            Err(e) => return Err(e),
        }
    } else {
        libfw_core::Capabilities::default()
    };

    let level = tune::resolve_level(config.compress_level, &caps);
    let static_params = TuneParams::from_config(
        config.concurrency,
        config.upload_window,
        config.download_window,
        config.chunk_size,
        config.download_chunk_size,
        level,
        &caps,
    );
    let params = {
        let mut t = tune.borrow_mut();
        t.set_direction(direction);
        t.begin_transfer(base_url, &caps, &LocalStore, now_ms(), &static_params)
    };
    control.set_max_parallel(params.concurrency);
    Ok(params.compress_level)
}

#[cfg(test)]
mod tests {
    #[test]
    #[cfg(target_arch = "wasm32")]
    fn engine_default_state_is_idle() {
        let engine = super::LibfwClient::new(wasm_bindgen::JsValue::NULL);
        assert_eq!(engine.state(), "idle");
        assert!(!engine.has_callbacks());
    }

    #[test]
    #[cfg(target_arch = "wasm32")]
    fn engine_options_parse() {
        let opts = js_sys::Object::new();
        js_sys::Reflect::set(&opts, &wasm_bindgen::JsValue::from_str("concurrency"), &wasm_bindgen::JsValue::from_f64(2.0))
            .unwrap();
        js_sys::Reflect::set(&opts, &wasm_bindgen::JsValue::from_str("compress"), &wasm_bindgen::JsValue::FALSE).unwrap();
        let engine = super::LibfwClient::new(opts.into());
        assert_eq!(engine.config.concurrency, 2);
        assert!(!engine.config.compress);
    }

    #[test]
    #[cfg(target_arch = "wasm32")]
    fn state_transitions_via_public_api() {
        use crate::state::TaskState;
        let engine = super::LibfwClient::new(wasm_bindgen::JsValue::NULL);
        engine.control.begin(TaskState::Downloading);
        engine.pause();
        assert_eq!(engine.state(), "paused");
        engine.resume();
        assert_eq!(engine.state(), "downloading");
        engine.cancel();
        assert_eq!(engine.state(), "failed");
    }
}
