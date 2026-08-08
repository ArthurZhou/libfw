//! Registry of JS callbacks handed to the WASM engine.
//!
//! The JS SDK passes a single object with all callbacks via
//! `LibfwClient::set_callbacks`. The engine stores it and invokes methods
//! through `js_sys::Reflect`, so no wasm-bindgen build-time JS module is
//! required (the SDK fully owns WASM instantiation).

use std::cell::RefCell;
use std::rc::Rc;

use js_sys::{Array, Function, Reflect};
use wasm_bindgen::prelude::*;

use crate::error::LibfwError;

/// Invoke a method on the JS callbacks object.
#[derive(Debug, Clone)]
pub struct Callbacks {
    inner: Rc<RefCell<Option<JsValue>>>,
}

impl Default for Callbacks {
    fn default() -> Self {
        Callbacks::new()
    }
}

impl Callbacks {
    /// Create an empty registry (no callbacks yet).
    pub fn new() -> Self {
        Callbacks {
            inner: Rc::new(RefCell::new(None)),
        }
    }

    /// Install the JS callbacks object.
    pub fn set(&self, callbacks: JsValue) {
        *self.inner.borrow_mut() = Some(callbacks);
    }

    /// Whether callbacks have been installed.
    pub fn is_set(&self) -> bool {
        self.inner.borrow().is_some()
    }

    /// Synchronously invoke `method` with `args` on the JS object.
    ///
    /// For callbacks returning a `Promise`, wrap the result with
    /// [`js_sys::Promise::from`] and await it via `JsFuture`.
    pub fn call(&self, method: &str, args: &[JsValue]) -> Result<JsValue, LibfwError> {
        let obj = self
            .inner
            .borrow()
            .clone()
            .ok_or_else(|| LibfwError::Protocol("JS callbacks not installed".into()))?;
        let f: Function = Reflect::get(&obj, &JsValue::from_str(method))
            .map_err(|e| LibfwError::Js(format!("missing callback `{method}`: {e:?}")))?
            .into();
        let this = obj;
        let arr = Array::new();
        for a in args {
            arr.push(a);
        }
        f.apply(&this, &arr)
            .map_err(|e| LibfwError::Js(format!("callback `{method}` failed: {e:?}")))
    }

    /// Notify the JS layer that a file download/upload has begun.
    pub fn on_file_start(&self, path: &str, size: u64) -> Result<(), LibfwError> {
        self.call(
            "onFileStart",
            &[JsValue::from_str(path), JsValue::from_f64(size as f64)],
        )
        .map(|_| ())
    }

    /// Push a decompressed byte chunk for `path` at `offset` to JS.
    pub fn on_write_chunk(&self, path: &str, offset: u64, data: &[u8]) -> Result<(), LibfwError> {
        let arr = js_sys::Uint8Array::from(data);
        self.call(
            "onWriteChunk",
            &[
                JsValue::from_str(path),
                JsValue::from_f64(offset as f64),
                arr.into(),
            ],
        )
        .map(|_| ())
    }

    /// A file finished transferring successfully.
    pub fn on_file_completed(&self, path: &str) -> Result<(), LibfwError> {
        self.call("onFileCompleted", &[JsValue::from_str(path)])
            .map(|_| ())
    }

    /// Overall progress update.
    pub fn on_progress(&self, done: u64, total: u64) -> Result<(), LibfwError> {
        self.call(
            "onProgress",
            &[JsValue::from_f64(done as f64), JsValue::from_f64(total as f64)],
        )
        .map(|_| ())
    }

    /// Load persisted resume state for `path` from JS (IndexedDB).
    ///
    /// `direction` separates upload from download state so a download of a
    /// same-named file never leaks its `{offset, size}` into a later
    /// upload (which would silently skip chunks) and vice versa.
    ///
    /// Returns `Ok(None)` when no state exists.
    pub async fn load_state(&self, direction: &str, path: &str) -> Result<Option<JsValue>, LibfwError> {
        let promise = self.call(
            "loadState",
            &[JsValue::from_str(direction), JsValue::from_str(path)],
        )?;
        let value = await_promise(promise).await?;
        if value.is_null() || value.is_undefined() {
            Ok(None)
        } else {
            Ok(Some(value))
        }
    }

    /// Persist resume state for `path` via JS (IndexedDB).
    ///
    /// `direction` (see [`Callbacks::load_state`]) namespaces the key so
    /// upload and download resume state cannot collide.
    pub async fn save_state(&self, direction: &str, path: &str, state: &JsValue) -> Result<(), LibfwError> {
        let promise = self.call(
            "saveState",
            &[JsValue::from_str(direction), JsValue::from_str(path), state.clone()],
        )?;
        await_promise(promise).await.map(|_| ())
    }

    /// Ask JS for the list of files to upload: array of
    /// `{ path, size, mtime }`.
    pub async fn file_list(&self) -> Result<Vec<crate::plan::FileEntry>, LibfwError> {
        let promise = self.call("getFileList", &[])?;
        let value = await_promise(promise).await?;
        crate::plan::parse_file_entries(&value)
    }

    /// Ask JS to read `length` bytes of the upload file `path` at `offset`.
    pub async fn read_file(&self, path: &str, offset: u64, length: u64) -> Result<Vec<u8>, LibfwError> {
        let promise = self.call(
            "readFile",
            &[
                JsValue::from_str(path),
                JsValue::from_f64(offset as f64),
                JsValue::from_f64(length as f64),
            ],
        )?;
        let value = await_promise(promise).await?;
        u8_vec_from_js(&value)
    }

    /// Log a debug message to the JS console.
    pub fn log(&self, msg: &str) {
        let _ = self.call("log", &[JsValue::from_str(msg)]);
    }
}

/// Await a JS promise value produced by a callback.
pub async fn await_promise(value: JsValue) -> Result<JsValue, LibfwError> {
    let promise = js_sys::Promise::from(value);
    wasm_bindgen_futures::JsFuture::from(promise)
        .await
        .map_err(LibfwError::from)
}

/// Extract a `Vec<u8>` from an `ArrayBuffer` or `Uint8Array` JS value.
pub fn u8_vec_from_js(value: &JsValue) -> Result<Vec<u8>, LibfwError> {
    if value.is_instance_of::<js_sys::ArrayBuffer>() {
        let buf: js_sys::ArrayBuffer = value.clone().unchecked_into();
        let arr = js_sys::Uint8Array::new(&buf);
        Ok(arr.to_vec())
    } else if value.is_instance_of::<js_sys::Uint8Array>() {
        let arr: js_sys::Uint8Array = value.clone().unchecked_into();
        Ok(arr.to_vec())
    } else {
        Err(LibfwError::Js(
            "expected ArrayBuffer or Uint8Array from JS callback".into(),
        ))
    }
}
