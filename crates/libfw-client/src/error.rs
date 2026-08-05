//! Unified error type surfaced by the WASM engine.
//!
//! Every network, storage and protocol failure is converted into a
//! [`LibfwError`] and finally into a JS `Error` (via
//! [`LibfwError::to_js`]) so the JS SDK can wrap it in `LibfwError`.

use wasm_bindgen::JsValue;

/// Errors produced by the WASM engine.
#[derive(Debug, thiserror::Error)]
pub enum LibfwError {
    /// The server answered with an unexpected status code.
    #[error("http {status} for `{url}`")]
    Http { status: u16, url: String },
    /// A network-level failure (fetch rejected, body stream broke, …).
    #[error("network error: {0}")]
    Network(String),
    /// The server's body could not be decompressed.
    #[error("decompression error: {0}")]
    Decompress(String),
    /// An upload chunk could not be compressed.
    #[error("compression error: {0}")]
    Compress(String),
    /// The transfer protocol contract was violated.
    #[error("protocol error: {0}")]
    Protocol(String),
    /// A JS callback returned a non-`Promise`/rejected value.
    #[error("js error: {0}")]
    Js(String),
    /// The task was cancelled by the user.
    #[error("transfer cancelled")]
    Cancelled,
    /// An upload file is missing or unreadable.
    #[error("storage error: {0}")]
    Storage(String),
}

impl LibfwError {
    /// Convert into a JS `Error`-compatible `JsValue`.
    pub fn to_js(&self) -> JsValue {
        let msg = self.to_string();
        let err = js_sys::Error::new(&msg);
        // Tag it so the JS SDK can distinguish `LibfwError`s.
        let _ = js_sys::Reflect::set(
            &err,
            &JsValue::from_str("isLibfwError"),
            &JsValue::TRUE,
        );
        err.into()
    }
}

impl From<JsValue> for LibfwError {
    fn from(value: JsValue) -> Self {
        LibfwError::Js(
            value
                .as_string()
                .unwrap_or_else(|| format!("{value:?}")),
        )
    }
}

impl From<libfw_core::error::DecompressError> for LibfwError {
    fn from(e: libfw_core::error::DecompressError) -> Self {
        LibfwError::Decompress(e.to_string())
    }
}

impl From<libfw_core::error::CompressError> for LibfwError {
    fn from(e: libfw_core::error::CompressError) -> Self {
        LibfwError::Compress(e.to_string())
    }
}

/// Human-readable form of an arbitrary `JsValue` (for error messages).
pub fn js_value_string(v: &JsValue) -> String {
    v.as_string().unwrap_or_else(|| format!("{v:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_messages_are_stable() {
        assert_eq!(
            LibfwError::Http {
                status: 404,
                url: "/f".into()
            }
            .to_string(),
            "http 404 for `/f`"
        );
        assert_eq!(LibfwError::Cancelled.to_string(), "transfer cancelled");
    }

    #[test]
    #[cfg(target_arch = "wasm32")]
    fn js_errors_carry_tag() {
        let js = LibfwError::Cancelled.to_js();
        let err = js_sys::Error::from(js);
        let tag = js_sys::Reflect::get(&err, &JsValue::from_str("isLibfwError"))
            .ok()
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        assert!(tag);
    }
}
