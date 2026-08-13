//! WebSocket transport for the WASM engine.
//!
//! Wraps `web_sys::WebSocket` behind an async message queue so the transfer
//! loops can `.await` the next frame exactly like the old `fetch` path. Every
//! frame is a binary message whose first byte is the frame type (see
//! [`libfw_core::ws`]); control frames carry JSON payloads and data frames
//! carry the compact block format.
//!
//! The connection is used for **both directions** of a transfer and for all
//! control commands (handshake, directory listing, metadata) — there are no
//! separate HTTP calls on the transfer path anymore.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::rc::Rc;

use js_sys::{ArrayBuffer, Function, Promise, Uint8Array};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;
use web_sys::{BinaryType, CloseEvent, Event, MessageEvent, WebSocket};

use libfw_core::storage::DirEntry;
use libfw_core::ws::*;

use crate::error::{js_value_string, LibfwError};
use crate::js::u8_vec_from_js;

/// Shared, mutable WebSocket state.
#[derive(Default)]
struct WsState {
    socket: Option<WebSocket>,
    /// Cumulative bytes handed to the socket via [`WsConnection::send`]
    /// (enqueued, not necessarily transmitted over the wire yet).
    sent_bytes: Cell<u64>,
    /// Frames received but not yet consumed by [`WsConnection::next`].
    incoming: VecDeque<Vec<u8>>,
    /// Resolver of the pending `next()` promise (delivers the next frame).
    pending_resolve: Option<Function>,
    /// Resolver / rejecter of the connection-open promise.
    open_resolve: Option<Function>,
    open_reject: Option<Function>,
    /// Set when the socket errors or closes unexpectedly.
    failure: Option<String>,
    /// Kept alive for the socket's lifetime (dropping them detaches).
    _on_open: Option<Closure<dyn FnMut(Event)>>,
    _on_message: Option<Closure<dyn FnMut(MessageEvent)>>,
    _on_error: Option<Closure<dyn FnMut(Event)>>,
    _on_close: Option<Closure<dyn FnMut(CloseEvent)>>,
}

/// An open, authenticated WebSocket connection to a libfw server.
#[derive(Clone)]
pub struct WsConnection {
    state: Rc<RefCell<WsState>>,
    timeout_ms: u32,
}

impl WsConnection {
    /// Derive the `ws(s)://` endpoint from an `http(s)://` base URL and
    /// connect: open the socket, wait for `onopen`, send `FRAME_HELLO` and
    /// wait for `FRAME_HELLO_OK`.
    pub async fn connect(
        base_url: &str,
        token: &str,
        timeout_ms: u32,
        ws_url_override: Option<&str>,
    ) -> Result<WsConnection, LibfwError> {
        let url = match ws_url_override {
            Some(u) if !u.is_empty() => u.to_string(),
            _ => ws_url(base_url)?,
        };
        let state = Rc::new(RefCell::new(WsState::default()));

        let ws = WebSocket::new(&url)
            .map_err(|e| LibfwError::Network(format!("ws open failed for `{url}`: {e:?}")))?;
        ws.set_binary_type(BinaryType::Arraybuffer);

        // onopen → resolve the open promise.
        let st = state.clone();
        let on_open = Closure::wrap(Box::new(move |_ev: Event| {
            if let Some(res) = st.borrow_mut().open_resolve.take() {
                let _ = res.call1(&JsValue::UNDEFINED, &JsValue::UNDEFINED);
            }
        }) as Box<dyn FnMut(Event)>);
        ws.set_onopen(Some(on_open.as_ref().unchecked_ref()));

        // onmessage → deliver the frame to the pending `next()` or queue it.
        let st = state.clone();
        let on_message = Closure::wrap(Box::new(move |event: MessageEvent| {
            let data = event.data();
            let bytes = if data.is_instance_of::<ArrayBuffer>() {
                Uint8Array::new(&data).to_vec()
            } else if data.is_instance_of::<Uint8Array>() {
                Uint8Array::new(&data).to_vec()
            } else {
                return;
            };
            let mut st = st.borrow_mut();
            if let Some(res) = st.pending_resolve.take() {
                let value = Uint8Array::from(bytes.as_slice());
                let _ = res.call1(&JsValue::UNDEFINED, &value.into());
            } else {
                st.incoming.push_back(bytes);
            }
        }) as Box<dyn FnMut(MessageEvent)>);
        ws.set_onmessage(Some(on_message.as_ref().unchecked_ref()));

        // onerror / onclose → record the failure and reject the open promise.
        let st = state.clone();
        let on_error = Closure::wrap(Box::new(move |_ev: Event| {
            let mut st = st.borrow_mut();
            st.failure.get_or_insert_with(|| "websocket error".into());
            if let Some(rej) = st.open_reject.take() {
                let _ = rej.call1(&JsValue::UNDEFINED, &JsValue::from_str("websocket error"));
            }
        }) as Box<dyn FnMut(Event)>);
        ws.set_onerror(Some(on_error.as_ref().unchecked_ref()));

        let st = state.clone();
        let on_close = Closure::wrap(Box::new(move |_ev: CloseEvent| {
            let mut st = st.borrow_mut();
            st.failure.get_or_insert_with(|| "connection closed".into());
            if let Some(rej) = st.open_reject.take() {
                let _ = rej.call1(
                    &JsValue::UNDEFINED,
                    &JsValue::from_str("connection closed"),
                );
            }
        }) as Box<dyn FnMut(CloseEvent)>);
        ws.set_onclose(Some(on_close.as_ref().unchecked_ref()));

        {
            let mut st = state.borrow_mut();
            st.socket = Some(ws.clone());
            st._on_open = Some(on_open);
            st._on_message = Some(on_message);
            st._on_error = Some(on_error);
            st._on_close = Some(on_close);
        }

        // Wait for `onopen` (or an immediate error/close).
        let (promise, resolve, reject) = new_resolver();
        {
            let mut st = state.borrow_mut();
            st.open_resolve = Some(resolve);
            st.open_reject = Some(reject);
        }
        match JsFuture::from(with_timeout(promise, timeout_ms)).await {
            Ok(_) => {}
            Err(e) => {
                let msg = state
                    .borrow()
                    .failure
                    .clone()
                    .unwrap_or_else(|| format!("ws handshake failed: {}", js_value_string(&e)));
                return Err(LibfwError::Network(msg));
            }
        }

        let conn = WsConnection {
            state,
            timeout_ms,
        };

        // Handshake: HELLO → HELLO_OK (or ERROR).
        conn.send(&control_frame(FRAME_HELLO, &Hello::new(token)))?;
        loop {
            let frame = conn.next().await?;
            match frame_type(&frame) {
                Some(FRAME_HELLO_OK) => break,
                Some(FRAME_ERROR) => {
                    let err: ErrorMessage = parse_control(&frame, FRAME_ERROR)
                        .ok_or_else(|| LibfwError::Protocol("bad ERROR frame".into()))?;
                    return Err(LibfwError::Protocol(format!(
                        "server rejected handshake: {}: {}",
                        err.code, err.message
                    )));
                }
                _ => return Err(LibfwError::Protocol(
                    "expected HELLO_OK after handshake".into(),
                )),
            }
        }
        Ok(conn)
    }

    /// Send one raw frame as a binary WebSocket message.
    ///
    /// This only queues the frame into the socket's send buffer; the bytes
    /// are counted as progress when they actually leave the socket (see
    /// [`WsConnection::transmitted_bytes`]).
    pub fn send(&self, frame: &[u8]) -> Result<(), LibfwError> {
        let ws = self
            .state
            .borrow()
            .socket
            .clone()
            .ok_or_else(|| LibfwError::Network("websocket not open".into()))?;
        self.state.borrow().sent_bytes.set(
            self.state
                .borrow()
                .sent_bytes
                .get()
                .saturating_add(frame.len() as u64),
        );
        let mut copy = frame.to_vec();
        ws.send_with_u8_array(&mut copy)
            .map_err(|e| LibfwError::Network(format!("ws send failed: {}", js_value_string(&e))))
    }

    /// Bytes this connection has ACTUALLY put on the wire: everything
    /// enqueued via [`WsConnection::send`] minus what is still sitting in
    /// the socket's send buffer (`bufferedAmount`). On a slow link this lags
    /// `send()` and is the true measure of wire progress.
    pub fn transmitted_bytes(&self) -> u64 {
        let st = self.state.borrow();
        let sent = st.sent_bytes.get();
        let buffered = st
            .socket
            .as_ref()
            .map(|ws| ws.buffered_amount() as u64)
            .unwrap_or(0);
        sent.saturating_sub(buffered)
    }

    /// Total bytes enqueued into the socket (for diagnostics/tests).
    #[allow(dead_code)]
    pub fn enqueued_bytes(&self) -> u64 {
        self.state.borrow().sent_bytes.get()
    }

    /// Non-blocking pop of one already-buffered frame.
    ///
    /// Used by the upload loop to poll both incoming frames and live wire
    /// progress without registering a promise resolver (which would risk a
    /// frame being delivered to a stale resolver on a poll timeout).
    pub fn try_recv(&self) -> Option<Vec<u8>> {
        self.state.borrow_mut().incoming.pop_front()
    }

    /// Await the next frame, timing out after [`WsConnection::timeout_ms`].
    pub async fn next(&self) -> Result<Vec<u8>, LibfwError> {
        loop {
            // Fast path: a frame is already queued.
            if let Some(msg) = self.state.borrow_mut().incoming.pop_front() {
                return Ok(msg);
            }
            let state = self.state.clone();
            if let Some(err) = state.borrow().failure.clone() {
                return Err(LibfwError::Network(err));
            }
            // Register a resolver; re-check the queue in case a message
            // arrived between the fast-path check and the registration.
            let (promise, resolve, _reject) = new_resolver();
            {
                let mut st = state.borrow_mut();
                if let Some(msg) = st.incoming.pop_front() {
                    drop(st);
                    let _ = resolve.call1(&JsValue::UNDEFINED, &u8_to_js(&msg));
                    return Ok(msg);
                }
                st.pending_resolve = Some(resolve);
            }
            let value = match JsFuture::from(with_timeout(promise, self.timeout_ms)).await {
                Ok(v) => v,
                Err(e) => {
                    // Clear the abandoned resolver so a frame that arrives
                    // late (after the deadline) is queued for the next
                    // `next()`/poll instead of being delivered to the dead
                    // promise and silently dropped. Callers normally drop
                    // the connection on timeout, but this hardens against
                    // reuse.
                    self.state.borrow_mut().pending_resolve.take();
                    return Err(LibfwError::Network(format!(
                        "ws read timed out: {}",
                        js_value_string(&e)
                    )));
                }
            };
            if value.is_undefined() || value.is_null() {
                continue;
            }
            return u8_vec_from_js(&value);
        }
    }

    /// Close the connection (best-effort).
    ///
    /// Normally a connection is handed back to a [`WsPool`] instead of being
    /// closed, so this is only used for explicit teardown.
    #[allow(dead_code)]
    pub fn close(&self) {
        if let Some(ws) = self.state.borrow().socket.clone() {
            let _ = ws.close();
        }
    }

    /// Whether the underlying socket is still open and healthy enough to
    /// reuse for another transfer.
    ///
    /// The libfw server keeps a connection alive across sequential transfers
    /// (its control loop continues after a `COMPLETE`), so a finished file's
    /// connection can be handed back to the pool instead of being torn down.
    pub fn is_usable(&self) -> bool {
        let st = self.state.borrow();
        st.socket.is_some() && st.failure.is_none()
    }

    /// Ask the server to list a directory; returns the entries or an error.
    pub async fn list_dir(&self, path: &str) -> Result<Vec<DirEntry>, LibfwError> {
        self.send(&control_frame(
            FRAME_LIST_REQ,
            &serde_json::json!({ "path": path }),
        ))?;
        loop {
            let frame = self.next().await?;
            match frame_type(&frame) {
                Some(FRAME_LIST_REPLY) => {
                    #[derive(serde::Deserialize)]
                    struct Reply {
                        entries: Vec<DirEntry>,
                    }
                    let reply: Reply = parse_control(&frame, FRAME_LIST_REPLY)
                        .ok_or_else(|| LibfwError::Protocol("bad LIST_REPLY".into()))?;
                    return Ok(reply.entries);
                }
                Some(FRAME_ERROR) => {
                    return Err(parse_error(&frame)
                        .unwrap_or_else(|| LibfwError::Protocol("listing failed".into())));
                }
                _ => {}
            }
        }
    }

    /// Ask the server for a file's authoritative `(etag, size)`.
    pub async fn file_meta(&self, path: &str) -> Result<(String, u64), LibfwError> {
        self.send(&control_frame(
            FRAME_META_REQ,
            &serde_json::json!({ "path": path }),
        ))?;
        loop {
            let frame = self.next().await?;
            match frame_type(&frame) {
                Some(FRAME_META_REPLY) => {
                    #[derive(serde::Deserialize)]
                    struct Reply {
                        size: u64,
                        #[serde(default)]
                        etag: String,
                    }
                    let reply: Reply = parse_control(&frame, FRAME_META_REPLY)
                        .ok_or_else(|| LibfwError::Protocol("bad META_REPLY".into()))?;
                    return Ok((reply.etag, reply.size));
                }
                Some(FRAME_ERROR) => {
                    return Err(parse_error(&frame)
                        .unwrap_or_else(|| LibfwError::Protocol("metadata lookup failed".into())));
                }
                _ => {}
            }
        }
    }
}

/// Parse a `FRAME_ERROR` payload into a [`LibfwError`].
pub fn parse_error(frame: &[u8]) -> Option<LibfwError> {
    let err: ErrorMessage = parse_control(frame, FRAME_ERROR)?;
    let msg = format!("{}: {}", err.code, err.message);
    match err.code.as_str() {
        "not_found" => Some(LibfwError::Http { status: 404, url: err.message }),
        "auth" | "handshake" => Some(LibfwError::Protocol(msg)),
        "too_large" => Some(LibfwError::Http { status: 413, url: err.message }),
        _ => Some(LibfwError::Protocol(msg)),
    }
}

/// A small pool of reusable WebSocket connections.
///
/// A folder transfer used to open (and close) one connection **per file**,
/// which is wasteful when many small files share one transfer. With a pool,
/// the transfer checks a connection out for one file and checks it back in
/// when the file finishes; the server keeps the socket alive across
/// sequential transfers, so the next file reuses it. The pool grows to at
/// most `concurrency` connections (one per in-flight file) and never
/// re-uses a connection that errored or closed.
#[derive(Clone)]
pub struct WsPool {
    inner: Rc<RefCell<Vec<WsConnection>>>,
}

impl Default for WsPool {
    fn default() -> Self {
        WsPool::new()
    }
}

impl WsPool {
    /// Create an empty pool.
    pub fn new() -> Self {
        WsPool {
            inner: Rc::new(RefCell::new(Vec::new())),
        }
    }

    /// Take an idle, still-usable connection; open a fresh one when the pool
    /// is empty (or all idle connections have gone stale).
    pub async fn checkout(
        &self,
        base_url: &str,
        token: &str,
        timeout_ms: u32,
        ws_url_override: Option<&str>,
    ) -> Result<WsConnection, LibfwError> {
        loop {
            match self.inner.borrow_mut().pop() {
                Some(conn) if conn.is_usable() => return Ok(conn),
                Some(_) => continue, // stale → drop it and try the next
                None => break,
            }
        }
        WsConnection::connect(base_url, token, timeout_ms, ws_url_override).await
    }

    /// Return a connection for reuse. Connections that errored or closed are
    /// dropped instead (their socket closes when the last handle is dropped).
    pub fn checkin(&self, conn: WsConnection) {
        if conn.is_usable() {
            self.inner.borrow_mut().push(conn);
        }
    }
}

/// Convert bytes into a JS `Uint8Array` value.
fn u8_to_js(bytes: &[u8]) -> JsValue {
    Uint8Array::from(bytes).into()
}

/// Create a `(Promise, resolve, reject)` triple.
fn new_resolver() -> (Promise, Function, Function) {
    let mut resolve: Option<Function> = None;
    let mut reject: Option<Function> = None;
    let promise = Promise::new(&mut |res: Function, rej: Function| {
        resolve = Some(res);
        reject = Some(rej);
    });
    (
        promise,
        resolve.expect("resolver"),
        reject.expect("rejecter"),
    )
}

/// Race a promise against a timer that rejects after `ms` (0 disables).
fn with_timeout(promise: Promise, ms: u32) -> Promise {
    if ms == 0 {
        return promise;
    }
    let timer = Promise::new(&mut |_resolve, reject| {
        let window = web_sys::window().expect("window");
        let f: &Function = reject.unchecked_ref();
        let err = js_sys::Error::new(&format!("libfw ws timed out after {ms}ms"));
        let _ = window.set_timeout_with_callback_and_timeout_and_arguments_1(
            f,
            ms as i32,
            &err.into(),
        );
    });
    Promise::race(&js_sys::Array::of2(&promise, &timer))
}

/// Derive the `ws(s)://` endpoint from a base URL.
fn ws_url(base_url: &str) -> Result<String, LibfwError> {
    let base = base_url.trim_end_matches('/');
    if base.is_empty() {
        // Same-origin: derive scheme + host from the page.
        return Ok(format!("{}/ws", origin()));
    }
    if let Some(rest) = base.strip_prefix("https://") {
        return Ok(format!("wss://{rest}/ws"));
    }
    if let Some(rest) = base.strip_prefix("http://") {
        return Ok(format!("ws://{rest}/ws"));
    }
    if let Some(rest) = base.strip_prefix("wss://") {
        return Ok(format!("wss://{rest}/ws"));
    }
    if let Some(rest) = base.strip_prefix("ws://") {
        return Ok(format!("ws://{rest}/ws"));
    }
    // A relative base (e.g. `/api`) → same-origin + base + /ws.
    Ok(format!("{}{base}/ws", origin()))
}

/// The page origin (`ws(s)://host`).
fn origin() -> String {
    let loc = web_sys::window().expect("window.location").location();
    let proto = if loc.protocol().ok().map(|p| p == "https:").unwrap_or(false) {
        "wss"
    } else {
        "ws"
    };
    let host = loc.host().unwrap_or_default();
    format!("{proto}://{host}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(target_arch = "wasm32")]
    fn u8_to_js_roundtrip() {
        let data = vec![1u8, 2, 3, 250];
        let v = u8_to_js(&data);
        let back = u8_vec_from_js(&v).unwrap();
        assert_eq!(back, data);
    }

    #[test]
    fn parse_error_maps_codes() {
        let frame = control_frame(FRAME_ERROR, &ErrorMessage {
            code: "not_found".into(),
            message: "x".into(),
        });
        let err = parse_error(&frame).unwrap();
        assert!(matches!(err, LibfwError::Http { status: 404, .. }));
    }
}
