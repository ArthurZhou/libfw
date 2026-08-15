//! Thin `web-sys` fetch wrapper plus response-body streaming helpers.
//!
//! Everything here runs on the browser's `fetch`/`ReadableStream` APIs so
//! the WASM engine never allocates more than a bounded read buffer per
//! transfer.

use std::cell::{Cell, RefCell};
use std::rc::Rc;

use js_sys::Reflect;
use wasm_bindgen::JsCast;
use wasm_bindgen::JsValue;
use wasm_bindgen::closure::Closure;
use wasm_bindgen_futures::JsFuture;
use web_sys::{Headers, Request, RequestInit, Response, XmlHttpRequest, XmlHttpRequestEventTarget};

use crate::error::{js_value_string, LibfwError};

/// Build a `Request` for `url` with the given method, headers and body.
pub fn request(
    url: &str,
    method: &str,
    headers: &Headers,
    body: Option<&JsValue>,
) -> Result<Request, LibfwError> {
    let init = RequestInit::new();
    init.set_method(method);
    if let Some(body) = body {
        init.set_body(body);
    }
    init.set_headers(headers);
    Request::new_with_str_and_init(url, &init)
        .map_err(|e| LibfwError::Network(format!("failed to build request for `{url}`: {e:?}")))
}

/// Perform a `fetch` and return the `Response`, aborting after `timeout_ms`.
pub async fn fetch(request: &Request, timeout_ms: u32) -> Result<Response, LibfwError> {
    let window = web_sys::window()
        .ok_or_else(|| LibfwError::Js("no window available".into()))?;
    let promise = window.fetch_with_request(request);
    let value = JsFuture::from(with_timeout(promise, timeout_ms))
        .await
        .map_err(|e| LibfwError::Network(format!("fetch failed: {}", js_value_string(&e))))?;
    Ok(value.unchecked_into())
}

/// Race a promise against a timer that rejects after `ms` (0 disables).
///
/// Enforces the client's per-request / per-read timeout so a hung peer
/// cannot stall a transfer forever (an `AbortController`-style guard without
/// an explicit signal object).
fn with_timeout(promise: js_sys::Promise, ms: u32) -> js_sys::Promise {
    if ms == 0 {
        return promise;
    }
    let timer = js_sys::Promise::new(&mut |_resolve, reject| {
        let window = web_sys::window().expect("window");
        let f: &js_sys::Function = reject.unchecked_ref();
        let err = js_sys::Error::new(&format!("libfw request timed out after {ms}ms"));
        let _ = window.set_timeout_with_callback_and_timeout_and_arguments_1(
            f,
            ms as i32,
            &err.into(),
        );
    });
    js_sys::Promise::race(&js_sys::Array::of2(&promise, &timer))
}

/// Read the entire response body into memory (used for small JSON payloads
/// like directory listings).
pub async fn read_all(resp: &Response, timeout_ms: u32) -> Result<Vec<u8>, LibfwError> {
    let promise = resp.array_buffer().map_err(|e| {
        LibfwError::Network(format!("arrayBuffer() failed: {}", js_value_string(&e)))
    })?;
    let value = JsFuture::from(with_timeout(promise, timeout_ms))
        .await
        .map_err(|e| LibfwError::Network(format!("body read failed: {}", js_value_string(&e))))?;
    let buf: js_sys::ArrayBuffer = value.unchecked_into();
    Ok(js_sys::Uint8Array::new(&buf).to_vec())
}

/// Stream a response body chunk by chunk, invoking `on_chunk` for each
/// `Uint8Array` slice. Memory stays bounded: chunks are handed to the
/// caller and dropped immediately.
///
/// The callback is async so transfers can pause/resume/cancel between
/// chunks while yielding to the JS event loop.
pub async fn stream_body<F, Fut>(
    resp: &Response,
    timeout_ms: u32,
    mut on_chunk: F,
) -> Result<(), LibfwError>
where
    F: FnMut(Vec<u8>) -> Fut,
    Fut: std::future::Future<Output = Result<(), LibfwError>>,
{
    let body = resp
        .body()
        .ok_or_else(|| LibfwError::Network("response has no body stream".into()))?;
    let reader: web_sys::ReadableStreamDefaultReader = body.get_reader().unchecked_into();
    loop {
        // A stalled body (peer stops sending) must also time out, not just
        // the initial connection.
        let value = JsFuture::from(with_timeout(reader.read(), timeout_ms))
            .await
            .map_err(|e| {
                LibfwError::Network(format!("body read failed: {}", js_value_string(&e)))
            })?;
        let obj: js_sys::Object = value.unchecked_into();
        let done = Reflect::get(&obj, &JsValue::from_str("done"))
            .map_err(|e| LibfwError::Network(format!("bad stream chunk: {e:?}")))?
            .as_bool()
            .unwrap_or(false);
        if done {
            break;
        }
        let value = Reflect::get(&obj, &JsValue::from_str("value"))
            .map_err(|e| LibfwError::Network(format!("bad stream chunk: {e:?}")))?;
        let bytes = crate::js::u8_vec_from_js(&value)?;
        if !bytes.is_empty() {
            on_chunk(bytes).await?;
        }
    }
    Ok(())
}

/// POST `body` to `url` via `XMLHttpRequest`, resolving with the HTTP status.
///
/// `fetch` cannot observe upload progress, so a slow-but-active upload on a
/// low-bandwidth link would be killed by a wall-clock timeout. XHR exposes
/// `upload.onprogress`, which lets us implement a **no-progress** timeout: a
/// rolling deadline that is pushed forward on every upload/response progress
/// tick, and only a transfer that has stalled for `timeout_ms` (nothing
/// moving) is aborted.
///
/// `header_pairs` is copied onto the XHR (XHR cannot consume a `Headers`
/// object). The promise resolves with the HTTP status (u16 as f64) and
/// rejects on network error or no-progress timeout.
pub fn xhr_post(
    url: &str,
    header_pairs: &[(String, String)],
    body: &[u8],
    timeout_ms: u32,
) -> Result<js_sys::Promise, LibfwError> {
    let xhr = XmlHttpRequest::new()
        .map_err(|e| LibfwError::Js(format!("XmlHttpRequest::new failed: {e:?}")))?;
    xhr.open_with_async("POST", url, true)
        .map_err(|e| LibfwError::Js(format!("xhr open failed: {e:?}")))?;
    for (name, value) in header_pairs {
        xhr.set_request_header(name, value)
            .map_err(|e| LibfwError::Js(format!("xhr set_request_header failed: {e:?}")))?;
    }
    let window = web_sys::window()
        .ok_or_else(|| LibfwError::Js("no window available".into()))?;
    let upload = xhr
        .upload()
        .map_err(|e| LibfwError::Js(format!("xhr.upload() failed: {e:?}")))?;

    // Rolling deadline (ms epoch). Anything that moves data refreshes it.
    let deadline: Rc<Cell<f64>> = Rc::new(Cell::new(js_sys::Date::now()));
    // True once the promise has settled (resolve or reject) — guards against
    // double-settling from racing event handlers / the watchdog.
    let finished: Rc<Cell<bool>> = Rc::new(Cell::new(false));
    let watchdog_id: Rc<Cell<i32>> = Rc::new(Cell::new(0));
    // All event closures must stay alive until the request settles; the
    // settle path clears this to release them (no per-chunk leaks).
    let holders: Rc<RefCell<Vec<Closure<dyn FnMut()>>>> = Rc::new(RefCell::new(Vec::new()));

    let promise = js_sys::Promise::new(&mut |resolve, reject| {
        let xhr = xhr.clone();
        let window = window.clone();
        let deadline = deadline.clone();
        let finished = finished.clone();
        let watchdog_id = watchdog_id.clone();
        let holders = holders.clone();
        let resolve = resolve.clone();
        let reject = reject.clone();
        let timeout_ms = timeout_ms;

        // No-progress watchdog: polls the deadline every 500 ms and aborts +
        // rejects when nothing has moved for `timeout_ms`.
        let watchdog = {
            let xhr = xhr.clone();
            let window = window.clone();
            let deadline = deadline.clone();
            let finished = finished.clone();
            let watchdog_id = watchdog_id.clone();
            let reject = reject.clone();
            let holders = holders.clone();
            Closure::wrap(Box::new(move || {
                if finished.get() {
                    return;
                }
                if js_sys::Date::now() - deadline.get() > timeout_ms as f64 {
                    finished.set(true);
                    window.clear_interval_with_handle(watchdog_id.get());
                    let _ = xhr.abort();
                    let _ = reject.call1(
                        &JsValue::NULL,
                        &JsValue::from_str(&format!(
                            "libfw upload stalled for {timeout_ms}ms (no progress)"
                        )),
                    );
                    holders.borrow_mut().clear();
                }
            }) as Box<dyn FnMut()>)
        };
        let id = match window.set_interval_with_callback_and_timeout_and_arguments_0(
            watchdog.as_ref().unchecked_ref(),
            500,
        ) {
            Ok(id) => id,
            Err(e) => {
                let _ = reject.call1(
                    &JsValue::NULL,
                    &JsValue::from_str(&format!("libfw setInterval failed: {e:?}")),
                );
                return;
            }
        };
        watchdog_id.set(id);
        holders.borrow_mut().push(watchdog);

        // Upload body progress: any movement refreshes the deadline.
        let on_upload_progress = {
            let deadline = deadline.clone();
            Closure::wrap(Box::new(move || {
                deadline.set(js_sys::Date::now());
            }) as Box<dyn FnMut()>)
        };
        let upload_target: &XmlHttpRequestEventTarget = upload.unchecked_ref();
        upload_target.set_onprogress(Some(on_upload_progress.as_ref().unchecked_ref()));
        holders.borrow_mut().push(on_upload_progress);

        // Response phase: refresh the deadline once headers/body start
        // arriving, and settle when the request is DONE.
        let on_readystatechange = {
            let xhr = xhr.clone();
            let window = window.clone();
            let deadline = deadline.clone();
            let finished = finished.clone();
            let watchdog_id = watchdog_id.clone();
            let resolve = resolve.clone();
            let reject = reject.clone();
            let holders = holders.clone();
            Closure::wrap(Box::new(move || {
                if finished.get() {
                    return;
                }
                let state = xhr.ready_state();
                if state >= 2 {
                    deadline.set(js_sys::Date::now());
                }
                if state == 4 {
                    finished.set(true);
                    window.clear_interval_with_handle(watchdog_id.get());
                    let status = xhr.status().unwrap_or(0);
                    if status > 0 {
                        let _ = resolve.call1(&JsValue::NULL, &JsValue::from_f64(status as f64));
                    } else {
                        let _ = reject.call1(
                            &JsValue::NULL,
                            &JsValue::from_str("libfw upload network error (empty status)"),
                        );
                    }
                    holders.borrow_mut().clear();
                }
            }) as Box<dyn FnMut()>)
        };
        xhr.set_onreadystatechange(Some(on_readystatechange.as_ref().unchecked_ref()));
        holders.borrow_mut().push(on_readystatechange);

        // Network error.
        let on_error = {
            let window = window.clone();
            let finished = finished.clone();
            let watchdog_id = watchdog_id.clone();
            let reject = reject.clone();
            let holders = holders.clone();
            Closure::wrap(Box::new(move || {
                if finished.get() {
                    return;
                }
                finished.set(true);
                window.clear_interval_with_handle(watchdog_id.get());
                let _ = reject.call1(&JsValue::NULL, &JsValue::from_str("libfw upload network error"));
                holders.borrow_mut().clear();
            }) as Box<dyn FnMut()>)
        };
        let xhr_target: &XmlHttpRequestEventTarget = xhr.unchecked_ref();
        xhr_target.set_onerror(Some(on_error.as_ref().unchecked_ref()));
        holders.borrow_mut().push(on_error);

        // Abort (only reachable when the watchdog already rejected, thanks to
        // the `finished` guard).
        let on_abort = {
            let window = window.clone();
            let finished = finished.clone();
            let watchdog_id = watchdog_id.clone();
            let reject = reject.clone();
            let holders = holders.clone();
            Closure::wrap(Box::new(move || {
                if finished.get() {
                    return;
                }
                finished.set(true);
                window.clear_interval_with_handle(watchdog_id.get());
                let _ = reject.call1(&JsValue::NULL, &JsValue::from_str("libfw upload aborted"));
                holders.borrow_mut().clear();
            }) as Box<dyn FnMut()>)
        };
        xhr_target.set_onabort(Some(on_abort.as_ref().unchecked_ref()));
        holders.borrow_mut().push(on_abort);

        // Fire the request (web-sys accepts the raw byte slice directly).
        if let Err(e) = xhr.send_with_opt_u8_array(Some(body)) {
            let _ = reject.call1(
                &JsValue::NULL,
                &JsValue::from_str(&format!("libfw xhr.send failed: {e:?}")),
            );
            holders.borrow_mut().clear();
        }
    });

    Ok(promise)
}

/// Header map helper: `Authorization: Bearer <token>` plus the protocol
/// handshake and optional `Accept-Encoding`.
pub fn auth_headers(token: &str, accept_zrip: bool) -> Result<Headers, LibfwError> {
    let headers = Headers::new()
        .map_err(|e| LibfwError::Js(format!("Headers::new failed: {e:?}")))?;
    headers
        .set("Authorization", &format!("Bearer {token}"))
        .map_err(|e| LibfwError::Js(format!("set Authorization failed: {e:?}")))?;
    // Advertise the wire protocol so the server can verify client/server
    // builds are matched (it replies 426 on a mismatch).
    headers
        .set(
            libfw_core::HEADER_PROTOCOL,
            libfw_core::protocol_header_value(),
        )
        .map_err(|e| LibfwError::Js(format!("set protocol header failed: {e:?}")))?;
    if accept_zrip {
        headers
            .set("Accept-Encoding", "zrip")
            .map_err(|e| LibfwError::Js(format!("set Accept-Encoding failed: {e:?}")))?;
    }
    Ok(headers)
}

/// Percent-encode a virtual path for use in a URL, preserving `/`.
pub fn encode_path(path: &str) -> String {
    path.split('/')
        .map(|seg| {
            js_sys::encode_uri_component(seg)
                .as_string()
                .unwrap_or_else(|| seg.to_string())
        })
        .collect::<Vec<_>>()
        .join("/")
}

/// Build the full URL for a resource.
pub fn file_url(base_url: &str, path: &str) -> String {
    let base = base_url.trim_end_matches('/');
    format!("{base}/file/{}", encode_path(path))
}

/// Build the full URL for a directory listing.
pub fn dir_url(base_url: &str, path: &str) -> String {
    let base = base_url.trim_end_matches('/');
    if path.is_empty() {
        format!("{base}/dir")
    } else {
        format!("{base}/dir/{}", encode_path(path))
    }
}

#[cfg(test)]
mod tests {
    #[test]
    #[cfg(target_arch = "wasm32")]
    fn url_encoding_preserves_slashes() {
        use super::encode_path;
        assert_eq!(encode_path("a b/c d.txt"), "a%20b/c%20d.txt");
        assert_eq!(encode_path("simple/file.txt"), "simple/file.txt");
    }

    #[test]
    #[cfg(target_arch = "wasm32")]
    fn url_builders() {
        use super::{dir_url, file_url};
        assert_eq!(file_url("http://h:8080/", "a/b"), "http://h:8080/file/a/b");
        assert_eq!(dir_url("http://h/", ""), "http://h/dir");
        assert_eq!(dir_url("http://h", "sub"), "http://h/dir/sub");
    }
}
