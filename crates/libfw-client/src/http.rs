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
use web_sys::{AbortController, Headers, Request, RequestInit, Response, XmlHttpRequest, XmlHttpRequestEventTarget};

use crate::error::{js_value_string, LibfwError};

/// Build a `Request` for `url` with the given method, headers and body.
///
/// Returns the request together with its `AbortController`: pass it back to
/// [`fetch`]/[`read_all`]/[`stream_body`] so a timeout aborts the underlying
/// transfer instead of leaving the request (and its connection buffers)
/// running in the background.
pub fn request(
    url: &str,
    method: &str,
    headers: &Headers,
    body: Option<&JsValue>,
) -> Result<(Request, AbortController), LibfwError> {
    let init = RequestInit::new();
    init.set_method(method);
    if let Some(body) = body {
        init.set_body(body);
    }
    init.set_headers(headers);
    let ctrl = AbortController::new()
        .map_err(|e| LibfwError::Js(format!("AbortController unavailable: {e:?}")))?;
    init.set_signal(Some(&ctrl.signal()));
    let req = Request::new_with_str_and_init(url, &init)
        .map_err(|e| LibfwError::Network(format!("failed to build request for `{url}`: {e:?}")));
    req.map(|r| (r, ctrl))
}

/// Perform a `fetch` and return the `Response`, aborting after `timeout_ms`.
pub async fn fetch(
    request: &Request,
    timeout_ms: u32,
    ctrl: &AbortController,
) -> Result<Response, LibfwError> {
    let window = web_sys::window()
        .ok_or_else(|| LibfwError::Js("no window available".into()))?;
    let promise = window.fetch_with_request(request);
    let value = JsFuture::from(with_timeout(promise, timeout_ms, Some(ctrl)))
        .await
        .map_err(|e| LibfwError::Network(format!("fetch failed: {}", js_value_string(&e))))?;
    Ok(value.unchecked_into())
}

/// Wrap a JS promise with a deadline.
///
/// The timeout is enforced by aborting the request's `AbortController` only
/// when the deadline actually fires. If the underlying promise resolves or
/// rejects before that time, the timer is cleared so it cannot abort a
/// finished request and generate a spurious `AbortError` later.
fn with_timeout(
    promise: js_sys::Promise,
    ms: u32,
    abort: Option<&AbortController>,
) -> js_sys::Promise {
    if ms == 0 {
        return promise;
    }

    let holders: Rc<RefCell<Vec<Closure<dyn FnMut(JsValue)>>>> = Rc::new(RefCell::new(Vec::new()));
    let timeout_holder: Rc<RefCell<Option<Closure<dyn FnMut()>>>> = Rc::new(RefCell::new(None));
    let scheduled = Rc::new(Cell::new(None::<i32>));

    js_sys::Promise::new(&mut |resolve, reject| {
        let Some(window) = web_sys::window() else {
            // No browser window at all: just pass through the underlying
            // promise without imposing a timeout.
            let resolve_fn = js_sys::Function::from(resolve.clone());
            let reject_fn = js_sys::Function::from(reject.clone());
            let on_ok = Closure::wrap(Box::new(move |v: JsValue| {
                let _ = resolve_fn.call1(&JsValue::UNDEFINED, &v);
            }) as Box<dyn FnMut(JsValue)>);
            let on_err = Closure::wrap(Box::new(move |e: JsValue| {
                let _ = reject_fn.call1(&JsValue::UNDEFINED, &e);
            }) as Box<dyn FnMut(JsValue)>);
            let _ = promise.then(&on_ok);
            let _ = promise.catch(&on_err);
            holders.borrow_mut().push(on_ok);
            holders.borrow_mut().push(on_err);
            return;
        };

        let resolve_fn = js_sys::Function::from(resolve.clone());
        let reject_fn = js_sys::Function::from(reject.clone());
        let window_for_success = window.clone();
        let scheduled_for_success = scheduled.clone();
        let holders_for_success = holders.clone();
        let timeout_holder_for_success = timeout_holder.clone();
        let on_ok = Closure::wrap(Box::new(move |value: JsValue| {
            if let Some(id) = scheduled_for_success.take() {
                window_for_success.clear_timeout_with_handle(id);
            }
            timeout_holder_for_success.borrow_mut().take();
            holders_for_success.borrow_mut().clear();
            let _ = resolve_fn.call1(&JsValue::UNDEFINED, &value);
        }) as Box<dyn FnMut(JsValue)>);

        let window_for_fail = window.clone();
        let scheduled_for_fail = scheduled.clone();
        let holders_for_fail = holders.clone();
        let timeout_holder_for_fail = timeout_holder.clone();
        let on_err = Closure::wrap(Box::new(move |err: JsValue| {
            if let Some(id) = scheduled_for_fail.take() {
                window_for_fail.clear_timeout_with_handle(id);
            }
            timeout_holder_for_fail.borrow_mut().take();
            holders_for_fail.borrow_mut().clear();
            let _ = reject_fn.call1(&JsValue::UNDEFINED, &err);
        }) as Box<dyn FnMut(JsValue)>);

        let _ = promise.then(&on_ok);
        let _ = promise.catch(&on_err);
        holders.borrow_mut().push(on_ok);
        holders.borrow_mut().push(on_err);

        let timeout_abort = abort.cloned();
        let timeout_reject = reject.clone();
        let timeout_holder_for_timeout = timeout_holder.clone();
        let timeout_cb = Closure::wrap(Box::new(move || {
            if let Some(ctrl) = timeout_abort.as_ref() {
                ctrl.abort();
            }
            let err = js_sys::Error::new(&format!("libfw request timed out after {ms}ms"));
            timeout_holder_for_timeout.borrow_mut().take();
            let _ = timeout_reject.call1(&JsValue::UNDEFINED, &err.into());
        }) as Box<dyn FnMut()>);
        let timer_id = window
            .set_timeout_with_callback_and_timeout_and_arguments_0(
                timeout_cb.as_ref().unchecked_ref(),
                ms as i32,
            )
            .expect("setTimeout callback should be registered");
        scheduled.set(Some(timer_id));
        timeout_holder.borrow_mut().replace(timeout_cb);
    })
}

/// Read the entire response body into memory (used for small JSON payloads
/// like directory listings).
pub async fn read_all(
    resp: &Response,
    timeout_ms: u32,
    ctrl: &AbortController,
) -> Result<Vec<u8>, LibfwError> {
    let promise = resp.array_buffer().map_err(|e| {
        LibfwError::Network(format!("arrayBuffer() failed: {}", js_value_string(&e)))
    })?;
    let value = JsFuture::from(with_timeout(promise, timeout_ms, Some(ctrl)))
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
    ctrl: &AbortController,
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
        // the initial connection. The controller abort cancels the whole
        // response body, which is exactly what we want here.
        let value = JsFuture::from(with_timeout(reader.read(), timeout_ms, Some(ctrl)))
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
/// object). When `on_progress` is set it is invoked on every upload progress
/// tick with `(loaded, total)` bytes, so the engine can report **wire-level**
/// progress in real time instead of jumping per completed chunk. The promise
/// resolves with the HTTP status (u16 as f64) and rejects on network error
/// or no-progress timeout.
pub fn xhr_post(
    url: &str,
    header_pairs: &[(String, String)],
    body: &[u8],
    timeout_ms: u32,
    on_progress: Option<Rc<dyn Fn(u64, u64)>>,
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
    // The upload-progress handler has a different closure signature, so it
    // gets its own holder (cleared on the same settle paths as `holders`).
    let progress_holder: Rc<RefCell<Option<Closure<dyn FnMut(web_sys::ProgressEvent)>>>> =
        Rc::new(RefCell::new(None));

    let promise = js_sys::Promise::new(&mut |resolve, reject| {
        let xhr = xhr.clone();
        let window = window.clone();
        let deadline = deadline.clone();
        let finished = finished.clone();
        let watchdog_id = watchdog_id.clone();
        let holders = holders.clone();
        let progress_holder = progress_holder.clone();
        let on_progress = on_progress.clone();
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
            let progress_holder = progress_holder.clone();
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
                    progress_holder.borrow_mut().take();
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

        // Upload body progress: refresh the no-progress deadline and, when a
        // callback was supplied, report wire-level progress (`loaded` bytes
        // of `total` have been handed to the network stack).
        let on_upload_progress = {
            let deadline = deadline.clone();
            let on_progress = on_progress.clone();
            Closure::wrap(Box::new(move |ev: web_sys::ProgressEvent| {
                deadline.set(js_sys::Date::now());
                if let Some(cb) = &on_progress {
                    cb(ev.loaded() as u64, ev.total() as u64);
                }
            }) as Box<dyn FnMut(web_sys::ProgressEvent)>)
        };
        let upload_target: &XmlHttpRequestEventTarget = upload.unchecked_ref();
        upload_target.set_onprogress(Some(on_upload_progress.as_ref().unchecked_ref()));
        *progress_holder.borrow_mut() = Some(on_upload_progress);

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
            let progress_holder = progress_holder.clone();
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
                    progress_holder.borrow_mut().take();
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
            let progress_holder = progress_holder.clone();
            Closure::wrap(Box::new(move || {
                if finished.get() {
                    return;
                }
                finished.set(true);
                window.clear_interval_with_handle(watchdog_id.get());
                let _ = reject.call1(&JsValue::NULL, &JsValue::from_str("libfw upload network error"));
                holders.borrow_mut().clear();
                progress_holder.borrow_mut().take();
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
            let progress_holder = progress_holder.clone();
            Closure::wrap(Box::new(move || {
                if finished.get() {
                    return;
                }
                finished.set(true);
                window.clear_interval_with_handle(watchdog_id.get());
                let _ = reject.call1(&JsValue::NULL, &JsValue::from_str("libfw upload aborted"));
                holders.borrow_mut().clear();
                progress_holder.borrow_mut().take();
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
            progress_holder.borrow_mut().take();
        }
    });

    Ok(promise)
}

/// Header map helper: `Authorization: Bearer <token>` plus the protocol
/// handshake and optional `Accept-Encoding` / zrip level.
///
/// `level` is only sent when `accept_zrip` is true (a level without a zrip
/// request is meaningless — and identity responses never echo one).
pub fn auth_headers(token: &str, accept_zrip: bool, level: Option<i32>) -> Result<Headers, LibfwError> {
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
        if let Some(l) = level {
            headers
                .set(libfw_core::HEADER_COMPRESS_LEVEL, &l.to_string())
                .map_err(|e| LibfwError::Js(format!("set compress-level header failed: {e:?}")))?;
        }
    }
    Ok(headers)
}

/// Fetch the server's capability advertisement (`GET /capabilities`).
///
/// The endpoint is public by design (no credentials needed — the payload is
/// a non-sensitive contract for adaptive clients). A 404 means a legacy
/// server without the route; the caller decides how to fall back.
pub async fn fetch_capabilities(
    base_url: &str,
    timeout_ms: u32,
) -> Result<libfw_core::Capabilities, LibfwError> {
    let url = format!("{}/capabilities", base_url.trim_end_matches('/'));
    let headers = Headers::new()
        .map_err(|e| LibfwError::Js(format!("Headers::new failed: {e:?}")))?;
    let (req, ctrl) = request(&url, "GET", &headers, None)?;
    let resp = fetch(&req, timeout_ms, &ctrl).await?;
    let status = resp.status();
    if status != 200 {
        return Err(LibfwError::Http { status, url });
    }
    let body = read_all(&resp, timeout_ms, &ctrl).await?;
    serde_json::from_slice(&body)
        .map_err(|e| LibfwError::Protocol(format!("bad /capabilities JSON: {e}")))
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
