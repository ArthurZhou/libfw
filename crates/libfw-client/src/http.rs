//! Thin `web-sys` fetch wrapper plus response-body streaming helpers.
//!
//! Everything here runs on the browser's `fetch`/`ReadableStream` APIs so
//! the WASM engine never allocates more than a bounded read buffer per
//! transfer.

use js_sys::Reflect;
use wasm_bindgen::JsCast;
use wasm_bindgen::JsValue;
use wasm_bindgen_futures::JsFuture;
use web_sys::{Headers, Request, RequestInit, Response};

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

/// Perform a `fetch` and return the `Response`.
pub async fn fetch(request: &Request) -> Result<Response, LibfwError> {
    let window = web_sys::window()
        .ok_or_else(|| LibfwError::Js("no window available".into()))?;
    let promise = window.fetch_with_request(request);
    let value = JsFuture::from(promise)
        .await
        .map_err(|e| LibfwError::Network(format!("fetch failed: {}", js_value_string(&e))))?;
    Ok(value.unchecked_into())
}

/// Read the entire response body into memory (used for small JSON payloads
/// like directory listings).
pub async fn read_all(resp: &Response) -> Result<Vec<u8>, LibfwError> {
    let promise = resp.array_buffer().map_err(|e| {
        LibfwError::Network(format!("arrayBuffer() failed: {}", js_value_string(&e)))
    })?;
    let value = JsFuture::from(promise)
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
pub async fn stream_body<F, Fut>(resp: &Response, mut on_chunk: F) -> Result<(), LibfwError>
where
    F: FnMut(Vec<u8>) -> Fut,
    Fut: std::future::Future<Output = Result<(), LibfwError>>,
{
    let body = resp
        .body()
        .ok_or_else(|| LibfwError::Network("response has no body stream".into()))?;
    let reader: web_sys::ReadableStreamDefaultReader = body.get_reader().unchecked_into();
    loop {
        let value = JsFuture::from(reader.read())
            .await
            .map_err(|e| LibfwError::Network(format!("body read failed: {}", js_value_string(&e))))?;
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
