//! Native (non-WASM) client: an async Rust transport for the libfw protocol.
//!
//! This is the "Rust crate" half of libfw-client. It speaks the exact same
//! wire protocol as the browser engine (see [`crate::wasm`]) — resumable
//! `Range` downloads, the tus-style *session* upload protocol, zrip
//! compression and `/capabilities` driven adaptive tuning — but over
//! `tokio` + `reqwest` instead of `fetch`/XHR, so a plain Rust binary,
//! service or script can move files to a libfw server.
//!
//! # What it shares with the browser engine
//!
//! * [`ClientConfig`] — the same knobs (concurrency, windows, chunk size,
//!   retries, timeouts, `auto_tune`, compression policy).
//! * [`crate::tune`] — the same adaptive tuning engine, so "good vs. bad
//!   network" behaviour is identical to the SDK: it ramps the per-file
//!   window, cross-file concurrency and chunk size against the server's
//!   advertised limits and settles where the link saturates. The settle is
//!   remembered in memory and reused by the next transfer of this client.
//! * `libfw-core` — headers, metadata, compression and range math.
//!
//! # What differs from the browser engine
//!
//! * Resume state lives in a small JSON sidecar next to the destination file
//!   instead of IndexedDB (see [`download`]'s `resume` helpers).
//! * Tuning state is **in memory only**, for the lifetime of the client: a
//!   settle is reused by later transfers of the same client, and a new client
//!   always re-measures the link from the advertised minimums. The browser
//!   engine additionally caches its settle in `localStorage` (bounded by
//!   `tune_ttl_ms`, see [`crate::tune::TuningEngine::set_cache`]); the native
//!   client installs no store, so `tune_ttl_ms` has no effect here.
//! * No File System Access API: downloads are written with ordinary file IO.
//! * **Cancellation is structural**: drop the transfer future (or wrap it in
//!   `tokio::select!` / `tokio::time::timeout`). Both directions are
//!   resumable, so an abandoned transfer costs at most the in-flight blocks.
//!
//! # Example
//!
//! ```no_run
//! use libfw_client::{ClientConfig, native::NativeClient};
//!
//! # async fn run() -> Result<(), libfw_client::LibfwError> {
//! let mut config = ClientConfig::default();
//! config.auto_tune = true; // adapt to the link via /capabilities
//!
//! let client = NativeClient::new("http://127.0.0.1:8080", "dev-token", config);
//! let bytes = client.download_file("docs/plan.pdf", "plan.pdf").await?;
//! println!("downloaded {bytes} bytes");
//! # Ok(())
//! # }
//! ```

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use futures::StreamExt;
use libfw_core::capabilities::Capabilities;
use libfw_core::{HEADER_COMPRESS_LEVEL, HEADER_PROTOCOL, protocol_header_value};
use reqwest::header::{ACCEPT_ENCODING, AUTHORIZATION, HeaderMap, HeaderValue};
use serde::Deserialize;

use crate::config::ClientConfig;
use crate::error::LibfwError;
use crate::tune::{
    TuneEvent, TuneParams, TunePhase, TuningEngine, TransferKind, now_ms, resolve_level,
};

mod download;
mod upload;

/// A progress/lifecycle notification (mirrors the SDK's `onEvent` stream).
#[derive(Debug, Clone)]
pub enum NativeEvent {
    /// A file transfer started.
    FileStart {
        /// Virtual path being transferred.
        path: String,
        /// Total size in bytes.
        size: u64,
    },
    /// Cumulative progress for the current transfer (all files).
    Progress {
        /// Bytes transferred so far.
        done: u64,
        /// Total bytes to transfer.
        total: u64,
    },
    /// A file finished.
    FileDone {
        /// Virtual path that completed.
        path: String,
        /// Bytes moved for this file in this run.
        bytes: u64,
    },
    /// The adaptive tuning engine changed state (only with `auto_tune`).
    Tuning(TuneEvent),
    /// A human-readable diagnostic (retries, resume decisions, …).
    Log(String),
}

/// Shared, thread-safe progress sink.
pub type EventSink = Arc<dyn Fn(NativeEvent) + Send + Sync>;

/// Snapshot of the adaptive tuning engine, for logging/metrics.
#[derive(Debug, Clone)]
pub struct NativeTuneStatus {
    /// Lifecycle phase.
    pub phase: TunePhase,
    /// Live parameters (window/concurrency/chunk size/zrip level).
    pub params: TuneParams,
    /// Smoothed link statistics.
    pub stats: crate::tune::TuneStats,
    /// Hash of the `/capabilities` payload the tuning is based on.
    pub caps_hash: String,
}

/// Configuration for [`NativeClient`].
#[derive(Clone)]
pub struct NativeConfig {
    /// Base URL the libfw server is mounted at (e.g. `https://host:8443`).
    pub base_url: String,
    /// Bearer token sent with every request.
    pub token: String,
    /// Transfer knobs — identical to the browser SDK's options.
    pub client: ClientConfig,
    /// Optional progress/lifecycle sink.
    pub on_event: Option<EventSink>,
}

impl std::fmt::Debug for NativeConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeConfig")
            .field("base_url", &self.base_url)
            // Never print the bearer token.
            .field("token", &"<redacted>")
            .field("client", &self.client)
            .field("on_event", &self.on_event.is_some())
            .finish()
    }
}

impl NativeConfig {
    /// Build a configuration for `base_url` + `token` with client defaults.
    pub fn new(base_url: impl Into<String>, token: impl Into<String>) -> NativeConfig {
        NativeConfig {
            base_url: base_url.into(),
            token: token.into(),
            client: ClientConfig::default(),
            on_event: None,
        }
    }

    /// Use an explicit [`ClientConfig`].
    pub fn with_client(mut self, client: ClientConfig) -> NativeConfig {
        self.client = client;
        self
    }

    /// Install a progress/lifecycle sink.
    pub fn with_events(mut self, sink: EventSink) -> NativeConfig {
        self.on_event = Some(sink);
        self
    }
}

/// The native libfw client: one HTTP connection pool + one tuning engine.
///
/// All transfer methods take `&self`, so a single client can be shared across
/// tasks (`Arc<NativeClient>`).
pub struct NativeClient {
    cfg: NativeConfig,
    http: reqwest::Client,
    tune: Mutex<TuningEngine>,
}

impl NativeClient {
    /// Create a client. The HTTP pool is sized by `client.concurrency`, so
    /// the same knob bounds total network parallelism here and in the browser.
    pub fn new(
        base_url: impl Into<String>,
        token: impl Into<String>,
        client: ClientConfig,
    ) -> NativeClient {
        NativeClient::with_config(NativeConfig::new(base_url, token).with_client(client))
    }

    /// Create a client from a full [`NativeConfig`].
    pub fn with_config(cfg: NativeConfig) -> NativeClient {
        let timeout = std::time::Duration::from_millis(cfg.client.timeout_ms as u64);
        let http = reqwest::Client::builder()
            .connect_timeout(timeout)
            // "Nothing has moved for `timeout_ms`" — the same watchdog the
            // browser engine implements — rather than a whole-transfer
            // deadline, so a large file on a slow link is never killed just
            // for taking a long time.
            .read_timeout(timeout)
            .pool_max_idle_per_host(cfg.client.concurrency.max(1) * 2)
            .build()
            .expect("reqwest client construction cannot fail");
        let auto_tune = cfg.client.auto_tune;
        let compress_level = cfg.client.compress_level;
        NativeClient {
            cfg,
            http,
            tune: Mutex::new(TuningEngine::new(auto_tune, compress_level)),
        }
    }

    /// The configured transfer knobs.
    pub fn config(&self) -> &ClientConfig {
        &self.cfg.client
    }

    /// A snapshot of the adaptive tuning engine (phase, live params, stats).
    pub fn tune_status(&self) -> Option<NativeTuneStatus> {
        let tune = self.tune.lock().ok()?;
        if !tune.enabled() {
            return None;
        }
        Some(NativeTuneStatus {
            phase: tune.phase(),
            params: tune.params(),
            stats: tune.stats(),
            caps_hash: tune.caps_hash().to_string(),
        })
    }

    /// The capabilities currently loaded from the server, if any.
    pub fn capabilities(&self) -> Option<Capabilities> {
        self.tune.lock().ok().and_then(|t| t.caps())
    }

    /// List the immediate children of a virtual directory (empty = root).
    pub async fn list(&self, dir: &str) -> Result<Vec<RemoteFile>, LibfwError> {
        list_dir(self, dir).await
    }

    /// Fetch a remote file's authoritative size and ETag (`HEAD`).
    ///
    /// A directory answers `404` (there is no file to stat), which makes this
    /// a cheap way to tell "file" from "folder" before downloading.
    pub async fn stat(&self, remote: &str) -> Result<RemoteStat, LibfwError> {
        validate_relative(remote)?;
        let (etag, size) = self.fetch_meta(remote).await?;
        Ok(RemoteStat { size, etag })
    }

    /// Download every file under `dir` into `dest_dir`, preserving structure.
    ///
    /// Files are fetched with up to `concurrency` concurrent transfers; each
    /// one uses the per-file `download_window` (parallel `Range` GETs) when
    /// the tuning engine says the link needs it.
    pub async fn download_folder(
        &self,
        dir: &str,
        dest_dir: impl AsRef<Path>,
    ) -> Result<u64, LibfwError> {
        let files = collect_remote_files(self, dir).await?;
        let total = files.iter().map(|f| f.size).sum();
        let dest_dir = dest_dir.as_ref().to_path_buf();
        let concurrency = self.effective_concurrency().max(1);

        let results: Vec<Result<u64, LibfwError>> = futures::stream::iter(files.into_iter().map(
            |file| {
                let dest = dest_dir.clone();
                async move {
                    // Keep the virtual path structure under `dest_dir`.
                    let local = join_virtual(&dest, &file.path);
                    self.download_file(&file.path, local).await
                }
            },
        ))
        .buffer_unordered(concurrency)
        .collect()
        .await;

        let mut done = 0u64;
        for result in results {
            done = done.saturating_add(result?);
        }
        self.emit(NativeEvent::Progress { done, total });
        Ok(done)
    }

    /// Upload `files` (local path → virtual remote path pairs).
    ///
    /// Runs `concurrency` files in parallel; each file uses the session
    /// protocol, so an interrupted upload resumes from the server's own
    /// received-range report instead of re-sending everything.
    pub async fn upload_files(
        &self,
        files: Vec<(PathBuf, String)>,
    ) -> Result<u64, LibfwError> {
        let concurrency = self.effective_concurrency().max(1);
        let results: Vec<Result<u64, LibfwError>> =
            futures::stream::iter(files.into_iter().map(|(local, remote)| async move {
                self.upload_file(&local, &remote).await
            }))
            .buffer_unordered(concurrency)
            .collect()
            .await;

        let mut done = 0u64;
        for result in results {
            done = done.saturating_add(result?);
        }
        Ok(done)
    }

    /// Upload a whole local directory tree, mirroring its structure under
    /// `remote_dir` (POSIX separators).
    pub async fn upload_folder(
        &self,
        local_dir: impl AsRef<Path>,
        remote_dir: &str,
    ) -> Result<u64, LibfwError> {
        let root = local_dir.as_ref().to_path_buf();
        let mut files = Vec::new();
        for entry in walk_files(&root)? {
            let rel = entry
                .strip_prefix(&root)
                .unwrap_or(&entry)
                .to_string_lossy()
                .replace('\\', "/");
            let remote = if remote_dir.is_empty() {
                rel
            } else {
                format!("{}/{}", remote_dir.trim_matches('/'), rel)
            };
            files.push((entry, remote));
        }
        self.upload_files(files).await
    }

    // ---------------------------------------------------------------- internals

    /// Emit an event (no-op without a sink).
    pub(crate) fn emit(&self, event: NativeEvent) {
        if let Some(sink) = &self.cfg.on_event {
            sink(event);
        }
    }

    /// Log a diagnostic through the event sink.
    pub(crate) fn log(&self, msg: impl Into<String>) {
        self.emit(NativeEvent::Log(msg.into()));
    }

    pub(crate) fn base_url(&self) -> &str {
        &self.cfg.base_url
    }

    pub(crate) fn token(&self) -> &str {
        &self.cfg.token
    }

    pub(crate) fn http(&self) -> &reqwest::Client {
        &self.http
    }

    /// Current cross-file concurrency (the tuned value when adaptive tuning is
    /// on, otherwise the configured value).
    pub(crate) fn effective_concurrency(&self) -> usize {
        match self.tune.lock() {
            Ok(t) if t.enabled() => t.params().concurrency.max(1),
            _ => self.cfg.client.concurrency.max(1),
        }
    }

    /// Run `f` with a mutable borrow of the tuning engine.
    pub(crate) fn with_tune<R>(&self, f: impl FnOnce(&mut TuningEngine) -> R) -> Option<R> {
        self.tune.lock().ok().map(|mut t| f(&mut t))
    }

    /// The transfer preamble shared by uploads and downloads: resolve the
    /// zrip level policy, optionally consult `/capabilities`, then let the
    /// tuning engine pick the starting parameters.
    ///
    /// Mirrors `wasm::prepare_transfer` so both clients adapt identically.
    pub(crate) async fn prepare_transfer(
        &self,
        direction: TransferKind,
    ) -> Result<TuneParams, LibfwError> {
        let caps = if self.cfg.client.auto_tune {
            match self.fetch_capabilities().await {
                Ok(caps) => caps,
                // A legacy server without the route → tuning off, defaults on.
                Err(LibfwError::Http { status: 404, .. }) => {
                    self.with_tune(|t| t.set_enabled(false));
                    Capabilities::default()
                }
                Err(e) => return Err(e),
            }
        } else {
            Capabilities::default()
        };

        let level = resolve_level(self.cfg.client.compress_level, &caps);
        let static_params = TuneParams::from_config(
            self.cfg.client.concurrency,
            self.cfg.client.upload_window,
            self.cfg.client.download_window,
            self.cfg.client.chunk_size,
            level,
            &caps,
        );
        let params = self
            .with_tune(|t| {
                t.set_direction(direction);
                t.begin_transfer(&caps, now_ms(), &static_params)
            })
            .unwrap_or(static_params);
        Ok(params)
    }

    /// Feed one measurement window to the tuning engine and re-emit events.
    pub(crate) fn tune_tick(&self, done_bytes: u64, rtt_ms: Option<f64>, error: bool) {
        if let Some(Some(event)) = self.with_tune(|t| t.tick(now_ms(), done_bytes, rtt_ms, error)) {
            self.emit(NativeEvent::Tuning(event));
        }
    }

    /// Mark the transfer outcome: a failure forgets the in-session settle, so
    /// the next transfer re-ramps instead of reusing parameters that failed.
    pub(crate) fn finish_transfer(&self, ok: bool) {
        let _ = self.with_tune(|t| t.transfer_end(ok));
    }

    /// The zrip level an upload should use (policy, or the micro-benchmarked
    /// `Auto` result once a sample has been measured).
    pub(crate) fn upload_level(&self, caps: &Capabilities, sample: Option<&[u8]>, mbps: f64) -> i32 {
        match self.with_tune(|t| t.upload_compress_level(caps, sample, mbps)) {
            Some(level) => level,
            None => resolve_level(self.cfg.client.compress_level, caps),
        }
    }

    /// `GET /capabilities` — the server's advertised limits and codecs.
    ///
    /// This is a network call (the endpoint is public, no credentials
    /// needed); [`NativeClient::capabilities`] returns the cached copy the
    /// tuning engine loaded during the last transfer.
    pub async fn fetch_capabilities(&self) -> Result<Capabilities, LibfwError> {
        let url = format!("{}/capabilities", self.cfg.base_url.trim_end_matches('/'));
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| LibfwError::Network(e.to_string()))?;
        let status = resp.status().as_u16();
        if status != 200 {
            return Err(LibfwError::Http { status, url });
        }
        let body = resp
            .bytes()
            .await
            .map_err(|e| LibfwError::Network(e.to_string()))?;
        serde_json::from_slice(&body)
            .map_err(|e| LibfwError::Protocol(format!("bad /capabilities JSON: {e}")))
    }

    /// Build the standard request headers (auth + protocol handshake), with
    /// optional zrip negotiation.
    pub(crate) fn headers(&self, accept_zrip: bool, level: Option<i32>) -> HeaderMap {
        let mut headers = HeaderMap::new();
        if let Ok(value) = HeaderValue::from_str(&format!("Bearer {}", self.cfg.token)) {
            headers.insert(AUTHORIZATION, value);
        }
        headers.insert(HEADER_PROTOCOL, HeaderValue::from_static(protocol_header_value()));
        if accept_zrip {
            headers.insert(ACCEPT_ENCODING, HeaderValue::from_static("zrip"));
            if let Some(level) = level {
                if let Ok(value) = HeaderValue::from_str(&level.to_string()) {
                    headers.insert(HEADER_COMPRESS_LEVEL, value);
                }
            }
        }
        headers
    }

    /// Absolute URL for a virtual file path.
    pub(crate) fn file_url(&self, path: &str) -> String {
        format!(
            "{}/file/{}",
            self.cfg.base_url.trim_end_matches('/'),
            encode_path(path)
        )
    }

    /// Absolute URL for a virtual directory listing.
    pub(crate) fn dir_url(&self, path: &str) -> String {
        let base = self.cfg.base_url.trim_end_matches('/');
        if path.is_empty() {
            format!("{base}/dir")
        } else {
            format!("{base}/dir/{}", encode_path(path))
        }
    }

    /// Exponential backoff for a failed attempt (0-based).
    pub(crate) fn backoff(&self, attempt: u32) -> std::time::Duration {
        std::time::Duration::from_millis(self.cfg.client.backoff_ms(attempt) as u64)
    }
}

/// Percent-encode a virtual path, preserving `/` separators.
///
/// Mirrors the browser engine's `encode_path` (RFC 3986 unreserved set) so
/// both clients produce identical URLs.
pub(crate) fn encode_path(path: &str) -> String {
    let mut out = String::with_capacity(path.len());
    for byte in path.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' | b'/' => {
                out.push(*byte as char)
            }
            other => out.push_str(&format!("%{other:02X}")),
        }
    }
    out
}

/// Join a virtual (POSIX) path onto a local directory.
pub(crate) fn join_virtual(base: &Path, virtual_path: &str) -> PathBuf {
    let mut out = base.to_path_buf();
    for segment in virtual_path.split('/').filter(|s| !s.is_empty() && *s != ".") {
        out.push(segment);
    }
    out
}

/// Reject paths that would escape the destination root (`..`, absolute).
pub(crate) fn validate_relative(path: &str) -> Result<(), LibfwError> {
    if path.starts_with('/') {
        return Err(LibfwError::Protocol(format!(
            "server returned an absolute path: `{path}`"
        )));
    }
    for segment in path.split('/') {
        if segment == ".." || segment.contains('\\') {
            return Err(LibfwError::Protocol(format!(
                "refusing unsafe path from server: `{path}`"
            )));
        }
    }
    Ok(())
}

/// Recursively walk `root`, returning every regular file below it.
fn walk_files(root: &Path) -> Result<Vec<PathBuf>, LibfwError> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let entries = std::fs::read_dir(&dir)
            .map_err(|e| LibfwError::Storage(format!("read_dir {}: {e}", dir.display())))?;
        for entry in entries {
            let entry =
                entry.map_err(|e| LibfwError::Storage(format!("dir entry: {e}")))?;
            let file_type = entry
                .file_type()
                .map_err(|e| LibfwError::Storage(format!("file type: {e}")))?;
            let path = entry.path();
            if file_type.is_symlink() {
                // Never follow symlinks (matches the server's storage rules).
                continue;
            }
            if file_type.is_dir() {
                stack.push(path);
            } else if file_type.is_file() {
                out.push(path);
            }
        }
    }
    out.sort();
    Ok(out)
}

/// A directory listing entry as served by `GET /dir/...`.
#[derive(Debug, Clone, Deserialize)]
pub struct RemoteFile {
    /// Full virtual path relative to the mounted root.
    pub path: String,
    /// Whether this entry is a directory.
    #[serde(rename = "is_dir", alias = "isDir")]
    pub is_dir: bool,
    /// Byte size (0 for directories).
    #[serde(default)]
    pub size: u64,
    /// Last-modified unix time.
    #[serde(default)]
    pub mtime: u64,
}

impl RemoteFile {
    /// Convert into the crate's [`crate::plan::FileEntry`] shape.
    pub(crate) fn to_file_entry(&self) -> crate::plan::FileEntry {
        crate::plan::FileEntry {
            path: self.path.clone(),
            size: self.size,
            mtime: self.mtime,
        }
    }
}

/// Metadata of a remote file, as returned by [`NativeClient::stat`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteStat {
    /// Authoritative size in bytes (the server reads the real file).
    pub size: u64,
    /// Content identifier for resume validation (server-computed).
    pub etag: String,
}

/// List the immediate children of a virtual directory.
pub(crate) async fn list_dir(
    client: &NativeClient,
    dir: &str,
) -> Result<Vec<RemoteFile>, LibfwError> {
    let url = client.dir_url(dir);
    let resp = client
        .http()
        .get(&url)
        .headers(client.headers(false, None))
        .send()
        .await
        .map_err(|e| LibfwError::Network(e.to_string()))?;
    let status = resp.status().as_u16();
    if status != 200 {
        return Err(LibfwError::Http { status, url });
    }
    let body = resp
        .bytes()
        .await
        .map_err(|e| LibfwError::Network(e.to_string()))?;
    serde_json::from_slice(&body)
        .map_err(|e| LibfwError::Protocol(format!("bad listing JSON: {e}")))
}

/// Collect every file under `dir` (iterative DFS so deep trees are safe).
pub(crate) async fn collect_remote_files(
    client: &NativeClient,
    dir: &str,
) -> Result<Vec<crate::plan::FileEntry>, LibfwError> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_string()];
    while let Some(current) = stack.pop() {
        for entry in list_dir(client, &current).await? {
            validate_relative(&entry.path)?;
            if entry.is_dir {
                stack.push(entry.path);
            } else {
                out.push(entry.to_file_entry());
            }
        }
    }
    out.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(out)
}

/// Compress `raw` the way the wire protocol expects: one zstd stream made of
/// many ~64 KiB independent frames, so no single frame can exceed the
/// server's per-frame cap regardless of the chunk size.
pub(crate) fn compress_chunk(raw: &[u8], level: i32) -> Result<Vec<u8>, LibfwError> {
    use libfw_core::compress::{CompressionFormat, compressor_with_level};
    let mut enc = compressor_with_level(CompressionFormat::Zrip, level)?;
    let mut out = Vec::with_capacity(raw.len());
    for window in raw.chunks(libfw_core::STREAM_BUF_SIZE) {
        enc.compress(window, &mut out)?;
    }
    enc.finish(&mut out)?;
    Ok(out)
}

/// The protocol's deterministic session id for a file version.
pub(crate) fn session_id_for(meta: &libfw_core::metadata::FileMeta) -> String {
    meta.etag.trim_matches('"').to_string()
}

/// `true` when a status means "the remote file changed"/"shrank" and the
/// transfer must restart from byte 0 rather than retry.
pub(crate) fn is_restart_err(e: &LibfwError) -> bool {
    matches!(
        e,
        LibfwError::Http {
            status: 416 | 200,
            ..
        }
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_path_preserves_slashes_and_encodes_specials() {
        assert_eq!(encode_path("a b/c d.txt"), "a%20b/c%20d.txt");
        assert_eq!(encode_path("simple/file.txt"), "simple/file.txt");
        assert_eq!(encode_path("dir/中文.pdf"), "dir/%E4%B8%AD%E6%96%87.pdf");
        // Reserved characters must not survive unescaped.
        assert_eq!(encode_path("a?b#c"), "a%3Fb%23c");
    }

    #[test]
    fn join_virtual_builds_nested_paths() {
        let base = Path::new("/tmp/dest");
        assert_eq!(
            join_virtual(base, "a/b/c.txt"),
            PathBuf::from("/tmp/dest/a/b/c.txt")
        );
        assert_eq!(join_virtual(base, ""), PathBuf::from("/tmp/dest"));
    }

    #[test]
    fn validate_relative_rejects_traversal() {
        assert!(validate_relative("a/b.txt").is_ok());
        assert!(validate_relative("../etc/passwd").is_err());
        assert!(validate_relative("a/../../b").is_err());
        assert!(validate_relative("/abs").is_err());
        assert!(validate_relative("a\\b").is_err());
    }

    #[test]
    fn compress_chunk_splits_into_small_frames() {
        use libfw_core::compress::{CompressionFormat, decompressor};
        // 3 MiB > the server's 2 MiB frame cap: must still round-trip.
        let raw = vec![0x5Au8; 3 * 1024 * 1024];
        let payload = compress_chunk(&raw, 1).unwrap();
        assert!(!payload.is_empty());
        let mut dec = decompressor(CompressionFormat::Zrip);
        let mut out = Vec::new();
        dec.decompress(&payload, &mut out).unwrap();
        dec.finish(&mut out).unwrap();
        assert_eq!(out, raw);
    }

    #[test]
    fn session_id_strips_etag_quotes() {
        let meta = libfw_core::metadata::FileMeta::new("a.txt", 1, 2);
        let session = session_id_for(&meta);
        assert!(!session.contains('"'));
        assert_eq!(session, meta.etag.trim_matches('"'));
    }
}
