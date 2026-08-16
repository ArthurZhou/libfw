//! Adaptive tuning engine: TCP-style ramp over *real* transfers.
//!
//! # Model
//!
//! The engine treats each transfer as a slow-start probe: measurements are
//! taken from the bytes the transfer actually moves (no synthetic probe
//! requests), and the shared parameter table is ramped up dimension by
//! dimension — per-file window first, then cross-file concurrency, then
//! chunk size — until the link saturates, then settled and persisted to
//! `localStorage` (via a [`TuneStore`]) for reuse by later transfers.
//!
//! # Design notes
//!
//! - **Pure decision logic**: [`ramp_action`], [`choose_auto_level`],
//!   [`origin_hash`] and the clamp/EWMA helpers are plain functions with
//!   table-driven tests. Everything that touches the clock or storage is
//!   injected, so native tests never need a browser.
//! - **Shared state**: one [`TuningEngine`] per client, wrapped in
//!   `Rc<RefCell<_>>` (WASM is single-threaded). All transfers read the
//!   current params when they schedule work.
//! - **One-second windows**: [`TuningEngine::tick`] accumulates bytes / RTT
//!   samples / errors and evaluates exactly one window per second of wall
//!   clock, so measurements are comparable across links.
//! - **Persistence**: settled params are stored under
//!   `libfw.tune.<originHash>` with a TTL and the server's `capsHash`;
//!   stale entries, hash mismatches and repeated failures invalidate it.

use std::cell::RefCell;
use std::rc::Rc;

use libfw_core::Capabilities;
use serde::{Deserialize, Serialize};
use wasm_bindgen::JsValue;

/// Measurement window length (ms).
pub const TUNE_WINDOW_MS: f64 = 1_000.0;
/// Throughput growth that justifies raising a dimension (≥ +5%).
pub const GAIN_THRESHOLD: f64 = 0.05;
/// RTT inflation (vs the ramping baseline) that signals saturation (> 30%).
pub const RTT_INFLATE_THRESHOLD: f64 = 0.30;
/// How far a dimension drops on errors (× 0.5).
pub const DEGRADE_FACTOR: f64 = 0.5;
/// EWMA smoothing for RTT samples.
pub const RTT_EWMA_ALPHA: f64 = 0.25;
/// Default TTL of a persisted tuning cache (1 hour).
pub const DEFAULT_TUNE_TTL_MS: u64 = 3_600_000;
/// Cache schema version — bump to invalidate all persisted entries.
pub const CACHE_VERSION: u32 = 1;
/// A transfer must have completed at least this many windows before its
/// settled params are persisted (short transfers are measurement noise).
pub const MIN_WINDOWS_TO_PERSIST: u32 = 2;
/// RTT deviation (vs cached stats) that forces a re-ramp (> 50%).
pub const RTT_DRIFT_RE_RAMP: f64 = 0.50;
/// Consecutive failed transfers that invalidate the cache.
pub const MAX_CONSECUTIVE_FAILURES: u32 = 3;
/// Sample size for the compression-level micro-benchmark.
///
/// 64 KiB rather than 256 KiB: the benchmark runs synchronously on the WASM
/// main thread (no worker threads available), so a large sample would block
/// the JS event loop for a noticeable period (50 ms+) on slow devices. 64 KiB
/// is sufficient for zstd's dictionary-learning stage to converge and gives a
/// representative ratio without the UI stutter. The caller is expected to
/// pass `sample[..LEVEL_SAMPLE_SIZE.min(sample.len())]`.
pub const LEVEL_SAMPLE_SIZE: usize = 64 * 1024;

// ---------------------------------------------------------------------------
// Data model
// ---------------------------------------------------------------------------

/// Lifecycle of the tuning state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TunePhase {
    /// No transfer has run yet in this session.
    Uninitialized,
    /// Ramping parameters up from the advertised minimums.
    Ramping,
    /// Parameters converged; served from cache on later transfers.
    Settled,
    /// An error shrank the parameters; waiting for stable windows.
    Degraded,
}

/// Smoothed transfer statistics.
#[derive(Debug, Clone, Copy, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TuneStats {
    /// EWMA of chunk-request time-to-first-byte (ms).
    pub rtt_ms: f64,
    /// Last-window throughput (Mbps).
    pub mbps: f64,
}

/// The shared, live parameter table.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TuneParams {
    /// Cross-file fan-out / global in-flight request cap.
    pub concurrency: usize,
    /// Per-file upload in-flight chunk window.
    pub upload_window: usize,
    /// Per-file download in-flight chunk window.
    pub download_window: usize,
    /// Upload chunk size (bytes).
    pub chunk_size: u64,
    /// Download byte-range chunk size (bytes).
    pub download_chunk_size: u64,
    /// zrip level for this transfer's compressed body.
    pub compress_level: i32,
}

impl TuneParams {
    /// The parameter table at the server's advertised minimums (ramp start).
    pub fn from_caps_mins(caps: &Capabilities) -> TuneParams {
        TuneParams {
            concurrency: caps.limits.concurrency.min.max(1) as usize,
            upload_window: caps.limits.upload_window.min.max(1) as usize,
            download_window: caps.limits.download_window.min.max(1) as usize,
            chunk_size: caps.limits.chunk_size.min.max(1) as u64,
            download_chunk_size: caps.limits.download_chunk_size.min.max(1) as u64,
            compress_level: caps.compression.zrip_levels.min,
        }
    }

    /// A static table derived from the client configuration (no tuning:
    /// legacy behavior, but still level-resolved and clamped to the caps).
    pub fn from_config(
        concurrency: usize,
        upload_window: usize,
        download_window: usize,
        chunk_size: u64,
        download_chunk_size: u64,
        compress_level: i32,
        caps: &Capabilities,
    ) -> TuneParams {
        TuneParams {
            concurrency: caps.limits.concurrency.clamp(concurrency.max(1) as i64).max(1) as usize,
            upload_window: caps.limits.upload_window.clamp(upload_window.max(1) as i64).max(1)
                as usize,
            download_window: caps
                .limits
                .download_window
                .clamp(download_window.max(1) as i64)
                .max(1) as usize,
            chunk_size: caps.limits.chunk_size.clamp(chunk_size.max(1) as i64).max(1) as u64,
            download_chunk_size: caps
                .limits
                .download_chunk_size
                .clamp(download_chunk_size.max(1) as i64)
                .max(1) as u64,
            compress_level: caps.clamp_level(compress_level),
        }
    }

    /// Clamp every dimension into the server's advertised ranges (defensive
    /// against a server that shrank its caps since we cached params).
    pub fn clamped_into(&self, caps: &Capabilities) -> TuneParams {
        TuneParams {
            concurrency: caps.limits.concurrency.clamp(self.concurrency as i64).max(1) as usize,
            upload_window: caps.limits.upload_window.clamp(self.upload_window as i64).max(1)
                as usize,
            download_window: caps
                .limits
                .download_window
                .clamp(self.download_window as i64)
                .max(1) as usize,
            chunk_size: caps.limits.chunk_size.clamp(self.chunk_size as i64).max(1) as u64,
            download_chunk_size: caps
                .limits
                .download_chunk_size
                .clamp(self.download_chunk_size as i64)
                .max(1) as u64,
            compress_level: caps.clamp_level(self.compress_level),
        }
    }

    /// In-flight bytes for an upload: concurrency × window × chunk size.
    pub fn upload_in_flight(&self) -> u64 {
        self.concurrency as u64 * self.upload_window as u64 * self.chunk_size
    }

    /// In-flight bytes for a download: concurrency × window × chunk size.
    pub fn download_in_flight(&self) -> u64 {
        self.concurrency as u64 * self.download_window as u64 * self.download_chunk_size
    }
}

/// Persisted tuning cache entry (`localStorage["libfw.tune.<originHash>"]`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TuneCache {
    pub v: u32,
    /// Settle timestamp (epoch ms).
    pub ts: u64,
    /// Capabilities hash this entry was tuned against.
    pub caps_hash: String,
    pub params: TuneParams,
    pub stats: TuneStats,
}

// ---------------------------------------------------------------------------
// Storage
// ---------------------------------------------------------------------------

/// Injected persistence backend (localStorage in the browser; a map in tests).
pub trait TuneStore {
    fn get(&self, key: &str) -> Option<String>;
    fn set(&self, key: &str, value: &str);
    fn remove(&self, key: &str);
}

/// localStorage-backed store (only reachable in the browser).
pub struct LocalStore;

impl TuneStore for LocalStore {
    fn get(&self, key: &str) -> Option<String> {
        web_sys::window()?
            .local_storage()
            .ok()
            .flatten()?
            .get_item(key)
            .ok()
            .flatten()
    }

    fn set(&self, key: &str, value: &str) {
        if let Some(storage) = web_sys::window().and_then(|w| w.local_storage().ok().flatten()) {
            let _ = storage.set_item(key, value);
        }
    }

    fn remove(&self, key: &str) {
        if let Some(storage) = web_sys::window().and_then(|w| w.local_storage().ok().flatten()) {
            let _ = storage.remove_item(key);
        }
    }
}

/// The localStorage key for a base URL: `libfw.tune.<originHash>`.
///
/// Keyed by **origin** (not base path): the same server advertises the same
/// capabilities regardless of mount point, so all its paths share one cache.
pub fn tune_key(base_url: &str) -> String {
    format!("libfw.tune.{}", origin_hash(base_url))
}

/// Stable content hash of a base URL's origin (scheme + host + port).
pub fn origin_hash(base_url: &str) -> String {
    use sha2::Digest;
    let origin = extract_origin(base_url);
    let digest = sha2::Sha256::digest(origin.as_bytes());
    format!("sha256-{}", hex_lower(&digest))
}

/// `scheme://host[:port]` (lowercased, userinfo stripped). Pure string work
/// so native tests exercise it without a browser `URL` object.
fn extract_origin(url: &str) -> String {
    match url.find("://") {
        Some(i) => {
            let scheme = &url[..i];
            let rest = &url[i + 3..];
            let host_port = rest.split('/').next().unwrap_or("");
            // Strip userinfo (`user:pass@host`) just in case.
            let host_port = host_port.rsplit('@').next().unwrap_or(host_port);
            format!(
                "{}://{}",
                scheme.to_ascii_lowercase(),
                host_port.to_ascii_lowercase()
            )
        }
        None => url.to_ascii_lowercase(),
    }
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

// ---------------------------------------------------------------------------
// Pure helpers
// ---------------------------------------------------------------------------

/// EWMA of `sample` into `prev` (`None` seeds the first sample).
pub fn ewma(prev: Option<f64>, sample: f64, alpha: f64) -> f64 {
    match prev {
        None => sample,
        Some(p) => alpha * sample + (1.0 - alpha) * p,
    }
}

/// Bandwidth-delay product in bytes from Mbps and RTT ms.
pub fn bdp_bytes(mbps: f64, rtt_ms: f64) -> u64 {
    (mbps * 1e6 / 8.0 * rtt_ms / 1_000.0).max(0.0) as u64
}

/// The dimension currently being raised.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RampDim {
    /// The active transfer's per-file window (upload or download).
    Window,
    /// Cross-file concurrency (global in-flight request cap).
    Concurrency,
    /// The active transfer's chunk size.
    ChunkSize,
}

/// What a window evaluation decided to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RampAction {
    /// Raise the current dimension (×2, or +1 near the cap).
    Raise,
    /// Current dimension is capped — move to the next one.
    AdvanceDim,
    /// The link is saturated (or stable long enough): settle.
    Settle,
    /// An error happened: halve the current dimension, phase → Degraded.
    Degrade,
    /// Below the gain threshold but not yet conclusive: hold.
    Hold,
}

/// Inputs to the ramp decision (one measurement window).
#[derive(Debug, Clone, Copy)]
pub struct RampInput {
    /// Throughput of the window just closed (Mbps).
    pub mbps: f64,
    /// Throughput of the previous window (0 on the first window).
    pub prev_mbps: f64,
    /// `rtt_ewma / rtt_baseline - 1` (0 when no baseline exists yet).
    pub rtt_inflation: f64,
    /// Errors observed in this window.
    pub errors: u32,
    /// Engine is in the post-degrade stability hold.
    pub degraded: bool,
    /// Consecutive windows below the gain threshold (before this one).
    pub low_gain_windows: u32,
    /// Current in-flight bytes (concurrency × window × chunk).
    pub in_flight: u64,
    /// Bandwidth-delay product estimate (bytes); 0 = unknown.
    pub bdp: u64,
    /// Whether the current dimension is at the server's max.
    pub at_cap: bool,
    /// Whether the current dimension is the last one to ramp.
    pub last_dim: bool,
}

/// The pure ramp decision for one measurement window.
///
/// Returns `(action, new_low_gain_windows)`.
///
/// Rules (in priority order):
/// 1. any error → [`RampAction::Degrade`];
/// 2. current dimension capped → advance (or settle at the last one);
/// 3. in-flight ≥ BDP → saturated, settle;
/// 4. RTT inflated > 30% vs baseline → saturated, settle;
/// 5. throughput gain ≥ +5% → raise (except during the degraded hold);
/// 6. gain < 5% for 2 consecutive windows → settle;
/// 7. otherwise hold.
pub fn ramp_action(input: &RampInput) -> (RampAction, u32) {
    if input.errors > 0 {
        return (RampAction::Degrade, 0);
    }
    if input.at_cap {
        return if input.last_dim {
            (RampAction::Settle, 0)
        } else {
            (RampAction::AdvanceDim, 0)
        };
    }
    if input.bdp > 0 && input.in_flight >= input.bdp {
        return (RampAction::Settle, 0);
    }
    if input.rtt_inflation > RTT_INFLATE_THRESHOLD {
        return (RampAction::Settle, 0);
    }
    let gain = if input.degraded {
        // Degraded stability hold: the window just saw (near-)zero traffic,
        // so `prev_mbps` may be 0 — the first-window pseudo-gain below must
        // not re-raise dimensions we just halved. Hold and settle instead.
        0.0
    } else if input.prev_mbps > 0.0 {
        input.mbps / input.prev_mbps - 1.0
    } else {
        // First window: no baseline; treat the mere presence of traffic as
        // growth so the first raise happens immediately.
        1.0
    };
    if gain >= GAIN_THRESHOLD {
        return (RampAction::Raise, 0);
    }
    let low = input.low_gain_windows + 1;
    if low >= 2 {
        (RampAction::Settle, low)
    } else {
        (RampAction::Hold, low)
    }
}

/// Client-side compression level configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressLevel {
    /// Micro-benchmark the advertised range on a real file sample.
    Auto,
    /// Cheapest (min) — least CPU.
    Fast,
    /// The server's advertised default.
    Balanced,
    /// Best ratio (max).
    Max,
    /// An explicit level (clamped into the advertised range).
    Fixed(i32),
}

/// Resolve a [`CompressLevel`] against the advertised zrip range.
///
/// `Auto` without a benchmarkable sample degrades to the server default.
pub fn resolve_level(level: CompressLevel, caps: &Capabilities) -> i32 {
    let z = caps.compression.zrip_levels;
    match level {
        CompressLevel::Fast => z.min,
        CompressLevel::Balanced | CompressLevel::Auto => z.default,
        CompressLevel::Max => z.max,
        CompressLevel::Fixed(l) => z.clamp_level(l),
    }
}

/// Choose the best level from micro-benchmark results.
///
/// `candidates`: `(level, compressed_len, compress_ms)` in ascending level
/// order. Scores each by `saved_transfer_ms - compress_ms`, where
/// `saved_transfer_ms` converts the saved bytes at the current link speed.
/// Ties go to the cheaper (lower) level. A sample that compresses to
/// nothing (already-compressed data) yields the cheapest level so the CPU
/// is never wasted.
pub fn choose_auto_level(
    candidates: &[(i32, usize, f64)],
    uncompressed: usize,
    mbps: f64,
) -> i32 {
    if candidates.is_empty() {
        return 0;
    }
    // Bytes per ms at the current (or a conservative default) link speed.
    let bytes_per_ms = if mbps > 0.0 {
        mbps * 1e6 / 8.0 / 1_000.0
    } else {
        // 10 Mbps default: don't over-invest CPU on an unknown link.
        10e6 / 8.0 / 1_000.0
    };
    let max_saved = candidates
        .iter()
        .map(|&(_, clen, _)| uncompressed.saturating_sub(clen))
        .max()
        .unwrap_or(0);
    if max_saved == 0 {
        // Incompressible sample: cheapest level.
        return candidates.iter().map(|&(l, _, _)| l).min().unwrap_or(0);
    }
    let mut best = (candidates[0].0, f64::NEG_INFINITY);
    for &(lvl, clen, cms) in candidates {
        let saved = uncompressed.saturating_sub(clen) as f64;
        let score = saved / bytes_per_ms - cms;
        // Strict `>`: on ties the earlier (cheaper) candidate wins.
        if score > best.1 {
            best = (lvl, score);
        }
    }
    best.0
}

// ---------------------------------------------------------------------------
// Engine
// ---------------------------------------------------------------------------

/// A tuning event emitted when a window evaluation changed the state.
#[derive(Debug, Clone)]
pub struct TuneEvent {
    pub phase: TunePhase,
    pub params: TuneParams,
    pub stats: TuneStats,
}

/// Shared handle to the tuning engine (Rc so clones share one table).
pub type TuneHandle = Rc<RefCell<TuningEngine>>;

/// The tuning state machine.
pub struct TuningEngine {
    enabled: bool,
    ttl_ms: u64,
    level_cfg: CompressLevel,
    phase: TunePhase,
    caps: Option<Capabilities>,
    caps_hash: String,
    params: TuneParams,
    stats: TuneStats,
    /// Cached auto-level result for this session (per-file benchmark).
    auto_level: Option<i32>,
    /// Whether the auto level was benchmarked this session.
    auto_level_benchmarked: bool,
    // Window accumulator.
    window_start_ms: f64,
    window_done_base: u64,
    window_errors: u32,
    windows_closed: u32,
    rtt_ewma: Option<f64>,
    prev_mbps: f64,
    low_gain_windows: u32,
    dim: RampDim,
    degraded_windows: u32,
    consecutive_failures: u32,
    /// Whether we settled from a Degraded hold (not from a full ramp).
    /// When true, `transfer_end` skips cache persistence so a halved
    /// parameter set is never written as the settled baseline.
    post_degrade_settle: bool,
    /// Which direction is ramping (drives window/chunk dimension selection).
    direction: TransferKind,
    // JS callback for tuning events (SDK `onTuning`).
    on_tuning: Option<js_sys::Function>,
}

impl TuningEngine {
    /// Create the engine from client configuration.
    pub fn new(enabled: bool, ttl_ms: u64, level_cfg: CompressLevel) -> TuningEngine {
        TuningEngine {
            enabled,
            ttl_ms,
            level_cfg,
            phase: TunePhase::Uninitialized,
            caps: None,
            caps_hash: String::new(),
            // Placeholder; replaced on the first begin_transfer.
            params: TuneParams {
                concurrency: 1,
                upload_window: 1,
                download_window: 1,
                chunk_size: 1,
                download_chunk_size: 1,
                compress_level: 0,
            },
            stats: TuneStats::default(),
            auto_level: None,
            auto_level_benchmarked: false,
            window_start_ms: 0.0,
            window_done_base: 0,
            window_errors: 0,
            windows_closed: 0,
            rtt_ewma: None,
            prev_mbps: 0.0,
            low_gain_windows: 0,
            dim: RampDim::Window,
            degraded_windows: 0,
            consecutive_failures: 0,
            post_degrade_settle: false,
            direction: TransferKind::Download,
            on_tuning: None,
        }
    }

    /// Record which direction the coming transfer tunes (sets which
    /// window/chunk-size dimensions the ramp moves).
    pub fn set_direction(&mut self, kind: TransferKind) {
        self.direction = kind;
    }

    /// Current direction.
    pub fn direction(&self) -> TransferKind {
        self.direction
    }

    /// Install the JS `onTuning(phase, params, stats)` callback.
    pub fn set_on_tuning(&mut self, cb: Option<js_sys::Function>) {
        self.on_tuning = cb;
    }

    /// Current phase.
    pub fn phase(&self) -> TunePhase {
        self.phase
    }

    /// Current live parameters.
    pub fn params(&self) -> TuneParams {
        self.params.clone()
    }

    /// Current stats.
    pub fn stats(&self) -> TuneStats {
        self.stats
    }

    /// Whether tuning is active (autoTune on).
    pub fn enabled(&self) -> bool {
        self.enabled
    }

    /// Force-disable tuning for the rest of this client's lifetime (used
    /// when the server has no `/capabilities` route — a legacy peer).
    pub fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
    }

    /// The capabilities currently loaded for this session.
    pub fn caps(&self) -> Option<Capabilities> {
        self.caps.clone()
    }

    /// Snapshot the engine state as a JS object for the SDK:
    /// `{ phase, params, stats }`.
    pub fn state_js(&self) -> JsValue {
        let o = js_sys::Object::new();
        let phase = match self.phase {
            TunePhase::Uninitialized => "uninitialized",
            TunePhase::Ramping => "ramping",
            TunePhase::Settled => "settled",
            TunePhase::Degraded => "degraded",
        };
        let _ = js_sys::Reflect::set(&o, &JsValue::from_str("phase"), &JsValue::from_str(phase));
        let _ = js_sys::Reflect::set(&o, &JsValue::from_str("params"), &params_to_js(&self.params));
        let stats = js_sys::Object::new();
        let _ = js_sys::Reflect::set(
            &stats,
            &JsValue::from_str("rttMs"),
            &JsValue::from_f64(self.stats.rtt_ms),
        );
        let _ = js_sys::Reflect::set(
            &stats,
            &JsValue::from_str("mbps"),
            &JsValue::from_f64(self.stats.mbps),
        );
        let _ = js_sys::Reflect::set(&o, &JsValue::from_str("stats"), &stats.into());
        let _ = js_sys::Reflect::set(
            &o,
            &JsValue::from_str("capsHash"),
            &JsValue::from_str(&self.caps_hash),
        );
        o.into()
    }

    /// The capabilities hash we are currently tuned against.
    pub fn caps_hash(&self) -> &str {
        &self.caps_hash
    }

    /// Start a transfer: consult the cache, then either reuse (Settled) or
    /// reset to the advertised minimums (Ramping).
    ///
    /// Returns the parameters the transfer should start with. When tuning
    /// is disabled this returns static config-derived params (legacy), so
    /// callers can always use the result.
    pub fn begin_transfer(
        &mut self,
        base_url: &str,
        caps: &Capabilities,
        store: &dyn TuneStore,
        now_ms: f64,
        static_params: &TuneParams,
    ) -> TuneParams {
        self.caps = Some(caps.clone());
        self.caps_hash = caps.caps_hash();
        self.windows_closed = 0;
        self.window_errors = 0;
        self.window_done_base = 0;
        self.window_start_ms = now_ms;
        self.auto_level_benchmarked = false;
        self.auto_level = None;
        self.post_degrade_settle = false;

        if !self.enabled {
            self.phase = TunePhase::Uninitialized;
            self.params = static_params.clamped_into(caps);
            return self.params.clone();
        }

        let key = tune_key(base_url);
        let cached: Option<TuneCache> = store
            .get(&key)
            .and_then(|s| serde_json::from_str(&s).ok())
            .filter(|c: &TuneCache| c.v == CACHE_VERSION);

        match cached {
            Some(c)
                if c.caps_hash == self.caps_hash
                    && (now_ms - c.ts as f64) < self.ttl_ms as f64 =>
            {
                // Fresh entry for an unchanged server → reuse, Settled.
                self.params = c.params.clamped_into(caps);
                self.stats = c.stats;
                self.phase = TunePhase::Settled;
                self.dim = RampDim::Window;
            }
            cached => {
                // Miss / expired / changed caps → ramp from the minimums.
                if cached
                    .filter(|c| c.caps_hash != self.caps_hash)
                    .is_some()
                {
                    store.remove(&key);
                }
                self.params = TuneParams::from_caps_mins(caps);
                self.stats = TuneStats::default();
                self.phase = TunePhase::Ramping;
                self.dim = RampDim::Window;
            }
        }
        self.params.clone()
    }

    /// Feed one measurement sample; the engine evaluates exactly one window
    /// per [`TUNE_WINDOW_MS`] of wall clock and may return a tuning event.
    ///
    /// `done_bytes` is the absolute transferred count (the engine computes
    /// the window delta), `rtt_ms` an optional TTFB sample, `error` whether
    /// a chunk/round failed in this window.
    pub fn tick(
        &mut self,
        now_ms: f64,
        done_bytes: u64,
        rtt_ms: Option<f64>,
        error: bool,
    ) -> Option<TuneEvent> {
        if error {
            self.window_errors += 1;
        }
        if let Some(r) = rtt_ms.filter(|&r| r > 0.0) {
            let new_ewma = ewma(self.rtt_ewma, r, RTT_EWMA_ALPHA);
            if self.enabled {
                // Settled reuse: a freshly measured RTT deviating > 50%
                // from the *cached* stats means the link changed → force
                // a re-ramp. Compare against `self.stats.rtt_ms` before
                // it is overwritten by this window's own reading.
                //
                // Guard: on the first window (windows_closed == 0) the EWMA
                // equals the raw sample and has not been smoothed at all.
                // Only accept a drift signal on window 0 when the deviation
                // is substantial enough (> 2×) to be obviously real; for
                // smaller drifts wait until window 1 so the EWMA has at
                // least one alpha-blend pass before we compare.
                let drift_ratio = if self.stats.rtt_ms > 0.0 {
                    (new_ewma - self.stats.rtt_ms).abs() / self.stats.rtt_ms
                } else {
                    0.0
                };
                let drift_visible = drift_ratio > RTT_DRIFT_RE_RAMP
                    && (self.windows_closed >= 1 || drift_ratio > 1.0);
                if let Some(caps) = self.caps.as_ref().filter(|_| {
                    self.phase == TunePhase::Settled
                        && self.stats.rtt_ms > 0.0
                        && drift_visible
                }) {
                    self.params = TuneParams::from_caps_mins(caps);
                    self.stats = TuneStats::default();
                    self.phase = TunePhase::Ramping;
                    self.dim = RampDim::Window;
                    self.low_gain_windows = 0;
                }
            }
            self.rtt_ewma = Some(new_ewma);
        }
        let elapsed = now_ms - self.window_start_ms;
        if elapsed < TUNE_WINDOW_MS {
            return None;
        }
        if !self.enabled {
            return None;
        }
        let caps = self.caps.clone()?;

        let bytes = done_bytes.saturating_sub(self.window_done_base);
        let mbps = if elapsed > 0.0 {
            bytes as f64 / elapsed * 1_000.0 * 8.0 / 1e6
        } else {
            0.0
        };
        self.stats.mbps = mbps;
        if let Some(rtt) = self.rtt_ewma {
            self.stats.rtt_ms = rtt;
        }

        // BDP / RTT saturation only applies while actively ramping: a
        // Degraded phase's "stability windows" (or a Settled reuse) must not
        // be cut short by a tiny-BDP computation on near-zero traffic.
        let ramping = self.phase == TunePhase::Ramping;
        let input = RampInput {
            mbps,
            prev_mbps: self.prev_mbps,
            rtt_inflation: if ramping { self.rtt_inflation() } else { 0.0 },
            errors: self.window_errors,
            degraded: self.phase == TunePhase::Degraded,
            low_gain_windows: self.low_gain_windows,
            in_flight: self.in_flight(),
            bdp: if ramping {
                bdp_bytes(mbps, self.rtt_ewma.unwrap_or(self.stats.rtt_ms))
            } else {
                0
            },
            at_cap: self.dim_at_cap(&caps),
            last_dim: self.dim == RampDim::ChunkSize,
        };
        let (action, low) = ramp_action(&input);
        self.low_gain_windows = low;
        self.windows_closed += 1;
        self.prev_mbps = mbps;

        match action {
            RampAction::Raise => {
                self.raise_dim(&caps);
                if self.phase == TunePhase::Degraded {
                    self.phase = TunePhase::Ramping;
                }
            }
            RampAction::AdvanceDim => {
                self.dim = next_dim(self.dim);
            }
            RampAction::Settle => {
                self.phase = TunePhase::Settled;
                self.low_gain_windows = 0;
            }
            RampAction::Degrade => {
                self.halve_dim();
                self.phase = TunePhase::Degraded;
                self.degraded_windows = 0;
                self.low_gain_windows = 0;
            }
            RampAction::Hold => {
                if self.phase == TunePhase::Degraded {
                    // Conservative settle after 2 stable windows.
                    self.degraded_windows += 1;
                    if self.degraded_windows >= 2 {
                        self.phase = TunePhase::Settled;
                        // Mark that we settled from a degraded hold so that
                        // `transfer_end` does NOT persist the reduced params
                        // as the tuned baseline — the next ramp will
                        // re-explore from the minimums and cache only after
                        // a full convergence.
                        self.post_degrade_settle = true;
                    }
                }
            }
        }

        // Reset the window accumulator.
        self.window_start_ms = now_ms;
        self.window_done_base = done_bytes;
        self.window_errors = 0;

        let event = TuneEvent {
            phase: self.phase,
            params: self.params.clone(),
            stats: self.stats,
        };
        self.emit(&event);
        Some(event)
    }

    /// Mark the transfer outcome. On success with a Settled phase and
    /// enough windows, persist the cache; on failure, count toward cache
    /// invalidation.
    pub fn transfer_end(
        &mut self,
        base_url: &str,
        store: &dyn TuneStore,
        now_ms: f64,
        ok: bool,
    ) {
        if !self.enabled {
            return;
        }
        if ok {
            self.consecutive_failures = 0;
            // Only persist a genuine full-ramp settle, not a conservative
            // post-degrade settle (which ends with halved params — persisting
            // those would make every subsequent transfer start below the
            // optimal point and skip the ramp entirely).
            if self.phase == TunePhase::Settled
                && !self.post_degrade_settle
                && self.windows_closed >= MIN_WINDOWS_TO_PERSIST
            {
                self.persist(base_url, store, now_ms);
            }
        } else {
            self.consecutive_failures += 1;
            if self.consecutive_failures >= MAX_CONSECUTIVE_FAILURES {
                store.remove(&tune_key(base_url));
                self.consecutive_failures = 0;
                self.phase = TunePhase::Ramping;
            }
        }
    }

    /// Persist the current settled state.
    pub fn persist(&self, base_url: &str, store: &dyn TuneStore, now_ms: f64) {
        if !self.enabled || self.phase != TunePhase::Settled {
            return;
        }
        let cache = TuneCache {
            v: CACHE_VERSION,
            ts: now_ms as u64,
            caps_hash: self.caps_hash.clone(),
            params: self.params.clone(),
            stats: self.stats,
        };
        if let Ok(json) = serde_json::to_string(&cache) {
            store.set(&tune_key(base_url), &json);
        }
    }

    /// The upload compression level for this session.
    ///
    /// `Auto` benchmarks the advertised candidates against `sample` once per
    /// session (reusing the fastest cache afterwards); without a sample it
    /// resolves to the server default.
    pub fn upload_compress_level(
        &mut self,
        caps: &Capabilities,
        sample: Option<&[u8]>,
        mbps: f64,
    ) -> i32 {
        match self.level_cfg {
            CompressLevel::Auto => {
                if !self.auto_level_benchmarked {
                    self.auto_level_benchmarked = true;
                    self.auto_level = Some(match sample {
                        Some(s) if !s.is_empty() => {
                            // Limit the sample to LEVEL_SAMPLE_SIZE so the
                            // synchronous benchmark does not stall the WASM
                            // main thread for too long on large files.
                            let s = &s[..LEVEL_SAMPLE_SIZE.min(s.len())];
                            let candidates = auto_candidates(caps);
                            let results = benchmark_levels(s, &candidates);
                            choose_auto_level(&results, s.len(), mbps.max(1.0))
                        }
                        _ => caps.compression.zrip_levels.default,
                    });
                }
                self.auto_level.unwrap_or(caps.compression.zrip_levels.default)
            }
            other => resolve_level(other, caps),
        }
    }

    /// Whether the auto level has been benchmarked this session.
    pub fn auto_level_ready(&self) -> bool {
        self.auto_level_benchmarked
    }

    fn rtt_inflation(&self) -> f64 {
        match (self.rtt_ewma, self.stats.rtt_ms) {
            (Some(cur), base) if base > 0.0 && self.phase == TunePhase::Ramping => {
                cur / base - 1.0
            }
            _ => 0.0,
        }
    }

    fn in_flight(&self) -> u64 {
        match self.dim {
            RampDim::Window | RampDim::Concurrency | RampDim::ChunkSize => match self
                .last_transfer_kind()
            {
                TransferKind::Upload => self.params.upload_in_flight(),
                TransferKind::Download => self.params.download_in_flight(),
            },
        }
    }

    fn dim_at_cap(&self, caps: &Capabilities) -> bool {
        match self.dim {
            RampDim::Window => match self.last_transfer_kind() {
                TransferKind::Upload => {
                    self.params.upload_window as i64 >= caps.limits.upload_window.max
                }
                TransferKind::Download => {
                    self.params.download_window as i64 >= caps.limits.download_window.max
                }
            },
            RampDim::Concurrency => {
                self.params.concurrency as i64 >= caps.limits.concurrency.max
            }
            RampDim::ChunkSize => match self.last_transfer_kind() {
                TransferKind::Upload => self.params.chunk_size >= caps.limits.chunk_size.max as u64,
                TransferKind::Download => {
                    self.params.download_chunk_size >= caps.limits.download_chunk_size.max as u64
                }
            },
        }
    }

    fn raise_dim(&mut self, caps: &Capabilities) {
        let dim_max: i64 = match self.dim {
            RampDim::Window => match self.last_transfer_kind() {
                TransferKind::Upload => caps.limits.upload_window.max,
                TransferKind::Download => caps.limits.download_window.max,
            },
            RampDim::Concurrency => caps.limits.concurrency.max,
            RampDim::ChunkSize => match self.last_transfer_kind() {
                TransferKind::Upload => caps.limits.chunk_size.max,
                TransferKind::Download => caps.limits.download_chunk_size.max,
            },
        };
        // Multiplicative increase (×2) until near the cap, then additive
        // (+1) — TCP slow-start style.
        let step = |v: i64| -> i64 {
            if v <= 0 {
                1
            } else if v * 2 <= dim_max {
                v * 2
            } else {
                (v + 1).min(dim_max)
            }
        };
        match self.dim {
            RampDim::Window => match self.last_transfer_kind() {
                TransferKind::Upload => {
                    self.params.upload_window = step(self.params.upload_window as i64).max(1) as usize
                }
                TransferKind::Download => {
                    self.params.download_window =
                        step(self.params.download_window as i64).max(1) as usize
                }
            },
            RampDim::Concurrency => {
                self.params.concurrency = step(self.params.concurrency as i64).max(1) as usize
            }
            RampDim::ChunkSize => match self.last_transfer_kind() {
                TransferKind::Upload => {
                    self.params.chunk_size = step(self.params.chunk_size as i64).max(1) as u64
                }
                TransferKind::Download => {
                    self.params.download_chunk_size =
                        step(self.params.download_chunk_size as i64).max(1) as u64
                }
            },
        }
    }

    fn halve_dim(&mut self) {
        let step = |v: i64| -> i64 { ((v as f64) * DEGRADE_FACTOR).floor().max(1.0) as i64 };
        match self.dim {
            RampDim::Window => match self.last_transfer_kind() {
                TransferKind::Upload => {
                    self.params.upload_window = step(self.params.upload_window as i64) as usize
                }
                TransferKind::Download => {
                    self.params.download_window = step(self.params.download_window as i64) as usize
                }
            },
            RampDim::Concurrency => {
                self.params.concurrency = step(self.params.concurrency as i64) as usize
            }
            RampDim::ChunkSize => match self.last_transfer_kind() {
                TransferKind::Upload => {
                    self.params.chunk_size = step(self.params.chunk_size as i64) as u64
                }
                TransferKind::Download => {
                    self.params.download_chunk_size = step(self.params.download_chunk_size as i64)
                        as u64
                }
            },
        }
    }
}

/// Which transfer direction is ramping (drives which window/chunk dims move).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferKind {
    Upload,
    Download,
}

impl TuningEngine {
    fn last_transfer_kind(&self) -> TransferKind {
        // The ramp dimension set is reset per transfer; the direction is
        // recorded by begin_transfer callers via `set_direction`.
        self.direction
    }
}

/// The ramp order: per-file window → concurrency → chunk size.
fn next_dim(dim: RampDim) -> RampDim {
    match dim {
        RampDim::Window => RampDim::Concurrency,
        RampDim::Concurrency => RampDim::ChunkSize,
        // ChunkSize is the terminal dimension: `ramp_action` returns
        // `Settle` (not `AdvanceDim`) when it is at cap, so this arm is
        // unreachable in practice. It is kept for exhaustiveness; if a new
        // dimension is added, the compiler will force updating this function.
        RampDim::ChunkSize => RampDim::ChunkSize,
    }
}

/// Benchmark candidates: min, default, max of the advertised range.
fn auto_candidates(caps: &Capabilities) -> Vec<i32> {
    let z = caps.compression.zrip_levels;
    vec![z.min, z.default, z.max]
}

/// Compress `sample` at each level, returning (level, len, ms).
///
/// One-shot `zrip::compress` (single frame) is fine for a 256 KiB sample.
fn benchmark_levels(sample: &[u8], levels: &[i32]) -> Vec<(i32, usize, f64)> {
    levels
        .iter()
        .map(|&level| {
            let t0 = now_ms();
            let out = zrip::compress(sample, level).unwrap_or_else(|_| sample.to_vec());
            (level, out.len(), now_ms() - t0)
        })
        .collect()
}

/// Wall clock (epoch ms). WASM: `Date.now()`; native: `std::time`.
pub fn now_ms() -> f64 {
    #[cfg(target_arch = "wasm32")]
    {
        js_sys::Date::now()
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs_f64() * 1_000.0)
            .unwrap_or(0.0)
    }
}

impl TuningEngine {
    fn emit(&self, event: &TuneEvent) {
        if let Some(cb) = &self.on_tuning {
            let params = params_to_js(&event.params);
            let stats = js_sys::Object::new();
            let _ = js_sys::Reflect::set(
                &stats,
                &JsValue::from_str("rttMs"),
                &JsValue::from_f64(event.stats.rtt_ms),
            );
            let _ = js_sys::Reflect::set(
                &stats,
                &JsValue::from_str("mbps"),
                &JsValue::from_f64(event.stats.mbps),
            );
            let phase = match event.phase {
                TunePhase::Uninitialized => "uninitialized",
                TunePhase::Ramping => "ramping",
                TunePhase::Settled => "settled",
                TunePhase::Degraded => "degraded",
            };
            let _ = cb.call3(
                &JsValue::NULL,
                &JsValue::from_str(phase),
                &params,
                &stats.into(),
            );
        }
    }
}

fn params_to_js(params: &TuneParams) -> JsValue {
    let o = js_sys::Object::new();
    let _ = js_sys::Reflect::set(&o, &JsValue::from_str("concurrency"), &JsValue::from_f64(params.concurrency as f64));
    let _ = js_sys::Reflect::set(&o, &JsValue::from_str("uploadWindow"), &JsValue::from_f64(params.upload_window as f64));
    let _ = js_sys::Reflect::set(&o, &JsValue::from_str("downloadWindow"), &JsValue::from_f64(params.download_window as f64));
    let _ = js_sys::Reflect::set(&o, &JsValue::from_str("chunkSize"), &JsValue::from_f64(params.chunk_size as f64));
    let _ = js_sys::Reflect::set(&o, &JsValue::from_str("downloadChunkSize"), &JsValue::from_f64(params.download_chunk_size as f64));
    let _ = js_sys::Reflect::set(&o, &JsValue::from_str("compressLevel"), &JsValue::from_f64(params.compress_level as f64));
    o.into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::collections::HashMap;

    #[derive(Default)]
    struct FakeStore {
        map: RefCell<HashMap<String, String>>,
    }

    impl TuneStore for FakeStore {
        fn get(&self, key: &str) -> Option<String> {
            self.map.borrow().get(key).cloned()
        }
        fn set(&self, key: &str, value: &str) {
            self.map.borrow_mut().insert(key.to_string(), value.to_string());
        }
        fn remove(&self, key: &str) {
            self.map.borrow_mut().remove(key);
        }
    }

    fn caps() -> Capabilities {
        Capabilities::default()
    }

    fn static_params() -> TuneParams {
        TuneParams {
            concurrency: 4,
            upload_window: 8,
            download_window: 4,
            chunk_size: 2 * 1024 * 1024,
            download_chunk_size: 256 * 1024,
            compress_level: 1,
        }
    }

    fn engine(store: &dyn TuneStore) -> TuningEngine {
        let mut e = TuningEngine::new(true, DEFAULT_TUNE_TTL_MS, CompressLevel::Balanced);
        e.set_direction(TransferKind::Upload);
        e.begin_transfer("https://h.example/", &caps(), store, 0.0, &static_params());
        e
    }

    // --- pure decision logic ------------------------------------------------

    #[test]
    fn ramp_action_error_degrades() {
        let input = RampInput {
            mbps: 10.0,
            prev_mbps: 5.0,
            rtt_inflation: 0.0,
            errors: 1,
            degraded: false,
            low_gain_windows: 0,
            in_flight: 100,
            bdp: 1_000,
            at_cap: false,
            last_dim: false,
        };
        assert_eq!(ramp_action(&input).0, RampAction::Degrade);
    }

    #[test]
    fn ramp_action_cap_advances_or_settles() {
        let base = RampInput {
            mbps: 10.0,
            prev_mbps: 5.0,
            rtt_inflation: 0.0,
            errors: 0,
            degraded: false,
            low_gain_windows: 0,
            in_flight: 100,
            bdp: 1_000,
            at_cap: true,
            last_dim: false,
        };
        assert_eq!(ramp_action(&base).0, RampAction::AdvanceDim);
        let last = RampInput { last_dim: true, ..base };
        assert_eq!(ramp_action(&last).0, RampAction::Settle);
    }

    #[test]
    fn ramp_action_bdp_and_rtt_saturate() {
        let base = RampInput {
            mbps: 10.0,
            prev_mbps: 5.0,
            rtt_inflation: 0.0,
            errors: 0,
            degraded: false,
            low_gain_windows: 0,
            in_flight: 2_000,
            bdp: 1_000,
            at_cap: false,
            last_dim: false,
        };
        assert_eq!(ramp_action(&base).0, RampAction::Settle, "in-flight ≥ BDP settles");

        let rtt = RampInput {
            rtt_inflation: 0.5,
            in_flight: 10,
            bdp: 1_000,
            ..base
        };
        assert_eq!(ramp_action(&rtt).0, RampAction::Settle, "RTT inflation settles");
    }

    #[test]
    fn ramp_action_gain_raises_and_low_gain_settles() {
        // +100% growth → raise (and the low-gain counter resets).
        let growing = RampInput {
            mbps: 20.0,
            prev_mbps: 10.0,
            ..RampInput {
                mbps: 0.0,
                prev_mbps: 0.0,
                rtt_inflation: 0.0,
                errors: 0,
                degraded: false,
                low_gain_windows: 7,
                in_flight: 10,
                bdp: 1_000,
                at_cap: false,
                last_dim: false,
            }
        };
        let (action, low) = ramp_action(&growing);
        assert_eq!(action, RampAction::Raise);
        assert_eq!(low, 0, "raise resets the low-gain counter");

        // +4% growth (below the 5% threshold) → hold, counter 1.
        let flat = RampInput {
            mbps: 10.4,
            prev_mbps: 10.0,
            low_gain_windows: 0,
            ..growing
        };
        let (action, low) = ramp_action(&flat);
        assert_eq!(action, RampAction::Hold);
        assert_eq!(low, 1, "first below-threshold window holds");

        // Second below-threshold window → settle.
        let flat2 = RampInput { low_gain_windows: 1, ..flat };
        let (action, low) = ramp_action(&flat2);
        assert_eq!(action, RampAction::Settle);
        assert_eq!(low, 2);

        // First window (no baseline) counts as growth.
        let first = RampInput {
            mbps: 1.0,
            prev_mbps: 0.0,
            low_gain_windows: 0,
            ..growing
        };
        assert_eq!(ramp_action(&first).0, RampAction::Raise);
    }

    #[test]
    fn ramp_action_degraded_hold_never_raises() {
        // Near-zero traffic right after a degrade (prev_mbps == 0) must not
        // trigger the first-window pseudo-gain: the engine holds, then
        // settles on the second stable window instead of re-raising.
        let degraded = RampInput {
            mbps: 0.008,
            prev_mbps: 0.0,
            degraded: true,
            ..RampInput {
                mbps: 0.0,
                prev_mbps: 0.0,
                rtt_inflation: 0.0,
                errors: 0,
                degraded: false,
                low_gain_windows: 0,
                in_flight: 10,
                bdp: 1_000,
                at_cap: false,
                last_dim: false,
            }
        };
        let (action, low) = ramp_action(&degraded);
        assert_eq!(action, RampAction::Hold);
        assert_eq!(low, 1, "first stability window holds");
        let (action, low) = ramp_action(&RampInput { low_gain_windows: 1, ..degraded });
        assert_eq!(action, RampAction::Settle);
        assert_eq!(low, 2, "second stability window settles");
    }

    #[test]
    fn ewma_seeds_and_smooths() {
        assert_eq!(ewma(None, 10.0, 0.25), 10.0);
        let v = ewma(Some(10.0), 20.0, 0.25);
        assert!((v - 12.5).abs() < 1e-9);
    }

    #[test]
    fn bdp_conversion() {
        // 10 Mbps, 100 ms RTT → 10e6/8 * 0.1 = 125_000 bytes.
        assert_eq!(bdp_bytes(10.0, 100.0), 125_000);
        assert_eq!(bdp_bytes(0.0, 0.0), 0);
    }

    #[test]
    fn origin_hash_is_stable_and_discriminates() {
        assert_eq!(origin_hash("https://h.example/"), origin_hash("https://h.example/a/b"));
        assert_eq!(origin_hash("https://h.example:8443/x"), origin_hash("https://h.example:8443/y"));
        assert_ne!(origin_hash("https://h.example/"), origin_hash("https://h.example:8443/"));
        assert_ne!(origin_hash("https://h.example/"), origin_hash("http://h.example/"));
        assert_ne!(origin_hash("https://h.example/"), origin_hash("https://other.example/"));
        assert!(tune_key("https://h.example/").starts_with("libfw.tune.sha256-"));
    }

    #[test]
    fn resolve_level_mapping() {
        let c = caps();
        assert_eq!(resolve_level(CompressLevel::Fast, &c), -8);
        assert_eq!(resolve_level(CompressLevel::Balanced, &c), 1);
        assert_eq!(resolve_level(CompressLevel::Max, &c), 4);
        assert_eq!(resolve_level(CompressLevel::Auto, &c), 1);
        assert_eq!(resolve_level(CompressLevel::Fixed(2), &c), 2);
        assert_eq!(resolve_level(CompressLevel::Fixed(99), &c), 4);
        assert_eq!(resolve_level(CompressLevel::Fixed(-99), &c), -8);
    }

    #[test]
    fn choose_auto_level_picks_by_net_gain() {
        // Incompressible sample → cheapest level (min), never waste CPU.
        assert_eq!(
            choose_auto_level(&[(-8, 1000, 0.1), (1, 1000, 5.0), (4, 1000, 40.0)], 1000, 10.0),
            -8
        );

        // CPU cost dominates (fast link) → cheaper level wins.
        let cpu_bound = choose_auto_level(&[(-8, 800_000, 0.1), (4, 400_000, 30.0)], 1_000_000, 1000.0);
        assert_eq!(cpu_bound, -8);

        // Savings dominate (slow link, cheap CPU) → stronger level wins.
        let savings = choose_auto_level(&[(-8, 800_000, 0.1), (4, 400_000, 30.0)], 1_000_000, 10.0);
        assert_eq!(savings, 4);

        // Ties → cheaper (first) candidate.
        assert_eq!(choose_auto_level(&[(0, 500, 1.0), (4, 500, 1.0)], 1000, 100.0), 0);
    }

    // --- engine behaviour ---------------------------------------------------

    #[test]
    fn begin_transfer_miss_ramps_from_mins() {
        let store = FakeStore::default();
        let mut e = TuningEngine::new(true, DEFAULT_TUNE_TTL_MS, CompressLevel::Balanced);
        e.set_direction(TransferKind::Upload);
        let p = e.begin_transfer("https://h.example/", &caps(), &store, 0.0, &static_params());
        assert_eq!(e.phase(), TunePhase::Ramping);
        assert_eq!(p.concurrency, 1);
        assert_eq!(p.upload_window, 1);
        assert_eq!(p.chunk_size, 262_144);
    }

    #[test]
    fn ramp_raises_window_then_concurrency_until_cap() {
        let store = FakeStore::default();
        let mut e = engine(&store);
        // High-latency profile (400 ms RTT): BDP ≈ 5 MB at 100 Mbps, so the
        // ramp can climb the whole window dimension before saturating.
        // Deltas double every window (12.5 → 25 → 50 MB/s) → +100% gain.
        let mut t = 1_000.0;
        let mut done = 12_500_000u64;
        let mut delta = 12_500_000u64;
        for _ in 0..3 {
            let ev = e.tick(t, done, Some(400.0), false).expect("window closed");
            assert_eq!(ev.phase, TunePhase::Ramping);
            t += 1_000.0;
            delta *= 2;
            done += delta; // cumulative: 12.5M → 37.5M → 87.5M → ...
        }
        // Window: 1 → 2 → 4 → 8 (max). Next raise hits the cap → advance.
        assert_eq!(e.params().upload_window, 8);
        let ev = e.tick(t, done, Some(400.0), false).unwrap();
        assert_eq!(ev.params.upload_window, 8); // capped
        t += 1_000.0;
        delta *= 2;
        done += delta;
        // Concurrency now ramps: 1 → 2...
        let ev = e.tick(t, done, Some(400.0), false).unwrap();
        assert_eq!(ev.params.concurrency, 2);
        // ...and continues while the window dimension stays put.
        let ev = e.tick(t + 1_000.0, done + delta * 2, Some(400.0), false).unwrap();
        assert_eq!(ev.params.concurrency, 4);
        assert_eq!(ev.params.upload_window, 8);
    }

    #[test]
    fn error_window_degrades_and_halves() {
        let store = FakeStore::default();
        let mut e = engine(&store);
        // Ramp the window up first (400 ms RTT keeps BDP above in-flight).
        let mut t = 1_000.0;
        let mut done = 12_500_000u64;
        let mut delta = 12_500_000u64;
        for _ in 0..2 {
            e.tick(t, done, Some(400.0), false);
            t += 1_000.0;
            delta *= 2;
            done += delta; // 12.5M → 37.5M: deltas double → +100% gain
        }
        assert_eq!(e.params().upload_window, 4);
        // Error window → degrade: 4 × 0.5 = 2.
        let ev = e.tick(t, done, Some(400.0), true).unwrap();
        assert_eq!(ev.phase, TunePhase::Degraded);
        assert_eq!(ev.params.upload_window, 2);
        // Two stable windows → conservative settle (BDP is NOT evaluated in
        // the degraded hold — near-zero traffic must not look "saturated").
        let ev = e.tick(t + 1_000.0, done + 1_000, None, false).unwrap();
        assert_eq!(ev.phase, TunePhase::Degraded);
        let ev = e.tick(t + 2_000.0, done + 2_000, None, false).unwrap();
        assert_eq!(ev.phase, TunePhase::Settled);
        assert_eq!(ev.params.upload_window, 2);
    }

    #[test]
    fn low_gain_twice_settles_and_persists() {
        let store = FakeStore::default();
        let mut e = engine(&store);
        // Raise once, then flatten the throughput → 2 flat windows settle
        // (window 1 = hold, window 2 = settle).
        let ev = e.tick(1_000.0, 12_500_000, Some(400.0), false).unwrap();
        assert_eq!(ev.phase, TunePhase::Ramping);
        let ev = e.tick(2_000.0, 25_000_000, Some(400.0), false).unwrap(); // +0% → hold
        assert_eq!(ev.phase, TunePhase::Ramping);
        let ev = e.tick(3_000.0, 26_000_000, Some(400.0), false).unwrap(); // 2nd flat window
        assert_eq!(ev.phase, TunePhase::Settled);
        let ev = e.tick(4_000.0, 27_040_000, Some(400.0), false).unwrap(); // stays settled
        assert_eq!(ev.phase, TunePhase::Settled);
        assert_eq!(ev.params.upload_window, 2);

        // ≥2 windows closed & settled → transfer_end persists.
        e.transfer_end("https://h.example/", &store, 4_000.0, true);
        let key = tune_key("https://h.example/");
        let cached: TuneCache = serde_json::from_str(&store.get(&key).unwrap()).unwrap();
        assert_eq!(cached.params.upload_window, 2);
        assert_eq!(cached.caps_hash, caps().caps_hash());
    }

    #[test]
    fn cache_reuse_settles_immediately() {
        let store = FakeStore::default();
        // First session: ramp to settle and persist.
        {
            let mut e = engine(&store);
            e.tick(1_000.0, 12_500_000, Some(400.0), false);
            e.tick(2_000.0, 25_000_000, Some(400.0), false);
            e.tick(3_000.0, 26_000_000, Some(400.0), false);
            e.tick(4_000.0, 27_040_000, Some(400.0), false);
            e.transfer_end("https://h.example/", &store, 4_000.0, true);
        }
        // Second session (fresh engine, same store): reuse → Settled.
        let mut e2 = TuningEngine::new(true, DEFAULT_TUNE_TTL_MS, CompressLevel::Balanced);
        e2.set_direction(TransferKind::Upload);
        let p = e2.begin_transfer("https://h.example/", &caps(), &store, 10_000.0, &static_params());
        assert_eq!(e2.phase(), TunePhase::Settled);
        assert_eq!(p.upload_window, 2);
    }

    #[test]
    fn ttl_expiry_re_ramps() {
        let store = FakeStore::default();
        {
            let mut e = engine(&store);
            e.tick(1_000.0, 12_500_000, Some(400.0), false);
            e.tick(2_000.0, 25_000_000, Some(400.0), false);
            e.tick(3_000.0, 26_000_000, Some(400.0), false);
            e.tick(4_000.0, 27_040_000, Some(400.0), false);
            e.transfer_end("https://h.example/", &store, 4_000.0, true);
        }
        // TTL = 1h; begin at now = 4_000 + 3_600_001 → expired.
        let mut e2 = TuningEngine::new(true, DEFAULT_TUNE_TTL_MS, CompressLevel::Balanced);
        e2.set_direction(TransferKind::Upload);
        let p = e2.begin_transfer(
            "https://h.example/",
            &caps(),
            &store,
            4_000.0 + DEFAULT_TUNE_TTL_MS as f64 + 1.0,
            &static_params(),
        );
        assert_eq!(e2.phase(), TunePhase::Ramping);
        assert_eq!(p.upload_window, 1, "expired cache → ramp from mins");
    }

    #[test]
    fn caps_hash_change_invalidates_cache() {
        let store = FakeStore::default();
        {
            let mut e = engine(&store);
            e.tick(1_000.0, 12_500_000, Some(400.0), false);
            e.tick(2_000.0, 25_000_000, Some(400.0), false);
            e.tick(3_000.0, 26_000_000, Some(400.0), false);
            e.tick(4_000.0, 27_040_000, Some(400.0), false);
            e.transfer_end("https://h.example/", &store, 4_000.0, true);
        }
        // A server that changed its caps (different hash) → cache removed.
        let mut changed = caps();
        changed.limits.concurrency.default = 6;
        let mut e2 = TuningEngine::new(true, DEFAULT_TUNE_TTL_MS, CompressLevel::Balanced);
        e2.set_direction(TransferKind::Upload);
        let p = e2.begin_transfer("https://h.example/", &changed, &store, 10_000.0, &static_params());
        assert_eq!(e2.phase(), TunePhase::Ramping);
        assert_eq!(p.upload_window, 1);
        assert!(store.get(&tune_key("https://h.example/")).is_none(), "stale cache removed");
    }

    #[test]
    fn short_transfer_does_not_persist() {
        let store = FakeStore::default();
        let mut e = engine(&store);
        // Only ONE window closed before the transfer ends.
        e.tick(1_000.0, 12_500_000, Some(40.0), false);
        e.transfer_end("https://h.example/", &store, 1_500.0, true);
        assert!(store.get(&tune_key("https://h.example/")).is_none(), "short transfers are noise");
    }

    #[test]
    fn repeated_failures_remove_cache() {
        let store = FakeStore::default();
        let mut e = engine(&store);
        // Seed a persisted settle.
        e.tick(1_000.0, 12_500_000, Some(40.0), false);
        e.tick(2_000.0, 25_000_000, Some(40.0), false);
        e.tick(3_000.0, 26_000_000, Some(40.0), false);
        e.tick(4_000.0, 27_040_000, Some(40.0), false);
        e.transfer_end("https://h.example/", &store, 4_000.0, true);
        assert!(store.get(&tune_key("https://h.example/")).is_some());

        // Three consecutive failed transfers invalidate the cache.
        for _ in 0..3 {
            e.transfer_end("https://h.example/", &store, 5_000.0, false);
        }
        assert!(store.get(&tune_key("https://h.example/")).is_none());
    }

    #[test]
    fn rtt_drift_re_ramps_from_cache() {
        let store = FakeStore::default();
        {
            let mut e = engine(&store);
            e.tick(1_000.0, 12_500_000, Some(400.0), false);
            e.tick(2_000.0, 25_000_000, Some(400.0), false);
            e.tick(3_000.0, 26_000_000, Some(400.0), false);
            e.tick(4_000.0, 27_040_000, Some(400.0), false);
            e.transfer_end("https://h.example/", &store, 4_000.0, true);
        }
        let mut e2 = TuningEngine::new(true, DEFAULT_TUNE_TTL_MS, CompressLevel::Balanced);
        e2.set_direction(TransferKind::Upload);
        e2.begin_transfer("https://h.example/", &caps(), &store, 10_000.0, &static_params());
        assert_eq!(e2.phase(), TunePhase::Settled);
        // New session measures RTT 3× the cached 400 ms → >50% drift → re-ramp.
        let ev = e2.tick(11_000.0, 12_500_000, Some(1_200.0), false).unwrap();
        assert_eq!(ev.phase, TunePhase::Ramping);
        assert!(
            ev.params.upload_window <= 2,
            "re-ramp starts from mins (first measured window may raise once)"
        );
    }

    #[test]
    fn upload_auto_level_benchmarks_once_and_caches() {
        let store = FakeStore::default();
        let mut e = TuningEngine::new(true, DEFAULT_TUNE_TTL_MS, CompressLevel::Auto);
        e.begin_transfer("https://h.example/", &caps(), &store, 0.0, &static_params());
        let sample = vec![0xABu8; 256 * 1024];
        let lvl1 = e.upload_compress_level(&caps(), Some(&sample), 50.0);
        let lvl2 = e.upload_compress_level(&caps(), Some(&sample), 50.0);
        assert_eq!(lvl1, lvl2, "benchmark runs once, result cached");
        assert!(e.auto_level_ready());
        assert!((-8..=4).contains(&lvl1), "level inside advertised range, got {lvl1}");
        // Incompressible sample → cheapest level.
        let mut e2 = TuningEngine::new(true, DEFAULT_TUNE_TTL_MS, CompressLevel::Auto);
        e2.begin_transfer("https://h.example/", &caps(), &store, 0.0, &static_params());
        let lvl = e2.upload_compress_level(&caps(), Some(&[0u8; 0]), 50.0);
        assert_eq!(lvl, caps().compression.zrip_levels.default, "no sample → server default");
    }

    #[test]
    fn disabled_engine_uses_static_params() {
        let store = FakeStore::default();
        let mut e = TuningEngine::new(false, DEFAULT_TUNE_TTL_MS, CompressLevel::Balanced);
        e.set_direction(TransferKind::Upload);
        let p = e.begin_transfer("https://h.example/", &caps(), &store, 0.0, &static_params());
        assert_eq!(p.concurrency, 4, "static config preserved");
        assert_eq!(p.upload_window, 8);
        assert_eq!(e.phase(), TunePhase::Uninitialized);
        // Ticks are no-ops.
        assert!(e.tick(1_000.0, 12_500_000, Some(40.0), false).is_none());
        assert_eq!(p, static_params().clamped_into(&caps()));
    }
}