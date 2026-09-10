//! Adaptive tuning engine: TCP-style ramp over *real* transfers.
//!
//! # Model
//!
//! The engine treats each transfer as a slow-start probe: measurements are
//! taken from the bytes the transfer actually moves (no synthetic probe
//! requests), and the shared parameter table is ramped up dimension by
//! dimension — per-file window first, then cross-file concurrency — until the
//! link saturates, then settled.
//!
//! The **chunk size** is not ramped: it follows the measured link
//! ([`chunk_for_link`]) — about [`TARGET_CHUNK_SPAN_MS`] of the current
//! throughput, clamped into the server's advertised range and the client's
//! in-flight budget. That gives a wider/faster link fewer, bigger requests and
//! a narrow link the advertised minimum, and it applies for the whole
//! transfer (including after the ramp settles). It used to be the *last* ramp
//! dimension, which in practice never ran: the BDP rule settles the ramp on
//! the window/concurrency dimensions first, so the chunk stayed pinned at the
//! advertised minimum (256 KiB) on every link.
//!
//! # Lifetime of a tuning result
//!
//! Within one client the settled parameters are reused by the next transfer —
//! otherwise a folder of many files would re-ramp for each file — and they are
//! discarded if a transfer fails. Optionally the result is *persisted* through
//! a [`TuneStore`] (in the browser: `localStorage`) so a page refresh does not
//! re-ramp: [`TuningEngine::set_cache`] installs the store together with a TTL
//! (default [`DEFAULT_TUNE_TTL_MS`], `0` disables the cache). Entries are keyed
//! by origin *and* direction ([`tune_key`]), tagged with the server
//! capabilities they were measured against, and expire from the moment the
//! ramp settled — a link measured long ago is re-measured even if it is used
//! constantly. The native client installs no store and keeps everything in
//! memory.
//!
//! # Design notes
//!
//! - **Pure decision logic**: [`ramp_action`], [`choose_auto_level`] and the
//!   clamp/EWMA helpers are plain functions with table-driven tests. The
//!   clock is injected (`tick` takes `now_ms`), so native tests never need a
//!   browser.
//! - **Shared state**: one [`TuningEngine`] per client, wrapped in
//!   `Rc<RefCell<_>>` (WASM is single-threaded). All transfers read the
//!   current params when they schedule work.
//! - **One-second windows**: [`TuningEngine::tick`] accumulates bytes / RTT
//!   samples / errors and evaluates exactly one window per second of wall
//!   clock, so measurements are comparable across links.

use std::cell::RefCell;
use std::rc::Rc;

use libfw_core::{Capabilities, IntRange};
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

// Hard client-side ceilings on every tuned dimension.
//
// The server's `/capabilities` is *advisory input from a peer we do not
// control*: it is fetched before authentication and may be served by a
// proxy, a misconfigured host or a hostile peer. Tuning must therefore
// never let an advertised range drive the client past its own hard
// limits — a `chunkSize` of 1 GiB or a `concurrency` of 10 000 would
// otherwise make the browser allocate / open far past any sane budget.
/// Absolute cap on tuned cross-file concurrency.
pub const HARD_MAX_CONCURRENCY: i64 = 64;
/// Absolute cap on either per-file in-flight window.
pub const HARD_MAX_WINDOW: i64 = 64;
/// Absolute cap on the tuned chunk size (bytes).
pub const HARD_MAX_CHUNK_SIZE: i64 = 16 * 1024 * 1024;
/// Absolute cap on tuned in-flight bytes per direction
/// (`concurrency × window × chunk_size`); the download path must hold the
/// whole window in its reorder buffer, so this bounds peak memory too.
/// 256 MiB is high enough to let the built-in caps (4 × 8 × 8 MiB) settle
/// while still stopping an absurd advertisement from exhausting the tab.
pub const HARD_MAX_IN_FLIGHT_BYTES: u64 = 256 * 1024 * 1024;
/// Absolute cap on concurrent in-flight HTTP requests.
///
/// The in-flight pool is sized `concurrency × per-file window` so it bounds
/// parallelism without becoming the bottleneck; this ceiling stops a hostile
/// advertisement (or a 64 × 64 client config) from opening hundreds of
/// sockets. Chromium itself multiplexes only ~6 connections per HTTP/1.1
/// origin, so 64 is effectively "as parallel as the browser can use".
pub const HARD_MAX_IN_FLIGHT_REQUESTS: usize = 64;
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
    /// Parameters converged for this session.
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
    /// Shared transfer chunk size for both uploads and parallel downloads.
    pub chunk_size: u64,
    /// zrip level for this transfer's compressed body.
    pub compress_level: i32,
}

/// Concurrent HTTP request budget for the shared in-flight pool:
/// `concurrency` files × the widest per-file window, clamped to
/// [`HARD_MAX_IN_FLIGHT_REQUESTS`].
///
/// The pool exists to *bound* parallelism, not to define it. Sizing it to
/// `concurrency` alone made the per-file window a dead letter: right after an
/// adaptive ramp starts, the advertised `concurrency` minimum is 1, so every
/// chunk was serialized and growing the window changed nothing (measured:
/// 1.1 MiB/s on a 4 MiB/s link with `uploadWindow = 4`).
pub fn request_budget(concurrency: usize, per_file_window: usize) -> usize {
    concurrency
        .max(1)
        .saturating_mul(per_file_window.max(1))
        .clamp(1, HARD_MAX_IN_FLIGHT_REQUESTS)
}

/// Default lifetime of a cached tuning result (1 hour).
///
/// A settled parameter set is a property of the *link* the client measured,
/// so keeping it for a while lets a page reload (or the next visit) skip the
/// ramp instead of re-probing from the advertised minimums every time. The
/// TTL is the safety valve: a link that changed in between (a laptop that
/// moved networks, a mobile client on a different cell) re-measures once the
/// entry is older than this. `0` disables the cache entirely.
pub const DEFAULT_TUNE_TTL_MS: u64 = 60 * 60 * 1_000;

/// Version tag inside a cache entry, so a format change invalidates old rows
/// instead of feeding the engine a layout it cannot read.
pub const TUNE_CACHE_VERSION: u32 = 1;

/// Where settled tuning results are persisted (the browser's `localStorage`).
///
/// Deliberately three tiny methods with **no async/fallible surface**: every
/// storage failure (private mode, quota, disabled storage) is swallowed by the
/// implementation, because caching is an optimisation and must never fail a
/// transfer.
pub trait TuneStore {
    /// Read a previously stored result.
    fn get(&self, key: &str) -> Option<String>;
    /// Store (or replace) a result.
    fn set(&self, key: &str, value: &str);
    /// Drop a result (a failed transfer invalidates it).
    fn remove(&self, key: &str);
}

/// A persisted tuning result for one origin + direction.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TuneCache {
    /// [`TUNE_CACHE_VERSION`] at the time of writing.
    pub v: u32,
    /// `caps_hash` the parameters were measured against.
    pub caps_hash: String,
    /// When the settle happened (epoch ms, same clock as `tick`).
    pub saved_at_ms: u64,
    /// The settled parameter table.
    pub params: TuneParamsCache,
}

/// The persisted subset of [`TuneParams`].
///
/// A separate mirror keeps the stored layout stable against internal field
/// churn (and keeps `compress_level` out: it is a per-transfer policy, never a
/// cached measurement).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TuneParamsCache {
    pub concurrency: usize,
    pub upload_window: usize,
    pub download_window: usize,
    pub chunk_size: u64,
}

impl TuneParamsCache {
    fn of(params: &TuneParams) -> Self {
        TuneParamsCache {
            concurrency: params.concurrency,
            upload_window: params.upload_window,
            download_window: params.download_window,
            chunk_size: params.chunk_size,
        }
    }

    fn into_params(self, compress_level: i32) -> TuneParams {
        TuneParams {
            concurrency: self.concurrency,
            upload_window: self.upload_window,
            download_window: self.download_window,
            chunk_size: self.chunk_size,
            compress_level,
        }
    }
}

/// Storage key for one origin + direction.
///
/// The origin (scheme://host[:port]) is carried verbatim — it is short, and a
/// readable key beats a hash when debugging a browser's storage inspector. The
/// direction is part of the key because the ramped dimensions differ (an
/// upload settle says nothing about the download window).
pub fn tune_key(origin: &str, direction: TransferKind) -> String {
    let dir = match direction {
        TransferKind::Upload => "upload",
        TransferKind::Download => "download",
    };
    format!("libfw.tune.v{TUNE_CACHE_VERSION}.{dir}.{origin}")
}

/// `scheme://host[:port]` of a URL, or an empty string when it cannot be told.
///
/// Only `http`/`https` are recognised: anything else (or a malformed URL) means
/// "do not cache" — a wrong cache key is worse than no cache.
pub fn origin_of(url: &str) -> String {
    let (scheme, rest) = match url.split_once("://") {
        Some((s, r)) => (s, r),
        None => return String::new(),
    };
    if !scheme.eq_ignore_ascii_case("http") && !scheme.eq_ignore_ascii_case("https") {
        return String::new();
    }
    // Authority ends at the first `/`, `?` or `#`.
    let authority = rest
        .split(['/', '?', '#'])
        .next()
        .unwrap_or("")
        .trim();
    if authority.is_empty() {
        return String::new();
    }
    format!("{}://{}", scheme.to_ascii_lowercase(), authority.to_ascii_lowercase())
}

impl TuneParams {
    /// Concurrent HTTP requests this parameter set may keep in flight.
    pub fn request_budget(&self) -> usize {
        request_budget(self.concurrency, self.upload_window.max(self.download_window))
    }

    /// The parameter table at the server's advertised minimums (ramp start).
    pub fn from_caps_mins(caps: &Capabilities) -> TuneParams {
        TuneParams {
            concurrency: floor_of(&caps.limits.concurrency, HARD_MAX_CONCURRENCY) as usize,
            upload_window: floor_of(&caps.limits.upload_window, HARD_MAX_WINDOW) as usize,
            download_window: floor_of(&caps.limits.download_window, HARD_MAX_WINDOW) as usize,
            chunk_size: floor_of(&caps.limits.chunk_size, HARD_MAX_CHUNK_SIZE) as u64,
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
        compress_level: i32,
        caps: &Capabilities,
    ) -> TuneParams {
        TuneParams {
            concurrency: bounded(
                concurrency.max(1) as i64,
                &caps.limits.concurrency,
                HARD_MAX_CONCURRENCY,
            ) as usize,
            upload_window: bounded(
                upload_window.max(1) as i64,
                &caps.limits.upload_window,
                HARD_MAX_WINDOW,
            ) as usize,
            download_window: bounded(
                download_window.max(1) as i64,
                &caps.limits.download_window,
                HARD_MAX_WINDOW,
            ) as usize,
            chunk_size: bounded(
                chunk_size.max(1) as i64,
                &caps.limits.chunk_size,
                HARD_MAX_CHUNK_SIZE,
            ) as u64,
            compress_level: caps.clamp_level(compress_level),
        }
    }

    /// Clamp every dimension into the server's advertised ranges (defensive
    /// against a server that shrank its caps since we cached params).
    pub fn clamped_into(&self, caps: &Capabilities) -> TuneParams {
        TuneParams {
            concurrency: bounded(
                self.concurrency as i64,
                &caps.limits.concurrency,
                HARD_MAX_CONCURRENCY,
            ) as usize,
            upload_window: bounded(
                self.upload_window as i64,
                &caps.limits.upload_window,
                HARD_MAX_WINDOW,
            ) as usize,
            download_window: bounded(
                self.download_window as i64,
                &caps.limits.download_window,
                HARD_MAX_WINDOW,
            ) as usize,
            chunk_size: bounded(
                self.chunk_size as i64,
                &caps.limits.chunk_size,
                HARD_MAX_CHUNK_SIZE,
            ) as u64,
            compress_level: caps.clamp_level(self.compress_level),
        }
    }

    /// In-flight bytes for an upload: concurrency × window × chunk size.
    pub fn upload_in_flight(&self) -> u64 {
        self.concurrency as u64 * self.upload_window as u64 * self.chunk_size
    }

    /// In-flight bytes for a download: concurrency × window × chunk size.
    pub fn download_in_flight(&self) -> u64 {
        self.concurrency as u64 * self.download_window as u64 * self.chunk_size
    }
}

/// The advertised `min`, intersected with the client's hard ceiling.
///
/// `/capabilities` is untrusted input: a hostile or broken server may
/// advertise a `min` larger than anything we are willing to allocate, so the
/// ceiling always wins (and the result is never below 1).
fn floor_of(range: &IntRange, hard_max: i64) -> i64 {
    range.min.clamp(1, hard_max.max(1))
}

/// Clamp `v` into the advertised range **and** the client's hard ceiling.
///
/// The advertised bounds are never trusted: `min` may exceed `max` (an
/// inverted range must not panic the way `i64::clamp` would) and either
/// bound may be far above what a browser can afford.
fn bounded(v: i64, range: &IntRange, hard_max: i64) -> i64 {
    let hard_max = hard_max.max(1);
    let v = v.max(1);
    if range.min > range.max {
        // Empty/inverted advertisement: the range carries no information, so
        // honour the requested value and only apply our own ceiling.
        return v.min(hard_max).max(1);
    }
    let lo = range.min.clamp(1, hard_max);
    let hi = range.max.min(hard_max).max(lo);
    v.clamp(lo, hi)
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

/// Minimum transfer time one chunk should represent (ms).
///
/// The chunk size follows the *measured link* instead of being ramped last
/// (which in practice never happened: the ramp settles on the window /
/// concurrency dimensions first — the BDP rule stops it — so the chunk stayed
/// at the advertised minimum forever, e.g. 256 KiB even on a 30 Mb/s link).
///
/// One chunk should carry about this much of the link's throughput, so a
/// wide/faster link gets few big requests (less per-request overhead, better
/// per-request compression) while a narrow link keeps the advertised minimum
/// (fine granularity, bounded memory).
///
/// Deliberately *throughput-only*: an RTT term would make a single chunk as
/// big as the whole bandwidth-delay product, which immediately satisfies the
/// ramp's BDP rule and freezes cross-file concurrency at 1 — and uploads
/// cannot sample an RTT at all (XHR exposes no TTFB).
pub const TARGET_CHUNK_SPAN_MS: f64 = 100.0;

/// Boundary the derived chunk size is rounded down to (bytes).
///
/// Chunks do not *have* to be aligned, but round request sizes keep the wire
/// log, the SDK panel and the server's block bookkeeping readable — and the
/// alignment is then re-clamped into the advertised range, so an advertised
/// minimum that is not a multiple of it still wins.
const CHUNK_ALIGN: u64 = 64 * 1024;

/// The chunk size a link of this throughput can use, clamped into the
/// server's advertised range, the client's hard ceiling and the in-flight
/// memory budget (`budget_max`).
///
/// `mbps` is the engine's measured throughput for the current window.
pub fn chunk_for_link(mbps: f64, range: &IntRange, budget_max: u64) -> u64 {
    let floor = floor_of(range, HARD_MAX_CHUNK_SIZE).max(1) as u64;
    if !mbps.is_finite() || mbps <= 0.0 {
        return floor.min(budget_max.max(1)).max(1);
    }
    // bytes = bits/s ÷ 8 × span(s), rounded to a tidy boundary
    let bytes = (mbps * 1e6 / 8.0 * TARGET_CHUNK_SPAN_MS / 1_000.0).clamp(0.0, i64::MAX as f64) as u64;
    let aligned = (bytes / CHUNK_ALIGN).saturating_mul(CHUNK_ALIGN);
    let clamped = bounded(aligned.max(1) as i64, range, HARD_MAX_CHUNK_SIZE) as u64;
    clamped.clamp(1, budget_max.max(1))
}

/// The dimension currently being raised.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RampDim {
    /// The active transfer's per-file window (upload or download).
    Window,
    /// Cross-file concurrency (global in-flight request cap).
    Concurrency,
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
    /// The engine is actively ramping (`phase == Ramping`). A settled state
    /// (parameters reused from the previous transfer of this client) is *not*
    /// ramping: its first window has no baseline yet and must not be mistaken
    /// for +100% growth.
    pub ramping: bool,
    /// Consecutive windows below the gain threshold (before this one).
    pub low_gain_windows: u32,
    /// Current in-flight bytes (concurrency × window × chunk).
    pub in_flight: u64,
    /// Bandwidth-delay product estimate (bytes); 0 = unknown.
    ///
    /// Only consulted on the cross-file concurrency dimension: on the window
    /// dimension the estimate is derived from the very throughput that single
    /// connection achieved, so it would always "prove" that one connection is
    /// enough and freeze the ramp at its minimum; on the chunk dimension the
    /// thing being grown is request *count*, not in-flight bytes.
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
/// 3. cross-file concurrency already ≥ BDP → the pipe is full, settle;
/// 4. RTT inflated > 30% vs baseline → saturated, settle;
/// 5. throughput gain ≥ +5% while ramping → raise (a held window, a settled
///    reuse and the degraded stability hold never register a pseudo-gain);
/// 6. gain < 5% for 2 consecutive windows → the dimension is exhausted: move
///    to the next one (settle at the last dimension, or during a degraded
///    hold);
/// 7. otherwise hold.
///
/// The ramped dimensions are the per-file window and cross-file concurrency;
/// the chunk size follows the measured link instead ([`chunk_for_link`]).
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
    } else if input.ramping {
        // First window of a fresh ramp: no baseline; treat the mere presence
        // of traffic as growth so the first raise happens immediately.
        1.0
    } else {
        // Settled reuse: no ramp is in flight, so the first window only
        // establishes the baseline. Inventing +100% growth here would raise a
        // dimension out of the tuned optimum on every transfer that reuses
        // the previous settle.
        0.0
    };
    // Only an *active* ramp explores dimensions. A settled engine (a reuse of
    // the previous transfer's parameters) must not raise a dimension out of
    // the tuned optimum just because one window happened to be faster; only a
    // failure re-arms the ramp (`transfer_end(false)`).
    if input.ramping && gain >= GAIN_THRESHOLD {
        return (RampAction::Raise, 0);
    }
    let low = input.low_gain_windows + 1;
    if low >= 2 {
        // Flat throughput means the dimension we were growing is exhausted, not
        // necessarily the link: a single-file transfer gains nothing from
        // cross-file concurrency, and a big chunk size can still pay off after
        // the window stopped helping. So explore the *next* dimension instead
        // of settling — except at the last dimension, and in the degraded hold
        // where the conservative settle is the whole point.
        if input.degraded || input.last_dim {
            (RampAction::Settle, low)
        } else {
            (RampAction::AdvanceDim, 0)
        }
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
    level_cfg: CompressLevel,
    /// zrip level resolved from the client's policy against the current
    /// caps — remembered so a mid-transfer re-ramp keeps the policy level
    /// instead of falling back to the advertised minimum.
    policy_level: i32,
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
    /// RTT the *current ramp* started from (the link's baseline latency).
    ///
    /// Saturation is judged against this fixed reference, not against the
    /// previous window's EWMA (comparing the EWMA with itself is always 0
    /// and made the RTT-inflation rule dead code).
    rtt_baseline: Option<f64>,
    prev_mbps: f64,
    low_gain_windows: u32,
    dim: RampDim,
    degraded_windows: u32,
    /// Parameters a transfer of *this* client settled on, reused by the next
    /// transfer (same direction) so a folder of files does not re-ramp per
    /// file. Dropped when the caps change or a transfer fails.
    settled: Option<TuneParams>,
    /// `caps_hash` the settled parameters were measured against.
    settled_caps_hash: String,
    /// Direction the in-memory settle applies to (window dimensions differ per
    /// direction, so a download must not inherit an upload's settle).
    settled_direction: Option<TransferKind>,
    /// Whether we settled from a Degraded hold (not from a full ramp), in
    /// which case the halved parameter set is not reused as a baseline.
    post_degrade_settle: bool,
    /// Which direction is ramping (drives window/chunk dimension selection).
    direction: TransferKind,
    /// Persistent cache of settled results (the browser's `localStorage`);
    /// `None` = in-memory only (e.g. the native client).
    store: Option<Rc<dyn TuneStore>>,
    /// How long a cached result stays usable (ms); `0` disables the cache.
    cache_ttl_ms: u64,
    /// Origin the cache key is built from (`scheme://host[:port]`; empty =
    /// caching off, e.g. an unusable base URL).
    origin: String,
    // JS callback for tuning events (SDK `onTuning`).
    on_tuning: Option<js_sys::Function>,
}

impl TuningEngine {
    /// Create the engine from client configuration.
    pub fn new(enabled: bool, level_cfg: CompressLevel) -> TuningEngine {
        TuningEngine {
            enabled,
            level_cfg,
            policy_level: libfw_core::ZRIP_DEFAULT_LEVEL,
            phase: TunePhase::Uninitialized,
            caps: None,
            caps_hash: String::new(),
            // Placeholder; replaced on the first begin_transfer.
            params: TuneParams {
                concurrency: 1,
                upload_window: 1,
                download_window: 1,
                chunk_size: 1,
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
            rtt_baseline: None,
            prev_mbps: 0.0,
            low_gain_windows: 0,
            dim: RampDim::Window,
            degraded_windows: 0,
            settled: None,
            settled_caps_hash: String::new(),
            settled_direction: None,
            post_degrade_settle: false,
            direction: TransferKind::Download,
            store: None,
            cache_ttl_ms: DEFAULT_TUNE_TTL_MS,
            origin: String::new(),
            on_tuning: None,
        }
    }

    /// Persist settled results through `store`, for at most `ttl_ms`.
    ///
    /// The store is consulted from [`Self::begin_transfer`] (reuse) and written
    /// on every settle; a failed transfer removes the entry. `ttl_ms == 0`
    /// disables the cache while keeping the in-memory reuse per client.
    pub fn set_cache(&mut self, store: Rc<dyn TuneStore>, ttl_ms: u64) {
        self.store = Some(store);
        self.cache_ttl_ms = ttl_ms;
    }

    /// The TTL currently applied to cached results (0 = caching disabled).
    pub fn cache_ttl_ms(&self) -> u64 {
        self.cache_ttl_ms
    }

    /// Record the server base URL the cache key is derived from.
    pub fn set_origin(&mut self, url: &str) {
        self.origin = origin_of(url);
    }

    /// Cache key for the current origin + direction (`None` when caching is
    /// off: no store, no TTL, or an unusable origin).
    fn cache_key(&self) -> Option<String> {
        if self.store.is_none() || self.cache_ttl_ms == 0 || self.origin.is_empty() {
            return None;
        }
        Some(tune_key(&self.origin, self.direction))
    }

    /// Drop the persisted entry for the current origin + direction.
    fn remove_cache_entry(&self) {
        if let (Some(store), Some(key)) = (&self.store, self.cache_key()) {
            store.remove(&key);
        }
    }

    /// Read the persisted settle for the current origin + direction.
    ///
    /// Returns `None` — and cleans the entry up — unless it is readable, was
    /// measured against the *current* capabilities and is younger than the
    /// TTL. The TTL counts from the moment the ramp settled (not from the last
    /// reuse), so a link that was measured long ago is re-measured even if it
    /// is used constantly.
    fn load_cached_settle(&mut self, now_ms: f64, caps: &Capabilities) -> Option<TuneParams> {
        let key = self.cache_key()?;
        let store = self.store.clone()?;
        let raw = store.get(&key)?;
        let entry: TuneCache = match serde_json::from_str(&raw) {
            Ok(entry) => entry,
            Err(_) => {
                // Unreadable (a corrupted row or an older layout): drop it.
                store.remove(&key);
                return None;
            }
        };
        let aged = now_ms - entry.saved_at_ms as f64;
        let fresh = aged >= 0.0 && aged <= self.cache_ttl_ms as f64;
        if entry.v != TUNE_CACHE_VERSION || entry.caps_hash != caps.caps_hash() || !fresh {
            store.remove(&key);
            return None;
        }
        Some(entry.params.into_params(self.policy_level))
    }

    /// Persist the current parameters as this origin + direction's settle.
    fn save_cached_settle(&self, now_ms: f64) {
        let Some(key) = self.cache_key() else { return };
        let Some(store) = self.store.as_ref() else { return };
        let entry = TuneCache {
            v: TUNE_CACHE_VERSION,
            caps_hash: self.caps_hash.clone(),
            saved_at_ms: if now_ms > 0.0 { now_ms as u64 } else { 0 },
            params: TuneParamsCache::of(&self.params),
        };
        if let Ok(json) = serde_json::to_string(&entry) {
            store.set(&key, &json);
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

    /// Start a transfer.
    ///
    /// The parameters are taken from the best available source, in order:
    /// 1. an in-memory settle from an earlier transfer of this client (same
    ///    direction, same capabilities);
    /// 2. a *cached* settle for this origin + direction that is still inside
    ///    its TTL and was measured against the same capabilities — so a page
    ///    refresh (or a later visit) skips the ramp;
    /// 3. otherwise the advertised minimums (a fresh ramp).
    ///
    /// Returns the parameters the transfer should start with. When tuning is
    /// disabled this returns static config-derived params (legacy), so
    /// callers can always use the result.
    pub fn begin_transfer(
        &mut self,
        caps: &Capabilities,
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
        // Per-transfer measurement state MUST NOT leak across transfers: a
        // stale `prev_mbps` / `low_gain_windows` / RTT EWMA from the previous
        // link would make the first window of this transfer settle (or raise)
        // on numbers that describe a different connection.
        self.prev_mbps = 0.0;
        self.low_gain_windows = 0;
        self.rtt_ewma = None;
        self.rtt_baseline = None;
        self.degraded_windows = 0;

        // The compression level is a *client policy*, not a ramped dimension
        // (the ramp only moves window / concurrency / chunk size), so it is
        // resolved from the client's configured policy in every branch below.
        let policy_level = static_params.compress_level;
        self.policy_level = policy_level;

        if !self.enabled {
            self.phase = TunePhase::Uninitialized;
            self.params = static_params.clamped_into(caps);
            self.enforce_in_flight_budget();
            return self.params.clone();
        }

        // Reuse only a settle measured against *these* capabilities *and* this
        // direction: a server that changed its advertisement — or an upload
        // settle offered to a download — invalidates it.
        let in_memory = self.settled.clone().filter(|_| {
            self.settled_caps_hash == self.caps_hash
                && self.settled_direction == Some(self.direction)
        });
        let reusable = match in_memory {
            Some(settled) => Some(settled),
            None => self.load_cached_settle(now_ms, caps),
        };
        match reusable {
            Some(settled) => {
                self.params = settled.clamped_into(caps);
                self.params.compress_level = policy_level;
                self.phase = TunePhase::Settled;
            }
            None => {
                self.params = TuneParams::from_caps_mins(caps);
                self.params.compress_level = policy_level;
                self.stats = TuneStats::default();
                self.phase = TunePhase::Ramping;
            }
        }
        self.dim = RampDim::Window;
        // The advertised minimums are untrusted: keep the table inside the
        // client's per-file in-flight byte budget.
        self.enforce_in_flight_budget();
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
            if self.enabled && self.rtt_baseline.is_none() {
                // Seed the ramp's latency baseline from this transfer's first
                // sample; `rtt_inflation` measures saturation against it, so
                // a growing RTT settles the ramp instead of pushing it on.
                self.rtt_baseline = Some(new_ewma);
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

        // Size the next chunk from the link we just measured: a wider/faster
        // link gets bigger chunks (fewer requests, better per-request
        // compression), a narrow one keeps the advertised minimum. This runs
        // on every window — including after the ramp settles — so the chunk
        // size tracks the link for the whole transfer instead of only being
        // explored once the window and concurrency dimensions were exhausted
        // (which the BDP rule almost always pre-empted, leaving the chunk at
        // the 256 KiB minimum forever).
        self.retune_chunk_size(mbps, &caps);

        // BDP / RTT saturation only applies while actively ramping: a
        // Degraded phase's "stability windows" (or a settled reuse) must not
        // be cut short by a tiny-BDP computation on near-zero traffic.
        let ramping = self.phase == TunePhase::Ramping;
        let input = RampInput {
            mbps,
            prev_mbps: self.prev_mbps,
            rtt_inflation: if ramping { self.rtt_inflation() } else { 0.0 },
            errors: self.window_errors,
            degraded: self.phase == TunePhase::Degraded,
            ramping,
            low_gain_windows: self.low_gain_windows,
            in_flight: self.in_flight(),
            // See `RampInput::bdp`: the in-flight/BDP check only gates
            // cross-file concurrency, never the window or the chunk size.
            bdp: if ramping && self.dim == RampDim::Concurrency {
                bdp_bytes(mbps, self.rtt_ewma.unwrap_or(self.stats.rtt_ms))
            } else {
                0
            },
            at_cap: self.dim_at_cap(&caps),
            last_dim: self.dim == RampDim::Concurrency,
        };
        let (action, low) = ramp_action(&input);
        self.low_gain_windows = low;
        self.windows_closed += 1;
        self.prev_mbps = mbps;

        match action {
            RampAction::Raise => {
                self.raise_dim(&caps);
                self.enforce_in_flight_budget();
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
                // Remember this settle for the *next* transfer of this same
                // client, and persist it for later visits (bounded by the
                // cache TTL). A post-degrade settle is excluded: its halved
                // parameters are not a good reuse baseline.
                if !self.post_degrade_settle {
                    self.settled = Some(self.params.clone());
                    self.settled_caps_hash = self.caps_hash.clone();
                    self.settled_direction = Some(self.direction);
                    self.save_cached_settle(now_ms);
                }
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

    /// Mark the transfer outcome.
    ///
    /// A failure forgets the in-session settle *and* drops the cached entry, so
    /// the next transfer re-ramps from the advertised minimums instead of
    /// reusing parameters that just failed.
    pub fn transfer_end(&mut self, ok: bool) {
        if !self.enabled {
            return;
        }
        if !ok {
            self.settled = None;
            self.settled_caps_hash.clear();
            self.settled_direction = None;
            self.remove_cache_entry();
            self.phase = TunePhase::Ramping;
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
        // Inflation is measured against the latency the *ramp* started from,
        // never against the current EWMA (comparing the EWMA with itself is
        // always 0, which made this rule unreachable).
        match (self.rtt_ewma, self.rtt_baseline) {
            (Some(cur), Some(base)) if base > 0.0 => (cur / base - 1.0).max(0.0),
            _ => 0.0,
        }
    }

    fn in_flight(&self) -> u64 {
        match self.last_transfer_kind() {
            TransferKind::Upload => self.params.upload_in_flight(),
            TransferKind::Download => self.params.download_in_flight(),
        }
    }

    /// The current value of the dimension being ramped.
    fn dim_value(&self) -> i64 {
        match self.dim {
            RampDim::Window => match self.last_transfer_kind() {
                TransferKind::Upload => self.params.upload_window as i64,
                TransferKind::Download => self.params.download_window as i64,
            },
            RampDim::Concurrency => self.params.concurrency as i64,
        }
    }

    /// The maximum the current dimension may reach: the server's advertised
    /// max intersected with the client's hard ceiling.
    fn dim_max(&self, caps: &Capabilities) -> i64 {
        let advertised = match self.dim {
            RampDim::Window => match self.last_transfer_kind() {
                TransferKind::Upload => caps.limits.upload_window.max,
                TransferKind::Download => caps.limits.download_window.max,
            },
            RampDim::Concurrency => caps.limits.concurrency.max,
        };
        let hard = match self.dim {
            RampDim::Window => HARD_MAX_WINDOW,
            RampDim::Concurrency => HARD_MAX_CONCURRENCY,
        };
        advertised.min(hard).max(1)
    }

    /// Largest chunk size that keeps `concurrency × window × chunk` inside
    /// [`HARD_MAX_IN_FLIGHT_BYTES`] for the widest direction.
    fn max_chunk_for_budget(&self) -> u64 {
        let widest = self.params.upload_window.max(self.params.download_window).max(1) as u64;
        let divisor = (self.params.concurrency.max(1) as u64) * widest;
        (HARD_MAX_IN_FLIGHT_BYTES / divisor).max(1)
    }

    /// Point the chunk size at the measured link (see [`chunk_for_link`]).
    ///
    /// A ±50 % deadband (hysteresis) keeps a link that hovers around a size
    /// boundary from flipping the chunk back and forth every window — each
    /// flip also re-aligns the remaining upload blocks / download batches, so
    /// stability matters more than tracking the estimate exactly.
    fn retune_chunk_size(&mut self, mbps: f64, caps: &Capabilities) {
        let target = chunk_for_link(mbps, &caps.limits.chunk_size, self.max_chunk_for_budget());
        let current = self.params.chunk_size.max(1);
        let grow = current.saturating_mul(3) / 2;
        let shrink = (current / 2).max(1);
        if target > grow || target < shrink {
            self.params.chunk_size = target;
            // A bigger chunk can push `concurrency × window × chunk` past the
            // memory budget (it divides by the *widest* direction's window, so
            // the mix matters); keep the table inside it.
            self.enforce_in_flight_budget();
        }
    }

    /// Shrink the tuned chunk size into the in-flight byte budget.
    ///
    /// Applied to the ramp-start table (whose values come straight from the
    /// advertised minimums) and after every raise, so no sequence of
    /// server-advertised values can push the engine past its memory budget.
    fn enforce_in_flight_budget(&mut self) {
        let max_chunk = self.max_chunk_for_budget();
        self.params.chunk_size = self.params.chunk_size.clamp(1, max_chunk);
    }

    fn dim_at_cap(&self, caps: &Capabilities) -> bool {
        self.dim_value() >= self.dim_max(caps)
    }

    fn raise_dim(&mut self, caps: &Capabilities) {
        let dim_max: i64 = self.dim_max(caps);
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

/// The ramp order: per-file window → cross-file concurrency.
///
/// The chunk size is NOT a ramp dimension: it follows the measured link
/// (see [`chunk_for_link`]).
fn next_dim(dim: RampDim) -> RampDim {
    match dim {
        RampDim::Window => RampDim::Concurrency,
        // Concurrency is the terminal dimension: `ramp_action` returns
        // `Settle` (not `AdvanceDim`) when it is at cap, so this arm is
        // unreachable in practice. It is kept for exhaustiveness; if a new
        // dimension is added, the compiler will force updating this function.
        RampDim::Concurrency => RampDim::Concurrency,
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
    let _ = js_sys::Reflect::set(&o, &JsValue::from_str("compressLevel"), &JsValue::from_f64(params.compress_level as f64));
    o.into()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn caps() -> Capabilities {
        Capabilities::default()
    }

    fn static_params() -> TuneParams {
        TuneParams {
            concurrency: 4,
            upload_window: 8,
            download_window: 4,
            chunk_size: 2 * 1024 * 1024,
            compress_level: 1,
        }
    }

    /// An auto-tuning engine with one upload transfer already in flight.
    fn engine() -> TuningEngine {
        let mut e = TuningEngine::new(true, CompressLevel::Balanced);
        e.set_direction(TransferKind::Upload);
        e.begin_transfer(&caps(), 0.0, &static_params());
        e
    }

    /// Drives the ramp with a flat throughput profile until the engine settles
    /// (the explore-then-settle path exercised by `flat_gain_...`), so the
    /// reuse tests start from a genuinely tuned state.
    fn ramp_to_settle(e: &mut TuningEngine) {
        let step = 12_500_000u64;
        let mut done = step;
        for i in 0..16 {
            let t = 1_000.0 * (i as f64 + 1.0);
            if let Some(TuneEvent {
                phase: TunePhase::Settled,
                ..
            }) = e.tick(t, done, Some(400.0), false)
            {
                return;
            }
            done += step;
        }
        panic!("engine did not settle");
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
            ramping: true,
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
            ramping: true,
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
            ramping: true,
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
                ramping: true,
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

        // Second below-threshold window → the dimension is exhausted. Not the
        // last dimension → explore the next one (chunk size still matters even
        // when the window stopped helping).
        let flat2 = RampInput { low_gain_windows: 1, ..flat };
        let (action, low) = ramp_action(&flat2);
        assert_eq!(action, RampAction::AdvanceDim);
        assert_eq!(low, 0, "advancing resets the low-gain counter");

        // …unless it is the last dimension → settle.
        let flat_last = RampInput {
            last_dim: true,
            low_gain_windows: 1,
            ..flat
        };
        assert_eq!(ramp_action(&flat_last).0, RampAction::Settle);

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
                ramping: true,
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
    fn request_budget_covers_files_and_the_per_file_window() {
        // A minimal ramp must still be able to pipeline its window: the pool is
        // `concurrency × window`, not `concurrency`.
        assert_eq!(request_budget(1, 1), 1);
        assert_eq!(request_budget(1, 8), 8);
        assert_eq!(request_budget(4, 8), 32);
        // Zeroes can never produce an empty (deadlocked) pool.
        assert_eq!(request_budget(0, 0), 1);
        // Hostile/absurd input stays inside the hard ceiling.
        assert_eq!(request_budget(64, 64), HARD_MAX_IN_FLIGHT_REQUESTS);

        // The tuned table reports the widest direction's window.
        let mut p = static_params();
        p.concurrency = 2;
        p.upload_window = 8;
        p.download_window = 4;
        assert_eq!(p.request_budget(), 16);
        p.upload_window = 4;
        assert_eq!(p.request_budget(), 8);
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
        let mut e = TuningEngine::new(true, CompressLevel::Balanced);
        e.set_direction(TransferKind::Upload);
        let p = e.begin_transfer(&caps(), 0.0, &static_params());
        assert_eq!(e.phase(), TunePhase::Ramping);
        assert_eq!(p.concurrency, 1);
        assert_eq!(p.upload_window, 1);
        assert_eq!(p.chunk_size, 262_144);
    }

    #[test]
    fn settled_reuse_first_window_does_not_raise() {
        // A settled cache reuse is not a ramp: its first window has no
        // baseline, so it must hold rather than invent +100% growth and bump
        // a dimension out of the cached optimum.
        let input = RampInput {
            mbps: 100.0,
            prev_mbps: 0.0,
            rtt_inflation: 0.0,
            errors: 0,
            degraded: false,
            ramping: false,
            low_gain_windows: 0,
            in_flight: 10,
            bdp: 0,
            at_cap: false,
            last_dim: false,
        };
        let (action, low) = ramp_action(&input);
        assert_eq!(action, RampAction::Hold);
        assert_eq!(low, 1);
    }

    #[test]
    fn reused_params_do_not_raise_on_the_first_window() {
        let mut e = engine();
        ramp_to_settle(&mut e);
        let tuned = e.params().clone();
        e.transfer_end(true);
        e.begin_transfer(&caps(), 60_000.0, &static_params());
        // A burst of throughput right away must not raise the tuned
        // dimensions: this transfer has no baseline yet and is not ramping.
        let ev = e.tick(61_000.0, 100_000_000, Some(400.0), false).unwrap();
        assert_eq!(ev.phase, TunePhase::Settled);
        assert_eq!(
            ev.params.upload_window, tuned.upload_window,
            "settled reuse must not raise the window"
        );
        assert_eq!(ev.params.concurrency, tuned.concurrency);
    }

    #[test]
    fn rtt_inflation_settles_the_ramp() {
        let mut e = engine();
        // Window 1 anchors the ramp's latency baseline at 100 ms.
        let ev = e.tick(1_000.0, 12_500_000, Some(100.0), false).unwrap();
        assert_eq!(ev.phase, TunePhase::Ramping);
        // Constant throughput but the EWMA RTT inflates > 30% → saturated.
        let ev = e.tick(2_000.0, 25_000_000, Some(400.0), false).unwrap();
        assert_eq!(ev.phase, TunePhase::Settled, "RTT inflation must settle");
    }

    #[test]
    fn ramp_start_keeps_the_configured_compress_policy() {
        let mut e = TuningEngine::new(true, CompressLevel::Balanced);
        e.set_direction(TransferKind::Upload);
        let sp = static_params(); // compress_level: 1 (Balanced)
        let p = e.begin_transfer(&caps(), 0.0, &sp);
        assert_eq!(
            p.compress_level, 1,
            "a ramp must not silently drop to the advertised minimum (-8)"
        );
        // A settle — and the in-session reuse that follows it — keep the
        // policy level too (it is never a ramped dimension).
        ramp_to_settle(&mut e);
        e.transfer_end(true);
        let p2 = e.begin_transfer(&caps(), 60_000.0, &sp);
        assert_eq!(p2.compress_level, 1, "reuse keeps the policy level");
    }

    #[test]
    fn absurd_advertised_caps_are_clamped_to_hard_ceilings() {
        let mut hostile = caps();
        hostile.limits.concurrency = IntRange {
            default: 1,
            max: 100_000,
            min: 1_000,
        };
        hostile.limits.upload_window = IntRange {
            default: 1,
            max: 100_000,
            min: 500,
        };
        hostile.limits.download_window = IntRange {
            default: 1,
            max: 100_000,
            min: 500,
        };
        hostile.limits.chunk_size = IntRange {
            default: i64::MAX,
            max: i64::MAX,
            min: i64::MAX,
        };
        let mut e = TuningEngine::new(true, CompressLevel::Balanced);
        e.set_direction(TransferKind::Upload);
        let p = e.begin_transfer(&hostile, 0.0, &static_params());
        assert!(p.concurrency <= HARD_MAX_CONCURRENCY as usize);
        assert!(p.upload_window <= HARD_MAX_WINDOW as usize);
        assert!(p.download_window <= HARD_MAX_WINDOW as usize);
        assert!(p.chunk_size <= HARD_MAX_CHUNK_SIZE as u64);
        assert!(
            p.upload_in_flight() <= HARD_MAX_IN_FLIGHT_BYTES,
            "in-flight bytes {} exceed the budget",
            p.upload_in_flight()
        );
    }

    #[test]
    fn inverted_advertised_range_does_not_panic() {
        let mut broken = caps();
        broken.limits.concurrency = IntRange {
            default: 4,
            max: 2,
            min: 8,
        };
        broken.limits.chunk_size = IntRange {
            default: 0,
            max: 0,
            min: 0,
        };
        // Both the static and the ramping paths must survive junk caps.
        let mut e = TuningEngine::new(false, CompressLevel::Balanced);
        e.set_direction(TransferKind::Upload);
        let p = e.begin_transfer(&broken, 0.0, &static_params());
        assert!(p.concurrency >= 1 && p.chunk_size >= 1);
        let mut e2 = TuningEngine::new(true, CompressLevel::Balanced);
        e2.set_direction(TransferKind::Upload);
        let p2 = e2.begin_transfer(&broken, 0.0, &static_params());
        assert!(p2.concurrency >= 1 && p2.chunk_size >= 1);
    }

    #[test]
    fn ramp_raises_window_then_concurrency_until_cap() {
        let mut e = engine();
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
        let mut e = engine();
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
    fn flat_gain_explores_dimensions_then_settles() {
        let mut e = engine();
        // Constant throughput after the first raise: every dimension gets its
        // 2 flat windows and the ramp then moves on to the next one instead of
        // settling. The chunk size is not a ramp dimension any more — it
        // follows the measured link (see the `chunk_for_link` tests).
        let step = 12_500_000u64;
        let mut done = step;
        let mut t = 1_000.0;

        // Window: one pseudo-gain raise, then 2 flat windows → explore.
        let ev = e.tick(t, done, Some(400.0), false).unwrap();
        assert_eq!(ev.params.upload_window, 2);
        for _ in 0..2 {
            t += 1_000.0;
            done += step;
            e.tick(t, done, Some(400.0), false);
        }
        assert_eq!(
            e.phase(),
            TunePhase::Ramping,
            "a flat window dimension must not end the ramp"
        );

        // Cross-file concurrency is the last dimension → 2 flat windows settle.
        t += 1_000.0;
        done += step;
        let ev = e.tick(t, done, Some(400.0), false).unwrap();
        assert_eq!(ev.phase, TunePhase::Ramping);
        t += 1_000.0;
        done += step;
        let ev = e.tick(t, done, Some(400.0), false).unwrap();
        assert_eq!(ev.phase, TunePhase::Settled);
        assert_eq!(ev.params.upload_window, 2);
        e.transfer_end(true);
    }

    #[test]
    fn chunk_size_tracks_the_link_and_ignores_jitter() {
        let mut e = engine();
        // First window ≈100 Mb/s → 100 ms of it, 64 KiB-aligned.
        let mut done = 12_500_000u64;
        let ev = e.tick(1_000.0, done, Some(400.0), false).unwrap();
        let first = ev.params.chunk_size;
        assert_eq!(first, 1_245_184, "the chunk must follow the measured link");

        // ±10 % jitter must not move it (the deadband absorbs it, and every
        // move re-aligns the in-flight block list).
        let mut t = 1_000.0;
        for _ in 0..3 {
            t += 1_000.0;
            done += 13_500_000; // ≈108 Mb/s
            let ev = e.tick(t, done, Some(400.0), false).unwrap();
            assert_eq!(ev.params.chunk_size, first, "jitter must not re-size the chunk");
        }

        // A clearly wider band does grow it.
        t += 1_000.0;
        done += 100_000_000; // ≈800 Mb/s
        let ev = e.tick(t, done, Some(400.0), false).unwrap();
        assert!(ev.params.chunk_size > first, "the chunk must grow on a wider band");
    }

    #[test]
    fn chunk_size_follows_the_measured_link() {
        let range = IntRange {
            min: 262_144,
            default: 2 * 1024 * 1024,
            max: 8 * 1024 * 1024,
        };
        let budget = 8 * 1024 * 1024;
        // Unknown link → the advertised minimum (never a guess).
        assert_eq!(chunk_for_link(0.0, &range, budget), 262_144);
        // Narrow/slow link: still the min (100 ms of 10 Mb/s < 256 KiB).
        assert_eq!(chunk_for_link(10.0, &range, budget), 262_144);
        // Wider band → bigger chunk (100 ms of the link, 64 KiB-aligned).
        assert_eq!(chunk_for_link(30.0, &range, budget), 327_680);
        assert_eq!(chunk_for_link(100.0, &range, budget), 1_245_184);
        // …capped by the advertised max…
        assert_eq!(chunk_for_link(1_000.0, &range, budget), 8 * 1024 * 1024);
        // …and by the in-flight byte budget.
        assert_eq!(chunk_for_link(1_000.0, &range, 1_500_000), 1_500_000);
        // Monotonic in bandwidth, and never below the advertised minimum.
        let mut last = 0;
        for mbps in [1.0, 5.0, 20.0, 80.0, 320.0] {
            let v = chunk_for_link(mbps, &range, budget);
            assert!(v >= last && v >= 262_144, "{mbps} Mb/s → {v}");
            last = v;
        }
        // Junk (NaN / inverted range) must not panic or return 0.
        assert!(chunk_for_link(f64::NAN, &range, budget) >= 1);
        assert!(chunk_for_link(-5.0, &range, budget) >= 1);
        let inverted = IntRange {
            min: 9_000_000,
            max: 1,
            default: 0,
        };
        let v = chunk_for_link(50.0, &inverted, budget);
        assert!(v >= 1 && v <= budget, "{v}");
    }

    #[test]
    fn settled_state_is_reused_within_the_same_client() {
        let mut e = engine();
        ramp_to_settle(&mut e);
        assert_eq!(e.phase(), TunePhase::Settled);
        let tuned = e.params().clone();
        e.transfer_end(true);
        // A follow-up transfer on the same client does not pay for another
        // ramp: the settle of the previous transfer is reused.
        let p = e.begin_transfer(&caps(), 60_000.0, &static_params());
        assert_eq!(e.phase(), TunePhase::Settled, "in-session reuse skips the ramp");
        assert_eq!(p.upload_window, tuned.upload_window);
        assert_eq!(p.concurrency, tuned.concurrency);
        assert_eq!(p.chunk_size, tuned.chunk_size);
        assert_eq!(p.compress_level, 1, "reuse keeps the configured policy level");
    }

    #[test]
    fn a_new_engine_always_re_ramps_from_mins() {
        // With no store installed there is nothing to persist: a fresh client
        // re-measures the link from scratch. (The browser engine installs a
        // `localStorage` store — see the cache tests below.)
        let mut e = engine();
        ramp_to_settle(&mut e);
        e.transfer_end(true);
        let fresh = engine();
        assert_eq!(fresh.phase(), TunePhase::Ramping);
        assert_eq!(fresh.params().upload_window, 1);
        assert_eq!(fresh.params().chunk_size, 262_144);
    }

    // --- persisted (browser) tuning cache -----------------------------------

    /// In-memory `TuneStore` standing in for `localStorage`.
    #[derive(Clone, Default)]
    struct FakeStore(Rc<RefCell<std::collections::HashMap<String, String>>>);

    impl FakeStore {
        fn len(&self) -> usize {
            self.0.borrow().len()
        }
        fn put_raw(&self, key: &str, value: &str) {
            self.0.borrow_mut().insert(key.to_string(), value.to_string());
        }
        fn keys(&self) -> Vec<String> {
            self.0.borrow().keys().cloned().collect()
        }
    }

    impl TuneStore for FakeStore {
        fn get(&self, key: &str) -> Option<String> {
            self.0.borrow().get(key).cloned()
        }
        fn set(&self, key: &str, value: &str) {
            self.0.borrow_mut().insert(key.to_string(), value.to_string());
        }
        fn remove(&self, key: &str) {
            self.0.borrow_mut().remove(key);
        }
    }

    const ORIGIN: &str = "http://localhost:8080";

    /// An engine wired to `store`, pointed at `origin`, with no transfer yet.
    ///
    /// Used for the *reading* side of the cache: starting a transfer here at a
    /// bogus `now_ms` would look like a clock jump and evict the row.
    fn wired(
        store: &FakeStore,
        ttl_ms: u64,
        direction: TransferKind,
        origin: &str,
    ) -> TuningEngine {
        let mut e = TuningEngine::new(true, CompressLevel::Balanced);
        e.set_cache(Rc::new(store.clone()), ttl_ms);
        e.set_direction(direction);
        e.set_origin(origin);
        e
    }

    /// The same engine, with an upload transfer already in flight at t = 0.
    fn cached_engine(store: &FakeStore, ttl_ms: u64) -> TuningEngine {
        let mut e = wired(store, ttl_ms, TransferKind::Upload, ORIGIN);
        e.begin_transfer(&caps(), 0.0, &static_params());
        e
    }

    #[test]
    fn a_settle_is_cached_and_reused_by_a_new_engine() {
        let store = FakeStore::default();
        let mut a = cached_engine(&store, DEFAULT_TUNE_TTL_MS);
        ramp_to_settle(&mut a);
        assert_eq!(a.phase(), TunePhase::Settled);
        let tuned = a.params().clone();
        a.transfer_end(true);
        assert_eq!(store.len(), 1, "the settle is written on convergence");
        assert_eq!(
            store.keys(),
            vec![tune_key(ORIGIN, TransferKind::Upload)],
            "keyed by origin + direction"
        );

        // A *new* engine (i.e. a page refresh) reuses it instead of ramping.
        let mut b = wired(&store, DEFAULT_TUNE_TTL_MS, TransferKind::Upload, ORIGIN);
        let p = b.begin_transfer(&caps(), 20_000.0, &static_params());
        assert_eq!(b.phase(), TunePhase::Settled, "a cache hit skips the ramp");
        assert_eq!(p.upload_window, tuned.upload_window);
        assert_eq!(p.concurrency, tuned.concurrency);
        assert_eq!(p.chunk_size, tuned.chunk_size);
        assert_eq!(p.compress_level, 1, "the policy level is not cached");
    }

    #[test]
    fn a_cached_settle_expires_after_the_ttl() {
        let store = FakeStore::default();
        let ttl = 60_000;
        let mut a = cached_engine(&store, ttl);
        ramp_to_settle(&mut a);
        let saved = a.params().clone();
        assert!(saved.upload_window > 1, "the ramp must have raised something");
        a.transfer_end(true);

        // The TTL counts from the settle, so the boundary is exact.
        let row: TuneCache = serde_json::from_str(
            &store.get(&tune_key(ORIGIN, TransferKind::Upload)).unwrap(),
        )
        .unwrap();
        let saved_at = row.saved_at_ms as f64;

        // Just inside the TTL → reuse.
        let mut b = wired(&store, ttl, TransferKind::Upload, ORIGIN);
        b.begin_transfer(&caps(), saved_at + ttl as f64 - 1.0, &static_params());
        assert_eq!(b.phase(), TunePhase::Settled);

        // Past the TTL → re-ramp, and the stale row is dropped.
        let mut c = wired(&store, ttl, TransferKind::Upload, ORIGIN);
        let p = c.begin_transfer(&caps(), saved_at + ttl as f64 + 1.0, &static_params());
        assert_eq!(c.phase(), TunePhase::Ramping);
        assert_eq!(p.upload_window, 1, "an expired result re-ramps from the mins");
        assert_eq!(store.len(), 0, "an expired row is cleaned up");
    }

    #[test]
    fn a_zero_ttl_disables_the_cache() {
        let store = FakeStore::default();
        assert_eq!(cached_engine(&store, 0).cache_ttl_ms(), 0);
        let mut a = cached_engine(&store, 0);
        ramp_to_settle(&mut a);
        a.transfer_end(true);
        assert_eq!(store.len(), 0, "ttl = 0 never writes");

        // An existing row is ignored as well.
        let store2 = FakeStore::default();
        let mut x = cached_engine(&store2, DEFAULT_TUNE_TTL_MS);
        ramp_to_settle(&mut x);
        x.transfer_end(true);
        assert_eq!(store2.len(), 1);
        let mut y = wired(&store2, 0, TransferKind::Upload, ORIGIN);
        y.begin_transfer(&caps(), 20_000.0, &static_params());
        assert_eq!(y.phase(), TunePhase::Ramping, "ttl = 0 never reads");
    }

    #[test]
    fn a_cached_settle_is_scoped_to_one_direction() {
        let store = FakeStore::default();
        let mut a = cached_engine(&store, DEFAULT_TUNE_TTL_MS);
        ramp_to_settle(&mut a);
        a.transfer_end(true);

        // The ramped upload dimensions say nothing about a download.
        let mut b = wired(&store, DEFAULT_TUNE_TTL_MS, TransferKind::Download, ORIGIN);
        let p = b.begin_transfer(&caps(), 20_000.0, &static_params());
        assert_eq!(b.phase(), TunePhase::Ramping);
        assert_eq!(p.download_window, 1);

        // …but the upload direction still hits.
        let mut c = wired(&store, DEFAULT_TUNE_TTL_MS, TransferKind::Upload, ORIGIN);
        c.begin_transfer(&caps(), 20_000.0, &static_params());
        assert_eq!(c.phase(), TunePhase::Settled);
    }

    #[test]
    fn a_cached_settle_is_scoped_to_one_origin() {
        let store = FakeStore::default();
        let mut a = cached_engine(&store, DEFAULT_TUNE_TTL_MS);
        ramp_to_settle(&mut a);
        a.transfer_end(true);

        let mut b = wired(
            &store,
            DEFAULT_TUNE_TTL_MS,
            TransferKind::Upload,
            "https://other.example.com:8443",
        );
        b.begin_transfer(&caps(), 20_000.0, &static_params());
        assert_eq!(b.phase(), TunePhase::Ramping, "another origin is another link");
        assert_eq!(store.len(), 1, "the other origin's row is left alone");
    }

    #[test]
    fn an_unusable_origin_disables_the_cache() {
        // Only http(s) URLs with an authority are cacheable: a bad key would
        // mean reusing another server's link.
        let store = FakeStore::default();
        for url in ["", "not a url", "ftp://host/x", "http://", "ws://h:80"] {
            let mut e = wired(&store, DEFAULT_TUNE_TTL_MS, TransferKind::Upload, url);
            let _ = e.begin_transfer(&caps(), 0.0, &static_params());
            ramp_to_settle(&mut e);
            e.transfer_end(true);
            assert_eq!(store.len(), 0, "{url:?} must not be cached");
        }
    }

    #[test]
    fn changed_caps_invalidate_a_cached_settle() {
        let store = FakeStore::default();
        let mut a = cached_engine(&store, DEFAULT_TUNE_TTL_MS);
        ramp_to_settle(&mut a);
        a.transfer_end(true);
        assert_eq!(store.len(), 1);

        let mut changed = caps();
        changed.limits.concurrency.default = 6;
        let mut b = wired(&store, DEFAULT_TUNE_TTL_MS, TransferKind::Upload, ORIGIN);
        let p = b.begin_transfer(&changed, 20_000.0, &static_params());
        assert_eq!(b.phase(), TunePhase::Ramping);
        assert_eq!(p.upload_window, 1);
        assert_eq!(store.len(), 0, "a caps mismatch drops the entry");
    }

    #[test]
    fn a_failed_transfer_removes_the_cached_settle() {
        let store = FakeStore::default();
        let mut a = cached_engine(&store, DEFAULT_TUNE_TTL_MS);
        ramp_to_settle(&mut a);
        a.transfer_end(false);
        assert_eq!(store.len(), 0, "parameters that just failed are not kept");

        // Sanity: the same engine re-ramps rather than replaying the failure.
        let p = a.begin_transfer(&caps(), 20_000.0, &static_params());
        assert_eq!(a.phase(), TunePhase::Ramping);
        assert_eq!(p.upload_window, 1);
    }

    #[test]
    fn a_corrupt_or_foreign_cache_entry_is_dropped() {
        let store = FakeStore::default();
        let key = tune_key(ORIGIN, TransferKind::Upload);

        // Not JSON at all.
        store.put_raw(&key, "{not json");
        let mut e = wired(&store, DEFAULT_TUNE_TTL_MS, TransferKind::Upload, ORIGIN);
        e.begin_transfer(&caps(), 20_000.0, &static_params());
        assert_eq!(e.phase(), TunePhase::Ramping);
        assert_eq!(store.len(), 0, "an unreadable row is removed");

        // A future layout (version bump) is ignored, not mis-parsed.
        let future = TuneCache {
            v: TUNE_CACHE_VERSION + 1,
            caps_hash: caps().caps_hash(),
            saved_at_ms: 1_000,
            params: TuneParamsCache {
                concurrency: 12,
                upload_window: 16,
                download_window: 16,
                chunk_size: 4 * 1024 * 1024,
            },
        };
        store.put_raw(&key, &serde_json::to_string(&future).unwrap());
        let mut e = wired(&store, DEFAULT_TUNE_TTL_MS, TransferKind::Upload, ORIGIN);
        e.begin_transfer(&caps(), 20_000.0, &static_params());
        assert_eq!(e.phase(), TunePhase::Ramping);
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn a_valid_row_written_by_another_client_is_reused() {
        // The row format is the contract between sessions: a literal entry
        // (as `localStorage` would hold it) must load.
        let store = FakeStore::default();
        let row = TuneCache {
            v: TUNE_CACHE_VERSION,
            caps_hash: caps().caps_hash(),
            saved_at_ms: 5_000,
            params: TuneParamsCache {
                concurrency: 3,
                upload_window: 7,
                download_window: 3,
                chunk_size: 1_048_576,
            },
        };
        store.put_raw(
            &tune_key(ORIGIN, TransferKind::Upload),
            &serde_json::to_string(&row).unwrap(),
        );
        let mut e = wired(&store, DEFAULT_TUNE_TTL_MS, TransferKind::Upload, ORIGIN);
        let p = e.begin_transfer(&caps(), 10_000.0, &static_params());
        assert_eq!(e.phase(), TunePhase::Settled);
        assert_eq!(p.concurrency, 3);
        assert_eq!(p.upload_window, 7);
        assert_eq!(p.chunk_size, 1_048_576);
        assert_eq!(store.len(), 1, "a fresh, valid row is kept");
    }

    #[test]
    fn tune_key_and_origin_of_normalise_the_scope() {
        assert_eq!(
            tune_key("http://localhost:8080", TransferKind::Upload),
            "libfw.tune.v1.upload.http://localhost:8080"
        );
        assert_eq!(
            tune_key("http://localhost:8080", TransferKind::Download),
            "libfw.tune.v1.download.http://localhost:8080"
        );
        assert_ne!(
            tune_key("http://a", TransferKind::Upload),
            tune_key("http://a", TransferKind::Download)
        );

        assert_eq!(origin_of("http://Host:8080/x?y#z"), "http://host:8080");
        assert_eq!(origin_of("HTTPS://h/p"), "https://h");
        assert_eq!(origin_of("http://h"), "http://h");
        // Anything unusable → empty (caching off).
        assert_eq!(origin_of(""), "");
        assert_eq!(origin_of("/relative/path"), "");
        assert_eq!(origin_of("ftp://h/x"), "");
        assert_eq!(origin_of("http://"), "");
    }

    #[test]
    fn failed_transfer_forgets_the_settled_state() {
        let mut e = engine();
        ramp_to_settle(&mut e);
        e.transfer_end(false);
        // Parameters that just failed must not be reused blindly.
        let p = e.begin_transfer(&caps(), 60_000.0, &static_params());
        assert_eq!(e.phase(), TunePhase::Ramping);
        assert_eq!(p.upload_window, 1, "a failed transfer re-ramps from the mins");
    }

    #[test]
    fn changed_caps_invalidate_the_settled_state() {
        let mut e = engine();
        ramp_to_settle(&mut e);
        e.transfer_end(true);
        // A server that changed its advertisement invalidates the settle.
        let mut changed = caps();
        changed.limits.concurrency.default = 6;
        let p = e.begin_transfer(&changed, 60_000.0, &static_params());
        assert_eq!(e.phase(), TunePhase::Ramping);
        assert_eq!(p.upload_window, 1);
    }

    #[test]
    fn reused_settle_does_not_re_ramp_on_later_windows() {
        let mut e = engine();
        ramp_to_settle(&mut e);
        let tuned = e.params().clone();
        e.transfer_end(true);
        e.begin_transfer(&caps(), 60_000.0, &static_params());
        // Several windows of a faster link must not restart the ramp: reuse is
        // a settled state, not a new exploration. (The chunk size may still
        // follow the measured link — it is sized, not ramped.)
        let mut t = 61_000.0;
        for i in 1..=3u64 {
            let ev = e.tick(t, i * 100_000_000, Some(400.0), false).unwrap();
            assert_eq!(ev.phase, TunePhase::Settled);
            assert_eq!(ev.params.upload_window, tuned.upload_window);
            assert_eq!(ev.params.concurrency, tuned.concurrency);
            t += 1_000.0;
        }
    }

    #[test]
    fn transfer_end_without_settle_keeps_ramping_next_time() {
        let mut e = engine();
        e.tick(1_000.0, 12_500_000, Some(400.0), false);
        e.transfer_end(true);
        // One window is not a convergence: the next transfer re-ramps.
        let p = e.begin_transfer(&caps(), 60_000.0, &static_params());
        assert_eq!(e.phase(), TunePhase::Ramping);
        assert_eq!(p.upload_window, 1);
    }

    #[test]
    fn upload_auto_level_benchmarks_once() {
        let mut e = TuningEngine::new(true, CompressLevel::Auto);
        e.begin_transfer(&caps(), 0.0, &static_params());
        let sample = vec![0xABu8; 256 * 1024];
        let lvl1 = e.upload_compress_level(&caps(), Some(&sample), 50.0);
        let lvl2 = e.upload_compress_level(&caps(), Some(&sample), 50.0);
        assert_eq!(lvl1, lvl2, "benchmark runs once, result cached");
        assert!(e.auto_level_ready());
        assert!((-8..=4).contains(&lvl1), "level inside advertised range, got {lvl1}");
        // Incompressible sample → cheapest level.
        let mut e2 = TuningEngine::new(true, CompressLevel::Auto);
        e2.begin_transfer(&caps(), 0.0, &static_params());
        let lvl = e2.upload_compress_level(&caps(), Some(&[0u8; 0]), 50.0);
        assert_eq!(lvl, caps().compression.zrip_levels.default, "no sample → server default");
    }

    #[test]
    fn disabled_engine_uses_static_params() {
        let mut e = TuningEngine::new(false, CompressLevel::Balanced);
        e.set_direction(TransferKind::Upload);
        let p = e.begin_transfer(&caps(), 0.0, &static_params());
        assert_eq!(p.concurrency, 4, "static config preserved");
        assert_eq!(p.upload_window, 8);
        assert_eq!(e.phase(), TunePhase::Uninitialized);
        // Ticks are no-ops.
        assert!(e.tick(1_000.0, 12_500_000, Some(40.0), false).is_none());
        assert_eq!(p, static_params().clamped_into(&caps()));
    }
}