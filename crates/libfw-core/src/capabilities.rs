//! Server capability advertisement (`GET /capabilities` payload).
//!
//! The server *declares* what it supports (compression formats & levels,
//! tuning-parameter ranges); the **client decides** the concrete values
//! inside those ranges. This module holds the serde shapes, the libfw
//! defaults and a stable capability hash so clients can detect when a
//! server's advertisement changed under them (e.g. for cache invalidation).
//!
//! # Stability of `caps_hash`
//!
//! [`Capabilities::caps_hash`] hashes the canonical JSON encoding of the
//! struct (serde_json preserves declared field order), so equal values
//! always produce equal hashes across processes and versions.

use crate::compress::{CompressionFormat, ZRIP_DEFAULT_LEVEL, ZRIP_MAX_LEVEL, ZRIP_MIN_LEVEL};
use crate::constants::{
    DEFAULT_CONCURRENCY, DEFAULT_DOWNLOAD_CHUNK_SIZE, DEFAULT_DOWNLOAD_WINDOW,
    DEFAULT_MAX_UPLOAD_SIZE, DEFAULT_UPLOAD_WINDOW, MAX_RETRIES, protocol_header_value,
};
use sha2::{Digest, Sha256};

/// Inclusive integer range with a server-chosen default.
///
/// `min`/`max` are hard bounds the server can enforce; `default` is what a
/// client gets by not asking at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct IntRange {
    pub min: i64,
    pub max: i64,
    pub default: i64,
}

impl IntRange {
    /// Clamp `v` into `min..=max`.
    pub fn clamp(&self, v: i64) -> i64 {
        v.clamp(self.min, self.max)
    }

    /// Whether `v` lies inside `min..=max`.
    pub fn contains(&self, v: i64) -> bool {
        (self.min..=self.max).contains(&v)
    }
}

/// Zrip (zstd) level range advertised by the server.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ZripLevels {
    pub min: i32,
    pub max: i32,
    pub default: i32,
}

impl Default for ZripLevels {
    fn default() -> Self {
        ZripLevels {
            min: ZRIP_MIN_LEVEL,
            max: ZRIP_MAX_LEVEL,
            default: ZRIP_DEFAULT_LEVEL,
        }
    }
}

impl ZripLevels {
    /// Clamp `level` into `min..=max` (falls back to `default` when the
    /// server advertises an empty range).
    pub fn clamp_level(&self, level: i32) -> i32 {
        if self.min <= self.max {
            level.clamp(self.min, self.max)
        } else {
            self.default
        }
    }

    /// Whether `level` is inside the advertised range.
    pub fn contains(&self, level: i32) -> bool {
        (self.min..=self.max).contains(&level)
    }
}

/// Compression capabilities: formats served and zrip level range.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CompressionCaps {
    /// Formats in preference order; `identity` is always implicitly allowed.
    pub formats: Vec<CompressionFormat>,
    /// Advertised zrip level range (meaningless when `zrip` ∉ `formats`).
    #[serde(rename = "zripLevels")]
    pub zrip_levels: ZripLevels,
}

impl Default for CompressionCaps {
    fn default() -> Self {
        CompressionCaps {
            formats: vec![CompressionFormat::None, CompressionFormat::Zrip],
            zrip_levels: ZripLevels::default(),
        }
    }
}

impl CompressionCaps {
    /// Whether `format` is advertised.
    pub fn supports(&self, format: CompressionFormat) -> bool {
        self.formats.contains(&format)
    }
}

/// Tuning-parameter ranges the server will tolerate.
///
/// `maxUploadSize` is a plain byte cap (no tuning dimension); everything
/// else is an [`IntRange`]. All defaults mirror `libfw-core` constants so a
/// server that never overrides anything still advertises sane values.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Limits {
    /// Cross-file fan-out / global in-flight request cap.
    pub concurrency: IntRange,
    /// Per-file upload in-flight chunk window.
    pub upload_window: IntRange,
    /// Per-file download in-flight chunk window.
    pub download_window: IntRange,
    /// Upload chunk size (bytes).
    pub chunk_size: IntRange,
    /// Download (byte-range) chunk size (bytes).
    pub download_chunk_size: IntRange,
    /// Hard cap on a single upload body (bytes).
    pub max_upload_size: u64,
    /// Per-chunk retry budget.
    pub max_retries: IntRange,
    /// Request timeout the server will honor (ms).
    pub timeout_ms: IntRange,
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            concurrency: IntRange {
                min: 1,
                max: 16,
                default: DEFAULT_CONCURRENCY as i64,
            },
            upload_window: IntRange {
                min: 1,
                max: 8,
                default: DEFAULT_UPLOAD_WINDOW as i64,
            },
            download_window: IntRange {
                min: 1,
                max: 8,
                default: DEFAULT_DOWNLOAD_WINDOW as i64,
            },
            chunk_size: IntRange {
                min: 256 * 1024,
                max: 8 * 1024 * 1024,
                default: crate::CHUNK_SIZE as i64,
            },
            download_chunk_size: IntRange {
                min: 64 * 1024,
                max: 4 * 1024 * 1024,
                default: DEFAULT_DOWNLOAD_CHUNK_SIZE as i64,
            },
            max_upload_size: DEFAULT_MAX_UPLOAD_SIZE,
            max_retries: IntRange {
                min: 1,
                max: 10,
                default: MAX_RETRIES as i64,
            },
            timeout_ms: IntRange {
                min: 30_000,
                max: 1_800_000,
                default: 600_000,
            },
        }
    }
}

/// Full capability advertisement served at `GET /capabilities`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Capabilities {
    /// Wire protocol name/version (e.g. `libfw/1`).
    pub protocol: String,
    pub compression: CompressionCaps,
    pub limits: Limits,
}

impl Default for Capabilities {
    fn default() -> Self {
        Capabilities {
            protocol: protocol_header_value().to_string(),
            compression: CompressionCaps::default(),
            limits: Limits::default(),
        }
    }
}

impl Capabilities {
    /// Stable, content-addressed hash of this advertisement.
    ///
    /// Format: `sha256-<64 hex chars>`. Used by clients to invalidate cached
    /// tuning state when the server changes what it advertises.
    pub fn caps_hash(&self) -> String {
        // serde_json::to_vec cannot fail for these plain data types.
        let bytes = serde_json::to_vec(self).expect("capabilities serialization cannot fail");
        let digest = Sha256::digest(&bytes);
        format!("sha256-{}", hex(&digest))
    }

    /// Clamp a zrip level into the advertised range.
    pub fn clamp_level(&self, level: i32) -> i32 {
        self.compression.zrip_levels.clamp_level(level)
    }

    /// Whether `level` is inside the advertised range.
    pub fn is_valid_level(&self, level: i32) -> bool {
        self.compression.zrip_levels.contains(level)
    }
}

/// Lowercase hex encoding of a digest (avoids pulling `hex` for one format).
fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compress::{compressor_with_level, is_valid_zrip_level, negotiate_level};

    #[test]
    fn default_caps_serde_roundtrip() {
        let caps = Capabilities::default();
        let json = serde_json::to_string(&caps).unwrap();
        let back: Capabilities = serde_json::from_str(&json).unwrap();
        assert_eq!(back, caps);
        // Spot-check the wire shape a server will serve.
        assert!(json.contains("\"protocol\":\"libfw/1\""));
        assert!(json.contains("\"zripLevels\""));
        assert!(json.contains("\"uploadWindow\""));
        assert!(json.contains("\"maxUploadSize\""));
        assert!(json.contains("\"identity\""));
    }

    #[test]
    fn caps_hash_is_stable_and_content_addressed() {
        let a = Capabilities::default();
        let b = Capabilities::default();
        assert_eq!(a.caps_hash(), b.caps_hash());
        assert!(a.caps_hash().starts_with("sha256-"));
        assert_eq!(a.caps_hash().len(), "sha256-".len() + 64);

        let mut changed = a.clone();
        changed.limits.concurrency.default = 6;
        assert_ne!(a.caps_hash(), changed.caps_hash());

        let mut changed2 = a.clone();
        changed2.compression.zrip_levels.max = 3;
        assert_ne!(a.caps_hash(), changed2.caps_hash());
    }

    #[test]
    fn negotiate_level_clamps_and_defaults() {
        // None → server default.
        assert_eq!(negotiate_level(None, -8, 4, 1), 1);
        // In-range passes through untouched.
        assert_eq!(negotiate_level(Some(3), -8, 4, 1), 3);
        // Out-of-range clamps.
        assert_eq!(negotiate_level(Some(-20), -8, 4, 1), -8);
        assert_eq!(negotiate_level(Some(99), -8, 4, 1), 4);
    }

    #[test]
    fn zrip_level_bounds() {
        assert!(is_valid_zrip_level(ZRIP_MIN_LEVEL));
        assert!(is_valid_zrip_level(ZRIP_MAX_LEVEL));
        assert!(is_valid_zrip_level(0));
        assert!(!is_valid_zrip_level(ZRIP_MIN_LEVEL - 1));
        assert!(!is_valid_zrip_level(ZRIP_MAX_LEVEL + 1));
        let lv = ZripLevels::default();
        assert_eq!(lv.clamp_level(99), ZRIP_MAX_LEVEL);
        assert_eq!(lv.clamp_level(-99), ZRIP_MIN_LEVEL);
        assert_eq!(lv.clamp_level(2), 2);
    }

    #[test]
    fn limits_clamp_and_contains() {
        let r = IntRange { min: 1, max: 16, default: 4 };
        assert_eq!(r.clamp(0), 1);
        assert_eq!(r.clamp(100), 16);
        assert_eq!(r.clamp(8), 8);
        assert!(r.contains(16));
        assert!(!r.contains(17));
    }

    #[test]
    fn compression_caps_supports() {
        let caps = CompressionCaps::default();
        assert!(caps.supports(CompressionFormat::Zrip));
        assert!(caps.supports(CompressionFormat::None));

        let identity_only = CompressionCaps {
            formats: vec![CompressionFormat::None],
            zrip_levels: ZripLevels::default(),
        };
        assert!(identity_only.supports(CompressionFormat::None));
        assert!(!identity_only.supports(CompressionFormat::Zrip));
    }

    #[test]
    fn compressor_with_level_roundtrip_across_range() {
        let data: Vec<u8> = (0..200_000u32).map(|i| (i % 97) as u8).collect();
        for level in [ZRIP_MIN_LEVEL, -1, 0, ZRIP_DEFAULT_LEVEL, 4] {
            let mut c = compressor_with_level(CompressionFormat::Zrip, level).unwrap();
            let mut compressed = Vec::new();
            c.compress(&data, &mut compressed).unwrap();
            c.finish(&mut compressed).unwrap();
            assert!(!compressed.is_empty(), "level {level} produced no output");

            let mut d = crate::compress::decompressor(CompressionFormat::Zrip);
            let mut plain = Vec::new();
            d.decompress(&compressed, &mut plain).unwrap();
            d.finish(&mut plain).unwrap();
            assert_eq!(plain, data, "roundtrip mismatch at level {level}");
        }
    }

    #[test]
    fn compressor_with_level_identity_ignores_level() {
        let mut c = compressor_with_level(CompressionFormat::None, 99).unwrap();
        let mut compressed = Vec::new();
        c.compress(b"passthrough", &mut compressed).unwrap();
        c.finish(&mut compressed).unwrap();
        assert_eq!(compressed, b"passthrough");
    }
}