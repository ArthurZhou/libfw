//! Path translation: storage ("real") paths vs. client-visible shadow paths.
//!
//! Some deployments do not want real storage layout (directory names,
//! hierarchy, naming habits) to reach the client. A [`PathCodec`] converts
//! between the two sides:
//!
//! - [`encode`](PathCodec::encode): real → shadow (outbound; response
//!   bodies, listings, upload echoes).
//! - [`decode`](PathCodec::decode): shadow → real (inbound; request URLs).
//!
//! Codecs operate on **canonical relative paths** (what
//! [`validate_rel_path`](crate::validate_rel_path) produces): no leading
//! `/`, no `.`/`..`, no empty segments. The root is `""`.
//!
//! Authorization is unaffected by translation: the server validates the
//! *real* path against `allowed_paths` (see the `resolve_client_path`
//! helper in `libfw-server`), so token semantics stay exactly as before.
//!
//! # Codecs
//!
//! | Codec | Shadow looks like | Use when |
//! |---|---|---|
//! | [`IdentityPathCodec`] | the real path | default; no translation |
//! | [`EncryptedPathCodec`] | `v1.<base64url>` opaque blob | real paths must be hidden completely |
//! | [`MountPathCodec`] | a readable alias (`home/alice/…`) | readable, stable shadow namespaces |

#[cfg(feature = "path-encrypt")]
use std::sync::Arc;

/// Error decoding a client-supplied shadow path back to a real path.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PathCodecError {
    /// The shadow did not carry a recognized format version.
    #[error("unknown shadow path version `{0}`")]
    UnknownVersion(String),
    /// The shadow was structurally invalid (bad encoding, bad key data).
    #[error("invalid shadow path: {0}")]
    InvalidShadow(String),
    /// The shadow does not belong to any configured mapping.
    #[error("shadow path does not match any configured mapping: `{0}`")]
    Unmapped(String),
}

/// Converts between real storage paths and client-visible shadow paths.
///
/// [`encode`](PathCodec::encode) is total: every real path *must* be
/// representable (an encrypting codec can always encrypt; a mapping codec
/// falls back to identity for unmapped subtrees so listings never fail).
/// [`decode`](PathCodec::decode) fails when the shadow is invalid,
/// tampered with, or unmapped — the caller turns that into a `400`.
pub trait PathCodec: Send + Sync + 'static {
    /// Real path → shadow path.
    fn encode(&self, real: &str) -> String;
    /// Shadow path → real path.
    fn decode(&self, shadow: &str) -> Result<String, PathCodecError>;
}

/// The default codec: shadow paths *are* real paths. No translation.
#[derive(Debug, Clone, Copy, Default)]
pub struct IdentityPathCodec;

impl PathCodec for IdentityPathCodec {
    fn encode(&self, real: &str) -> String {
        real.to_string()
    }
    fn decode(&self, shadow: &str) -> Result<String, PathCodecError> {
        Ok(shadow.to_string())
    }
}

/// Prefix-mapping codec: readable shadow namespaces backed by real subtrees.
///
/// Each entry maps a shadow prefix to a real prefix (canonical relative
/// form, no leading/trailing `/`, root is `""`):
///
/// ```
/// use libfw_core::pathmap::{MountPathCodec, PathCodec};
///
/// let codec = MountPathCodec::new(vec![
///     ("home/alice".to_string(), "data/vol-3/alice".to_string()),
/// ]);
///
/// // real → shadow: the real subtree is aliased under the shadow prefix
/// assert_eq!(
///     codec.encode("data/vol-3/alice/report.pdf"),
///     "home/alice/report.pdf"
/// );
/// // shadow → real
/// assert_eq!(
///     codec.decode("home/alice/report.pdf").unwrap(),
///     "data/vol-3/alice/report.pdf"
/// );
/// // a shadow outside every mapping is rejected …
/// assert!(codec.decode("other/thing.txt").is_err());
/// // … while a real path outside every mapping passes through unchanged
/// assert_eq!(codec.encode("public/readme.md"), "public/readme.md");
/// ```
///
/// Matching is on segment boundaries: `home/alice` matches `home/alice/x`
/// but not `home/alice2/x`. When several entries match, the longest shadow
/// prefix wins (most specific mapping).
#[derive(Debug, Clone, Default)]
pub struct MountPathCodec {
    /// `(shadow_prefix, real_prefix)`, ordered most-specific first.
    entries: Vec<(String, String)>,
}

impl MountPathCodec {
    /// Build a codec from `(shadow_prefix, real_prefix)` pairs.
    ///
    /// Prefixes are normalized (leading/trailing `/` and empty segments
    /// stripped). A fully empty pair (root↔root) is ignored — use
    /// `IdentityPathCodec` for that.
    pub fn new(entries: Vec<(String, String)>) -> Self {
        let norm: Vec<(String, String)> = entries
            .into_iter()
            .filter_map(|(shadow, real)| {
                let s = normalize_prefix(&shadow);
                let r = normalize_prefix(&real);
                if s.is_empty() && r.is_empty() {
                    return None;
                }
                Some((s, r))
            })
            .collect();
        MountPathCodec { entries: norm }
    }
}

impl PathCodec for MountPathCodec {
    fn encode(&self, real: &str) -> String {
        match self.encode_match(real) {
            Some((shadow_root, rest)) => join_prefix(shadow_root, rest),
            None => real.to_string(),
        }
    }

    fn decode(&self, shadow: &str) -> Result<String, PathCodecError> {
        match self.decode_match(shadow) {
            Some((real_root, rest)) => Ok(join_prefix(real_root, rest)),
            None => Err(PathCodecError::Unmapped(shadow.to_string())),
        }
    }
}

impl MountPathCodec {
    /// Longest entry whose **real** prefix matches `real` (encode
    /// direction): returns the shadow root + the remainder.
    fn encode_match<'a>(&'a self, real: &'a str) -> Option<(&'a str, &'a str)> {
        self.entries
            .iter()
            .filter(|(_, r)| segment_prefix(real, r.as_str()))
            .max_by_key(|(_, r)| r.len())
            .map(|(s, r)| (s.as_str(), real[r.len()..].trim_start_matches('/')))
    }

    /// Longest entry whose **shadow** prefix matches `shadow` (decode
    /// direction): returns the real root + the remainder.
    fn decode_match<'a>(&'a self, shadow: &'a str) -> Option<(&'a str, &'a str)> {
        self.entries
            .iter()
            .filter(|(s, _)| segment_prefix(shadow, s.as_str()))
            .max_by_key(|(s, _)| s.len())
            .map(|(s, r)| (r.as_str(), shadow[s.len()..].trim_start_matches('/')))
    }
}

fn normalize_prefix(p: &str) -> String {
    let mut out = String::new();
    for seg in p.split('/') {
        match seg {
            "" | "." => {}
            ".." => continue, // defensive: drop escapes rather than error
            seg => {
                if !out.is_empty() {
                    out.push('/');
                }
                out.push_str(seg);
            }
        }
    }
    out
}

/// Does `prefix` match `path` on a segment boundary? The empty prefix
/// matches everything.
fn segment_prefix(path: &str, prefix: &str) -> bool {
    if prefix.is_empty() {
        return true;
    }
    if path == prefix {
        return true;
    }
    path.starts_with(prefix) && path.as_bytes().get(prefix.len()) == Some(&b'/')
}

fn join_prefix(root: &str, rest: &str) -> String {
    if root.is_empty() {
        return rest.to_string();
    }
    if rest.is_empty() {
        return root.to_string();
    }
    format!("{root}/{rest}")
}

/// Encrypting codec: shadow paths are opaque `v1.<base64url>` blobs.
///
/// Real paths are AES-256-GCM encrypted with a server-held key; the
/// ciphertext carries a random 96-bit nonce and a 128-bit auth tag, so the
/// shadow is unreadable **and** unforgeable (tampering fails the tag check
/// at decode time). Encrypting the same path twice yields different
/// shadows: a listing leaks nothing, not even equality of paths.
///
/// Shadow format: `v1.` + base64url(no-pad) of `nonce(12) ‖ ciphertext+tag`.
///
/// ```
/// use libfw_core::pathmap::{EncryptedPathCodec, PathCodec};
///
/// let codec = EncryptedPathCodec::from_hex(
///     "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
/// ).unwrap();
///
/// let shadow = codec.encode("docs/机密/plan.pdf");
/// assert_ne!(shadow, "docs/机密/plan.pdf");
/// assert_eq!(codec.decode(&shadow).unwrap(), "docs/机密/plan.pdf");
/// // the same path encrypts differently every time
/// assert_ne!(codec.encode("docs/机密/plan.pdf"), shadow);
/// // tampered shadows are rejected
/// let mut bad = shadow.into_bytes();
/// let n = bad.len();
/// bad[n - 1] = if bad[n - 1] == b'A' { b'B' } else { b'A' };
/// assert!(codec.decode(&String::from_utf8(bad).unwrap()).is_err());
/// ```
#[cfg(feature = "path-encrypt")]
#[derive(Clone)]
pub struct EncryptedPathCodec {
    cipher: Arc<aes_gcm::Aes256Gcm>,
}

#[cfg(feature = "path-encrypt")]
impl std::fmt::Debug for EncryptedPathCodec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncryptedPathCodec")
            .field("cipher", &"<aes-256-gcm key hidden>")
            .finish()
    }
}

#[cfg(feature = "path-encrypt")]
const SHADOW_VERSION: &str = "v1";
#[cfg(feature = "path-encrypt")]
const NONCE_LEN: usize = 12;

#[cfg(feature = "path-encrypt")]
impl EncryptedPathCodec {
    /// Build the codec from a 32-byte key.
    pub fn new(key: [u8; 32]) -> Self {
        use aes_gcm::KeyInit;
        EncryptedPathCodec {
            cipher: Arc::new(aes_gcm::Aes256Gcm::new((&key).into())),
        }
    }

    /// Build the codec from a 64-char hex key (32 bytes).
    pub fn from_hex(hex: &str) -> Result<Self, PathCodecError> {
        let key = decode_hex(hex).ok_or_else(|| {
            PathCodecError::InvalidShadow("LIBFW_PATH_KEY must be 64 hex chars (32 bytes)".into())
        })?;
        Ok(Self::new(key))
    }
}

#[cfg(feature = "path-encrypt")]
impl PathCodec for EncryptedPathCodec {
    fn encode(&self, real: &str) -> String {
        use aes_gcm::aead::{Aead, Payload};
        use base64::Engine;
        let mut nonce = [0u8; NONCE_LEN];
        getrandom::getrandom(&mut nonce).expect("OS RNG");
        let ct = self
            .cipher
            .encrypt(
                (&nonce).into(),
                Payload {
                    msg: real.as_bytes(),
                    aad: b"libfw-path-v1",
                },
            )
            .expect("AES-256-GCM encryption cannot fail");
        let mut blob = Vec::with_capacity(NONCE_LEN + ct.len());
        blob.extend_from_slice(&nonce);
        blob.extend_from_slice(&ct);
        format!(
            "{SHADOW_VERSION}.{}",
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(blob)
        )
    }

    fn decode(&self, shadow: &str) -> Result<String, PathCodecError> {
        use aes_gcm::aead::{Aead, Payload};
        use base64::Engine;
        let (version, payload) = shadow.split_once('.').ok_or_else(|| {
            PathCodecError::InvalidShadow("missing version prefix".into())
        })?;
        if version != SHADOW_VERSION {
            return Err(PathCodecError::UnknownVersion(version.to_string()));
        }
        let blob = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(payload)
            .map_err(|_| PathCodecError::InvalidShadow("bad base64".into()))?;
        if blob.len() < NONCE_LEN + 16 {
            return Err(PathCodecError::InvalidShadow("too short".into()));
        }
        let (nonce, ct) = blob.split_at(NONCE_LEN);
        let plain = self.cipher.decrypt(
            nonce.into(),
            Payload {
                msg: ct,
                aad: b"libfw-path-v1",
            },
        ).map_err(|_| PathCodecError::InvalidShadow("authentication failed (tampered?)".into()))?;
        String::from_utf8(plain)
            .map_err(|_| PathCodecError::InvalidShadow("not valid UTF-8".into()))
    }
}

#[cfg(feature = "path-encrypt")]
fn decode_hex(hex: &str) -> Option<[u8; 32]> {
    let hex = hex.trim();
    if hex.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(out)
}

#[cfg(all(test, feature = "path-encrypt"))]
mod encrypt_tests {
    use super::*;

    const KEY_HEX: &str =
        "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";

    #[test]
    fn round_trip_various_paths() {
        let codec = EncryptedPathCodec::from_hex(KEY_HEX).unwrap();
        for real in [
            "",
            "a.txt",
            "docs/plan.pdf",
            "docs/机密/plan.pdf",
            "deeply/nested/dir/file with spaces.txt",
            "a/../b", // handled upstream by validate_rel_path; still round-trips
        ] {
            let shadow = codec.encode(real);
            assert!(shadow.starts_with("v1."), "{shadow}");
            assert_eq!(codec.decode(&shadow).unwrap(), real);
        }
    }

    #[test]
    fn shadows_are_opaque_and_unique() {
        let codec = EncryptedPathCodec::from_hex(KEY_HEX).unwrap();
        let s1 = codec.encode("docs/a.txt");
        let s2 = codec.encode("docs/a.txt");
        assert_ne!(s1, s2, "random nonce ⇒ distinct shadows");
        assert!(!s1.contains("docs"), "shadow must not leak the path: {s1}");
    }

    #[test]
    fn tampering_is_rejected() {
        let codec = EncryptedPathCodec::from_hex(KEY_HEX).unwrap();
        let shadow = codec.encode("secret/plan.txt");
        let mut bytes = shadow.into_bytes();
        let n = bytes.len();
        // Flip a byte well before the end: base64 trailing chars can contain
        // unused low bits (when byte count ≢ 0 mod 3), so flipping the very
        // last char may be a no-op in the decoded byte stream. Using a
        // position near the middle of the base64 payload guarantees we hit
        // a fully-significant byte.
        let mid = n / 2;
        bytes[mid] = if bytes[mid] == b'A' { b'B' } else { b'A' };
        let bad = String::from_utf8(bytes).unwrap();
        assert_eq!(
            codec.decode(&bad),
            Err(PathCodecError::InvalidShadow(
                "authentication failed (tampered?)".into()
            ))
        );
    }

    #[test]
    fn wrong_key_rejected() {
        let good = EncryptedPathCodec::from_hex(KEY_HEX).unwrap();
        let bad_key = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
        let evil = EncryptedPathCodec::from_hex(bad_key).unwrap();
        let shadow = good.encode("docs/a.txt");
        assert!(evil.decode(&shadow).is_err());
    }

    #[test]
    fn bad_version_and_base64() {
        let codec = EncryptedPathCodec::from_hex(KEY_HEX).unwrap();
        assert!(matches!(
            codec.decode("v0.abc"),
            Err(PathCodecError::UnknownVersion(_))
        ));
        assert!(matches!(
            codec.decode("v1.!!!not-base64!!!"),
            Err(PathCodecError::InvalidShadow(_))
        ));
    }

    #[test]
    fn hex_key_validation() {
        assert!(EncryptedPathCodec::from_hex(KEY_HEX).is_ok());
        assert!(EncryptedPathCodec::from_hex("zz").is_err());
        assert!(EncryptedPathCodec::from_hex(&KEY_HEX[..62]).is_err());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_passthrough() {
        let codec = IdentityPathCodec;
        assert_eq!(codec.encode("docs/a.txt"), "docs/a.txt");
        assert_eq!(codec.decode("docs/a.txt").unwrap(), "docs/a.txt");
    }

    #[test]
    fn mount_round_trip_and_boundaries() {
        let codec = MountPathCodec::new(vec![
            ("home/alice".into(), "data/vol-3/alice".into()),
            ("pub".into(), "public".into()),
        ]);
        assert_eq!(
            codec.encode("data/vol-3/alice/report.pdf"),
            "home/alice/report.pdf"
        );
        assert_eq!(
            codec.decode("home/alice/report.pdf").unwrap(),
            "data/vol-3/alice/report.pdf"
        );
        // Segment boundary: `home/alice` must not match `home/alice2`.
        assert!(codec.decode("home/alice2/x.txt").is_err());
        // Unmapped shadow rejected …
        assert_eq!(
            codec.decode("other/thing.txt"),
            Err(PathCodecError::Unmapped("other/thing.txt".into()))
        );
        // … unmapped real passes through (listings never fail).
        assert_eq!(codec.encode("public/readme.md"), "pub/readme.md");
        assert_eq!(codec.encode("unmapped/x.txt"), "unmapped/x.txt");
        // Root alias.
        assert_eq!(codec.encode("public"), "pub");
        assert_eq!(codec.decode("pub").unwrap(), "public");
        assert_eq!(codec.encode(""), "");
    }

    #[test]
    fn mount_most_specific_wins() {
        let codec = MountPathCodec::new(vec![
            ("t".into(), "top".into()),
            ("t/sub".into(), "top/deep".into()),
        ]);
        assert_eq!(codec.encode("top/deep/x"), "t/sub/x");
        assert_eq!(codec.decode("t/sub/x").unwrap(), "top/deep/x");
        assert_eq!(codec.decode("t/other").unwrap(), "top/other");
    }

    #[test]
    fn mount_normalizes_prefixes() {
        let codec = MountPathCodec::new(vec![
            ("/home/alice/".into(), "/data/vol/".into()),
        ]);
        assert_eq!(codec.encode("data/vol/x.txt"), "home/alice/x.txt");
        assert_eq!(codec.decode("home/alice/x.txt").unwrap(), "data/vol/x.txt");
    }
}