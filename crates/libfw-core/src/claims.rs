//! Bearer-token claims and permissions.
//!
//! `libfw` never *issues* tokens; it only parses and validates them. A token
//! verifier (JWT library, external validation service, …) is expected to
//! produce a [`TokenClaims`] which is then checked against the requested
//! path and [`Permission`] by a [`Validator`](crate::auth::Validator).

use serde::{Deserialize, Serialize};

/// Permission dimension of a token.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Permission {
    /// May download / read files.
    Read,
    /// May upload / write files.
    Write,
}

impl std::fmt::Display for Permission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Permission::Read => write!(f, "read"),
            Permission::Write => write!(f, "write"),
        }
    }
}

/// Parsed, verified token payload used for fine-grained authorization.
///
/// All fields except `sub` are optional so that minimal tokens remain
/// usable (validation is performed by the [`Validator`](crate::auth::Validator)).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenClaims {
    /// Subject — the user or client the token belongs to.
    pub sub: String,
    /// Unix timestamp (seconds) at which the token expires.
    #[serde(default)]
    pub exp: Option<i64>,
    /// Permissions granted to this token. Empty means "no permissions".
    #[serde(default)]
    pub permissions: Vec<Permission>,
    /// Path prefixes the token may access. Empty means "no paths allowed".
    #[serde(default)]
    pub allowed_paths: Vec<String>,
}

impl TokenClaims {
    /// Returns true if `now` (unix seconds) is past `exp`.
    ///
    /// Per RFC 7519 §4.1.4 the token is valid while `now < exp`; a token
    /// whose `exp` equals the current second is already expired.  The
    /// previous `now >= exp` was correct — this comment preserves the
    /// intent explicitly.
    pub fn is_expired(&self, now: i64) -> bool {
        self.exp.is_some_and(|exp| now >= exp)
    }

    /// Returns true if this token grants `perm`.
    pub fn has_permission(&self, perm: Permission) -> bool {
        self.permissions.contains(&perm)
    }
}
