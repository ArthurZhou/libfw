//! Authorization contracts: actions, validator trait and path rules.
//!
//! The server-side flow is: extract `Authorization: Bearer <token>` →
//! verify it (via a [`TokenVerifier`] of your choice) into
//! [`TokenClaims`](crate::claims::TokenClaims) → ask a [`Validator`]
//! whether the claims allow the requested `path` + [`Action`].

use crate::claims::{Permission, TokenClaims};

/// The operation a client is trying to perform.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    /// Download / read a resource.
    Read,
    /// Upload / write a resource.
    Write,
}

impl Action {
    /// The [`Permission`] required to perform this action.
    pub fn required_permission(self) -> Permission {
        match self {
            Action::Read => Permission::Read,
            Action::Write => Permission::Write,
        }
    }
}

impl std::fmt::Display for Action {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Action::Read => write!(f, "read"),
            Action::Write => write!(f, "write"),
        }
    }
}

/// Errors raised during authorization.
///
/// Servers map these to HTTP status codes: [`AuthError::MissingToken`] and
/// [`AuthError::Expired`] → `401 Unauthorized`; [`AuthError::Forbidden`] →
/// `403 Forbidden`.
#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    /// The `Authorization` header is absent or malformed.
    #[error("missing or malformed bearer token")]
    MissingToken,
    /// The token payload could not be verified (bad signature, unknown issuer, …).
    #[error("invalid token: {0}")]
    Invalid(String),
    /// The token has expired.
    #[error("token expired")]
    Expired,
    /// The claims do not permit this path / action.
    #[error("permission denied: {action} on `{path}`")]
    Forbidden {
        /// The requested path.
        path: String,
        /// The requested action.
        action: Action,
    },
}

/// Turns a raw bearer token string into validated claims.
///
/// Implementations are expected to hold the verification key (or call out
/// to an external validation service). `libfw` deliberately does **not**
/// ship a JWT implementation here so the choice stays framework-agnostic.
pub trait TokenVerifier: Send + Sync + 'static {
    /// Verify `token` and return its claims.
    ///
    /// Should return [`AuthError::MissingToken`] for empty input,
    /// [`AuthError::Invalid`] for unverifiable tokens and
    /// [`AuthError::Expired`] for expired ones.
    fn verify(&self, token: &str) -> Result<TokenClaims, AuthError>;
}

/// Decides whether `claims` are allowed to perform `action` on `path`.
pub trait Validator: Send + Sync + 'static {
    /// Validate a request. Returns `Ok(())` or a descriptive
    /// [`AuthError`] to be turned into `401`/`403` by the server.
    fn validate(
        &self,
        claims: &TokenClaims,
        path: &str,
        action: Action,
    ) -> Result<(), AuthError>;
}

/// The default validator: permission check + path-prefix check.
///
/// A request is allowed when:
///
/// 1. the token is not expired ([`TokenClaims::exp`]), and
/// 2. it carries the [`Permission`] required by `action`, and
/// 3. the requested path starts with one of `allowed_paths`.
///
/// Paths are compared on a segment boundary: `allowed_paths = ["/docs"]`
/// matches `/docs`, `/docs/a.txt` and `/docs/` but **not** `/docshop/x`.
/// The root prefix `"/"` (or `""`) grants access to the whole tree. An
/// empty `allowed_paths` list denies everything.
#[derive(Debug, Clone, Default)]
pub struct PathValidator {
    /// When true, `allowed_paths` are treated as raw string prefixes
    /// (no segment-boundary normalization). Default: false.
    pub raw_prefix_match: bool,
}

impl PathValidator {
    /// Creates a validator with segment-boundary path matching.
    pub fn new() -> Self {
        PathValidator::default()
    }
}

impl Validator for PathValidator {
    fn validate(
        &self,
        claims: &TokenClaims,
        path: &str,
        action: Action,
    ) -> Result<(), AuthError> {
        if claims.is_expired(now_epoch_seconds()) {
            return Err(AuthError::Expired);
        }
        if !claims.has_permission(action.required_permission()) {
            return Err(AuthError::Forbidden {
                path: path.to_string(),
                action,
            });
        }
        let allowed = if self.raw_prefix_match {
            claims
                .allowed_paths
                .iter()
                .any(|prefix| path.starts_with(prefix.as_str()))
        } else {
            path_matches_any(path, &claims.allowed_paths)
        };
        if !allowed {
            return Err(AuthError::Forbidden {
                path: path.to_string(),
                action,
            });
        }
        Ok(())
    }
}

/// Segment-boundary prefix match for a path against a list of prefixes.
///
/// Normalizes a leading `/` so `/docs`, `docs` and `docs/` are equivalent.
/// A root prefix (`""`, `"/"`, `"/"`-like) matches **everything**, which is
/// how `allowed_paths: ["/"]` is conventionally used to grant full access.
fn path_matches_any(path: &str, prefixes: &[String]) -> bool {
    let p = path.trim_start_matches('/');
    prefixes.iter().any(|prefix| {
        let q = prefix.trim_matches('/');
        if q.is_empty() {
            // Root prefix → grant access to the whole tree.
            return true;
        }
        if p == q {
            return true;
        }
        p.starts_with(q) && p.as_bytes().get(q.len()) == Some(&b'/')
    })
}

/// Current unix time in seconds.
fn now_epoch_seconds() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn claims(perms: &[Permission], paths: &[&str], exp: Option<i64>) -> TokenClaims {
        TokenClaims {
            sub: "tester".into(),
            exp,
            permissions: perms.to_vec(),
            allowed_paths: paths.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn allows_segment_boundary_paths() {
        let v = PathValidator::new();
        let c = claims(&[Permission::Read], &["/docs"], None);
        for path in ["/docs", "/docs/", "/docs/a.txt", "docs/a.txt"] {
            assert!(v.validate(&c, path, Action::Read).is_ok(), "{path}");
        }
    }

    #[test]
    fn rejects_sibling_prefix_paths() {
        let v = PathValidator::new();
        let c = claims(&[Permission::Read], &["/docs"], None);
        assert!(matches!(
            v.validate(&c, "/docshop/x", Action::Read),
            Err(AuthError::Forbidden { .. })
        ));
    }

    #[test]
    fn rejects_wrong_permission() {
        let v = PathValidator::new();
        let c = claims(&[Permission::Read], &["/docs"], None);
        assert!(matches!(
            v.validate(&c, "/docs/a", Action::Write),
            Err(AuthError::Forbidden { .. })
        ));
    }

    #[test]
    fn rejects_expired() {
        let v = PathValidator::new();
        let c = claims(&[Permission::Read], &["/docs"], Some(1_000));
        assert!(matches!(v.validate(&c, "/docs/a", Action::Read), Err(AuthError::Expired)));
    }

    #[test]
    fn empty_allowed_paths_denies_all() {
        let v = PathValidator::new();
        let c = claims(&[Permission::Read], &[], None);
        assert!(matches!(
            v.validate(&c, "/anything", Action::Read),
            Err(AuthError::Forbidden { .. })
        ));
    }

    #[test]
    fn root_prefix_grants_full_access() {
        let v = PathValidator::new();
        for root in ["/", "", "/ "] {
            let c = claims(&[Permission::Read], &[root.trim()], None);
            for path in ["/a.txt", "/deep/nested/file.bin", "/"] {
                assert!(
                    v.validate(&c, path, Action::Read).is_ok(),
                    "root {root:?} should allow {path}"
                );
            }
        }
    }

    #[test]
    fn raw_prefix_mode_matches_directly() {
        let mut v = PathValidator::new();
        v.raw_prefix_match = true;
        let c = claims(&[Permission::Read], &["/doc"], None);
        assert!(v.validate(&c, "/docshop", Action::Read).is_ok());
    }

    #[test]
    fn is_expired_boundary() {
        let c = claims(&[], &[], Some(100));
        assert!(!c.is_expired(50));
        assert!(!c.is_expired(99));
        assert!(c.is_expired(100));
        assert!(c.is_expired(101));
    }
}
