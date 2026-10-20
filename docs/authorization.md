# Authorization: implementing a verifier

libfw never issues tokens. It owns exactly two decisions:

1. **Authentication** — turn `Authorization: Bearer <token>` into
   `TokenClaims` (`TokenVerifier`).
2. **Authorization** — decide whether those claims may perform an `Action`
   (`Read` / `Write`) on a path (`Validator`).

Both are traits in `libfw-core`, so the examples' fixed-token verifier is a
placeholder you replace with your own. This guide shows the verifiers you
are likely to need, the `Validator` variants that go with them, and how the
pieces are wired into `ServerState`.

- Trait definitions: [`TokenVerifier`](../crates/libfw-core/src/auth.rs),
  [`Validator`](../crates/libfw-core/src/auth.rs)
- Claims: [`TokenClaims`](../crates/libfw-core/src/claims.rs)
- Shipped validator: `PathValidator`

---

## The contract

```rust
pub struct TokenClaims {
    pub sub: String,                  // subject: user / client / key id
    pub exp: Option<i64>,             // unix seconds; None = never expires
    pub permissions: Vec<Permission>, // Read | Write
    pub allowed_paths: Vec<String>,   // path prefixes, real storage paths
}

pub trait TokenVerifier: Send + Sync + 'static {
    fn verify(&self, token: &str) -> Result<TokenClaims, AuthError>;
}

pub trait Validator: Send + Sync + 'static {
    fn validate(&self, claims: &TokenClaims, path: &str, action: Action)
        -> Result<(), AuthError>;
}
```

Request flow on the server:

```
Authorization header
    → BearerClaims extractor            (401 on malformed header)
    → ServerState::verifier.verify()    (401 on bad/expired token)
    → ServerState::resolve_client_path()
        → decode shadow path            (400 on tampered shadow)
        → ServerState::validator.validate(real_path)  (403 on denial)
    → handler
```

Two consequences worth designing for:

- **`verify` is synchronous.** It runs on the request path with no `.await`.
  A JWT signature check is cheap; a network round-trip to an introspection
  endpoint is not. Cache, or fail closed — see
  [Remote introspection](#5-remote-introspection-oauth2-style).
- **`verify` only authenticates.** Returning permissive claims from `verify`
  does not bypass access control; the `Validator` still runs on the decoded
  **real** path. Keep authorization policy in the `Validator` when it is
  path-shaped, and in `verify` when it comes from the token itself.

`AuthError` → HTTP mapping is fixed:

| `AuthError` | Status |
| --- | --- |
| `MissingToken`, `Invalid(_)`, `Expired` | `401 Unauthorized` |
| `Forbidden { path, action }` | `403 Forbidden` |

---

## 1. Fixed token (development only)

This is what both example servers do — one literal token, full access:

```rust
use libfw_core::auth::{AuthError, TokenVerifier};
use libfw_core::claims::{Permission, TokenClaims};

#[derive(Clone)]
struct DevTokenVerifier;

impl TokenVerifier for DevTokenVerifier {
    fn verify(&self, token: &str) -> Result<TokenClaims, AuthError> {
        if token == "dev-token" {
            Ok(TokenClaims {
                sub: "dev-user".into(),
                exp: None,
                permissions: vec![Permission::Read, Permission::Write],
                allowed_paths: vec!["/".into()],
            })
        } else {
            Err(AuthError::Invalid("unknown token".into()))
        }
    }
}
```

Never ship this. It is a hard-coded secret with full read/write over the
whole tree, and `exp: None` means it can never be revoked by time.

A slightly better variant keeps the secret out of the binary and supports
several principals:

```rust
use std::collections::HashMap;
use libfw_core::auth::{AuthError, TokenVerifier};
use libfw_core::claims::{Permission, TokenClaims};

pub struct StaticTokenVerifier {
    // token → claims
    tokens: HashMap<String, TokenClaims>,
}

impl StaticTokenVerifier {
    /// `TOKENS="alice:tok-a:rw:/,bob:tok-b:r:/public"` style loading.
    pub fn from_env() -> Self {
        let mut tokens = HashMap::new();
        for entry in std::env::var("LIBFW_TOKENS").unwrap_or_default().split(',') {
            let mut parts = entry.split(':');
            let (Some(sub), Some(token), Some(perms), Some(paths)) =
                (parts.next(), parts.next(), parts.next(), parts.next())
            else { continue };
            let permissions = perms
                .chars()
                .filter_map(|c| match c {
                    'r' => Some(Permission::Read),
                    'w' => Some(Permission::Write),
                    _ => None,
                })
                .collect();
            tokens.insert(
                token.to_string(),
                TokenClaims {
                    sub: sub.to_string(),
                    exp: None,
                    permissions,
                    allowed_paths: paths.split(';').map(str::to_string).collect(),
                },
            );
        }
        Self { tokens }
    }
}

impl TokenVerifier for StaticTokenVerifier {
    fn verify(&self, token: &str) -> Result<TokenClaims, AuthError> {
        if token.is_empty() {
            return Err(AuthError::MissingToken);
        }
        // Compare in constant time: `HashMap::get` already leaks only the
        // bucket, but comparing candidate keys with `==` is the usual
        // timing footgun when tokens are checked one by one.
        let mut found = None;
        for (candidate, claims) in &self.tokens {
            if ct_eq(candidate.as_bytes(), token.as_bytes()) {
                found = Some(claims.clone());
            }
        }
        found.ok_or_else(|| AuthError::Invalid("unknown token".into()))
    }
}

fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b) {
        diff |= x ^ y;
    }
    diff == 0
}
```

Rotation still means restarting the process (or rebuilding the verifier), but
no secret is compiled in.

---

## 2. HMAC-signed compact tokens

A self-contained token you can mint anywhere, with no JWT dependency:

```
token    = base64url(payload_json) "." hex(hmac_sha256(secret, base64url_payload))
payload  = { "sub": "...", "exp": 1710000000, "r": true, "w": false,
             "paths": ["/docs"] }
```

```toml
[dependencies]
hmac = "0.12"
sha2 = "0.10"
subtle = "2"
base64 = "0.22"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
```

```rust
use hmac::{Hmac, Mac};
use libfw_core::auth::{AuthError, TokenVerifier};
use libfw_core::claims::{Permission, TokenClaims};
use sha2::Sha256;
use subtle::ConstantTimeEq;

type HmacSha256 = Hmac<Sha256>;

#[derive(serde::Deserialize)]
struct Payload {
    sub: String,
    #[serde(default)]
    exp: Option<i64>,
    #[serde(default)]
    r: bool,
    #[serde(default)]
    w: bool,
    #[serde(default)]
    paths: Vec<String>,
}

pub struct HmacVerifier {
    secret: Vec<u8>,
}

fn b64_decode(s: &str) -> Option<Vec<u8>> {
    use base64::Engine as _;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(s).ok()
}

impl TokenVerifier for HmacVerifier {
    fn verify(&self, token: &str) -> Result<TokenClaims, AuthError> {
        if token.is_empty() {
            return Err(AuthError::MissingToken);
        }
        let (body, sig_hex) = token
            .rsplit_once('.')
            .ok_or_else(|| AuthError::Invalid("malformed token".into()))?;

        let sig = hex_decode(sig_hex)
            .ok_or_else(|| AuthError::Invalid("malformed signature".into()))?;
        let mut mac = HmacSha256::new_from_slice(&self.secret)
            .map_err(|e| AuthError::Invalid(format!("hmac key: {e}")))?;
        mac.update(body.as_bytes());
        // `verify_slice` is constant-time and length-safe.
        mac.verify_slice(&sig)
            .map_err(|_| AuthError::Invalid("bad signature".into()))?;

        let raw = b64_decode(body)
            .ok_or_else(|| AuthError::Invalid("bad payload encoding".into()))?;
        let p: Payload = serde_json::from_slice(&raw)
            .map_err(|e| AuthError::Invalid(format!("bad payload: {e}")))?;

        if p.exp.is_some_and(|exp| now_secs() >= exp) {
            return Err(AuthError::Expired);
        }

        let mut permissions = Vec::new();
        if p.r {
            permissions.push(Permission::Read);
        }
        if p.w {
            permissions.push(Permission::Write);
        }

        Ok(TokenClaims {
            sub: p.sub,
            exp: p.exp,
            permissions,
            allowed_paths: p.paths,
        })
    }
}

fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn hex_decode(s: &str) -> Option<Vec<u8>> {
    if s.len() % 2 != 0 {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
        .collect()
}
```

Mint the matching token on your login service with the same secret. Rotate
secrets by keeping two verifiers (current + previous) and accepting either.

---

## 3. JWT (HS256 / RS256)

`TokenVerifier` is sync, so use the sync `decode` API. Map your JWT claim
names onto `TokenClaims`:

```toml
[dependencies]
jsonwebtoken = "9"
serde = { version = "1", features = ["derive"] }
```

```rust
use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use libfw_core::auth::{AuthError, TokenVerifier};
use libfw_core::claims::{Permission, TokenClaims};

#[derive(serde::Deserialize)]
struct JwtClaims {
    sub: String,
    exp: usize,
    #[serde(default)]
    scope: String,             // e.g. "files:read files:write"
    #[serde(default)]
    paths: Vec<String>,        // e.g. ["/docs", "/uploads"]
}

pub struct JwtVerifier {
    key: DecodingKey,
    validation: Validation,
}

impl JwtVerifier {
    pub fn hs256(secret: &[u8]) -> Self {
        let mut validation = Validation::new(Algorithm::HS256);
        validation.set_required_spec_claims(&["exp", "sub"]);
        Self { key: DecodingKey::from_secret(secret), validation }
    }

    pub fn rs256_pem(pem: &[u8]) -> Result<Self, jsonwebtoken::errors::Error> {
        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_required_spec_claims(&["exp", "sub"]);
        Ok(Self {
            key: DecodingKey::from_rsa_pem(pem)?,
            validation,
        })
    }
}

impl TokenVerifier for JwtVerifier {
    fn verify(&self, token: &str) -> Result<TokenClaims, AuthError> {
        if token.is_empty() {
            return Err(AuthError::MissingToken);
        }
        let data = decode::<JwtClaims>(token, &self.key, &self.validation).map_err(|e| {
            use jsonwebtoken::errors::ErrorKind;
            match e.kind() {
                ErrorKind::ExpiredSignature => AuthError::Expired,
                other => AuthError::Invalid(format!("jwt: {other:?}")),
            }
        })?;
        let c = data.claims;

        let mut permissions = Vec::new();
        for scope in c.scope.split_whitespace() {
            match scope {
                "files:read" => permissions.push(Permission::Read),
                "files:write" => permissions.push(Permission::Write),
                _ => {}
            }
        }

        Ok(TokenClaims {
            sub: c.sub,
            exp: Some(c.exp as i64),
            permissions,
            allowed_paths: c.paths,
        })
    }
}
```

`decode` already verifies `exp`; the `exp` field is still copied into
`TokenClaims` so `PathValidator` re-checks it consistently.

### RS256 with a rotating JWKS

```rust
impl TokenVerifier for JwksVerifier {
    fn verify(&self, token: &str) -> Result<TokenClaims, AuthError> {
        // `kid` comes from the JWT header. The key set is refreshed by a
        // background task; requests only read the cached map, so this stays
        // synchronous and non-blocking.
        let kid = jsonwebtoken::decode_header(token)
            .map_err(|e| AuthError::Invalid(format!("jwt header: {e}")))?
            .kid
            .ok_or_else(|| AuthError::Invalid("missing kid".into()))?;
        let key = self
            .keys
            .read()
            .unwrap()
            .get(&kid)
            .cloned()
            .ok_or_else(|| AuthError::Invalid("unknown signing key".into()))?;
        decode::<JwtClaims>(token, &key, &self.validation) /* … as above … */
            .map_err(|e| AuthError::Invalid(format!("jwt: {e}")))
            .map(claims_from_jwt)
    }
}
```

Refresh the `keys` map from your IdP's `/.well-known/jwks.json` on a timer
(for example every 15 minutes, and once at startup). Do not fetch JWKS
inside `verify`: a slow or unreachable IdP would otherwise stall every
request.

---

## 4. Permissions and path rules

`PathValidator` (the shipped `Validator`) enforces three rules:

1. `exp` is in the future,
2. the claims contain the permission the action requires
   (`Action::Read → Permission::Read`, `Action::Write → Permission::Write`),
3. the path is inside one of `allowed_paths`.

Path matching is on a **segment boundary**, after normalizing `/`:

| `allowed_paths` | matches | does not match |
| --- | --- | --- |
| `["/docs"]` | `docs`, `docs/`, `docs/a.txt` | `docshop/x` |
| `["/"]` or `[""]` | everything | — |
| `[]` | nothing | everything |

`allowed_paths` names **real storage paths**, even when a `PathCodec` is
installed: the server decodes the client's shadow path first and validates
the decoded path, so a token can never be tricked into authorizing a
different subtree by re-encoding a shadow.

Set `raw_prefix_match` only if you deliberately want string prefixes:

```rust
let mut validator = PathValidator::new();
validator.raw_prefix_match = true; // "/doc" now matches "/docshop/x"
```

### Custom validators

Implement `Validator` when the rule is not "prefix + permission" — group
ACLs, extension rules, read-only mounts, deny-lists, quotas:

```rust
use libfw_core::auth::{Action, AuthError, Validator};
use libfw_core::claims::{Permission, TokenClaims};

/// Read-only mirror: `Write` is denied everywhere, `Read` only under the
/// token's paths (delegates the rest to the default rule).
pub struct ReadOnlyValidator {
    inner: libfw_core::auth::PathValidator,
}

impl Validator for ReadOnlyValidator {
    fn validate(
        &self,
        claims: &TokenClaims,
        path: &str,
        action: Action,
    ) -> Result<(), AuthError> {
        if action == Action::Write {
            return Err(AuthError::Forbidden {
                path: path.to_string(),
                action,
            });
        }
        self.inner.validate(claims, path, action)
    }
}

/// Refuse executable/script uploads regardless of permission.
pub struct NoScriptsValidator {
    inner: libfw_core::auth::PathValidator,
}

impl Validator for NoScriptsValidator {
    fn validate(
        &self,
        claims: &TokenClaims,
        path: &str,
        action: Action,
    ) -> Result<(), AuthError> {
        const BLOCKED: &[&str] = &["sh", "bat", "ps1", "exe", "dll"];
        let blocked = action == Action::Write
            && path
                .rsplit('.')
                .next()
                .is_some_and(|ext| BLOCKED.contains(&ext.to_ascii_lowercase().as_str()));
        if blocked {
            return Err(AuthError::Forbidden {
                path: path.to_string(),
                action,
            });
        }
        self.inner.validate(claims, path, action)
    }
}
```

Guard rails for a custom validator:

- Return `Forbidden { path, action }` (→ `403`) rather than `Invalid`
  (→ `401`) for a policy denial; clients surface the difference.
- Do not trust `path` blindly: it has already passed `validate_rel_path`
  (no absolute paths, no `..`, no NUL, no empty segments), but it is still
  attacker-controlled text. Normalize case/Unicode yourself if your rules
  are case-insensitive.
- Keep it cheap and allocation-light; it runs once per request, including
  every chunk POST of an upload.
- The `Validator` sees the **real** path. If your rules are expressed in
  shadow-path terms, translate explicitly.

---

## 5. Remote introspection (OAuth2 style)

Some deployments delegate verification to an auth service
(`POST /introspect`). Since `verify` cannot await, use a **cache with
background refresh** and fail closed on a miss:

```rust
use std::collections::HashMap;
use std::sync::RwLock;
use libfw_core::auth::{AuthError, TokenVerifier};
use libfw_core::claims::TokenClaims;

pub struct IntrospectionVerifier {
    // token hash → claims, refreshed out of band
    cache: RwLock<HashMap<String, TokenClaims>>,
}

impl TokenVerifier for IntrospectionVerifier {
    fn verify(&self, token: &str) -> Result<TokenClaims, AuthError> {
        if token.is_empty() {
            return Err(AuthError::MissingToken);
        }
        let key = sha256_hex(token);
        let claims = self
            .cache
            .read()
            .unwrap()
            .get(&key)
            .cloned()
            // Unknown token → 401. Never "allow and check later": an
            // unreachable auth service must not widen access.
            .ok_or_else(|| AuthError::Invalid("token not in cache".into()))?;
        if claims.is_expired(now_secs()) {
            return Err(AuthError::Expired);
        }
        Ok(claims)
    }
}
```

Refresh strategy, in order of preference:

1. **Event-driven** — your auth service pushes token grants/revocations to
   an internal channel; the cache mirrors it. Lowest latency, no polling.
2. **Short TTL + lazy fill** — on a miss, return `401`; a side task
   introspects tokens seen recently and populates the cache. The client's
   retry then succeeds. Keeps requests non-blocking.
3. **Blocking introspection with a strict timeout** — spawn a blocking task
   (`tokio::task::block_in_place` / a dedicated thread pool) and wait, but
   only if you accept that request latency now includes your auth service.

Whichever you choose, sign or hash cache keys and never log raw tokens.

---

## 6. Wiring it up

```rust
use std::sync::Arc;
use libfw_core::auth::PathValidator;
use libfw_server::{router, FsStorage, ServerState};

let state = Arc::new(
    ServerState::builder()
        .storage(FsStorage::new("/srv/files"))
        .verifier(HmacVerifier { secret })     // ← your verifier
        .validator(PathValidator::new())       // or a custom Validator
        .build(),
);

let app = router(state);
```

Pitfalls:

- `verifier` and `validator` are **required**; `build()` panics when either
  is missing.
- The verifier is shared (`Arc<dyn TokenVerifier>`) across all connections,
  so it must be `Send + Sync + 'static`. Keep mutable state behind
  `RwLock` / `Mutex` / atomics.
- `TokenClaims.sub` is not just informational: the server derives a
  per-subject tag from it for upload-session temp isolation. Two different
  bearers that map to the same `sub` share in-progress upload space (useful
  for multi-device resume); different `sub` values are strictly isolated.
  Use a stable, low-cardinality identity — not a per-request nonce.
- `/capabilities` is intentionally **public** (no bearer token) and must stay
  free of secrets. Do not put tenant or user data in the advertisement.
- If you mount libfw under a prefix (`Router::nest("/api", router(state))`),
  the bearer flow is unchanged — the path your `Validator` receives is the
  libfw-relative path, not `/api/...`.

---

## 7. Testing a verifier

Verifiers are ordinary synchronous functions, so unit-test them directly
and assert the *error variant*, not just "is an error":

```rust
#[test]
fn rejects_expired() {
    let v = HmacVerifier { secret: b"k".to_vec() };
    let token = mint(&Payload { sub: "a".into(), exp: Some(1), r: true, w: false, paths: vec!["/".into()] });
    assert!(matches!(v.verify(&token), Err(AuthError::Expired)));
}

#[test]
fn rejects_tampered_payload() {
    let v = HmacVerifier { secret: b"k".to_vec() };
    let good = mint(&payload_full_access());
    let (body, sig) = good.split_once('.').unwrap();
    let forged = format!("{}.{}", b64(flip_byte(body)), sig);
    assert!(matches!(v.verify(&forged), Err(AuthError::Invalid(_))));
}
```

For end-to-end coverage, drive the router in-process — no socket needed:

```rust
use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use tower::ServiceExt;

#[tokio::test]
async fn forbidden_path_is_403() {
    let app = router(state);
    let req = Request::builder()
        .uri("/file/private/x.txt")
        .header(header::AUTHORIZATION, "Bearer tok")
        .body(Body::empty())
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::FORBIDDEN);
}
```

Assert all three layers separately:

| Case | Expected |
| --- | --- |
| no `Authorization` header | `401` |
| `Authorization: Basic …` | `401` |
| bad signature / unknown key | `401` |
| expired `exp` | `401` |
| valid token, path outside `allowed_paths` | `403` |
| valid token, missing `Write` for `POST` | `403` |

---

## Checklist before shipping

- [ ] No literal secrets in the binary; load keys from the environment or a
      secret store.
- [ ] `exp` is always set (or expiry is enforced by your validator).
- [ ] Signature comparison is constant-time (`Hmac::verify_slice`, JWT
      library, or `subtle`).
- [ ] `sub` is stable and meaningful; it drives upload-session isolation.
- [ ] Unknown/unreachable-auth states **deny**, never allow.
- [ ] `allowed_paths` are real storage paths, written without relying on
      shadow paths.
- [ ] `/capabilities` stays public and non-sensitive.
- [ ] Tests cover `401` vs `403` for every route family you expose.
