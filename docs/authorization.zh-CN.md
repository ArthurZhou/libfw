# 鉴权：二次开发验证单元

libfw 不签发 token，它只负责两件事：

1. **认证** —— 把 `Authorization: Bearer <token>` 变成 `TokenClaims`
   （`TokenVerifier`）。
2. **授权** —— 判断这份 claims 是否可以在某个路径上执行某个动作
   （`Read` / `Write`，由 `Validator` 决定）。

两者都是 `libfw-core` 中的 trait。示例服务器里那个"固定 token"的验证器只是
占位实现，生产环境应当替换成自己的逻辑。本文给出常见验证器的完整写法、
配套的 `Validator`，以及如何装配进 `ServerState`。

- trait 定义：[`TokenVerifier`](../crates/libfw-core/src/auth.rs)、
  [`Validator`](../crates/libfw-core/src/auth.rs)
- claims：[`TokenClaims`](../crates/libfw-core/src/claims.rs)
- 内置授权器：`PathValidator`

---

## 契约

```rust
pub struct TokenClaims {
    pub sub: String,                  // 主体：用户 / 客户端 / 密钥 id
    pub exp: Option<i64>,             // unix 秒；None = 永不过期
    pub permissions: Vec<Permission>, // Read | Write
    pub allowed_paths: Vec<String>,   // 路径前缀，真实存储路径
}

pub trait TokenVerifier: Send + Sync + 'static {
    fn verify(&self, token: &str) -> Result<TokenClaims, AuthError>;
}

pub trait Validator: Send + Sync + 'static {
    fn validate(&self, claims: &TokenClaims, path: &str, action: Action)
        -> Result<(), AuthError>;
}
```

服务端请求链路：

```
Authorization 头
    → BearerClaims 提取器              （头格式错误 → 401）
    → ServerState::verifier.verify()   （token 无效/过期 → 401）
    → ServerState::resolve_client_path()
        → 解码影子路径                  （影子路径被篡改 → 400）
        → ServerState::validator.validate(真实路径)   （越权 → 403）
    → handler
```

由此有两点需要在设计时注意：

- **`verify` 是同步的。** 它直接在请求路径上执行，没有 `.await`。JWT 验签
  很便宜；访问内省接口的网络往返则不便宜。要么缓存，要么"未知即拒绝"，
  见[远程内省](#5-远程内省oauth2-风格)。
- **`verify` 只负责认证。** 在 `verify` 里返回宽松的 claims 并不能绕过访问
  控制：`Validator` 仍会在解码后的**真实路径**上执行。与路径相关的策略放
  `Validator`，来自 token 本身的策略放 `verify`。

`AuthError` 到 HTTP 状态码的映射是固定的：

| `AuthError` | 状态码 |
| --- | --- |
| `MissingToken`、`Invalid(_)`、`Expired` | `401 Unauthorized` |
| `Forbidden { path, action }` | `403 Forbidden` |

---

## 1. 固定 token（仅开发）

两个示例服务器用的就是这个：一个字面量 token，拥有全部权限。

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

不要在生产使用：硬编码密钥、整棵树可读写，而且 `exp: None` 意味着无法通过
时间吊销。

稍微好一点的版本把密钥移出二进制，并支持多个主体：

```rust
use std::collections::HashMap;
use libfw_core::auth::{AuthError, TokenVerifier};
use libfw_core::claims::{Permission, TokenClaims};

pub struct StaticTokenVerifier {
    tokens: HashMap<String, TokenClaims>, // token → claims
}

impl StaticTokenVerifier {
    /// 形如 `TOKENS="alice:tok-a:rw:/,bob:tok-b:r:/public"` 的加载方式。
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
        // 常量时间比较：逐个 `==` 是常见的计时侧信道。
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

轮换仍需重启或重建验证器，但至少没有把密钥编译进程序。

---

## 2. HMAC 签名的紧凑 token

不引入 JWT 依赖也能自造签名 token：

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
    pub secret: Vec<u8>,
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
        // `verify_slice` 是常量时间且长度安全的比较。
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

登录服务用同一把密钥签发对应 token 即可。轮换时可同时保留新旧两个验证器。

---

## 3. JWT（HS256 / RS256）

`TokenVerifier` 是同步的，因此使用同步的 `decode`。把 JWT 的 claim 映射到
`TokenClaims`：

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
    scope: String,       // 如 "files:read files:write"
    #[serde(default)]
    paths: Vec<String>,  // 如 ["/docs", "/uploads"]
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

`decode` 已经校验 `exp`；这里仍把 `exp` 抄进 `TokenClaims`，让 `PathValidator`
再检查一次，保持两条路径行为一致。

### RS256 + 轮换 JWKS

```rust
impl TokenVerifier for JwksVerifier {
    fn verify(&self, token: &str) -> Result<TokenClaims, AuthError> {
        // `kid` 来自 JWT header。密钥集合由后台任务刷新，请求只读缓存，
        // 从而保持同步、不阻塞。
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
        decode::<JwtClaims>(token, &key, &self.validation)
            .map_err(|e| AuthError::Invalid(format!("jwt: {e}")))
            .map(claims_from_jwt)
    }
}
```

由定时任务（例如每 15 分钟一次，启动时一次）从 IdP 的
`/.well-known/jwks.json` 刷新 `keys`。**不要**在 `verify` 里拉取 JWKS：
IdP 慢或不可达会拖死所有请求。

---

## 4. 权限与路径规则

`PathValidator`（内置 `Validator`）检查三件事：

1. `exp` 未过期；
2. claims 含该动作所需的权限（`Action::Read → Permission::Read`，
   `Action::Write → Permission::Write`）；
3. 路径位于 `allowed_paths` 之一。

路径按**段边界**匹配（先规范化 `/`）：

| `allowed_paths` | 匹配 | 不匹配 |
| --- | --- | --- |
| `["/docs"]` | `docs`、`docs/`、`docs/a.txt` | `docshop/x` |
| `["/"]` 或 `[""]` | 全部 | — |
| `[]` | 无 | 全部 |

即使安装了 `PathCodec`，`allowed_paths` 仍然指**真实存储路径**：服务端先解码
客户端传来的影子路径，再校验解码后的路径，因此 token 无法通过重新编码影子
路径来"绕过"到另一棵子树。

只有确实需要字符串前缀语义时才打开 `raw_prefix_match`：

```rust
let mut validator = PathValidator::new();
validator.raw_prefix_match = true; // 现在 "/doc" 会匹配 "/docshop/x"
```

### 自定义 Validator

当规则不是"前缀 + 权限"时（分组 ACL、扩展名规则、只读挂载、拒绝列表、
配额），实现 `Validator`：

```rust
use libfw_core::auth::{Action, AuthError, Validator};
use libfw_core::claims::TokenClaims;

/// 只读镜像：任何 `Write` 都拒绝，`Read` 仍走默认规则。
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

/// 无论权限如何，都拒绝可执行/脚本文件的上传。
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

自定义 `Validator` 的注意事项：

- 策略拒绝要返回 `Forbidden { path, action }`（→ `403`），而不是 `Invalid`
  （→ `401`）；客户端会区分两者。
- 不要盲目信任 `path`：它已通过 `validate_rel_path`（无绝对路径、无 `..`、
  无 NUL、无空段），但它依然是攻击者可控的文本。若规则大小写无关，请自行
  规范化。
- 保持轻量：每个请求都会执行一次，上传时每个 chunk POST 也会执行。
- `Validator` 看到的是**真实路径**。若你的规则基于影子路径表达，请显式转换。

---

## 5. 远程内省（OAuth2 风格）

有些部署把验证委托给鉴权服务（`POST /introspect`）。由于 `verify` 不能
`.await`，请使用**缓存 + 后台刷新**，并在未命中时**拒绝**：

```rust
use std::collections::HashMap;
use std::sync::RwLock;
use libfw_core::auth::{AuthError, TokenVerifier};
use libfw_core::claims::TokenClaims;

pub struct IntrospectionVerifier {
    cache: RwLock<HashMap<String, TokenClaims>>, // token 哈希 → claims
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
            // 未知 token → 401。绝不做"先放行后校验"：鉴权服务不可达时
            // 不能放大权限。
            .ok_or_else(|| AuthError::Invalid("token not in cache".into()))?;
        if claims.is_expired(now_secs()) {
            return Err(AuthError::Expired);
        }
        Ok(claims)
    }
}
```

刷新策略，按优先级：

1. **事件驱动** —— 鉴权服务把授权/吊销推送到内部通道，缓存随之更新。延迟
   最低，无需轮询。
2. **短 TTL + 懒填充** —— 未命中就返回 `401`；旁路任务内省最近出现的 token
   并写入缓存，客户端重试后成功。请求路径始终不阻塞。
3. **带严格超时的阻塞内省** —— 用 `tokio::task::block_in_place` 或独立线程池
   执行，并接受请求延迟包含鉴权服务耗时。

无论哪种，缓存键请做哈希或签名，并且永远不要打印原始 token。

---

## 6. 装配

```rust
use std::sync::Arc;
use libfw_core::auth::PathValidator;
use libfw_server::{router, FsStorage, ServerState};

let state = Arc::new(
    ServerState::builder()
        .storage(FsStorage::new("/srv/files"))
        .verifier(HmacVerifier { secret })     // ← 你的验证器
        .validator(PathValidator::new())       // 或自定义 Validator
        .build(),
);

let app = router(state);
```

易错点：

- `verifier` 与 `validator` 是**必填**的，缺失时 `build()` 会 panic。
- 验证器以 `Arc<dyn TokenVerifier>` 在所有连接间共享，因此必须
  `Send + Sync + 'static`；可变状态请放进 `RwLock` / `Mutex` / 原子类型。
- `TokenClaims.sub` 不只是信息字段：服务端会用它派生上传会话临时文件的
  归属标签。映射到同一 `sub` 的不同 bearer 会共享未完成的上传空间（适合
  多设备续传）；不同 `sub` 之间严格隔离。请使用稳定、低基数的身份，
  而不是每请求一个随机数。
- `/capabilities` 有意设计为**公开**（无需 bearer token），其中不得包含任何
  机密。不要把租户或用户数据放进能力声明。
- 把 libfw 挂在子路径（`Router::nest("/api", router(state))`）不影响认证流程
  —— `Validator` 收到的是 libfw 相对路径，不含 `/api`。

---

## 7. 测试验证器

验证器就是普通的同步函数，可直接单测，并且断言**错误变体**而不只是"出错了"：

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

端到端可直接在进程内驱动 router（无需监听端口）：

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

至少分别覆盖三层：

| 场景 | 期望 |
| --- | --- |
| 没有 `Authorization` 头 | `401` |
| `Authorization: Basic …` | `401` |
| 签名错误 / 未知密钥 | `401` |
| `exp` 已过期 | `401` |
| token 有效但路径不在 `allowed_paths` | `403` |
| token 有效但 `POST` 缺少 `Write` | `403` |

---

## 上线前检查清单

- [ ] 二进制中没有硬编码密钥；密钥来自环境变量或密钥管理服务。
- [ ] `exp` 始终设置（或由 validator 强制过期）。
- [ ] 签名比较是常量时间（`Hmac::verify_slice`、JWT 库或 `subtle`）。
- [ ] `sub` 稳定且有业务含义 —— 它决定上传会话隔离。
- [ ] 未知/鉴权服务不可达的状态一律**拒绝**，绝不放行。
- [ ] `allowed_paths` 写的是真实存储路径，不依赖影子路径。
- [ ] `/capabilities` 保持公开且不含敏感信息。
- [ ] 对每类路由都覆盖了 `401` 与 `403` 两种情况。
