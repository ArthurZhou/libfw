# 服务端集成

libfw 分为三个发布 crate 加若干可运行示例：

| Crate | 职责 | 依赖 |
| --- | --- | --- |
| `libfw-core` | 契约：claims、验证器/授权器 trait、存储 trait、压缩、范围、协议常量 | — |
| `libfw-server` | axum 路由 + handler + `FsStorage` | `libfw-core` |
| `libfw-client` | 浏览器引擎（WASM）**以及** 原生 Rust 客户端 | `libfw-core` |

`libfw-server` 很薄：它只是把 `libfw-core` 的契约接到 axum 上。其他框架可以用
core 提供的辅助函数实现同样的路由。

- [axum](#axum)
- [其他框架](#其他框架)
- [ServerState 参考](#serverstate-参考)
- [存储后端](#存储后端)
- [路径转换（影子路径）](#路径转换影子路径)
- [能力声明与限制](#能力声明与限制)
- [会话清理](#会话清理)
- [部署](#部署)
- [示例](#示例)

## axum

```toml
[dependencies]
libfw-server = "0.4"
axum = "0.8"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

```rust,no_run
use std::sync::Arc;
use axum::Router;
use libfw_core::auth::{AuthError, PathValidator, TokenVerifier};
use libfw_core::claims::{Permission, TokenClaims};
use libfw_server::{router, FsStorage, ServerState};

#[derive(Clone)]
struct MyVerifier;

impl TokenVerifier for MyVerifier {
    fn verify(&self, token: &str) -> Result<TokenClaims, AuthError> {
        // 生产环境请换成 JWT / HMAC / 内省，见 docs/authorization.zh-CN.md
        if token.is_empty() {
            return Err(AuthError::MissingToken);
        }
        Ok(TokenClaims {
            sub: token.to_string(),
            exp: None,
            permissions: vec![Permission::Read, Permission::Write],
            allowed_paths: vec!["/".into()],
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let state = Arc::new(
        ServerState::builder()
            .storage(FsStorage::new("/srv/files"))
            .verifier(MyVerifier)
            .validator(PathValidator::new())
            .build(),
    );

    let app: Router = router(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:8080").await?;
    axum::serve(listener, app).await?;
    Ok(())
}
```

`router(state)` 返回一个完整的 `Router`：

```
GET|HEAD  /file/{*path}
POST      /file/{*path}
GET       /dir
GET       /dir/{*path}
GET       /capabilities
```

用 `Router::nest` 挂到子路径：

```rust,no_run
# use axum::Router;
# fn build() -> Router { Router::new() }
let app = Router::new()
    .nest("/api", router(state))          // → /api/file/...、/api/dir、/api/capabilities
    .route("/", axum::routing::get(|| async { "hello" }));
```

传给 `Validator` 的路径是 libfw 相对路径（`/file/x.txt` 到达时是 `x.txt`），
因此前缀不会出现在鉴权决策里。

可以自由叠加中间件 —— 它就是一个普通 axum `Router`：

```rust,no_run
# use axum::Router;
# let app: Router = Router::new();
use tower_http::trace::TraceLayer;

let app = app.layer(TraceLayer::new_for_http());
```

### 在自己的 handler 里复用认证

`BearerClaims` 是公开的 `FromRequestParts` 提取器，复用同一个验证器，因此自定义
路由也能共享认证逻辑：

```rust,no_run
use axum::extract::State;
use axum::Json;
use libfw_server::BearerClaims;

async fn whoami(BearerClaims(claims): BearerClaims) -> Json<serde_json::Value> {
    Json(serde_json::json!({ "sub": claims.sub }))
}
```

## 其他框架

`libfw-core` 与框架无关。如果不用 axum，可以直接构造 `ServerState`，并调用
`libfw-server` 内部使用的同一批辅助函数：

| 需求 | 辅助函数 |
| --- | --- |
| 解析 `Range` | `libfw_server::parse_range_header` → `ParsedRange` |
| 生成 `Content-Range` | `libfw_server::content_range_value`、`content_range_none_value` |
| ETag 比较 | `etag_matches_if_none_match`、`if_range_matches` |
| 路径校验 | `libfw_server::validate_rel_path` |
| 影子路径 | `ServerState::resolve_client_path` / `expose_path` |
| 压缩 | `libfw_core::compressor_with_level`、`decompressor_with_limit` |
| 头名称常量 | `HEADER_FILE_META`、`HEADER_SESSION` 等（由 `libfw-server` 重导出） |

仓库中的 `examples/actix-server` 是同一套路由的完整 actix-web 实现。它有意
不提供 `/capabilities`，因此自适应客户端连接它时会退回静态配置。

若自己实现路由，以下不变量是客户端正常工作的前提：

1. `HEAD /file/{path}` 返回 `ETag` 与**未压缩**的 `Content-Length`。
2. `GET` 支持 `Range`、`If-Range`、`If-None-Match`，并以
   `Content-Range: bytes */<size>` 返回 `416`。
3. `POST` 接受 base64 的 `x-libfw-file-meta`；会话上传按位置写入；只有
   `x-libfw-final: 1` 才提交。
4. 会话探测（`x-libfw-session-status: 1`）返回未压缩偏移的
   `{"ranges": [[start,end], …]}`。
5. 即使 body 被压缩，区间也按未压缩文件偏移解释。

## ServerState 参考

`ServerState::builder()` 必须提供 `storage`、`verifier`、`validator`；缺少任一项
`build()` 会 panic。

| 构建方法 | 默认值 | 作用 |
| --- | --- | --- |
| `.storage(impl StorageBackend)` | 必填 | 文件内容后端 |
| `.verifier(impl TokenVerifier)` | 必填 | token → claims |
| `.validator(impl Validator)` | 必填 | claims + 路径 + 动作 → 放行/拒绝 |
| `.path_codec(impl PathCodec)` | `IdentityPathCodec` | 真实 ↔ 影子路径 |
| `.compression(CompressionFormat)` | `Zrip` | 下载压缩 |
| `.max_upload_size(u64)` | 100 GiB | 实际上传上限 |
| `.limits(Limits)` | 内置默认 | 对外声明的调优区间 |
| `.zrip_levels(ZripLevels)` | `None` | 声明并接受级别区间 |
| `.cleanup_interval(Duration)` | 1 小时 | 临时会话清理周期（`ZERO` 关闭） |
| `.session_ttl(Duration)` | 24 小时 | 被放弃会话临时文件的最大存活时间 |

公开字段与 builder 对应，构建后的 `ServerState` 可以读取：

```rust,no_run
# use std::sync::Arc;
# use libfw_server::ServerState;
# fn f(state: &Arc<ServerState>) {
let caps = state.capabilities();
println!("对外声明的上传上限：{} 字节", caps.limits.max_upload_size);
# }
```

方法：

- `authorize(claims, path, action)` —— 直接执行授权器。
- `resolve_client_path(claims, shadow, action)` —— 解码并授权客户端路径，返回
  真实路径或 `PathResolveError`（`Invalid`/`Codec` → `400`，`Auth` →
  `401`/`403`）。
- `expose_path(real)` —— 把真实路径编码成响应里使用的影子路径。
- `capabilities()` —— `/capabilities` 载荷。
- `spawn_stale_session_cleanup()` —— 启动清理任务。

## 存储后端

### `FsStorage`

`FsStorage::new(root)` 服务本地目录，是参考实现：

- 路径会被规范化并拒绝穿越（`..`、绝对路径、NUL）。
- 每个路径分量都会异步检查符号链接，因此根目录内放置的软链无法把读写重定向到
  根目录之外。
- 上传先写临时文件，提交时原子重命名。
- 会话上传按位置写入共享临时文件，并维护 `.blocks` 区间边车；
  `SHA-256(sub)` 的前 16 个十六进制字符被嵌入文件名，实现按用户隔离。
- 通过内部锁注册表按目标串行化区间读取（`.blocks` 解析、`received_ranges`），
  因此不同文件的并发分块互不阻塞，同一文件的写入者依次执行。

### 自定义后端

实现 `StorageBackend` 即可接入 S3、对象存储、内存或数据库：

```rust,no_run
use async_trait::async_trait;
use std::io::Read;
use libfw_core::metadata::FileMeta;
use libfw_core::{DirEntry, RangeSpec, StorageBackend, StorageError, UploadSink, WriteMode};

struct MyBackend;

#[async_trait]
impl StorageBackend for MyBackend {
    async fn file_meta(&self, path: &str) -> Result<Option<FileMeta>, StorageError> { todo!() }
    async fn read_stream(&self, path: &str, range: RangeSpec)
        -> Result<Box<dyn Read + Send>, StorageError> { todo!() }
    async fn write_stream(&self, path: &str, mode: WriteMode)
        -> Result<Box<dyn UploadSink>, StorageError> { todo!() }
    async fn list_dir(&self, path: &str) -> Result<Vec<DirEntry>, StorageError> { todo!() }
    async fn mkdir_all(&self, path: &str) -> Result<(), StorageError> { todo!() }
    async fn remove(&self, path: &str) -> Result<(), StorageError> { todo!() }
}
```

`write_stream` 会收到 `WriteMode`：

| 模式 | 契约 |
| --- | --- |
| `Create` | 目标已存在时返回 `AlreadyExists` |
| `Overwrite` | 创建或截断 |
| `Resume { offset }` | 从 `offset` 继续；目标当前不是恰好 `offset` 字节时失败 |

`UploadSink` 支持两个层级。只实现最小集合时，并发会话路径会优雅降级：

| 方法 | 是否必须 | 默认行为 |
| --- | --- | --- |
| `write` | 是 | — |
| `write_at(offset, buf)` | 建议 | 退化为追加 |
| `received_ranges()` | 建议 | 返回 `[]` → 续传退化为整文件重发 |
| `len` | 是 | — |
| `commit()` | 是 | — |
| `abort()` | 是 | — |

`StorageBackend::write_stream_session(path, session, owner, mode)` 让后端选择加入
位置写入会话协议；默认转发给 `write_stream`。`cleanup_stale_sessions(max_age)`
默认不做任何清理。

若后端无法提供已收区间，提交只能按长度校验（现有条件下最强的不变量），续传会
重发整个文件。这依然正确，只是效率较低。

## 路径转换（影子路径）

默认情况下客户端看到真实存储路径。`PathCodec` 可以隐藏目录结构：

| Codec | 影子路径形态 | 说明 |
| --- | --- | --- |
| `IdentityPathCodec` | 就是真实路径 | 默认，零开销 |
| `MountPathCodec` | 可读别名，如 `home/alice/x` | 稳定、可读的命名空间 |
| `EncryptedPathCodec` | `v1.<base64url>` 不透明串 | AES-256-GCM，需 `path-encrypt` 特性 |

```rust,no_run
use libfw_core::pathmap::{MountPathCodec, PathCodec};

let codec = MountPathCodec::new(vec![
    ("home/alice".to_string(), "data/vol-3/alice".to_string()),
]);

assert_eq!(codec.encode("data/vol-3/alice/report.pdf"), "home/alice/report.pdf");
assert_eq!(codec.decode("home/alice/report.pdf").unwrap(), "data/vol-3/alice/report.pdf");
```

```rust,no_run
# #[cfg(feature = "path-encrypt")]
# fn f(key_hex: &str) -> Result<(), Box<dyn std::error::Error>> {
use libfw_server::EncryptedPathCodec;

let codec = EncryptedPathCodec::from_hex(key_hex)?; // 64 个十六进制字符 = 32 字节
# let _ = codec;
# Ok(())
# }
```

行为：

- **入站** —— `resolve_client_path` 先做形态校验，再解码，然后按**真实路径**授权。
  token 语义完全不变。
- **出站** —— 目录列表与上传响应会通过 `expose_path` 把真实路径编回影子路径，
  因此列表中的影子路径可以直接复用。
- **层级组合** —— `{dirShadow}/字面量/子路径` 可用：解码最长的可解码段前缀并
  追加其余部分，再授权。一个目录影子即可覆盖整棵子树，无需为每个文件生成影子。
- **篡改** —— 没有任何前缀可解码的影子路径返回 `400`。
- **确定性** —— GCM 影子路径每次编码使用随机 nonce；若需要稳定、可比较的名字，
  请用 `MountPathCodec`。
- **根** —— 规范根（`""`）映射到自身，`GET /dir` 路由就是这样传参的。

## 能力声明与限制

`/capabilities` 是自适应客户端协商所依据的契约。`ServerState::capabilities()`
根据 builder 设置生成：

- `formats` 反映配置的下载压缩（`Zrip` → `["identity","zrip"]`，
  `None` → `["identity"]`）。
- `limits` 来自 `.limits(...)` 或内置默认值。
- `maxUploadSize` 被夹取为 `min(声明值, 实际执行值)` —— 声明永远不会高于服务端
  真正接受的限制。
- `zrip_levels` 来自 `.zrip_levels(...)`；为 `None` 时忽略级别头，下载固定使用
  `ZRIP_DEFAULT_LEVEL`（1）。设置后服务端会夹取请求级别并回显实际使用的级别。

服务端**不会**做的是：强制它声明的窗口、并发或分块大小。这些是客户端在声明
区间内的决定。声明激进值时请记住：开启 `autoTune: true` 的客户端会真的爬到那里。

该载荷刻意公开（在认证之前被拉取），因此绝不要放入租户、用户或密钥数据。

## 会话清理

被放弃的上传会留下 `.libfw-sess-*` 临时文件与 `.blocks` 边车。启动清理：

```rust,no_run
# use std::sync::Arc;
# use libfw_server::ServerState;
# fn f(state: &Arc<ServerState>) {
// 使用 builder 上的 cleanup_interval / session_ttl
let _handle = state.spawn_stale_session_cleanup();
# }
```

- `cleanup_interval(Duration::ZERO)` 关闭清理并返回一个已完成的任务。
- 清理任务调用 `StorageBackend::cleanup_stale_sessions(ttl)`；日志中的数量是
  **会话**数，不是文件数。
- 不维护长期临时文件的自定义后端可以保留默认实现，直接报告 `0`。

## 部署

**反向代理。** 对 `/file/*` 关闭响应缓冲，保留 `Range` 与 `If-Range`，并允许
`POST /file/*` 的较大请求体。nginx 典型配置：

```nginx
location / {
    proxy_pass http://127.0.0.1:8080;
    proxy_request_buffering off;   # 流式接收上传，不落盘
    proxy_buffering off;           # 流式下发下载 / 区间内容
    client_max_body_size 0;        # 交给 libfw 的 max_upload_size 限制
    proxy_http_version 1.1;
}
```

**HTTP/3。** 本库与传输层无关。在前面放支持 QUIC 的代理/负载均衡器后，每个并行
`Range` GET / 分块 POST 都各占一条 QUIC 流，没有队头阻塞。

**超时。** 空闲/读超时应当大于客户端的 `timeoutMs`（默认 60 秒）加上一个分块
大小所需的传输时间，否则代理会掐断健康的慢速链路。

**CORS。** 只有浏览器 SDK 与 API 不同源时才需要。两个示例服务为开发方便开启了
宽松 CORS；生产环境请收紧，并放行 `Authorization`、`Range`、`If-Range`、
`If-None-Match`、`x-libfw-*` 这些头。

**可观测性。** `libfw-server` 通过 `tracing` 输出上传会话清理与存储失败事件；
可以再加 `tower-http` 的 `TraceLayer` 生成请求 span。不要记录
`Authorization` 头或原始 token。

**安全检查清单。**

- [ ] 替换示例中的固定 token 验证器（见
      [鉴权](./authorization.zh-CN.md)）。
- [ ] 把 `.max_upload_size(...)` 设为磁盘/配额能承受的值。
- [ ] 存储根目录放在 Web 根之外；`FsStorage` 会拒绝穿越，但该目录本身也不应被
      其他服务直接暴露。
- [ ] 把 CORS 限制在自己的源。
- [ ] 目录名敏感时考虑启用 `PathCodec`。
- [ ] 让 `session_ttl` 与客户端合理的续传时间窗匹配。

## 示例

```bash
# 带内嵌 Web UI 的完整 axum 服务（UI 在 /）
cargo run -p axum-server -- data 8080

# 最小 actix-web 集成（仅 API，无 /capabilities）
cargo run -p libfw-actix-server -- data 8081

# 原生 Rust 客户端
cargo run -p rust-client -- --url http://127.0.0.1:8080 --token dev-token ls
```

axum 示例读取以下环境变量：

| 变量 | 默认值 | 含义 |
| --- | --- | --- |
| `LIBFW_PATH_KEY` | 未设置 | 64 个十六进制字符的密钥 → 启用 `EncryptedPathCodec` |
| `LIBFW_CLEANUP_INTERVAL` | `3600` | 清理周期（`30s`、`5m`、`2h`、`1d`） |
| `LIBFW_SESSION_TTL` | `24h` | 被放弃会话临时文件的最大存活时间 |

它还提供 `GET /health`（服务名、版本、协议、存储根目录）与 `GET /sdk/*`
（直接从仓库的 `sdk/` 目录提供），Web UI 就是通过它加载 WASM 引擎的。
