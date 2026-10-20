# libfw

面向 Rust 的流式文件/目录传输库：可续传的 HTTP 传输、并行字节区间下载、并发分块
上传、分块 zstd 压缩与自适应调优，并自带浏览器（WASM）与原生 Rust 两种客户端。

```text
            ┌──────────────── libfw-core（契约层）───────────────────┐
            │ claims · verifier · validator · storage · ranges · codec│
            └───────────────┬───────────────────────┬────────────────┘
                            │                       │
        ┌───────────────────▼──────────┐  ┌─────────▼──────────────────┐
        │ libfw-server（axum 路由）    │  │ libfw-client               │
        │ FsStorage · Range/ETag · zrip│  │ native(reqwest) + wasm     │
        └──────────────────────────────┘  └─────────┬──────────────────┘
                                                    │
                                          sdk/（npm，ESM + WASM）
```

- **双向可续传。** 下载按校验过的 ETag/偏移继续；上传从服务端共享临时文件继续，
  只补发缺口。
- **按请求并行。** 下载是 N 个并发 `Range` GET，上传是 M 个并发分块 POST ——
  单个请求卡住不会阻塞其他请求。
- **内存恒定。** 两端都使用有界窗口 + 64 KiB 读缓冲；上传通过原子重命名提交。
- **默认开启压缩。** `zrip`（zstd）分帧，按传输协商；级别由服务端夹取并回显。
- **鉴权可插拔。** `TokenVerifier` + `Validator` 两个 trait；libfw 不签发 token。
- **存储可插拔。** `StorageBackend` + `UploadSink`；内置 `FsStorage`。
- **可选路径隐私。** `PathCodec` 用可读别名或 AES-256-GCM 影子路径隐藏真实目录。
- **自适应调优。** 基于真实传输的 TCP 式爬坡，由公开的 `/capabilities` 契约驱动。

需要 Rust 2024 edition（1.85+）。浏览器的文件夹读写依赖 File System Access API，
因此需要 Chromium 内核浏览器，在不支持 File System Access API 的浏览器客户端上，将会使用先下载到内存再压缩成单文件下载的方式。

## 文档

| 指南 | 中文 | English |
| --- | --- | --- |
| 概览、快速开始、功能地图 | 本文件 | [README.md](./README.md) |
| **鉴权 / 自定义验证单元** | [docs/authorization.zh-CN.md](./docs/authorization.zh-CN.md) | [docs/authorization.md](./docs/authorization.md) |
| 服务端集成 | [docs/server-integration.zh-CN.md](./docs/server-integration.zh-CN.md) | [docs/server-integration.md](./docs/server-integration.md) |
| HTTP 协议参考 | [docs/protocol.zh-CN.md](./docs/protocol.zh-CN.md) | [docs/protocol.md](./docs/protocol.md) |
| 原生 Rust 客户端 | [docs/native-client.zh-CN.md](./docs/native-client.zh-CN.md) | [docs/native-client.md](./docs/native-client.md) |
| 浏览器 SDK | [sdk/README.md](./sdk/README.md) | — |
| Rust 客户端 CLI 示例 | [examples/rust-client/README.zh-CN.md](./examples/rust-client/README.zh-CN.md) | [English](./examples/rust-client/README.md) |
| axum 示例服务 | [examples/axum-server/README.zh-CN.md](./examples/axum-server/README.zh-CN.md) | [English](./examples/axum-server/README.md) |
| API 文档 | [docs.rs/libfw-core](https://docs.rs/libfw-core) · [libfw-server](https://docs.rs/libfw-server) · [libfw-client](https://docs.rs/libfw-client) | — |

指南索引见 [docs/README.md](./docs/README.md)。

## 仓库结构

```text
crates/
  libfw-core/      claims、鉴权 trait、存储 trait、压缩、区间、协议常量、
                   路径 codec、能力声明
  libfw-server/    axum 路由 + handler、FsStorage、会话临时文件处理
  libfw-client/    客户端引擎：native（reqwest）+ wasm（wasm-bindgen）
examples/
  axum-server/     完整示例：API + 内嵌 Web UI（`/`）
  actix-server/    最小 actix-web 集成（仅 API）
  rust-client/     原生 CLI：ls / download / upload / capabilities
sdk/               libfw-client npm 包（ESM + TS 类型 + wasm）
docs/              指南（服务端、鉴权、协议、原生客户端）
tests/            Playwright 端到端用例 + Node 单测
```

## 快速开始

```bash
# 1.（可选，Web UI 需要）构建一次 WASM 引擎
wasm-pack build crates/libfw-client --target web --out-dir ../../sdk/pkg --release

# 2. 启动示例服务：存储根目录 `data`，端口 8080，token `dev-token`
cargo run -p axum-server -- data 8080
```

打开 <http://127.0.0.1:8080/> 即可使用 Web UI（浏览、上传、下载文件与目录，实时
进度，支持暂停/继续/取消）。不构建 UI 也能直接调用 REST API：

```bash
BASE=http://127.0.0.1:8080
TOKEN=dev-token

# 能力声明（公开，无需 token）
curl -s "$BASE/capabilities"

# 上传一个小文件（meta = {"path","size"} 的 base64）
META=$(printf '{"path":"dir/a.txt","size":11}' | base64 -w0)
curl -X POST "$BASE/file/dir/a.txt" \
  -H "Authorization: Bearer $TOKEN" \
  -H "x-libfw-file-meta: $META" \
  --data-binary "hello world"

# 区间下载
curl -H "Authorization: Bearer $TOKEN" -H 'Range: bytes=0-4' \
  "$BASE/file/dir/a.txt"

# 目录列表
curl -H "Authorization: Bearer $TOKEN" "$BASE/dir/dir"
```

`dev-test.bat`（Windows）/ `dev-test.sh`（Linux、macOS）会执行
`cargo test --workspace`、启动示例服务并打开 UI。

## 作为服务端使用

```rust
use std::sync::Arc;
use libfw_core::auth::{AuthError, PathValidator, TokenVerifier};
use libfw_core::claims::{Permission, TokenClaims};
use libfw_server::{router, FsStorage, ServerState};

#[derive(Clone)]
struct MyVerifier;

impl TokenVerifier for MyVerifier {
    fn verify(&self, token: &str) -> Result<TokenClaims, AuthError> {
        // 生产环境换成 JWT / HMAC / 内省，见 docs/authorization.zh-CN.md
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

let state = Arc::new(
    ServerState::builder()
        .storage(FsStorage::new("/srv/files"))
        .verifier(MyVerifier)
        .validator(PathValidator::new())
        .build(),
);

// GET|HEAD|POST /file/{*path}、GET /dir[/{*path}]、GET /capabilities
let app = router(state);
```

用 `.nest("/api", router(state))` 挂到子路径。builder 还支持
`.path_codec(..)`、`.compression(..)`、`.max_upload_size(..)`、
`.limits(..)`、`.zrip_levels(..)`、`.cleanup_interval(..)` 与
`.session_ttl(..)`。完整说明见
[docs/server-integration.zh-CN.md](./docs/server-integration.zh-CN.md)。

非 axum 框架可以直接复用 `libfw-core` 的契约，以及 `libfw-server` 导出的
区间/ETag 辅助函数；`examples/actix-server` 是完整的 actix-web 实现。

## 鉴权

服务端先提取 `Authorization: Bearer <token>`，交给你的 `TokenVerifier` 得到
`TokenClaims`，再交给 `Validator` 判断该 claims 能否在目标路径上执行该动作。

```rust
pub struct TokenClaims {
    pub sub: String,                  // 主体 —— 同时决定上传会话隔离
    pub exp: Option<i64>,             // unix 过期时间，None = 永不过期
    pub permissions: Vec<Permission>, // Read | Write
    pub allowed_paths: Vec<String>,   // 真实存储路径上的前缀
}
```

内置 `PathValidator` 要求 token 未过期、拥有动作所需权限，且路径位于
`allowed_paths` 之一（按段边界匹配：`/docs` 匹配 `/docs/a.txt`，不匹配
`/docshop/x`）。分组 ACL、扩展名规则、只读挂载等场景请实现自定义 `Validator`。

**示例里的固定 `dev-token` 只是占位，不是 API。** 真正的扩展点是
`TokenVerifier` trait —— 按
[JWT、HMAC、JWKS 或远程内省](./docs/authorization.zh-CN.md) 实现一次即可。
该指南包含可直接复制的验证器实现、自定义 validator、`/introspect` 缓存策略、
进程内集成测试与上线检查清单。

## 存储与影子路径

`FsStorage` 会规范化并校验路径（拒绝 `..`、绝对路径、NUL 与软链逃逸），上传
先写临时文件，提交时原子重命名。会话上传按位置写入共享临时文件，并维护
`.blocks` 区间边车，按 token 主体隔离。

实现 `StorageBackend` 即可接入 S3、对象存储或内存；服务端其余部分（区间、ETag、
压缩）保持不变。`UploadSink` 有合理的默认实现：即使后端无法报告已收区间也能
工作，只是续传时会重发。

目录名敏感时，可以安装 `PathCodec`：`MountPathCodec` 提供可读别名，
`EncryptedPathCodec`（feature `path-encrypt`）提供不透明的 AES-256-GCM 影子
路径。客户端只会看到影子路径，而鉴权仍作用在解码后的真实路径上。详见
[docs/server-integration.zh-CN.md](./docs/server-integration.zh-CN.md#路径转换影子路径)。

## 作为 Rust 客户端使用

驱动浏览器 SDK 的同一个 crate 也能直接在原生环境使用：

```toml
[dependencies]
libfw-client = "0.4"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

```rust
use libfw_client::{ClientConfig, native::NativeClient};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ClientConfig { auto_tune: true, ..ClientConfig::default() };
    let client = NativeClient::new("http://127.0.0.1:8080", "dev-token", config);

    for entry in client.list("").await? {
        println!("{} {}", if entry.is_dir { "dir " } else { "file" }, entry.path);
    }

    let bytes = client.download_file("docs/plan.pdf", "./plan.pdf").await?;
    println!("已下载 {bytes} 字节（0 表示本地已是最新）");

    client.upload_file("./plan.pdf".as_ref(), "archive/plan.pdf").await?;
    Ok(())
}
```

- 下载续传使用目标文件旁的 JSON 边车（`<dest>.libfw-resume.json`），约每 4 MiB
  原子写入一次。
- 上传续传使用 tus 风格的会话协议，以服务端为准。
- 调优结果在同一客户端的多次传输间于内存中复用；不做持久化，新进程会重新测量。
- 取消是结构性的：drop future，或用 `tokio::time::timeout` 包一层。
- 一个客户端 = 一个连接池 + 一个调优引擎 —— 用 `Arc<NativeClient>` 共享它。

API 参考：[docs/native-client.zh-CN.md](./docs/native-client.zh-CN.md)。可运行
CLI：[examples/rust-client](./examples/rust-client/README.zh-CN.md)。

## 在浏览器中使用

```bash
wasm-pack build crates/libfw-client --target web --out-dir ../../sdk/pkg --release
npm --prefix sdk run build:umd   # 可选 UMD 打包
```

```js
import { LibfwClient } from 'libfw-client';

const client = new LibfwClient({
  baseUrl: '/',
  autoTune: true,
  onEvent: (e) => {
    if (e.type === 'progress') console.log(e.done, '/', e.total);
    if (e.type === 'tuning') console.log(e.phase, e.params, e.stats);
  },
});

await client.downloadFolder('your_token_here'); // 选择本地目录
await client.upload('your_token_here', fileInput.files);
```

WASM 实例化、File System Access API、IndexedDB 续传状态、不支持时的 ZIP 兜底，
以及硬刷新后的下载检查点，全部由 SDK 负责。完整选项/API 见
[sdk/README.md](./sdk/README.md)。

## 协议一览

| 方法 | 路由 | 用途 |
| --- | --- | --- |
| `GET` / `HEAD` | `/file/{*path}` | 下载（`Range`、`ETag`、`If-Range`、zrip）/ 元数据 |
| `POST` | `/file/{*path}` | 上传分块 / 会话提交 |
| `GET` | `/dir[/{*path}]` | JSON 目录列表 |
| `GET` | `/capabilities` | 公开的调优契约 |

关键头：`x-libfw-protocol`、`x-libfw-file-meta`（base64 JSON）、
`x-libfw-offset`、`x-libfw-session`、`x-libfw-session-status`、
`x-libfw-final`、`x-libfw-compress`、`x-libfw-compress-level`。

上传是 tus 风格：先探测服务端已持有哪些字节区间，只 POST 缺口（可乱序、按位置
写入），最后带大小与覆盖校验提交。下载是并行 `Range` GET，在客户端内重排，每个
区间独立重试。

完整细节（含会话算法与状态码表）：[docs/protocol.zh-CN.md](./docs/protocol.zh-CN.md)。

## 自适应调优

开启 `autoTune` / `auto_tune` 后，客户端读取 `/capabilities`，从声明的最小值
开始，先爬升单文件窗口，再爬升跨文件并发，使用 1 秒 EWMA 的 RTT 与吞吐采样。
分块大小不是爬坡维度，而是跟随实测吞吐（约 100 ms 的数据量，夹在声明区间内）；
zrip 级别来自客户端策略。出错时参数减半，并需稳定窗口后才收敛。

浏览器引擎会把收敛结果按源与方向缓存到 `localStorage`（`tuneTtlMs`，默认 1
小时，`0` 关闭）；原生客户端只存内存。实时状态可通过 `tuneStatus()` /
`tune_status()` 与 `tuning` 事件获取。

## 构建与文档

```bash
cargo build --workspace                       # 原生目标
cargo doc --no-deps --workspace --open        # rustdoc
wasm-pack build crates/libfw-client --target web --out-dir ../../sdk/pkg --release
npm --prefix sdk run build:umd                # 可选 UMD
```

三个发布 crate 都带有 `[package.metadata.docs.rs]`（`all-features = true`），
且 rustdoc 构建无警告，因此 [docs.rs](https://docs.rs/libfw-core) 会渲染完整
API，包括 feature 开关后的 `EncryptedPathCodec`。

`libfw-client` 在 docs.rs 上按原生目标构建：包含 `native` 模块；仅 `wasm32` 的
模块，其 JavaScript 接口见 [sdk/README.md](./sdk/README.md) 与
`sdk/index.d.ts`。

## 测试

```bash
cargo test --workspace          # 单测 + 集成测试（含在临时端口上启动真实服务端）
npm run test:unit               # Node 单测（ZIP 写入、UTF-8 文件名）
```

浏览器端到端用例在 `tests/e2e/`，基于 Playwright 连接真实示例服务运行：

| 用例 | 覆盖内容 |
| --- | --- |
| `npm run e2e:matrix` | 完整上传/下载矩阵 |
| `npm run e2e:resume` | 中断后继续上传 |
| `npm run e2e:resume-fs` | fs 模式下载续传（注入 OPFS 目录） |
| `npm run e2e:download` | 并行区间、下载中进度、实时统计 |
| `npm run e2e:tune` | 调优面板与调优事件 |
| `npm run e2e:tune-cache` | `localStorage` 收敛复用、TTL 过期、`tuneTtlMs: 0` |
| `npm run e2e:refresh` | 硬刷新后双向续传 |

```bash
# 终端 1
cargo run -p axum-server -- dev-data 8080
# 终端 2
npx playwright install chromium        # 仅一次
LIBFW_DATA=dev-data node tests/e2e/upload-matrix.cjs
```

| 变量 | 默认值 | 含义 |
| --- | --- | --- |
| `BASE` | `http://127.0.0.1:8080` | 示例服务地址 |
| `TOKEN` | `dev-token` | bearer token |
| `LIBFW_DATA` | 系统临时目录 | **服务端**的存储根（续传用例会检查它） |
| `LIBFW_TMP` | 系统临时目录 | 用例的临时文件目录 |
| `CHROME` / `BROWSER_CHANNEL` | Playwright 自带 Chromium | 指定浏览器二进制或 channel |

## 版本与兼容

- 线路协议通过 `x-libfw-protocol: libfw/1` 版本化；版本不匹配时快速失败并返回
  `426`，而不是静默损坏传输。
- `/capabilities` 声明的是建议区间而非命令：客户端负责夹取，服务端不强制窗口或
  分块大小；`maxUploadSize` 始终被夹到服务端真正接受的上限。
- Feature：`libfw-core/path-encrypt`（由 `libfw-server/path-encrypt` 转发）
  启用 `EncryptedPathCodec`。

## 许可证

MIT
