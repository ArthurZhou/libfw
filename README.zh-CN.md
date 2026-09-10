# libfw 中文文档

libfw 是一个面向 Rust 的高性能、低内存流式文件/目录传输库，适用于浏览器和服务端之间的可恢复大文件传输。它使用 Cargo workspace 组织代码，核心能力包括：

- 可恢复传输：`Range` / `ETag` / `If-Range`；刷新页面后也能续传（上传由服务端保留已收分块，下载每 ~4 MiB 落盘一次检查点）
- 自动压缩：基于 `zrip` 的分块 zstd 压缩
- 细粒度鉴权：`Authorization: Bearer <token>`
- 客户端双形态：既能作为原生 Rust crate 使用，也能作为浏览器 SDK（WASM + npm）使用
- 自适应传输：面向好/差网络环境自动调优并发与分块
- 服务器端可嵌入：支持 axum / actix-web

## 仓库结构

```text
crates/
  libfw-core/     共享协议、鉴权、存储、压缩、范围处理
  libfw-server/   可嵌入的 axum 路由和 HTTP 处理逻辑
  libfw-client/   客户端：原生 Rust 传输层 + WASM 引擎（生成 sdk/ 包）
examples/
  axum-server/    带浏览器 UI 的 axum 示例服务器
  actix-server/   actix-web 集成示例（API 参考实现）
  rust-client/    原生 Rust 客户端 CLI（上传/下载/列目录/能力查询）
sdk/              npm 包：ESM + TS 类型 + wasm 文件
```

## 快速开始

### 1. 启动 axum 示例

```bash
cargo run -p axum-server -- data 8080
```

示例默认使用 `dev-token`，根目录为 `./data`，端口为 `8080`。浏览器访问：

- http://127.0.0.1:8080/
- Web UI 会直接嵌入到 `/` 路径上

### 2. 启动 actix 示例

```bash
cargo run -p libfw-actix-server -- data 8081
```

两个示例都接受 `Authorization: Bearer dev-token`，可用于本地开发和验证。

### 3. 直接调用 HTTP API

```bash
# 上传文件：流式上传，支持断点续传
curl -X POST \
  -H "Authorization: Bearer dev-token" \
  -H 'x-libfw-file-meta: eyJwYXRoIjoiZGlyL2EudHh0Iiwic2l6ZSI6MTF9' \
  --data-binary "hello world" \
  http://127.0.0.1:8080/file/dir/a.txt

# 下载指定范围
curl -H "Authorization: Bearer dev-token" \
  -H "Range: bytes=0-4" \
  http://127.0.0.1:8080/file/dir/a.txt

# 列出目录
curl -H "Authorization: Bearer dev-token" \
  http://127.0.0.1:8080/dir/dir
```

## 示例说明

### axum 示例

`examples/axum-server` 是完整示例，包含：

- `/`：内嵌前端 Web UI
- `/health`：服务信息
- `/sdk/*`：静态托管 SDK 资源
- `/file/{*path}`：上传/下载文件接口
- `/dir/{*path}`：目录列表接口
- `/capabilities`：能力声明接口

这个示例还支持 `LIBFW_PATH_KEY` 环境变量；当设置为 64 位十六进制键时，会在服务端启用加密版 shadow path：

```bash
LIBFW_PATH_KEY=$(openssl rand -hex 32) cargo run -p axum-server
```

### actix 示例

`examples/actix-server` 是最小化的 actix-web 集成参考实现，便于你在自己的框架里复用 `libfw_core` 与 `libfw_server` 的能力。

### rust-client 示例（原生 Rust 客户端）

同一个 crate 除了驱动浏览器 SDK，也能在任何非 `wasm32` 目标上当作普通 Rust 依赖使用：`libfw_client::native::NativeClient` 是基于 `tokio` + `reqwest` 的异步传输层，协议与浏览器引擎完全一致（session 上传、`Range`/`ETag` 断点续传、zrip 压缩、基于 `/capabilities` 的自适应调优）。浏览器专有的部分（File System Access API、IndexedDB）改为普通文件读写 + JSON 续传边车文件；调优状态**只保存在内存中**（随该客户端实例存活，供后续传输复用），原生客户端不安装任何持久化，因此 `tuneTtlMs` 对原生端无效（浏览器引擎会缓存调优结果，见英文 README 的 “Adaptive tuning” 一节）。

```toml
[dependencies]
libfw-client = "0.4"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

```rust,no_run
use libfw_client::{ClientConfig, native::NativeClient};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 选项与 SDK 一致；auto_tune 会按网络自适应
    let config = ClientConfig { auto_tune: true, ..ClientConfig::default() };
    let client = NativeClient::new("http://127.0.0.1:8080", "dev-token", config);

    let bytes = client.download_file("docs/plan.pdf", "./plan.pdf").await?;
    println!("已下载 {bytes} 字节（0 表示本地已是最新）");

    client.upload_file("./plan.pdf".as_ref(), "archive/plan.pdf").await?;
    Ok(())
}
```

可直接运行的 CLI（`ls` / `download` / `upload` / `capabilities`，带进度与调优输出）见 `examples/rust-client`：[README](examples/rust-client/README.md) / [中文说明](examples/rust-client/README.zh-CN.md)。

```bash
cargo run -p axum-server -- dev-data 8080   # 终端 1：启动服务端
cargo run -p rust-client -- --url http://127.0.0.1:8080 --token dev-token \
    --auto-tune upload ./big.bin docs/big.bin
```

## 鉴权与路径控制

服务端的处理流程是：

1. 提取 `Authorization: Bearer <token>`
2. 解析 token 成 claims
3. 校验对应 path 和 action
4. 执行 read / write 逻辑

`TokenVerifier` 需要你实现自己的 token 验证逻辑；`PathValidator` 会根据 token 的权限和允许路径决定是否允许访问。`allowed_paths` 视为真实存储路径，不受 shadow path 影响。

核心结构如下：

```rust
pub struct TokenClaims {
    pub sub: String,
    pub exp: Option<i64>,
    pub permissions: Vec<Permission>,
    pub allowed_paths: Vec<String>,
}
```

访问规则：

- `Read` 对应下载
- `Write` 对应上传
- 路径匹配遵循 segment boundary 规则，如 `/docs` 能覆盖 `/docs`、`/docs/a.txt`、`/docs/`，但不会匹配 `/docshop/x`
- root 前缀 `/` 表示整个树可访问

## shadow path（影子路径）

默认情况下，客户端看到的路径和实际存储路径相同；如果你需要隐藏真实目录结构，可以为服务端安装 `PathCodec`。

当前支持：

- `IdentityPathCodec`：默认行为，shadow = real
- `MountPathCodec`：可读别名映射
- `EncryptedPathCodec`：启用 `path-encrypt` 时可用，使用 AES-256-GCM，篡改后的影子路径会被拒绝

`ServerState::resolve_client_path` 会先做形态校验，然后解码 shadow path，再校验真实路径的权限；返回的真实路径只在后端使用，不直接回传给客户端。

## 存储后端

内建的 `FsStorage` 支持本地文件系统，上传会先写入临时文件，成功提交后原子重命名，避免中断时留下半成品文件。其路径会做规范化和防穿越检查，避免 `..`、绝对路径和 NUL 字节攻击。

如果需要接入对象存储、S3 或内存后端，可以实现 `StorageBackend`，其余的 range / ETag / 压缩逻辑仍保持一致。

## 浏览器 SDK

SDK 位于 `sdk/`，它包装了 WASM 引擎、File System Access API 和 IndexedDB。完整说明见 [sdk/README.md](sdk/README.md)。

### 构建 WASM

```bash
wasm-pack build crates/libfw-client --target web --out-dir ../../sdk/pkg --release
```

### 典型用法

```js
import { LibfwClient } from 'libfw-client';

const client = new LibfwClient({
  baseUrl: '/',
  concurrency: 4,
  uploadWindow: 8,
  downloadWindow: 4,
  compress: true,
  onEvent: (e) => console.log(e),
});

await client.downloadFolder('your_token_here');
await client.upload('your_token_here');
```

## 构建与测试

```bash
cargo test --workspace
```

如果你需要重新构建 SDK：

```bash
wasm-pack build crates/libfw-client --target web --out-dir ../../sdk/pkg --release
```

## 备注

- 示例服务器默认使用 HTTP/1.1
- 若需要 HTTP/3，需要在反向代理或负载均衡中启用 QUIC / HTTP/3
- 本项目的业务逻辑集中在 `crates/`，示例和交互层保持分离

## 相关链接

- [README.md](README.md)
- [sdk/README.md](sdk/README.md)
