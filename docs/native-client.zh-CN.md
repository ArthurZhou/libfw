# 原生 Rust 客户端

`libfw-client` 一个 crate 两种形态：

- **原生**（`native::NativeClient`）—— 基于 `tokio` + `reqwest` 的异步实现，
  用于任意非 `wasm32` 目标的二进制、服务与 CLI。
- **浏览器**（通过 `wasm-bindgen` 暴露的 `LibfwClient`）—— `sdk/` 背后的引擎，
  见 [SDK 指南](../sdk/README.md)。

两者线路协议完全一致，共用 `ClientConfig` 与自适应调优引擎。本文介绍原生端。

- [安装](#安装)
- [快速开始](#快速开始)
- [API 参考](#api-参考)
- [配置](#配置)
- [事件](#事件)
- [续传](#续传)
- [取消](#取消)
- [自适应调优](#自适应调优)
- [错误](#错误)
- [并发与共享](#并发与共享)
- [示例 CLI](#示例-cli)

## 安装

```toml
[dependencies]
libfw-client = "0.4"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

`reqwest`（`rustls-tls-native-roots`、HTTP/2）与 `tokio` 只在非 `wasm32` 目标
引入，因此同一份 manifest 仍可用 `wasm-pack` 构建 WASM 引擎。

## 快速开始

```rust,no_run
use libfw_client::{ClientConfig, native::NativeClient};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ClientConfig {
        auto_tune: true,
        ..ClientConfig::default()
    };
    let client = NativeClient::new("http://127.0.0.1:8080", "dev-token", config);

    for entry in client.list("").await? {
        println!("{} {} {}", if entry.is_dir { "d" } else { "f" }, entry.size, entry.path);
    }

    let written = client.download_file("docs/plan.pdf", "./plan.pdf").await?;
    println!("写出 {written} 字节（0 表示本地已是最新）");

    client.upload_file("./plan.pdf".as_ref(), "archive/plan.pdf").await?;
    Ok(())
}
```

## API 参考

### 构造

| 方法 | 说明 |
| --- | --- |
| `NativeClient::new(base_url, token, ClientConfig)` | 显式配置 |
| `NativeClient::with_config(NativeConfig)` | 完整配置，含事件回调 |
| `client.config() -> &ClientConfig` | 当前配置 |
| `client.capabilities() -> Option<Capabilities>` | 调优引擎最近加载的 `/capabilities` |
| `client.fetch_capabilities() -> Result<Capabilities>` | 真正请求 `GET /capabilities` |
| `client.tune_status() -> Option<NativeTuneStatus>` | `{ phase, params, stats, caps_hash }`，未开启调优时为 `None` |

### 文件与目录

| 方法 | 返回 | 说明 |
| --- | --- | --- |
| `list(dir: &str)` | `Vec<RemoteFile>` | 直接子项；`""` 为根 |
| `stat(remote)` | `RemoteStat { size, etag }` | 目录返回 `404` |
| `download_file(remote, dest)` | 本次写入字节数 | 支持续传；已完成时为 `0` |
| `download_folder(dir, dest_dir)` | 本次写入字节数 | 在 `dest_dir` 下保留虚拟结构 |
| `upload_file(local: &Path, remote)` | 本次发送字节数 | 按服务端已收区间续传 |
| `upload_files(Vec<(PathBuf, String)>)` | 本次发送字节数 | 以 `concurrency` 并行 |
| `upload_folder(local_dir, remote_dir)` | 本次发送字节数 | 镜像目录树；跳过软链 |

`download_folder` 与 `upload_folder` 保留层级：把 `tree` 下载到 `./out` 会写出
`./out/tree/...`；把 `./src` 上传到 `backup` 会写到 `backup/<相对路径>`。

### 重试与续传内部行为

- 下载先 `HEAD`（权威 ETag + 大小），再把剩余字节按有界区间拉取。即使窗口为 1，
  超过 512 KiB 的文件也会分块下载，且每批开始前都会重新读取当前窗口/分块大小
  —— 传输途中的爬坡会作用到正在下载的文件。
- 续传过程中收到 `200`/`416` 说明远端已变化或变小；客户端会记录并从头开始。
- 上传先探测会话，只补发缺口，然后提交。

## 配置

`ClientConfig` 与 SDK 选项一一对应：

| 字段 | 默认值 | 含义 |
| --- | --- | --- |
| `concurrency` | 4 | 并行传输的文件数 |
| `upload_window` | 8 | 单文件上传的在途分块数 |
| `download_window` | 4 | 单文件下载的在途区间请求数 |
| `compress` | `true` | 协商 zrip 压缩 |
| `compress_level` | `CompressLevel::Balanced` | `Auto` / `Fast` / `Balanced` / `Max` / `Fixed(i32)` |
| `chunk_size` | 2 MiB | 上传分块与下载区间的共享大小 |
| `max_retries` | 3 | 每块/每文件重试次数 |
| `base_retry_delay_ms` | 500 | 初始指数退避 |
| `max_retry_delay_ms` | 30 000 | 退避上限 |
| `timeout_ms` | 60 000 | 单请求停滞超时（不是整传输期限） |
| `auto_tune` | `false` | 开启自适应爬坡 |
| `tune_ttl_ms` | 1 小时 | 原生端无效（只存内存，见下） |

```rust,no_run
use libfw_client::{ClientConfig, CompressLevel};

let config = ClientConfig {
    concurrency: 8,
    upload_window: 16,
    download_window: 8,
    compress_level: CompressLevel::Max,
    timeout_ms: 120_000,
    auto_tune: true,
    ..ClientConfig::default()
};
```

`NativeConfig` 在其上再加端点与可选事件回调：

```rust,no_run
use std::sync::Arc;
use libfw_client::{ClientConfig, native::{NativeClient, NativeConfig, NativeEvent}};

let client = NativeClient::with_config(
    NativeConfig::new("https://files.example.com", "token")
        .with_client(ClientConfig { auto_tune: true, ..ClientConfig::default() })
        .with_events(Arc::new(|event: NativeEvent| match event {
            NativeEvent::Progress { done, total } => eprintln!("{done}/{total}"),
            NativeEvent::Tuning(t) => eprintln!("tuning: {:?} {:?}", t.phase, t.params),
            _ => {}
        })),
);
```

`NativeConfig` 的 `Debug` 实现会隐去 token。

## 事件

`NativeEvent` 通过 `NativeConfig::with_events` 传入的回调投递：

| 变体 | 字段 |
| --- | --- |
| `FileStart` | `path`、`size` |
| `Progress` | `done`、`total`（当前传输的累计值） |
| `FileDone` | `path`、`bytes` |
| `Tuning(TuneEvent)` | `phase`、`params`、`stats`（仅 `auto_tune`） |
| `Log(String)` | 重试、续传决策、重启提示 |

回调类型是 `Arc<dyn Fn(NativeEvent) + Send + Sync>`，可能从任意传输任务调用。
请保持轻量、不阻塞（需要 IO 时转发到 channel）。

## 续传

下载续传状态是目标文件旁的 JSON 边车：

```
./plan.pdf
./plan.pdf.libfw-resume.json    { "etag": "\"1234-1710000000\"", "offset": 4194304, "size": 12345678 }
```

- 边车原子写入（先写临时文件再重命名），最多每 4 MiB 一次。
- 下次运行先 `HEAD`；ETag 一致则带 `If-Range` 从 `offset` 继续，否则从 0 开始。
- 删除边车会强制完整重新下载；删除目标文件同样从 0 开始（没有可续的本地数据）。
- 下载完成后边车会被删除。

上传续传在服务端：会话 id 即文件 ETag（size + mtime），因此中断的上传能从
服务端的 `.libfw-sess-*` 临时文件继续，而被编辑过的文件会开启新会话。
**已提交**的上传会删除区间边车，所以再次上传同一文件会重发（位置写入保证幂等）。

## 取消

没有 `pause()` / `cancel()` 句柄。取消是结构性的 —— 直接 drop future，或给它
加上边界：

```rust,no_run
# use libfw_client::native::NativeClient;
# async fn f(client: &NativeClient) -> Result<(), Box<dyn std::error::Error>> {
use std::time::Duration;

let bytes = tokio::time::timeout(
    Duration::from_secs(30),
    client.download_file("big.bin", "./big.bin"),
)
.await??;

// 或：tokio::select! { _ = shutdown.cancelled() => {}, r = transfer => { r?; } }
# Ok(())
# }
```

两个方向都支持续传，因此放弃传输最多损失在途的几个块。下载可能丢失上次检查点
之后最多约 4 MiB；上传会在服务端留下可续传的临时文件。

## 自适应调优

开启 `auto_tune: true` 后，客户端先读取 `/capabilities`，从声明的最小值开始爬坡：

1. 单文件窗口；
2. 跨文件并发，

使用 1 秒 EWMA 的 RTT/吞吐窗口。分块大小**不参与**爬坡：它跟随实测吞吐
（约 100 ms 的数据量，向下取整到 64 KiB，并夹在声明区间与客户端硬上限内）。
zrip 级别来自 `compress_level` 策略，永远不是爬坡维度。出错时参数减半
（`degraded`），并需连续两个稳定窗口后才收敛。

状态生命周期：

- **只在内存**，随客户端实例存活。同一客户端的后续传输会复用收敛结果（一个
  多文件目录不会每传一个文件重新爬坡），传输失败时丢弃。
- **不持久化。** 新进程会重新测量链路，`tune_ttl_ms` 在这里无效；在
  `localStorage` 中缓存的是浏览器引擎。
- 服务端没有 `/capabilities` 时会关闭调优并退回静态配置。

读取实时状态：

```rust,no_run
# use libfw_client::native::NativeClient;
# fn f(client: &NativeClient) {
if let Some(status) = client.tune_status() {
    println!("{:?} {:?} {:?}", status.phase, status.params, status.stats);
}
# }
```

很短的传输可能在 1 秒窗口结束前就完成了，此时会停在 `ramping` 且参数为声明的
最小值 —— 这是预期行为。

## 错误

所有可能失败的方法都返回 `Result<T, LibfwError>`：

| 变体 | 含义 |
| --- | --- |
| `Http { status, url }` | HTTP 状态非成功 |
| `Network(String)` | 传输失败 |
| `Compress` / `Decompress` | 编解码失败 |
| `Protocol(String)` | 对端数据格式错误（JSON 非法、路径不安全） |
| `Storage(String)` | 本地文件 IO 失败 |
| `Cancelled` | 传输已取消 |
| `Js(String)` | 仅 WASM 使用，原生端不会产生 |
| `Other(String)` | 其他 |

`Http { status: 404 }` 也是 `stat`/`list` 表达"不存在"的方式，同时用于识别
没有 `/capabilities` 的服务端。

## 并发与共享

所有传输方法都接收 `&self`，因此一个客户端可以被共享：

```rust,no_run
# use std::sync::Arc;
# use libfw_client::native::NativeClient;
# async fn f(client: NativeClient) -> Result<(), Box<dyn std::error::Error>> {
let client = Arc::new(client);
let a = { let c = client.clone(); tokio::spawn(async move { c.download_file("a.bin", "./a.bin").await }) };
let b = { let c = client.clone(); tokio::spawn(async move { c.download_file("b.bin", "./b.bin").await }) };
let _ = tokio::try_join!(a, b)?;
# Ok(())
# }
```

一个客户端 = 一个 `reqwest` 连接池 + 一个调优引擎。连接池按 `concurrency`
定容，因此同一个旋钮约束了总网络并行度。`download_folder` / `upload_files`
内部已经按该预算并行；请用 `Arc<NativeClient>` 而不是每文件一个客户端，以共享
连接池与调优状态。

## 示例 CLI

`examples/rust-client` 是可运行的 CLI（`ls`、`download`、`upload`、
`capabilities`）。见[它的 README](../examples/rust-client/README.zh-CN.md)。

```bash
cargo run -p axum-server -- dev-data 8080
cargo run -p rust-client -- --url http://127.0.0.1:8080 --token dev-token \
    --auto-tune upload ./big.bin docs/big.bin
```
