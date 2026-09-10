# libfw Rust 客户端示例

这是一个最小的 **原生 Rust 客户端**：不用浏览器、不用 WASM、不用 npm，只用
`tokio` + `reqwest` 走与浏览器 SDK 完全相同的线路协议。

它演示了把客户端嵌进自己程序时需要的三件事：

* 构造 `ClientConfig`（与 SDK 选项一一对应）；
* 安装事件回调，接收进度 / 自适应调优更新；
* 调用异步传输方法。

## 运行

先启动任意 libfw 服务端，最省事的是仓库自带的 axum 示例：

```bash
# 终端 1 —— 服务端监听 :8080，token 为 dev-token
cargo run -p axum-server -- dev-data 8080

# 终端 2 —— 本示例（逐次传参，或先导出 LIBFW_URL / LIBFW_TOKEN）
cargo run -p rust-client -- --url http://127.0.0.1:8080 --token dev-token ls
cargo run -p rust-client -- --url http://127.0.0.1:8080 --token dev-token upload ./Cargo.toml uploads/Cargo.toml
cargo run -p rust-client -- --url http://127.0.0.1:8080 --token dev-token download uploads/Cargo.toml ./Cargo.toml.bak
cargo run -p rust-client -- --url http://127.0.0.1:8080 --token dev-token capabilities
```

服务地址与 token 默认读环境变量 `LIBFW_URL` / `LIBFW_TOKEN`，也可用
`--url` / `--token` 传入。

## 命令

| 命令 | 说明 |
| --- | --- |
| `ls [DIR]` | 列出服务端目录（`GET /dir/...`）。 |
| `download <REMOTE> <DEST>` | 下载单个文件或整个目录树（用 `stat` 判断：目录对 `HEAD` 返回 404）。 |
| `upload <LOCAL> [REMOTE]` | 上传单个文件或整个目录树（保留目录结构）。返回本次实际发送的字节数——服务端已完整持有该文件时为 `0`。 |
| `capabilities` | 打印服务端 `/capabilities`（自适应引擎协商所依据的契约）。 |

## 选项

```
--url <URL>                  服务端地址                 (环境变量 LIBFW_URL)
--token <TOKEN>              Bearer token              (环境变量 LIBFW_TOKEN)
--concurrency <N>            并发文件数                 (默认 4)
--window <N>                 单文件在途窗口             (默认上传 8 / 下载 4)
--chunk-size <BYTES>         共享分块大小               (默认 2 MiB)
--max-retries <N>            每块重试次数               (默认 3)
--timeout-ms <MS>            停滞超时                   (默认 60000)
--compress-level <POLICY>    auto|fast|balanced|max|N  (默认 balanced)
--no-compress                关闭 zrip 压缩
--auto-tune                  依据 /capabilities 自适应
--quiet, -q                  不打印进度
```

## 客户端替你做了什么

* **可断点续传的下载** —— `HEAD` 拿到权威的大小与 ETag，目标文件旁的 JSON 边车文件
  记录已完成的偏移，重传只取剩余区间；已下载完成的文件再次下载是空操作。
* **可断点续传的上传** —— tus 风格 *session* 协议：先探测服务端已持有的区间，只补发缺口。
* **并行传输** —— 每文件 `--window` 个并发 `Range` GET，同时 `--concurrency` 个文件在途，
  单文件吞吐由链路带宽决定，而不是被单连接的 `chunkSize / RTT` 限制。超过 512 KiB 的文件
  即使窗口为 1 也会被切成有界区间分批拉取，且每批开始前重新读取当前的窗口 / 分块大小，
  因此传输途中的调优爬坡会立刻作用到正在下载的文件（过去"整文件一个 206"既无法断点续传，
  也无法并行或调优）。
* **压缩** —— 双向 zrip(zstd) 分帧；`--compress-level auto` 会用首个上传文件的真实样本
  在服务端播报的区间内实测，按"省下的字节 vs 花掉的 CPU"取最优档位。
* **自适应调优** —— 加 `--auto-tune` 后，引擎先读取服务端播报的上限，用最小配置
  先探一次链路，再以 TCP 慢启动方式逐步抬升窗口 / 并发直到饱和。
  分块大小不参与爬坡，而是**跟随实测带宽**（约等于当前吞吐的 100 ms 数据量，
  并夹在服务端播报区间与内存预算之内）：带宽越大、分块越大、请求越少。
  收敛结果会被**同一客户端**的后续传输复用（整个目录多文件不会每传一个文件重新爬坡）；
  原生客户端不落盘、也不跨进程复用，新进程总是从播报的最小配置重新测量链路
  （浏览器引擎会把收敛结果缓存到 `localStorage`，见 SDK 的 `tuneTtlMs`）。

## 嵌入到自己的项目

```rust
use libfw_client::{ClientConfig, native::{NativeClient, NativeConfig, NativeEvent}};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ClientConfig { auto_tune: true, ..ClientConfig::default() };
    let client = NativeClient::with_config(
        NativeConfig::new("https://files.example.com", "your-token")
            .with_client(config)
            .with_events(std::sync::Arc::new(|event: NativeEvent| {
                if let NativeEvent::Progress { done, total } = event {
                    eprintln!("{done}/{total}");
                }
            })),
    );

    let bytes = client.download_file("reports/2026.pdf", "./2026.pdf").await?;
    println!("下载 {bytes} 字节（0 表示本地已是最新）");

    client.upload_file("./2026.pdf".as_ref(), "archive/2026.pdf").await?;
    Ok(())
}
```

## 说明

* 下载会在 `DEST` 之下保留**虚拟路径**结构，与浏览器 SDK 行为一致：
  `download tree ./out` 会写出 `./out/tree/...`。
* 上传提交成功后服务端会删除区间边车文件，因此对同一文件再次上传会重发一次
  （位置写入保证幂等）；断点续传针对的是**被中断**的上传。
* 很短的传输可能在 1 秒测量窗口关闭前就结束了，此时调优阶段会停在 `ramping`
  且参数为播报的最小值——这是预期行为，不是错误。
* 没有 `pause()`/`cancel()` 句柄：取消是结构性的，直接 drop 传输 future 即可
  （或用 `tokio::select!` / `tokio::time::timeout` 包裹）。两个方向都支持续传，
  因此放弃传输最多损失在途若个数据块。

实现见 [`crates/libfw-client/src/native/`](../../crates/libfw-client/src/native/)，
集成测试见 `crates/libfw-client/tests/native_client.rs`（会在 TCP 上启动真实的
libfw 服务端）。
