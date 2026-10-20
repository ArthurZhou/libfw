# libfw Rust 客户端示例

一个最小的**原生 Rust 客户端**：不用浏览器、不用 WASM、不用 npm，只用
`tokio` + `reqwest` 走与浏览器 SDK 完全相同的线路协议。它演示把客户端嵌进自己
程序时需要的三件事：

* 构造 `ClientConfig`（与 SDK 选项一一对应）；
* 安装事件回调，接收进度与自适应调优更新；
* 调用异步传输方法。

完整客户端参考：[docs/native-client.zh-CN.md](../../docs/native-client.zh-CN.md)。

## 运行

先启动任意 libfw 服务端，最省事的是仓库自带的 axum 示例：

```bash
# 终端 1 —— 服务端监听 :8080，token 为 dev-token
cargo run -p axum-server -- dev-data 8080

# 终端 2 —— 本示例
cargo run -p rust-client -- --url http://127.0.0.1:8080 --token dev-token ls
cargo run -p rust-client -- --url http://127.0.0.1:8080 --token dev-token \
    upload ./Cargo.toml uploads/Cargo.toml
cargo run -p rust-client -- --url http://127.0.0.1:8080 --token dev-token \
    download uploads/Cargo.toml ./Cargo.toml.bak
cargo run -p rust-client -- --url http://127.0.0.1:8080 --token dev-token capabilities
```

服务地址与 token 默认读环境变量 `LIBFW_URL` / `LIBFW_TOKEN`，也可用
`--url` / `--token` 传入。

## 命令

| 命令 | 说明 |
| --- | --- |
| `ls [DIR]` | 列出服务端目录（`GET /dir/...`） |
| `download <REMOTE> <DEST>` | 下载单个文件或整个目录树（`HEAD` 返回 404 即为目录） |
| `upload <LOCAL> [REMOTE]` | 上传单个文件或整个目录树；`REMOTE` 缺省为本地名字 |
| `capabilities` | 打印 `/capabilities`（调优引擎协商所依据的契约） |

`download` 与 `upload` 会自动判断文件/目录，并打印**本次实际移动**的字节数 ——
`0` 表示服务端/本地已是最新。

## 选项

```
--url <URL>                  服务端地址                 (环境变量 LIBFW_URL)
--token <TOKEN>              Bearer token              (环境变量 LIBFW_TOKEN)
--concurrency <N>            并发文件数                 (默认 4)
--window <N>                 单文件在途窗口（双向）
                             (未指定时默认上传 8 / 下载 4)
--chunk-size <BYTES>         共享分块大小               (默认 2 MiB)
--max-retries <N>            每块重试次数               (默认 3)
--timeout-ms <MS>            停滞超时                   (默认 60000)
--compress-level <POLICY>    auto|fast|balanced|max|N  (默认 balanced)
--no-compress                关闭 zrip 压缩
--auto-tune                  依据 /capabilities 自适应
--quiet, -q                  不打印进度
--help, -h                   用法
```

`--window` 会同时设置 `upload_window` 与 `download_window`；不指定时使用库默认值
（8 与 4）。

## 客户端替你做了什么

* **可续传的下载** —— `HEAD` 拿到权威大小与 ETag，目标文件旁的 JSON 边车记录
  进度，重传只取剩余字节。已完成的下载再次执行是空操作。
* **可续传的上传** —— tus 风格会话协议：先探测服务端已持有哪些区间，只补发缺口。
* **并行传输** —— 每文件 `--window` 个并发区间 GET，同时 `--concurrency` 个文件
  在途，单文件吞吐由链路带宽决定，而不是被单连接的 `chunkSize / RTT` 限制。
  超过 512 KiB 的文件即使窗口为 1 也会分块，且每批开始前重新读取当前窗口/分块
  大小，因此传输途中的爬坡会作用到正在下载的文件。
* **压缩** —— 双向 zrip（zstd）分帧。`--compress-level auto` 会用首个上传文件的
  真实样本在服务端声明的区间内实测，按"省下的字节 vs 花掉的 CPU"取最优档位。
* **自适应调优** —— 加 `--auto-tune` 后，客户端先读取服务端声明，用最小配置
  探一次链路，再以 TCP 慢启动方式逐步抬升单文件窗口与跨文件并发。分块大小跟随
  实测吞吐，不参与爬坡。收敛结果会被**同一客户端**的后续传输复用；原生客户端
  不做持久化，新进程会重新测量（浏览器引擎会缓存到 `localStorage`，见 SDK 的
  `tuneTtlMs`）。

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
    println!("已下载 {bytes} 字节（0 表示本地已是最新）");

    client.upload_file("./2026.pdf".as_ref(), "archive/2026.pdf").await?;
    Ok(())
}
```

## 说明

* 下载会在 `DEST` 之下保留**虚拟路径**结构，与浏览器 SDK 行为一致：
  `download tree ./out` 会写出 `./out/tree/...`。
* 上传提交成功后服务端会删除区间边车文件，因此对同一文件再次上传会重发一次
  （位置写入保证幂等）。断点续传针对的是**被中断**的上传。
* 很短的传输可能在 1 秒测量窗口关闭前就结束，此时调优阶段会停在 `ramping`
  且参数为声明的最小值 —— 这是预期行为，不是错误。
* 没有 `pause()` / `cancel()` 句柄：取消是结构性的，直接 drop 传输 future 即可
  （或用 `tokio::select!` / `tokio::time::timeout` 包裹）。两个方向都支持续传，
  因此放弃传输最多损失在途的几个数据块。
* 目录对 `HEAD` 返回 `404`，`download` 就是靠这一点区分文件夹与文件。

实现见 [`crates/libfw-client/src/native/`](../../crates/libfw-client/src/native/)；
集成测试见 `crates/libfw-client/tests/native_client.rs`（会在 TCP 上启动真实的
libfw 服务端）。
