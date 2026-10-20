# libfw axum 示例服务

一个完整可运行的 libfw 部署示例：完整的 HTTP API，外加内嵌的 Web UI，
可以浏览、上传、下载文件与目录，并显示实时进度，支持暂停/继续/取消与传输日志。

```bash
cargo run -p axum-server -- <存储目录> [端口]
# 默认：存储 ./data，端口 8080，token dev-token
```

然后打开 <http://127.0.0.1:8080/>。

Web UI 需要先构建一次 WASM 引擎：

```bash
wasm-pack build crates/libfw-client --target web --out-dir ../../sdk/pkg --release
```

没有 `sdk/pkg` 时页面仍会渲染，并提示需要执行的命令；REST API 不受影响。

## 路由

| 方法 | 路由 | 鉴权 | 用途 |
| --- | --- | --- | --- |
| `GET` | `/` | 否 | 内嵌 Web UI（`index.html`，通过 `include_str!` 嵌入） |
| `GET` | `/health` | 否 | 服务名、版本、协议、存储根目录 |
| `GET` | `/sdk/*` | 否 | 仓库中的 `sdk/` 目录（开发便利） |
| `GET`/`HEAD` | `/file/{*path}` | 是 | 下载（`Range`、`ETag`、`If-Range`、zrip） |
| `POST` | `/file/{*path}` | 是 | 上传（顺序或会话分块） |
| `GET` | `/dir[/{*path}]` | 是 | JSON 目录列表 |
| `GET` | `/capabilities` | **否** | 面向自适应客户端的能力声明 |

除 `/capabilities` 外，所有 libfw 路由都要求 `Authorization: Bearer dev-token`。
完整线路契约见 [docs/protocol.zh-CN.md](../../docs/protocol.zh-CN.md)。

## 配置

| 参数 / 变量 | 默认值 | 含义 |
| --- | --- | --- |
| `<存储目录>`（argv） | `./data` | 存储根目录（不存在则创建） |
| `<端口>`（argv） | `8080` | 监听 `127.0.0.1` 的端口 |
| `LIBFW_PATH_KEY` | 未设置 | 64 个十六进制字符 → 启用 `EncryptedPathCodec`（AES-256-GCM 影子路径） |
| `LIBFW_CLEANUP_INTERVAL` | `3600` | 陈旧会话临时文件清理周期（`30s`、`15m`、`2h`、`1d`） |
| `LIBFW_SESSION_TTL` | `24h` | 被放弃的 `.libfw-sess-*` 临时文件最大存活时间 |

```bash
# 加密影子路径
LIBFW_PATH_KEY=$(openssl rand -hex 32) cargo run -p axum-server -- data 8080

# 开发时更快的清理
LIBFW_CLEANUP_INTERVAL=60s LIBFW_SESSION_TTL=10m cargo run -p axum-server -- data 8080
```

## 试试 API

```bash
BASE=http://127.0.0.1:8080
TOKEN=dev-token

curl -s "$BASE/health"
curl -s "$BASE/capabilities"

META=$(printf '{"path":"dir/a.txt","size":11}' | base64 -w0)
curl -X POST "$BASE/file/dir/a.txt" \
  -H "Authorization: Bearer $TOKEN" \
  -H "x-libfw-file-meta: $META" \
  --data-binary "hello world"

curl -H "Authorization: Bearer $TOKEN" -H 'Range: bytes=0-4' "$BASE/file/dir/a.txt"
curl -H "Authorization: Bearer $TOKEN" "$BASE/dir/dir"
```

## 说明

- Web UI 通过 `include_str!("../index.html")` 嵌入，因此**修改 `index.html`
  后必须重新编译服务端**。SDK 并不嵌入：它从仓库的 `sdk/` 目录以 `/sdk`
  路径提供，所以重新构建 WASM 引擎后页面无需重新编译即可加载新版本。
- Windows 上服务端运行中无法重新编译（可执行文件被占用）。
- 开发用验证器只接受字面量 `dev-token` 并授予全部读写权限。对外暴露前请替换，
  见 [docs/authorization.zh-CN.md](../../docs/authorization.zh-CN.md)。
- CORS 宽松、并附加 `Cache-Control: no-store`，都是为开发方便；生产环境请收紧。
- 会话临时文件在后台自动清理，因此客户端中途消失也不会永久泄漏临时文件。
