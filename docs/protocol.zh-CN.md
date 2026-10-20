# HTTP 协议参考

libfw 客户端（浏览器 SDK 或原生 Rust）与服务端之间的完整线路协议。全部基于
普通 HTTP，没有 WebSocket，除分块边界外没有自定义帧格式。

- 协议版本：`libfw/1`（`x-libfw-protocol`）
- 基础内容类型：`application/octet-stream`
- URL 中的路径是**规范相对路径**（`dir/sub/file.txt`）；开头的 `/`、`..`、
  `.` 段与 NUL 字节都会被拒绝。
- 服务端安装 `PathCodec` 时，路径可以是*影子路径*，见
  [服务端集成](./server-integration.zh-CN.md#路径转换影子路径)。

## 路由

| 方法 | 路由 | 用途 |
| --- | --- | --- |
| `GET` | `/file/{*path}` | 下载（整体或 `Range`） |
| `HEAD` | `/file/{*path}` | 仅元数据（大小 + ETag） |
| `POST` | `/file/{*path}` | 上传分块 / 提交 |
| `GET` | `/dir` | 根目录列表 |
| `GET` | `/dir/{*path}` | 目录列表（JSON） |
| `GET` | `/capabilities` | 能力声明（**公开**） |

除 `/capabilities` 外，所有路由都要求 `Authorization: Bearer <token>`。
头缺失或格式错误返回 `401`，策略拒绝返回 `403`。

所有路由都会经过协议版本中间件：若请求携带不兼容的 `x-libfw-protocol`，
服务端返回 `426 Upgrade Required`，而不是静默地按错误语义处理。不带该头的
请求会被接受（方便 `curl` 与自写脚本）。

## 请求 / 响应头

| 头 | 方向 | 含义 |
| --- | --- | --- |
| `Authorization` | → | `Bearer <token>`（除 `/capabilities` 外必填） |
| `x-libfw-protocol` | → | `libfw/1` 版本握手 |
| `Range` | → | `bytes=start-end` 或 `bytes=-suffix` |
| `If-Range` | → | ETag；不匹配时服务端忽略 `Range`，返回 `200` + 完整内容 |
| `If-None-Match` | → | ETag；匹配时返回 `304` |
| `Accept-Encoding` | → | `zrip` 表示请求压缩下载 |
| `x-libfw-compress` | ↔ | `zrip` / `identity`；下载响应中表示实际所用格式 |
| `x-libfw-compress-level` | ↔ | 请求的 zrip 级别；响应回显实际使用的级别 |
| `x-libfw-file-meta` | → | 上传用的 base64(JSON `FileMeta`) |
| `x-libfw-offset` | → | 本上传分块的绝对字节偏移 |
| `x-libfw-session` | → | 单文件上传会话 id |
| `x-libfw-session-status` | → | `1` 时把请求变成续传探测（不写入 body） |
| `x-libfw-final` | → | `1` 表示这是提交请求 |
| `ETag` | ← | 不透明版本标识（由 size + mtime 派生） |
| `Content-Range` | ← | `206` 时为 `bytes start-end/total`，`416` 时为 `bytes */total` |
| `Accept-Ranges` | ← | 恒为 `bytes` |

### `FileMeta`（`x-libfw-file-meta`）

头的值是 **UTF-8 JSON 的 base64**，不是裸 JSON：HTTP 头无法承载非 Latin-1
字节，裸 JSON 会让中文文件名直接失败。

```json
{ "path": "dir/a.txt", "size": 11, "mtime": 1710000000, "etag": "11-1710000000" }
```

`path` 是虚拟路径，`size` 是提交时必须精确匹配的字节数，`mtime` 为 unix 秒，
`etag` 可选（服务端提交时会重算）。编解码辅助函数在
`libfw_core::metadata`：`encode_file_meta_header` / `decode_file_meta_header`。

## 下载

### 普通下载

```http
GET /file/docs/plan.pdf HTTP/1.1
Authorization: Bearer <token>
x-libfw-protocol: libfw/1
```

```http
HTTP/1.1 200 OK
Accept-Ranges: bytes
Content-Type: application/octet-stream
Content-Length: 1048576
ETag: "1048576-1710000000"
x-libfw-compress: identity
```

协商压缩后，响应体是一串 zrip（zstd）帧，此时不发送 `Content-Length`
（压缩后大小无法预先得知）；响应会给出 `x-libfw-compress: zrip` 与实际级别。

### 范围下载

```http
GET /file/docs/plan.pdf HTTP/1.1
Range: bytes=0-262143
If-Range: "1048576-1710000000"
Accept-Encoding: zrip
x-libfw-compress-level: 1
```

```http
HTTP/1.1 206 Partial Content
Content-Range: bytes 0-262143/1048576
ETag: "1048576-1710000000"
x-libfw-compress: zrip
x-libfw-compress-level: 1
```

规则：

- `Range` 针对**未压缩**的文件偏移；每个请求区间独立压缩。
- `If-Range` 与当前 ETag 不匹配 → 忽略区间，返回 `200` + 完整内容。
- 不可满足的区间（start ≥ size、`bytes=-0`）→ `416`，带
  `Content-Range: bytes */<total>`，无 body。对客户端而言这表示续传偏移已
  失效，应从 0 重新开始。
- `If-None-Match` 匹配 → `304`，无 body。
- 目录对 `HEAD`/`GET` 返回 `404`；目录内容请用列表路由。

### 压缩级别

服务端声明级别区间（`zripLevels`），并把任何请求级别夹取进该区间。只有服务端
以 `CompressionFormat::Zrip` 构建（默认）时才会响应
`x-libfw-compress: zrip`。响应回显的 `x-libfw-compress` /
`x-libfw-compress-level` 才是权威值 —— 永远不要假设请求的级别被原样采用。

## 上传

有两种形态。都从 `POST /file/{*path}` 开始并携带 `x-libfw-file-meta`，都返回
`201 Created`：

```json
{ "file": { "path": "dir/a.txt", "size": 11, "mtime": 1710000000, "etag": "11-1710000000" } }
```

若安装了 `PathCodec`，返回的 `path` 已经被转换回**影子路径**。

### 顺序上传（不带 session 头）

`x-libfw-offset` 决定写入模式：

| `x-libfw-offset` | 模式 | 冲突时 |
| --- | --- | --- |
| 缺失 | create | 目标已存在 → `409` |
| `0` | overwrite | 截断 |
| `N > 0` | resume | 目标当前不是恰好 `N` 字节 → `412` |

body 先写入临时文件，成功后原子重命名。任何失败都会丢弃临时文件（`abort`），
因此失败的顺序上传不会留下半成品目标。

### 会话上传（并发分块）

两个内置客户端都使用这一形态。一次逻辑上传 = N 个分块请求 + 1 个提交请求，
它们携带相同的 `x-libfw-session`。

```http
POST /file/big.bin HTTP/1.1
Authorization: Bearer <token>
x-libfw-session: 67108864-1710000000
x-libfw-offset: 2097152
x-libfw-file-meta: <base64 {"path":"big.bin","size":67108864,...}>
x-libfw-compress: zrip
Content-Type: application/octet-stream

<[2097152, 4194304) 区间的压缩字节>
```

```http
HTTP/1.1 201 Created
{ "file": { ... } }
```

算法：

1. **探测（probe）** —— 发送数据前可以先问服务端已持有哪些区间：

   ```http
   POST /file/big.bin HTTP/1.1
   x-libfw-session: 67108864-1710000000
   x-libfw-session-status: 1
   x-libfw-file-meta: <...>
   ```

   ```json
   { "ranges": [[0, 1048576], [2097152, 4194304]] }
   ```

   区间为**未压缩**偏移的 `[start, end)`。探测不写入、不提交；只会打开
   （必要时创建）共享临时文件。

2. **数据分块** —— 每块携带**绝对** `x-libfw-offset`，按位置写入，因此可以
   乱序到达、可以流水线并发。重传同一分块是幂等的。

3. **提交（commit）** —— 最后一个请求带 `x-libfw-final: 1`（通常是空 body），
   服务端校验已收区间覆盖整个文件且长度等于 `meta.size`，然后原子重命名。

   没有 `x-libfw-final` 就不会提交：数据分块只会留下可续传的临时文件。

4. **放弃** —— 客户端消失会留下 `.libfw-sess-*` 临时文件及其 `.blocks`
   区间边车。服务端的清理任务会删除超过会话 TTL 的临时文件（tus
   `Expiration` 语义；默认 24 小时，扫描间隔 1 小时）。活跃上传会刷新时间，
   清理不会打断正在进行的传输。

值得显式处理的提交失败：

| 响应 | 含义 | 正确处理 |
| --- | --- | --- |
| `400` "some blocks are missing" | 中间某个分块没落盘 | 重新探测、补发缺口、再次提交 |
| `400` "commit yields N bytes…" | 大小不符 | 重新探测；若服务端比预期多，说明文件已变化 |
| `413` | `meta.size` 或分块超过服务端上限 | 停止；换小文件或调大服务端上限 |
| `412` | 续传偏移不匹配 | 从 0 重新开始 |
| `409` | create 语义遇到已存在文件 | 改为覆盖（`x-libfw-offset: 0`） |

请求 body 出错（页面刷新、连接断开）按传输事件处理：已收到的分块**保留**。
其他错误（压缩损坏、超限、磁盘错误）会丢弃临时文件。

### 会话隔离

服务端把由 `TokenClaims.sub` 派生的标签（其 SHA-256 的前 16 个十六进制字符）
嵌入会话临时文件名。因此两个不同的认证主体即使使用同一个 session id，也不会
冲突，更无法读到对方的在途数据。

### 会话 id

`x-libfw-session` 应当对**同一个文件版本**保持稳定，这样刷新后的页面才能找回
同一个会话。两个客户端都使用文件 ETag（size + mtime）去掉引号，例如
`67108864-1710000000`。两次尝试之间文件被编辑过就会得到新 id，从而开始新会话。

## 目录列表

```http
GET /dir/docs HTTP/1.1
Authorization: Bearer <token>
```

```json
[
  { "path": "docs/deep", "is_dir": true,  "size": 0,  "mtime": 1710000000 },
  { "path": "docs/a.txt", "is_dir": false, "size": 11, "mtime": 1710000001 }
]
```

`GET /dir`（不带路径）列出根目录。条目携带完整虚拟路径（安装 codec 时已转换为
影子路径），因此列表里的路径可以直接用于后续请求。

## 能力声明

`GET /capabilities` 是公开接口（无需 bearer token），返回自适应客户端协商所依据
的契约：

```json
{
  "protocol": "libfw/1",
  "compression": {
    "formats": ["identity", "zrip"],
    "zripLevels": { "default": 1, "max": 4, "min": -8 }
  },
  "limits": {
    "concurrency":    { "default": 4, "max": 4, "min": 1 },
    "uploadWindow":   { "default": 8, "max": 8, "min": 1 },
    "downloadWindow": { "default": 4, "max": 4, "min": 1 },
    "chunkSize":      { "default": 2097152, "max": 8388608, "min": 262144 },
    "maxUploadSize": 21474836480,
    "maxRetries": 3,
    "timeoutMs": 60000
  }
}
```

说明：

- **只是建议，不做强制。** 服务端不会硬性限制它声明的窗口、并发或分块大小；
  客户端需要自行夹取。唯一例外是 `maxUploadSize`：`ServerState::capabilities()`
  会把它降到服务端实际执行的上限，因此永远不会被虚报。
- **`caps_hash`。** `Capabilities::caps_hash()` 是 canonical serde JSON 的
  `sha256-<hex>`。字段顺序即结构体声明顺序，调整字段顺序会改变哈希。客户端用
  该哈希标记缓存的调优结果，变化时丢弃。
- **区间是 `min..=max` 加 `default`。** 反向区间必须视为"使用默认值"
  （`IntRange::clamp` 与 `ZripLevels::clamp_level` 都这样处理）。
- **该接口无鉴权**，因此载荷中绝不能包含租户或用户数据。

## 状态码

| 状态码 | 含义 |
| --- | --- |
| `200` | 完整下载 / 探测响应 |
| `201` | 上传分块被接受，或文件已提交 |
| `206` | 部分内容 |
| `304` | `If-None-Match` 匹配 |
| `400` | 头格式错误、压缩流无效、提交不完整、影子路径无效 |
| `401` | token 缺失 / 格式错误 / 无效 / 过期 |
| `403` | token 有效但对该路径或动作无权限 |
| `404` | 文件或目录不存在 |
| `409` | create 上传遇到已存在文件 |
| `412` | 续传偏移不匹配 |
| `413` | 上传超过 `maxUploadSize` |
| `416` | 区间不可满足 |
| `426` | `x-libfw-protocol` 不兼容 |
| `500` | 存储 / IO 失败 |

## curl 走查

```bash
BASE=http://127.0.0.1:8080
TOKEN=dev-token

# 能力声明（公开）
curl -s "$BASE/capabilities" | jq .

# 顺序上传一个小文件
META=$(printf '{"path":"dir/a.txt","size":11}' | base64 -w0)
curl -sS -X POST "$BASE/file/dir/a.txt" \
  -H "Authorization: Bearer $TOKEN" \
  -H "x-libfw-file-meta: $META" \
  --data-binary "hello world" | jq .

# 范围下载
curl -sS -D- -o chunk.bin \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Range: bytes=0-4' \
  "$BASE/file/dir/a.txt"

# 目录列表
curl -sS -H "Authorization: Bearer $TOKEN" "$BASE/dir/dir" | jq .

# 会话上传：探测、一个分块、提交
SESSION=a.txt-11
META=$(printf '{"path":"dir/b.txt","size":11}' | base64 -w0)
curl -sS -X POST "$BASE/file/dir/b.txt" \
  -H "Authorization: Bearer $TOKEN" -H "x-libfw-session: $SESSION" \
  -H "x-libfw-session-status: 1" -H "x-libfw-file-meta: $META"
curl -sS -X POST "$BASE/file/dir/b.txt" \
  -H "Authorization: Bearer $TOKEN" -H "x-libfw-session: $SESSION" \
  -H "x-libfw-offset: 0" -H "x-libfw-file-meta: $META" \
  --data-binary "hello world"
curl -sS -X POST "$BASE/file/dir/b.txt" \
  -H "Authorization: Bearer $TOKEN" -H "x-libfw-session: $SESSION" \
  -H "x-libfw-final: 1" -H "x-libfw-file-meta: $META"
```

`base64 -w0` 是 GNU coreutils 语法；macOS 请用 `| base64`（不折行）。

## 传输层说明

- **并行度按请求计。** 下载是 N 个独立的 `Range` GET，上传是 M 个独立的
  分块 POST。某个请求卡住不会阻塞其他请求 —— 这正是弱网下的关键。
- **HTTP/3 / QUIC** 无需修改客户端：服务端协商 h3 时 `fetch`/`reqwest` 会自动
  使用，每个并行请求各占一条 QUIC 流。仓库里的示例服务是 HTTP/1.1；要获得 h3
  请在前面放支持 QUIC 的反向代理或负载均衡器。
- **压缩按 body 进行。** 请求与响应各自独立压缩；zrip 帧约 64 KiB，因此无论
  文件多大，内存占用都保持恒定。
