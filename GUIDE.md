libfw 传输库开发要求（修订版）

1. 项目概述

libfw 是一套面向文件及文件夹传输的 Rust 库，基于 Workspace 架构，支持服务端无缝嵌入与客户端 WASM+JS SDK 深度封装。核心目标是提供高性能、低内存占用的流式传输，具备断点续传、自动压缩和细粒度鉴权能力。JS SDK 作为 libfw-client 的有机组成部分，与 WASM 核心紧密集成，为前端开发者提供纯 JavaScript 使用体验。

2. 技术栈与约束

· 语言：Rust（核心逻辑、服务端扩展、WASM 引擎）；JavaScript/TypeScript（客户端 SDK 封装层） 
· 编译目标：服务端为对应平台静态/动态库；客户端 Rust 代码编译为 wasm32-unknown-unknown 
· 压缩库：集成 zrip，实现流式压缩与解压 
· 传输协议：HTTP/1.1 或更高，使用 Range、ETag、If-Range 等标准头 
· 浏览器环境：仅支持现代浏览器，依赖 File System Access API（showDirectoryPicker、createWritable 等） 
· 鉴权：Bearer Token 无状态校验，仅负责解析与验证，不实现签发

3. 模块拆分与职责

项目根为 Rust Workspace，包含三个核心 crate，其中 libfw-client 在编译产物中同时交付 WASM 模块与配套 JS SDK。

包名 类型 职责说明 
libfw-core Rust 库 定义共享数据结构、Trait、协议常量、HTTP 头定义、zrip 流式接口抽象、分片传输元数据。 
libfw-server Rust 库 提供服务端路由处理/中间件：请求拦截与 Token 校验、处理 Range 响应实现断点续传下载、流式接收分片并组合落盘。 
libfw-client Rust 库 + JS/TS SDK Rust 部分：WASM 核心调度器，封装 HTTP 客户端、分片并发控制、状态机、重试逻辑、zrip 解压流，通过 wasm-bindgen 导出生命周期控制接口并声明 JS 导入的落盘回调。 JS/TS 部分（SDK）：完全内部接管 WASM 实例化与内存交互，暴露 LibfwClient 类及 downloadFolder(token)、upload(token, files) 等高级 Promise API，负责 File System API 的目录选择、句柄缓存，并将 WASM 回调推送的字节流写入本地磁盘。

说明：JS SDK 不是独立的 Rust crate，而是 libfw-client 的嵌入式部分，随 Rust 编译产物一起发布（例如通过 npm 包形式提供统一的 libfw-client 包）。

4. 功能需求详细

4.1 核心传输（libfw-core 定义，server/client 实现）

· 断点续传 
· 服务端： 
· 正确解析 Range 请求头，返回 206 Partial Content 及 Content-Range、ETag 或 Last-Modified。 
· 支持 If-Range / If-None-Match 条件请求，用于客户端偏移量校验。 
· 文件变更时返回 412/416 等状态码，通知客户端重置。 
· 客户端（WASM 引擎）： 
· 持久化任务进度（浏览器端使用 IndexedDB），记录已完成分片、ETag、文件大小。 
· 网络异常或暂停后恢复时，先从持久化状态读取偏移量，发起带校验头的请求；若服务端判定偏移无效，则重新计算或报错。 
· 自动重试机制：指数退避、最大重试次数，重试前校验偏移有效性。 
· 流式传输 
· 上传：客户端按固定大小分片（如 2MB）读取文件，HTTP 分块上传，服务端流式接收、解压（若启用压缩）并顺序写入临时文件，完成后重命名为目标文件。 
· 下载：服务端流式读取文件并压缩（可选），客户端 WASM 流式接收、解压，通过回调将 Uint8Array 块传给 JS SDK；JS SDK 通过 createWritable 流式写入磁盘。 
· 内存占用：Rust 侧处理单个文件的堆内存峰值不随文件大小增长（恒定缓冲区，如 64KB 滑动窗口）。 
· 自动压缩 
· 基于 zrip 实现 Compressor 和 Decompressor，支持流式输入输出。 
· 压缩启用时，服务端与客户端通过请求/响应头协商（如 Content-Encoding: zrip 或自定义 X-Libfw-Compress: zrip），在 HTTP body 流中透明压缩/解压。

4.2 服务端能力（libfw-server）

· 无缝路由嵌入 
· 提供可挂载的 Handler/中间件（如针对 actix-web 或 axum 的 extractor/service），业务代码只需配置基础路径和 Token 校验公钥/回调即可启用。 
· 支持自定义存储后端：通过实现 StorageBackend Trait，可对接本地文件系统、对象存储等。 
· 细粒度鉴权 
· 从 Authorization: Bearer <token> 中提取 JWT 或自定义格式 Token，解析载荷（Claims）。 
· 校验维度： 
· path：请求的实际路径必须在允许的路径前缀/模式内。 
· permission：区分 read（下载）、write（上传），拒绝无权限操作。 
· exp：验证过期时间，过期返回 401。 
· 校验失败时： 
· Token 不合法或过期返回 401 Unauthorized。 
· 路径/权限不符返回 403 Forbidden。 
· 不负责 Token 签发，仅持有解码密钥或调用外部校验服务。

4.3 客户端能力（libfw-client 的 WASM 引擎 + JS SDK）

· 零成本 WASM 调用 
· JS SDK 内部完成 WASM 模块的加载、实例化、导出函数绑定，完全对二次开发者透明。 
· 开发者仅需引用 LibfwClient 类，所有方法返回 Promise，无任何 WebAssembly 或内存操作暴露。 
· 分片调度与状态机（WASM 引擎） 
· 在 WASM 中发起 HTTP 请求（使用 web-sys 或自定义 fetch 导入），支持并发连接数限制（可配置，默认 4）。 
· 任务状态机：Idle → Downloading/Uploading → Paused → Resumed → Completed/Failed。 
· 每个文件任务分为多个分片，维护分片队列、错误列表；单个分片失败自动重试，超时或达到重试上限后整体任务失败。 
· 下载时，接收的数据经 zrip 解压后，通过 js_callback_write_chunk(offset: u64, data: &[u8]) 推送给 JS 层。 
· File System API 桥接（JS SDK） 
· 提供 client.downloadFolder(token: string)： 
- [ ] 1. 调用 window.showDirectoryPicker() 让用户选择本地目录。 
2. 与 WASM 协商，WASM 获取文件列表并逐个下载。 
3. 对每个文件，WASM 通过回调告诉 JS SDK 相对路径和文件元信息；JS SDK 通过 directoryHandle.getFileHandle(path, {create: true}) 获取或创建文件句柄，并维护句柄缓存。 
4. WASM 推送数据块时，JS SDK 通过 fileHandle.createWritable() 定位到对应偏移（write({ type: 'write', position, data })）流式写入。 
5. 文件夹结构完全保留，包括嵌套目录，JS SDK 负责在本地创建对应目录树。 
· client.upload(token: string, files?: FileList) 类似，支持从本地目录选取或直接传入文件列表，JS SDK 读取文件流并传给 WASM 上传。 
· 暂停/恢复/取消：JS SDK 提供 pause、resume、cancel 方法，内部调用 WASM 导出的控制接口，并管理句柄缓存状态。

5. 接口与集成要求

5.1 libfw-core 公共接口

· TokenClaims 结构体（字段：sub, exp, permissions, allowed_paths 等）。 
· Validator Trait：fn validate(&self, claims: &TokenClaims, path: &str, action: Action) -> Result<()>。 
· StorageBackend Trait（服务端）：支持 read_stream、write_stream 等方法。 
· 传输常量定义：CHUNK_SIZE、HEADER_COMPRESS、HEADER_FILE_META 等。

5.2 libfw-server 集成

· 以 axum 框架为例，提供 LibfwLayer 或 LibfwRouter，使用方法类似：
let app = Router::new()
    .nest("/files", libfw_server::router(storage, validator));
· 配置项：存储后端实例、Token 解码密钥或校验闭包、压缩开关、上传大小限制等。

5.3 libfw-client 使用示例

import { LibfwClient } from 'libfw-client';

const client = new LibfwClient({ concurrency: 4, compress: true });

// 下载整个文件夹
await client.downloadFolder('your_token_here');

// 上传
const fileInput = document.querySelector('input[type=file]');
await client.upload('your_token_here', fileInput.files);

6. 性能与非功能需求

· 内存：WASM 侧处理单个文件时，Rust 分配的内存上限不随文件大小增长，恒定小于 2MB（不含 HTTP 缓冲区）。 
· 并发：WASM 内 HTTP 请求并发数可配置，默认 4，避免浏览器连接数限制。 
· 错误处理：所有网络错误、文件系统错误、Token 校验错误均需转换为明确的异常或状态码，JS SDK 抛出统一格式的 LibfwError。 
· 可移植性：libfw-server 需兼容主流 Rust Web 框架（至少提供 actix-web 和 axum 的集成示例）；libfw-client 的 JS SDK 需封装为标准的 ES 模块和 UMD 包，支持 tree-shaking。 
· 文档：每个公开 API 必须附带 JSDoc 注释和 Rust doc，提供完整的使用示例。

7. 交付物

8. Rust Workspace 源码，包含 libfw-core、libfw-server、libfw-client 三个 crate。
9. libfw-client 的 npm 包，包含编译好的 WASM 二进制、JS 绑定及 TypeScript 类型定义。
10. 集成示例： 
11. · 服务端示例（基于 axum 的简单文件服务）。 
12. · 客户端示例（一个 HTML 页面演示下载和上传）。
13. 单元测试与集成测试（WASM 部分使用 wasm-bindgen-test）。
14. README 与 API 文档。

---





每次开发流程开始前阅读PROGRESS.md，结束后将本次干的事情写入。
