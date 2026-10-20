# libfw documentation

Start with the [project README](../README.md) for an overview and a quick
start, then dive into the guide that matches your task.

| Guide | English | 中文 | Covers |
| --- | --- | --- | --- |
| Authorization | [authorization.md](./authorization.md) | [authorization.zh-CN.md](./authorization.zh-CN.md) | implementing `TokenVerifier` (fixed, HMAC, JWT, JWKS, introspection), custom `Validator`s, testing, checklist |
| Server integration | [server-integration.md](./server-integration.md) | [server-integration.zh-CN.md](./server-integration.zh-CN.md) | axum and other frameworks, `ServerState` reference, storage backends, shadow paths, cleanup, deployment |
| Protocol | [protocol.md](./protocol.md) | [protocol.zh-CN.md](./protocol.zh-CN.md) | routes, headers, download/upload algorithms, capabilities JSON, status codes, curl |
| Native Rust client | [native-client.md](./native-client.md) | [native-client.zh-CN.md](./native-client.zh-CN.md) | `NativeClient` API, configuration, events, resume, cancellation, tuning |

Also see:

- [Browser SDK README](../sdk/README.md) — `LibfwClient` options and methods,
  File System Access API behavior, events, errors.
- [Rust client example](../examples/rust-client/README.md) — runnable CLI.
- [axum example server](../examples/axum-server/README.md) — demo server with
  an embedded web UI.
- API reference on docs.rs: [`libfw-core`](https://docs.rs/libfw-core),
  [`libfw-server`](https://docs.rs/libfw-server),
  [`libfw-client`](https://docs.rs/libfw-client).

## Reading order by goal

| I want to… | Read |
| --- | --- |
| serve files from my Rust app | server-integration → authorization |
| protect the API with my own tokens | authorization |
| understand the wire format | protocol |
| write a Rust client | native-client |
| build a browser upload/download UI | [sdk/README.md](../sdk/README.md) |
| tune transfers on bad networks | protocol (capabilities) + native-client / SDK (adaptive tuning) |
| run the demo end to end | [root README quick start](../README.md#quick-start) |
