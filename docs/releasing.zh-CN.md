# 发布流程

两个 registry，两套认证方式：

| 目标 | 包 | 认证 |
| --- | --- | --- |
| crates.io | `libfw-core`、`libfw-server`、`libfw-client` | `CARGO_REGISTRY_TOKEN` secret（crates.io 没有 OIDC） |
| npmjs.com | `libfw-client`（JS/WASM） | **OIDC 可信发布**，完全不用 token |

全部由 `.github/workflows/release.yml` 驱动：发布一个 GitHub Release 触发，
或用 `workflow_dispatch` 手动跑一次 dry run。

## 为什么 npm 不再用 token

npm 正在淘汰长期有效的发布 token：会从日志里泄漏、永不过期、还得手动轮换。
可信发布（Trusted Publishing）改用一个 15 分钟级的凭证 —— GitHub 每次运行
workflow 时通过 OpenID Connect 现场签发，registry 校验「哪个仓库的哪个
workflow 文件」在请求，然后回一个只能用于这一次发布、无法被提取或复用的
token。

顺带还有 provenance 证明（用户可以验证 tarball 确实是由这个 workflow 从
这个 commit 构建出来的）。

## 一次性配置

**只做一次**，之后永远不用碰 secret。

### 0. 前置条件

- 包必须已经存在于 npm 上（首次发布只能手动或用 token 走一遍 —— npm 不支持
  用 OIDC 创建全新包）。
- 只支持 GitHub 托管的 runner，自建 runner 不行。
- npm CLI ≥ 11.5.1，Node ≥ 22.14。workflow 里装的是 Node 24 并执行
  `npm install -g npm@latest`，这块已经处理好了。

### 1. 在 npmjs.com 注册可信发布者

打开 https://www.npmjs.com/package/libfw-client/access → **Trusted Publisher**
→ **GitHub Actions**，严格按下面填（大小写敏感、不要写路径、不要带 `.git`）：

| 字段 | 值 |
| --- | --- |
| Organization or user | `ArthurZhou` |
| Repository | `libfw` |
| Workflow filename | `release.yml` |
| Environment name | *留空*（workflow 没有声明 environment） |
| Allowed actions | ☑ **npm publish** |

「Allowed actions」这个勾很关键：2026-09-03 之后创建的可信发布者默认只允许
**staged publishing**（`npm stage publish`，每次发布都要人在浏览器里点批准）。
workflow 里跑的是普通 `npm publish`，所以必须勾上 **npm publish**，否则即使
OIDC token 校验通过也会 403。

npm 在你保存时**不会校验**这些字段，写错了只会在第一次发布时炸。

### 2. 把仓库地址写进 package 清单

OIDC 发布要求 `package.json` 里的 `repository.url` 能解析到同一个 GitHub
仓库。`sdk/package.json` 里已经有了：

```json
"repository": {
  "type": "git",
  "url": "git+https://github.com/ArthurZhou/libfw.git"
}
```

以后仓库改名或转移，这个字符串和 npm 上的可信发布者配置必须一起改。

### 3. 删掉旧 secret

`NPM_TOKEN` 已经不被任何地方读取了。去 npmjs.com 的 Access Tokens 撤销该
token，然后删掉仓库 secret：

```bash
gh secret delete NPM_TOKEN
```

### 4. （首次 OIDC 发布成功后）彻底封死 token

npm → 包 → Settings → **Publishing access** → *Require two-factor
authentication and disallow tokens*。OIDC 发布照常工作，泄漏的 token 再也发
不了包。等一次绿的发布之后再开这个开关，保留 token 通道作为兜底。

## 发版

```bash
npm version patch --no-git-tag-version --prefix sdk   # 可选，CI 会用 tag 覆盖
gh release create v0.4.8 --generate-notes
```

Release 事件会串起来跑：`publish-crates`（cargo，顺序 core → server →
client）→ `publish-npm`（构建 WASM → 用 tag 盖版本号 → 发布）。

JS 包的版本号取自 release tag（`v0.4.8` → `0.4.8`），tag 是唯一事实来源；
仓库里的 `sdk/package.json` 要手动 bump。

## Dry run

```bash
gh workflow run release.yml --ref master -f dry_run=true
```

会完整跑一遍 WASM 构建并执行 `npm publish --dry-run`（不真发），并跳过 crates
任务。注意：dry run **不会**真正走 OIDC 交换 —— 只有一次真实的发布才能证明
可信发布者配置是对的。

## 排错

| 现象 | 原因 |
| --- | --- |
| `ENEEDAUTH` / "Unable to authenticate" | 缺 `id-token: write`；或经 `workflow_call` 调用时父子 workflow 都必须有该权限 |
| 拿到 OIDC token 却 401/403 | npmjs.com 上的 workflow 文件名不是 `release.yml`；或 Allowed actions 里没勾 npm publish |
| npm 静默走了 token 而不是 OIDC | `setup-node` 设了 `registry-url`，或环境里存在 `NODE_AUTH_TOKEN` —— 两者都会让 npm 优先用 token 认证 |
| 发布成功但没有 provenance 徽章 | 仓库或包是私有的；或从 `repository.url` 不一致的 fork 发布 |
| 发布报 `npm error code E404` | `repository.url` 不匹配；或包名/scope 与可信发布者配置不符 |
