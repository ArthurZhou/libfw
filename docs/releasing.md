# Releasing

Two registries, two different auth models:

| Target | Package | Auth |
| --- | --- | --- |
| crates.io | `libfw-core`, `libfw-server`, `libfw-client` | `CARGO_REGISTRY_TOKEN` secret (crates.io has no OIDC) |
| npmjs.com | `libfw-client` (JS/WASM) | **OIDC trusted publishing** — no token at all |

Everything runs from `.github/workflows/release.yml`, triggered by publishing a
GitHub Release (or `workflow_dispatch` for a dry run).

## Why npm no longer uses a token

npm is retiring long-lived publish tokens: they leak through logs, never expire,
and have to be rotated. Trusted publishing replaces them with a short-lived
credential that GitHub mints per workflow run over OpenID Connect. The registry
verifies *which repo + which workflow file* is asking, then hands back a token
that is valid for that one publish and cannot be extracted or reused.

It also gets you a provenance attestation for free (users can verify the tarball
was built by this workflow from this commit).

## One-time setup

Do this **once**, then never touch secrets again.

### 0. Prerequisites

- The package must already exist on npm (first publish has to be manual or
  token-based — npm cannot bootstrap a brand-new package via OIDC).
- GitHub-hosted runners only. Self-hosted runners are not supported.
- npm CLI ≥ 11.5.1 and Node ≥ 22.14. The workflow installs Node 24 and runs
  `npm install -g npm@latest`, so this is handled.

### 1. Register the trusted publisher on npmjs.com

Open https://www.npmjs.com/package/libfw-client/access → **Trusted Publisher** →
**GitHub Actions**, and fill in exactly (case-sensitive, no path, no `.git`):

| Field | Value |
| --- | --- |
| Organization or user | `ArthurZhou` |
| Repository | `libfw` |
| Workflow filename | `release.yml` |
| Environment name | *(leave empty — the workflow declares no environment)* |
| Allowed actions | ☑ **npm publish** |

The "Allowed actions" checkbox matters: publishers created after
2026-09-03 default to **staged publishing only** (`npm stage publish`, which
requires a human to approve each release in the browser). The workflow runs a
plain `npm publish`, so **npm publish must be ticked** or the run fails with a
403 even though the OIDC token was accepted.

npm does not validate these fields when you save them. A typo surfaces only as a
failed publish later.

### 2. Publish the repository URL

`npm publish` over OIDC requires `repository.url` in the package manifest to
resolve to the same GitHub repo. `sdk/package.json` already carries:

```json
"repository": {
  "type": "git",
  "url": "git+https://github.com/ArthurZhou/libfw.git"
}
```

If the repo is ever renamed or transferred, this string and the npm trusted
publisher must be updated together.

### 3. Delete the old secret

`NPM_TOKEN` is no longer read anywhere. Revoke the token on npmjs.com
(Access Tokens) and delete the repository secret:

```bash
gh secret delete NPM_TOKEN
```

### 4. (After the first successful OIDC release) lock tokens out

npm → package → Settings → **Publishing access** → *Require two-factor
authentication and disallow tokens*. OIDC publishing keeps working; a leaked
token can no longer publish anything. Do this only after one green release, so
you always have the token path as a fallback.

## Cutting a release

```bash
npm version patch --no-git-tag-version --prefix sdk   # optional, CI stamps it
gh release create v0.4.8 --generate-notes
```

The release event chains: `publish-crates` (cargo, in order core → server →
client) → `publish-npm` (build WASM → stamp version from the tag → publish).

The JS package version is taken from the release tag (`v0.4.8` → `0.4.8`), so the
tag is the single source of truth. `sdk/package.json` in the repo is left alone
until you bump it manually.

## Dry run

```bash
gh workflow run release.yml --ref master -f dry_run=true
```

Runs the full WASM build and `npm publish --dry-run` without publishing, and
skips the crates job. Note: a dry run does **not** exercise the OIDC exchange —
only a real publish proves the trusted publisher config is right.

## Troubleshooting

| Symptom | Cause |
| --- | --- |
| `ENEEDAUTH` / "Unable to authenticate" | `id-token: write` missing, or the job runs behind `workflow_call` without the permission on **both** parent and child |
| 401/403 with a valid OIDC token | Workflow filename on npmjs.com ≠ `release.yml`, or "npm publish" not ticked under Allowed actions |
| npm silently uses a token instead of OIDC | `registry-url` set on `setup-node`, or `NODE_AUTH_TOKEN` present in the environment — both make npm prefer token auth |
| Publish succeeds, no provenance badge | Repo or package is private, or publishing from a fork whose `repository.url` differs |
| `npm error code E404` on publish | `repository.url` mismatch, or the package name/scope does not match the trusted publisher |
