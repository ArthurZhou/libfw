// Fail-closed guard: refuses npm publish when the WASM payload is missing.
//
// History: libfw-client@0.4.7 shipped to npm WITHOUT pkg/ (the wasm-pack
// build output, gitignored at /sdk/pkg) — `index.js` imports
// `./pkg/libfw_client.js`, so the package was dead on arrival while every
// CI step still reported green. This script makes that state impossible to
// publish, from CI or a local terminal.
//
// Wired as `prepublishOnly` in package.json, so it runs before `npm publish`
// packs the tarball.
"use strict";

const fs = require("node:fs");
const path = require("node:path");

const root = path.join(__dirname, "..");

// [relative path, minimum plausible size in bytes]
const checks = [
  ["pkg/libfw_client_bg.wasm", 32 * 1024],
  ["pkg/libfw_client.js", 1024],
  ["pkg/libfw_client.d.ts", 0],
  ["pkg/libfw_client_bg.wasm.d.ts", 0],
  ["pkg/package.json", 0],
];

const broken = [];
for (const [rel, minSize] of checks) {
  const abs = path.join(root, rel);
  let size = -1;
  try {
    size = fs.statSync(abs).size;
  } catch {
    // fall through: size -1 means missing
  }
  if (size < minSize) broken.push(`${rel} (${size < 0 ? "missing" : `${size}B, expected >= ${minSize}B`})`);
}

if (broken.length > 0) {
  console.error(`
  ABORTED: this publish would ship a broken package.

  Missing or implausibly small (relative to sdk/):
${broken.map((b) => `    - ${b}`).join("\n")}

  Build the WASM payload first, from the repo root:
    wasm-pack build crates/libfw-client --target web --out-dir ../../sdk/pkg --release
  or from sdk/:  npm run build:wasm
`);
  process.exit(1);
}

console.log(`prepublish check OK: wasm payload present (${checks.map((c) => c[0]).join(", ")})`);
