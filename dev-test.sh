#!/usr/bin/env bash
# =============================================================================
# libfw — one-click dev-mode test launcher (Linux / macOS)
#
#   1. runs `cargo test --workspace`
#   2. starts the axum dev server on :8080 (token: dev-token); the server
#      embeds the web UI at `/` and serves the SDK from the repo
#   3. opens the browser to the web UI
#
# The WASM engine (sdk/pkg) must be built once for the web UI to work:
#   wasm-pack build crates/libfw-client --target web --out-dir ../../sdk/pkg --release
#
# Requirements:
#   - cargo (Rust toolchain)
#
# Usage: double-click this file (or run `./dev-test.sh`). Ctrl+C stops
# everything. The port can be overridden with PORT_API / PORT_WEB env vars.
# =============================================================================
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PORT_API="${PORT_API:-8080}"
TOKEN="${TOKEN:-dev-token}"
DATA_DIR="${DATA_DIR:-$ROOT/dev-data}"

SERVER_PID=""

cleanup() {
  trap - EXIT INT TERM
  echo
  echo "== stopping dev server =="
  [[ -n "$SERVER_PID" ]] && kill "$SERVER_PID" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

cd "$ROOT"

echo "== libfw dev-mode testing =="
echo "root: $ROOT"

command -v cargo >/dev/null 2>&1 || {
  echo "error: cargo not found — install the Rust toolchain (https://rustup.rs)"
  exit 1
}

if [[ ! -f "sdk/pkg/libfw_client.js" ]]; then
  echo "warning: sdk/pkg is missing — the web UI needs the WASM engine:"
  echo "  wasm-pack build crates/libfw-client --target web --out-dir ../../sdk/pkg --release"
fi

echo
echo "== 1/3 cargo test --workspace =="
cargo test --workspace

echo
echo "== 2/3 starting axum dev server on :$PORT_API (token: $TOKEN) =="
mkdir -p "$DATA_DIR"
cargo run -p axum-server -- "$DATA_DIR" "$PORT_API" &
SERVER_PID=$!

echo
echo "== 3/3 opening browser =="
WEB_URL="http://127.0.0.1:$PORT_API/"
sleep 3
if command -v xdg-open >/dev/null 2>&1; then
  xdg-open "$WEB_URL" >/dev/null 2>&1 || true
elif command -v open >/dev/null 2>&1; then
  open "$WEB_URL" >/dev/null 2>&1 || true
else
  echo "open $WEB_URL in your browser"
fi

cat <<EOF

== dev server running ==
  web UI     : $WEB_URL
  server API : http://127.0.0.1:$PORT_API  (token: $TOKEN)
  health     : http://127.0.0.1:$PORT_API/health

The axum server serves both the API and the web UI. Press Ctrl+C to stop.
EOF
wait