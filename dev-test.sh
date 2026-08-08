#!/usr/bin/env bash
# =============================================================================
# libfw — one-click dev-mode test launcher (Linux / macOS)
#
#   1. runs `cargo test --workspace`
#   2. starts the axum dev server on :8080 (token: dev-token); it also serves
#      the web demo from the repo root (via --static), so no python is needed
#   3. opens the browser to the demo page
#
# Requirements:
#   - cargo (Rust toolchain)
#
# Usage: double-click this file (or run `./dev-test.sh`). Ctrl+C stops
# everything. Ports can be overridden with PORT_API / PORT_WEB env vars.
# =============================================================================
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PORT_API="${PORT_API:-8080}"
PORT_WEB="${PORT_WEB:-5173}"
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
  echo "warning: sdk/pkg is missing — run:"
  echo "  wasm-pack build crates/libfw-client --target web --out-dir ../../sdk/pkg --release"
fi

echo
echo "== 1/4 cargo test --workspace =="
cargo test --workspace

echo
echo "== 2/3 starting axum dev server on :$PORT_API (token: $TOKEN) =="
mkdir -p "$DATA_DIR"
# --static "$ROOT" lets the same server serve the web demo (no python needed).
cargo run -p axum-server -- "$DATA_DIR" "$PORT_API" --static "$ROOT" &
SERVER_PID=$!

echo
echo "== 3/3 opening browser =="
DEMO_URL="http://127.0.0.1:$PORT_API/examples/web/index.html"
sleep 3
if command -v xdg-open >/dev/null 2>&1; then
  xdg-open "$DEMO_URL" >/dev/null 2>&1 || true
elif command -v open >/dev/null 2>&1; then
  open "$DEMO_URL" >/dev/null 2>&1 || true
else
  echo "open $DEMO_URL in your browser"
fi

cat <<EOF

== dev server running ==
  demo page : $DEMO_URL
  server API: http://127.0.0.1:$PORT_API  (token: $TOKEN)
  health     : http://127.0.0.1:$PORT_API/health

The axum server serves both the API and the web demo (static files from the
repo root). Press Ctrl+C to stop.
EOF
wait
