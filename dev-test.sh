#!/usr/bin/env bash
# =============================================================================
# libfw — one-click dev-mode test launcher (Linux / macOS)
#
#   1. runs `cargo test --workspace`
#   2. starts the axum dev server on :8080 (token: dev-token)
#   3. serves the web demo from the repo root on :5173
#   4. opens the browser to the demo page
#
# Requirements:
#   - cargo (Rust toolchain)
#   - python3 (or python) — used only to serve the static web demo
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
WEB_PID=""

cleanup() {
  trap - EXIT INT TERM
  echo
  echo "== stopping dev servers =="
  [[ -n "$SERVER_PID" ]] && kill "$SERVER_PID" 2>/dev/null || true
  [[ -n "$WEB_PID" ]] && kill "$WEB_PID" 2>/dev/null || true
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
echo "== 2/4 starting axum dev server on :$PORT_API (token: $TOKEN) =="
mkdir -p "$DATA_DIR"
cargo run -p axum-server -- "$DATA_DIR" "$PORT_API" &
SERVER_PID=$!

echo
echo "== 3/4 serving web demo on :$PORT_WEB =="
PY=""
if command -v python3 >/dev/null 2>&1; then
  PY="python3"
elif command -v python >/dev/null 2>&1; then
  PY="python"
fi
if [[ -n "$PY" ]]; then
  # Serve from the repo ROOT so examples/web/index.html can import ../../sdk/.
  (cd "$ROOT" && exec "$PY" -m http.server "$PORT_WEB") &
  WEB_PID=$!
else
  echo "warning: no python found — serve '$ROOT' manually (e.g. npx serve)."
fi

echo
echo "== 4/4 opening browser =="
DEMO_URL="http://127.0.0.1:$PORT_WEB/examples/web/index.html"
sleep 2
if command -v xdg-open >/dev/null 2>&1; then
  xdg-open "$DEMO_URL" >/dev/null 2>&1 || true
elif command -v open >/dev/null 2>&1; then
  open "$DEMO_URL" >/dev/null 2>&1 || true
else
  echo "open $DEMO_URL in your browser"
fi

cat <<EOF

== dev servers running ==
  demo page : $DEMO_URL
  server API: http://127.0.0.1:$PORT_API  (token: $TOKEN)

Press Ctrl+C to stop everything.
EOF
wait
