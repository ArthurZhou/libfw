@echo off
rem ============================================================================
rem  libfw - one-click dev-mode test launcher (Windows)
rem
rem   1. runs `cargo test --workspace`
rem   2. starts the axum dev server on :8080 (token: dev-token); the server
rem      embeds the web UI at `/` and serves the SDK from the repo
rem   3. opens the browser to the web UI
rem
rem  Requirement: cargo on PATH.
rem
rem  The WASM engine must be built once for the web UI to work:
rem    wasm-pack build crates/libfw-client --target web --out-dir ..\..\sdk\pkg --release
rem
rem  Usage: double-click this file. The server runs in its own window; close
rem  it (or press any key here) when done.
rem ============================================================================

setlocal
set "ROOT=%~dp0"
set "PORT_API=8080"
set "TOKEN=dev-token"

cd /d "%ROOT%"

echo == libfw dev-mode testing ==
echo root: %ROOT%

where cargo >nul 2>nul
if errorlevel 1 (
  echo error: cargo not found - install the Rust toolchain ^(https://rustup.rs^).
  pause
  exit /b 1
)

if not exist "sdk\pkg\libfw_client.js" (
  echo warning: sdk\pkg is missing - the web UI needs the WASM engine:
  echo   wasm-pack build crates/libfw-client --target web --out-dir ..\..\sdk\pkg --release
)

echo.
echo == 1/3 cargo test --workspace ==
call cargo test --workspace
if errorlevel 1 (
  echo.
  echo tests failed.
  pause
  exit /b 1
)

echo.
echo == 2/3 starting axum dev server on :%PORT_API% ^(token: %TOKEN%^) ==
if not exist "dev-data" mkdir "dev-data"
start "libfw axum server" cmd /k "cargo run -p axum-server -- dev-data %PORT_API%"

echo.
echo == 3/3 opening browser ==
set "WEB_URL=http://127.0.0.1:%PORT_API%/"
timeout /t 3 /nobreak >nul
start "" "%WEB_URL%"

echo.
echo == dev server running ==
echo   web UI    : %WEB_URL%
echo   server API: http://127.0.0.1:%PORT_API%   ^(token: %TOKEN%^)
echo   health    : http://127.0.0.1:%PORT_API%/health
echo.
echo The server runs in its own window. Close it when done.
echo.
pause
endlocal
