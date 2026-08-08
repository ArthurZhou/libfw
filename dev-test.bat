@echo off
rem ============================================================================
rem  libfw - one-click dev-mode test launcher (Windows)
rem
rem   1. runs `cargo test --workspace`
rem   2. starts the axum dev server on :8080 (token: dev-token); it also serves
rem      the web demo from the repo root (via --static), so no python is needed
rem   3. opens the browser to the demo page
rem
rem  Requirement: cargo on PATH.
rem
rem  Usage: double-click this file. The server runs in its own window; close
rem  it (or press any key here) when done.
rem ============================================================================

setlocal
set "ROOT=%~dp0"
set "PORT_API=8080"
set "PORT_WEB=5173"
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
  echo warning: sdk\pkg is missing - run:
  echo   wasm-pack build crates/libfw-client --target web --out-dir ../../sdk/pkg --release
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
rem --static serves the web demo from the repo root (no python needed).
start "libfw axum server" cmd /k "cargo run -p axum-server -- dev-data %PORT_API% --static %ROOT%"

echo.
echo == 3/3 opening browser ==
set "DEMO_URL=http://127.0.0.1:%PORT_API%/examples/web/index.html"
timeout /t 3 /nobreak >nul
start "" "%DEMO_URL%"

echo.
echo == dev server running ==
echo   demo page : %DEMO_URL%
echo   server API: http://127.0.0.1:%PORT_API%   ^(token: %TOKEN%^)
echo   health     : http://127.0.0.1:%PORT_API%/health
echo.
echo The server runs in its own window. Close it when done.
echo.
pause
endlocal
