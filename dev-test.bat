@echo off
rem ============================================================================
rem  libfw - one-click dev-mode test launcher (Windows)
rem
rem   1. runs `cargo test --workspace`
rem   2. starts the axum dev server on :8080 (token: dev-token)
rem   3. serves the web demo from the repo root on :5173
rem   4. opens the browser to the demo page
rem
rem  Requirements: cargo and python on PATH (python is only used to serve
rem  the static web demo).
rem
rem  Usage: double-click this file. The two servers run in their own
rem  windows; close them (or press any key here) when done.
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
echo == 1/4 cargo test --workspace ==
call cargo test --workspace
if errorlevel 1 (
  echo.
  echo tests failed.
  pause
  exit /b 1
)

echo.
echo == 2/4 starting axum dev server on :%PORT_API% ^(token: %TOKEN%^) ==
if not exist "dev-data" mkdir "dev-data"
start "libfw axum server" cmd /k "cargo run -p axum-server -- dev-data %PORT_API%"

echo.
echo == 3/4 serving web demo on :%PORT_WEB% ==
where python >nul 2>nul
if errorlevel 1 (
  echo warning: python not found - serve "%ROOT%" manually ^(e.g. npx serve^).
) else (
  rem Serve from the repo ROOT so examples\web\index.html can import ..\..\sdk\.
  start "libfw web demo" cmd /k "python -m http.server %PORT_WEB%"
)

echo.
echo == 4/4 opening browser ==
set "DEMO_URL=http://127.0.0.1:%PORT_WEB%/examples/web/index.html"
timeout /t 2 /nobreak >nul
start "" "%DEMO_URL%"

echo.
echo == dev servers running ==
echo   demo page : %DEMO_URL%
echo   server API: http://127.0.0.1:%PORT_API%   ^(token: %TOKEN%^)
echo.
echo The servers run in their own windows. Close them when done.
echo.
pause
endlocal
