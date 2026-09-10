/* Shared, cross-platform plumbing for the browser e2e suites.
 *
 * Everything that used to be hard-coded per script (the macOS Playwright
 * Chrome path, `/tmp/...` scratch dirs, port 8081) lives here so the same
 * scripts run on macOS, Linux and Windows:
 *
 *   BASE        web UI origin            (env BASE, default :8080 like dev-test.bat)
 *   TOKEN       bearer token             (env TOKEN, default `dev-token`)
 *   DATA        the *server's* data dir  (env LIBFW_DATA, default <tmp>/libfw-storage)
 *   TMP         local scratch for fixtures (env LIBFW_TMP, default <tmp>/libfw-e2e)
 *   CHROME      explicit browser binary  (env CHROME)
 *   BROWSER_CHANNEL  force a Playwright channel (`chrome`, `msedge`, `chromium`)
 *
 * `launch()` resolves the browser itself (bundled Chromium first, then a system
 * Chrome/Edge), so the suites run even when `npx playwright install` is blocked.
 */
const os = require('os');
const path = require('path');

const BASE = process.env.BASE || 'http://127.0.0.1:8080';
const TOKEN = process.env.TOKEN || 'dev-token';
const DATA = process.env.LIBFW_DATA || path.join(os.tmpdir(), 'libfw-storage');
const TMP = process.env.LIBFW_TMP || path.join(os.tmpdir(), 'libfw-e2e');

/** Browser launch candidates, most explicit first. */
function candidates(extra) {
  const out = [];
  if (process.env.CHROME) out.push({ executablePath: process.env.CHROME });
  if (process.env.BROWSER_CHANNEL) out.push({ channel: process.env.BROWSER_CHANNEL });
  // Playwright's own download ...
  out.push({});
  // ... then a system Chromium/Chrome/Edge: Playwright drives those happily
  // (they support the CDP throttling and the File System Access API the UI
  // uses), so the suites still run when `npx playwright install` is not
  // available (no network / CDN blocked).
  for (const channel of ['chrome', 'msedge', 'chromium']) out.push({ channel });
  for (const exe of [
    'C:/Program Files/Google/Chrome/Application/chrome.exe',
    'C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe',
    '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
    '/usr/bin/google-chrome',
    '/usr/bin/chromium',
  ]) {
    out.push({ executablePath: exe });
  }
  return out.map((o) => ({ ...o, ...extra }));
}

/**
 * Launch Chromium, falling back through the candidates above, so every suite
 * shares one browser-resolution policy.
 */
async function launch(chromium, extra = {}) {
  const tried = [];
  let lastErr;
  for (const opts of candidates({ headless: true, ...extra })) {
    try {
      return await chromium.launch(opts);
    } catch (err) {
      tried.push(`${opts.channel ?? opts.executablePath ?? 'bundled'}: ${String(err.message).split('\n')[0]}`);
      lastErr = err;
    }
  }
  throw new Error(`no usable Chromium found (tried ${tried.join(' | ')})`, { cause: lastErr });
}

/** Scratch file inside TMP (created on demand). */
function tmpFile(name) {
  require('fs').mkdirSync(TMP, { recursive: true });
  return path.join(TMP, name);
}

/** A deterministic sample file inside TMP (created/reused on demand). */
function sampleFile(name = 'upload-test.bin', bytes = 2 * 1024 * 1024) {
  const fs = require('fs');
  const crypto = require('crypto');
  const p = tmpFile(name);
  if (!fs.existsSync(p) || fs.statSync(p).size !== bytes) {
    fs.writeFileSync(p, crypto.randomBytes(bytes));
  }
  return p;
}

module.exports = {
  BASE,
  TOKEN,
  DATA,
  TMP,
  launch,
  tmpFile,
  sampleFile,
};
