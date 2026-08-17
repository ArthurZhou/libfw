/* Repro: upload a file through the axum UI and capture everything. */
const { chromium } = require('playwright');

const BASE = process.env.BASE || 'http://127.0.0.1:8081';
const EXE = process.env.CHROME || `${process.env.HOME}/Library/Caches/ms-playwright/chromium-1228/chrome-mac-arm64/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing`;

(async () => {
  const browser = await chromium.launch({ executablePath: EXE, headless: true });
  const ctx = await browser.newContext();
  const page = await ctx.newPage();

  const consoleMsgs = [];
  page.on('console', (m) => consoleMsgs.push(`[console.${m.type()}] ${m.text()}`));
  page.on('pageerror', (e) => consoleMsgs.push(`[pageerror] ${e.message}`));
  page.on('request', (r) => {
    if (r.method() !== 'GET') consoleMsgs.push(`[req] ${r.method()} ${r.url()} body=${r.postData()?.length ?? 0}B hdrs=${JSON.stringify(r.headers())}`);
  });
  page.on('response', (r) => {
    if (r.request().method() !== 'GET') consoleMsgs.push(`[resp] ${r.status()} ${r.url()}`);
  });

  await page.goto(BASE, { waitUntil: 'networkidle' });
  // Connect with dev token
  await page.fill('#token', 'dev-token');
  await page.click('#connect');
  await page.waitForTimeout(500);

  // Pick a file
  await page.setInputFiles('#files', process.env.FILE || '/tmp/libfw-upload-test.bin');
  consoleMsgs.push('[ui] files input set, waiting for transfer to finish...');

  // Wait until state is completed or failed (max 60s)
  const deadline = Date.now() + 60000;
  let state = '?';
  while (Date.now() < deadline) {
    state = await page.textContent('#st-state').catch(() => '?');
    if (state === 'completed' || state === 'failed') break;
    await page.waitForTimeout(300);
  }

  const ui = {
    state,
    progress: await page.textContent('#st-progress').catch(() => '?'),
    pct: await page.textContent('#st-pct').catch(() => '?'),
    bar: await page.getAttribute('#bar', 'style').catch(() => '?'),
    log: await page.textContent('#log').catch(() => '?'),
    files: await page.textContent('#files').catch(() => '?'),
    listing: await page.textContent('#listing').catch(() => '?'),
  };
  console.log('=== UI STATE ===');
  console.log(JSON.stringify(ui, null, 2));
  console.log('=== CONSOLE / NETWORK ===');
  console.log(consoleMsgs.join('\n'));

  await browser.close();
})().catch((e) => { console.error('REPRO FAILED', e); process.exit(1); });