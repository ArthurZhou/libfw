/* Instrumented repro: log what getFileList / upload internals return. */
const { chromium } = require('playwright');

const BASE = process.env.BASE || 'http://127.0.0.1:8081';
const EXE = process.env.CHROME || `${process.env.HOME}/Library/Caches/ms-playwright/chromium-1228/chrome-mac-arm64/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing`;

(async () => {
  const browser = await chromium.launch({ executablePath: EXE, headless: true });
  const ctx = await browser.newContext();
  const page = await ctx.newPage();
  const logs = [];
  page.on('console', (m) => logs.push(`[console.${m.type()}] ${m.text()}`));
  page.on('pageerror', (e) => logs.push(`[pageerror] ${e.message}`));
  page.on('request', (r) => {
    if (r.method() !== 'GET') logs.push(`[req] ${r.method()} ${r.url()} body=${r.postData()?.length ?? 0}B`);
  });

  // Patch _getFileList on the prototype BEFORE any client is constructed.
  await page.addInitScript(() => {
    const orig = window.LibfwClient?.prototype?._getFileList;
    if (orig) {
      window.LibfwClient.prototype._getFileList = async function (...a) {
        const r = await orig.apply(this, a);
        console.log(`[patch] getFileList -> ${JSON.stringify(r)}`);
        return r;
      };
    }
  });

  await page.goto(BASE, { waitUntil: 'networkidle' });
  await page.fill('#token', 'dev-token');
  await page.click('#connect');
  await page.waitForTimeout(500);

  // Patch now that module is loaded (addInitScript ran before module import too, but guard).
  await page.evaluate(() => {
    const client = window.libfw?.client;
    if (!client) return console.log('[patch] no client');
    const proto = Object.getPrototypeOf(client);
    if (!proto._getFileList.__patched) {
      const orig = proto._getFileList;
      proto._getFileList = async function (...a) {
        const r = await orig.apply(this, a);
        console.log(`[patch] getFileList -> ${JSON.stringify(r)}`);
        return r;
      };
      proto._getFileList.__patched = true;
    }
    // Also trace readFile
    const origRead = proto._readFile;
    proto._readFile = async function (...a) {
      const r = await origRead.apply(this, a);
      console.log(`[patch] readFile(${a.join(',')}) -> ${r?.length} bytes`);
      return r;
    };
  });

  await page.setInputFiles('#files', process.env.FILE || '/tmp/libfw-upload-test.bin');
  const deadline = Date.now() + 60000;
  let state = '?';
  while (Date.now() < deadline) {
    state = await page.textContent('#st-state').catch(() => '?');
    if (state === 'completed' || state === 'failed') break;
    await page.waitForTimeout(300);
  }
  const plan = await page.evaluate(() => window.libfw?.client?._uploadPlan ?? 'NO CLIENT');
  const filesMap = await page.evaluate(() => [...(window.libfw?.client?._uploadFiles?.keys?.() ?? [])]);
  console.log('=== state:', state);
  console.log('=== _uploadPlan:', JSON.stringify(plan));
  console.log('=== _uploadFiles keys:', JSON.stringify(filesMap));
  console.log('=== log:', JSON.stringify(await page.textContent('#log')));
  console.log('=== console/network ===');
  console.log(logs.join('\n'));
  await browser.close();
})().catch((e) => { console.error('REPRO FAILED', e); process.exit(1); });