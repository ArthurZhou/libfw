/* Instrument _collectProvidedFiles + _ready to find where the plan goes empty. */
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

  await page.goto(BASE, { waitUntil: 'networkidle' });
  await page.fill('#token', 'dev-token');
  await page.click('#connect');
  await page.waitForTimeout(300);

  await page.evaluate(() => {
    const proto = Object.getPrototypeOf(window.libfw.client);
    const orig = proto._collectProvidedFiles;
    proto._collectProvidedFiles = async function (...a) {
      const arg = a[0];
      const info = {
        isArray: Array.isArray(arg),
        len: arg?.length,
        firstIsFile: arg?.[0] instanceof File,
        firstCtor: arg?.[0]?.constructor?.name,
        firstKeys: arg?.[0] ? Object.keys(arg[0]).slice(0, 8) : null,
      };
      console.log(`[patch] _collectProvidedFiles arg=${JSON.stringify(info)}`);
      const r = await orig.apply(this, a);
      console.log(`[patch] _collectProvidedFiles -> ${JSON.stringify(r)}`);
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
  console.log('=== state:', state);
  console.log('=== console ===');
  console.log(logs.join('\n'));
  await browser.close();
})().catch((e) => { console.error('REPRO FAILED', e); process.exit(1); });