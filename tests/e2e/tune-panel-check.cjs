/* Verify the adaptive-tuning panel renders live values during a transfer. */
const { chromium } = require('playwright');
const fs = require('fs');
const crypto = require('crypto');

const BASE = process.env.BASE || 'http://127.0.0.1:8081';
const EXE = process.env.CHROME || `${process.env.HOME}/Library/Caches/ms-playwright/chromium-1228/chrome-mac-arm64/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing`;

(async () => {
  const browser = await chromium.launch({ executablePath: EXE, headless: true });
  const ctx = await browser.newContext();
  const page = await ctx.newPage();
  const cdp = await ctx.newCDPSession(page);
  await cdp.send('Network.enable');
  await cdp.send('Network.emulateNetworkConditions', {
    offline: false, latency: 120,
    downloadThroughput: 8 * 1024 * 1024, uploadThroughput: 4 * 1024 * 1024,
  });

  const tuneEvents = [];
  await page.goto(BASE, { waitUntil: 'networkidle' });
  await page.fill('#token', 'dev-token');
  await page.click('#connect');
  await page.waitForTimeout(400);

  // Capture tuning events flowing through onEvent.
  await page.evaluate(() => {
    const orig = window.libfw.client._emit.bind(window.libfw.client);
    window.libfw.client._emit = (e) => { if (e.type === 'tuning') console.log(`[tune-ev] ${e.phase} conc=${e.params?.concurrency} uw=${e.params?.uploadWindow} lvl=${e.params?.compressLevel} rtt=${e.stats?.rttMs?.toFixed?.(0)} mbps=${e.stats?.mbps?.toFixed?.(1)}`); orig(e); };
  });
  page.on('console', (m) => { if (m.text().startsWith('[tune-ev]')) tuneEvents.push(m.text()); });

  fs.mkdirSync('/tmp/libfw-e2e', { recursive: true });
  const p = '/tmp/libfw-e2e/tune-check.bin';
  fs.writeFileSync(p, crypto.randomBytes(14 * 1024 * 1024));
  await page.setInputFiles('#files', p);

  // Sample the panel while transferring.
  const samples = [];
  const deadline = Date.now() + 60000;
  while (Date.now() < deadline) {
    const state = await page.textContent('#st-state').catch(() => '?');
    const phase = await page.textContent('#t-phase').catch(() => '?');
    const conc = await page.textContent('#t-conc').catch(() => '?');
    const lvl = await page.textContent('#t-level').catch(() => '?');
    const rtt = await page.textContent('#t-rtt').catch(() => '?');
    const mbps = await page.textContent('#t-mbps').catch(() => '?');
    samples.push({ state, phase, conc, lvl, rtt, mbps });
    if (state === 'idle' && samples.length > 2) break;
    await page.waitForTimeout(250);
  }

  const last = samples[samples.length - 1];
  console.log('=== tune events ===');
  console.log(tuneEvents.join('\n') || '(none)');
  console.log('=== panel samples ===');
  console.log(JSON.stringify(samples, null, 1));
  const populated = samples.some((s) => s.phase !== '—' && s.phase !== 'uninitialized');
  const hasParams = samples.some((s) => /^\d+$/.test(s.conc) && /^-?\d+$/.test(s.lvl));
  // Upload ticks carry no RTT sample (XHR cannot expose TTFB), so only
  // require the throughput stat; RTT renders for downloads.
  const hasStats = samples.some((s) => s.mbps.endsWith('Mb/s'));
  const engaged = tuneEvents.length > 0;
  console.log(`\nphase shown: ${populated ? 'YES' : 'NO'} | params shown: ${hasParams ? 'YES' : 'NO'} | stats shown: ${hasStats ? 'YES' : 'NO'} | tuning events: ${engaged ? 'YES' : 'NO'}`);
  await browser.close();
  process.exit(populated && hasParams && hasStats && engaged ? 0 : 1);
})().catch((e) => { console.error('TUNE-CHECK CRASHED', e); process.exit(2); });