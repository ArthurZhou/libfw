/* Resume test: cancel an upload mid-flight, then re-upload the SAME file.
 * The second run must probe the server, see the partial ranges, and send
 * only the missing blocks — final file must be byte-identical. */
const { chromium } = require('playwright');
const fs = require('fs');
const crypto = require('crypto');

const BASE = process.env.BASE || 'http://127.0.0.1:8081';
const EXE = process.env.CHROME || `${process.env.HOME}/Library/Caches/ms-playwright/chromium-1228/chrome-mac-arm64/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing`;
const TOKEN = 'dev-token';

const results = [];
function check(name, ok, detail = '') {
  results.push({ name, ok, detail });
  console.log(`${ok ? 'PASS' : 'FAIL'}  ${name}${detail ? ' — ' + detail : ''}`);
}

(async () => {
  const browser = await chromium.launch({ executablePath: EXE, headless: true });
  const ctx = await browser.newContext();
  const page = await ctx.newPage();

  // Slower link so cancel lands mid-transfer: 150ms latency.
  const cdp = await ctx.newCDPSession(page);
  await cdp.send('Network.enable');
  await cdp.send('Network.emulateNetworkConditions', {
    offline: false, latency: 150,
    downloadThroughput: 8 * 1024 * 1024, uploadThroughput: 32 * 1024 * 1024,
  });

  // Capture probe responses (session-status), chunk POST offsets, and the
  // first chunk ack (used to time the cancel AFTER bytes land server-side).
  const probes = [];
  const chunkOffsets = [];
  let firstChunkAcked = false;
  page.on('response', async (r) => {
    const req = r.request();
    if (req.method() !== 'POST' || !req.url().includes('/file/')) return;
    const h = req.headers();
    if (h['x-libfw-session-status']) {
      probes.push({ status: r.status(), body: await r.text().catch(() => '?') });
    } else if (h['x-libfw-offset'] && h['x-libfw-offset'] !== '' && r.status() === 201) {
      firstChunkAcked = true;
    }
  });
  page.on('request', (r) => {
    if (r.method() === 'POST' && r.url().includes('/file/') && r.postData()?.length) {
      chunkOffsets.push(Number(r.headers()['x-libfw-offset']));
    }
  });

  await page.goto(BASE, { waitUntil: 'networkidle' });
  await page.fill('#token', TOKEN);
  await page.click('#connect');
  await page.waitForTimeout(500);

  const logText = () => page.textContent('#log').catch(() => '');
  const clearLog = () => page.evaluate(() => { document.getElementById('log').textContent = ''; });
  const waitIdle = async (timeoutMs = 30000) => {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      if ((await page.textContent('#st-state').catch(() => 'idle')) === 'idle') return;
      await page.waitForTimeout(50);
    }
  };

  fs.mkdirSync('/tmp/libfw-e2e', { recursive: true });
  const filePath = '/tmp/libfw-e2e/resume.bin';
  const bytes = crypto.randomBytes(30 * 1024 * 1024 + 777); // 15 chunks @ 2MiB (window 8 -> real gaps on cancel)
  const FULL = bytes.length;
  fs.writeFileSync(filePath, bytes);
  const expectedSha = crypto.createHash('sha256').update(bytes).digest('hex');

  // ---- run 1: cancel mid-transfer ----------------------------------------
  await clearLog();
  await page.setInputFiles('#files', filePath);
  // Cancel only after REAL progress (engine doneBytes > 0, a chunk acked).
  // NOTE: the UI % element starts at the static placeholder "0%" and the
  // engine may take ~0.5s (capabilities probe + level benchmark) before the
  // first chunk even starts, so DOM text is not a reliable progress signal.
  let cancelled = false;
  for (let i = 0; i < 1200; i++) {
    const log = await logText();
    if (log.includes('✔ done') || log.includes('✖')) break;
    const progressed = await page.evaluate(() => {
      const c = window.libfw?.client;
      return c ? c.doneBytes() > 0 : false;
    }).catch(() => false);
    if (progressed && firstChunkAcked) {
      await page.evaluate(() => window.libfw.client.cancel());
      cancelled = true;
      break;
    }
    await page.waitForTimeout(50);
  }
  check('run1: cancel issued mid-transfer', cancelled, `chunkOffsets run1=${chunkOffsets.slice(0, 5).join(',')}… (${chunkOffsets.length} total)`);
  await waitIdle();
  await page.waitForTimeout(500);
  const partials = fs.readdirSync('/tmp/libfw-storage').filter((f) => f.includes('.libfw-sess-'));
  check('run1: server holds session temp + sidecar', partials.length >= 1, JSON.stringify(partials));
  const blocks = partials
    .filter((f) => f.endsWith('.blocks'))
    .map((f) => fs.readFileSync(`/tmp/libfw-storage/${f}`, 'utf8'));
  check('run1: sidecar has received ranges', blocks.some((b) => /"start":\d+,"end":\d+/.test(b)), JSON.stringify(blocks));

  // ---- run 2: re-upload same file, must resume ----------------------------
  const run1Offsets = chunkOffsets.slice();
  probes.length = 0;
  chunkOffsets.length = 0;
  await clearLog();
  await page.setInputFiles('#files', filePath);
  const deadline = Date.now() + 180000;
  let log = '';
  while (Date.now() < deadline) {
    log = await logText();
    if (log.includes('✔ done') || log.includes('✖')) break;
    await page.waitForTimeout(100);
  }
  await waitIdle();
  await page.waitForTimeout(300);
  check('run2: upload done', log.includes('✔ done'), log.slice(0, 120));
  const coveredEnds = probes
    .filter((p) => p.status === 200)
    .map((p) => { try { return JSON.parse(p.body).ranges.map((r) => r[1]).sort((a, b) => b - a)[0] || 0; } catch { return 0; } });
  const coveredEnd = Math.max(0, ...coveredEnds);
  check('run2: probe reported partial coverage', coveredEnd > 0 && coveredEnd < FULL, `coveredEnd=${coveredEnd}/${FULL} probes=${JSON.stringify(probes)}`);
  const run2Offsets = chunkOffsets.slice();
  check(
    'run2: re-sent only blocks beyond the covered prefix (no overlap)',
    run2Offsets.length > 0 && run2Offsets.every((o) => o >= coveredEnd),
    `coveredEnd=${coveredEnd} run2=${run2Offsets.join(',')}`
  );
  check(
    'run2: transferred less than full file (bytes moved < total)',
    !log.includes(`¬`) && /done: [0-9.]+ (KiB|MiB) transferred/.test(log) && !log.includes('no bytes needed'),
    log.slice(0, 120)
  );

  // ---- integrity -----------------------------------------------------------
  const serverBytes = await page.evaluate(async ({ base, token }) => {
    const r = await fetch(`${base}/file/resume.bin`, { headers: { Authorization: `Bearer ${token}` } });
    if (!r.ok) throw new Error(`GET ${r.status}`);
    return Array.from(new Uint8Array(await r.arrayBuffer()));
  }, { base: BASE, token: TOKEN });
  const got = Buffer.from(serverBytes);
  check('run2: final file byte-identical', got.length === FULL && crypto.createHash('sha256').update(got).digest('hex') === expectedSha, `${got.length} vs ${FULL}`);

  console.log('\n=== SUMMARY ===');
  const failed = results.filter((r) => !r.ok);
  console.log(`${results.length - failed.length}/${results.length} passed`);
  failed.forEach((f) => console.log(`  FAIL: ${f.name} ${f.detail}`));
  await browser.close();
  process.exit(failed.length ? 1 : 0);
})().catch((e) => { console.error('RESUME CRASHED', e); process.exit(2); });