/* Diagnostic: does the browser engine actually keep `upload_window` blocks in
 * flight, or does it serialize them? Prints the block request timeline (start /
 * end per POST), the observed concurrency, and the tuning events, so a slow
 * transfer can be attributed to the client or to the emulated link.
 *
 *   node tests/e2e/upload-concurrency.cjs [fileMiB] [uploadMiBps]
 */
const { chromium } = require('playwright');
const fs = require('fs');
const crypto = require('crypto');
const { BASE, launch, sampleFile } = require('./harness.cjs');

const MIB = process.env.FILE_MIB ? Number(process.env.FILE_MIB) : 16;
const UP_MIBPS = process.env.UP_MIBPS ? Number(process.env.UP_MIBPS) : 4;
const TOKEN = process.env.TOKEN || 'dev-token';

(async () => {
  const filePath = sampleFile(`conc-${MIB}mib.bin`, MIB * 1024 * 1024);
  const want = crypto.createHash('sha256').update(fs.readFileSync(filePath)).digest('hex');

  const browser = await launch(chromium);
  const ctx = await browser.newContext();
  const page = await ctx.newPage();
  const cdp = await ctx.newCDPSession(page);
  await cdp.send('Network.enable');
  await cdp.send('Network.emulateNetworkConditions', {
    offline: false,
    latency: 100,
    downloadThroughput: 8 * 1024 * 1024,
    uploadThroughput: UP_MIBPS * 1024 * 1024,
  });

  const tune = [];
  const t0 = Date.now();
  const blocks = new Map(); // request -> { offset, start }
  page.on('console', (m) => {
    if (m.text().startsWith('[tune]')) tune.push(`${Date.now() - t0}ms ${m.text()}`);
  });
  page.on('request', (r) => {
    if (r.method() !== 'POST' || !r.url().includes('/file/')) return;
    const h = r.headers();
    if (h['x-libfw-final'] || !r.postData()) return;
    blocks.set(r, { offset: Number(h['x-libfw-offset']), bytes: r.postData().length, start: Date.now() });
  });
  page.on('requestfinished', (r) => {
    const b = blocks.get(r);
    if (b) b.end = Date.now();
  });
  page.on('requestfailed', (r) => {
    const b = blocks.get(r);
    if (b) b.end = Date.now();
  });

  await page.goto(BASE, { waitUntil: 'networkidle' });
  await page.fill('#token', TOKEN);
  await page.click('#connect');
  await page.waitForFunction(
    () => document.getElementById('status-pill')?.textContent.includes('ready'),
    null,
    { timeout: 30000 }
  );
  await page.evaluate(() => {
    const client = window.libfw.client;
    const orig = client._emit.bind(client);
    client._emit = (e) => {
      if (e.type === 'tuning') {
        console.log(
          `[tune] phase=${e.phase} uw=${e.params?.uploadWindow} chunk=${e.params?.chunkSize} ` +
            `conc=${e.params?.concurrency} mbps=${e.stats?.mbps?.toFixed?.(1)}`
        );
      }
      return orig(e);
    };
  });

  const started = Date.now();
  await page.setInputFiles('#files', filePath);
  for (let i = 0; i < 2400; i++) {
    const log = await page.textContent('#log').catch(() => '');
    if (log.includes('✔') || log.includes('✖')) break;
    await page.waitForTimeout(100);
  }
  const elapsed = (Date.now() - started) / 1000;

  // Overlap analysis over the block requests.
  const events = [];
  for (const b of blocks.values()) {
    events.push({ t: b.start, d: +1 });
    if (b.end) events.push({ t: b.end, d: -1 });
  }
  events.sort((a, b) => a.t - b.t || a.d - b.d);
  let live = 0;
  let maxLive = 0;
  for (const e of events) {
    live += e.d;
    maxLive = Math.max(maxLive, live);
  }
  const durs = [...blocks.values()].filter((b) => b.end).map((b) => b.end - b.start).sort((a, b) => a - b);
  const median = durs.length ? durs[Math.floor(durs.length / 2)] : 0;
  const wire = [...blocks.values()].reduce((n, b) => n + b.bytes, 0);

  console.log('=== tuning events ===');
  console.log(tune.join('\n') || '(none)');
  console.log('=== block timeline ===');
  console.log(
    `blocks=${blocks.size} wire=${(wire / 1048576).toFixed(2)}MiB elapsed=${elapsed.toFixed(1)}s ` +
      `=> ${(wire / 1048576 / elapsed).toFixed(2)}MiB/s (link cap ${UP_MIBPS}MiB/s)`
  );
  console.log(`max in-flight block POSTs=${maxLive} median block duration=${median}ms`);
  console.log(`first 12 offsets: ${[...blocks.values()].slice(0, 12).map((b) => b.offset).join(',')}`);

  const serverSha = await page.evaluate(async ({ base, token, name }) => {
    const r = await fetch(`${base}/file/${name}`, { headers: { Authorization: `Bearer ${token}` } });
    if (!r.ok) throw new Error(`GET ${r.status}`);
    const d = await crypto.subtle.digest('SHA-256', await r.arrayBuffer());
    return [...new Uint8Array(d)].map((b) => b.toString(16).padStart(2, '0')).join('');
  }, { base: BASE, token: TOKEN, name: `conc-${MIB}mib.bin` });
  console.log(`integrity: ${serverSha === want ? 'MATCH' : `MISMATCH (${serverSha.slice(0, 12)} vs ${want.slice(0, 12)})`}`);

  await browser.close();
})().catch((e) => {
  console.error('CONCURRENCY DIAG CRASHED', e);
  process.exit(2);
});
