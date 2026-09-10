/* Browser download check: the SDK engine must fetch a large file as MANY
 * bounded range requests (never one whole-file 206), report live progress and
 * live tuning samples (RTT + throughput) *during* the transfer, and re-render
 * the panel. The file is seeded directly in the server's data dir, so this
 * suite needs no upload.
 *
 * Regression covered: a fresh adaptive ramp starts at the advertised minimum
 * window (1), and the download path used to take that as "one connection, one
 * big request" — no chunking, no resume granularity, no tuning feedback.
 */
const { chromium } = require('playwright');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { BASE, DATA, launch, tmpFile } = require('./harness.cjs');

const NAME = 'dl-check.bin';
const TOKEN = process.env.TOKEN || 'dev-token';
const SIZE = 12 * 1024 * 1024 + 4321; // many chunks + an odd tail

const results = [];
function check(name, ok, detail = '') {
  results.push({ name, ok, detail });
  console.log(`${ok ? 'PASS' : 'FAIL'}  ${name}${detail ? ' — ' + detail : ''}`);
}

(async () => {
  // ---------------------------------------------------------------- fixture
  fs.mkdirSync(DATA, { recursive: true });
  const payload = crypto.randomBytes(SIZE);
  fs.writeFileSync(path.join(DATA, NAME), payload);
  const want = crypto.createHash('sha256').update(payload).digest('hex');

  const browser = await launch(chromium);
  const ctx = await browser.newContext({ acceptDownloads: true });
  const page = await ctx.newPage();

  // Realistic-ish link: 120 ms RTT / 8 MiB/s down keeps the transfer running
  // past the engine's one-second measurement window.
  const cdp = await ctx.newCDPSession(page);
  await cdp.send('Network.enable');
  await cdp.send('Network.emulateNetworkConditions', {
    offline: false,
    latency: 120,
    downloadThroughput: 8 * 1024 * 1024,
    uploadThroughput: 4 * 1024 * 1024,
  });

  const pageErrors = [];
  page.on('pageerror', (e) => pageErrors.push(e.message));
  const rangeGets = [];
  page.on('request', (r) => {
    if (r.method() === 'GET' && r.url().includes(`/file/`) && r.headers()['range']) {
      rangeGets.push(r.headers()['range']);
    }
  });

  await page.goto(BASE, { waitUntil: 'networkidle' });
  await page.fill('#token', TOKEN);
  await page.click('#connect');
  await page.waitForFunction(
    () => document.getElementById('status-pill')?.textContent.includes('ready'),
    null,
    { timeout: 30000 }
  );

  // Capture the tuning events the engine pushes during the download.
  const tuneLines = [];
  const progressEvents = [];
  page.on('console', (m) => {
    if (m.text().startsWith('[dl-tune]')) tuneLines.push(m.text());
    if (m.text().startsWith('[dl-progress]')) progressEvents.push(Number(m.text().split(' ')[1]));
  });
  await page.evaluate(() => {
    const client = window.libfw.client;
    const orig = client._emit.bind(client);
    client._emit = (e) => {
      if (e.type === 'tuning') {
        console.log(
          `[dl-tune] phase=${e.phase} dw=${e.params?.downloadWindow} chunk=${e.params?.chunkSize} ` +
            `rtt=${e.stats?.rttMs?.toFixed?.(1)} mbps=${e.stats?.mbps?.toFixed?.(1)}`
        );
      } else if (e.type === 'progress') {
        console.log(`[dl-progress] ${e.done}`);
      }
      return orig(e);
    };
    // Headless Chromium has no interactive directory picker, so use the
    // in-memory download path (the mode this test is about).
    client._options.downloadMode = 'browser';
  });

  // ---------------------------------------------------------------- download
  await page.waitForSelector('#listing tr');
  const dlPromise = page.waitForEvent('download', { timeout: 120000 });
  await page.locator('#listing tr', { hasText: NAME }).first().locator('[data-act="dl"]').click();

  // Sample the UI while the transfer is in flight.
  const samples = [];
  const deadline = Date.now() + 120000;
  while (Date.now() < deadline) {
    const [state, phase, rtt, mbps, pct] = await Promise.all([
      page.textContent('#st-state').catch(() => '?'),
      page.textContent('#t-phase').catch(() => '?'),
      page.textContent('#t-rtt').catch(() => '?'),
      page.textContent('#t-mbps').catch(() => '?'),
      page.textContent('#st-pct').catch(() => '?'),
    ]);
    samples.push({ state, phase, rtt, mbps, pct });
    if (state !== 'running' && samples.length > 2) break;
    await page.waitForTimeout(250);
  }

  const download = await dlPromise;
  const out = tmpFile('dl-check-out.bin');
  await download.saveAs(out);
  const got = fs.readFileSync(out);
  const sha = crypto.createHash('sha256').update(got).digest('hex');

  // ---------------------------------------------------------------- asserts
  check('download: bytes match the source', got.length === SIZE && sha === want, `${got.length} vs ${SIZE}`);
  const uniqueRanges = new Set(rangeGets);
  check(
    'download: fetched as many bounded ranges (no single whole-file 206)',
    uniqueRanges.size >= 4,
    `${rangeGets.length} ranged GETs, ${uniqueRanges.size} distinct: ${[...uniqueRanges].slice(0, 4).join(', ')}…`
  );
  // The regression this guards: ONE request for `bytes=0-<last>`.
  check(
    'download: no single request covered the whole file',
    !rangeGets.includes(`bytes=0-${SIZE - 1}`),
    `last range: ${rangeGets[rangeGets.length - 1]}`
  );

  const final = samples[samples.length - 1];
  check('download: state reached a terminal value', final.state === 'completed', `state=${final.state}`);
  // Progress must move *while* a range GET is still in flight: the engine
  // reports per received body slice, so there are strictly more progress
  // events than completed range requests (the old code only reported once a
  // whole chunk had been buffered, so a big fetch looked frozen).
  check(
    'progress: reported inside each in-flight fetch (more events than ranges)',
    progressEvents.length > uniqueRanges.size,
    `${progressEvents.length} progress events vs ${uniqueRanges.size} ranged GETs`
  );
  check(
    'progress: monotonic byte counter',
    progressEvents.every((v, i) => i === 0 || v >= progressEvents[i - 1]),
    progressEvents.slice(0, 8).join(',')
  );
  check('progress: intermediate percentages observed', samples.some((s) => /^([1-9]\d?\.\d|99\.\d)%$/.test(s.pct)), samples.map((s) => s.pct).join(','));
  check(
    'tuning: live events during the transfer',
    tuneLines.length > 0,
    tuneLines.slice(-3).join(' | ') || '(none)'
  );
  const rttSeen = samples.some((s) => /\d+\s*ms/.test(s.rtt));
  const mbpsSeen = samples.some((s) => /Mb\/s$/.test(s.mbps));
  check('panel: live RTT rendered during a download', rttSeen, samples.map((s) => s.rtt).join(','));
  check('panel: live throughput rendered during a download', mbpsSeen, samples.map((s) => s.mbps).join(','));
  check('no page errors', pageErrors.length === 0, JSON.stringify(pageErrors));

  console.log('\n=== SUMMARY ===');
  const failed = results.filter((r) => !r.ok);
  console.log(`${results.length - failed.length}/${results.length} passed`);
  failed.forEach((f) => console.log(`  FAIL: ${f.name} ${f.detail}`));
  await browser.close();
  process.exit(failed.length ? 1 : 0);
})().catch((e) => {
  console.error('DOWNLOAD CHECK CRASHED', e);
  process.exit(2);
});
