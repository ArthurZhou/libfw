/* Verify the browser tuning cache: a settled ramp is stored in `localStorage`
 * and reused after a page reload, expires after `tuneTtlMs`, and is disabled by
 * `tuneTtlMs: 0`.
 *
 * The example UI exposes the live client as `window.libfw.client`, so this
 * suite drives the same `LibfwClient` a user would (upload via `#files`), reads
 * the tuning phase from `tuneStatus()` / the `tuning` events, and inspects the
 * cache rows directly.
 *
 *   node tests/e2e/tune-cache-check.cjs        (server on :8080, token dev-token)
 */
const { chromium } = require('playwright');
const fs = require('fs');
const crypto = require('crypto');
const { BASE, TOKEN, launch, tmpFile } = require('./harness.cjs');

const checks = [];
const check = (name, ok, detail = '') => {
  checks.push({ name, ok });
  console.log(`${ok ? 'PASS' : 'FAIL'}  ${name}${detail ? ` — ${detail}` : ''}`);
};

/** A fresh upload payload of `bytes` (a unique file name → a real transfer). */
function payload(bytes) {
  const p = tmpFile(`tune-cache-${crypto.randomUUID()}.bin`);
  fs.mkdirSync(require('path').dirname(p), { recursive: true });
  fs.writeFileSync(p, crypto.randomBytes(bytes));
  return p;
}

/** Cache rows for this origin, newest key first. */
const readCache = (page) =>
  page.evaluate(() => {
    const out = [];
    for (let i = 0; i < localStorage.length; i++) {
      const key = localStorage.key(i);
      if (key && key.startsWith('libfw.tune.')) out.push({ key, row: JSON.parse(localStorage.getItem(key)) });
    }
    return out;
  });

/** Connect the UI client and start capturing `tuning` events. */
async function connect(page, ttlMinutes) {
  await page.fill('#token', TOKEN);
  if (ttlMinutes != null) await page.fill('#tune-ttl', String(ttlMinutes));
  await page.click('#connect');
  await page.waitForFunction(() => !!window.libfw?.client, null, { timeout: 10000 });
  await page.evaluate(() => {
    window.__tuneEvents = [];
    const c = window.libfw.client;
    const orig = c._emit.bind(c);
    c._emit = (e) => { if (e.type === 'tuning') window.__tuneEvents.push(e.phase); return orig(e); };
  });
}

const tuneEvents = (page) => page.evaluate(() => window.__tuneEvents.slice());

/** Upload one file through the UI and wait for the transfer to finish. */
async function uploadOnce(page, bytes = 12 * 1024 * 1024) {
  const file = payload(bytes);
  await page.setInputFiles('#files', file);
  await page.waitForFunction(
    () => {
      const s = document.getElementById('st-state');
      return s && s.textContent !== 'running' && s.textContent !== 'idle';
    },
    null,
    { timeout: 120000 },
  );
  return page.textContent('#st-state');
}

(async () => {
  const browser = await launch(chromium);
  const ctx = await browser.newContext();
  const page = await ctx.newPage();
  // A link slow enough that the engine actually ramps (and settles) while the
  // first upload runs: at ~0.5 MB/s an 8 MiB file spans ~16 measurement
  // windows, which is what the explore-then-settle path needs.
  const cdp = await ctx.newCDPSession(page);
  await cdp.send('Network.enable');
  await cdp.send('Network.emulateNetworkConditions', {
    offline: false,
    latency: 100,
    downloadThroughput: 8 * 1024 * 1024,
    uploadThroughput: 512 * 1024,
  });

  // The first transfer has to converge (that is what gets cached); the later
  // ones only need to *start* the engine, so a small payload keeps it quick.
  const SETTLE_BYTES = 8 * 1024 * 1024;
  const QUICK_BYTES = 512 * 1024;

  await page.goto(BASE, { waitUntil: 'networkidle' });
  await page.evaluate(() => localStorage.clear());

  // --- 1. a settled ramp is cached ----------------------------------------
  await connect(page, 60);
  const first = await uploadOnce(page, SETTLE_BYTES);
  const events1 = await tuneEvents(page);
  const rows1 = await readCache(page);
  const origin = await page.evaluate(() => location.origin);
  const key = `libfw.tune.v1.upload.${origin}`;

  check('first transfer completes', first === 'completed', `state=${first}`);
  check('the ramp ran', events1.includes('ramping'), `events=${events1.join(',')}`);
  check('the ramp settled', events1.includes('settled'), `events=${events1.slice(-4).join(',')}`);
  check('the settle is cached', rows1.length === 1 && rows1[0].key === key, `keys=${rows1.map((r) => r.key).join(',')}`);
  const cached = rows1[0]?.row;
  check(
    'the cached row holds the settled params + a timestamp',
    !!cached && cached.v === 1 && !!cached.caps_hash && cached.saved_at_ms > 0 && cached.params.upload_window >= 1,
    JSON.stringify(cached?.params),
  );

  if (!cached) {
    console.log('\nthe cache was never written — the remaining checks cannot run');
    await browser.close();
    process.exit(1);
  }
  const tuned = cached.params;

  // --- 2. a page reload reuses it (no ramp) -------------------------------
  await page.reload({ waitUntil: 'networkidle' });
  await connect(page, 60);
  const statusBefore = await page.evaluate(() => window.libfw.client.tuneStatus());
  await page.setInputFiles('#files', payload(QUICK_BYTES));
  // The engine settles the parameters as soon as the transfer is prepared,
  // i.e. before any measurement window closes.
  await page.waitForFunction(() => window.__tuneEvents.includes('settled'), null, { timeout: 20000 });
  const early = await page.evaluate(() => window.libfw.client.tuneStatus());
  const events2 = await tuneEvents(page);
  const earlyParams = await page.evaluate(() => {
    const p = window.libfw.client.tuneStatus()?.params || {};
    return { concurrency: p.concurrency, uploadWindow: p.uploadWindow, chunkSize: p.chunkSize };
  });
  await page.waitForFunction(
    () => {
      const s = document.getElementById('st-state');
      return s && s.textContent !== 'running' && s.textContent !== 'idle';
    },
    null,
    { timeout: 120000 },
  );

  check('a fresh client starts unsettled', !statusBefore || statusBefore.phase !== 'settled', `phase=${statusBefore?.phase}`);
  check('reload reuses the cached settle', early?.phase === 'settled', `phase=${early?.phase}`);
  check('reload does not re-ramp', !events2.includes('ramping'), `events=${events2.join(',')}`);
  check(
    'the reused params match the cached row',
    earlyParams.concurrency === tuned.concurrency &&
      earlyParams.uploadWindow === tuned.upload_window &&
      earlyParams.chunkSize === tuned.chunk_size,
    JSON.stringify(earlyParams),
  );

  // --- 3. an expired row re-ramps ----------------------------------------
  await page.evaluate(({ suffix }) => {
    for (let i = 0; i < localStorage.length; i++) {
      const k = localStorage.key(i);
      if (!k || !k.startsWith('libfw.tune.') || !k.includes(suffix)) continue;
      const row = JSON.parse(localStorage.getItem(k));
      row.saved_at_ms = Date.now() - 61 * 60 * 1000; // 61 min old, TTL is 60
      localStorage.setItem(k, JSON.stringify(row));
    }
  }, { suffix: '.upload.' });
  await page.reload({ waitUntil: 'networkidle' });
  await connect(page, 60);
  const expired = await uploadOnce(page, QUICK_BYTES);
  const events3 = await tuneEvents(page);
  check('an expired row re-ramps', events3.includes('ramping'), `events=${events3.slice(0, 4).join(',')}`);
  check('the expired transfer still completes', expired === 'completed', `state=${expired}`);

  // --- 4. tuneTtlMs: 0 disables the cache entirely ------------------------
  await page.evaluate(() => localStorage.clear());
  await page.reload({ waitUntil: 'networkidle' });
  await connect(page, 0);
  const disabled = await uploadOnce(page, QUICK_BYTES);
  const events4 = await tuneEvents(page);
  const rows4 = await readCache(page);
  check('ttl = 0 re-ramps', events4.includes('ramping'), `events=${events4.slice(0, 4).join(',')}`);
  check('ttl = 0 writes nothing', rows4.length === 0, `keys=${rows4.map((r) => r.key).join(',')}`);
  check('the uncached transfer still completes', disabled === 'completed', `state=${disabled}`);

  await browser.close();
  const failed = checks.filter((c) => !c.ok).length;
  console.log(`\n${checks.length - failed}/${checks.length} checks passed`);
  process.exit(failed ? 1 : 0);
})().catch((e) => {
  console.error('TUNE-CACHE-CHECK CRASHED', e);
  process.exit(2);
});
