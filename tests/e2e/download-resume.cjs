/* Download resume in **fs mode** (File System Access API), verified against
 * OPFS so no native directory picker is needed: the SDK is given a
 * `FileSystemDirectoryHandle` via the `directoryHandle` option, downloads a
 * large file, is cancelled mid-transfer, and then downloads it again.
 *
 * The second run must (a) re-fetch only the bytes the partial is missing and
 * (b) leave a byte-identical file on disk. A resumed append that starts at
 * position 0 (instead of seeking past the partial's prefix) fails (b) — the
 * tail overwrites the prefix.
 */
const { chromium } = require('playwright');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { BASE, DATA, TOKEN, launch } = require('./harness.cjs');

const NAME = 'dl-resume.bin';
const SIZE = 20 * 1024 * 1024 + 1234;
const DIR = 'libfw-e2e-resume';

const results = [];
function check(name, ok, detail = '') {
  results.push({ name, ok, detail });
  console.log(`${ok ? 'PASS' : 'FAIL'}  ${name}${detail ? ' — ' + detail : ''}`);
}

const sha = (buf) => crypto.createHash('sha256').update(buf).digest('hex');

(async () => {
  // ---------------------------------------------------------------- fixture
  fs.mkdirSync(DATA, { recursive: true });
  const payload = crypto.randomBytes(SIZE); // incompressible: wire ≈ file
  fs.writeFileSync(path.join(DATA, NAME), payload);
  const want = sha(payload);

  const browser = await launch(chromium);
  const ctx = await browser.newContext();
  const page = await ctx.newPage();

  // 4 MiB/s: long enough to cancel mid-transfer.
  const cdp = await ctx.newCDPSession(page);
  await cdp.send('Network.enable');
  await cdp.send('Network.emulateNetworkConditions', {
    offline: false,
    latency: 100,
    downloadThroughput: 4 * 1024 * 1024,
    uploadThroughput: 4 * 1024 * 1024,
  });

  const ranges = [];
  page.on('request', (r) => {
    if (r.method() === 'GET' && r.url().includes('/file/') && r.headers()['range']) {
      ranges.push(r.headers()['range']);
    }
  });
  const pageErrors = [];
  page.on('pageerror', (e) => pageErrors.push(e.message));

  await page.goto(BASE, { waitUntil: 'networkidle' });
  await page.fill('#token', TOKEN);
  await page.click('#connect');
  await page.waitForFunction(
    () => document.getElementById('status-pill')?.textContent.includes('ready'),
    null,
    { timeout: 30000 }
  );

  // fs mode with an injected OPFS directory (no native picker).
  await page.evaluate(async (dirName) => {
    const root = await navigator.storage.getDirectory();
    try {
      await root.removeEntry(dirName, { recursive: true });
    } catch {
      /* fresh */
    }
    const dir = await root.getDirectoryHandle(dirName, { create: true });
    const c = window.libfw.client;
    c._options.downloadMode = 'fs';
    c._options.autoTune = true;
    c._options.directoryHandle = () => dir;
    window.__dlDir = dir;
  }, DIR);

  // ------------------------------------------------- run 1: cancel mid-way
  await page.evaluate((name) => {
    const c = window.libfw.client;
    window.__run1 = c
      .downloadFile('dev-token', name)
      .then(() => 'completed')
      .catch((e) => `error: ${e?.message ?? e}`);
  }, NAME);

  let cancelled = false;
  for (let i = 0; i < 900; i++) {
    const done = await page.evaluate(() => window.libfw.client.doneBytes()).catch(() => 0);
    if (done > SIZE * 0.3) {
      await page.evaluate(() => window.libfw.client.cancel());
      cancelled = true;
      break;
    }
    if (await page.textContent('#st-state').catch(() => '') === 'failed') break;
    await page.waitForTimeout(100);
  }
  const run1 = await page.evaluate(() => window.__run1);
  check('run1: cancelled mid-transfer', cancelled, `outcome=${run1}`);

  // Give the SDK's finally-block (flush + offset sync) time to settle.
  await page.waitForTimeout(1500);

  const after1 = await page.evaluate(async ({ dirName, name }) => {
    const dir = await (await navigator.storage.getDirectory()).getDirectoryHandle(dirName);
    let size = -1;
    try {
      size = (await (await dir.getFileHandle(name)).getFile()).size;
    } catch {
      /* missing */
    }
    const state = await window.libfw.client._loadResumeState('download', name);
    return { size, state };
  }, { dirName: DIR, name: NAME });
  check(
    'run1: partial file + persisted offset on disk',
    after1.size > 0 && after1.size < SIZE && Number(after1.state?.offset) > 0,
    `onDisk=${after1.size} state.offset=${after1.state?.offset} etag=${String(after1.state?.etag).slice(0, 12)}`
  );

  // ------------------------------------- run 2: resume, must be identical
  const run1Ranges = ranges.length;
  await page.evaluate((name) => {
    const c = window.libfw.client;
    window.__run2 = c
      .downloadFile('dev-token', name)
      .then(() => 'completed')
      .catch((e) => `error: ${e?.message ?? e}`);
  }, NAME);
  for (let i = 0; i < 1800; i++) {
    const out = await page.evaluate(() => window.__run2);
    if (out !== undefined) break;
    await page.waitForTimeout(200);
  }
  const run2 = await page.evaluate(() => window.__run2);
  await page.waitForTimeout(1000);
  const run2Ranges = ranges.slice(run1Ranges);

  check('run2: download completed', run2 === 'completed', String(run2));
  const firstStart = run2Ranges.length
    ? Number(String(run2Ranges[0]).replace('bytes=', '').split('-')[0])
    : 0;
  check(
    'run2: only fetched the missing tail (resumed)',
    run2Ranges.length > 0 && firstStart >= Number(after1.state?.offset),
    `first range=${run2Ranges[0]} (offset was ${after1.state?.offset}, ${run2Ranges.length} ranges)`
  );

  // Integrity: hash the OPFS file in the page.
  const got = await page.evaluate(async ({ dirName, name }) => {
    const dir = await (await navigator.storage.getDirectory()).getDirectoryHandle(dirName);
    const file = await (await dir.getFileHandle(name)).getFile();
    const buf = await file.arrayBuffer();
    const digest = await crypto.subtle.digest('SHA-256', buf);
    return {
      size: buf.byteLength,
      sha: [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, '0')).join(''),
    };
  }, { dirName: DIR, name: NAME });
  check(
    'run2: resumed file is byte-identical',
    got.size === SIZE && got.sha === want,
    `size=${got.size}/${SIZE} sha=${got.sha.slice(0, 12)} want=${want.slice(0, 12)}`
  );
  check('no page errors', pageErrors.length === 0, JSON.stringify(pageErrors));

  console.log('\n=== SUMMARY ===');
  const failed = results.filter((r) => !r.ok);
  console.log(`${results.length - failed.length}/${results.length} passed`);
  failed.forEach((f) => console.log(`  FAIL: ${f.name} ${f.detail}`));
  await browser.close();
  process.exit(failed.length ? 1 : 0);
})().catch((e) => {
  console.error('DOWNLOAD-RESUME CRASHED', e);
  process.exit(2);
});
