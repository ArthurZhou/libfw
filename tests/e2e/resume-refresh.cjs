/* Resume ACROSS A HARD PAGE REFRESH (the "reload the tab, then continue" case).
 *
 * A refresh kills the page without running any `finally` block, so this suite
 * exercises both persistence paths in their worst case:
 *
 *   upload   — the server owns the partial (temp + `.blocks` sidecar). A
 *              disconnected chunk request must NOT discard it, and the reloaded
 *              page must find the same session (the client session id is the
 *              file's ETag = size+mtime, which survives a reload) and re-send
 *              only the missing blocks.
 *   download — the bytes are on disk (fs mode) and the offset/ETag in
 *              IndexedDB. Chromium only publishes a `createWritable()` stream
 *              on `close()`, so the SDK checkpoints the prefix (close +
 *              reopen with `keepExistingData`) every time the engine reports a
 *              durable offset, letting the reloaded page resume from the last
 *              checkpoint instead of byte 0.
 *
 *   node tests/e2e/resume-refresh.cjs        (server on :8080, token dev-token)
 */
const { chromium } = require('playwright');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { BASE, DATA, TOKEN, launch, tmpFile } = require('./harness.cjs');

const UP_NAME = 'refresh-up.bin';
const DOWN_NAME = 'refresh-down.bin';
const DL_DIR = 'libfw-e2e-refresh';
const UP_SIZE = 30 * 1024 * 1024 + 777;
const DOWN_SIZE = 20 * 1024 * 1024 + 1234;

const results = [];
function check(name, ok, detail = '') {
  results.push({ name, ok, detail });
  console.log(`${ok ? 'PASS' : 'FAIL'}  ${name}${detail ? ' — ' + detail : ''}`);
}
const sha = (buf) => crypto.createHash('sha256').update(buf).digest('hex');

(async () => {
  fs.mkdirSync(DATA, { recursive: true });
  const upBytes = crypto.randomBytes(UP_SIZE);
  const upPath = tmpFile(UP_NAME);
  fs.writeFileSync(upPath, upBytes);
  const upSha = sha(upBytes);
  const downBytes = crypto.randomBytes(DOWN_SIZE);
  fs.writeFileSync(path.join(DATA, DOWN_NAME), downBytes);
  const downSha = sha(downBytes);

  const browser = await launch(chromium);
  const ctx = await browser.newContext();
  const page = await ctx.newPage();
  const cdp = await ctx.newCDPSession(page);
  await cdp.send('Network.enable');
  await cdp.send('Network.emulateNetworkConditions', {
    offline: false,
    latency: 120,
    downloadThroughput: 4 * 1024 * 1024,
    uploadThroughput: 8 * 1024 * 1024,
  });

  const postOffsets = [];
  const probes = [];
  const rangeGets = [];
  page.on('request', (r) => {
    if (r.method() === 'POST' && r.url().includes('/file/') && r.postData()?.length) {
      postOffsets.push(Number(r.headers()['x-libfw-offset']));
    }
    if (r.method() === 'GET' && r.url().includes('/file/') && r.headers()['range']) {
      rangeGets.push(r.headers()['range']);
    }
  });
  page.on('response', async (r) => {
    const req = r.request();
    if (req.method() === 'POST' && req.headers()['x-libfw-session-status']) {
      probes.push({ status: r.status(), body: await r.text().catch(() => '?') });
    }
  });
  const pageErrors = [];
  page.on('pageerror', (e) => pageErrors.push(e.message));

  /** Load the UI, connect, and wait for the engine. */
  const connect = async () => {
    await page.goto(BASE, { waitUntil: 'networkidle' });
    await page.fill('#token', TOKEN);
    await page.click('#connect');
    await page.waitForFunction(
      () => document.getElementById('status-pill')?.textContent.includes('ready'),
      null,
      { timeout: 30000 },
    );
    await page.waitForFunction(() => !!window.libfw?.client, null, { timeout: 10000 });
  };
  const doneBytes = () =>
    page.evaluate(() => window.libfw?.client?.doneBytes?.() || 0).catch(() => 0);

  // ============================================ upload killed by a refresh
  await connect();
  await page.setInputFiles('#files', upPath);
  for (let i = 0; i < 1200; i++) {
    if ((await doneBytes()) > UP_SIZE * 0.25) break;
    await page.waitForTimeout(50);
  }
  const upKilledAt = await doneBytes();
  await page.reload({ waitUntil: 'domcontentloaded' }); // hard kill: no finally runs
  const killedPosts = postOffsets.length;
  const sessionTemps = fs
    .readdirSync(DATA)
    .filter((f) => f.includes('.libfw-sess-') && f.includes(UP_NAME));
  check('upload: killed mid-transfer', upKilledAt > 0 && upKilledAt < UP_SIZE, `${upKilledAt}/${UP_SIZE}`);
  check(
    'upload: the server kept the temp + sidecar after the disconnect',
    sessionTemps.some((f) => f.endsWith('.blocks')) && sessionTemps.some((f) => !f.endsWith('.blocks')),
    JSON.stringify(sessionTemps.map((f) => f.replace(/^\.libfw-sess-[^-]+-/, ''))),
  );

  await connect();
  probes.length = 0;
  await page.setInputFiles('#files', upPath);
  await page.waitForFunction(
    () => document.getElementById('st-state')?.textContent === 'completed',
    null,
    { timeout: 180000 },
  );
  const resent = postOffsets.slice(killedPosts);
  const coveredEnd = Math.max(
    0,
    ...probes
      .filter((p) => p.status === 200)
      .map((p) => {
        try {
          return Math.max(0, ...JSON.parse(p.body).ranges.map((r) => r[1]));
        } catch {
          return 0;
        }
      }),
  );
  check('upload: the reloaded page probed and saw the partial', coveredEnd > 0, `coveredEnd=${coveredEnd}`);
  check(
    'upload: only the missing tail was re-sent',
    resent.length > 0 && Math.min(...resent) >= coveredEnd,
    `min=${Math.min(...resent)} coveredEnd=${coveredEnd} blocks=${resent.length}`,
  );
  const upGot = await page.evaluate(
    async ({ base, token, name }) => {
      const r = await fetch(`${base}/file/${name}`, { headers: { Authorization: `Bearer ${token}` } });
      const b = await r.arrayBuffer();
      const d = await crypto.subtle.digest('SHA-256', b);
      return { size: b.byteLength, sha: [...new Uint8Array(d)].map((x) => x.toString(16).padStart(2, '0')).join('') };
    },
    { base: BASE, token: TOKEN, name: UP_NAME },
  );
  check(
    'upload: byte-identical after the resumed upload',
    upGot.size === UP_SIZE && upGot.sha === upSha,
    `size=${upGot.size}/${UP_SIZE} sha=${upGot.sha.slice(0, 12)}`,
  );

  // ========================================== download killed by a refresh
  await connect();
  await page.evaluate(async (dirName) => {
    const root = await navigator.storage.getDirectory();
    try {
      await root.removeEntry(dirName, { recursive: true });
    } catch {
      /* fresh */
    }
    await root.getDirectoryHandle(dirName, { create: true });
    const c = window.libfw.client;
    c._options.downloadMode = 'fs';
    // A fresh page can only re-resolve a persisted directory handle lazily —
    // exactly what a user re-picking the same folder (or an app storing the
    // handle) does.
    c._options.directoryHandle = async () =>
      (await navigator.storage.getDirectory()).getDirectoryHandle(dirName, { create: true });
  }, DL_DIR);
  await page.evaluate((name) => {
    window.libfw.client.downloadFile('dev-token', name).catch(() => {});
  }, DOWN_NAME);
  for (let i = 0; i < 1200; i++) {
    if ((await doneBytes()) > DOWN_SIZE * 0.25) break;
    await page.waitForTimeout(50);
  }
  const dlKilledAt = await doneBytes();
  await page.reload({ waitUntil: 'domcontentloaded' });
  const opfsAfterKill = await page.evaluate(async (dirName) => {
    const dir = await (await navigator.storage.getDirectory()).getDirectoryHandle(dirName, { create: true });
    const out = [];
    for await (const [n, h] of dir.entries()) {
      let size = -1;
      try {
        size = (await h.getFile()).size;
      } catch {
        /* missing */
      }
      out.push({ name: n, size });
    }
    return out;
  }, DL_DIR);
  const partial = opfsAfterKill.find((e) => e.name === DOWN_NAME)?.size ?? 0;
  check('download: killed mid-transfer', dlKilledAt > 0 && dlKilledAt < DOWN_SIZE, `${dlKilledAt}/${DOWN_SIZE}`);
  check(
    'download: a checkpointed prefix survived the refresh',
    partial > 0 && partial < DOWN_SIZE,
    `onDisk=${partial} (killed at ~${dlKilledAt})`,
  );

  const run1Ranges = rangeGets.length;
  await connect();
  await page.evaluate(async (dirName) => {
    const c = window.libfw.client;
    c._options.downloadMode = 'fs';
    c._options.directoryHandle = async () =>
      (await navigator.storage.getDirectory()).getDirectoryHandle(dirName, { create: true });
  }, DL_DIR);
  await page.evaluate((name) => {
    window.__done = false;
    window.libfw.client
      .downloadFile('dev-token', name)
      .then(() => {
        window.__outcome = 'completed';
      })
      .catch((e) => {
        window.__outcome = `error: ${e?.message ?? e}`;
      })
      .finally(() => {
        window.__done = true;
      });
  }, DOWN_NAME);
  for (let i = 0; i < 1800; i++) {
    if (await page.evaluate(() => window.__done === true)) break;
    await page.waitForTimeout(200);
  }
  const outcome = await page.evaluate(() => window.__outcome);
  const tail = rangeGets.slice(run1Ranges);
  const firstStart = tail.length ? Number(String(tail[0]).replace('bytes=', '').split('-')[0]) : -1;
  check('download: completed after the refresh', outcome === 'completed', String(outcome));
  check(
    'download: only the missing tail was re-fetched',
    tail.length > 0 && firstStart >= partial,
    `first=${tail[0]} (onDisk ${partial}, ${tail.length} ranges)`,
  );
  const downGot = await page.evaluate(
    async ({ dirName, name }) => {
      const dir = await (await navigator.storage.getDirectory()).getDirectoryHandle(dirName, { create: true });
      const b = await (await (await dir.getFileHandle(name)).getFile()).arrayBuffer();
      const d = await crypto.subtle.digest('SHA-256', b);
      return { size: b.byteLength, sha: [...new Uint8Array(d)].map((x) => x.toString(16).padStart(2, '0')).join('') };
    },
    { dirName: DL_DIR, name: DOWN_NAME },
  );
  check(
    'download: byte-identical after the resumed download',
    downGot.size === DOWN_SIZE && downGot.sha === downSha,
    `size=${downGot.size}/${DOWN_SIZE} sha=${downGot.sha.slice(0, 12)}`,
  );

  check('no page errors', pageErrors.length === 0, JSON.stringify(pageErrors));

  await browser.close();
  console.log('\n=== SUMMARY ===');
  const failed = results.filter((r) => !r.ok);
  console.log(`${results.length - failed.length}/${results.length} passed`);
  failed.forEach((f) => console.log(`  FAIL: ${f.name} ${f.detail}`));
  process.exit(failed.length ? 1 : 0);
})().catch((e) => {
  console.error('RESUME-REFRESH CRASHED', e);
  process.exit(2);
});
