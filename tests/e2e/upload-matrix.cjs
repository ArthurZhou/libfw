/* Full e2e matrix: empty/small/big/multi-chunk uploads, overwrite,
 * integrity roundtrip via server GET, folder upload, UI download.
 *
 * Determinism notes:
 *  - Network is throttled via CDP so transfers take seconds and the UI's
 *    transient "completed" state + intermediate progress are observable.
 *  - Completion is detected from the UI log ("✔ done" / "✖ failed"),
 *    not the state pill (which flips back to "idle" immediately).
 */
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
  const ctx = await browser.newContext({ acceptDownloads: true });
  const page = await ctx.newPage();

  // Throttle so transfers are observable (2 MiB/s up, 100ms latency).
  const cdp = await ctx.newCDPSession(page);
  await cdp.send('Network.enable');
  await cdp.send('Network.emulateNetworkConditions', {
    offline: false, latency: 100,
    downloadThroughput: 8 * 1024 * 1024, uploadThroughput: 2 * 1024 * 1024,
  });

  const pageErrors = [];
  page.on('pageerror', (e) => pageErrors.push(e.message));
  const chunkPosts = [];
  page.on('request', (r) => {
    if (r.method() === 'POST' && r.url().includes('/file/') && r.postData()?.length) {
      chunkPosts.push({ url: r.url(), body: r.postData().length, offset: r.headers()['x-libfw-offset'] });
    }
  });

  await page.goto(BASE, { waitUntil: 'networkidle' });
  await page.fill('#token', TOKEN);
  await page.click('#connect');
  await page.waitForTimeout(500);

  const logText = () => page.textContent('#log').catch(() => '');
  const clearLog = () => page.evaluate(() => { document.getElementById('log').textContent = ''; });
  const waitIdle = async (timeoutMs = 10000) => {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      if ((await page.textContent('#st-state').catch(() => 'idle')) === 'idle') return;
      await page.waitForTimeout(50);
    }
  };
  const waitDone = async (timeoutMs = 180000) => {
    const deadline = Date.now() + timeoutMs;
    let log = '';
    while (Date.now() < deadline) {
      log = await logText();
      if (log.includes('✔ done') || log.includes('✖')) return log;
      await page.waitForTimeout(100);
    }
    return log;
  };
  // Run one upload via a UI input and wait for ITS log line.
  const doUpload = async (selector, payload, what) => {
    await clearLog();
    await page.setInputFiles(selector, payload);
    const log = await waitDone();
    await waitIdle();
    await page.waitForTimeout(300);
    return log;
  };
  const serverBytes = async (p) => {
    return await page.evaluate(async ({ base, token, p }) => {
      const r = await fetch(`${base}/file/${p.split('/').map(encodeURIComponent).join('/')}`, {
        headers: { Authorization: `Bearer ${token}` },
      });
      if (!r.ok) throw new Error(`GET ${r.status}`);
      return Array.from(new Uint8Array(await r.arrayBuffer()));
    }, { base: BASE, token: TOKEN, p });
  };
  const listing = async () => {
    return await page.evaluate(async ({ base, token }) => {
      const r = await fetch(`${base}/dir`, { headers: { Authorization: `Bearer ${token}` } });
      return await r.json();
    }, { base: BASE, token: TOKEN });
  };
  const sha = (buf) => crypto.createHash('sha256').update(buf).digest('hex');

  // ---------------------------------------------------------------- fixtures
  const fix = (name, bytes) => {
    const p = `/tmp/libfw-e2e/${name}`;
    fs.writeFileSync(p, bytes);
    return { p, bytes, sha: sha(bytes) };
  };
  const empty = fix('empty.txt', Buffer.alloc(0));
  const hello = fix('hello.txt', Buffer.from('libfw e2e hello '.repeat(200))); // 3 KB compressible
  const big = fix('big.bin', crypto.randomBytes(5 * 1024 * 1024 + 12345)); // >2MiB chunk size, odd tail

  // ---------------------------------------------------------------- 1. empty file
  let log = await doUpload('#files', empty.p, 'empty');
  check('empty: upload done', log.includes('✔ done'), log.slice(0, 80));
  await page.waitForTimeout(400);
  let entries = await listing();
  let e = entries.find((x) => x.path === 'empty.txt');
  check('empty: listed with size 0', !!e && e.size === 0, JSON.stringify(e));

  // ---------------------------------------------------------------- 2. small compressible file
  log = await doUpload('#files', hello.p, 'hello');
  check('hello: upload done', log.includes('✔ done'), log.slice(0, 80));
  await page.waitForTimeout(400);
  const helloServer = Buffer.from(await serverBytes('hello.txt'));
  check('hello: integrity (sha match)', sha(helloServer) === hello.sha, `${sha(helloServer).slice(0, 12)} vs ${hello.sha.slice(0, 12)}`);

  // ---------------------------------------------------------------- 3. big multi-chunk file
  chunkPosts.length = 0;
  await clearLog();
  await page.setInputFiles('#files', big.p);
  let sawIntermediate = false;
  let lastPct = '0.0%';
  for (let i = 0; i < 400; i++) {
    if ((await logText()).includes('✔ done') || (await logText()).includes('✖')) break;
    lastPct = await page.textContent('#st-pct').catch(lastPct);
    if (lastPct !== '0.0%' && lastPct !== '100.0%') sawIntermediate = true;
    await page.waitForTimeout(50);
  }
  log = await waitDone();
  await waitIdle();
  await page.waitForTimeout(300);
  check('big: upload done', log.includes('✔ done'), log.slice(0, 80));
  check('big: intermediate progress observed', sawIntermediate, `lastPct=${lastPct}`);
  const offsets = chunkPosts.map((c) => Number(c.offset)).sort((a, b) => a - b);
  const totalChunkBytes = chunkPosts.reduce((n, c) => n + c.body, 0);
  check('big: multi-chunk sent (≥3 posts)', chunkPosts.length >= 3, `${chunkPosts.length} posts offsets=${offsets.join(',')}`);
  check('big: wire bytes > 0', totalChunkBytes > 0, `${totalChunkBytes}B wire`);
  const bigServer = Buffer.from(await serverBytes('big.bin'));
  check('big: integrity (sha match)', sha(bigServer) === big.sha, `${sha(bigServer).slice(0, 12)} vs ${big.sha.slice(0, 12)}`);
  entries = await listing();
  e = entries.find((x) => x.path === 'big.bin');
  check('big: listed size matches', !!e && e.size === big.bytes.length, `listed=${e?.size} expect=${big.bytes.length}`);

  // ---------------------------------------------------------------- 4. overwrite: same name, new content
  const small = Buffer.from('overwritten-by-smaller-content');
  log = await doUpload('#files', [
    { name: 'big.bin', mimeType: 'application/octet-stream', buffer: small },
  ], 'overwrite');
  check('overwrite: upload done', log.includes('✔ done'), log.slice(0, 80));
  await page.waitForTimeout(400);
  const overwritten = Buffer.from(await serverBytes('big.bin'));
  check('overwrite: server has new content', sha(overwritten) === sha(small), `${sha(overwritten).slice(0, 12)} vs ${sha(small).slice(0, 12)}`);

  // ---------------------------------------------------------------- 5. folder upload (webkitdirectory)
  const readme = Buffer.from('# readme\nhello folder\n');
  const notes = Buffer.from('nested note\n'.repeat(500));
  const fdir = '/tmp/libfw-e2e/folder-src';
  fs.mkdirSync(fdir + '/docs/deep', { recursive: true });
  fs.writeFileSync(fdir + '/docs/readme.md', readme);
  fs.writeFileSync(fdir + '/docs/deep/notes.txt', notes);
  log = await doUpload('#folder', fdir, 'folder');
  check('folder: upload done', log.includes('✔ done'), log.slice(0, 80));
  await page.waitForTimeout(400);
  // Playwright's webkitdirectory injection sets webkitRelativePath relative
  // to the common ancestor of the injected paths (a real browser picker uses
  // the picked folder as root), so match on the path SUFFIX to stay
  // deterministic.
  const folderEntries = await listing();
  const docs = folderEntries.find((x) => x.is_dir);
  check('folder: root dir entry created', !!docs, JSON.stringify(folderEntries));
  const allPaths = await page.evaluate(async ({ base, token }) => {
    const out = [];
    const walk = async (p) => {
      const r = await fetch(`${base}/dir${p ? '/' + p.split('/').map(encodeURIComponent).join('/') : ''}`, {
        headers: { Authorization: `Bearer ${token}` },
      });
      for (const e of await r.json()) {
        out.push(e.path);
        if (e.is_dir) await walk(e.path);
      }
    };
    await walk('');
    return out;
  }, { base: BASE, token: TOKEN });
  const readmePath = allPaths.find((p) => p.endsWith('docs/readme.md'));
  const r = readmePath ? Buffer.from(await serverBytes(readmePath)) : null;
  check('folder: readme.md integrity', r && r.toString() === readme.toString(), `path=${readmePath ?? 'missing'}`);
  const notesPath = allPaths.find((p) => p.endsWith('docs/deep/notes.txt'));
  const n = notesPath ? Buffer.from(await serverBytes(notesPath)) : null;
  check('folder: nested notes.txt integrity', n && n.length === notes.length, `path=${notesPath ?? 'missing'}, got ${n?.length ?? 'missing'}, expect ${notes.length}`);

  // ---------------------------------------------------------------- 6. UI download roundtrip (browser fallback)
  const dl = fix('download-me.bin', crypto.randomBytes(300000));
  log = await doUpload('#files', dl.p, 'download-source');
  check('download: source upload done', log.includes('✔ done'), log.slice(0, 80));
  await page.evaluate(() => { window.libfw.client._options.downloadMode = 'browser'; });
  const dlPromise = page.waitForEvent('download', { timeout: 60000 });
  await page.locator('#listing tr', { hasText: 'download-me.bin' }).first().locator('[data-act="dl"]').click();
  const download = await dlPromise;
  await download.saveAs('/tmp/libfw-e2e/downloaded-me.bin');
  const downloaded = fs.readFileSync('/tmp/libfw-e2e/downloaded-me.bin');
  check('download: bytes match source', sha(downloaded) === dl.sha, `${dl.bytes.length}B`);

  // ---------------------------------------------------------------- summary
  const errs = pageErrors.filter((m) => !m.includes('favicon'));
  check('no page errors during matrix', errs.length === 0, JSON.stringify(errs));
  console.log('\n=== SUMMARY ===');
  const failed = results.filter((r) => !r.ok);
  console.log(`${results.length - failed.length}/${results.length} passed`);
  failed.forEach((f) => console.log(`  FAIL: ${f.name} ${f.detail}`));
  await browser.close();
  process.exit(failed.length ? 1 : 0);
})().catch((e) => { console.error('E2E CRASHED', e); process.exit(2); });