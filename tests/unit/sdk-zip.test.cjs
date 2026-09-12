'use strict';

/**
 * Unit tests for the browser SDK's dependency-free ZIP writer
 * (`sdk/zip.js::createZip`) and the folder-archive naming rule
 * (`sdk/index.js::LibfwClient#_archiveName`).
 *
 * These are plain Node tests — no browser, no WASM instantiation. They cover
 * two regressions that only surfaced through a real browser folder download:
 *
 *  1. `createZip` encoded entry names as UTF-8 but left the ZIP "EFS" bit
 *     (general purpose flag bit 11) clear, so extractors decoded them with the
 *     local OEM code page (CP936/GBK on zh-CN Windows) and produced mojibake.
 *  2. `_archiveName` used `\w` as a whitelist, which is ASCII-only, so every
 *     non-ASCII folder name collapsed to `_` (a Chinese folder downloaded as
 *     `_.zip`).
 *
 * Run: `node tests/unit/sdk-zip.test.cjs` (or `pnpm test:unit`).
 */

const assert = require('node:assert/strict');
const path = require('node:path');
const { pathToFileURL } = require('node:url');

const ROOT = path.join(__dirname, '..', '..');
const ZIP_PATH = path.join(ROOT, 'sdk', 'zip.js');
const SDK_PATH = path.join(ROOT, 'sdk', 'index.js');

/** ZIP signatures and the EFS (UTF-8 name/comment) general purpose flag. */
const LOCAL_MAGIC = Buffer.from([0x50, 0x4b, 0x03, 0x04]); // "PK\x03\x04"
const CENTRAL_MAGIC = Buffer.from([0x50, 0x4b, 0x01, 0x02]); // "PK\x01\x02"
const EFS_FLAG = 0x0800;

let passed = 0;
let failed = 0;

/**
 * Run one synchronous check, recording pass/fail without aborting the suite.
 * @param {string} name
 * @param {() => void} fn
 */
function check(name, fn) {
  try {
    fn();
    passed += 1;
    console.log(`  ok   ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`  FAIL ${name}\n       ${err && err.message}`);
  }
}

/**
 * Read a `createZip` result back as bytes.
 * @param {Blob} blob
 * @returns {Promise<Buffer>}
 */
async function blobBytes(blob) {
  return Buffer.from(await blob.arrayBuffer());
}

/** @param {Buffer} buf @returns {number} offset of the central-directory header */
function findCentralDirectory(buf) {
  const at = buf.indexOf(CENTRAL_MAGIC);
  assert.notEqual(at, -1, 'central directory header not found');
  return at;
}

/** @param {Buffer} buf @returns {number} offset of the first local header */
function findLocalHeader(buf) {
  const at = buf.indexOf(LOCAL_MAGIC);
  assert.notEqual(at, -1, 'local file header not found');
  return at;
}

async function main() {
  const { createZip } = await import(pathToFileURL(ZIP_PATH).href);
  const { LibfwClient } = await import(pathToFileURL(SDK_PATH).href);

  console.log('createZip — UTF-8 (EFS) headers');

  const cjkName = '中文资料/说明 文档.txt';
  const zip = await blobBytes(createZip([{ name: cjkName, data: new Uint8Array([1, 2, 3]) }]));

  check('local file header sets general purpose flag bit 11 (EFS)', () => {
    const flags = zip.readUInt16LE(findLocalHeader(zip) + 6);
    assert.equal(flags & EFS_FLAG, EFS_FLAG, `flags=0x${flags.toString(16)}, expected bit 11 set`);
    assert.equal(flags, EFS_FLAG, `flags=0x${flags.toString(16)}, expected exactly 0x0800`);
  });

  check('central directory header sets general purpose flag bit 11 (EFS)', () => {
    const flags = zip.readUInt16LE(findCentralDirectory(zip) + 8);
    assert.equal(flags & EFS_FLAG, EFS_FLAG, `flags=0x${flags.toString(16)}, expected bit 11 set`);
    assert.equal(flags, EFS_FLAG, `flags=0x${flags.toString(16)}, expected exactly 0x0800`);
  });

  check('entry name bytes are UTF-8 and declared length matches them', () => {
    const local = findLocalHeader(zip);
    const len = zip.readUInt16LE(local + 26);
    const nameBytes = zip.subarray(local + 30, local + 30 + len);
    const expected = Buffer.from(cjkName, 'utf8');
    assert.equal(len, expected.length, 'name length field must be the UTF-8 byte length');
    assert.deepEqual(nameBytes, expected, 'entry name bytes must be UTF-8');
    assert.notEqual(nameBytes.length, cjkName.length, 'fixture must contain multi-byte characters');
  });

  check('central directory repeats the same UTF-8 name bytes', () => {
    const cd = findCentralDirectory(zip);
    const len = zip.readUInt16LE(cd + 28);
    const nameBytes = zip.subarray(cd + 46, cd + 46 + len);
    assert.deepEqual(nameBytes, Buffer.from(cjkName, 'utf8'));
  });

  await checkAsync('the flag is unconditional (ASCII names are flagged too)', async () => {
    const ascii = await blobBytes(createZip([{ name: 'plain.txt', data: new Uint8Array(0) }]));
    assert.equal(ascii.readUInt16LE(findLocalHeader(ascii) + 6), EFS_FLAG);
    assert.equal(ascii.readUInt16LE(findCentralDirectory(ascii) + 8), EFS_FLAG);
  });

  console.log('\n_archiveName — non-ASCII folder names survive');

  const client = new LibfwClient({ baseUrl: 'http://localhost:8080' });
  const cases = [
    ['中文资料', '中文资料.zip'],
    ['项目 文档', '项目 文档.zip'],
    ['файлы', 'файлы.zip'],
    ['résumé', 'résumé.zip'],
    ['report.pdf', 'report.pdf.zip'],
    ['photos/2026/假 期', '假 期.zip'],
  ];
  for (const [input, expected] of cases) {
    await checkAsync(`_archiveName(${JSON.stringify(input)}) === ${JSON.stringify(expected)}`, async () => {
      assert.equal(await client._archiveName(input), expected);
    });
  }

  await checkAsync('only genuinely illegal characters are replaced', async () => {
    // `:` and `"` are adjacent, so the `+` quantifier collapses them to one `_`.
    assert.equal(await client._archiveName('a<b>c:"d|e?f*g'), 'a_b_c_d_e_f_g.zip');
    assert.equal(await client._archiveName('a\\b'), 'a_b.zip');
  });

  await checkAsync('control characters are replaced', async () => {
    assert.equal(await client._archiveName('na\u0000me\u001f'), 'na_me_.zip');
  });

  await checkAsync('an empty / root path falls back to download.zip', async () => {
    assert.equal(await client._archiveName('/'), 'download.zip');
    assert.equal(await client._archiveName(''), 'download.zip');
  });

  await checkAsync('trailing whitespace is trimmed (Windows-hostile)', async () => {
    assert.equal(await client._archiveName('资料 '), '资料.zip');
  });

  await checkAsync('resolveDisplayName still drives the archive name', async () => {
    const resolved = new LibfwClient({
      baseUrl: 'http://localhost:8080',
      resolveDisplayName: (p) => (p === 'raw-id' ? '显示 名称' : p),
    });
    assert.equal(await resolved._archiveName('raw-id'), '显示 名称.zip');
  });

  await checkAsync('path traversal is still rejected by _safeEntryName', async () => {
    await assert.rejects(() => client._safeEntryName('../evil.txt'), /unsafe path/);
    await assert.rejects(() => client._safeEntryName('a/../../evil.txt'), /unsafe path/);
    await assert.rejects(() => client._safeEntryName('C:/Windows/system32'), /unsafe path/);
    await assert.rejects(() => client._safeEntryName('a\\b.txt'), /unsafe path/);
    assert.equal(await client._safeEntryName('/中文/资料.txt'), '中文/资料.txt');
  });

  console.log(`\n${passed} passed, ${failed} failed`);
  if (failed > 0) process.exitCode = 1;
}

/**
 * Async variant of {@link check}.
 * @param {string} name
 * @param {() => Promise<void>} fn
 */
async function checkAsync(name, fn) {
  try {
    await fn();
    passed += 1;
    console.log(`  ok   ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`  FAIL ${name}\n       ${err && err.message}`);
  }
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
