/**
 * Minimal, dependency-free ZIP writer for the browser-download fallback.
 *
 * When the File System Access API is unavailable, folder downloads are
 * buffered in memory and packed into a single `.zip` archive via
 * {@link createZip}. The archive uses the STORE method (no compression) —
 * the SDK stays dependency-free (no deflate implementation) and CPU cost is
 * negligible. Entries carry the full virtual path (with `/` separators), so
 * extractors recreate the folder structure automatically.
 *
 * @module libfw/zip
 */

/** CRC-32 (IEEE 802.3, reflected) table, generated once. */
const CRC_TABLE = (() => {
  const table = new Uint32Array(256);
  for (let n = 0; n < 256; n += 1) {
    let c = n;
    for (let k = 0; k < 8; k += 1) {
      c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
    }
    table[n] = c >>> 0;
  }
  return table;
})();

/**
 * Compute the CRC-32 checksum of a byte array.
 * @param {Uint8Array} bytes
 * @returns {number} unsigned 32-bit CRC
 */
function crc32(bytes) {
  let crc = 0xffffffff;
  for (let i = 0; i < bytes.length; i += 1) {
    crc = CRC_TABLE[(crc ^ bytes[i]) & 0xff] ^ (crc >>> 8);
  }
  return (crc ^ 0xffffffff) >>> 0;
}

/**
 * Pack a list of files into a ZIP `Blob` (STORE method).
 *
 * @param {Array<{name: string, data: Uint8Array}>} entries
 *        `name` is the virtual path inside the archive (`/` separators);
 *        `data` is the file content (may be empty).
 * @returns {Blob} `application/zip` blob
 */
export function createZip(entries) {
  const encoder = new TextEncoder();
  const body = [];
  const central = [];
  let offset = 0;

  for (const entry of entries) {
    const nameBytes = encoder.encode(entry.name);
    const data = entry.data;
    const crc = crc32(data);

    // Local file header (30 bytes + name).
    const local = new Uint8Array(30 + nameBytes.length);
    const dv = new DataView(local.buffer);
    dv.setUint32(0, 0x04034b50, true); // "PK\x03\x04"
    dv.setUint16(4, 20, true); // version needed to extract
    dv.setUint16(6, 0, true); // general purpose flags
    dv.setUint16(8, 0, true); // compression method: STORE
    dv.setUint16(10, 0, true); // last mod time
    dv.setUint16(12, 0x0021, true); // last mod date (1980-01-01)
    dv.setUint32(14, crc, true);
    dv.setUint32(18, data.length, true); // compressed size
    dv.setUint32(22, data.length, true); // uncompressed size
    dv.setUint16(26, nameBytes.length, true);
    dv.setUint16(28, 0, true); // extra field length
    local.set(nameBytes, 30);

    body.push(local, data);
    central.push({ nameBytes, crc, size: data.length, offset });
    offset += local.length + data.length;
  }

  // Central directory.
  const dir = [];
  let cdSize = 0;
  for (const c of central) {
    const cd = new Uint8Array(46 + c.nameBytes.length);
    const dv = new DataView(cd.buffer);
    dv.setUint32(0, 0x02014b50, true); // "PK\x01\x02"
    dv.setUint16(4, 20, true); // version made by
    dv.setUint16(6, 20, true); // version needed to extract
    dv.setUint16(8, 0, true); // flags
    dv.setUint16(10, 0, true); // method: STORE
    dv.setUint16(12, 0, true); // mod time
    dv.setUint16(14, 0x0021, true); // mod date
    dv.setUint32(16, c.crc, true);
    dv.setUint32(20, c.size, true); // compressed size
    dv.setUint32(24, c.size, true); // uncompressed size
    dv.setUint16(28, c.nameBytes.length, true);
    dv.setUint16(30, 0, true); // extra field length
    dv.setUint16(32, 0, true); // comment length
    dv.setUint16(34, 0, true); // disk number start
    dv.setUint16(36, 0, true); // internal file attributes
    dv.setUint32(38, 0, true); // external file attributes
    dv.setUint32(42, c.offset, true); // local header offset
    cd.set(c.nameBytes, 46);
    dir.push(cd);
    cdSize += cd.length;
  }

  const cdOffset = offset;

  // End of central directory record (22 bytes).
  const eocd = new Uint8Array(22);
  const edv = new DataView(eocd.buffer);
  edv.setUint32(0, 0x06054b50, true); // "PK\x05\x06"
  edv.setUint16(4, 0, true); // disk number
  edv.setUint16(6, 0, true); // disk with central dir
  edv.setUint16(8, central.length, true); // entries on this disk
  edv.setUint16(10, central.length, true); // total entries
  edv.setUint32(12, cdSize, true); // central dir size
  edv.setUint32(16, cdOffset, true); // central dir offset
  edv.setUint16(20, 0, true); // comment length

  return new Blob([...body, ...dir, eocd], { type: 'application/zip' });
}
