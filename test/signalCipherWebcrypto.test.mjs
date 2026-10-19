/* =============================================================================
 *  AETHERON CORE  —  test/signalCipherWebcrypto.test.mjs
 *  -----------------------------------------------------------------------------
 *  THE CLIENT CONTRACT TEST.
 *
 *  signalCipher.test.mjs proves the server can open its own output. That is not
 *  the interesting question: OpenSSL talks to OpenSSL either way. The payload is
 *  opened by the PHONE, with Web Crypto, and Web Crypto is a different
 *  implementation with its own opinions (96-bit IV, tag appended to the
 *  ciphertext, base64 via atob).
 *
 *  So this test seals with the server module and opens with the exact snippet
 *  the app ships — the three key forms included. If this file ever fails, the
 *  mobile client is broken even though every server-side test still passes.
 *
 *  Run:  node test/signalCipherWebcrypto.test.mjs
 * ============================================================================= */

import { webcrypto } from 'node:crypto';
import { encryptSignal } from '../src/services/signalCipher.js';

const subtle = webcrypto.subtle;
const { TextDecoder, TextEncoder } = globalThis;

/* ------- the snippet, verbatim as it will ship in the app ------- */
async function keyToBytes(text) {
  const t = String(text).trim();
  if (/^[0-9a-fA-F]{64}$/.test(t)) {
    const out = new Uint8Array(32);
    for (let i = 0; i < 32; i++) out[i] = parseInt(t.substr(i * 2, 2), 16);
    return out;
  }
  if (/^[A-Za-z0-9+/]+={0,2}$/.test(t) && t.length % 4 === 0) {
    try {
      const raw = Uint8Array.from(atob(t), (c) => c.charCodeAt(0));
      if (raw.length === 32) return raw;
    } catch { /* not base64 */ }
  }
  const hash = await subtle.digest('SHA-256', new TextEncoder().encode(t));
  return new Uint8Array(hash);
}

async function openSignal(blob, keyText) {
  const key = await subtle.importKey('raw', await keyToBytes(keyText), { name: 'AES-GCM' }, false, ['decrypt']);
  const raw = Uint8Array.from(atob(String(blob).replace(/^v\d+\./, '')), (c) => c.charCodeAt(0));
  const iv = raw.subarray(0, 12);
  const tag = raw.subarray(12, 28);
  const body = raw.subarray(28);
  const data = new Uint8Array(body.length + tag.length);
  data.set(body, 0);
  data.set(tag, body.length);
  const plain = await subtle.decrypt({ name: 'AES-GCM', iv, tagLength: 128 }, key, data);
  return JSON.parse(new TextDecoder().decode(plain));
}
/* ---------------------------------------------------------------- */

const cases = [
  ['64-hex key', '44f5057bf5ad263ac8d571ea796711a7657514563a4a59d5e0962a989d4e19f0'],
  ['base64 key', Buffer.alloc(32, 9).toString('base64')],
  ['passphrase', 'my secret passphrase'],
];

const payload = {
  kind: 'opened', symbol: 'USDJPY', action: 'SELL', entry_price: 150.123,
  stop_loss: 150.9, take_profit_1: 149.2, take_profit_2: 148.4,
  confidence: 'Medium', reasoning: 'اختبار عبر الويب كربرو',
  position: { action: 'SELL', entry: 150.123 }, ts: 1760000000000,
};

let fail = 0;
for (const [label, key] of cases) {
  const blob = encryptSignal(payload, key);
  const back = await openSignal(blob, key);
  const same = JSON.stringify(back) === JSON.stringify(payload);
  console.log(`${same ? '✓' : '✗'} ${label} (${blob.length} chars)`);
  if (!same) { fail++; console.log('   got:', JSON.stringify(back).slice(0, 200)); }
}

// A wrong key must throw, not return garbage.
try {
  await openSignal(encryptSignal(payload, cases[0][1]), 'a different key');
  console.log('✗ wrong key was accepted'); fail++;
} catch {
  console.log('✓ wrong key is rejected by the GCM tag');
}

console.log(fail === 0 ? '\n✅ Web Crypto and the server agree on the format\n' : `\n❌ ${fail} failure(s)\n`);
process.exit(fail === 0 ? 0 : 1);
