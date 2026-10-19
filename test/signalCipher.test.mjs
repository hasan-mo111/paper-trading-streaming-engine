/* =============================================================================
 *  AETHERON CORE  —  test/signalCipher.test.mjs
 *  -----------------------------------------------------------------------------
 *  The cipher is a contract with a client we do not compile: the mobile app has
 *  to reproduce these exact bytes in Web Crypto. So these tests pin the wire
 *  format down, not just the round trip:
 *
 *    1. the "v1." prefix and a 12-byte IV + 16-byte tag layout
 *    2. key derivation: 64-hex, base64-32, and passphrase → SHA-256
 *    3. a fresh IV per message (a repeated IV with GCM is a broken cipher)
 *    4. a wrong key, a tampered byte and an unknown version all FAIL
 *
 *  Run:  node test/signalCipher.test.mjs
 * ============================================================================= */

import assert from 'node:assert/strict';
import { createHash, randomBytes } from 'node:crypto';

import {
  CIPHER_VERSION,
  decryptSignal,
  encryptSignal,
  normalizeKey,
} from '../src/services/signalCipher.js';

let passed = 0;
let failed = 0;
function test(name, fn) {
  try {
    fn();
    passed += 1;
    console.log(`  ✓ ${name}`);
  } catch (e) {
    failed += 1;
    console.error(`  ✗ ${name}\n      ${e.message}`);
  }
}

const HEX_KEY = '44f5057bf5ad263ac8d571ea796711a7657514563a4a59d5e0962a989d4e19f0';
const B64_KEY = Buffer.alloc(32, 7).toString('base64');
const PAYLOAD = {
  symbol: 'XAUUSD',
  action: 'BUY',
  entry_price: 2400.5,
  stop_loss: 2380.25,
  take_profit_1: 2430,
  take_profit_2: 2450,
  confidence: 'High',
  reasoning: 'كسر هيكل صاعد على H4 وسحب سيولة قبل الدخول',
  position: { action: 'BUY', entry: 2400.5 },
  ts: 1_760_000_000_000,
};

console.log('\nAI signal cipher — unit tests\n');

/* ---- 1. key derivation ---------------------------------------------------- */

test('a 64-character hex key is used as 32 raw bytes', () => {
  const key = normalizeKey(HEX_KEY);
  assert.equal(key.length, 32);
  assert.equal(key.toString('hex'), HEX_KEY);
});

test('a base64 key decoding to 32 bytes is used as-is', () => {
  const key = normalizeKey(B64_KEY);
  assert.equal(key.length, 32);
  assert.deepEqual(key, Buffer.alloc(32, 7));
});

test('a passphrase is hashed with SHA-256 (never truncated)', () => {
  const key = normalizeKey('correct horse battery staple');
  assert.equal(key.length, 32);
  assert.equal(
    key.toString('hex'),
    createHash('sha256').update('correct horse battery staple', 'utf8').digest('hex')
  );
});

test('an empty key is rejected instead of silently weakening a message', () => {
  assert.throws(() => normalizeKey(''));
  assert.throws(() => normalizeKey('   '));
});

/* ---- 2. wire format ------------------------------------------------------ */

test('the blob is `v1.` + base64(IV ‖ Tag ‖ ciphertext)', () => {
  const blob = encryptSignal(PAYLOAD, HEX_KEY);
  assert.ok(blob.startsWith(`${CIPHER_VERSION}.`), 'version prefix missing');

  const raw = Buffer.from(blob.slice(CIPHER_VERSION.length + 1), 'base64');
  // 12 IV + 16 tag + ciphertext (longer than the 16-byte GCM minimum).
  assert.ok(raw.length > 12 + 16 + 16, `unexpected length ${raw.length}`);
  assert.equal(blob.includes('"'), false, 'the blob must be opaque base64');
});

test('every message gets a fresh IV', () => {
  const a = Buffer.from(encryptSignal(PAYLOAD, HEX_KEY).slice(3), 'base64');
  const b = Buffer.from(encryptSignal(PAYLOAD, HEX_KEY).slice(3), 'base64');
  assert.notEqual(a.toString('hex'), b.toString('hex'), 'two blobs are identical');
  assert.notEqual(a.subarray(0, 12).toString('hex'), b.subarray(0, 12).toString('hex'), 'IV reused');
});

/* ---- 3. round trip ------------------------------------------------------- */

test('the exact payload survives a round trip (hex key)', () => {
  const blob = encryptSignal(PAYLOAD, HEX_KEY);
  assert.deepEqual(decryptSignal(blob, HEX_KEY), PAYLOAD);
});

test('the hex and base64 forms of the same key inter-operate', () => {
  const raw = Buffer.alloc(32, 0);
  const blob = encryptSignal({ ok: true }, raw.toString('hex'));
  assert.deepEqual(decryptSignal(blob, raw.toString('base64')), { ok: true });
});

test('a passphrase key round-trips against its SHA-256 derivation', () => {
  const blob = encryptSignal({ ok: true }, 'hunter2');
  assert.deepEqual(decryptSignal(blob, createHash('sha256').update('hunter2').digest('hex')), { ok: true });
});

test('a random 32-byte key round-trips 200 times', () => {
  for (let i = 0; i < 200; i += 1) {
    const key = randomBytes(32);
    const payload = { n: i, text: 'x'.repeat(i % 7) };
    assert.deepEqual(decryptSignal(encryptSignal(payload, key), key), payload);
  }
});

/* ---- 4. failure modes ---------------------------------------------------- */

test('a wrong key throws instead of returning garbage', () => {
  const blob = encryptSignal(PAYLOAD, HEX_KEY);
  const wrong = randomBytes(32).toString('hex');
  assert.throws(() => decryptSignal(blob, wrong));
});

test('a tampered ciphertext byte is detected by the auth tag', () => {
  const blob = encryptSignal(PAYLOAD, HEX_KEY);
  const raw = Buffer.from(blob.slice(3), 'base64');
  raw[raw.length - 1] ^= 0x01; // flip one bit in the ciphertext
  const tampered = `v1.${raw.toString('base64')}`;
  assert.throws(() => decryptSignal(tampered, HEX_KEY));
});

test('a tampered IV is detected as well', () => {
  const blob = encryptSignal(PAYLOAD, HEX_KEY);
  const raw = Buffer.from(blob.slice(3), 'base64');
  raw[0] ^= 0xff;
  assert.throws(() => decryptSignal(`v1.${raw.toString('base64')}`, HEX_KEY));
});

test('an unknown version prefix is refused', () => {
  const body = encryptSignal(PAYLOAD, HEX_KEY).slice(3);
  assert.throws(() => decryptSignal(`v2.${body}`, HEX_KEY), /version/i);
  assert.throws(() => decryptSignal(body, HEX_KEY), /version/i);
});

test('a truncated blob is refused before touching the decipher', () => {
  assert.throws(() => decryptSignal('v1.', HEX_KEY));
  assert.throws(() => decryptSignal('v1.AAAA', HEX_KEY));
});

/* ---------------------------------------------------------------- summary */

console.log(`\n${failed === 0 ? '✅' : '❌'} Signal cipher: ${passed} passed, ${failed} failed\n`);
process.exit(failed === 0 ? 0 : 1);
