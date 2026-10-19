/* =============================================================================
 *  AETHERON CORE  —  src/services/signalCipher.js
 *  -----------------------------------------------------------------------------
 *  The one place that knows how an AI signal is sealed for the mobile client.
 *
 *  WHY A SEPARATE MODULE
 *  ---------------------
 *  Encryption is a contract with a client we do not compile. Keeping the format
 *  in one small, dependency-free module makes it testable, and means the HTTP
 *  layer, the analyst and the tests all agree on the exact bytes a phone has to
 *  decrypt. A client implementer only needs this file's three rules.
 *
 *  FORMAT (must be reproduced exactly by the app)
 *  ----------------------------------------------
 *      v1.<base64( IV(12 bytes) || AuthTag(16 bytes) || Ciphertext )>
 *
 *    • AES-256-GCM, 12-byte random IV per message, 128-bit auth tag.
 *    • Plaintext is the UTF-8 JSON of the signal object.
 *    • The `v1.` prefix is a format version, so the scheme can rotate later
 *      without guessing at a client.
 *
 *  KEY DERIVATION (must be reproduced exactly by the app)
 *  ------------------------------------------------------
 *    1. 64 hex characters            → those 32 raw bytes.
 *    2. valid base64 decoding to 32  → those 32 raw bytes.
 *    3. anything else (passphrase)   → SHA-256(UTF-8 bytes) → 32 bytes.
 *
 *    Rule 3 is why a human-chosen key still works: truncating a passphrase to
 *    32 bytes silently weakens it; hashing spreads the entropy over the key.
 * ============================================================================= */

import { createCipheriv, createDecipheriv, createHash, randomBytes } from 'node:crypto';

/** Wire format version. Bump together with the client. */
export const CIPHER_VERSION = 'v1';

/** AES-GCM parameters — 96-bit IV and 128-bit tag are the interoperable choice. */
export const CIPHER_ALGORITHM = 'aes-256-gcm';
const IV_BYTES = 12;
const TAG_BYTES = 16;
const KEY_BYTES = 32;

/**
 * Turn whatever the operator set in AI_SIGNAL_ENC_KEY into a 32-byte AES key.
 *
 * @param {string|Buffer} key
 * @returns {Buffer} 32 bytes
 */
export function normalizeKey(key) {
  if (Buffer.isBuffer(key)) {
    return key.length === KEY_BYTES ? key : createHash('sha256').update(key).digest();
  }

  const text = String(key ?? '').trim();
  if (!text) throw new Error('signal key is empty');

  // 1. 64 hex characters.
  if (/^[0-9a-fA-F]{64}$/.test(text)) return Buffer.from(text, 'hex');

  // 2. Base64 that decodes to exactly 32 bytes.
  if (/^[A-Za-z0-9+/]+={0,2}$/.test(text) && text.length % 4 === 0) {
    const raw = Buffer.from(text, 'base64');
    if (raw.length === KEY_BYTES) return raw;
  }

  // 3. Passphrase → SHA-256.
  return createHash('sha256').update(text, 'utf8').digest();
}

/**
 * Seal a payload for the mobile client.
 *
 * @param {object} payload   JSON-serialisable signal
 * @param {string|Buffer} key
 * @param {object} [opts]
 * @param {string} [opts.algorithm]
 * @returns {string} `v1.<base64>`
 */
export function encryptSignal(payload, key, { algorithm = CIPHER_ALGORITHM } = {}) {
  const secret = normalizeKey(key);
  const iv = randomBytes(IV_BYTES);

  const cipher = createCipheriv(algorithm, secret, iv);
  const body = Buffer.concat([
    cipher.update(Buffer.from(JSON.stringify(payload), 'utf8')),
    cipher.final(),
  ]);
  const tag = cipher.getAuthTag();

  return `${CIPHER_VERSION}.${Buffer.concat([iv, tag, body]).toString('base64')}`;
}

/**
 * Open a sealed payload. Exported for tests and for server-side tooling — the
 * browser has its own implementation of the same three rules.
 *
 * Throws on a wrong key, a tampered blob or an unknown version.
 *
 * @param {string} blob
 * @param {string|Buffer} key
 * @param {object} [opts]
 * @param {string} [opts.algorithm]
 * @returns {object} the original payload
 */
export function decryptSignal(blob, key, { algorithm = CIPHER_ALGORITHM } = {}) {
  const text = String(blob ?? '');
  const dot = text.indexOf('.');
  if (dot < 0 || text.slice(0, dot) !== CIPHER_VERSION) {
    throw new Error(`unsupported cipher version (expected "${CIPHER_VERSION}.")`);
  }

  const raw = Buffer.from(text.slice(dot + 1), 'base64');
  if (raw.length <= IV_BYTES + TAG_BYTES) throw new Error('ciphertext is too short');

  const iv = raw.subarray(0, IV_BYTES);
  const tag = raw.subarray(IV_BYTES, IV_BYTES + TAG_BYTES);
  const body = raw.subarray(IV_BYTES + TAG_BYTES);

  const decipher = createDecipheriv(algorithm, normalizeKey(key), iv);
  decipher.setAuthTag(tag);
  const plain = Buffer.concat([decipher.update(body), decipher.final()]).toString('utf8');
  return JSON.parse(plain);
}

export default { encryptSignal, decryptSignal, normalizeKey, CIPHER_VERSION, CIPHER_ALGORITHM };
