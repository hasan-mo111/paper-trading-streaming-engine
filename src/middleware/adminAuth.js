/* =============================================================================
 *  AETHERON CORE  —  src/middleware/adminAuth.js
 *  -----------------------------------------------------------------------------
 *  The admin console's own authentication. It deliberately does NOT use
 *  Supabase auth:
 *
 *    • the operator is a single server-side account, not an app user;
 *    • the credential lives in the Render environment, never in the database;
 *    • the browser receives only a short-lived HMAC-signed cookie.
 *
 *  PASSWORD STORAGE
 *  ----------------
 *  ADMIN_PASSWORD_HASH=scrypt$<salt-b64url>$<hash-b64url> is preferred. A plain
 *  ADMIN_PASSWORD is still accepted so a first deploy is one env var away; the
 *  boot audit warns about short values.
 *
 *  SESSION COOKIE
 *  --------------
 *  `<base64url(payload)>.<base64url(HMAC-SHA256(payload))>`, HttpOnly,
 *  SameSite=Strict and Secure in production. There is no server-side session
 *  table, so a Render restart does not log the operator out — as long as the
 *  password / ADMIN_SESSION_SECRET does not change.
 * ============================================================================= */

import crypto from 'node:crypto';
import { ENV } from '../config/env.js';
import { createLogger } from '../services/logger.js';

const log = createLogger('adminAuth');

export const ADMIN_COOKIE = 'aetheron_admin';

const SCRYPT_N = 16_384;
const SCRYPT_R = 8;
const SCRYPT_P = 1;
const KEY_LEN = 64;

/** base64url without padding — safe inside a cookie and a JSON payload. */
function b64url(buffer) {
  return Buffer.from(buffer).toString('base64url');
}

function sha256(value) {
  return crypto.createHash('sha256').update(String(value), 'utf8').digest();
}

/** Constant-time comparison that does not leak the length. */
function safeEqual(a, b) {
  const left = sha256(a);
  const right = sha256(b);
  return crypto.timingSafeEqual(left, right);
}

/**
 * Produce `scrypt$salt$hash` for ADMIN_PASSWORD_HASH.
 * @param {string} password
 * @returns {string}
 */
export function hashPassword(password) {
  const salt = crypto.randomBytes(16);
  const hash = crypto.scryptSync(String(password), salt, KEY_LEN, {
    N: SCRYPT_N,
    r: SCRYPT_R,
    p: SCRYPT_P,
    maxmem: 64 * 1024 * 1024,
  });
  return `scrypt$${b64url(salt)}$${b64url(hash)}`;
}

/**
 * Verify a password against a scrypt hash. Malformed hashes fail closed.
 * @param {string} password
 * @param {string} stored
 */
export function verifyPasswordHash(password, stored) {
  try {
    const [scheme, saltText, hashText] = String(stored).split('$');
    if (scheme !== 'scrypt' || !saltText || !hashText) return false;
    const salt = Buffer.from(saltText, 'base64url');
    const expected = Buffer.from(hashText, 'base64url');
    if (expected.length !== KEY_LEN) return false;
    const actual = crypto.scryptSync(String(password), salt, expected.length, {
      N: SCRYPT_N,
      r: SCRYPT_R,
      p: SCRYPT_P,
      maxmem: 64 * 1024 * 1024,
    });
    return crypto.timingSafeEqual(actual, expected);
  } catch {
    return false;
  }
}

function readCookie(req, name) {
  const header = req?.headers?.cookie;
  if (!header) return null;
  for (const part of String(header).split(';')) {
    const idx = part.indexOf('=');
    if (idx === -1) continue;
    if (part.slice(0, idx).trim() === name) {
      const raw = part.slice(idx + 1).trim();
      try {
        return decodeURIComponent(raw);
      } catch {
        // A malformed percent escape is an invalid cookie, not a server error.
        return null;
      }
    }
  }
  return null;
}

export class AdminAuth {
  /**
   * @param {object} [opts]
   * @param {object} [opts.env]
   */
  constructor({ env = ENV } = {}) {
    this.env = env;
    this.cfg = env.admin ?? {};
    this.failures = new Map(); // ip → { count, resetAt }
    this.counters = { logins: 0, failures: 0, denied: 0 };

    const credential = this.cfg.PASSWORD_HASH || this.cfg.PASSWORD || '';
    const secret = this.cfg.SESSION_SECRET || `aetheron-admin:${this.cfg.USERNAME}:${credential}`;
    this.sessionKey = sha256(secret);
    this.ttlMs = Number(this.cfg.SESSION_TTL_MS ?? 12 * 3_600_000);
  }

  get enabled() {
    return Boolean(this.cfg.ENABLED !== false && (this.cfg.PASSWORD_HASH || this.cfg.PASSWORD));
  }

  get username() {
    return String(this.cfg.USERNAME || 'admin');
  }

  /* ------------------------------------------------------------ credentials */

  verifyPassword(password) {
    if (!password) return false;
    if (this.cfg.PASSWORD_HASH) return verifyPasswordHash(password, this.cfg.PASSWORD_HASH);
    if (!this.cfg.PASSWORD) return false;
    return safeEqual(password, this.cfg.PASSWORD);
  }

  /** Check both fields without revealing which one was wrong. */
  verifyCredentials(username, password) {
    if (!this.enabled) return false;
    // Evaluate both sides even when the username is wrong: scrypt should cost
    // the same for every failed login, so timing cannot enumerate the account.
    const userOk = safeEqual(String(username ?? ''), this.username);
    const passOk = this.verifyPassword(String(password ?? ''));
    return userOk && passOk;
  }

  /* --------------------------------------------------------------- sessions */

  issueSession() {
    const now = Date.now();
    const payload = b64url(JSON.stringify({ v: 1, sub: this.username, iat: now, exp: now + this.ttlMs }));
    const sig = b64url(crypto.createHmac('sha256', this.sessionKey).update(payload).digest());
    return `${payload}.${sig}`;
  }

  verifySessionToken(token) {
    if (!token) return null;
    const [payload, sig] = String(token).split('.');
    if (!payload || !sig) return null;
    try {
      const expected = b64url(crypto.createHmac('sha256', this.sessionKey).update(payload).digest());
      const left = Buffer.from(sig);
      const right = Buffer.from(expected);
      if (left.length !== right.length || !crypto.timingSafeEqual(left, right)) return null;
      const data = JSON.parse(Buffer.from(payload, 'base64url').toString('utf8'));
      if (!data || data.v !== 1 || data.sub !== this.username) return null;
      if (!Number.isFinite(data.exp) || data.exp <= Date.now()) return null;
      return data;
    } catch {
      return null;
    }
  }

  sessionFromRequest(req) {
    return this.verifySessionToken(readCookie(req, ADMIN_COOKIE));
  }

  cookieHeader(token) {
    const parts = [
      `${ADMIN_COOKIE}=${encodeURIComponent(token)}`,
      'Path=/admin',
      'HttpOnly',
      'SameSite=Strict',
      `Max-Age=${Math.floor(this.ttlMs / 1000)}`,
    ];
    if (this.env.server?.IS_PRODUCTION) parts.push('Secure');
    return parts.join('; ');
  }

  clearCookieHeader() {
    const parts = [`${ADMIN_COOKIE}=`, 'Path=/admin', 'HttpOnly', 'SameSite=Strict', 'Max-Age=0'];
    if (this.env.server?.IS_PRODUCTION) parts.push('Secure');
    return parts.join('; ');
  }

  /* ------------------------------------------------------------ rate limiter */

  loginAllowed(ip) {
    const entry = this.failures.get(ip);
    if (!entry) return true;
    if (entry.resetAt <= Date.now()) {
      this.failures.delete(ip);
      return true;
    }
    return entry.count < Number(this.cfg.LOGIN_MAX ?? 8);
  }

  recordFailure(ip) {
    const windowMs = Number(this.cfg.LOGIN_WINDOW_MS ?? 15 * 60_000);
    const entry = this.failures.get(ip);
    if (!entry || entry.resetAt <= Date.now()) {
      this.failures.set(ip, { count: 1, resetAt: Date.now() + windowMs });
      return;
    }
    entry.count += 1;
    // Bound the map against a distributed spray of source addresses.
    if (this.failures.size > 5_000) {
      const oldest = this.failures.keys().next().value;
      if (oldest) this.failures.delete(oldest);
    }
  }

  clearFailures(ip) {
    this.failures.delete(ip);
  }

  /** Reject cross-site writes even if a cookie somehow leaks. */
  sameOrigin(req) {
    const origin = req.get?.('origin') ?? req.headers?.origin;
    if (!origin) return true; // curl / native admin tooling
    const host = req.get?.('host') ?? req.headers?.host;
    if (!host) return false;
    const expected = `${req.protocol}://${host}`;
    try {
      return new URL(origin).origin === new URL(expected).origin;
    } catch {
      return false;
    }
  }

  /**
   * Express middleware for every admin API except login/me.
   * @param {{api?:boolean}} [opts]
   */
  middleware({ api = true } = {}) {
    return (req, res, next) => {
      if (!this.enabled) {
        return res.status(503).json({
          error: {
            code: 'admin_disabled',
            message: 'Admin console is not configured. Set ADMIN_PASSWORD (or ADMIN_PASSWORD_HASH) in Render → Environment.',
          },
        });
      }
      const session = this.sessionFromRequest(req);
      if (!session) {
        this.counters.denied += 1;
        if (api) {
          return res.status(401).json({ error: { code: 'admin_unauthorized', message: 'Sign in to continue' } });
        }
        return res.redirect('/admin/');
      }
      req.admin = session;
      return next();
    };
  }

  stats() {
    return {
      enabled: this.enabled,
      username: this.username,
      sessionTtlMs: this.ttlMs,
      ...this.counters,
      trackedLoginIps: this.failures.size,
    };
  }
}

export default AdminAuth;
