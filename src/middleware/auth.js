/* =============================================================================
 *  AETHERON CORE  —  src/middleware/auth.js
 *  -----------------------------------------------------------------------------
 *  Token → user resolution, plus the express middlewares that enforce it.
 *
 *  HOW A TOKEN IS ACCEPTED
 *  -----------------------
 *  1. `Authorization: Bearer <supabase-access-token>` (native apps, Postman)
 *  2. `?access_token=<token>` (EventSource / SSE — the browser API cannot set
 *     headers, this is the standard workaround)
 *  3. `x-access-token: <token>` (older Android HTTP stacks)
 *
 *  The token is verified server-side against Supabase (`auth.getUser`), then the
 *  resulting `user.id` is mapped to a trading profile in `profiles`. A verified
 *  token is cached for AUTH_CACHE_TTL_MS so the tick and exit loops never make a
 *  network round-trip.
 *
 *  NOTE ON THE DEV ESCAPE HATCH
 *  -----------------------------
 *  DEV_TOKEN exists purely so the engine can be developed without a Supabase
 *  project. env.js blanks it in production, so a leaked default cannot work
 *  there even if somebody hard-codes it.
 * ============================================================================= */

import { ENV } from '../config/env.js';
import { createLogger } from '../services/logger.js';

const log = createLogger('auth');

/** Extract a bearer token from a request, whichever transport the client used. */
export function tokenFromRequest(req) {
  const header = req.get?.('authorization') ?? req.headers?.authorization;
  if (header && /^bearer\s+/i.test(header)) return header.replace(/^bearer\s+/i, '').trim();

  const custom = req.get?.('x-access-token') ?? req.headers?.['x-access-token'];
  if (custom) return String(custom).trim();

  const q = req.query ?? {};
  const fromQuery = q.access_token ?? q.token;
  if (fromQuery) return String(fromQuery).trim();

  return null;
}

/** Small TTL cache with a hard cap so a token flood cannot grow it forever. */
class TtlCache {
  constructor({ ttlMs, max }) {
    this.ttlMs = ttlMs;
    this.max = max;
    this.map = new Map();
    this.hits = 0;
    this.misses = 0;
  }

  get(key) {
    const hit = this.map.get(key);
    if (!hit) {
      this.misses += 1;
      return undefined;
    }
    if (hit.expiresAt < Date.now()) {
      this.map.delete(key);
      this.misses += 1;
      return undefined;
    }
    this.hits += 1;
    return hit.value;
  }

  set(key, value) {
    if (this.map.size >= this.max) {
      // Cheap eviction: drop the oldest insertion.
      const oldest = this.map.keys().next().value;
      if (oldest !== undefined) this.map.delete(oldest);
    }
    this.map.set(key, { value, expiresAt: Date.now() + this.ttlMs });
  }

  get size() {
    return this.map.size;
  }
}

export class AuthService {
  /**
   * @param {object} opts
   * @param {SupabaseService} opts.supabase
   * @param {object} [opts.env]
   */
  constructor({ supabase, env = ENV } = {}) {
    this.supabase = supabase;
    this.env = env;
    this.tokenCache = new TtlCache({ ttlMs: env.auth.CACHE_TTL_MS, max: env.auth.CACHE_MAX });
    this.profileCache = new TtlCache({ ttlMs: env.quota.PROFILE_TTL_MS, max: env.auth.CACHE_MAX });
    this.counters = { resolutions: 0, cacheHits: 0, rejected: 0 };
  }

  /* ------------------------------------------------------------------ tokens */

  /**
   * Resolve a token to `{ id, email, plan, … }`, or null.
   * A null result means "not authenticated" — never an exception.
   */
  async resolveToken(token) {
    if (!token) return null;

    // ---- development escape hatch (disabled in production by env.js) ----
    if (this.env.auth.DEV_TOKEN && token === this.env.auth.DEV_TOKEN) {
      return {
        id: 'dev-local-user',
        email: 'dev@localhost',
        plan: 'PRO', // a local developer always sees the full feature set
        dev: true,
        token: 'dev',
      };
    }

    if (this.tokenCache.get(token)) return this.tokenCache.get(token);

    const user = await this.supabase.getUserByToken(token);
    this.counters.resolutions += 1;
    if (!user) {
      this.counters.rejected += 1;
      return null;
    }

    const profile = await this.getProfile(user.id);
    const identity = {
      id: user.id,
      email: user.email,
      plan: profile?.plan ?? 'FREE',
      dev: false,
    };
    this.tokenCache.set(token, identity);
    return identity;
  }

  /**
   * Trading profile for a user id, cached briefly.
   * The cache exists because the exit loop reads the plan/size parameters
   * several times per second; the daily *counter* is never served from cache.
   */
  async getProfile(userId) {
    if (!userId) return null;
    const cached = this.profileCache.get(userId);
    if (cached) return cached;
    const profile = await this.supabase.getProfile(userId);
    if (profile) this.profileCache.set(userId, profile);
    return profile;
  }

  /** Force the next read to hit the database (after a plan change). */
  invalidate(userId) {
    if (userId) this.profileCache.set(userId, null);
  }

  /* -------------------------------------------------------------- middleware */

  /**
   * Hard gate. Populates `req.user`; answers 401 otherwise.
   *
   * @param {object} [opts]
   * @param {boolean} [opts.optional]  continue anonymously when no token is present
   * @param {boolean} [opts.sse]       emit an SSE `auth` frame instead of JSON
   */
  middleware({ optional = false, sse = false } = {}) {
    return async (req, res, next) => {
      const token = tokenFromRequest(req);

    if (!token) {
      if (optional) return next();
      return this.#deny(req, res, sse, 'missing_token', 'Authorization header (or ?access_token=) is required');
    }

    const user = await this.resolveToken(token);
    if (!user) {
      this.counters.rejected += 1;
      if (optional) return next();
      return this.#deny(req, res, sse, 'invalid_token', 'Token is invalid or expired');
    }

      // Profile carries plan + sizing parameters; the engine needs both.
      req.user = user;
      req.profile = await this.getProfile(user.id);
      next();
    };
  }

  /** SSE-friendly denial: the socket is already a 200 event-stream by then. */
  #deny(req, res, sse, code, message) {
    if (sse) {
      try {
        if (!res.headersSent) {
          res.writeHead(401, {
            'content-type': 'text/event-stream; charset=utf-8',
            'cache-control': 'no-cache, no-transform',
            connection: 'keep-alive',
          });
          res.flushHeaders?.();
        }
        res.write(`event: auth\ndata: ${JSON.stringify({ type: 'AUTH_ERROR', code, message })}\n\n`);
        res.end();
      } catch {
        /* client already gone */
      }
      return;
    }
    res.status(401).json({ error: { code, message } });
  }

  /** Diagnostics for /health. */
  stats() {
    return {
      ...this.counters,
      tokenCacheSize: this.tokenCache.size,
      profileCacheSize: this.profileCache.size,
      devTokenEnabled: Boolean(this.env.auth.DEV_TOKEN),
    };
  }
}

/**
 * Convenience factory used by the bootstrap.
 * Exposes the service plus ready-made express middlewares:
 *   require / optional / sse / optionalSse
 */
export function createAuth({ supabase, env = ENV } = {}) {
  const service = new AuthService({ supabase, env });
  return {
    service,
    resolveToken: (token) => service.resolveToken(token),
    getProfile: (userId) => service.getProfile(userId),
    require: service.middleware(),
    optional: service.middleware({ optional: true }),
    sse: service.middleware({ sse: true }),
    optionalSse: service.middleware({ optional: true, sse: true }),
  };
}

export default AuthService;