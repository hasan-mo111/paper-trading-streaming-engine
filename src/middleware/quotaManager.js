/* =============================================================================
 *  AETHERON CORE  —  src/middleware/quotaManager.js
 *  -----------------------------------------------------------------------------
 *  The freemium gate and the HTTP rate limiter.
 *
 *  THE RULE
 *  --------
 *    FREE → at most FREE_DAILY_EXECUTIONS (default 3) AUTOMATIC server
 *           executions per day. Once spent, the engine still produces signals
 *           but downgrades the event to `SIGNAL_ONLY` — the user sees the idea,
 *           the server simply does not place the order.
 *    PRO  → unlimited.
 *
 *  WHY EXITS NEVER CONSUME QUOTA
 *  ------------------------------
 *  A position that has been opened must always be stoppable, whatever the
 *  user's plan or counter state. Charging the counter for closing a position
 *  would let a FREE user end up with an unstoppable trade, so `kind: 'EXIT'`
 *  is explicitly free. Only `kind: 'ENTRY'` spends.
 *
 *  ATOMICITY
 *  ---------
 *  The counter is owned by Postgres (see `aetheron_consume_execution` in
 *  supabase/schema.sql). Reading-then-writing from the app would race across
 *  two Render instances and let a user slip past the limit; the RPC does the
 *  read-modify-write inside one transaction, with a compare-and-swap UPDATE as
 *  a fallback. When the database is unreachable we FAIL CLOSED (treat the
 *  quota as exhausted) rather than fail open.
 * ============================================================================= */

import { ENV } from '../config/env.js';
import { createLogger } from '../services/logger.js';

const log = createLogger('quota');

/** Plan identifiers — kept as constants so a typo cannot silently widen access. */
export const PLANS = Object.freeze({ FREE: 'FREE', PRO: 'PRO' });

export class QuotaManager {
  /**
   * @param {object} opts
   * @param {SupabaseService} opts.supabase
   * @param {AuthService} [opts.auth]      used to read plan + sizing profile
   * @param {object} [opts.env]
   */
  constructor({ supabase, auth = null, env = ENV } = {}) {
    this.supabase = supabase;
    this.auth = auth;
    this.env = env;

    /** In-memory mirror so `/api/user/quota` does not need a DB round-trip. */
    this.usage = new Map(); // `${userId}|${day}` → { used, limit, plan, at }
    this.counters = { checks: 0, allowed: 0, denied: 0, exitsBypassed: 0, failures: 0 };

    /** Guards against two concurrent entries on the same user in one tick. */
    this.inflight = new Map(); // userId → Promise
  }

  /* ------------------------------------------------------------------ plans */

  /** Daily limit for a plan. 0 (PRO) means unlimited. */
  limitFor(plan) {
    return String(plan).toUpperCase() === PLANS.PRO
      ? this.env.quota.PRO_DAILY_EXECUTIONS // 0 = unlimited
      : this.env.quota.FREE_DAILY_EXECUTIONS;
  }

  /** Resolve a user's plan + balance/risk parameters. */
  async contextFor(userId, fallbackPlan = PLANS.FREE) {
    const profile = (await this.auth?.getProfile(userId)) ?? null;
    const plan = String(profile?.plan ?? fallbackPlan).toUpperCase() === PLANS.PRO ? PLANS.PRO : PLANS.FREE;
    return {
      userId,
      plan,
      limit: this.limitFor(plan),
      unlimited: this.limitFor(plan) === 0,
      accountBalance: Number(profile?.account_balance ?? this.env.broker.DEFAULT_BALANCE),
      riskPercentage: Number(profile?.risk_percentage ?? this.env.broker.DEFAULT_RISK_PCT),
      tradingEnabled: profile ? profile.enabled !== false : true,
      symbols: Array.isArray(profile?.symbols) && profile.symbols.length ? profile.symbols : null,
    };
  }

  /* ------------------------------------------------------------------ usage */

  #key(userId, day) {
    return `${userId}|${day}`;
  }

  /** Current quota day, honouring the configured midnight offset. */
  today(date = new Date()) {
    return this.supabase.dayKey(date);
  }

  /** Read today's usage without spending anything. */
  async usageFor(userId, plan = PLANS.FREE) {
    const day = this.today();
    const limit = this.limitFor(plan);
    const cached = this.usage.get(this.#key(userId, day));

    let used = cached?.used;
    if (used == null) {
      used = await this.supabase.getDailyUsage(userId, day);
      this.usage.set(this.#key(userId, day), { used, limit, plan, at: Date.now() });
    }

    const remaining = limit === 0 ? Infinity : Math.max(0, limit - used);
    return {
      userId,
      plan,
      day,
      limit, // 0 = unlimited
      unlimited: limit === 0,
      used,
      remaining,
      exhausted: limit !== 0 && used >= limit,
    };
  }

  /**
   * Ask for permission to execute.
   *
   * This function does NOT spend anything — it only answers "may we?".
   * Spending happens in `consume()` after the broker has actually filled, so
   * a rejected order never costs the user one of their three daily trades.
   *
   * @param {string} userId
   * @param {object} [opts]
   * @param {'FREE'|'PRO'} [opts.plan]
   * @param {'ENTRY'|'EXIT'} [opts.kind]
   * @returns {Promise<{allowed:boolean, reason:string, usage:object}>}
   */
  async canExecute(userId, { plan = PLANS.FREE, kind = 'ENTRY' } = {}) {
    this.counters.checks += 1;

    // Exits are always allowed — see the header note.
    if (kind === 'EXIT') {
      this.counters.exitsBypassed += 1;
      const usage = await this.usageFor(userId, plan);
      return { allowed: true, reason: 'exit-not-metered', usage };
    }

    const usage = await this.usageFor(userId, plan);
    if (usage.unlimited) {
      this.counters.allowed += 1;
      return { allowed: true, reason: 'pro-unlimited', usage };
    }
    if (!usage.exhausted) {
      this.counters.allowed += 1;
      return { allowed: true, reason: 'within-quota', usage };
    }

    this.counters.denied += 1;
    return {
      allowed: false,
      reason: 'daily-quota-exhausted',
      usage,
      // Consumed by the engine to build the SIGNAL_ONLY chat message.
      downgrade: {
        type: 'SIGNAL_ONLY',
        message:
          `${plan} plan — daily automatic execution limit reached ` +
          `(${usage.used}/${usage.limit}). Signal published, not executed.`,
      },
    };
  }

  /**
   * Spend one execution. Call this only after a real fill.
   *
   * Concurrent calls for the same user are serialised, because two entries
   * firing in the same millisecond would otherwise both read `used = 2` and
   * both be admitted as the third and fourth trade.
   */
  async consume(userId, { plan = PLANS.FREE, kind = 'ENTRY' } = {}) {
    if (kind === 'EXIT') return { spent: false, reason: 'exit-not-metered' };

    const previous = this.inflight.get(userId);
    if (previous) await previous.catch(() => {});

    const task = (async () => {
      const day = this.today();
      const limit = this.limitFor(plan);
      if (limit === 0) {
        // PRO: no counter to keep, but the row is still written so the UI can
        // report "N executions today" for an unlimited plan.
        const res = await this.supabase.consumeExecution(userId, 0, day);
        this.usage.set(this.#key(userId, day), { used: res.used, limit, plan, at: Date.now() });
        return { spent: false, reason: 'pro-unlimited', used: res.used, source: res.source };
      }

      const res = await this.supabase.consumeExecution(userId, limit, day);

      if (res.source === 'failed') {
        // Fail closed: an unreadable counter must not become a free pass.
        this.counters.failures += 1;
        log.error(`counter unavailable for ${userId} — treating the quota as exhausted`);
        return { spent: true, failed: true, used: limit, source: 'failed' };
      }

      this.usage.set(this.#key(userId, day), { used: res.used, limit, plan, at: Date.now() });
      return { spent: true, used: res.used, source: res.source };
    })();

    this.inflight.set(userId, task);
    try {
      return await task;
    } finally {
      if (this.inflight.get(userId) === task) this.inflight.delete(userId);
    }
  }

  /** Broadcast-friendly status blob for the chat stream / quota endpoint. */
  async describe(userId, plan = PLANS.FREE) {
    const usage = await this.usageFor(userId, plan);
    return {
      plan: usage.plan,
      day: usage.day,
      limit: usage.limit,
      unlimited: usage.unlimited,
      used: usage.used,
      remaining: usage.remaining === Infinity ? null : usage.remaining,
    };
  }

  /** Forget cached usage (used by tests and after an admin reset). */
  invalidate(userId) {
    for (const key of [...this.usage.keys()]) {
      if (key.startsWith(`${userId}|`)) this.usage.delete(key);
    }
  }

  stats() {
    return { ...this.counters, trackedUsers: this.usage.size };
  }
}

/* =============================================================================
 *  HTTP RATE LIMITER  —  sliding window, per key (IP by default)
 * ============================================================================= */

/**
 * Minimal fixed-memory sliding-window limiter. No dependency, no unbounded
 * growth: expired buckets are swept on a timer.
 *
 * @param {object} opts
 * @param {number} opts.windowMs
 * @param {number} opts.max        requests per window
 * @param {(req)=>string} [opts.keyFn]
 */
export function createRateLimiter({
  windowMs = ENV.quota.RATE_LIMIT_WINDOW_MS,
  max = ENV.quota.RATE_LIMIT_MAX,
  keyFn = (req) => req.ip ?? req.socket?.remoteAddress ?? 'unknown',
  logger = log,
} = {}) {
  /** key → array of request timestamps */
  const hits = new Map();

  const sweep = setInterval(() => {
    const cutoff = Date.now() - windowMs;
    for (const [key, stamps] of hits) {
      const kept = stamps.filter((t) => t > cutoff);
      if (kept.length === 0) hits.delete(key);
      else hits.set(key, kept);
    }
  }, windowMs);
  sweep.unref?.();

  /**
   * @returns {{allowed:boolean, remaining:number, resetMs:number}}
   */
  const check = (req) => {
    const key = keyFn(req);
    const now = Date.now();
    const cutoff = now - windowMs;
    const stamps = (hits.get(key) ?? []).filter((t) => t > cutoff);

    if (stamps.length >= max) {
      hits.set(key, stamps);
      return { allowed: false, remaining: 0, resetMs: stamps[0] + windowMs - now };
    }
    stamps.push(now);
    hits.set(key, stamps);
    return { allowed: true, remaining: max - stamps.length, resetMs: windowMs };
  };

  /** Express middleware form: 429s with standard rate-limit headers. */
  const middleware = (req, res, next) => {
    const result = check(req);
    res.set?.('X-RateLimit-Limit', String(max));
    res.set?.('X-RateLimit-Remaining', String(result.remaining));
    res.set?.('X-RateLimit-Reset', String(Math.ceil(result.resetMs / 1000)));
    if (!result.allowed) {
      res.set?.('Retry-After', String(Math.ceil(result.resetMs / 1000)));
      logger.warn(`rate limit hit for ${keyFn(req)}`);
      return res.status(429).json({
        error: { code: 'rate_limited', message: 'Too many requests — slow down' },
      });
    }
    return next();
  };

  return { check, middleware, stop: () => clearInterval(sweep), get size() { return hits.size; } };
}

export default QuotaManager;