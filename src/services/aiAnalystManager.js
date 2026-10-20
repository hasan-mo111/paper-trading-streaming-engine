/* =============================================================================
 *  AETHERON CORE  —  src/services/aiAnalystManager.js
 *  -----------------------------------------------------------------------------
 *  Runs one AiSignalService per instrument and owns the clock between them.
 *
 *  WHY A MANAGER INSTEAD OF A WIDER ANALYST
 *  ----------------------------------------
 *  Everything the analyst knows is per-instrument: the candles it snapshots, the
 *  trade it is following, the signal it last published, the Telegram card it
 *  de-dupes. Widening that class into a map of symbols would mean touching every
 *  one of its private helpers. Instead this class instantiates the existing,
 *  already-tested analyst once per symbol and layers the three things that are
 *  genuinely shared on top:
 *
 *    1. ONE CLOCK. Four independent 30-minute timers would drift into four
 *       simultaneous bursts. Here a single scheduler walks the symbols in order
 *       and can stretch or stretch-and-pause the whole loop.
 *
 *    2. QUOTA BACKOFF. A free model tier answers 429 when the per-minute or
 *       per-day cap is spent, and 402 when credits run out. On that reply the
 *       manager abandons the rest of the cycle (they would fail identically),
 *       multiplies the interval (×1 → ×2 → ×4, capped) and holds a cooldown
 *       before trying again. When a full cycle succeeds the factor decays back
 *       toward 1. That is the "if the limit runs out, slow down" behaviour.
 *
 *    3. CHEAP CYCLES WHILE A TRADE IS OPEN. A position's levels are fixed once
 *       it is opened; tick handlers already enforce the stop and the targets.
 *       So while a symbol holds a trade the model is consulted only every
 *       MANAGEMENT_EVERY cycles (looking for a reversal), and the cycles in
 *       between publish a status built from the live price — zero cost. With
 *       four symbols this roughly halves quota consumption when the book is
 *       busy, which is exactly when calls are most expensive to waste.
 *
 *  It exposes the same shape the old single-analyst wiring used (enabled, run,
 *  start, stop, status, onPrice, attachTelegram), so routes and /health do not
 *  care that the analyst is now plural.
 * ============================================================================= */

import { AiSignalService } from './aiSignal.js';
import { SYMBOL_SPECS } from '../config/db.js';
import { createLogger } from './logger.js';

const log = createLogger('ai');

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/** Aggregate counters mirrored from every per-symbol analyst. */
const COUNTERS = [
  'runs',
  'published',
  'holds',
  'rejected',
  'errors',
  'fallbacks',
  'opened',
  'closed',
  'wins',
  'losses',
  'reversals',
  'statuses',
  'rateLimits',
];

export class AiAnalystManager {
  /**
   * @param {object} deps
   * @param {object} deps.market       shared MarketAggregator
   * @param {object} deps.dispatcher   shared SSEDispatcher
   * @param {object} deps.env          parsed env (env.ai is the analyst config)
   * @param {object} [deps.news]       EconomicNewsService (shared, cached)
   * @param {object} [deps.telegram]   Telegram gateway handle
   * @param {Function} [deps.fetchFn]  injected fetch, for tests
   */
  constructor({ market, dispatcher, env, news = null, telegram = null, fetchFn = null }) {
    this.env = env;
    this.cfg = env.ai;
    this.market = market;
    this.dispatcher = dispatcher;
    this.news = news;

    // Only symbols the platform actually trades. An operator typo in
    // AI_SIGNAL_SYMBOLS is dropped here rather than causing four failed
    // snapshots per cycle.
    const requested = Array.isArray(this.cfg.SYMBOLS) ? this.cfg.SYMBOLS : [];
    const known = requested.map((s) => String(s).toUpperCase()).filter((s) => SYMBOL_SPECS[s]);
    this.symbols = known.length ? [...new Set(known)] : ['XAUUSD'];

    /** @type {Map<string, AiSignalService>} */
    this.services = new Map();
    for (const symbol of this.symbols) {
      // Catch-up is a manager concern now: a per-symbol retry timer would fire
      // off-clock and fight the shared one. Everything else is inherited.
      const scopedEnv = {
        ...env,
        ai: { ...env.ai, SYMBOL: symbol, CATCHUP_ENABLED: false },
      };
      this.services.set(
        symbol,
        new AiSignalService({ market, dispatcher, env: scopedEnv, news, telegram, fetchFn })
      );
    }

    this.timer = null;
    /** True while a cycle is walking the symbols. */
    this.running = false;
    /** Interval multiplier: 1 normally, doubled per quota rejection. */
    this.factor = 1;
    /** Epoch ms before which no cycle may start. */
    this.pauseUntil = 0;
    /** Cycles attempted since boot (used for the management cadence). */
    this.cycle = 0;
    this.catchupsUsed = 0;
    this.lastCycleAt = null;
    this.lastResults = [];
    this.stopped = true;
  }

  /* ------------------------------------------------------------- accessors */

  get enabled() {
    return [...this.services.values()].some((svc) => svc.enabled);
  }

  get configured() {
    return Boolean(this.cfg.API_KEY);
  }

  get baseIntervalMs() {
    return Math.max(60_000, Number(this.cfg.INTERVAL_MS) || 1_800_000);
  }

  /** The interval actually in force, after any quota penalty. */
  intervalMs() {
    return Math.round(this.baseIntervalMs * this.factor);
  }

  /** ms until the cooldown lifts (0 when not cooling down). */
  cooldownRemainingMs(now = Date.now()) {
    return Math.max(0, this.pauseUntil - now);
  }

  /* -------------------------------------------------------------- plumbing */

  attachTelegram(handle) {
    for (const svc of this.services.values()) svc.attachTelegram(handle);
  }

  /**
   * Fan a trade tick out to the analyst that owns it. Symbols with no trade and
   * no service are simply ignored — this runs on every tick of every symbol.
   */
  onPrice(symbol, price) {
    const svc = this.services.get(String(symbol).toUpperCase());
    if (svc) svc.onPrice(symbol, price);
  }

  /** One instrument's analyst (tests and tooling). */
  serviceFor(symbol) {
    return this.services.get(String(symbol).toUpperCase()) ?? null;
  }

  /* ----------------------------------------------------------------- status */

  /**
   * Aggregate status for /health and /api/ai/signal.
   *
   * Every per-symbol status is plaintext now: the levels travel on the public
   * SSE feed exactly as they do here, so the aggregate is a straight
   * collection plus the shared counters.
   */
  status() {
    const perSymbol = {};
    const totals = Object.fromEntries(COUNTERS.map((k) => [k, 0]));

    for (const [symbol, svc] of this.services) {
      const st = svc.status();
      perSymbol[symbol] = { ...st, news: undefined };
      for (const key of COUNTERS) totals[key] += Number(st[key] || 0);
    }

    const cooldownMs = this.cooldownRemainingMs();

    return {
      enabled: this.enabled,
      configured: this.configured,
      multiSymbol: true,
      symbols: [...this.symbols],
      provider: this.cfg.PROVIDER,
      providerLabel: this.cfg.PROVIDER_LABEL,
      model: this.cfg.MODEL,
      intervalMs: this.intervalMs(),
      baseIntervalMs: this.baseIntervalMs,
      rateLimitFactor: this.factor,
      staggerMs: this.cfg.STAGGER_MS,
      managementEvery: this.cfg.MANAGEMENT_EVERY,
      running: this.running,
      cycle: this.cycle,
      catchupsUsed: this.catchupsUsed,
      cooldownMs,
      cooldownUntil: cooldownMs > 0 ? new Date(Date.now() + cooldownMs).toISOString() : null,
      lastCycleAt: this.lastCycleAt ? new Date(this.lastCycleAt).toISOString() : null,
      lastResults: this.lastResults,
      openTrades: [...this.services.entries()]
        .filter(([, svc]) => Boolean(svc.position))
        .map(([symbol]) => symbol),
      ...totals,
      perSymbol,
      news: this.news?.status?.() ?? null,
    };
  }

  /* ------------------------------------------------------------------- run */

  /**
   * Walk every symbol once.
   *
   * @param {string} reason  'schedule' | 'boot' | 'catchup' | 'manual'
   * @returns {Promise<object>} aggregate result
   */
  async run(reason = 'schedule') {
    if (!this.enabled) return { ok: false, error: 'disabled', reason };
    if (this.running) return { ok: false, error: 'busy', reason };

    const cooldown = this.cooldownRemainingMs();
    if (cooldown > 0 && reason !== 'manual') {
      return { ok: false, error: 'rate-limited', reason, retryInMs: cooldown };
    }

    this.running = true;
    this.cycle += 1;
    const managementSkip =
      Number(this.cfg.MANAGEMENT_EVERY) > 1 && this.cycle % Number(this.cfg.MANAGEMENT_EVERY) !== 0;
    const results = [];

    try {
      for (const [symbol, svc] of this.services) {
        if (!svc.enabled) {
          results.push({ symbol, ok: false, error: 'disabled' });
          continue;
        }

        // While a trade is open the model is only consulted on reversal-check
        // cycles; the others report from the live price at zero cost.
        if (svc.position && managementSkip) {
          const r = svc.statusUpdate(reason);
          results.push(this.#summarise(symbol, r, svc, 'management-cycle'));
        } else {
          const r = await svc.run(reason);
          results.push(this.#summarise(symbol, r, svc));

          // Quota exhausted: the remaining symbols would fail the same way and
          // each failure spends a little more of a budget that is already gone.
          if (r.error === 'rate-limited') {
            this.#penalise(r.detail || r.error, symbol);
            break;
          }
        }

        if (Number(this.cfg.STAGGER_MS) > 0) await sleep(Number(this.cfg.STAGGER_MS));
      }

      const ok = results.some((r) => r.ok);
      if (ok) this.#relax();

      // Give /api/ai/analyze the same shape it had with a single analyst: a
      // top-level `error` it can map onto a status code.
      const error = ok ? undefined : this.#aggregateError(results);

      this.lastResults = results;
      this.lastCycleAt = Date.now();
      return { ok, error, reason, results, intervalMs: this.intervalMs(), factor: this.factor };
    } finally {
      this.running = false;
    }
  }

  /** Why a whole cycle produced nothing, in the vocabulary the route expects. */
  #aggregateError(results) {
    if (results.length === 0) return 'no-symbols';
    if (results.every((r) => r.error === 'disabled')) return 'disabled';
    if (results.some((r) => r.error === 'rate-limited')) return 'rate-limited';
    if (results.every((r) => r.error === 'insufficient-data')) return 'insufficient-data';
    return 'all-failed';
  }

  /* ---------------------------------------------------------------- results */

  /**
   * Reduce a per-symbol run to a summary that can be published safely.
   *
   * `run()` results reach clients through GET /api/ai/analyze and the manager
   * keeps them in status(), so this stays a small summary rather than the full
   * signal object — but the direction is part of that summary now that the feed
   * is plaintext.
   *
   * @param {string} symbol
   * @param {object} r      what the service returned
   * @param {object} svc    the service (unused, kept for call-site symmetry)
   * @param {string} [skipped]
   */
  #summarise(symbol, r, svc, skipped = null) {
    const out = {
      symbol,
      ok: Boolean(r?.ok),
      error: r?.error ?? null,
      detail: r?.detail ? String(r.detail).slice(0, 200) : null,
      effect: r?.effect ?? r?.kind ?? null,
      skipped: skipped ?? null,
    };
    if (r?.signal) out.action = r.signal.action ?? null;
    return out;
  }

  /* -------------------------------------------------------------- schedule */

  start() {
    this.stopped = false;
    if (!this.enabled) {
      log.warn(
        this.configured
          ? 'AI analyst disabled (AI_SIGNAL_ENABLED=false)'
          : `AI analyst idle — ${this.cfg.PROVIDER === 'groq' ? 'GROQ_API_KEY' : 'OPENROUTER_API_KEY'} not set`
      );
      return;
    }
    if (this.timer) return;

    const bootDelay = Math.max(1_000, Number(this.cfg.BOOT_DELAY_MS) || 120_000);
    this.timer = setTimeout(() => this.#tick('boot'), bootDelay);
    this.timer.unref?.();

    log.ok(
      `AI analyst armed — ${this.cfg.MODEL} on ${this.cfg.PROVIDER_LABEL || 'provider'} · ` +
        `${this.symbols.join(', ')} every ${Math.round(this.baseIntervalMs / 60_000)}m ` +
        `(stagger ${Math.round(Number(this.cfg.STAGGER_MS) / 1000)}s, ` +
        `management every ${this.cfg.MANAGEMENT_EVERY} cycle(s))`
    );
  }

  stop() {
    this.stopped = true;
    if (this.timer) clearTimeout(this.timer);
    this.timer = null;
    for (const svc of this.services.values()) svc.stop?.();
  }

  #tick(reason) {
    this.timer = null;
    this.run(reason)
      .catch((e) => log.error('AI analyst cycle failed:', e.message))
      .finally(() => this.#scheduleNext());
  }

  #scheduleNext() {
    if (this.stopped || this.timer) return;

    const failed = this.lastResults.length > 0 && !this.lastResults.some((r) => r.ok);
    const cooling = this.cooldownRemainingMs() > 0;

    let delay = this.intervalMs();
    if (cooling) {
      // Hold the full interval AND the cooldown: slowing down is the point.
      delay = Math.max(delay, this.cooldownRemainingMs());
    } else if (failed && this.catchupsUsed < Number(this.cfg.CATCHUP_MAX ?? 3)) {
      // Nothing at all was published this cycle — retry sooner than 30 minutes.
      this.catchupsUsed += 1;
      delay = Math.min(delay, Number(this.cfg.CATCHUP_DELAY_MS) || 180_000);
      log.warn(`AI analyst: empty cycle — retrying in ${Math.round(delay / 1000)}s`);
    } else {
      this.catchupsUsed = 0;
    }

    this.timer = setTimeout(() => this.#tick('catchup'), delay);
    this.timer.unref?.();
  }

  /* ----------------------------------------------------------------- quota */

  /** A quota rejection: stretch the interval and hold a cooldown. */
  #penalise(detail, symbol) {
    const max = Math.max(1, Number(this.cfg.RATE_LIMIT_FACTOR_MAX) || 4);
    this.factor = Math.min(this.factor * 2, max);
    this.pauseUntil = Date.now() + Math.max(60_000, Number(this.cfg.RATE_LIMIT_COOLDOWN_MS) || 600_000);
    log.warn(
      `AI analyst: ${symbol} hit the model quota (${String(detail).slice(0, 120)}) — ` +
        `interval ×${this.factor} (${Math.round(this.intervalMs() / 60_000)}m), ` +
        `pause ${Math.round((this.pauseUntil - Date.now()) / 60_000)}m`
    );
  }

  /** A successful cycle buys the interval back, one step at a time. */
  #relax() {
    if (this.factor > 1) {
      this.factor = Math.max(1, this.factor / 2);
      log.ok(`AI analyst: quota recovered — interval ×${this.factor} (${Math.round(this.intervalMs() / 60_000)}m)`);
    }
    this.pauseUntil = 0;
    this.catchupsUsed = 0;
  }
}

export default AiAnalystManager;
