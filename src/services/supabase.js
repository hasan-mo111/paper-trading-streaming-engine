/* =============================================================================
 *  AETHERON CORE  —  src/services/supabase.js
 *  -----------------------------------------------------------------------------
 *  Everything that touches Supabase lives here. Two rules drive the design:
 *
 *  1. NEVER CRASH THE ENGINE.
 *     The client is imported lazily and every call is wrapped: if Supabase is
 *     unreachable the engine keeps trading on RAM and simply stops persisting.
 *     A dead database must never take the market down with it.
 *
 *  2. NEVER BLOCK THE TICK PATH.
 *     Candle writes go through a write-behind queue (dedup by
 *     symbol|timeframe|ts, batched upsert) and run off the hot path.
 *
 *  TABLES (see supabase/schema.sql)
 *  --------------------------------
 *    candles            (symbol, timeframe, ts) PK — permanent OHLC history
 *    profiles           user_id PK — plan, balance, risk, enabled
 *    daily_executions   (user_id, day) PK — atomic daily quota counter
 *    executions         audit log of every order the engine produced
 *    chat_events        chat-ops history, replayed to reconnecting clients
 * ============================================================================= */

import { ENV } from '../config/env.js';
import { createLogger } from './logger.js';
import { normaliseSymbol, normaliseInterval, seriesKey } from '../config/db.js';

const log = createLogger('supabase');

/**
 * Turn a PostgREST error into something actionable.
 *
 * Supabase reports four very different situations through one channel, and all
 * of them otherwise surface as an identical log line. Telling them apart here
 * is the difference between a two-minute fix and an hour of guessing:
 *
 *   "Invalid API key"                 → the KEY is wrong (whitespace/truncated/
 *                                       wrong project). Fix in the dashboard.
 *   "relation … does not exist"       → the key is GOOD, but schema.sql was
 *                                       never run.
 *   "permission denied for table …"   → the key is an anon/publishable key.
 *                                       The service_role key is required.
 *   "JWT expired"/401                 → the key was rotated or revoked.
 *
 * @returns {{error:string, remedy:string}}
 */
function diagnoseSupabaseError(message) {
  const m = String(message ?? '');

  if (/invalid api key/i.test(m)) {
    return {
      error: m,
      remedy:
        'The service_role key is not recognised. Check for a trailing newline or stray space, ' +
        'that the key and URL belong to the SAME Supabase project, and that you copied ' +
        'service_role (not anon / publishable) from Settings -> API.',
    };
  }
  if (/does not exist|schema cache/i.test(m)) {
    return {
      error: m,
      remedy: 'Credentials are valid but a table is missing. Run supabase/schema.sql in the Supabase SQL Editor.',
    };
  }
  if (/permission denied|row-level security|unauthorized/i.test(m)) {
    return {
      error: m,
      remedy: 'The key authenticated but lacks rights. This usually means the anon/publishable key was used instead of service_role.',
    };
  }
  if (/jwt expired|jwt invalid/i.test(m)) {
    return { error: m, remedy: 'The key was rotated or revoked. Copy a fresh service_role key from Settings -> API.' };
  }
  return { error: m, remedy: 'See the Supabase dashboard -> Logs -> API for the full request.' };
}

/** Shape of one candle row on its way into PostgREST. */
const toRow = (symbol, timeframe, candle, closed) => ({
  symbol,
  timeframe,
  ts: candle.time, // epoch seconds, aligned to the timeframe grid
  open: candle.open,
  high: candle.high,
  low: candle.low,
  close: candle.close,
  tick_volume: Math.round(candle.tickVolume ?? 0) || 0,
  closed: Boolean(closed),
});

export class SupabaseService {
  /**
   * @param {object} [opts]
   * @param {object} [opts.env]  config override (tests inject a fake)
   * @param {object} [opts.client] pre-built client (tests / dependency injection)
   */
  constructor({ env = ENV, client = null } = {}) {
    this.env = env;
    this.client = client;
    this.loadError = null;
    this.schema = env.supabase.SCHEMA;

    /* ---- write-behind queue ------------------------------------------- */
    /** dedup map: `${symbol}|${timeframe}|${ts}` → row */
    this.queue = new Map();
    this.flushTimer = null;
    this.flushing = false;
    this.stats = { queued: 0, flushed: 0, rows: 0, errors: 0, lastFlushError: null, lastFlushAt: 0 };

    /**
     * Distinct error messages already explained in the log. Prevents the same
     * line repeating every flush interval when a credential is wrong.
     */
    this.reportedErrors = new Set();

    /** in-memory fallbacks so the engine behaves identically without Supabase */
    this.local = {
      candles: new Map(), // seriesKey → Map(ts → candle)
      daily: new Map(), // `${userId}|${day}` → count
      profiles: new Map(), // userId → profile
      executions: [],
      chatEvents: [],
    };
  }

  /* ======================================================================== */
  /*  LIFECYCLE                                                              */
  /* ======================================================================== */

  get configured() {
    return Boolean(this.env.supabase.URL && this.env.supabase.SERVICE_ROLE_KEY);
  }

  get enabled() {
    return Boolean(this.client);
  }

  /**
   * Create the client. Never throws — a missing/failed configuration degrades
   * to local mode and is reported through `loadError`.
   */
  async init() {
    if (this.client) return this.enabled;
    if (!this.configured) {
      this.loadError = this.env.supabase.REQUIRED
        ? 'Supabase is REQUIRED but SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY are not set'
        : null;
      log.warn('Supabase not configured — persistence, remote quota and Supabase auth are OFF (local mode)');
      return false;
    }

    try {
      // Lazy import: the dependency is only paid for when it is actually used,
      // and local mode never fails on a broken install.
      const { createClient } = await import('@supabase/supabase-js');
      this.client = createClient(this.env.supabase.URL, this.env.supabase.SERVICE_ROLE_KEY, {
        auth: { persistSession: false, autoRefreshToken: false },
        db: { schema: this.schema },
        global: { headers: { 'x-application-name': 'aetheron-core' } },
      });
      log.ok(`client ready → ${this.env.supabase.URL} (schema "${this.schema}")`);
      this.#startFlushTimer();
      return true;
    } catch (e) {
      this.loadError = e.message;
      log.error('client init failed:', e.message);
      return false;
    }
  }

  /** Latest health probe result, refreshed by the background prober. */
  lastHealth = { connected: false, configured: false, mode: 'local', error: null, at: 0 };

  /** Probe connectivity; surfaced on /health. Never throws. */
  async health() {
    if (!this.client) {
      this.lastHealth = { connected: false, configured: this.configured, mode: 'local', error: this.loadError, at: Date.now() };
      return this.lastHealth;
    }

    try {
      // A deliberately trivial RPC. On a schema that predates it the error is
      // "function does not exist" — which still proves the REST endpoint and
      // the credentials work, i.e. everything a health check cares about.
      const { error } = await this.client.rpc('aetheron_ping');

      if (!error) {
        this.lastHealth = { connected: true, configured: true, mode: 'supabase', at: Date.now() };
        return this.lastHealth;
      }

      const msg = error.message ?? String(error);

      if (/not found|does not exist|schema cache|pg_proc/i.test(msg)) {
        // Credentials are fine; the schema is simply older than this code.
        this.lastHealth = { connected: true, configured: true, mode: 'supabase', note: 'aetheron_ping not installed', at: Date.now() };
        return this.lastHealth;
      }

      this.lastHealth = { connected: false, configured: true, mode: 'supabase', ...diagnoseSupabaseError(msg), at: Date.now() };
      return this.lastHealth;
    } catch (e) {
      this.lastHealth = { connected: false, configured: true, mode: 'supabase', error: e.message, at: Date.now() };
      return this.lastHealth;
    }
  }

  /** Stop the write-behind timer. */
  stop() {
    if (this.flushTimer) clearInterval(this.flushTimer);
    this.flushTimer = null;
  }

  /* ======================================================================== */
  /*  GENERIC HELPERS                                                        */
  /* ======================================================================== */

  /**
   * Run a query, normalising every failure mode into `null` + a log line.
   * PostgREST returns HTTP 200 with `{error}` on RLS/constraint violations,
   * so the error object must be inspected explicitly.
   */
  async #query(promise, what) {
    if (!this.client) return null;
    try {
      const { data, error } = await promise;
      if (error) {
        this.stats.errors += 1;
        const msg = error.message ?? String(error);
        log.error(`${what} failed: ${msg}`);

        // The candle queue retries every few seconds forever, so without this
        // one bad credential would print an identical wall of red lines all
        // day. Name the remedy the first time each distinct error appears.
        if (!this.reportedErrors.has(msg)) {
          this.reportedErrors.add(msg);
          log.error(`  → ${diagnoseSupabaseError(msg).remedy}`);
        }
        return null;
      }
      return data ?? null;
    } catch (e) {
      this.stats.errors += 1;
      log.error(`${what} threw:`, e.message);
      return null;
    }
  }

  /** Current quota day key, honouring the configured midnight offset. */
  dayKey(date = new Date()) {
    const offsetMin = this.env.quota.TZ_OFFSET_MIN || 0;
    const shifted = new Date(date.getTime() + offsetMin * 60_000);
    return shifted.toISOString().slice(0, 10); // YYYY-MM-DD
  }

  /* ======================================================================== */
  /*  BOOT VERIFICATION                                                       */
  /* ======================================================================== */

  /**
   * Prove the credentials actually work, once, at startup.
   *
   * `createClient()` succeeds with ANY non-empty string — it makes no network
   * call — so a typo'd or whitespace-padded key only reveals itself when the
   * first write fails, minutes later, buried in a repeating log. This runs a
   * real query immediately, while there is still a human watching the deploy.
   *
   * @returns {Promise<{ok:boolean, error?:string, remedy?:string, ms?:number}>}
   */
  async verify({ table = 'candles' } = {}) {
    if (!this.client) {
      return { ok: false, error: 'Supabase is not configured', remedy: 'Set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY.' };
    }

    const started = Date.now();
    try {
      const { error } = await this.client.from(table).select('*').limit(1);
      if (!error) {
        const ms = Date.now() - started;
        log.ok(`credentials verified - "${table}" is reachable (${ms}ms)`);
        return { ok: true, ms };
      }

      const msg = error.message ?? String(error);
      const { remedy } = diagnoseSupabaseError(msg);
      log.error(`credentials REJECTED by ${this.env.supabase.URL}: ${msg}`);
      log.error(`  -> ${remedy}`);
      return { ok: false, error: msg, remedy };
    } catch (e) {
      log.error(`could not reach ${this.env.supabase.URL}: ${e.message}`);
      log.error('  -> Check the project URL and whether outbound HTTPS is allowed.');
      return { ok: false, error: e.message, remedy: 'Check the project URL and outbound network access.' };
    }
  }

  /* ======================================================================== */
  /*  CANDLES - permanent storage                                           */
  /* ======================================================================== */
  /* ======================================================================== */

  /**
   * Read the most recent CLOSED candles for one series, oldest → newest.
   * PostgREST cannot express "newest N, then re-sort" in one round-trip on
   * every plan, so we over-fetch by the newest-first ordering and reverse.
   *
   * @returns {Promise<Array<object>>} chronological candles (may be empty)
   */
  async loadRecentCandles(symbolInput, intervalInput, limit = 100) {
    const symbol = normaliseSymbol(symbolInput);
    const interval = normaliseInterval(intervalInput);
    if (!symbol || interval === null) return [];

    const localMap = this.local.candles.get(seriesKey(symbol, interval));
    const local = localMap ? [...localMap.values()].sort((a, b) => a.time - b.time).slice(-limit) : [];

    if (!this.client) return local;

    const rows = await this.#query(
      this.client
        .from('candles')
        .select('symbol,timeframe,ts,open,high,low,close,tick_volume,closed')
        .eq('symbol', symbol)
        .eq('timeframe', interval)
        .eq('closed', true)
        .order('ts', { ascending: false })
        .limit(limit),
      `loadRecentCandles(${symbol}|${interval})`
    );
    if (!rows) return local;

    const candles = rows
      .map((r) => ({
        time: Number(r.ts),
        open: Number(r.open),
        high: Number(r.high),
        low: Number(r.low),
        close: Number(r.close),
        tickVolume: Number(r.tick_volume ?? 0) || 0,
      }))
      .filter((c) => Number.isFinite(c.time) && Number.isFinite(c.close))
      .sort((a, b) => a.time - b.time);

    this.#cacheLocal(symbol, interval, candles);
    return candles;
  }

  /**
   * Startup warm-up: rehydrate several series in one pass.
   * Daily candles are what the 50-day Donchian channel needs, so they are
   * requested first and with the larger limit.
   *
   * @param {Array<{symbol:string,interval:number}>} requests
   * @returns {Promise<Map<string, Array<object>>>} keyed by `XAUUSD|86400`
   */
  async loadRecentCandlesMany(requests) {
    const out = new Map();
    // Sequential on purpose: a cold start with 20 parallel upserts/selects
    // would sit right on the PostgREST connection limit.
    for (const req of requests) {
      const { symbol, interval, limit } = req;
      const candles = await this.loadRecentCandles(symbol, interval, limit);
      out.set(seriesKey(symbol, interval), candles);
    }
    return out;
  }

  /** Queue one candle for persistence (write-behind). Never throws. */
  enqueueCandle(symbolInput, intervalInput, candle, closed = true) {
    const symbol = normaliseSymbol(symbolInput);
    const interval = normaliseInterval(intervalInput);
    if (!symbol || interval === null || !candle) return false;
    if (!Number.isFinite(candle.time) || !Number.isFinite(candle.close)) return false;

    const key = `${symbol}|${interval}|${candle.time}`;
    const row = toRow(symbol, interval, candle, closed);

    // Keep the local mirror current regardless of the DB state — it is what
    // makes local mode behave like a real database across restarts of the
    // upstream feed, and it backs up reads when Supabase is down.
    this.#cacheLocal(symbol, interval, [candle]);

    if (!this.env.persistence.ENABLED || !this.client) return false;

    // Never downgrade a closed candle to a forming one.
    const existing = this.queue.get(key);
    if (existing?.closed && !closed) return false;

    this.queue.set(key, row);
    this.stats.queued = this.queue.size;
    if (!this.flushTimer && this.client) this.#startFlushTimer();
    return true;
  }

  /** Drain the queue in batches. Concurrency-safe: overlapping calls no-op. */
  async flush() {
    if (this.flushing || this.queue.size === 0) return { rows: 0, skipped: true };
    this.flushing = true;
    const batchSize = this.env.supabase.UPSERT_BATCH;
    let written = 0;

    try {
      while (this.queue.size > 0) {
        const slice = [];
        for (const [key, row] of this.queue) {
          this.queue.delete(key);
          slice.push(row);
          if (slice.length >= batchSize) break;
        }
        this.stats.queued = this.queue.size;

        const { error } = await this.client.from('candles').upsert(slice, {
          onConflict: 'symbol,timeframe,ts',
        });
        if (error) {
          this.stats.errors += 1;
          this.stats.lastFlushError = error.message ?? String(error);
          log.error(`candle upsert failed (${slice.length} rows):`, this.stats.lastFlushError);
          continue; // drop the batch rather than retry forever — the next
          //           closed candle will rewrite this window anyway
        }
        written += slice.length;
        this.stats.rows += slice.length;
      }
      this.stats.flushed += 1;
      this.stats.lastFlushAt = Date.now();
      this.stats.lastFlushError = null;
    } catch (e) {
      this.stats.errors += 1;
      this.stats.lastFlushError = e.message;
      log.error('candle flush threw:', e.message);
    } finally {
      this.flushing = false;
    }
    return { rows: written };
  }

  #startFlushTimer() {
    if (this.flushTimer) return;
    this.flushTimer = setInterval(() => {
      this.flush().catch((e) => log.error('scheduled flush failed:', e.message));
    }, this.env.persistence.FLUSH_MS);
    // Never hold the event loop open just to drain candles.
    this.flushTimer.unref?.();
  }

  /* ======================================================================== */
  /*  AUTH — user token → profile                                           */
  /* ======================================================================== */

  /**
   * Validate a Supabase access token and return `{ id, email }`.
   * Uses the user-facing `getUser(token)` call, so no service-role bypass is
   * involved in authentication.
   */
  async getUserByToken(token) {
    if (!token || !this.client) return null;
    const { data, error } = await this.client.auth.getUser(token);
    if (error || !data?.user) return null;
    return { id: data.user.id, email: data.user.email ?? null };
  }

  /**
   * Load the trading profile. Missing rows fall back to a sane FREE default so
   * a brand-new user is never left without a plan.
   */
  async getProfile(userId) {
    if (!userId) return null;

    if (!this.client) {
      return (
        this.local.profiles.get(userId) ?? {
          user_id: userId,
          email: null,
          plan: 'FREE',
          account_balance: this.env.broker.DEFAULT_BALANCE,
          risk_percentage: this.env.broker.DEFAULT_RISK_PCT,
          enabled: true,
          symbols: [],
        }
      );
    }

    const row = await this.#query(
      this.client.from('profiles').select('*').eq('user_id', userId).maybeSingle(),
      `getProfile(${userId})`
    );
    if (!row) {
      return {
        user_id: userId,
        email: null,
        plan: 'FREE',
        account_balance: this.env.broker.DEFAULT_BALANCE,
        risk_percentage: this.env.broker.DEFAULT_RISK_PCT,
        enabled: true,
        symbols: [],
      };
    }

    const profile = {
      user_id: row.user_id,
      email: row.email ?? null,
      plan: String(row.plan ?? 'FREE').toUpperCase() === 'PRO' ? 'PRO' : 'FREE',
      account_balance: Number(row.account_balance ?? this.env.broker.DEFAULT_BALANCE),
      risk_percentage: Number(row.risk_percentage ?? this.env.broker.DEFAULT_RISK_PCT),
      enabled: row.enabled !== false,
      symbols: Array.isArray(row.symbols) ? row.symbols.map(String).map((s) => s.toUpperCase()) : [],
    };
    this.local.profiles.set(userId, profile);
    return profile;
  }

  /** Upsert a profile (admin / self-service settings). */
  async upsertProfile(profile) {
    if (!profile?.user_id) return false;
    this.local.profiles.set(profile.user_id, profile);
    if (!this.client) return false;
    const { error } = await this.client
      .from('profiles')
      .upsert(
        {
          user_id: profile.user_id,
          email: profile.email ?? null,
          plan: profile.plan === 'PRO' ? 'PRO' : 'FREE',
          account_balance: Number(profile.account_balance ?? 0),
          risk_percentage: Number(profile.risk_percentage ?? 0),
          enabled: profile.enabled !== false,
          symbols: Array.isArray(profile.symbols) ? profile.symbols : [],
        },
        { onConflict: 'user_id' }
      );
    if (error) {
      log.error('upsertProfile failed:', error.message);
      return false;
    }
    return true;
  }

  /* ======================================================================== */
  /*  QUOTA — atomic daily execution counter                                 */
  /* ======================================================================== */

  /**
   * Increment today's counter and report the new value.
   *
   * Prefers the `aetheron_consume_execution` RPC, which does the read-modify-
   * write inside a single Postgres transaction (truly atomic). When the RPC is
   * unavailable — older schema, or a Supabase hiccup — we fall back to a
   * compare-and-swap UPDATE (`WHERE count = <previous>`); PostgREST returns an
   * empty array on a lost race, which we detect and retry. Local mode keeps
   * the counter in RAM.
   *
   * @returns {Promise<{used:number, day:string, source:string}>}
   */
  async consumeExecution(userId, limit, day = this.dayKey()) {
    if (!userId) return { used: 0, day, source: 'none' };
    const key = `${userId}|${day}`;

    /* ---- local mode ------------------------------------------------- */
    if (!this.client) {
      const next = (this.local.daily.get(key) ?? 0) + 1;
      this.local.daily.set(key, next);
      void limit; // the limit is enforced by the caller in local mode too
      return { used: next, day, source: 'local' };
    }

    /* ---- preferred: atomic RPC -------------------------------------- */
    const rpc = await this.#query(
      this.client.rpc('aetheron_consume_execution', {
        p_user: userId,
        p_day: day,
        p_limit: Number(limit) || 0,
      }),
      `consumeExecution(${userId})`
    );
    if (rpc && typeof rpc === 'object') {
      const used = Number(rpc.used ?? rpc.count ?? 0);
      this.local.daily.set(key, used);
      return { used, day, source: 'rpc', exceeded: Boolean(rpc.exceeded) };
    }

    /* ---- fallback: compare-and-swap ---------------------------------- */
    for (let attempt = 0; attempt < 3; attempt++) {
      const current = await this.#query(
        this.client.from('daily_executions').select('count').eq('user_id', userId).eq('day', day).maybeSingle(),
        `readDaily(${userId})`
      );
      const previous = Number(current?.count ?? 0);

      const updated = await this.#query(
        this.client
          .from('daily_executions')
          .update({ count: previous + 1, updated_at: new Date().toISOString() })
          .eq('user_id', userId)
          .eq('day', day)
          .eq('count', previous) // ← the compare half of the swap
          .select('count'),
        `casDaily(${userId})`
      );
      if (Array.isArray(updated) && updated.length === 1) {
        this.local.daily.set(key, previous + 1);
        return { used: previous + 1, day, source: 'cas' };
      }
      if (updated === null) {
        // Row does not exist yet → plain upsert, then loop to confirm.
        await this.#query(
          this.client
            .from('daily_executions')
            .upsert({ user_id: userId, day, count: 1, updated_at: new Date().toISOString() }, { onConflict: 'user_id,day' }),
          `seedDaily(${userId})`
        );
      }
    }

    log.warn(`daily counter for ${userId} could not be updated atomically — failing closed`);
    return { used: Number(limit) || 0, day, source: 'failed' };
  }

  /** Read today's counter without incrementing it (for /api/user/quota). */
  async getDailyUsage(userId, day = this.dayKey()) {
    if (!userId) return 0;
    const key = `${userId}|${day}`;
    if (!this.client) return this.local.daily.get(key) ?? 0;

    const cached = this.local.daily.get(key);
    const row = await this.#query(
      this.client.from('daily_executions').select('count').eq('user_id', userId).eq('day', day).maybeSingle(),
      `getDailyUsage(${userId})`
    );
    if (row) {
      this.local.daily.set(key, Number(row.count ?? 0));
      return Number(row.count ?? 0);
    }
    return cached ?? 0;
  }

  /** Reset a user's counter (admin action — never exposed to the app). */
  async resetDaily(userId, day = this.dayKey()) {
    this.local.daily.delete(`${userId}|${day}`);
    if (!this.client) return true;
    const { error } = await this.client
      .from('daily_executions')
      .upsert({ user_id: userId, day, count: 0, updated_at: new Date().toISOString() }, { onConflict: 'user_id,day' });
    if (error) {
      log.error('resetDaily failed:', error.message);
      return false;
    }
    return true;
  }

  /* ======================================================================== */
  /*  AUDIT — executions + chat-ops history                                  */
  /* ======================================================================== */

  /** Append an immutable record of an order the engine produced. */
  async recordExecution(row) {
    if (row?.id) {
      this.local.executions.push(row);
      if (this.local.executions.length > 500) this.local.executions.shift();
    }
    if (!this.client || !row) return false;
    // Duplicate key (23505) means this signal was already audited — that is a
    // *good* outcome (idempotent retry), so it is not treated as an error.
    const { error } = await this.client.from('executions').insert(row);
    if (error && !/duplicate key|23505/i.test(error.message ?? '')) {
      log.error('recordExecution failed:', error.message);
      return false;
    }
    return true;
  }

  /** Recent executions for a user, newest first. */
  async listExecutions(userId, limit = 25) {
    if (!this.client) return this.local.executions.filter((e) => e.user_id === userId).slice(-limit).reverse();
    const rows = await this.#query(
      this.client
        .from('executions')
        .select('*')
        .eq('user_id', userId)
        .order('created_at', { ascending: false })
        .limit(limit),
      `listExecutions(${userId})`
    );
    return rows ?? [];
  }

  /**
   * Positions that were opened but never fully closed — i.e. what the engine
   * lost when the process restarted.
   *
   * The audit log is the recovery source of truth:
   *   • status = 'EXECUTED' + no 'TRAILING_STOP' close  → still open
   *   • a 'LEG1_ATR' close on the same signal_id           → Leg 1 is done
   * So a restart resumes managing the trade instead of forgetting it and
   * opening a second, uncorrelated position on top.
   */
  async listOpenPositions(userId) {
    const rows = await this.listExecutions(userId, 200);
    const bySignal = new Map();

    for (const row of rows) {
      const signalId = row.signal_id ?? row.id;
      let rec = bySignal.get(signalId);
      if (!rec) {
        rec = { signalId, entry: null, leg1Closed: false, fullyClosed: false };
        bySignal.set(signalId, rec);
      }
      if (row.status === 'EXECUTED' || row.status === 'FAILED' || row.status === 'REJECTED') {
        // Keep the newest ENTRY row.
        if (!rec.entry || row.created_at > rec.entry.created_at) rec.entry = row;
      }
      if (row.status === 'CLOSED' && row.reason === 'LEG1_ATR') rec.leg1Closed = true;
      if (row.status === 'CLOSED' && row.reason === 'TRAILING_STOP') rec.fullyClosed = true;
    }

    return [...bySignal.values()]
      .filter((r) => r.entry && !r.fullyClosed)
      .map((r) => ({ ...r.entry, signalId: r.signalId, leg1Closed: r.leg1Closed }));
  }

  /** Append a chat-ops message so it survives a client reconnect. */
  async recordChatEvent(row) {
    if (row) {
      this.local.chatEvents.push(row);
      if (this.local.chatEvents.length > 200) this.local.chatEvents.shift();
    }
    if (!this.client || !row) return false;
    const { error } = await this.client.from('chat_events').insert(row);
    if (error && !/duplicate key|23505/i.test(error.message ?? '')) {
      log.error('recordChatEvent failed:', error.message);
      return false;
    }
    return true;
  }

  /** Chat history for a user (or the global feed when `userId` is null). */
  async listChatEvents(userId = null, limit = 50) {
    if (!this.client) {
      return this.local.chatEvents.filter((e) => !userId || e.user_id === userId).slice(-limit).reverse();
    }
    let q = this.client.from('chat_events').select('*').order('created_at', { ascending: false }).limit(limit);
    if (userId) q = q.eq('user_id', userId);
    return (await this.#query(q, 'listChatEvents')) ?? [];
  }

  /* ======================================================================== */
  /*  INTERNALS                                                             */
  /* ======================================================================== */

  /** Mirror candles into RAM so local mode has the same read behaviour. */
  #cacheLocal(symbol, interval, candles) {
    const key = seriesKey(symbol, interval);
    let map = this.local.candles.get(key);
    if (!map) {
      map = new Map();
      this.local.candles.set(key, map);
    }
    for (const c of candles) map.set(c.time, c);
    // Bound the mirror — it is a cache, not the system of record.
    if (map.size > 1500) {
      const excess = map.size - 1500;
      const oldest = [...map.keys()].sort((a, b) => a - b).slice(0, excess);
      for (const t of oldest) map.delete(t);
    }
  }

  /** Diagnostics for /health. */
  getQueueStats() {
    return { ...this.stats, pending: this.queue.size, localSeries: this.local.candles.size };
  }
}

export default SupabaseService;