/* =============================================================================
 *  AETHERON CORE  —  src/config/env.js
 *  -----------------------------------------------------------------------------
 *  Single source of truth for every runtime knob.
 *
 *  RULES OF THIS FILE
 *  ------------------
 *  1. Every value has a safe default, so the process always boots (a Render
 *     health check must never fail just because an env var was forgotten).
 *  2. Values are coerced + clamped here, never in business logic. Downstream
 *     modules can trust the types.
 *  3. Nothing secret is ever exported to the client; `publicEnv()` is the only
 *     function whose output is allowed to leave the process.
 *  4. Deprecated names (BIQUOTE_*) are still honoured so existing deployments
 *     and the e2e suite keep working.
 * ============================================================================= */

/* ------------------------------------------------------------------ helpers */

/** Coerce to a finite number, falling back to `fallback`. */
const num = (raw, fallback) => {
  if (raw === undefined || raw === null || raw === '') return fallback;
  const n = Number(raw);
  return Number.isFinite(n) ? n : fallback;
};

/** Integer within [min, max]. */
const int = (raw, fallback, min = Number.MIN_SAFE_INTEGER, max = Number.MAX_SAFE_INTEGER) => {
  const n = Math.round(num(raw, fallback));
  return Math.min(max, Math.max(min, n));
};

/** Truthy / falsy / undefined parser — never treat the string "false" as true. */
const bool = (raw, fallback) => {
  if (raw === undefined || raw === null || raw === '') return fallback;
  return /^(1|true|yes|on)$/i.test(String(raw));
};

/** Comma separated list → trimmed, non-empty array. */
const list = (raw, fallback = []) => {
  if (!raw) return fallback;
  return String(raw)
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean);
};

/** Uppercase without surrounding whitespace. */
const upper = (raw, fallback = '') => (raw ? String(raw).trim().toUpperCase() : fallback);

/** Strip trailing slashes — Supabase URLs are used to build REST paths. */
const noSlash = (raw) => (raw ? String(raw).trim().replace(/\/+$/, '') : '');

/**
 * Normalise a credential.
 *
 * A key pasted into Render's dashboard, a `.env` file or a CI variable very
 * easily carries a trailing newline or a stray space. PostgREST answers
 * "Invalid API key" for that — the string matches no key in the project —
 * which looks exactly like a genuinely wrong key and sends people hunting for
 * a credential problem that is really a whitespace problem. Trimming here
 * removes that whole class of confusing failure.
 */
const secret = (raw) => (raw ? String(raw).trim() : '');

/* --------------------------------------------------------------- upstream --- */

const BIQUOTE_WS_FALLBACK = 'wss://biquote.io/hubs/tick';
const BIQUOTE_API_FALLBACK = 'https://biquote.io';

const upstream = {
  /** WebSocket endpoint of the MT5 tick feed (SignalR dialect). */
  WS: noSlash(process.env.UPSTREAM_WS ?? process.env.BIQUOTE_WS ?? BIQUOTE_WS_FALLBACK),
  /** REST base used for OHLC backfill. */
  API: noSlash(process.env.UPSTREAM_API ?? process.env.BIQUOTE_API ?? BIQUOTE_API_FALLBACK),
  /** Exponential backoff: BASE * 2^(n-1), capped, ±50% jitter. */
  BACKOFF_BASE_MS: int(process.env.UPSTREAM_BACKOFF_BASE_MS, 1_000, 250, 60_000),
  BACKOFF_MAX_MS: int(process.env.UPSTREAM_BACKOFF_MAX_MS, 30_000, 1_000, 600_000),
  /** Application keepalive sent upstream (SignalR ping, type 6). */
  PING_EVERY_MS: int(process.env.UPSTREAM_PING_EVERY_MS, 20_000, 1_000, 120_000),
  /**
   * Silence before we declare the socket a zombie. Deliberately generous:
   * forex/metals are closed at weekends and a tight watchdog would reconnect
   * in a loop for the whole Saturday.
   */
  WATCHDOG_MS: int(process.env.UPSTREAM_WATCHDOG_MS, 300_000, 10_000, 3_600_000),
  REST_TIMEOUT_MS: int(process.env.UPSTREAM_REST_TIMEOUT_MS, 15_000, 1_000, 120_000),
  /** Symbols to subscribe to. Overridden by the symbol registry when empty. */
  SYMBOLS: list(process.env.UPSTREAM_SYMBOLS, []).map((s) => s.toUpperCase()),
};

/* --------------------------------------------------------------- database -- */

const supabase = {
  /** Project URL, e.g. https://xxxx.supabase.co */
  URL: noSlash(process.env.SUPABASE_URL),
  /**
   * Service-role key. Server-side only: it bypasses RLS, so it must never
   * reach a browser. Used for candles, executions and the quota counter.
   */
  SERVICE_ROLE_KEY: secret(process.env.SUPABASE_SERVICE_ROLE_KEY ?? process.env.SUPABASE_KEY),
  /**
   * When true a missing Supabase configuration aborts the boot instead of
   * degrading to in-memory only. Keep it false so `/health` stays green.
   */
  REQUIRED: bool(process.env.SUPABASE_REQUIRED, false),
  /** Schema holding the engine tables (change to isolate environments). */
  SCHEMA: process.env.SUPABASE_SCHEMA ?? 'public',
  /** How many days are rehydrated on startup — 50-day Donchian + warm ATR. */
  WARMUP_DAILY_CANDLES: int(process.env.BOOTSTRAP_DAILY_CANDLES, 100, 60, 1_000),
  /** Rows per upsert round-trip (PostgREST URL length / statement size). */
  UPSERT_BATCH: int(process.env.SUPABASE_UPSERT_BATCH, 250, 1, 1_000),
  /** Generic request timeout. */
  TIMEOUT_MS: int(process.env.SUPABASE_TIMEOUT_MS, 15_000, 1_000, 120_000),
};

/* ------------------------------------------------------------------- auth -- */

const auth = {
  /**
   * When true, `/api/user/*` and `/api/stream/*` reject anonymous callers.
   * The public `/stream` chart feed is never affected.
   */
  REQUIRED: bool(process.env.AUTH_REQUIRED, true),
  /** Verified token → user map TTL. Keeps the hot loop off Supabase. */
  CACHE_TTL_MS: int(process.env.AUTH_CACHE_TTL_MS, 5 * 60_000, 1_000, 3_600_000),
  CACHE_MAX: int(process.env.AUTH_CACHE_MAX, 5_000, 100, 100_000),
  /**
   * Development escape hatch: a static token that maps to a local synthetic
   * user. Hard-disabled in production so a leaked default can never pass.
   */
  DEV_TOKEN: process.env.NODE_ENV === 'production' ? '' : secret(process.env.DEV_TOKEN),
};

/* ------------------------------------------------------------------ quota -- */

const quota = {
  /** Automatic executions a FREE user gets per day (the freemium hook). */
  FREE_DAILY_EXECUTIONS: int(process.env.FREE_DAILY_EXECUTIONS, 3, 0, 1_000),
  /** PRO is unlimited — expressed as 0 so "no row update" is meaningful. */
  PRO_DAILY_EXECUTIONS: int(process.env.PRO_DAILY_EXECUTIONS, 0, 0, 1_000),
  /** Midnight offset used to decide when the daily counter rolls over. */
  TZ_OFFSET_MIN: int(process.env.QUOTA_TZ_OFFSET_MIN, 0, -840, 840),
  /** Profile (plan, balance, risk) cache TTL. */
  PROFILE_TTL_MS: int(process.env.PROFILE_CACHE_TTL_MS, 60_000, 1_000, 3_600_000),
  /**
   * Sliding-window HTTP limiter protecting the API surface from a single IP.
   * (Per-token limiters cover the authenticated endpoints.)
   */
  RATE_LIMIT_WINDOW_MS: int(process.env.RATE_LIMIT_WINDOW_MS, 60_000, 1_000, 600_000),
  RATE_LIMIT_MAX: int(process.env.RATE_LIMIT_MAX, 240, 1, 100_000),
};

/* ----------------------------------------------------------------- broker -- */

const broker = {
  /**
   * `paper`  – simulated fills in-process (default, safe for production demos)
   * `mt5`    – POSTs to your MT5 bridge (an EA / VPS service). Server-side
   *            only: the module is never mounted on an HTTP route.
   */
  MODE: upper(process.env.BROKER_MODE, 'paper'),
  BASE_URL: noSlash(process.env.BROKER_BASE_URL),
  /** Shared secret for the MT5 bridge, sent as an HMAC signature. */
  /** Paper-mode simulated fill latency (keeps the async path honest). */
  PAPER_LATENCY_MS: int(process.env.BROKER_PAPER_LATENCY_MS, 40, 0, 5_000),
  /** Shared secret for the MT5 bridge — trimmed, like every other credential. */
  TOKEN: secret(process.env.BROKER_TOKEN),
  /** Fallback equity when a profile has none (USD). */
  DEFAULT_BALANCE: num(process.env.ACCOUNT_BALANCE, 10_000),
  /** Fallback risk per trade, as a fraction of balance (0.01 = 1%). */
  DEFAULT_RISK_PCT: num(process.env.RISK_PERCENT, 0.01),
  /** Absolute lot ceiling applied after volatility sizing. */
  MAX_LOT: num(process.env.MAX_LOT, 2),
};

/* ---------------------------------------------------------------- strategy -- */

const strategy = {
  /** Symbols the engine trades. Empty = every symbol in the registry. */
  SYMBOLS: list(process.env.STRATEGY_SYMBOLS, []).map((s) => s.toUpperCase()),
  /** Entry breakout lookback, in daily candles. */
  DONCHIAN_ENTRY: int(process.env.DONCHIAN_ENTRY, 50, 5, 500),
  /** Trailing-stop lookback, in daily candles. */
  DONCHIAN_EXIT: int(process.env.DONCHIAN_EXIT, 20, 2, 500),
  /** ATR window used for sizing and for the Leg-1 take profit. */
  ATR_PERIOD: int(process.env.ATR_PERIOD, 20, 2, 200),
  /** Timeframe the ATR and the exit loop run on. */
  ATR_TIMEFRAME: int(process.env.ATR_TIMEFRAME, 3600, 60, 86_400),
  /** Timeframe whose close arms a new entry evaluation. */
  ENTRY_TIMEFRAME: int(process.env.ENTRY_TIMEFRAME, 86_400, 60, 86_400),
  /** Leg 1 (fee coverage) share of the position. */
  LEG1_PCT: num(process.env.LEG1_PCT, 0.25),
  /** Leg 2 (runner) share of the position. */
  LEG2_PCT: num(process.env.LEG2_PCT, 0.75),
  /** Leg 1 closes once profit reaches ATR * this. */
  LEG1_ATR_MULT: num(process.env.LEG1_ATR_MULT, 1),
  /** Minimum spacing between two entries on the same symbol. */
  COOLDOWN_MS: int(process.env.STRATEGY_COOLDOWN_MS, 60 * 60_000, 0, 86_400_000),
  /** Exit checks are throttled to this cadence per symbol (tick hot path). */
  EXIT_THROTTLE_MS: int(process.env.STRATEGY_EXIT_THROTTLE_MS, 250, 10, 10_000),
  /** Re-arm the trailing stop only after it moved by this much (API hygiene). */
  TRAIL_MIN_STEP_POINTS: num(process.env.TRAIL_MIN_STEP_POINTS, 5),
  /** Set false to compute signals but never call the broker. */
  EXECUTION_ENABLED: bool(process.env.STRATEGY_EXECUTION_ENABLED, true),
};

/* --------------------------------------------------------------- streaming -- */

const stream = {
  /** Coalesced flush cadence for every downstream SSE channel. */
  INTERVAL_MS: int(process.env.STREAM_INTERVAL_MS, 400, 50, 10_000),
  /** Comment frame cadence — keeps proxies and mobile radios from dropping us. */
  HEARTBEAT_MS: int(process.env.SSE_HEARTBEAT_MS, 15_000, 1_000, 300_000),
  /** Status/HEALTH event cadence on the chat stream. */
  STATUS_INTERVAL_MS: int(process.env.STATUS_INTERVAL_MS, 5_000, 500, 300_000),
  /** Back-pressure guard: drop frames for clients this far behind. */
  MAX_BUFFER_BYTES: int(process.env.SSE_MAX_BUFFER_BYTES, 1_000_000, 64_000, 50_000_000),
  /** Reconnect delay advertised to EventSource clients. */
  RETRY_MS: int(process.env.SSE_RETRY_MS, 3_000, 300, 60_000),
  /** Chat history replayed to a freshly connected client. */
  CHAT_BACKLOG: int(process.env.SSE_CHAT_BACKLOG, 50, 0, 500),
};

/* --------------------------------------------------------------- persistence */

const persistence = {
  /** Master switch for candle persistence. */
  ENABLED: bool(process.env.PERSIST_ENABLED, true),
  /** Cadence of the write-behind queue. */
  FLUSH_MS: int(process.env.PERSIST_FLUSH_MS, 2_000, 200, 60_000),
  /**
   * The forming candle is upserted this often so a crash loses at most a few
   * minutes of the current bar (closed candles are written immediately).
   */
  FORMING_FLUSH_MS: int(process.env.PERSIST_FORMING_FLUSH_MS, 15_000, 1_000, 600_000),
};

/* ------------------------------------------------------------------- market - */

const market = {
  /** Closed candles retained per series in RAM (the ring size). */
  MAX_CANDLES: int(process.env.MAX_CANDLES, 100, 50, 5_000),
  /** Static dashboard + legacy public stream. */
  SERVE_PUBLIC: bool(process.env.SERVE_PUBLIC, true),
  TELEGRAM_ENABLED: bool(process.env.TELEGRAM_ENABLED, false),
};

/* ------------------------------------------------------------------- server - */

const server = {
  PORT: int(process.env.PORT, 3000, 1, 65_535),
  HOST: process.env.HOST ?? '0.0.0.0',
  NODE_ENV: process.env.NODE_ENV ?? 'development',
  IS_PRODUCTION: process.env.NODE_ENV === 'production',
  /** Express JSON body cap — trade payloads are rejected anyway; this is belt. */
  BODY_LIMIT: process.env.BODY_LIMIT ?? '16kb',
  /** Extra seconds before a forced exit during shutdown. */
  SHUTDOWN_GRACE_MS: int(process.env.SHUTDOWN_GRACE_MS, 5_000, 500, 60_000),
};

/* -------------------------------------------------------------------- export */

export const ENV = Object.freeze({
  server,
  upstream,
  supabase,
  auth,
  quota,
  broker,
  strategy,
  stream,
  persistence,
  market,
});

/**
 * The only values that may be serialised to a client. Secrets live in the rest
 * of ENV and stay in this process.
 */
export function publicEnv() {
  return {
    engine: 'Aetheron Core',
    version: '2.0.0',
    nodeEnv: server.NODE_ENV,
    persistence: persistence.ENABLED && Boolean(supabase.URL && supabase.SERVICE_ROLE_KEY),
    brokerMode: broker.MODE,
    executionEnabled: strategy.EXECUTION_ENABLED,
    quota: { freeDailyExecutions: quota.FREE_DAILY_EXECUTIONS, proUnlimited: quota.PRO_DAILY_EXECUTIONS === 0 },
    strategy: {
      name: 'Aetheron Universal',
      donchianEntry: strategy.DONCHIAN_ENTRY,
      donchianExit: strategy.DONCHIAN_EXIT,
      atrPeriod: strategy.ATR_PERIOD,
      leg1Pct: strategy.LEG1_PCT,
      leg2Pct: strategy.LEG2_PCT,
      leg1AtrMult: strategy.LEG1_ATR_MULT,
    },
  };
}

/**
 * Startup report. Two different problems produce the SAME Supabase error
 * ("Invalid API key"), so they are called out separately here:
 *
 *   • a malformed credential (whitespace, truncated paste, wrong project)
 *   • a valid credential but `schema.sql` was never run  → "relation does not
 *     exist" on every query
 *
 * The first is fixed in the Render dashboard, the second by running the schema.
 * Saying which one applies saves an hour of guessing.
 */
export function auditEnv() {
  const problems = [];
  const warnings = [];

  if (!supabase.URL || !supabase.SERVICE_ROLE_KEY) {
    const msg =
      'SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY missing — running WITHOUT persistence, ' +
      'quota persistence or Supabase auth (in-memory fallbacks active).';
    if (supabase.REQUIRED) problems.push(msg);
    else warnings.push(msg);
  } else {
    // Cheap structural checks. A service-role JWT has three dot-separated
    // parts; `sb_secret_…` style keys do not, so neither shape is mandated —
    // but whitespace or an obviously truncated value is worth naming loudly.
    const key = supabase.SERVICE_ROLE_KEY;
    if (/\s/.test(key)) {
      problems.push('SUPABASE_SERVICE_ROLE_KEY still contains whitespace — re-enter it in Render without spaces.');
    }
    if (key.includes('...') || key.endsWith('…')) {
      problems.push('SUPABASE_SERVICE_ROLE_KEY looks truncated (contains an ellipsis) — paste the full key.');
    }
    if (key.length < 40) {
      problems.push(
        `SUPABASE_SERVICE_ROLE_KEY is only ${key.length} characters — a real key is much longer. ` +
          'Check that you copied the service_role key, not the URL or an id.'
      );
    }
    if (!/^https?:\/\//i.test(supabase.URL) || !/\.supabase\.(co|in)$/i.test(supabase.URL)) {
      warnings.push(`SUPABASE_URL "${supabase.URL}" does not look like a Supabase project URL.`);
    }
  }

  if (!auth.DEV_TOKEN && !supabase.URL) {
    warnings.push(
      'No Supabase auth and no DEV_TOKEN set — the API surface is effectively closed. ' +
        'Set DEV_TOKEN (development only) or SUPABASE_URL to open it.'
    );
  }
  if (broker.MODE === 'mt5' && (!broker.BASE_URL || !broker.TOKEN)) {
    problems.push('BROKER_MODE=mt5 requires BROKER_BASE_URL and BROKER_TOKEN.');
  }
  if (strategy.LEG1_PCT + strategy.LEG2_PCT > 1.0001) {
    problems.push('LEG1_PCT + LEG2_PCT exceeds 100% — legs would over-allocate.');
  }
  if (market.MAX_CANDLES < strategy.DONCHIAN_ENTRY + 1) {
    problems.push(
      `MAX_CANDLES (${market.MAX_CANDLES}) cannot hold a ${strategy.DONCHIAN_ENTRY}-day Donchian channel.`
    );
  }

  return { problems, warnings, ready: problems.length === 0 };
}

export default ENV;