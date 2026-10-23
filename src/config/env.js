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

/* ------------------------------------------------------ admin console --- */

/*
 *  The admin console manages APK users, membership requests and the trade
 *  ledger. It is deliberately independent of Supabase auth: the browser logs
 *  in with a username/password held in the server environment, and the server
 *  issues a short-lived signed cookie. No admin password is ever stored in the
 *  database or shipped to a client.
 */
const admin = {
  /** Master switch. Without a password the console answers "not configured". */
  ENABLED: bool(process.env.ADMIN_ENABLED, true),
  USERNAME: (process.env.ADMIN_USERNAME || 'admin').trim() || 'admin',
  /** Plain password from the environment. Prefer ADMIN_PASSWORD_HASH in prod. */
  PASSWORD: secret(process.env.ADMIN_PASSWORD),
  /**
   * Optional scrypt hash: `scrypt$<salt-b64url>$<hash-b64url>`.
   * Generate one with: npm run hash:admin -- "your password"
   */
  PASSWORD_HASH: secret(process.env.ADMIN_PASSWORD_HASH),
  /** HMAC key for the session cookie. Empty = derive from the credential. */
  SESSION_SECRET: secret(process.env.ADMIN_SESSION_SECRET),
  SESSION_TTL_MS: int(process.env.ADMIN_SESSION_TTL_MS, 12 * 3_600_000, 5 * 60_000, 7 * 86_400_000),
  LOGIN_WINDOW_MS: int(process.env.ADMIN_LOGIN_WINDOW_MS, 15 * 60_000, 60_000, 3_600_000),
  LOGIN_MAX: int(process.env.ADMIN_LOGIN_MAX, 8, 3, 100),
};

/* ------------------------------------------------- APK membership plans -- */

/*
 *  The APK has no email/password signup. Each installation generates a device
 *  id, registers it, and the operator upgrades or bans that device from the
 *  admin console. Plans are labels plus a retention window for the trade
 *  history; nothing about them touches the trading engine's execution logic.
 */
const membership = {
  ENABLED: bool(process.env.MEMBERSHIP_ENABLED, true),
  /** How many days of closed trades each plan can read. */
  FREE_HISTORY_DAYS: int(process.env.FREE_HISTORY_DAYS, 7, 1, 36_500),
  MONTHLY_HISTORY_DAYS: int(process.env.MONTHLY_HISTORY_DAYS, 90, 1, 36_500),
  YEARLY_HISTORY_DAYS: int(process.env.YEARLY_HISTORY_DAYS, 3_650, 1, 36_500),
  /** Send a Telegram DM on upgrade/ban when the device is paired. */
  TELEGRAM_UPGRADE: bool(process.env.MEMBERSHIP_TELEGRAM, true),
  /** Minimum gap between two upgrade requests from the same device. */
  REQUEST_COOLDOWN_MS: int(process.env.MEMBERSHIP_REQUEST_COOLDOWN_MS, 5 * 60_000, 10_000, 86_400_000),
  /** Hard cap on the free-text note the user can send with a request. */
  MAX_MESSAGE_CHARS: int(process.env.MEMBERSHIP_MAX_MESSAGE_CHARS, 500, 40, 4_000),
  /** Public support handles shown on the APK's upgrade screen. */
  SUPPORT_TELEGRAM: secret(process.env.SUPPORT_TELEGRAM),
  SUPPORT_WHATSAPP: secret(process.env.SUPPORT_WHATSAPP),
  SUPPORT_EMAIL: secret(process.env.SUPPORT_EMAIL),
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
  /**
   * Closed candles retained per series in RAM (the ring size).
   *
   * Every series is shipped to a new SSE client as part of the `snapshot`, so
   * this is a bandwidth knob as much as a memory one: 300 bars is three times
   * the chart history of the old 100-bar ring while each snapshot stays in the
   * low hundreds of kilobytes. Override with MAX_CANDLES when needed.
   */
  MAX_CANDLES: int(process.env.MAX_CANDLES, 300, 50, 5_000),
  /** Static dashboard + legacy public stream. */
  SERVE_PUBLIC: bool(process.env.SERVE_PUBLIC, true),
  TELEGRAM_ENABLED: bool(process.env.TELEGRAM_ENABLED, false),
};

/* ---------------------------------------------------------------- ai analyst - */
/*
 *  The AI analyst is a SECOND, independent reader of the same candles the
 *  strategy trades: it sends a multi-timeframe snapshot (D1/H4/H1/M15/M5) to a
 *  chat model and publishes the strict-JSON verdict it gets back as a chat
 *  message and, optionally, a Telegram alert.
 *
 *  Every provider below speaks the OpenAI chat-completions dialect, so switching
 *  is a URL + key + model swap and nothing else in the analyst changes. Groq is
 *  the default — fast, and its free tier is a daily TOKEN budget instead of a
 *  request count — and OpenRouter stays one env var away
 *  (AI_SIGNAL_PROVIDER=openrouter).
 *
 *  It never places an order. The model's `action` is text on a screen; the
 *  strategy engine remains the only thing that can move a position. That is why
 *  every value here is a display/behaviour knob, not an execution one.
 */

const AI_PROVIDERS = Object.freeze({
  groq: {
    label: 'Groq',
    endpoint: 'https://api.groq.com/openai/v1/chat/completions',
    // The gpt-oss pair is what responds on current free-tier accounts; the
    // llama ids stay at the tail as a last resort and are parked for 6h if the
    // account answers "model does not exist".
    defaultModel: 'openai/gpt-oss-120b',
    /** Free models from the same provider, tried in order. They live on
     *  different token buckets, so a model that is momentarily throttled does
     *  not take the whole analyst down with it. */
    fallbackModels: [
      'openai/gpt-oss-20b',
      'llama-3.3-70b-versatile',
      'llama-3.1-8b-instant',
    ],
  },
  openrouter: {
    label: 'OpenRouter',
    endpoint: 'https://openrouter.ai/api/v1/chat/completions',
    defaultModel: 'openrouter/free',
    fallbackModels: ['openrouter/free'],
  },
});

/**
 * Collect one or more credentials from:
 *   GROQ_API_KEY            first key
 *   GROQ_API_KEY_2 … _8     extra keys (another account each)
 *   GROQ_API_KEYS=a,b,c     comma-separated alternative
 *
 * Order is stable and duplicates are removed. Values never leave the process:
 * status output reports slot counts only.
 */
const keyList = (name) => {
  const numbered = [];
  for (let i = 2; i <= 8; i++) numbered.push(process.env[`${name}_${i}`]);
  return [...new Set([process.env[name], ...numbered, ...list(process.env[`${name}S`])]
    .map((k) => secret(k))
    .filter(Boolean))];
};

const aiKeys = {
  groq: keyList('GROQ_API_KEY'),
  openrouter: keyList('OPENROUTER_API_KEY'),
};

/*
 * `auto` (the default) prefers Groq and falls back to OpenRouter when only that
 * key is present. An explicit name wins even without a key, so a wrong value is
 * visible in the boot log instead of silently switching provider.
 */
const requestedProvider = (process.env.AI_SIGNAL_PROVIDER || 'auto').trim().toLowerCase();
const providerName =
  requestedProvider === 'auto'
    ? aiKeys.groq.length
      ? 'groq'
      : aiKeys.openrouter.length
        ? 'openrouter'
        : 'groq'
    : AI_PROVIDERS[requestedProvider]
      ? requestedProvider
      : 'groq';
const provider = AI_PROVIDERS[providerName];

/*
 * A leftover OpenRouter model id from the pre-Groq setup (for example
 * AI_SIGNAL_MODEL=openrouter/free) would 404 on Groq and burn one model call
 * per process boot. If the configured id plainly belongs to another provider,
 * ignore it and use the active provider's default instead.
 */
const configuredModel = process.env.AI_SIGNAL_MODEL?.trim() || '';
const staleProviderModel = providerName === 'groq' && /^openrouter\//i.test(configuredModel);

const ai = {
  /** Master switch — the analyst idles unless enabled AND a key is present. */
  ENABLED: bool(process.env.AI_SIGNAL_ENABLED, true),
  /** Provider actually in use: 'groq' | 'openrouter'. */
  PROVIDER: providerName,
  PROVIDER_LABEL: provider.label,
  /** OpenAI-compatible chat-completions endpoint. */
  ENDPOINT: process.env.AI_SIGNAL_ENDPOINT?.trim() || provider.endpoint,
  /** The first key the endpoint expects. Absent ⇒ `configured:false`. */
  API_KEY: aiKeys[providerName][0] || '',
  /** Every key for the active provider, in order. The analyst rotates through
   *  them when one answers 429/402 (or is rejected outright). */
  API_KEYS: aiKeys[providerName],
  /** Raw key lists, kept so the boot log can say which credential is missing
   *  and so a provider switch can be made without a restart. */
  GROQ_API_KEY: aiKeys.groq[0] || '',
  GROQ_API_KEYS: aiKeys.groq,
  OPENROUTER_API_KEY: aiKeys.openrouter[0] || '',
  OPENROUTER_API_KEYS: aiKeys.openrouter,
  /** Model id — defaults to the provider's model when AI_SIGNAL_MODEL is unset
   *  or still carries the other provider's id. */
  MODEL: configuredModel && !staleProviderModel ? configuredModel : provider.defaultModel,
  /** Instrument the analyst reads. Kept for single-symbol deployments and for
   *  the per-symbol service inside the multi-symbol manager. */
  SYMBOL: upper(process.env.AI_SIGNAL_SYMBOL, 'XAUUSD'),
  /** Instruments the analyst covers. One model call per symbol per cycle, so
   *  this is the main driver of free-tier quota consumption. */
  SYMBOLS: list(process.env.AI_SIGNAL_SYMBOLS, ['XAUUSD', 'EURUSD', 'USDJPY', 'GBPUSD']).map((s) =>
    s.toUpperCase()
  ),
  /** Gap between two symbol calls inside one cycle. Four bursts of model calls
   *  look exactly like abuse to a per-minute quota; a stagger looks like normal
   *  use and is the cheapest way to stay under it. 0 disables the pause. */
  STAGGER_MS: int(process.env.AI_SIGNAL_STAGGER_MS, 12_000, 0, 120_000),
  /** After a 429/402 the cycle interval is multiplied (1 → 2 → 4 …) up to this
   *  cap. This is the "quota ran out, slow down" behaviour, not a hard stop. */
  RATE_LIMIT_FACTOR_MAX: num(process.env.AI_SIGNAL_RATE_LIMIT_FACTOR_MAX, 4),
  /** Hard pause after a quota rejection before any symbol is retried. */
  RATE_LIMIT_COOLDOWN_MS: int(
    process.env.AI_SIGNAL_RATE_LIMIT_COOLDOWN_MS,
    10 * 60_000,
    60_000,
    6 * 3_600_000
  ),
  /** While a trade is open the model is called once every N cycles; the other
   *  cycles publish a free tick-level status. 1 = call on every cycle. This is
   *  what keeps a multi-symbol book inside a small daily quota. */
  MANAGEMENT_EVERY: int(process.env.AI_SIGNAL_MANAGEMENT_EVERY, 2, 1, 10),
  /** Cadence of the automatic analysis. */
  INTERVAL_MS: int(process.env.AI_SIGNAL_INTERVAL_MS, 30 * 60_000, 60_000, 86_400_000),
  /** Seconds a client must wait between manual triggers. */
  MANUAL_COOLDOWN_S: int(process.env.AI_SIGNAL_MANUAL_COOLDOWN_S, 60, 5, 3_600),
  /** Warm-up after boot before the first automatic analysis (let candles land). */
  BOOT_DELAY_MS: int(process.env.AI_SIGNAL_BOOT_DELAY_MS, 120_000, 5_000, 3_600_000),
  /** Hard timeout for the upstream model call. A free-tier model can genuinely
   *  take a minute to answer, so the default is deliberately above the 60s a
   *  thoughtless value would use — a timeout here costs a whole cycle. */
  TIMEOUT_MS: int(process.env.AI_SIGNAL_TIMEOUT_MS, 90_000, 5_000, 300_000),
  /** Max tokens the model may spend on its JSON signal. Groq counts input +
   *  max_tokens against the model's per-minute cap, so a big value can push an
   *  otherwise valid request over the free-tier limit (413). 1500 is ample for
   *  the JSON answer and leaves headroom under an 8k TPM bucket. */
  MAX_TOKENS: int(process.env.AI_SIGNAL_MAX_TOKENS, 1500, 256, 8_000),
  /** Hard ceiling for the USER turn, in characters. The builder trims the
   *  oldest bars from the longest frame until the prompt fits. Keeps a news
   *  spike or a widened bar window from turning into a 413. */
  MAX_PROMPT_CHARS: int(process.env.AI_SIGNAL_MAX_PROMPT_CHARS, 12_000, 4_000, 60_000),
  /** Minimum reward:risk a BUY/SELL must satisfy to be published. */
  MIN_RR: num(process.env.AI_SIGNAL_MIN_RR, 2),
  /** How far (fraction of price) an entry may sit from the live quote. */
  MAX_ENTRY_DRIFT: num(process.env.AI_SIGNAL_MAX_ENTRY_DRIFT, 0.02),
  /** Bars sent per timeframe — enough to trend, sized so the whole request
   *  stays under the 8k-token free-tier per-minute bucket. */
  BARS: {
    D1: int(process.env.AI_SIGNAL_BARS_D1, 40, 20, 100),
    H4: int(process.env.AI_SIGNAL_BARS_H4, 40, 20, 100),
    H1: int(process.env.AI_SIGNAL_BARS_H1, 50, 20, 100),
    M15: int(process.env.AI_SIGNAL_BARS_M15, 40, 20, 100),
    M5: int(process.env.AI_SIGNAL_BARS_M5, 40, 20, 100),
  },
  /** Fan a published signal out to Telegram when a bot token is configured. */
  TELEGRAM: bool(process.env.AI_SIGNAL_TELEGRAM, true),
  /** Optional fixed chat/channel id — delivery without device pairing. */
  TELEGRAM_CHAT_ID: secret(process.env.AI_SIGNAL_CHAT_ID ?? process.env.TELEGRAM_CHAT_ID),
  /** Model calls attempted within a single cycle before it is declared failed.
   *  Two (ask, then correct) is the quota-sane default: every extra attempt is
   *  a real request against a free daily cap. The effective ceiling is
   *  `max(MAX_ATTEMPTS, model chain length)`, capped by MAX_CALLS_PER_CYCLE, so
   *  every configured model gets at least one shot. */
  MAX_ATTEMPTS: int(process.env.AI_SIGNAL_MAX_ATTEMPTS, 2, 1, 6),
  /** Hard ceiling on model calls per symbol per cycle. Key rotation (same model,
   *  next credential) does not consume this budget — only model attempts do. */
  MAX_CALLS_PER_CYCLE: int(process.env.AI_SIGNAL_MAX_CALLS_PER_CYCLE, 6, 1, 20),
  /** Extra model ids tried, in order, once the primary model has failed.
   *  Defaults to the provider's free model line-up: different model families
   *  are metered on separate token buckets, so one throttled model does not
   *  take the analyst down. */
  FALLBACK_MODELS: list(process.env.AI_SIGNAL_FALLBACK_MODELS, provider.fallbackModels),
  /** Last-resort model for the active provider, appended after FALLBACK_MODELS.
   *  Defaults to the provider's slowest-but-cheapest free model; a deliberate
   *  `AI_SIGNAL_FALLBACK_MODEL` adds one extra id beyond the configured list. */
  FALLBACK_MODEL:
    process.env.AI_SIGNAL_FALLBACK_MODEL?.trim() || provider.fallbackModels.slice(-1)[0] || '',
  /** When a whole cycle fails, retry it sooner than the next interval — a
   *  dropped cycle otherwise means a silent 30-minute hole in the feed. */
  CATCHUP_ENABLED: bool(process.env.AI_SIGNAL_CATCHUP, true),
  CATCHUP_DELAY_MS: int(process.env.AI_SIGNAL_CATCHUP_DELAY_MS, 180_000, 30_000, 900_000),
  CATCHUP_MAX: int(process.env.AI_SIGNAL_CATCHUP_MAX, 3, 0, 10),
  /** Exit behaviour while a trade is open: TP1 moves the stop to breakeven,
   *  TP2/SL/reversal close the tracked trade. */
  TRACK_ENABLED: bool(process.env.AI_SIGNAL_TRACK, true),
  /** Send one Telegram message every cycle: a new signal, a management update
   *  for the open trade, or the reason for standing aside. Without this a
   *  healthy analyst is indistinguishable from a dead one. */
  STATUS_EVERY_CYCLE: bool(process.env.AI_SIGNAL_STATUS_EVERY_CYCLE, true),
};

/* --------------------------------------------------------- economic calendar */

/*
 *  Upcoming high-impact releases, injected into the analyst's prompt. Kept as
 *  its own section because it is a data source, not an analyst behaviour — the
 *  same calendar could later inform the strategy engine too.
 */
const news = {
  ENABLED: bool(process.env.AI_NEWS_ENABLED, true),
  URL: process.env.AI_NEWS_URL?.trim() || 'https://nfs.faireconomy.media/ff_calendar_thisweek.xml',
  /** How long a downloaded week of events is trusted before a refresh. */
  TTL_MS: int(process.env.AI_NEWS_TTL_MS, 3 * 3_600_000, 60_000, 24 * 3_600_000),
  TIMEOUT_MS: int(process.env.AI_NEWS_TIMEOUT_MS, 15_000, 1_000, 120_000),
  /** How far ahead the prompt looks. One trading day is what the model can act on. */
  HORIZON_H: int(process.env.AI_NEWS_HORIZON_H, 24, 1, 168),
  /** HIGH keeps the prompt focused; MEDIUM widens it to second-tier prints. */
  MIN_IMPACT: upper(process.env.AI_NEWS_MIN_IMPACT, 'HIGH'),
  /** Currencies worth telling the analyst about ("ALL" also keeps global items). */
  COUNTRIES: list(process.env.AI_NEWS_COUNTRIES, ['USD', 'ALL']).map((s) => s.toUpperCase()),
  /** Minutes east of UTC the feed's times are written in (0 = the feed is UTC). */
  TZ_OFFSET_MIN: int(process.env.AI_NEWS_TZ_OFFSET_MIN, 0, -840, 840),
  MAX_EVENTS: int(process.env.AI_NEWS_MAX_EVENTS, 8, 1, 30),
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
  admin,
  membership,
  quota,
  broker,
  strategy,
  stream,
  persistence,
  market,
  ai,
  news,
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
    membership: {
      enabled: membership.ENABLED,
      plans: ['FREE', 'MONTHLY', 'YEARLY'],
      historyDays: {
        FREE: membership.FREE_HISTORY_DAYS,
        MONTHLY: membership.MONTHLY_HISTORY_DAYS,
        YEARLY: membership.YEARLY_HISTORY_DAYS,
      },
      support: {
        telegram: membership.SUPPORT_TELEGRAM || null,
        whatsapp: membership.SUPPORT_WHATSAPP || null,
        email: membership.SUPPORT_EMAIL || null,
      },
    },
    strategy: {
      name: 'Aetheron Universal',
      donchianEntry: strategy.DONCHIAN_ENTRY,
      donchianExit: strategy.DONCHIAN_EXIT,
      atrPeriod: strategy.ATR_PERIOD,
      leg1Pct: strategy.LEG1_PCT,
      leg2Pct: strategy.LEG2_PCT,
      leg1AtrMult: strategy.LEG1_ATR_MULT,
    },
    // Non-secret AI analyst summary: whether it is live, which provider/model,
    // cadence. The API key itself never appears here.
    ai: {
      enabled: ai.ENABLED && Boolean(ai.API_KEY),
      configured: Boolean(ai.API_KEY),
      provider: ai.PROVIDER,
      providerLabel: ai.PROVIDER_LABEL,
      model: ai.MODEL,
      models: [ai.MODEL, ...ai.FALLBACK_MODELS, ai.FALLBACK_MODEL].filter(
        (m, i, all) => m && all.indexOf(m) === i
      ),
      keys: ai.API_KEYS.length,
      symbol: ai.SYMBOL,
      symbols: ai.SYMBOLS,
      staggerMs: ai.STAGGER_MS,
      managementEvery: ai.MANAGEMENT_EVERY,
      intervalMs: ai.INTERVAL_MS,
      minRR: ai.MIN_RR,
      telegram: ai.TELEGRAM,
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

  // The AI analyst is optional: missing key disables it rather than blocking
  // boot, but saying so out loud beats a silently idle feature.
  if (ai.ENABLED && !ai.API_KEY) {
    warnings.push(
      `AI_SIGNAL_ENABLED is on but ${ai.PROVIDER === 'groq' ? 'GROQ_API_KEY' : 'OPENROUTER_API_KEY'} ` +
        `is not set — the AI analyst stays idle. Add it in Render → Environment to activate ${ai.MODEL}.`
    );
  }
  if (requestedProvider !== 'auto' && !AI_PROVIDERS[requestedProvider]) {
    warnings.push(
      `AI_SIGNAL_PROVIDER="${requestedProvider}" is not a known provider — using ${provider.label} instead.`
    );
  }

  // Admin console + APK memberships. These are optional features, but a
  // half-configured console is worse than a clearly disabled one.
  if (admin.ENABLED && !admin.PASSWORD && !admin.PASSWORD_HASH) {
    warnings.push(
      'ADMIN_PASSWORD (or ADMIN_PASSWORD_HASH) is not set — /admin/ is disabled. ' +
        'Add a strong password in Render → Environment, then redeploy.'
    );
  }
  if (admin.PASSWORD && admin.PASSWORD.length < 10) {
    warnings.push('ADMIN_PASSWORD is shorter than 10 characters — use a long random value.');
  }
  if (admin.ENABLED && !admin.SESSION_SECRET && (admin.PASSWORD || admin.PASSWORD_HASH)) {
    warnings.push(
      'ADMIN_SESSION_SECRET is not set — admin sessions are signed with a key derived from the password. ' +
        'Set a separate random secret so rotating the password does not just invalidate sessions.'
    );
  }
  if (membership.ENABLED && (!supabase.URL || !supabase.SERVICE_ROLE_KEY)) {
    warnings.push(
      'MEMBERSHIP_ENABLED is on but Supabase is not configured — APK users, requests and AI trades ' +
        'will live in memory only and disappear on restart.'
    );
  }

  return { problems, warnings, ready: problems.length === 0 };
}

export default ENV;