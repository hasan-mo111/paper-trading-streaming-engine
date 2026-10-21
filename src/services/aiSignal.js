/* =============================================================================
 *  AETHERON CORE  —  src/services/aiSignal.js
 *  -----------------------------------------------------------------------------
 *  The AI analyst. A second, INDEPENDENT reader of the candles the strategy
 *  already keeps in RAM, and the only place in this codebase that talks to a
 *  language model.
 *
 *  WHAT IT DOES
 *  ------------
 *    1. Snapshots five timeframes (D1, H4, H1, M15, M5) for one instrument.
 *    2. Sends them to an OpenAI-compatible chat endpoint (Groq by default,
 *       OpenRouter available) with a strict-JSON system prompt.
 *    3. Parses the reply and VALIDATES it like any other untrusted input:
 *       direction ∈ {BUY,SELL,HOLD}, prices finite and ordered, entry near the
 *       live quote, reward:risk ≥ MIN_RR. A signal that fails validation is
 *       downgraded to HOLD — never silently published as a trade idea.
 *    4. Publishes the result: an AI_SIGNAL chat message on the SSE channel and,
 *       when configured, a Telegram alert via the gateway.
 *
 *  WHAT IT DOES NOT DO
 *  -------------------
 *    It does not place orders, size positions or touch the broker. The model's
 *    output is text on a screen. The strategy engine remains the only actor
 *    that can open or close a position — that invariant is the whole point of
 *    the tradeGuard layer, and an LLM must never be allowed to break it.
 *
 *  Every dependency is injected (market, dispatcher, telegram, fetch) so the
 *  parsing/validation core is testable with no network and no key.
 * ============================================================================= */

import { getSymbolSpec, roundPrice } from '../config/db.js';
import { createLogger } from './logger.js';
import { CHAT_TYPES } from './sseDispatcher.js';
import { KeyPool, cooldownForMessage } from './keyPool.js';
import { filterTradableBars, isWeekendBar } from './marketSession.js';

const log = createLogger('ai');

/**
 * Does this error mean "you have spent your quota"? Every provider answers 429
 * for per-minute/per-day limits (OpenRouter: request/day; Groq: tokens/day),
 * 402 when credits run out, and third-party proxies phrase it as "rate limit" /
 * "quota exceeded". Kept as a module-level export so the multi-symbol manager
 * can back off on the same definition the analyst uses to stop retrying.
 *
 * @param {unknown} err
 * @returns {boolean}
 */
export function isRateLimitError(err) {
  const message = typeof err === 'string' ? err : (err && err.message) || '';
  return /(^|\D)(429|402)(\D|$)|rate.?limit|quota|too many requests|insufficient.?credit/i.test(
    String(message)
  );
}

/**
 * Is this a CREDENTIAL problem rather than a model problem? Quota exhaustion
 * (429/402) and rejected keys (401/403) are cured by rotating to the next slot;
 * a timeout, a 500 or a malformed completion is not.
 *
 * @param {unknown} err
 * @returns {boolean}
 */
export function isKeyError(err) {
  const message = typeof err === 'string' ? err : (err && err.message) || '';
  return (
    isRateLimitError(message) ||
    /(^|\D)(401|403)(\D|$)|invalid[_\s-]?api[_\s-]?key|unauthori[sz]ed|authentication|incorrect api key/i.test(
      String(message)
    )
  );
}

/**
 * A per-MODEL quota rejection. Groq names the model in its 429 — "Rate limit
 * reached for model `llama-3.3-70b-versatile` … on tokens per day (TPD)" — and
 * meters each model separately. OpenRouter's account-wide "free-models-per-day"
 * names no model, so it stays a key-level problem.
 *
 * @param {unknown} err
 * @returns {boolean}
 */
export function isModelQuotaError(err) {
  const message = typeof err === 'string' ? err : (err && err.message) || '';
  // 413 is Groq's answer when the request itself is bigger than the model's
  // per-minute budget ("Request too large ... on tokens per minute (TPM)").
  // It is a per-model limit, so the chain must move on instead of re-asking the
  // same model with the same oversized prompt every cycle.
  const limited =
    isRateLimitError(message) ||
    /(^|\D)413(\D|$)|request too large|tokens per minute \(TPM\)/i.test(message);
  return limited && /for model|model `|model "|model id/i.test(String(message));
}

/**
 * A model id that no longer exists (decommissioned, renamed, typo). Retrying it
 * every cycle spends one request per symbol for nothing; park it for hours.
 *
 * @param {unknown} err
 * @returns {boolean}
 */
export function isModelMissingError(err) {
  const message = typeof err === 'string' ? err : (err && err.message) || '';
  return /(^|\D)404(\D|$)|model.*(not found|does not exist|decommissioned|not supported)/i.test(
    String(message)
  );
}

/* --------------------------------------------------------------- timeframes */

/**
 * The five frames the prompt reasons over, mapped to the aggregator's seconds
 * keys (see config/db.js TIMEFRAMES). Order is macro → micro so the model reads
 * the higher-timeframe bias first.
 */
const FRAMES = Object.freeze([
  { bucket: 'D1', seconds: 86_400, label: 'Daily' },
  { bucket: 'H4', seconds: 14_400, label: '4H' },
  { bucket: 'H1', seconds: 3_600, label: '1H' },
  { bucket: 'M15', seconds: 900, label: '15m' },
  { bucket: 'M5', seconds: 300, label: '5m' },
]);

/** Minimum closed bars per timeframe before we consider the data usable. */
const MIN_BARS = 20;

/**
 * Extra user turn appended when a model answers with prose instead of the JSON
 * contract. Some open-weights deployments open with "Here's the setup…" and
 * never emit an object at all — the parser then has nothing to salvage. One
 * explicit correction recovers the cycle instead of burning a whole interval.
 */
export const JSON_CORRECTION =
  'Your previous reply was not valid JSON. Reply with ONLY the JSON object described ' +
  'in the schema — no prose, no markdown fences, no explanation before or after it. ' +
  'The reply must start with "{" and end with "}".';

/**
 * Pull the answer out of a chat message. Providers keep the final text in
 * `content`, but reasoning models may stash it in `reasoning_content` when the
 * token budget runs out first. Returns a trimmed string, or '' when there is
 * genuinely nothing usable.
 */
function pickContent(message) {
  for (const key of ['content', 'reasoning_content', 'reasoning']) {
    const v = message?.[key];
    if (typeof v === 'string' && v.trim()) return v.trim();
  }
  return '';
}

/* ------------------------------------------------------------ system prompt */
/*
 *  Kept verbatim in spirit to the desk instruction: bias → zone → risk, with a
 *  hard "do not force a trade" rule and a strict JSON contract. The model is
 *  told the payload format in the USER turn, so this stays the stable role.
 */
export const SYSTEM_PROMPT = [
  'You are an elite, institutional-grade algorithmic trading AI specialised in',
  'XAU/USD (Gold) and Forex markets. You analyse multi-timeframe market data',
  '(Daily, 4H, 1H, 15m, 5m) supplied as historical candles and output a precise,',
  'high-probability trading signal or a NO_TRADE (HOLD) decision.',
  '',
  'Analysis guidelines:',
  '1. Macro trend: use Daily and 4H to determine the major direction and',
  '   structural bias (liquidity zones, BOS, ChoCH).',
  '2. Execution zone: use 1H and 15m to locate the optimal entry, a structural',
  '   pullback or a liquidity sweep.',
  '3. Risk management: enforce a MINIMUM risk-to-reward of 1:2. Stop losses are',
  '   never arbitrary — they sit logically beyond a recent swing high/low.',
  '4. Filtering: in messy consolidation or high-risk noise, return action',
  '   "HOLD". Do not force a trade.',
  '5. Economic calendar: when upcoming releases are supplied with the data, treat',
  '   them as a risk filter. Do not open a trade straight into an imminent',
  '   high-impact release, and name the event in your reasoning when it shapes',
  '   the decision.',
  '',
  'Respond ONLY with a single valid JSON object — no markdown, no prose outside',
  'the JSON — in exactly this structure:',
  '{',
  '  "action": "BUY" | "SELL" | "HOLD",',
  '  "symbol": "XAU/USD",',
  '  "entry_price": 0.00,',
  '  "stop_loss": 0.00,',
  '  "take_profit_1": 0.00,',
  '  "take_profit_2": 0.00,',
  '  "confidence_score": "Low" | "Medium" | "High",',
  '  "reasoning": "A concise, professional 2-3 sentence explanation of why this',
  '                trade was taken, based on multi-timeframe structure and liquidity."',
  '}',
].join('\n');

/* ============================================================ AiSignalService */

export class AiSignalService {
  /**
   * @param {object}  opts
   * @param {object}  opts.market          MarketAggregator (candle store)
   * @param {object}  opts.dispatcher      SSEDispatcher (chat channel)
   * @param {object}  [opts.env]           ENV override (tests)
   * @param {object}  [opts.telegram]      Telegram gateway handle ({ notifyAll })
   * @param {Function} [opts.fetchFn]      fetch override (tests)
   * @param {object}  [opts.news]          EconomicNewsService (optional)
   * @param {object}  [opts.keyPool]       shared KeyPool (the manager passes one
   *                                       so all symbols rotate together)
   */
  constructor({
    market,
    dispatcher,
    env = null,
    telegram = null,
    fetchFn = null,
    news = null,
    keyPool = null,
  } = {}) {
    if (!market) throw new Error('AiSignalService requires a market aggregator');
    if (!dispatcher) throw new Error('AiSignalService requires an SSE dispatcher');

    this.market = market;
    this.dispatcher = dispatcher;
    this.env = env ?? market.env ?? null;
    this.news = news;
    // `env.ai` can be partial — a trimmed test env, or a knob added to ENV after
    // this process last booted. Merging over DEFAULTS guarantees every key the
    // service reads exists, so a missing entry degrades to a sane value instead
    // of silently disabling a feature through `undefined`.
    this.cfg = { ...DEFAULTS, ...((this.env && this.env.ai) || {}) };
    this.telegram = telegram;
    this.fetchFn = fetchFn ?? globalThis.fetch;

    // One pool per process, not per symbol: four services sharing a single key
    // list means key 1's daily budget is spent once, not re-discovered by four
    // independent loops. Standalone use (tests, embedded) builds its own.
    this.keyPool =
      keyPool ?? new KeyPool(this.#configuredKeys(this.cfg));

    this.enabled = Boolean(this.cfg.ENABLED && this.keyPool.size);

    /** Guard so a slow model call cannot be overlapped by the next tick. */
    this.running = false;
    this.lastManualAt = 0;
    this.bootTimer = null;
    this.intervalTimer = null;

    /** Observability for /health and /api/ai/signal. */
    this.stats = {
      runs: 0,
      published: 0,
      holds: 0,
      rejected: 0,
      errors: 0,
      fallbacks: 0,
      opened: 0,
      closed: 0,
      wins: 0,
      losses: 0,
      reversals: 0,
      statuses: 0,
      rateLimits: 0,
      /** Times a quota/credential error moved the analyst to the next key. */
      keyRotations: 0,
      /** Times a per-model daily bucket moved the analyst to the next model. */
      modelLimits: 0,
      /** Size of the last prompt, so a free-tier 413 is diagnosable. */
      promptChars: 0,
      promptTokensEst: 0,
      lastRunAt: null,
      lastError: null,
      lastModel: null,
      lastLatencyMs: null,
    };
    /** The most recent validated signal, replayed to new readers. */
    this.lastSignal = null;
    /** De-dupe key so an unchanged read does not spam Telegram. */
    this.lastTelegramKey = null;

    /**
     * The single trade the analyst is following right now.
     *
     * While this is set a new signal is NEVER published as an entry — the
     * analyst manages this one until it takes profit, stops out, or is reversed.
     * It is deliberately in-memory: after a restart the service starts flat
     * rather than trusting an "open" flag it cannot verify against a fill it
     * never received (the AI analyst never touches the broker).
     * @type {object|null}
     */
    this.position = null;
    /** Closed trades, newest last. Bounded: this is a status feed, not a ledger. */
    this.history = [];
    /** Catch-up retry timer, plus how many were burned inside the current interval. */
    this.catchupTimer = null;
    this.catchupsUsed = 0;
  }

  /* ------------------------------------------------------------- lifecycle */

  /**
   * Keys the environment gave us for this provider. `API_KEYS` is the resolved
   * list (one or more accounts); `API_KEY` is the single-key fallback used by
   * tests and older boot paths.
   *
   * @param {object} cfg
   * @returns {string[]}
   */
  #configuredKeys(cfg = this.cfg) {
    const list = Array.isArray(cfg.API_KEYS) ? cfg.API_KEYS : [];
    const keys = list.map((k) => String(k || '').trim()).filter(Boolean);
    if (keys.length) return keys;
    // Older call sites only set the provider-specific key, never API_KEY. Never
    // borrow the OTHER provider's credential: that would enable the analyst with
    // a token the active endpoint rejects.
    const legacy = cfg.PROVIDER === 'openrouter' ? cfg.OPENROUTER_API_KEY : cfg.GROQ_API_KEY;
    const single = String(cfg.API_KEY || legacy || '').trim();
    return single ? [single] : [];
  }

  /** Schedule the automatic loop. No-op when disabled or unconfigured. */
  start() {
    if (!this.enabled) {
      log.info(
        this.keyPool.size
          ? 'AI analyst disabled (AI_SIGNAL_ENABLED=false)'
          : `AI analyst idle — no ${this.cfg.PROVIDER_LABEL || 'provider'} API key ` +
              `(${this.cfg.PROVIDER === 'groq' ? 'GROQ_API_KEY' : 'AI provider key'} not set)`
      );
      return;
    }
    if (this.bootTimer || this.intervalTimer) return;

    // First read is delayed: at t=0 the aggregator has not backfilled yet, so a
    // snapshot now would be empty and the model would answer on noise.
    this.bootTimer = setTimeout(() => {
      this.run('boot').catch((e) => log.error('AI analyst boot run failed:', e.message));
    }, this.cfg.BOOT_DELAY_MS);
    this.bootTimer.unref?.();

    this.intervalTimer = setInterval(() => {
      // A new interval is a fresh budget for catch-up retries.
      this.catchupsUsed = 0;
      this.run('schedule').catch((e) => log.error('AI analyst scheduled run failed:', e.message));
    }, this.cfg.INTERVAL_MS);
    this.intervalTimer.unref?.();

    log.ok(
      `AI analyst armed — ${this.cfg.MODEL} every ${Math.round(this.cfg.INTERVAL_MS / 60_000)}m ` +
        `on ${this.cfg.SYMBOL}`
    );
  }

  stop() {
    if (this.bootTimer) clearTimeout(this.bootTimer);
    if (this.intervalTimer) clearInterval(this.intervalTimer);
    if (this.catchupTimer) clearTimeout(this.catchupTimer);
    this.bootTimer = null;
    this.intervalTimer = null;
    this.catchupTimer = null;
  }

  /** Attach the Telegram gateway once app.js has mounted it. */
  attachTelegram(handle) {
    if (handle) this.telegram = handle;
  }

  /* ----------------------------------------------------------------- status */

  status() {
    return {
      enabled: this.enabled,
      configured: this.keyPool.size > 0,
      provider: this.cfg.PROVIDER,
      providerLabel: this.cfg.PROVIDER_LABEL,
      running: this.running,
      symbol: this.cfg.SYMBOL,
      model: this.cfg.MODEL,
      /** Full attempt order, so a failing model is visible without a log dive. */
      modelChain: this.#modelChain(),
      /** Slot counts only — never key material. */
      keys: this.keyPool.status(),
      intervalMs: this.cfg.INTERVAL_MS,
      bootDelayMs: this.cfg.BOOT_DELAY_MS,
      maxTokens: this.cfg.MAX_TOKENS,
      minRR: this.cfg.MIN_RR,
      telegram: Boolean(this.cfg.TELEGRAM && (this.telegram || this.cfg.TELEGRAM_CHAT_ID)),
      trackEnabled: Boolean(this.cfg.TRACK_ENABLED),
      statusEveryCycle: Boolean(this.cfg.STATUS_EVERY_CYCLE),
      ...this.stats,
      catchupsUsed: this.catchupsUsed,
      // Plaintext on purpose: the same levels ride the public chart feed, and
      // hiding them here while publishing them on /stream would only make the
      // REST replay disagree with the live one.
      lastSignal: this.lastSignal,
      position: this.position,
      positionOpen: Boolean(this.position),
      historyCount: this.history.length,
      news: this.news?.status?.() ?? null,
    };
  }

  /* ==================================================================== run */

  /**
   * Execute one analysis cycle.
   * @param {'boot'|'schedule'|'manual'} reason
   * @returns {Promise<object>} a result descriptor (never throws for expected
   *          states — the route turns it into a status code).
   */
  async run(reason = 'schedule') {
    if (!this.enabled) return { ok: false, error: 'disabled', reason };
    if (this.running) return { ok: false, error: 'busy', reason };

    if (reason === 'manual') {
      const waitMs = this.cfg.MANUAL_COOLDOWN_S * 1_000;
      const since = Date.now() - this.lastManualAt;
      if (since < waitMs) {
        return { ok: false, error: 'cooldown', reason, retryInMs: waitMs - since };
      }
      this.lastManualAt = Date.now();
    }

    this.running = true;
    const startedAt = Date.now();
    try {
      const snapshot = this.#snapshot();
      if (!snapshot.ready) {
        log.warn(`AI analyst: insufficient candles (${snapshot.detail})`);
        return { ok: false, error: 'insufficient-data', reason, detail: snapshot.detail };
      }

      // Calendar context first, and never at the cost of a cycle: a failed
      // refresh leaves the previous window in place and the run continues.
      let newsLines = [];
      if (this.news) {
        await this.news.ensureFresh().catch(() => {});
        newsLines = this.news.forPrompt();
      }

      const attempt = await this.#askWithRetries(snapshot, newsLines, {
        force: reason === 'manual',
      });

      if (!attempt.parsed.ok) {
        const rateLimited = Boolean(attempt.rateLimited);
        this.stats.errors += 1;
        this.stats.lastError = rateLimited
          ? `rate-limit: ${attempt.parsed.error}`
          : `parse: ${attempt.parsed.error}`;
        log.warn(`AI analyst: no usable reply after ${attempt.attempts} attempt(s) —`, attempt.parsed.error);
        // A rate-limit failure must NOT schedule a catch-up: the retry would
        // hit the same wall and spend the remainder of the quota doing it.
        if (rateLimited) return { ok: false, error: 'rate-limited', reason, detail: attempt.parsed.error };
        // Do not let a failed cycle become a silent 30-minute hole in the feed.
        this.#scheduleCatchup();
        return { ok: false, error: 'bad-model-output', reason, detail: attempt.parsed.error };
      }

      const call = attempt.call;
      const signal = this.#validate(attempt.parsed.value, snapshot);
      // The turn may have been served by a fallback model — report the model
      // that actually answered, not the one that was configured.
      signal.model = call.model;
      const latency = Date.now() - startedAt;

      this.stats.runs += 1;
      this.stats.lastRunAt = Date.now();
      this.stats.lastLatencyMs = latency;
      this.stats.lastModel = call.model;
      // A fallback succeeded: keep the cycle healthy and do not leave a
      // transient 429/413 from an earlier attempt in the public status.
      this.stats.lastError = null;
      this.catchupsUsed = 0; // a healthy cycle clears the catch-up budget
      this.lastSignal = signal;

      if (signal.action === 'HOLD') this.stats.holds += 1;
      else this.stats.published += 1;
      if (!signal.valid) this.stats.rejected += 1;

      // The trade lifecycle decides whether this is an entry, a management
      // update, or the reversal that closes the running trade.
      const effect = this.#applySignal(signal, snapshot, reason);

      log.info(
        `AI analyst → ${signal.action}` +
          (signal.action !== 'HOLD' ? ` @ ${signal.entry_price} (R:R 1:${signal.rr.toFixed(2)})` : '') +
          ` · ${latency}ms · ${effect.kind}${signal.valid ? '' : ' · downgraded: ' + signal.note}`
      );
      return { ok: true, reason, signal, effect: effect.kind, position: this.position };
    } catch (e) {
      this.stats.runs += 1;
      this.stats.errors += 1;
      this.stats.lastError = e.message;
      this.stats.lastRunAt = Date.now();
      log.error('AI analyst run failed:', e.message);
      this.#scheduleCatchup();
      return { ok: false, error: 'exception', reason, detail: e.message };
    } finally {
      this.running = false;
    }
  }

  /* ======================================================== trade lifecycle */

  /**
   * Follow a live quote and act on the running trade the instant a level is
   * touched — a 30-minute cycle is far too coarse to be sitting inside a stop.
   *
   * Wired to the market aggregator's forming-tick hook, so it is O(1), does
   * no allocation while flat, and cannot throw into the tick path.
   *
   * @param {string} symbol
   * @param {number} price
   * @returns {object|null} the closed trade, or null when nothing happened
   */
  onPrice(symbol, price) {
    const p = this.position;
    if (!p || !Number.isFinite(price)) return null;
    if (String(symbol).toUpperCase() !== p.symbol) return null;

    const dir = p.action === 'BUY' ? 1 : -1;
    p.lastPrice = price;
    p.ticks = (p.ticks ?? 0) + 1;

    // Track the excursion so the status line can report heat taken.
    const movePct = ((price - p.entry) / p.entry) * 100 * dir;
    if (movePct > p.mfe) p.mfe = movePct;
    if (movePct < p.mae) p.mae = movePct;

    const atOrBeyond = (level) => (dir > 0 ? price >= level : price <= level);
    const atOrWorse = (level) => (dir > 0 ? price <= level : price >= level);

    // Stop first: when a single tick gaps through both, the stop is the fill a
    // real broker would have given.
    if (atOrWorse(p.sl)) return this.#closePosition('SL', price);
    if (atOrBeyond(p.tp2)) return this.#closePosition('TP2', price);
    if (atOrBeyond(p.tp1) && p.stage === 'open') return this.#protectAtBreakeven(price);
    return null;
  }

  /**
   * Decide what a fresh signal means for the book.
   *
   *  • flat  + actionable signal → open the trade (the only entry path)
   *  • open  + opposite idea at Medium/High confidence → close as REVERSAL
   *  • open  + anything else     → report, never re-enter
   *  • flat  + HOLD/invalid      → report why nothing was taken
   *
   * @returns {{ kind: 'opened'|'management'|'reversal-close'|'hold' }}
   */
  #applySignal(signal, snapshot, reason) {
    const actionable = signal.valid && (signal.action === 'BUY' || signal.action === 'SELL');

    if (this.position) {
      const opposite = signal.action !== this.position.action;
      const confident = signal.confidence_score === 'High' || signal.confidence_score === 'Medium';

      if (actionable && opposite && confident) {
        const closed = this.#closePosition('REVERSAL', snapshot.price, { signal });
        return { kind: closed ? 'reversal-close' : 'management' };
      }
      this.#publishStatus(signal, snapshot, reason, 'management');
      return { kind: 'management' };
    }

    if (actionable) {
      if (this.cfg.TRACK_ENABLED) this.#openPosition(signal, snapshot);
      this.#publishStatus(signal, snapshot, reason, this.cfg.TRACK_ENABLED ? 'opened' : 'signal');
      return { kind: 'opened' };
    }

    this.#publishStatus(signal, snapshot, reason, 'hold');
    return { kind: 'hold' };
  }

  /** Take the signal as a paper position the analyst will now manage. */
  #openPosition(signal, snapshot) {
    this.position = {
      id: `ai-${Date.now().toString(36)}`,
      action: signal.action,
      symbol: signal.symbol,
      entry: signal.entry_price,
      sl: signal.stop_loss,
      tp1: signal.take_profit_1,
      tp2: signal.take_profit_2,
      rr: signal.rr,
      confidence: signal.confidence_score,
      reasoning: signal.reasoning,
      model: signal.model,
      openedAt: new Date().toISOString(),
      openedMs: Date.now(),
      openedAtPrice: snapshot.price,
      stage: 'open', // 'open' | 'tp1'
      lastPrice: snapshot.price,
      mfe: 0,
      mae: 0,
      ticks: 0,
    };
    this.stats.opened += 1;
    log.ok(`AI trade OPEN → ${signal.action} ${signal.symbol} @ ${signal.entry_price}`);
  }

  /** TP1 reached: pull the stop to entry so the trade can no longer lose. */
  #protectAtBreakeven(price) {
    const p = this.position;
    if (!p) return null;
    p.stage = 'tp1';
    p.sl = p.entry;
    p.lastPrice = price;
    log.ok(`AI trade ${p.symbol}: TP1 reached at ${price} — stop moved to breakeven (${p.entry})`);
    this.#emitTradeEvent('TP1', p, price);
    return null;
  }

  /**
   * Close the running trade at `price` and report it.
   * @param {'SL'|'TP2'|'REVERSAL'} reason
   */
  #closePosition(reason, price, { signal = null } = {}) {
    const p = this.position;
    if (!p) return null;

    const dir = p.action === 'BUY' ? 1 : -1;
    const pnlPoints = (price - p.entry) * dir;
    const closed = {
      ...p,
      closedAt: new Date().toISOString(),
      closedMs: Date.now(),
      exitPrice: price,
      exitReason: reason,
      pnlPoints,
      pnlPct: (pnlPoints / p.entry) * 100,
      exitNote: signal?.reasoning ?? null,
    };

    this.position = null;
    this.history.push(closed);
    if (this.history.length > 50) this.history.shift();

    this.stats.closed += 1;
    if (reason === 'REVERSAL') this.stats.reversals += 1;
    if (pnlPoints >= 0) this.stats.wins += 1;
    else this.stats.losses += 1;

    const icon = reason === 'SL' ? '🛑' : reason === 'REVERSAL' ? '↩️' : '✅';
    log.ok(
      `AI trade CLOSED (${reason}) ${closed.action} ${closed.symbol} @ ${price} ` +
        `→ ${pnlPoints >= 0 ? '+' : ''}${pnlPoints.toFixed(2)} pts`
    );
    this.#emitTradeEvent(reason, closed, price, icon);
    return closed;
  }

  /** Schedule a single catch-up run after a failed cycle. */
  #scheduleCatchup() {
    if (!this.cfg.CATCHUP_ENABLED || this.catchupTimer) return;
    if (this.catchupsUsed >= this.cfg.CATCHUP_MAX) {
      log.warn('AI analyst: catch-up budget exhausted — waiting for the next interval');
      return;
    }
    this.catchupsUsed += 1;
    const delay = this.cfg.CATCHUP_DELAY_MS;
    this.catchupTimer = setTimeout(() => {
      this.catchupTimer = null;
      this.run('catchup').catch((e) => log.error('AI analyst catch-up failed:', e.message));
    }, delay);
    this.catchupTimer.unref?.();
    log.warn(`AI analyst: catch-up #${this.catchupsUsed} scheduled in ${Math.round(delay / 1_000)}s`);
  }

  /* ============================================================ data gather */

  /**
   * Build the five-timeframe snapshot for the configured symbol.
   * @returns {{ ready: boolean, detail: string, symbol: string, price: number|null,
   *            frames: object, counts: object }}
   */
  #snapshot() {
    const symbol = this.cfg.SYMBOL;
    const frames = {};
    const counts = {};
    let shortest = Infinity;
    const missing = [];

    for (const f of FRAMES) {
      const limit = this.cfg.BARS?.[f.bucket] ?? 40;
      // Belt and suspenders: the aggregator already refuses weekend bars, but
      // an older in-memory ring must never teach the model a flat weekend.
      const closed = filterTradableBars(this.market.closedCandles(symbol, f.seconds));
      const forming = this.market.formingCandle(symbol, f.seconds);
      const bars = closed.slice(-limit).map(compact);
      if (forming && Number.isFinite(forming.close) && !isWeekendBar(forming.time)) {
        bars.push(compact(forming));
      }
      frames[f.bucket] = { label: f.label, seconds: f.seconds, bars };
      counts[f.bucket] = closed.length;
      if (closed.length < MIN_BARS) missing.push(`${f.bucket}:${closed.length}`);
      shortest = Math.min(shortest, closed.length);
    }

    const price = compactPrice(this.market.priceOf(symbol, 300) ?? this.market.priceOf(symbol, 900));

    const ready = missing.length === 0;
    return {
      ready,
      detail: ready ? `${FRAMES.length} frames ok` : `need ≥${MIN_BARS} bars — ${missing.join(', ')}`,
      symbol,
      price,
      frames,
      counts,
      shortest,
    };
  }

  /* ============================================================== model call */

  async #askModel(
    snapshot,
    model = this.cfg.MODEL,
    maxTokens = this.cfg.MAX_TOKENS,
    { correction = null, news = [], key = '' } = {}
  ) {
    const user = buildUserMessage(snapshot, news, { maxChars: this.cfg.MAX_PROMPT_CHARS });
    // Conservative estimate (digit-heavy JSON tokenises worse than prose).
    // Exposed in /api/ai/signal so an oversized prompt is visible before the
    // provider answers 413.
    this.stats.promptChars = user.length;
    this.stats.promptTokensEst = Math.ceil((user.length + SYSTEM_PROMPT.length) / 2);
    const messages = [
      { role: 'system', content: SYSTEM_PROMPT },
      { role: 'user', content: user },
    ];
    // A reply we could not parse gets one follow-up turn spelling out the
    // contract; models that add a preamble usually comply on the second ask.
    if (correction) messages.push({ role: 'user', content: correction });

    // Groq and OpenRouter both speak this dialect, but not every deployment
    // honours JSON mode; a 400 that names `response_format` is retried once
    // without it rather than losing the cycle.
    const ask = (withJsonMode) => {
      const body = {
        model,
        temperature: 0.2,
        max_tokens: maxTokens,
        messages,
      };
      if (withJsonMode) {
        // Best-effort JSON mode. The parser is strict either way, so an ignored
        // flag degrades gracefully.
        body.response_format = { type: 'json_object' };
      }
      // gpt-oss is a reasoning model. At the default effort it can spend the
      // entire completion budget thinking and return empty `content`; low
      // effort leaves room for the JSON inside a free-tier token budget.
      if (this.cfg.PROVIDER === 'groq' && /gpt-oss/i.test(model)) {
        body.reasoning_effort = 'low';
      }

      const headers = {
        Authorization: `Bearer ${key}`,
        'Content-Type': 'application/json',
      };
      // OpenRouter attribution headers — optional, but they help the free tier
      // identify the caller. Other providers reject nothing but do not need them.
      if (String(this.cfg.ENDPOINT).includes('openrouter.ai')) {
        headers['HTTP-Referer'] = 'https://github.com/hasan-mo111/paper-trading-streaming-engine';
        headers['X-Title'] = 'Aetheron Core AI Analyst';
      }

      return this.fetchFn(this.cfg.ENDPOINT, {
        method: 'POST',
        headers,
        body: JSON.stringify(body),
        signal: AbortSignal.timeout(this.cfg.TIMEOUT_MS),
      });
    };

    const label = this.cfg.PROVIDER_LABEL || 'Provider';
    let res = await ask(true);
    let text = await res.text();
    let json = null;
    try {
      json = JSON.parse(text);
    } catch {
      /* fall through to the error below */
    }

    // JSON mode unsupported? Ask again without it before declaring failure.
    if (!res.ok && res.status === 400 && /response_format|json_object|json mode/i.test(text)) {
      log.warn(`${label} rejected JSON mode for "${model}" — retrying without response_format`);
      res = await ask(false);
      text = await res.text();
      json = null;
      try {
        json = JSON.parse(text);
      } catch {
        /* fall through to the error below */
      }
    }

    if (!res.ok) {
      const detail = json?.error?.message ?? text.slice(0, 200) ?? res.status;
      throw new Error(`${label} ${res.status}: ${detail}`);
    }

    const msg = json?.choices?.[0]?.message ?? {};
    // Some reasoning models stash the final answer in `reasoning_content` when
    // the token budget runs out before `content` is written. Check both.
    const content = pickContent(msg);
    if (!content) {
      const finish = json?.choices?.[0]?.finish_reason ?? 'unknown';
      throw new Error(
        `empty completion from "${model}" (finish_reason=${finish}, max_tokens=${maxTokens}). ` +
          'A reasoning model likely spent its whole budget thinking — raise AI_SIGNAL_MAX_TOKENS or switch AI_SIGNAL_MODEL.'
      );
    }
    return { content, model, finishReason: json?.choices?.[0]?.finish_reason ?? null };
  }

  /**
   * Ask the model chain until one reply parses. There are three independent
   * failure axes and each gets its own response:
   *
   *   • the CREDENTIAL is out (401/403, or an account-wide 429/402) → park that
   *     key for every model and retry the same model with the next slot.
   *   • the (KEY, MODEL) pair is out — Groq's per-model daily token bucket →
   *     park only that pair; another account may still serve the same model,
   *     otherwise walk to the next model.
   *   • the MODEL cannot answer (timeout, empty completion, a malformed reply, a
   *     decommissioned id) → walk to the next model, giving it the JSON-only
   *     correction turn and extra token headroom.
   *
   * The loop only reports `rateLimited` when no (key, model) pair is usable —
   * a real quota wall, and the manager should stretch the whole interval.
   *
   * @returns {Promise<{call:object, parsed:object, attempts:number, rateLimited?:boolean}>}
   */
  async #askWithRetries(snapshot, newsLines = [], { force = false } = {}) {
    const configured = this.#modelChain();
    // Skip models whose every account is already parked. This is what makes the
    // second, third and fourth model actually get used instead of re-asking an
    // exhausted primary every cycle. A manual ask is the exception: it may wake
    // one parked slot to test whether the quota came back.
    const usable = configured.filter((m) => this.keyPool.hasModelAvailable(m));
    const chain = usable.length || !force ? usable : configured;
    if (!chain.length) {
      this.stats.rateLimits += 1;
      this.stats.lastError = 'rate-limit: every model is cooling down';
      return {
        call: { content: '', model: configured[0] ?? this.cfg.MODEL },
        parsed: { ok: false, error: 'every model is cooling down' },
        attempts: 0,
        rateLimited: true,
      };
    }

    // Every usable model gets at least one shot; MAX_ATTEMPTS still buys a
    // correction turn when the chain is short. The hard ceiling protects the
    // daily quota from a pathological all-fail cycle.
    const maxCalls = Math.max(
      1,
      Math.min(
        Math.max(1, Number(this.cfg.MAX_CALLS_PER_CYCLE) || 6),
        Math.max(1, Number(this.cfg.MAX_ATTEMPTS) || 1, chain.length)
      )
    );
    let call = { content: '', model: chain[0] };
    let parsed = { ok: false, error: 'no attempt made' };
    let i = 0;
    // A manual ask may wake a parked key once; automatic cycles never do.
    let mayForce = Boolean(force);

    while (i < maxCalls) {
      const model = chain[Math.min(i, chain.length - 1)];
      // Attempt 0 asks cleanly; every later attempt re-sends the same turn with
      // the JSON contract spelled out. The token budget deliberately stays the
      // same: Groq counts input + max_tokens against the per-minute cap.
      const correction = i === 0 ? null : JSON_CORRECTION;
      // Do NOT inflate the retry budget. Groq counts input + max_tokens against
      // the model's per-minute limit, so an 8k retry turned every chatty reply
      // into a guaranteed 413 on the free tier. The JSON contract needs a few
      // hundred tokens; the prompt budget does the rest.
      const maxTokens = this.cfg.MAX_TOKENS;

      // Pick a credential that still has budget for THIS model.
      let slot;
      try {
        slot = this.keyPool.current(Date.now(), { force: mayForce, model });
        mayForce = false;
      } catch {
        // Every account is parked for this model already → next model.
        i += 1;
        continue;
      }

      try {
        call = await this.#askModel(snapshot, model, maxTokens, {
          correction,
          news: newsLines,
          key: slot.key,
        });
        parsed = this.#parseJson(call.content);
        if (parsed.ok) {
          this.keyPool.markOk(slot.key, { model });
          return { call, parsed, attempts: i + 1 };
        }
        log.warn(
          `AI analyst: attempt ${i + 1}/${maxCalls} on "${model}" was not JSON ` +
            `(${parsed.error}; finish_reason=${call.finishReason ?? 'unknown'})`
        );
      } catch (e) {
        log.warn(
          `AI analyst: attempt ${i + 1}/${maxCalls} on "${model}" ` +
            `(key ${slot.slot}/${this.keyPool.size}) failed — ${e.message}`
        );
        parsed = { ok: false, error: e.message };

        // Per-model daily bucket: park only this (key, model) pair.
        if (isModelQuotaError(e.message)) {
          this.keyPool.markLimited(slot.key, cooldownForMessage(e.message), { model });
          this.stats.modelLimits += 1;
          this.stats.lastError = `model ${model}: ${e.message}`;
          if (this.keyPool.hasModelAvailable(model)) continue; // another account
          this.stats.fallbacks += 1;
          i += 1; // next model
          continue;
        }

        // Credential problem: park the key for every model.
        if (isKeyError(e.message)) {
          const quota = isRateLimitError(e.message);
          // Daily counters reset at 00:00 UTC; per-minute limits in a minute.
          // A rejected credential is parked for a day: it will not heal itself.
          const cooldownMs = quota ? cooldownForMessage(e.message) : 24 * 3_600_000;
          this.keyPool.markLimited(slot.key, cooldownMs);
          this.stats.keyRotations += 1;
          this.stats.lastError = `key ${slot.slot}: ${e.message}`;
          if (this.keyPool.hasModelAvailable(model)) continue; // next credential
          if (this.keyPool.hasAvailable()) {
            this.stats.fallbacks += 1;
            i += 1; // next model, same working accounts
            continue;
          }
          this.stats.rateLimits += 1;
          this.stats.lastError = `rate-limit: ${e.message}`;
          return { call, parsed, attempts: i + 1, rateLimited: true };
        }

        // A retired model id must not be retried every cycle.
        if (isModelMissingError(e.message)) {
          this.keyPool.markModelDisabled(model, 6 * 3_600_000);
          this.stats.fallbacks += 1;
          i += 1;
          continue;
        }
      }

      if (i > 0) this.stats.fallbacks += 1;
      i += 1;
    }

    const stillUsable = configured.filter((m) => this.keyPool.hasModelAvailable(m));
    return { call, parsed, attempts: maxCalls, rateLimited: stillUsable.length === 0 };
  }

  /**
   * Ordered list of models to try: the configured primary, then any explicitly
   * configured fallbacks, then the provider's last-resort model. Deduplicated so
   * the primary is never tried twice in a row by accident.
   */
  #modelChain() {
    const extra = Array.isArray(this.cfg.FALLBACK_MODELS) ? this.cfg.FALLBACK_MODELS : [];
    const chain = [this.cfg.MODEL, ...extra, this.cfg.FALLBACK_MODEL].filter(Boolean);
    return [...new Set(chain)];
  }

  /* ================================================================ parsing */

  /** Extract a JSON object from a model reply, tolerating ```json fences. */
  #parseJson(text) {
    let s = String(text).trim();
    // Strip a fenced block if the model wrapped it anyway.
    const fence = /^```(?:json)?\s*([\s\S]*?)\s*```$/i.exec(s);
    if (fence) s = fence[1].trim();
    // Otherwise take the outermost {...} — models sometimes add a sentence.
    if (!s.startsWith('{')) {
      const first = s.indexOf('{');
      const last = s.lastIndexOf('}');
      if (first !== -1 && last > first) s = s.slice(first, last + 1);
    }
    try {
      const value = JSON.parse(s);
      if (!value || typeof value !== 'object' || Array.isArray(value)) {
        return { ok: false, error: 'not an object' };
      }
      return { ok: true, value };
    } catch (e) {
      return { ok: false, error: e.message };
    }
  }

  /* ============================================================= validation */

  /**
   * Coerce + validate the model object into a published signal.
   *
   * The contract we enforce (not the model's):
   *   • action ∈ {BUY, SELL, HOLD}; NO_TRADE ⇒ HOLD
   *   • BUY : SL < entry < TP1 ≤ TP2 ;  SELL : TP2 ≤ TP1 < entry < SL
   *   • entry within MAX_ENTRY_DRIFT of the live quote
   *   • reward:risk (to TP2) ≥ MIN_RR, first target ≥ 1R
   *
   * Anything that fails is downgraded to HOLD with a machine-readable `note`,
   * so a bad idea can never leave the building dressed as a good one.
   */
  #validate(raw, snapshot) {
    const spec = getSymbolSpec(snapshot.symbol);
    const digits = spec?.digits ?? 2;
    const display = spec?.display ?? snapshot.symbol;

    const action = String(raw.action ?? '').trim().toUpperCase();
    const norm = action === 'NO_TRADE' || action === 'NO-TRADE' ? 'HOLD' : action;
    const confidence = ['Low', 'Medium', 'High'].includes(raw.confidence_score)
      ? raw.confidence_score
      : 'Medium';
    const reasoning = typeof raw.reasoning === 'string' && raw.reasoning.trim() ? raw.reasoning.trim() : '—';

    const base = {
      action: 'HOLD',
      symbol: display,
      entry_price: 0,
      stop_loss: 0,
      take_profit_1: 0,
      take_profit_2: 0,
      confidence_score: confidence,
      reasoning,
      rr: 0,
      valid: true,
      note: null,
      asOf: new Date().toISOString(),
      model: this.cfg.MODEL,
      referencePrice: snapshot.price,
    };

    if (norm === 'HOLD') return base;
    if (norm !== 'BUY' && norm !== 'SELL') {
      return { ...base, valid: false, note: `unknown action "${action}"` };
    }

    const entry = Number(raw.entry_price);
    const sl = Number(raw.stop_loss);
    const tp1 = Number(raw.take_profit_1);
    const tp2 = Number(raw.take_profit_2);
    if (![entry, sl, tp1, tp2].every(Number.isFinite)) {
      return { ...base, valid: false, note: 'non-numeric price level' };
    }

    const long = norm === 'BUY';
    // Structural ordering: risk must be on the correct side of entry.
    const ordered = long ? sl < entry && tp1 > entry && tp2 >= tp1 : sl > entry && tp1 < entry && tp2 <= tp1;
    if (!ordered) {
      return { ...base, valid: false, note: 'stop/targets on the wrong side of entry' };
    }

    const risk = Math.abs(entry - sl);
    if (!(risk > 0)) return { ...base, valid: false, note: 'zero risk distance' };
    const reward1 = Math.abs(tp1 - entry);
    const reward2 = Math.abs(tp2 - entry);
    const rr = reward2 / risk;
    const rr1 = reward1 / risk;

    if (rr1 < 1) {
      return { ...base, valid: false, note: `first target below 1R (1:${rr1.toFixed(2)})`, rr };
    }
    if (rr < this.cfg.MIN_RR) {
      return { ...base, valid: false, note: `reward:risk 1:${rr.toFixed(2)} below minimum 1:${this.cfg.MIN_RR}`, rr };
    }

    // Sanity against the live market: a level 4% from spot is not an entry, it
    // is a data error (or the model hallucinating a different instrument).
    if (Number.isFinite(snapshot.price) && snapshot.price > 0) {
      const drift = Math.abs(entry - snapshot.price) / snapshot.price;
      if (drift > this.cfg.MAX_ENTRY_DRIFT) {
        return {
          ...base,
          valid: false,
          note: `entry ${(drift * 100).toFixed(2)}% away from live price`,
          rr,
        };
      }
    }

    return {
      ...base,
      action: norm,
      entry_price: roundPrice(snapshot.symbol, entry),
      stop_loss: roundPrice(snapshot.symbol, sl),
      take_profit_1: roundPrice(snapshot.symbol, tp1),
      take_profit_2: roundPrice(snapshot.symbol, tp2),
      rr,
      digits,
    };
  }

  /* =============================================================== publishing */

  /**
   * Publish one cycle to the chat feed and decide what Telegram receives.
   *
   *  'opened'     → the full trade card (the levels everyone acts on)
   *  'management' → a status line for the trade already running
   *  'hold'       → why the analyst stood aside
   *  'signal'     → an actionable read while tracking is switched off
   *
   * @param {'opened'|'management'|'hold'|'signal'} kind
   */
  /**
   * The exact payload published on the feed (and returned by /api/ai/signal).
   * One shape for every consumer: the dashboard, the mobile PWA and Telegram
   * must never see a different trade.
   */
  #signalPayload(signal, snapshot, kind, reason) {
    return {
      source: 'ai-analyst',
      v: 1,
      kind,
      reason,
      symbol: snapshot.symbol,
      price: snapshot.price,
      action: signal.action,
      valid: signal.valid,
      note: signal.note ?? null,
      rr: signal.rr,
      confidence: signal.confidence_score,
      model: signal.model,
      entry_price: signal.entry_price,
      stop_loss: signal.stop_loss,
      take_profit_1: signal.take_profit_1,
      take_profit_2: signal.take_profit_2,
      reasoning: signal.reasoning,
      position: this.position,
      asOf: signal.asOf,
      ts: Date.now(),
    };
  }

  #publishStatus(signal, snapshot, reason, kind) {
    const severity = signal.action === 'HOLD' ? 'info' : signal.valid ? 'success' : 'warning';

    // Plaintext by design: the trade rides the same public SSE connection as
    // the candles, so `meta` carries the levels and `message` is the human
    // card. There is no second, encrypted wire format to keep in sync.
    const meta = this.#signalPayload(signal, snapshot, kind, reason);
    const message = formatChat(signal);

    this.dispatcher.push({ type: CHAT_TYPES.AI_SIGNAL, message, severity, meta });
    this.#notifyCycle(signal, snapshot, kind).catch((e) => log.warn('AI Telegram notify failed:', e.message));
  }

  /**
   * Report a lifecycle event (stop-to-breakeven, take profit, reversal, stop)
   * on the chat feed and to Telegram.
   *
   * @param {'TP1'|'TP2'|'SL'|'REVERSAL'} kind
   */
  #emitTradeEvent(kind, payload, price, icon = '•') {
    const isClose = kind === 'SL' || kind === 'TP2' || kind === 'REVERSAL';
    const plain = isClose ? formatClose(payload) : formatProtect(payload, price);
    const body = {
      source: 'ai-analyst',
      v: 1,
      kind,
      symbol: payload.symbol,
      action: payload.action,
      entry_price: payload.entry,
      exit_price: isClose ? price : null,
      stop_loss: payload.sl,
      take_profit_1: payload.tp1,
      take_profit_2: payload.tp2,
      pnl_points: isClose ? payload.pnlPoints : null,
      pnl_pct: isClose ? payload.pnlPct : null,
      exit_reason: isClose ? kind : null,
      opened_at: payload.openedAt,
      closed_at: isClose ? payload.closedAt : null,
      ts: Date.now(),
    };

    const message = plain;
    const meta = { ...body, icon };

    this.dispatcher.push({
      type: isClose ? CHAT_TYPES.AI_TRADE_CLOSE : CHAT_TYPES.AI_TRADE_UPDATE,
      message,
      severity: kind === 'SL' ? 'warning' : 'success',
      meta,
    });

    if (!this.cfg.TELEGRAM) return;
    this.#sendTelegram(plain).catch((e) => log.warn('AI Telegram notify failed:', e.message));
  }

  /**
   * Publish a status for the trade already running WITHOUT calling the model.
   *
   * This is what makes a four-symbol book affordable on a free tier: once a
   * trade is open the levels are fixed, so the analyst only needs a fresh read
   * to look for a reversal — everything else is arithmetic on the live price.
   *
   * @param {string} reason
   * @returns {{ok:boolean, kind?:string, symbol?:string, error?:string}}
   */
  statusUpdate(reason = 'schedule') {
    const symbol = this.cfg.SYMBOL;
    const base = this.lastSignal ? { ...this.lastSignal } : null;
    const position = this.position;
    if (!base && !position) return { ok: false, error: 'nothing-to-report', symbol };

    const price = this.market.priceOf?.(symbol, 900) ?? position?.lastPrice ?? null;
    const signal = base ?? {
      action: position.action,
      symbol,
      valid: true,
      confidence_score: position.confidence ?? 'Low',
      reasoning: position.reasoning ?? '',
      entry_price: position.entry,
      stop_loss: position.sl,
      take_profit_1: position.tp1,
      take_profit_2: position.tp2,
      rr: position.rr ?? 0,
      model: position.model ?? null,
      asOf: new Date().toISOString(),
    };

    const kind = position ? 'management' : 'hold';
    this.#publishStatus({ ...signal, symbol }, { symbol, price }, reason, kind);
    return { ok: true, kind, symbol };
  }

  /**
   * Telegram for one cycle. A new trade is announced exactly once (de-duped on
   * its levels); every other cycle gets the status line that STATUS_EVERY_CYCLE
   * exists for — a quiet analyst and a dead analyst must not look alike.
   */
  async #notifyCycle(signal, snapshot, kind) {
    if (!this.cfg.TELEGRAM) return;

    if (kind === 'opened') {
      if (signal.action === 'HOLD' || !signal.valid) return;
      const key = `${signal.action}:${signal.entry_price}`;
      if (key === this.lastTelegramKey) return;
      const sent = await this.#sendTelegram(formatTelegram(signal));
      if (sent > 0) {
        this.lastTelegramKey = key;
        log.ok(`AI signal delivered to ${sent} Telegram chat(s)`);
      }
      return;
    }

    if (!this.cfg.STATUS_EVERY_CYCLE) return;
    const status = await this.#sendTelegram(
      formatStatus({ signal, position: this.position, price: snapshot.price, reason: kind })
    );
    if (status > 0) this.stats.statuses += 1;
  }

  /** One place that knows how a message actually leaves the process. */
  async #sendTelegram(text) {
    if (this.telegram?.notifyAll) return this.telegram.notifyAll(text);
    if (this.cfg.TELEGRAM_CHAT_ID) return sendDirect(this.cfg.TELEGRAM_CHAT_ID, text);
    return 0;
  }
}

/* ================================================================== helpers */

/** Provider defaults when a partial env is supplied (tests, embedded use). */
const DEFAULTS = Object.freeze({
  ENABLED: false,
  PROVIDER: 'groq',
  PROVIDER_LABEL: 'Groq',
  ENDPOINT: 'https://api.groq.com/openai/v1/chat/completions',
  API_KEY: '',
  API_KEYS: [],
  MODEL: 'openai/gpt-oss-120b',
  SYMBOL: 'XAUUSD',
  SYMBOLS: ['XAUUSD', 'EURUSD', 'USDJPY', 'GBPUSD'],
  STAGGER_MS: 12_000,
  RATE_LIMIT_FACTOR_MAX: 4,
  RATE_LIMIT_COOLDOWN_MS: 600_000,
  MANAGEMENT_EVERY: 2,
  INTERVAL_MS: 1_800_000,
  MANUAL_COOLDOWN_S: 60,
  BOOT_DELAY_MS: 120_000,
  TIMEOUT_MS: 90_000,
  // Groq counts input + max_tokens against the per-minute cap. 1500 is ample
  // for the JSON answer and leaves room under an 8k TPM free-tier limit.
  MAX_TOKENS: 1500,
  MIN_RR: 2,
  MAX_ENTRY_DRIFT: 0.02,
  // Oldest-first bars per frame. 210 rows keeps the whole request comfortably
  // below ~8k tokens once the epoch column is removed (see buildUserMessage).
  BARS: { D1: 40, H4: 40, H1: 50, M15: 40, M5: 40 },
  /** Hard character ceiling for the USER turn; the builder trims oldest bars. */
  MAX_PROMPT_CHARS: 12_000,
  TELEGRAM: false,
  TELEGRAM_CHAT_ID: '',
  MAX_ATTEMPTS: 2,
  MAX_CALLS_PER_CYCLE: 6,
  FALLBACK_MODELS: [],
  FALLBACK_MODEL: 'llama-3.1-8b-instant',
  CATCHUP_ENABLED: true,
  CATCHUP_DELAY_MS: 180_000,
  CATCHUP_MAX: 3,
  TRACK_ENABLED: true,
  STATUS_EVERY_CYCLE: true,
});

/** [epochSeconds, open, high, low, close] — compact to save prompt tokens. */
function compact(c) {
  return [Math.floor(c.time), round6(c.open), round6(c.high), round6(c.low), round6(c.close)];
}

const round6 = (n) => Math.round(Number(n) * 1e6) / 1e6;

function compactPrice(v) {
  return Number.isFinite(v) ? Number(v) : null;
}

/**
 * Build the USER turn: the snapshot as a small JSON document.
 *
 * Format choices, all driven by Groq's free-tier per-request token cap:
 *   • rows are [open, high, low, close] — the 10-digit epoch on every row was
 *     the single most expensive field and the model reads structure, not
 *     wall-clock labels;
 *   • each frame carries `first_bar_utc` + `interval_seconds`, so the window is
 *     still unambiguous for one timestamp per frame instead of one per row;
 *   • weekend gaps are already absent from the bars; `row_notes` explains the
 *     omission so the model does not read it as a data error;
 *   • `maxChars` trims the oldest bar from the longest frame until the payload
 *     fits. MIN_BARS per frame is preserved — a frame below that is not worth
 *     analysing anyway.
 *
 * @param {object} snapshot
 * @param {string[]} [news] upcoming high-impact events, as compact lines
 * @param {{maxChars?: number}} [opts]
 * @returns {string} JSON for the user turn
 */
export function buildUserMessage(snapshot, news = null, { maxChars = 0 } = {}) {
  /** Working copies keep the caller's snapshot untouched while we trim. */
  const frames = {};
  for (const [bucket, f] of Object.entries(snapshot.frames)) {
    frames[bucket] = {
      label: f.label,
      seconds: Number(f.seconds) || 0,
      bars: Array.isArray(f.bars) ? f.bars.map((b) => [...b]) : [],
    };
  }

  const events = Array.isArray(news) ? news.filter(Boolean) : [];
  const payload = {
    symbol: snapshot.symbol.replace(/^(.{3})(.{3})$/, '$1/$2'),
    as_of: new Date().toISOString(),
    current_price: snapshot.price,
    timeframes: {},
    row_format: ['open', 'high', 'low', 'close'],
    row_notes:
      'Rows are chronological (oldest first) as [open, high, low, close]. ' +
      'Weekend gaps are omitted. The last row of the shortest frame may be the forming candle.',
  };
  if (events.length) {
    payload.upcoming_high_impact_events_utc = events;
    payload.instruction =
      'Analyse the frames macro→micro. Treat the upcoming events above as a risk filter — ' +
      'do not open a trade straight into one. Return the strict JSON signal now.';
  } else {
    payload.instruction = 'Analyse the frames macro→micro and return the strict JSON signal now.';
  }

  const render = () => {
    for (const [bucket, f] of Object.entries(frames)) {
      const first = f.bars[0]?.[0];
      payload.timeframes[bucket] = {
        label: f.label,
        interval_seconds: f.seconds,
        first_bar_utc: Number.isFinite(first) ? new Date(first * 1000).toISOString() : null,
        rows: f.bars.map((b) => (b.length >= 5 ? [b[1], b[2], b[3], b[4]] : b.slice(-4))),
      };
    }
    return JSON.stringify(payload);
  };

  let text = render();
  if (maxChars > 0) {
    // Trim the frame that currently has the most rows, oldest first, until the
    // payload fits or every frame is down to the minimum usable window.
    let guard = 0;
    while (text.length > maxChars && guard < 500) {
      guard += 1;
      let target = null;
      for (const [bucket, f] of Object.entries(frames)) {
        if (f.bars.length <= MIN_BARS) continue;
        if (!target || f.bars.length > frames[target].bars.length) target = bucket;
      }
      if (!target) break;
      frames[target].bars.shift();
      text = render();
    }
  }
  return text;
}

/** One-line chat summary intended for the dashboard log. */
export function formatChat(signal) {
  const head = signal.action === 'HOLD' ? `🧠 AI ${signal.symbol} · HOLD` : `🧠 AI ${signal.symbol} · ${signal.action}`;
  if (signal.action === 'HOLD') {
    return `${head} — ${truncate(signal.reasoning, 220)}`;
  }
  const levels =
    `@ ${signal.entry_price} · SL ${signal.stop_loss} · TP1 ${signal.take_profit_1} · TP2 ${signal.take_profit_2}`;
  const rr = signal.rr ? ` · R:R 1:${signal.rr.toFixed(2)}` : '';
  const flag = signal.valid ? '' : ` ⚠ rejected: ${signal.note}`;
  return `${head} ${levels}${rr} · ${signal.confidence_score} confidence${flag}`;
}

/** HTML message for Telegram (escape first — the model's text is untrusted). */
export function formatTelegram(signal) {
  const body = [
    `🧠 <b>AI SIGNAL · ${esc(signal.symbol)}</b>`,
    `<b>${signal.action}</b> @ ${signal.entry_price}`,
    `SL ${signal.stop_loss} · TP1 ${signal.take_profit_1} · TP2 ${signal.take_profit_2}`,
    `R:R 1:${signal.rr.toFixed(2)} · Confidence: ${signal.confidence_score}`,
    '',
    esc(signal.reasoning),
  ];
  return body.join('\n');
}

const truncate = (s, n) => (s.length > n ? `${s.slice(0, n - 1)}…` : s);
const esc = (s) =>
  String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

/**
 * The every-cycle Telegram status: the trade being followed, or the reason for
 * standing aside. Deliberately one compact message — this lands ~48×/day.
 *
 * @param {{signal:object, position:object|null, price:number, reason:string}} ctx
 */
export function formatStatus({ signal, position, price, reason }) {
  const symbol = esc((position ?? signal)?.symbol ?? 'XAUUSD');
  const head = `🧠 <b>AI STATUS · ${symbol}</b>`;

  if (position) {
    const dir = position.action === 'BUY' ? 1 : -1;
    const ref = Number.isFinite(price) ? price : position.lastPrice ?? position.entry;
    const pts = (ref - position.entry) * dir;
    const pct = (pts / position.entry) * 100;
    const sign = pts >= 0 ? '+' : '';
    const mins = Math.max(0, Math.round((Date.now() - position.openedMs) / 60_000));
    const state =
      signal && signal.valid && signal.action === position.action
        ? 'setup still valid'
        : signal && signal.action !== position.action
          ? 'opposite idea forming'
          : 'following';

    return [
      head,
      `🔄 Following <b>${position.action}</b> @ ${position.entry}`,
      `Price ${ref} · ${sign}${pts.toFixed(2)} pts (${sign}${pct.toFixed(2)}%) · ${mins}m open`,
      `SL ${position.sl} · TP1 ${position.tp1} · TP2 ${position.tp2}${
        position.stage === 'tp1' ? ' · SL at breakeven' : ''
      }`,
      `State: ${state}`,
    ].join('\n');
  }

  const action = signal?.action ?? 'HOLD';
  const conf = signal?.confidence_score ? ` (${signal.confidence_score})` : '';
  const why = signal?.reasoning ? esc(truncate(signal.reasoning, 240)) : 'Waiting for an A-grade setup.';
  const note = action === 'HOLD' ? '' : signal?.valid === false ? ` ⚠ ${esc(signal.note ?? 'rejected')}` : '';
  return [head, `⏸️ Flat — <b>${action}</b>${conf}${note}`, why, `Cycle: ${reason}`].join('\n');
}

/** Telegram/chat message for a closed trade. */
export function formatClose(closed) {
  const icon = closed.exitReason === 'SL' ? '🛑' : closed.exitReason === 'REVERSAL' ? '↩️' : '✅';
  const label =
    closed.exitReason === 'SL' ? 'STOP LOSS' : closed.exitReason === 'REVERSAL' ? 'REVERSED OUT' : 'TAKE PROFIT';
  const sign = closed.pnlPoints >= 0 ? '+' : '';
  const mins = Math.max(0, Math.round((closed.closedMs - closed.openedMs) / 60_000));

  const body = [
    `${icon} <b>AI TRADE CLOSED · ${label}</b>`,
    `<b>${closed.action}</b> ${esc(closed.symbol)} @ ${closed.entry} → ${closed.exitPrice}`,
    `Result: ${sign}${closed.pnlPoints.toFixed(2)} pts (${sign}${closed.pnlPct.toFixed(2)}%) · ${mins}m`,
  ];
  if (closed.exitNote) body.push('', esc(truncate(closed.exitNote, 200)));
  return body.join('\n');
}

/** Telegram/chat message for the TP1 stop-to-breakeven action. */
export function formatProtect(position, price) {
  return [
    '🛡️ <b>TP1 REACHED · STOP → BREAKEVEN</b>',
    `<b>${position.action}</b> ${esc(position.symbol)} @ ${position.entry} · price ${price}`,
    `Stop moved to ${position.entry} — the trade can no longer lose.`,
  ].join('\n');
}

/** Direct send used only when no gateway is attached but a chat id is set. */
async function sendDirect(chatId, text) {
  const token = process.env.TELEGRAM_BOT_TOKEN;
  if (!token) return 0;
  try {
    const res = await fetch(`https://api.telegram.org/bot${token}/sendMessage`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ chat_id: chatId, text, parse_mode: 'HTML', disable_web_page_preview: true }),
    });
    return res.ok ? 1 : 0;
  } catch {
    return 0;
  }
}

export default AiSignalService;
