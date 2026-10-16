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
 *    2. Sends them to an OpenRouter model with a strict-JSON system prompt.
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

const log = createLogger('ai');

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
 * The generic OpenRouter free-model router. Used once, as a fallback, whenever
 * the configured model returns an unusable (typically empty) completion — free
 * models rotate and reasoning variants sometimes burn their whole token budget
 * on thought without emitting an answer. One retry keeps the analyst alive.
 */
export const FALLBACK_MODEL = 'openrouter/free';

/**
 * Appended as an extra user turn when a model answers with prose instead of the
 * JSON contract. Free routers (openrouter/free) pick a model at random, and some
 * open with "Here's the setup…" and never emit an object at all — the parser
 * then has nothing to salvage. One explicit correction recovers the cycle
 * instead of burning a whole interval.
 */
export const JSON_CORRECTION =
  'Your previous reply was not valid JSON. Reply with ONLY the JSON object described ' +
  'in the schema — no prose, no markdown fences, no explanation before or after it. ' +
  'The reply must start with "{" and end with "}".';

/**
 * Pull the answer out of a chat message. OpenRouter keeps the final text in
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
   */
  constructor({ market, dispatcher, env = null, telegram = null, fetchFn = null, news = null } = {}) {
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

    this.enabled = Boolean(this.cfg.ENABLED && this.cfg.OPENROUTER_API_KEY);

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

  /** Schedule the automatic loop. No-op when disabled or unconfigured. */
  start() {
    if (!this.enabled) {
      log.info(
        this.cfg.OPENROUTER_API_KEY
          ? 'AI analyst disabled (AI_SIGNAL_ENABLED=false)'
          : 'AI analyst idle — OPENROUTER_API_KEY not set'
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
      configured: Boolean(this.cfg.OPENROUTER_API_KEY),
      running: this.running,
      symbol: this.cfg.SYMBOL,
      model: this.cfg.MODEL,
      intervalMs: this.cfg.INTERVAL_MS,
      bootDelayMs: this.cfg.BOOT_DELAY_MS,
      maxTokens: this.cfg.MAX_TOKENS,
      minRR: this.cfg.MIN_RR,
      telegram: Boolean(this.cfg.TELEGRAM && (this.telegram || this.cfg.TELEGRAM_CHAT_ID)),
      trackEnabled: Boolean(this.cfg.TRACK_ENABLED),
      statusEveryCycle: Boolean(this.cfg.STATUS_EVERY_CYCLE),
      ...this.stats,
      catchupsUsed: this.catchupsUsed,
      lastSignal: this.lastSignal,
      position: this.position,
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

      const attempt = await this.#askWithRetries(snapshot, newsLines);

      if (!attempt.parsed.ok) {
        this.stats.errors += 1;
        this.stats.lastError = `parse: ${attempt.parsed.error}`;
        log.warn(`AI analyst: no usable reply after ${attempt.attempts} attempt(s) —`, attempt.parsed.error);
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
      const limit = this.cfg.BARS?.[f.bucket] ?? 60;
      const closed = this.market.closedCandles(symbol, f.seconds);
      const forming = this.market.formingCandle(symbol, f.seconds);
      const bars = closed.slice(-limit).map(compact);
      if (forming && Number.isFinite(forming.close)) bars.push(compact(forming));
      frames[f.bucket] = { label: f.label, bars };
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

  async #askModel(snapshot, model = this.cfg.MODEL, maxTokens = this.cfg.MAX_TOKENS, { correction = null, news = [] } = {}) {
    const user = buildUserMessage(snapshot, news);
    const messages = [
      { role: 'system', content: SYSTEM_PROMPT },
      { role: 'user', content: user },
    ];
    // A reply we could not parse gets one follow-up turn spelling out the
    // contract; models that add a preamble usually comply on the second ask.
    if (correction) messages.push({ role: 'user', content: correction });
    const body = {
      model,
      temperature: 0.2,
      max_tokens: maxTokens,
      // Best-effort JSON mode. Free models may ignore it; the parser is strict
      // either way, so an ignored flag degrades gracefully.
      response_format: { type: 'json_object' },
      messages,
    };

    const res = await this.fetchFn('https://openrouter.ai/api/v1/chat/completions', {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${this.cfg.OPENROUTER_API_KEY}`,
        'Content-Type': 'application/json',
        // OpenRouter attribution headers — optional, but they help the free
        // tier identify the caller.
        'HTTP-Referer': 'https://github.com/hasan-mo111/paper-trading-streaming-engine',
        'X-Title': 'Aetheron Core AI Analyst',
      },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(this.cfg.TIMEOUT_MS),
    });

    const text = await res.text();
    let json = null;
    try {
      json = JSON.parse(text);
    } catch {
      /* fall through to the error below */
    }

    if (!res.ok) {
      const detail = json?.error?.message ?? text.slice(0, 200) ?? res.status;
      throw new Error(`OpenRouter ${res.status}: ${detail}`);
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
   * Ask the model chain until one reply parses, correcting a prose answer and
   * escalating models on failure.
   *
   * Free routers are flaky in two different ways, and they need different
   * responses:
   *   • the call itself fails (timeout, 429, empty completion) → try the next
   *     model in the chain, with extra headroom for reasoning variants;
   *   • the call succeeds but answers in prose → re-ask with the JSON-only
   *     correction turn. Re-asking the same model is what actually recovers a
   *     chatty reply, so a repeat model is deliberate, not a bug.
   *
   * @returns {Promise<{call:object, parsed:object, attempts:number}>}
   */
  async #askWithRetries(snapshot, newsLines = []) {
    const chain = this.#modelChain();
    const maxAttempts = Math.max(1, this.cfg.MAX_ATTEMPTS);
    let call = { content: '', model: chain[0] };
    let parsed = { ok: false, error: 'no attempt made' };

    for (let i = 0; i < maxAttempts; i++) {
      const model = chain[Math.min(i, chain.length - 1)];
      // Attempt 0 asks cleanly; every later attempt spells out the contract and
      // buys extra headroom in case a preamble ate the token budget.
      const correction = i === 0 ? null : JSON_CORRECTION;
      const maxTokens = i === 0 ? this.cfg.MAX_TOKENS : Math.max(this.cfg.MAX_TOKENS, 4_000);

      try {
        call = await this.#askModel(snapshot, model, maxTokens, { correction, news: newsLines });
        parsed = this.#parseJson(call.content);
        if (parsed.ok) return { call, parsed, attempts: i + 1 };
        log.warn(
          `AI analyst: attempt ${i + 1}/${maxAttempts} on "${model}" was not JSON ` +
            `(${parsed.error}; finish_reason=${call.finishReason ?? 'unknown'})`
        );
      } catch (e) {
        log.warn(`AI analyst: attempt ${i + 1}/${maxAttempts} on "${model}" failed — ${e.message}`);
        parsed = { ok: false, error: e.message };
      }

      if (i > 0) this.stats.fallbacks += 1;
    }
    return { call, parsed, attempts: maxAttempts };
  }

  /**
   * Ordered list of models to try: the configured primary, then any explicitly
   * configured fallbacks, then the generic free router. Deduplicated so the
   * primary is never tried twice in a row by accident.
   */
  #modelChain() {
    const extra = Array.isArray(this.cfg.FALLBACK_MODELS) ? this.cfg.FALLBACK_MODELS : [];
    const chain = [this.cfg.MODEL, ...extra, FALLBACK_MODEL].filter(Boolean);
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
  #publishStatus(signal, snapshot, reason, kind) {
    const chat = formatChat(signal);
    const severity = signal.action === 'HOLD' ? 'info' : signal.valid ? 'success' : 'warning';

    this.dispatcher.push({
      type: CHAT_TYPES.AI_SIGNAL,
      message: chat,
      severity,
      meta: {
        source: 'ai-analyst',
        reason,
        kind,
        symbol: snapshot.symbol,
        price: snapshot.price,
        action: signal.action,
        valid: signal.valid,
        note: signal.note,
        rr: signal.rr,
        confidence: signal.confidence_score,
        model: signal.model,
        entry_price: signal.entry_price,
        stop_loss: signal.stop_loss,
        take_profit_1: signal.take_profit_1,
        take_profit_2: signal.take_profit_2,
        // Carried on the frame so a client (the mobile app) can render the whole
        // trade — levels and rationale — from the event alone, without a
        // follow-up read. Purely additive; the message text is unchanged.
        reasoning: signal.reasoning,
        // The trade currently being followed, so a client can show management
        // state without replaying the event history. null while flat.
        position: this.position,
        asOf: signal.asOf,
      },
    });

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
    const message = isClose ? formatClose(payload) : formatProtect(payload, price);

    this.dispatcher.push({
      type: isClose ? CHAT_TYPES.AI_TRADE_CLOSE : CHAT_TYPES.AI_TRADE_UPDATE,
      message,
      severity: kind === 'SL' ? 'warning' : 'success',
      meta: {
        source: 'ai-analyst',
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
        icon,
      },
    });

    if (!this.cfg.TELEGRAM) return;
    this.#sendTelegram(message).catch((e) => log.warn('AI Telegram notify failed:', e.message));
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

/** OpenRouter default when a partial env is supplied (tests). */
const DEFAULTS = Object.freeze({
  ENABLED: false,
  OPENROUTER_API_KEY: '',
  MODEL: 'openrouter/free',
  SYMBOL: 'XAUUSD',
  INTERVAL_MS: 1_800_000,
  MANUAL_COOLDOWN_S: 60,
  BOOT_DELAY_MS: 120_000,
  TIMEOUT_MS: 90_000,
  MAX_TOKENS: 2000,
  MIN_RR: 2,
  MAX_ENTRY_DRIFT: 0.02,
  BARS: { D1: 60, H4: 60, H1: 80, M15: 60, M5: 60 },
  TELEGRAM: false,
  TELEGRAM_CHAT_ID: '',
  MAX_ATTEMPTS: 3,
  FALLBACK_MODELS: [],
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
 * @param {object} snapshot
 * @param {string[]} [news] upcoming high-impact events, as compact lines
 */
export function buildUserMessage(snapshot, news = null) {
  const tf = {};
  for (const [bucket, f] of Object.entries(snapshot.frames)) {
    tf[bucket] = { label: f.label, rows: f.bars };
  }
  const payload = {
    symbol: snapshot.symbol.replace(/^(.{3})(.{3})$/, '$1/$2'),
    as_of: new Date().toISOString(),
    current_price: snapshot.price,
    timeframes: tf,
    row_format: ['epoch_seconds', 'open', 'high', 'low', 'close'],
  };

  const events = Array.isArray(news) ? news.filter(Boolean) : [];
  if (events.length) {
    payload.upcoming_high_impact_events_utc = events;
    payload.instruction =
      'Analyse the frames macro→micro. Treat the upcoming events above as a risk filter — ' +
      'do not open a trade straight into one. Return the strict JSON signal now.';
  } else {
    payload.instruction = 'Analyse the frames macro→micro and return the strict JSON signal now.';
  }
  return JSON.stringify(payload);
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
