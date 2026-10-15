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
   */
  constructor({ market, dispatcher, env = null, telegram = null, fetchFn = null } = {}) {
    if (!market) throw new Error('AiSignalService requires a market aggregator');
    if (!dispatcher) throw new Error('AiSignalService requires an SSE dispatcher');

    this.market = market;
    this.dispatcher = dispatcher;
    this.env = env ?? market.env ?? null;
    // `env.ai` may be missing when a caller passes a partial env (tests): fall
    // back to a safe defaults object so the service never throws on boot.
    this.cfg = (this.env && this.env.ai) || DEFAULTS;
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
      lastRunAt: null,
      lastError: null,
      lastModel: null,
      lastLatencyMs: null,
    };
    /** The most recent validated signal, replayed to new readers. */
    this.lastSignal = null;
    /** De-dupe key so an unchanged read does not spam Telegram. */
    this.lastTelegramKey = null;
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
    this.bootTimer = null;
    this.intervalTimer = null;
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
      ...this.stats,
      lastSignal: this.lastSignal,
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

      let call = await this.#callWithFallback(snapshot);
      let parsed = this.#parseJson(call.content);

      if (!parsed.ok) {
        // The HTTP call succeeded but the model answered with prose instead of
        // the contract — there is no JSON object for the parser to salvage.
        // Retry once with an explicit correction (and extra headroom, in case
        // the preamble ate the token budget) rather than losing the interval.
        const retryModel = this.cfg.MODEL === FALLBACK_MODEL ? call.model : FALLBACK_MODEL;
        log.warn(
          `AI analyst: reply was not JSON (${parsed.error}; finish_reason=${call.finishReason ?? 'unknown'}) ` +
            `— retrying once on "${retryModel}" with a JSON-only correction`
        );
        this.stats.fallbacks = (this.stats.fallbacks ?? 0) + 1;
        try {
          call = await this.#askModel(snapshot, retryModel, Math.max(this.cfg.MAX_TOKENS, 4_000), {
            correction: JSON_CORRECTION,
          });
          parsed = this.#parseJson(call.content);
        } catch (e) {
          log.warn('AI analyst: correction retry failed —', e.message);
        }
      }

      if (!parsed.ok) {
        this.stats.errors += 1;
        this.stats.lastError = `parse: ${parsed.error}`;
        log.warn('AI analyst: model returned unparseable JSON —', parsed.error);
        return { ok: false, error: 'bad-model-output', reason, detail: parsed.error };
      }

      const signal = this.#validate(parsed.value, snapshot);
      // The turn may have been served by the fallback model — report the model
      // that actually answered, not the one that was configured.
      signal.model = call.model;
      const latency = Date.now() - startedAt;

      this.stats.runs += 1;
      this.stats.lastRunAt = Date.now();
      this.stats.lastLatencyMs = latency;
      this.stats.lastModel = call.model;
      this.lastSignal = signal;

      if (signal.action === 'HOLD') this.stats.holds += 1;
      else this.stats.published += 1;
      if (!signal.valid) this.stats.rejected += 1;

      this.#publish(signal, snapshot, reason);
      log.info(
        `AI analyst → ${signal.action}` +
          (signal.action !== 'HOLD' ? ` @ ${signal.entry_price} (R:R 1:${signal.rr.toFixed(2)})` : '') +
          ` · ${latency}ms${signal.valid ? '' : ' · downgraded: ' + signal.note}`
      );
      return { ok: true, reason, signal };
    } catch (e) {
      this.stats.runs += 1;
      this.stats.errors += 1;
      this.stats.lastError = e.message;
      this.stats.lastRunAt = Date.now();
      log.error('AI analyst run failed:', e.message);
      return { ok: false, error: 'exception', reason, detail: e.message };
    } finally {
      this.running = false;
    }
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

  async #askModel(snapshot, model = this.cfg.MODEL, maxTokens = this.cfg.MAX_TOKENS, { correction = null } = {}) {
    const user = buildUserMessage(snapshot);
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
   * Ask the configured model; if that yields nothing usable, fall back once to
   * the generic free router with extra headroom. Free models are flaky and
   * reasoning variants often burn their token budget on thought and return an
   * empty `content` — a single, well-scoped retry keeps the analyst alive
   * instead of reporting a hard error to the operator.
   *
   * @returns {Promise<{content:string, model:string}>}
   */
  async #callWithFallback(snapshot) {
    try {
      return await this.#askModel(snapshot);
    } catch (primaryErr) {
      const fallback = FALLBACK_MODEL;
      if (this.cfg.MODEL === fallback) throw primaryErr;
      log.warn(`AI analyst: ${primaryErr.message} — retrying once on "${fallback}"`);
      this.stats.fallbacks = (this.stats.fallbacks ?? 0) + 1;
      // Extra headroom: the common empty-completion cause is a reasoning model
      // exhausting max_tokens before it emits the answer.
      return await this.#askModel(snapshot, fallback, Math.max(this.cfg.MAX_TOKENS, 8_000));
    }
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

  #publish(signal, snapshot, reason) {
    const chat = formatChat(signal);
    const severity = signal.action === 'HOLD' ? 'info' : signal.valid ? 'success' : 'warning';

    this.dispatcher.push({
      type: CHAT_TYPES.AI_SIGNAL,
      message: chat,
      severity,
      meta: {
        source: 'ai-analyst',
        reason,
        symbol: snapshot.symbol,
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
        asOf: signal.asOf,
      },
    });

    this.#notifyTelegram(signal).catch((e) => log.warn('AI Telegram notify failed:', e.message));
  }

  /**
   * Send the signal to Telegram. Only actionable, validated signals are sent —
   * a HOLD would otherwise wake the desk every cycle for nothing. An unchanged
   * signal is also suppressed so a flat market does not spam.
   */
  async #notifyTelegram(signal) {
    if (!this.cfg.TELEGRAM) return;
    if (signal.action === 'HOLD' || !signal.valid) return;

    const key = `${signal.action}:${signal.entry_price}`;
    if (key === this.lastTelegramKey) return;

    const text = formatTelegram(signal);
    let sent = 0;

    if (this.telegram?.notifyAll) {
      sent = await this.telegram.notifyAll(text);
    } else if (this.cfg.TELEGRAM_CHAT_ID) {
      sent = await sendDirect(this.cfg.TELEGRAM_CHAT_ID, text);
    }

    if (sent > 0) {
      this.lastTelegramKey = key;
      log.ok(`AI signal delivered to ${sent} Telegram chat(s)`);
    }
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
  TIMEOUT_MS: 60_000,
  MAX_TOKENS: 2000,
  MIN_RR: 2,
  MAX_ENTRY_DRIFT: 0.02,
  BARS: { D1: 60, H4: 60, H1: 80, M15: 60, M5: 60 },
  TELEGRAM: false,
  TELEGRAM_CHAT_ID: '',
});

/** [epochSeconds, open, high, low, close] — compact to save prompt tokens. */
function compact(c) {
  return [Math.floor(c.time), round6(c.open), round6(c.high), round6(c.low), round6(c.close)];
}

const round6 = (n) => Math.round(Number(n) * 1e6) / 1e6;

function compactPrice(v) {
  return Number.isFinite(v) ? Number(v) : null;
}

/** Build the USER turn: the snapshot as a small JSON document. */
export function buildUserMessage(snapshot) {
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
    instruction: 'Analyse the frames macro→micro and return the strict JSON signal now.',
  };
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
