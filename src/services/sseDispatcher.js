/* =============================================================================
 *  AETHERON CORE  —  src/services/sseDispatcher.js
 *  -----------------------------------------------------------------------------
 *  Owns the two production streams the Android client consumes.
 *
 *  ┌─ GET /api/stream/prices ───────────────────────────────────────────────┐
 *  │  Clean, chart-ready price data. One frame per flush tick carrying the  │
 *  │  latest bid/ask/mid per symbol plus the live candle — and nothing else.│
 *  │  No chat text, no positions, no account data. Cheap enough for a       │
 *  │  phone on a mobile network.                                            │
 *  └────────────────────────────────────────────────────────────────────────┘
 *
 *  ┌─ GET /api/stream/chat-events ──────────────────────────────────────────┐
 *  │  Chat-Ops: execution logs, trade confirmations, signal alerts and      │
 *  │  system health, formatted as JSON chat messages:                        │
 *  │      { "type": "EXECUTION",                                            │
 *  │        "message": "XAUUSD Buy 0.05 executed at 2750.50",                │
 *  │        "timestamp": "2026-01-01T00:00:00.000Z", … }                     │
 *  │  A reconnecting client is replayed the last CHAT_BACKLOG messages, so   │
 *  │  a phone that slept through a trade still sees it.                      │
 *  └────────────────────────────────────────────────────────────────────────┘
 *
 *  COALESCING
 *  ----------
 *  Ticks arrive many per second; clients need a few per second. Prices are
 *  therefore buffered per symbol and flushed once every STREAM_INTERVAL_MS,
 *  so however fast the feed fires, a client sees at most one frame per
 *  interval. Chat events are never coalesced — a confirmation is rare and
 *  losing one is not acceptable.
 * ============================================================================= */

import { randomUUID } from 'node:crypto';
import { ENV } from '../config/env.js';
import { SYMBOLS, getSymbolSpec, normaliseSymbol, sideLabel } from '../config/db.js';
import { createLogger } from './logger.js';

const log = createLogger('sse');

/** Chat message taxonomy. The Android client switches on `type`. */
export const CHAT_TYPES = Object.freeze({
  EXECUTION: 'EXECUTION',
  SIGNAL: 'SIGNAL',
  SIGNAL_ONLY: 'SIGNAL_ONLY',
  PARTIAL_CLOSE: 'PARTIAL_CLOSE',
  STOP_HIT: 'STOP_HIT',
  TRAILING: 'TRAILING',
  QUOTA: 'QUOTA',
  SYSTEM: 'SYSTEM',
  HEALTH: 'HEALTH',
  RISK: 'RISK',
  // A multi-timeframe read produced by the AI analyst. Additive: clients that
  // do not know it simply render the message text like any other chat line.
  AI_SIGNAL: 'AI_SIGNAL',
  // Lifecycle of the single trade the analyst follows after an AI_SIGNAL: a
  // stop-to-breakeven/partial action, and the final close. Also additive.
  AI_TRADE_UPDATE: 'AI_TRADE_UPDATE',
  AI_TRADE_CLOSE: 'AI_TRADE_CLOSE',
});

export class SSEDispatcher {
  /**
   * @param {object} opts
   * @param {SSEHub} opts.hub
   * @param {object} [opts.env]
   * @param {SupabaseService} [opts.persistence] chat history persistence
   */
  constructor({ hub, env = ENV, persistence = null, market = null } = {}) {
    this.hub = hub;
    this.env = env;
    this.persistence = persistence;
    /**
     * The aggregator, attached after construction (it depends on this
     * dispatcher for its tick hook). Needed to fold the live candle into price
     * frames, which the chart client needs for its current bar.
     */
    this.market = market;

    /** symbol → latest normalised tick, awaiting the next flush. */
    this.pendingPrices = new Map();
    /** In-RAM ring of chat messages for reconnect replay. */
    this.chatBacklog = [];
    this.backlogMax = env.stream.CHAT_BACKLOG;

    this.timers = [];
    this.counters = {
      priceFrames: 0,
      priceItems: 0,
      chatFrames: 0,
      healthFrames: 0,
      droppedSlowClients: 0,
    };
  }

  /* ======================================================================== */
  /*  LIFECYCLE                                                              */
  /* ======================================================================== */

  start() {
    this.stop();
    // `this.market` is passed explicitly: the interval callback must enrich each
    // frame with the live candle, otherwise chart clients never get a current bar.
    const priceTimer = setInterval(() => this.flushPrices(this.market), this.env.stream.INTERVAL_MS);
    const healthTimer = setInterval(() => this.pushHealth(), this.env.stream.STATUS_INTERVAL_MS);
    const heartbeatTimer = setInterval(() => this.hub.heartbeat(), this.env.stream.HEARTBEAT_MS);
    for (const t of [priceTimer, healthTimer, heartbeatTimer]) {
      t.unref?.();
      this.timers.push(t);
    }
  }

  stop() {
    for (const t of this.timers) clearInterval(t);
    this.timers = [];
  }

  /**
   * Wire the aggregator in after both objects exist.
   * (`createApplication` calls this once the market layer is built.)
   */
  attachMarket(market) {
    this.market = market;
  }

  /* ======================================================================== */
  /*  PRICES CHANNEL                                                          */
  /* ======================================================================== */

  /**
   * Accept a live tick. Called once per tick per symbol by the market
   * aggregator — deliberately not awaiting anything.
   */
  onTick(symbol, tick) {
    const sym = normaliseSymbol(symbol);
    if (!sym) return;

    const spec = getSymbolSpec(sym);
    const bid = Number(tick.bid);
    const ask = Number(tick.ask);
    const mid = Number(tick.mid ?? tick.ask ?? tick.bid);
    if (!Number.isFinite(mid)) return;

    this.pendingPrices.set(sym, {
      symbol: sym,
      display: spec.display,
      bid: Number.isFinite(bid) ? bid : mid,
      ask: Number.isFinite(ask) ? ask : mid,
      mid,
      spread: Number.isFinite(bid) && Number.isFinite(ask) ? Number((ask - bid).toFixed(spec.digits)) : null,
      digits: spec.digits,
      ts: tick.timestamp ?? new Date().toISOString(),
      direction: tick.direction ?? null,
    });
  }

  /** Fold the live candle into a price item (optional enrichment). */
  attachCandle(item, market, interval = 300) {
    const candle = market?.formingCandle(item.symbol, interval);
    if (!candle) return item;
    return {
      ...item,
      candle: {
        time: candle.time,
        open: candle.open,
        high: candle.high,
        low: candle.low,
        close: candle.close,
        tickVolume: candle.tickVolume,
      },
      interval,
    };
  }

  /**
   * Emit one coalesced price frame.
   * @param {import('./marketAggregator.js').MarketAggregator} [market]
   */
  flushPrices(market = null) {
    if (this.pendingPrices.size === 0) return null;
    if (this.hub.countFor('prices') === 0) {
      // Nobody is listening — drop the frame rather than formatting it.
      this.pendingPrices.clear();
      return null;
    }

    const items = [...this.pendingPrices.values()].map((i) => (market ? this.attachCandle(i, market) : i));
    this.pendingPrices.clear();

    const payload = {
      type: 'prices',
      serverTime: new Date().toISOString(),
      count: items.length,
      items,
    };
    // `requiresFilter` lets a client that asked for one symbol receive only that
    // symbol, instead of all four.
    this.hub.publish('prices', 'price', payload, { requiresFilter: true });
    this.counters.priceFrames += 1;
    this.counters.priceItems += items.length;
    return payload;
  }

  /** Current price map without emitting anything (for REST polls). */
  priceSnapshot() {
    return {
      type: 'prices',
      serverTime: new Date().toISOString(),
      count: this.pendingPrices.size,
      items: [...this.pendingPrices.values()],
    };
  }

  /* ======================================================================== */
  /*  CHAT CHANNEL                                                            */
  /* ======================================================================== */

  /**
   * Publish a chat-ops message.
   *
   * @param {object} opts
   * @param {string} opts.type      one of CHAT_TYPES
   * @param {string} opts.message   human-readable line for the app
   * @param {object} [opts.meta]    structured payload (symbol, side, volume…)
   * @param {string} [opts.userId]  scope to one authenticated user
   * @param {string} [opts.severity] 'info' | 'success' | 'warning' | 'error'
   * @param {boolean} [opts.persist]
   */
  push({ type, message, meta = {}, userId = null, severity = 'info', persist = true }) {
    if (!type) throw new Error('chat message requires a type');
    const text = String(message ?? '').trim();
    if (!text) return null;

    const event = {
      id: randomUUID(),
      type,
      severity,
      message: text,
      timestamp: new Date().toISOString(),
      meta,
      ...(userId ? { userId } : {}),
    };

    // Ring buffer for reconnect replay.
    this.chatBacklog.push(event);
    if (this.chatBacklog.length > this.backlogMax) this.chatBacklog.shift();

    // Broadcast on the public channel and, when scoped, on the user's private
    // channel too. A user on the private stream sees exactly their own events.
    this.hub.publish('chat', 'message', event);
    if (userId) this.hub.publish(`chat:${userId}`, 'message', event);
    this.counters.chatFrames += 1;

    // Persist asynchronously — a slow INSERT must never delay a trade event.
    if (persist && this.persistence) {
      Promise.resolve(
        this.persistence.recordChatEvent({
          id: event.id,
          user_id: userId,
          type,
          severity,
          message: text,
          meta,
          created_at: event.timestamp,
        })
      ).catch((e) => log.error('recordChatEvent failed:', e.message));
    }

    return event;
  }

  /* ---- typed shorthands used across the codebase ------------------------- */

  system(message, meta) {
    return this.push({ type: CHAT_TYPES.SYSTEM, message, meta, severity: 'info' });
  }

  risk(message, meta) {
    return this.push({ type: CHAT_TYPES.RISK, message, meta, severity: 'warning' });
  }

  quota(message, meta) {
    return this.push({ type: CHAT_TYPES.QUOTA, message, meta, severity: 'warning' });
  }

  /** Order filled. `payload` comes from the broker result. */
  execution(payload) {
    const { symbol, side, volume, entryPrice, positionId, mode } = payload;
    const spec = getSymbolSpec(symbol);
    const digits = spec?.digits ?? 2;
    const message =
      `${spec?.display ?? symbol} ${sideLabel(side)} ${volume} executed at ` +
      `${Number(entryPrice).toFixed(digits)}` +
      (positionId ? ` · ticket ${positionId}` : '') +
      (mode === 'paper' ? ' · paper' : '');

    return this.push({
      type: CHAT_TYPES.EXECUTION,
      message,
      severity: 'success',
      userId: payload.userId ?? null,
      meta: { ...payload, entryPrice: Number(Number(entryPrice).toFixed(digits)) },
    });
  }

  /** A signal that was NOT executed (FREE quota exhausted). */
  signalOnly(payload) {
    const { symbol, side, volume, reason } = payload;
    const spec = getSymbolSpec(symbol);
    const message =
      `${spec?.display ?? symbol} ${sideLabel(side)} ${volume} — SIGNAL ONLY, not executed` +
      (reason ? ` (${reason})` : '');
    return this.push({
      type: CHAT_TYPES.SIGNAL_ONLY,
      message,
      severity: 'warning',
      userId: payload.userId ?? null,
      meta: { ...payload, downgrade: 'FREE daily execution limit reached' },
    });
  }

  /** Raw strategy signal, before the quota decision. */
  signal(payload) {
    const { symbol, side, volume, entryPrice, atr, donchianHigh, donchianLow } = payload;
    const spec = getSymbolSpec(symbol);
    const digits = spec?.digits ?? 2;
    const parts = [`${spec?.display ?? symbol} ${String(side).toUpperCase()} signal`, `ATR(20) ${Number(atr ?? 0).toFixed(digits)}`];
    if (side === 'LONG' && Number.isFinite(donchianHigh)) parts.push(`broke 50d high ${Number(donchianHigh).toFixed(digits)}`);
    if (side === 'SHORT' && Number.isFinite(donchianLow)) parts.push(`broke 50d low ${Number(donchianLow).toFixed(digits)}`);
    parts.push(`sized ${volume}`);

    return this.push({
      type: CHAT_TYPES.SIGNAL,
      message: parts.join(' · '),
      severity: 'info',
      userId: payload.userId ?? null,
      meta: payload,
    });
  }

  /** Leg 1 / Leg 2 closed. */
  partialClose(payload) {
    const { symbol, side, volume, exitPrice, leg, profit, reason } = payload;
    const spec = getSymbolSpec(symbol);
    const digits = spec?.digits ?? 2;
    const message =
      `${spec?.display ?? symbol} ${sideLabel(side)} leg ${leg} closed: ` +
      `${volume} @ ${Number(exitPrice).toFixed(digits)}` +
      (Number.isFinite(profit) ? ` · ${profit >= 0 ? '+' : ''}${profit.toFixed(2)} USD` : '') +
      (reason ? ` · ${reason}` : '');
    return this.push({
      type: CHAT_TYPES.PARTIAL_CLOSE,
      message,
      severity: Number(profit ?? 0) >= 0 ? 'success' : 'warning',
      userId: payload.userId ?? null,
      meta: { ...payload, leg },
    });
  }

  /** Trailing stop moved. */
  trailing(payload) {
    const { symbol, side, sl, distance } = payload;
    const spec = getSymbolSpec(symbol);
    const digits = spec?.digits ?? 2;
    const message =
      `${spec?.display ?? symbol} ${sideLabel(side)} trailing stop → ` +
      `${Number(sl).toFixed(digits)}` + (Number.isFinite(distance) ? ` (${distance.toFixed(digits)} away)` : '');
    // Trailing updates are frequent — keep them out of the persistent history.
    return this.push({ type: CHAT_TYPES.TRAILING, message, severity: 'info', userId: payload.userId ?? null, meta: payload, persist: false });
  }

  /** Stop loss hit. */
  stopHit(payload) {
    const { symbol, side, volume, exitPrice, profit, leg } = payload;
    const spec = getSymbolSpec(symbol);
    const digits = spec?.digits ?? 2;
    const message =
      `${spec?.display ?? symbol} ${sideLabel(side)} stop hit — ` +
      `${volume} closed @ ${Number(exitPrice).toFixed(digits)}` +
      (Number.isFinite(profit) ? ` · ${profit >= 0 ? '+' : ''}${profit.toFixed(2)} USD` : '');
    return this.push({
      type: CHAT_TYPES.STOP_HIT,
      message,
      severity: 'warning',
      userId: payload.userId ?? null,
      meta: { ...payload, leg },
    });
  }

  /**
   * System health heartbeat for the chat stream. Only emitted when somebody is
   * listening — an idle engine should not be formatting frames.
   */
  pushHealth(healthProvider = null) {
    // `chat` is the broadcast channel, `chat:<id>` the per-user private one.
    if (this.hub.countWhere((ch) => ch === 'chat' || ch.startsWith('chat:')) === 0) return null;

    const h = typeof healthProvider === 'function' ? healthProvider() : healthProvider;
    const body = h ?? { status: 'ok', uptimeSec: Math.round(process.uptime()) };
    const event = this.push({
      type: CHAT_TYPES.HEALTH,
      message: `Engine ${body.status ?? 'ok'} · uptime ${Math.round((body.uptimeSec ?? process.uptime()) / 60)}m · ${this.hub.count} client(s)`,
      severity: body.status === 'ok' ? 'info' : 'warning',
      meta: body,
      persist: false,
    });
    this.counters.healthFrames += 1;
    return event;
  }

  /* ======================================================================== */
  /*  REPLAY                                                                 */
  /* ======================================================================== */

  /**
   * Messages replayed to a freshly connected chat client: the in-RAM ring, and
   * — when the user is authenticated — their persisted history.
   */
  async replayFor(userId = null) {
    const fromRam = this.chatBacklog.filter((e) => !userId || !e.userId || e.userId === userId);
    let fromDb = [];
    if (this.persistence) {
      try {
        const rows = await this.persistence.listChatEvents(userId, this.backlogMax);
        fromDb = rows.map((r) => ({
          id: r.id,
          type: r.type,
          severity: r.severity ?? 'info',
          message: r.message,
          timestamp: r.created_at,
          meta: r.meta ?? {},
          userId: r.user_id ?? null,
        }));
      } catch (e) {
        log.error('chat replay failed:', e.message);
      }
    }

    // De-duplicate on id (a replayed message may exist in both sources).
    const seen = new Set();
    const merged = [];
    for (const e of [...fromDb.reverse(), ...fromRam]) {
      if (!e?.id || seen.has(e.id)) continue;
      seen.add(e.id);
      merged.push(e);
    }
    return merged.slice(-this.backlogMax);
  }

  /* ======================================================================== */
  /*  CONNECT HELPERS (used by routes/sse.js)                                */
  /* ======================================================================== */

  /**
   * Open a `prices` stream: immediate `hello` + per-symbol snapshot of the
   * latest known price, so the chart has something to draw before the first tick.
   */
  openPrices(hub, req, res, { market = null, filters = {} } = {}) {
    const symbolFilter = normaliseSymbol(filters.symbol);
    const interval = Number.isFinite(Number(filters.interval)) ? Number(filters.interval) : 300;

    const client = hub.add(req, res, {
      channels: ['prices'],
      name: 'prices',
      filter: (item) => (!symbolFilter || item.symbol === symbolFilter),
    });

    const items = [];
    for (const symbol of SYMBOLS) {
      if (symbolFilter && symbol !== symbolFilter) continue;
      const price = market?.priceOf(symbol, interval);
      const cached = this.pendingPrices.get(symbol);
      if (price == null && !cached) continue;
      const spec = getSymbolSpec(symbol);
      const item = cached ?? {
        symbol,
        display: spec.display,
        bid: price,
        ask: price,
        mid: price,
        spread: null,
        digits: spec.digits,
        ts: new Date().toISOString(),
      };
      items.push(market ? this.attachCandle(item, market, interval) : item);
    }

    hub.sendTo(client, 'hello', {
      type: 'hello',
      channel: 'prices',
      serverTime: new Date().toISOString(),
      intervalMs: this.env.stream.INTERVAL_MS,
      symbols: SYMBOLS,
      items,
    });
    log.debug(`prices hello -> ${client.id}: ${items.length} symbol(s)`);
    return client;
  }

  /**
   * Open a `chat-events` stream: `hello` then the replay backlog.
   */
  async openChat(hub, req, res, { user = null, requireAuth = false } = {}) {
    // When the endpoint is configured as authenticated, refuse an anonymous
    // connection BEFORE opening the stream. Doing it here (rather than relying
    // on middleware alone) means the chat backlog — which contains a user's own
    // executions and quota state — can never be handed to an unauthenticated
    // socket, even for the single frame it takes to notice.
    if (requireAuth && !user?.id) {
      try {
        if (!res.headersSent) {
          res.writeHead(401, {
            'content-type': 'text/event-stream; charset=utf-8',
            'cache-control': 'no-cache, no-transform',
            connection: 'keep-alive',
          });
          res.flushHeaders?.();
        }
        res.write(
          `event: auth\ndata: ${JSON.stringify({
            type: 'AUTH_ERROR',
            code: 'missing_token',
            message: 'Pass ?access_token=<token> or an Authorization header to open the chat stream.',
          })}\n\n`
        );
        res.end();
      } catch {
        /* client already gone */
      }
      return null;
    }

    const channels = user?.id ? ['chat', `chat:${user.id}`] : ['chat'];
    const client = hub.add(req, res, { channels, name: 'chat', user });

    hub.sendTo(client, 'hello', {
      type: 'hello',
      channel: 'chat-events',
      serverTime: new Date().toISOString(),
      user: user ? { id: user.id, plan: user.plan } : null,
      types: Object.values(CHAT_TYPES),
      heartbeatMs: this.env.stream.HEARTBEAT_MS,
    });

    const backlog = await this.replayFor(user?.id ?? null);
    if (backlog.length > 0) {
      hub.sendTo(client, 'backlog', {
        type: 'backlog',
        channel: 'chat-events',
        count: backlog.length,
        items: backlog,
      });
      log.debug(`chat backlog -> ${client.id}: ${backlog.length} message(s)`);
    }
    return client;
  }

  stats() {
    return { ...this.counters, backlog: this.chatBacklog.length, pendingPrices: this.pendingPrices.size };
  }
}

export default SSEDispatcher;