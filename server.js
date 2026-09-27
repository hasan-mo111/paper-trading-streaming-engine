/* =============================================================================
 *  PAPER TRADING STREAMING ENGINE  —  server.js
 *  -----------------------------------------------------------------------------
 *  Developed by Eng Hasan Mohamad
 *  -----------------------------------------------------------------------------
 *  ARCHITECTURE
 *
 *   Deriv WS API ──►  DerivFeed (auto-reconnect + exponential backoff)
 *        │  history(100 candles)      │  live ohlc / tick updates
 *        ▼                            ▼
 *              MarketAggregator  (in-memory: exactly 100 closed candles
 *        │                       + 1 forming candle, per series)
 *        │  coalesced + throttled every 400ms
 *        ▼
 *              SSEHub ──►  GET /stream  ──►  Mobile / Web clients
 *                                                    │
 *                                                    ▼
 *                                         public/index.html
 *                              (TradingView Lightweight Charts dashboard)
 *
 *  PAIRS      : XAUUSD, EURUSD, USDJPY, GBPUSD   (Deriv "frx…" codes)
 *  TIMEFRAMES : 5m, 15m, 1h, 4h
 *  SERIES     : 4 × 4 = 16
 * =============================================================================
 */

import http from 'node:http';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { randomUUID } from 'node:crypto';
import express from 'express';
import { WebSocket } from 'ws';

/* ---------------------------------------------------------------------------
 * 1) CONFIGURATION
 * ------------------------------------------------------------------------- */

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const CONFIG = {
  // ---- HTTP / SSE ---------------------------------------------------------
  PORT: Number(process.env.PORT ?? 3000),
  HOST: process.env.HOST ?? '0.0.0.0',

  // Flush coalesced market changes down the SSE pipes every 400ms
  // (comfortably inside the required 300–500ms band).
  STREAM_INTERVAL_MS: 400,
  // Comment frame every 15s — stops proxies/mobile radios dropping the socket
  SSE_HEARTBEAT_MS: 15_000,
  // Low-frequency status so UIs can grey out stale data
  STATUS_INTERVAL_MS: 5_000,
  // Back-pressure guard: stop writing to clients that fall this far behind
  SSE_MAX_BUFFER_BYTES: 1_000_000,

  // ---- Deriv upstream -----------------------------------------------------
  DERIV: {
    APP_ID: process.env.DERIV_APP_ID ?? '1089',
    URL: process.env.DERIV_WS_URL ?? 'wss://ws.derivws.com/websockets/v3',
    BACKOFF_BASE_MS: 1_000, // 1s → 2s → 4s → 8s …
    BACKOFF_MAX_MS: 30_000,
    PING_EVERY_MS: 20_000, // application-level keepalive
    WATCHDOG_MS: 60_000, // silence for this long ⇒ zombie socket, force reconnect
    // Raw tick stream on top of candle updates.
    // Candle (`ohlc`) updates already fire on every tick and carry
    // open/high/low/close, so this is optional. Enable for extra fidelity.
    STREAM_TICKS: true,
  },

  // ---- In-memory store ----------------------------------------------------
  MAX_CANDLES: 100, // closed candles kept per series
};

/** Ring size for closed candles. */
const MAX_CANDLES = CONFIG.MAX_CANDLES;

/** How many times to re-request history while calibrating the candle interval. */
const HISTORY_CALIBRATION_TRIES = 3;

/** The 4 traded pairs with their exact Deriv symbol codes. */
export const PAIRS = [
  { code: 'frxXAUUSD', display: 'XAUUSD', name: 'Gold / US Dollar' },
  { code: 'frxEURUSD', display: 'EURUSD', name: 'Euro / US Dollar' },
  { code: 'frxUSDJPY', display: 'USDJPY', name: 'US Dollar / Japanese Yen' },
  { code: 'frxGBPUSD', display: 'GBPUSD', name: 'British Pound / US Dollar' },
];

/** The 4 supported timeframes. `seconds` is the internal unit. */
export const TIMEFRAMES = [
  { seconds: 300, label: '5m', name: '5 Minutes' },
  { seconds: 900, label: '15m', name: '15 Minutes' },
  { seconds: 3600, label: '1h', name: '1 Hour' },
  { seconds: 14400, label: '4h', name: '4 Hours' },
];

/** Stable composite key used everywhere: `frxXAUUSD|900` */
export const seriesKey = (symbol, interval) => `${symbol}|${interval}`;

/** All 16 series the engine maintains, pre-declared. */
export const SERIES = PAIRS.flatMap((p) =>
  TIMEFRAMES.map((t) => ({
    key: seriesKey(p.code, t.seconds),
    symbol: p.code,
    display: p.display,
    pairName: p.name,
    interval: t.seconds,
    intervalLabel: t.label,
    intervalName: t.name,
  }))
);

/* ---------------------------------------------------------------------------
 * 2) LOGGING HELPERS
 * ------------------------------------------------------------------------- */

const stamp = () => new Date().toISOString().replace('T', ' ').slice(0, 23);
const log = (...a) => console.log(`[${stamp()}]`, ...a);
const warn = (...a) => console.warn(`[${stamp()}] ⚠ `, ...a);
const err = (...a) => console.error(`[${stamp()}] ✖ `, ...a);

/** Floor an epoch (seconds) onto the timeframe grid. */
const floorEpoch = (epochSeconds, interval) =>
  Math.floor(epochSeconds / interval) * interval;

/** Deriv sends numbers as strings — normalise. */
const toNum = (v) => (typeof v === 'number' ? v : parseFloat(v));

/* ---------------------------------------------------------------------------
 * 3) DERIV FEED  —  upstream WebSocket with exponential-backoff reconnects
 * ------------------------------------------------------------------------- */

/**
 * A resilient, auto-reconnecting Deriv WebSocket client.
 *
 *  - Correlates one-shot request/response pairs with `req_id`.
 *  - On every (re)connect it calls `onOpen` so the app can re-backfill and
 *    re-subscribe automatically.
 *  - Forwards every other message to `onMessage`.
 */
class DerivFeed {
  constructor({ onOpen, onClose, onMessage, onError } = {}) {
    this.url = `${CONFIG.DERIV.URL}?app_id=${CONFIG.DERIV.APP_ID}`;
    this.onOpen = onOpen;
    this.onClose = onClose;
    this.onMessage = onMessage;
    this.onError = onError;

    /** @type {WebSocket|null} */
    this.ws = null;
    this.connected = false;
    this.reqId = 0;
    this.attempt = 0; // drives the exponential backoff
    this.lastMessageAt = 0;
    this.lastOpenAt = 0;
    this.stopped = false;

    /** @type {Map<number, (msg:object)=>void>} req_id → resolver */
    this.pending = new Map();

    this.pingTimer = null;
    this.watchdogTimer = null;
    this.reconnectTimer = null;
  }

  /* ---------- public API ---------- */

  start() {
    this.stopped = false;
    this.connect();
  }

  /** Fire-and-forget request (subscribe / forget / ping). Returns the req_id. */
  send(payload) {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return null;
    const reqId = ++this.reqId;
    this.ws.send(JSON.stringify({ ...payload, req_id: reqId }));
    return reqId;
  }

  /** Request → resolves with the first reply carrying the same `req_id`. */
  request(payload, timeoutMs = 15_000) {
    return new Promise((resolve, reject) => {
      const reqId = this.send(payload);
      if (reqId === null) return reject(new Error('Deriv socket is not connected'));

      const timer = setTimeout(() => {
        this.pending.delete(reqId);
        reject(new Error(`Deriv request timed out (req_id=${reqId})`));
      }, timeoutMs);

      this.pending.set(reqId, (msg) => {
        clearTimeout(timer);
        if (msg.error) reject(new Error(`${msg.error.code}: ${msg.error.message}`));
        else resolve(msg);
      });
    });
  }

  status() {
    return {
      connected: this.connected,
      url: this.url,
      reconnectAttempts: this.attempt,
      uptimeMs: this.lastOpenAt ? Date.now() - this.lastOpenAt : 0,
      secondsSinceLastMessage: this.lastMessageAt
        ? Math.round((Date.now() - this.lastMessageAt) / 1000)
        : null,
    };
  }

  stop() {
    this.stopped = true;
    this.#clearTimers();
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    try {
      this.ws?.close(1000, 'server shutting down');
    } catch {
      /* ignore */
    }
  }

  /* ---------- internals ---------- */

  #clearTimers() {
    if (this.pingTimer) clearInterval(this.pingTimer);
    if (this.watchdogTimer) clearInterval(this.watchdogTimer);
    this.pingTimer = this.watchdogTimer = null;
  }

  connect() {
    if (this.stopped) return;
    if (
      this.ws &&
      (this.ws.readyState === WebSocket.OPEN || this.ws.readyState === WebSocket.CONNECTING)
    ) {
      return; // already open / opening
    }

    log(`→ Connecting to Deriv API … (attempt #${this.attempt + 1})`);

    let ws;
    try {
      ws = new WebSocket(this.url, { handshakeTimeout: 15_000 });
    } catch (e) {
      err('WebSocket construction failed:', e.message);
      return this.#scheduleReconnect();
    }
    this.ws = ws;

    ws.on('open', () => {
      this.connected = true;
      this.attempt = 0; // a successful handshake resets the backoff
      this.lastOpenAt = Date.now();
      this.lastMessageAt = Date.now();
      log('✔ Connected to Deriv API');
      this.#startTimers();
      // Fire-and-forget: bootstrap failures are logged inside the handler
      Promise.resolve(this.onOpen?.()).catch((e) => err('onOpen failed:', e.message));
    });

    ws.on('message', (raw) => {
      this.lastMessageAt = Date.now();

      let msg;
      try {
        msg = JSON.parse(raw.toString());
      } catch {
        return warn('Received a non-JSON frame from Deriv — ignored.');
      }

      // One-shot request replies are terminal for that req_id
      if (msg.req_id != null && this.pending.has(msg.req_id)) {
        const resolver = this.pending.get(msg.req_id);
        this.pending.delete(msg.req_id);
        resolver(msg);
        return;
      }

      if (msg.error) {
        err(`Deriv error [${msg.error.code}]: ${msg.error.message}`);
      } else if (msg.msg_type === 'rate_limit') {
        warn('⚠ Deriv rate limit reached — cooling down 30s');
        this.#coolDown(30_000);
      }

      this.onMessage?.(msg);
    });

    ws.on('error', (e) => {
      err('WebSocket error:', e.message);
      this.onError?.(e);
    });

    ws.on('close', (code, reason) => {
      this.connected = false;
      this.#clearTimers();
      this.pending.clear();
      log(`✖ Connection closed (code=${code}${reason ? `, reason="${reason}"` : ''})`);
      this.onClose?.(code, reason);
      if (this.stopped || code === 1000) return; // deliberate shutdown
      this.#scheduleReconnect();
    });
  }

  /** Exponential backoff with jitter: 1s, 2s, 4s … capped at 30s. */
  #scheduleReconnect(extraDelay = 0) {
    if (this.stopped || this.reconnectTimer) return;
    this.attempt += 1;
    const cap = Math.min(
      CONFIG.DERIV.BACKOFF_MAX_MS,
      CONFIG.DERIV.BACKOFF_BASE_MS * 2 ** (this.attempt - 1)
    );
    // ±50% jitter prevents a thundering herd when many engines restart at once
    const delay = Math.round(cap * (0.5 + Math.random() * 0.5) + extraDelay);

    warn(`Reconnecting in ${(delay / 1000).toFixed(1)}s (backoff attempt ${this.attempt})`);
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.connect();
    }, delay);
  }

  /** The server told us to slow down — push the next attempt far out. */
  #coolDown(ms) {
    this.attempt = 5;
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      if (!this.connected) this.connect();
    }, ms);
  }

  #startTimers() {
    this.#clearTimers();

    // Application keepalive — Deriv answers { msg_type: "pong" }
    this.pingTimer = setInterval(() => {
      if (this.ws?.readyState === WebSocket.OPEN) this.send({ ping: 1 });
    }, CONFIG.DERIV.PING_EVERY_MS);

    // Watchdog — silence means a zombie socket, force a reconnect
    this.watchdogTimer = setInterval(() => {
      if (!this.connected) return;
      const silence = Date.now() - this.lastMessageAt;
      if (silence > CONFIG.DERIV.WATCHDOG_MS) {
        warn(`No data for ${Math.round(silence / 1000)}s — forcing reconnect`);
        try {
          this.ws.terminate();
        } catch {
          /* ignore */
        }
      }
    }, CONFIG.DERIV.WATCHDOG_MS / 2);
  }
}

/* ---------------------------------------------------------------------------
 * 4) MARKET AGGREGATOR  —  the in-memory candle store
 * ------------------------------------------------------------------------- */

/**
 * For every one of the 16 series we keep:
 *   • `candles` — the last MAX_CANDLES **closed** candles (oldest → newest)
 *   • `current` — the single *forming* candle, mutated live by ticks/ohlc
 *
 * Live changes are coalesced into `pending` between flushes so the SSE layer
 * emits at most one message per STREAM_INTERVAL_MS regardless of tick rate.
 */
class MarketAggregator {
  constructor() {
    /** @type {Map<string, object>} seriesKey → slot */
    this.store = new Map(
      SERIES.map((s) => [
        s.key,
        {
          ...s,
          candles: [], // closed candles, oldest first, capped at MAX_CANDLES
          current: null, // the forming candle
          lastUpdate: 0, // ms epoch of the last accepted live update
          ready: false, // true once history has been backfilled
          tickCount: 0, // diagnostics
        },
      ])
    );

    /** @type {Map<string, object>} seriesKey → coalesced change */
    this.pending = new Map();
  }

  /* ---------- history backfill ---------- */

  /**
   * Handle a history reply for one series.
   *
   * Granularity note (important): Deriv has **no** "interval" parameter for
   * `ticks_history: "candle"`. It derives the candle interval from the
   * requested time window — roughly `(end - start) / (count - 1)` — and snaps
   * it to the nearest granularity it supports. `historyRequest()` therefore
   * sends a window of exactly `(count - 1) × interval` seconds with `end`
   * floored onto the timeframe grid.
   *
   * Because that inference is approximate, we also *measure* the interval we
   * actually received and re-request with a corrected `count` until it matches
   * the timeframe we want. Self-calibrating instead of guessing.
   *
   * @returns {number|null} the observed interval, or null if the reply was unusable
   */
  applyHistory(res, slot) {
    const history = res.history;
    if (!history?.prices?.length) {
      warn(`Empty history for ${slot.key}`);
      return null;
    }

    const times = history.times ?? [];
    const parsed = history.prices
      .map((price, i) => {
        const t = times[i];
        const epoch = t !== null && typeof t === 'object' ? t.epoch : t;
        return {
          time: Number(epoch),
          open: toNum(price),
          high: toNum(price),
          low: toNum(price),
          close: toNum(price),
          tickVolume: toNum(history.tick_volumes?.[i] ?? 0) || 0,
        };
      })
      .filter((c) => Number.isFinite(c.time) && Number.isFinite(c.close))
      .sort((a, b) => a.time - b.time);

    if (parsed.length === 0) {
      warn(`No valid candles parsed for ${slot.key}`);
      return null;
    }

    // Measure what the upstream actually gave us
    const observed =
      parsed.length > 1
        ? Math.round(parsed[1].time - parsed[0].time)
        : (slot.upstreamInterval ?? slot.interval);

    if (observed !== slot.interval) {
      warn(
        `${slot.key}: upstream returned ${observed}s candles, ${slot.interval}s requested — ` +
          `recalibrating (attempt ${(slot.histRetries ?? 0) + 1}/${HISTORY_CALIBRATION_TRIES})`
      );
      return observed; // the caller re-requests with a corrected count
    }

    const forming = parsed.pop(); // last candle is the one currently forming
    slot.candles = parsed.slice(-MAX_CANDLES); // exactly MAX_CANDLES closed
    slot.current = forming;
    slot.ready = true;
    slot.lastUpdate = Date.now();
    slot.tickCount = 0;
    slot.upstreamInterval = observed;
    slot.gridWarned = false;

    log(
      `⬇ ${slot.key.padEnd(16)} ${slot.candles.length} closed + 1 forming ` +
        `(last ${forming?.close ?? 'n/a'} @ ${forming ? new Date(forming.time * 1000).toISOString() : '-'})`
    );
    return observed;
  }

  /**
   * The history request payload for one series.
   *
   * Deriv has no explicit "interval" parameter: it derives the candle interval
   * from the time window, i.e. `(end - start) / (count - 1)`, snapped to the
   * nearest supported granularity. Sending a window of exactly
   * `MAX_CANDLES × interval` seconds with `MAX_CANDLES + 1` candles therefore
   * pins the interval deterministically. `end` is floored onto the timeframe
   * grid so the returned epochs are exact multiples of the interval.
   */
  historyRequest(slot, count = MAX_CANDLES + 1) {
    const span = Math.max(1, count - 1) * slot.interval;
    const end = floorEpoch(Math.floor(Date.now() / 1000), slot.interval);
    return {
      ticks_history: 'candle',
      symbol: slot.symbol,
      adjust_start_time: 1,
      start: end - span,
      end,
      count, // last element is the currently-forming candle
      style: 'candles',
    };
  }

  /** The live-candle subscription payload (must be sent right after history). */
  subscribeRequest(slot) {
    const end = floorEpoch(Math.floor(Date.now() / 1000), slot.interval);
    return {
      ticks_history: 'candle',
      subscribe: 1,
      symbol: slot.symbol,
      adjust_start_time: 1,
      start: end - slot.interval, // window of exactly one interval
      end,
      count: 1,
      style: 'candles',
    };
  }

  /* ---------- live update paths ---------- */

  /**
   * Handle a live candle update (msg_type "ohlc").
   * This is what keeps the *current* candle's close / high / low up to date.
   */
  applyOhlc(candle, slot) {
    if (!candle || typeof candle.epoch !== 'number' || !slot) return;

    // One-time sanity check: the live stream must land on the same grid as
    // the backfilled history, otherwise the chart would show misaligned bars.
    if (candle.epoch % slot.interval !== 0 && !slot.gridWarned) {
      slot.gridWarned = true;
      warn(
        `${slot.key}: live candle epoch ${candle.epoch} is not aligned to the ` +
          `${slot.interval}s grid — check the upstream interval`
      );
    }

    const incoming = {
      time: candle.epoch,
      open: toNum(candle.open),
      high: toNum(candle.high),
      low: toNum(candle.low),
      close: toNum(candle.close),
      tickVolume: toNum(candle.tick_volume ?? 0) || 0,
    };

    const isNewCandle = !slot.current || incoming.time > slot.current.time;

    if (isNewCandle) {
      // ── ROLLOVER: close the previous candle, open a new one ────────────────
      let justClosed = null;
      if (slot.current && incoming.time === slot.current.time + slot.interval) {
        justClosed = slot.current;
        this.#pushClosed(slot, justClosed);
      } else if (slot.current) {
        // Gap in the feed (weekend / reconnect): synthesise the missing candles
        // by carrying the last close forward so the chart has no holes.
        warn(`${slot.key} gap detected — filling missing candle(s)`);
        let ghost = { ...slot.current, time: slot.current.time + slot.interval };
        while (ghost.time < incoming.time) {
          ghost = { ...ghost, time: ghost.time + slot.interval };
          this.#pushClosed(slot, ghost);
        }
      }
      slot.current = incoming;
      this.#markDirty(slot, justClosed, true);
    } else if (incoming.time === slot.current.time) {
      // ── SAME candle: mutate the forming candle in place (the hot path) ────
      slot.current.close = incoming.close;
      slot.current.high = Math.max(slot.current.high, incoming.high);
      slot.current.low = Math.min(slot.current.low, incoming.low);
      if (incoming.open) slot.current.open = incoming.open;
      if (incoming.tickVolume) slot.current.tickVolume = incoming.tickVolume;
      this.#markDirty(slot, null, false);
    } else {
      // ── Late / corrected update for an already-closed candle ──────────────
      const idx = slot.candles.findIndex((c) => c.time === incoming.time);
      if (idx !== -1) {
        slot.candles[idx] = incoming;
        this.#markDirty(slot, null, false, true);
      }
    }

    slot.lastUpdate = Date.now();
    slot.tickCount += 1;
  }

  /** Handle a raw tick — folds the last traded price into the forming candle. */
  applyTick(tick, slot) {
    if (!slot?.current) return;
    const price = toNum(tick.quote);
    if (!Number.isFinite(price)) return;

    slot.current.close = price;
    slot.current.high = Math.max(slot.current.high, price);
    slot.current.low = Math.min(slot.current.low, price);
    slot.lastUpdate = Date.now();
    slot.tickCount += 1;
    // Deliberately NOT marking dirty: the matching ohlc update already did,
    // and ticks are far too chatty to push per-tick. The flush timer is the
    // real rate limiter. This just keeps the in-memory candle accurate.
  }

  /* ---------- internals ---------- */

  /** Append a closed candle and keep the array at EXACTLY MAX_CANDLES. */
  #pushClosed(slot, candle) {
    slot.candles.push(candle);
    if (slot.candles.length > MAX_CANDLES) {
      slot.candles.splice(0, slot.candles.length - MAX_CANDLES);
    }
  }

  /** Record a change for the next SSE flush (coalescing by series). */
  #markDirty(slot, justClosed, isNewCandle, corrected = false) {
    const existing = this.pending.get(slot.key);
    if (existing) {
      if (justClosed) existing.closed = justClosed;
      existing.isNewCandle = existing.isNewCandle || isNewCandle;
      existing.corrected = existing.corrected || corrected;
    } else {
      this.pending.set(slot.key, {
        key: slot.key,
        symbol: slot.symbol,
        display: slot.display,
        interval: slot.interval,
        intervalLabel: slot.intervalLabel,
        current: null, // filled at flush time — always the freshest copy
        closed: justClosed,
        isNewCandle,
        corrected,
      });
    }
  }

  /** Mark everything dirty (used right after a reconnect backfill). */
  markAllDirty() {
    for (const slot of this.store.values()) this.#markDirty(slot, null, false);
  }

  /** Drain coalesced changes → the SSE `update` payload (or null if idle). */
  flush() {
    if (this.pending.size === 0) return null;
    const items = [];
    for (const [key, change] of this.pending) {
      const slot = this.store.get(key);
      if (!slot) continue;
      change.current = slot.current ? { ...slot.current } : null;
      change.closed = change.closed ? { ...change.closed } : null;
      items.push(change);
    }
    this.pending.clear();
    return { type: 'update', serverTime: Date.now(), items };
  }

  /** Full historical payload — sent instantly to every new SSE client. */
  snapshot() {
    const series = {};
    for (const slot of this.store.values()) {
      series[slot.key] = {
        symbol: slot.symbol,
        display: slot.display,
        pairName: slot.pairName,
        interval: slot.interval,
        intervalLabel: slot.intervalLabel,
        intervalName: slot.intervalName,
        ready: slot.ready,
        fresh: this.isFresh(slot),
        candles: slot.candles, // exactly MAX_CANDLES closed candles
        current: slot.current, // forming candle
      };
    }
    return {
      type: 'snapshot',
      serverTime: Date.now(),
      maxCandles: MAX_CANDLES,
      pairs: PAIRS.map((p) => ({ code: p.code, display: p.display, name: p.name })),
      timeframes: TIMEFRAMES.map((t) => ({ seconds: t.seconds, label: t.label, name: t.name })),
      series,
    };
  }

  isFresh(slot) {
    return Date.now() - slot.lastUpdate < Math.max(30_000, slot.interval * 3000);
  }

  stats() {
    return [...this.store.values()].map((s) => ({
      key: s.key,
      display: s.display,
      intervalLabel: s.intervalLabel,
      closed: s.candles.length,
      ready: s.ready,
      ticks: s.tickCount,
      lastPrice: s.current?.close ?? null,
      fresh: this.isFresh(s),
    }));
  }
}

/* ---------------------------------------------------------------------------
 * 5) SSE HUB  —  downstream fan-out
 * ------------------------------------------------------------------------- */

/** Minimal, dependency-free Server-Sent Events broadcaster. */
class SSEHub {
  constructor() {
    /** @type {Set<object>} */
    this.clients = new Set();
    this.seq = 0;
  }

  get count() {
    return this.clients.size;
  }

  /**
   * Register a new SSE client.
   * Optional query filters let a client narrow the stream to save bandwidth:
   *   /stream?symbol=frxXAUUSD&interval=900
   */
  add(req, res) {
    res.writeHead(200, {
      'Content-Type': 'text/event-stream; charset=utf-8',
      'Cache-Control': 'no-cache, no-transform',
      Connection: 'keep-alive',
      'X-Accel-Buffering': 'no', // tell nginx not to buffer us
    });
    res.flushHeaders?.();
    req.socket.setNoDelay(true);
    req.socket.setKeepAlive(true);

    const symbol = req.query.symbol ? String(req.query.symbol) : null;
    const interval = req.query.interval ? Number(req.query.interval) : null;

    const client = {
      id: randomUUID().slice(0, 8),
      res,
      // A filtered client only receives updates for the series it asked for
      wants: (item) =>
        (!symbol || item.symbol === symbol) && (!interval || item.interval === interval),
      connectedAt: Date.now(),
      sent: 0,
      dropped: 0,
      filter: symbol || interval ? `${symbol ?? '*'}|${interval ?? '*'}` : 'all',
    };

    res.write('retry: 3000\n\n'); // client auto-reconnect delay
    this.clients.add(client);
    log(`＋ SSE client ${client.id} connected (${client.filter}) — total: ${this.count}`);
    return client;
  }

  remove(client) {
    if (this.clients.delete(client)) {
      log(`－ SSE client ${client.id} disconnected — total: ${this.count}`);
    }
  }

  /** Write one SSE frame to a single client, with back-pressure protection. */
  sendTo(client, event, data) {
    if (client.res.writableEnded) return this.remove(client);
    if (client.res.writableLength > CONFIG.SSE_MAX_BUFFER_BYTES) {
      client.dropped += 1;
      if (client.dropped === 1 || client.dropped % 50 === 0) {
        warn(`SSE client ${client.id} too slow — dropping frames (${client.dropped})`);
      }
      return;
    }
    this.seq += 1;
    client.sent += 1;
    client.res.write(`id: ${this.seq}\nevent: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
  }

  /** Broadcast to every client, honouring each client's series filter. */
  broadcast(event, data) {
    if (this.clients.size === 0) return;
    for (const client of this.clients) {
      if (event === 'update' && Array.isArray(data.items)) {
        const items = data.items.filter((i) => client.wants(i));
        if (items.length === 0) continue;
        this.sendTo(client, event, { ...data, items });
      } else {
        this.sendTo(client, event, data);
      }
    }
  }

  /** SSE comment frames — keeps idle connections alive through proxies. */
  heartbeat() {
    for (const client of this.clients) {
      if (!client.res.writableEnded) client.res.write(`: heartbeat ${Date.now()}\n\n`);
    }
  }

  closeAll() {
    for (const client of this.clients) {
      try {
        client.res.end();
      } catch {
        /* ignore */
      }
    }
    this.clients.clear();
  }

  stats() {
    return [...this.clients].map((c) => ({
      id: c.id,
      filter: c.filter,
      uptimeSec: Math.round((Date.now() - c.connectedAt) / 1000),
      framesSent: c.sent,
      framesDropped: c.dropped,
    }));
  }
}

/* ---------------------------------------------------------------------------
 * 6) WIRE EVERYTHING TOGETHER
 * ------------------------------------------------------------------------- */

const market = new MarketAggregator();
const sse = new SSEHub();

/**
 * subscription_id → seriesKey.
 * Deriv hands back a fresh subscription id in the reply to every subscribe
 * request, and stamps it on every subsequent ohlc / tick frame — this map is
 * how we route a live frame back to its in-memory slot.
 */
const subIdToKey = new Map();

/** req_id → seriesKey, for the moment between "we sent it" and "id known". */
const reqIdToKey = new Map();

/** req_id → slot, for outstanding history backfill requests. */
const pendingHistory = new Map();

const feed = new DerivFeed({
  /**
   * Runs after every (re)connect:
   *   1. request 100 candles for all 16 series
   *   2. subscribe to live candle updates (+ optional raw tick stream)
   *
   * Each history request is immediately followed by its own subscribe request
   * for the same symbol. Deriv resolves a subscription's candle interval from
   * the *most recent* history request for that symbol, so keeping the two
   * adjacent is what makes 4 different timeframes per symbol work correctly.
   */
  onOpen() {
    log('Bootstrapping 16 series (4 pairs × 4 timeframes)…');
    subIdToKey.clear();
    reqIdToKey.clear();
    pendingHistory.clear();

    // 1 + 2) History then live subscription, back-to-back, for every series.
    //     Sent pipelined (no awaiting) so Deriv processes them in this exact
    //     order while we still get full network parallelism.
    for (const meta of SERIES) {
      const slot = market.store.get(meta.key);
      slot.histRetries = 0;
      slot.histRequestCount = MAX_CANDLES + 1;

      const histReqId = feed.send(market.historyRequest(slot));
      if (histReqId !== null) pendingHistory.set(histReqId, slot);

      const subReqId = feed.send(market.subscribeRequest(slot));
      if (subReqId !== null) reqIdToKey.set(subReqId, meta.key);
    }

    // 3) Optional raw tick stream — one per pair, folded into every timeframe
    if (CONFIG.DERIV.STREAM_TICKS) {
      for (const pair of PAIRS) {
        const reqId = feed.send({ ticks: 'tick', subscribe: 1, symbol: pair.code });
        if (reqId !== null) reqIdToKey.set(reqId, `tick:${pair.code}`);
      }
    }

    log(`✔ ${SERIES.length} history + ${SERIES.length} candle subscriptions dispatched`);
  },

  onClose() {
    // Tell clients the data is now stale so the UI can warn / grey out
    sse.broadcast('status', {
      connected: false,
      serverTime: Date.now(),
      message: 'Upstream connection lost — reconnecting…',
    });
  },

  onMessage(msg) {
    // A history backfill reply → fill the slot (with interval calibration)
    if (msg.req_id != null && pendingHistory.has(msg.req_id)) {
      const slot = pendingHistory.get(msg.req_id);
      pendingHistory.delete(msg.req_id);

      if (msg.error) {
        err(`history failed for ${slot.key}: ${msg.error.code} ${msg.error.message}`);
      } else if (msg.msg_type === 'history') {
        const observed = market.applyHistory(msg, slot);

        if (observed != null && observed !== slot.interval) {
          // The upstream gave us the wrong candle interval — re-request with a
          // `count` scaled so Deriv's own inference lands on the target.
          slot.histRetries = (slot.histRetries ?? 0) + 1;
          if (slot.histRetries <= HISTORY_CALIBRATION_TRIES) {
            const count = Math.max(
              2,
              Math.round((slot.histRequestCount ?? MAX_CANDLES + 1) * (observed / slot.interval))
            );

            const id = feed.send(market.historyRequest(slot, count));
            if (id !== null) {
              slot.histRequestCount = count;
              pendingHistory.set(id, slot);
            }
          } else {
            err(
              `${slot.key}: could not pin the ${slot.interval}s interval after ` +
                `${HISTORY_CALIBRATION_TRIES} attempts — the chart will use ${observed}s`
            );
            slot.upstreamInterval = observed;
            slot.ready = true;
          }
        } else if (observed != null) {
          // Push the reloaded candles to any client that was already connected
          // (they would otherwise keep showing the pre-disconnect data).
          market.markAllDirty();
        }
      }
    }

    // Learn the real subscription id the first time we see it
    if (msg.req_id != null && reqIdToKey.has(msg.req_id)) {
      const key = reqIdToKey.get(msg.req_id);
      reqIdToKey.delete(msg.req_id);
      if (msg.subscription?.id) {
        subIdToKey.set(msg.subscription.id, key);
        log(`  ↻ subscribed ${String(key).padEnd(16)} → id ${msg.subscription.id}`);
      }
      // The first reply of a candle subscription is `history` — already
      // backfilled above, so there is nothing else to do with it.
    }

    const key = msg.subscription?.id ? subIdToKey.get(msg.subscription.id) : null;
    if (!key) return;

    if (msg.msg_type === 'ohlc' && msg.candle) {
      const slot = market.store.get(key);
      if (slot) market.applyOhlc(msg.candle, slot);
      return;
    }

    if (msg.msg_type === 'tick') {
      // A tick stream is registered as "tick:<symbol>" — update EVERY
      // timeframe of that symbol with the last traded price.
      const symbol = key.startsWith('tick:') ? key.slice(5) : null;
      if (!symbol) return;
      for (const slot of market.store.values()) {
        if (slot.symbol === symbol) market.applyTick(msg, slot);
      }
    }
  },
});

/* ---------------------------------------------------------------------------
 * 7) EXPRESS + HTTP SERVER
 * ------------------------------------------------------------------------- */

const app = express();
app.disable('x-powered-by');

app.use(
  express.static(path.join(__dirname, 'public'), {
    maxAge: process.env.NODE_ENV === 'production' ? '1h' : 0,
    etag: true,
  })
);

/** Root → the Lightweight Charts dashboard. */
app.get('/', (_req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

/** Liveness + diagnostics. */
app.get('/health', (_req, res) => {
  res.json({
    status: 'ok',
    upstream: feed.status(),
    sseClients: sse.count,
    sseClientList: sse.stats(),
    series: market.stats(),
    memoryMb: Math.round((process.memoryUsage().heapUsed / 1048576) * 10) / 10,
    uptimeSec: Math.round(process.uptime()),
  });
});

/** Full in-memory state as JSON (Postman / external consumers). */
app.get('/api/snapshot', (_req, res) => res.json(market.snapshot()));

/** REST poll alternative to SSE. */
app.get('/api/candles', (req, res) => {
  const symbol = String(req.query.symbol ?? PAIRS[0].code);
  const interval = Number(req.query.interval ?? 900);
  const slot = market.store.get(seriesKey(symbol, interval));
  if (!slot) {
    return res.status(404).json({ error: `Unknown series ${symbol}|${interval}` });
  }
  res.json({
    symbol: slot.symbol,
    display: slot.display,
    interval: slot.interval,
    intervalLabel: slot.intervalLabel,
    ready: slot.ready,
    fresh: market.isFresh(slot),
    candles: slot.candles,
    current: slot.current,
  });
});

/**
 * THE SSE ENDPOINT
 *   1) on connect  → immediately sends the FULL 100-candle history for all
 *                    16 series so the client can draw immediately
 *   2) thereafter  → coalesced live `update` events every 400ms
 */
app.get('/stream', (req, res) => {
  const client = sse.add(req, res);

  const snapshot = market.snapshot();
  snapshot.upstream = feed.status();
  sse.sendTo(client, 'snapshot', snapshot);

  const total = Object.values(snapshot.series).reduce((n, s) => n + s.candles.length, 0);
  log(`   ↳ snapshot → ${client.id}: ${total} candles across 16 series`);

  const cleanup = () => sse.remove(client);
  req.on('close', cleanup);
  req.on('error', cleanup);
});

const server = http.createServer(app);

/* ---- Throttled flush loop: the heart of the downstream stream ------------ */
const flushTimer = setInterval(() => {
  const payload = market.flush();
  if (payload) sse.broadcast('update', payload);
}, CONFIG.STREAM_INTERVAL_MS);

/* ---- Keepalive comments -------------------------------------------------- */
const heartbeatTimer = setInterval(() => sse.heartbeat(), CONFIG.SSE_HEARTBEAT_MS);

/* ---- Periodic status so UIs can grey out stale data ---------------------- */
const statusTimer = setInterval(() => {
  if (sse.count === 0) return;
  sse.broadcast('status', {
    connected: feed.connected,
    serverTime: Date.now(),
    upstream: feed.status(),
    clients: sse.count,
  });
}, CONFIG.STATUS_INTERVAL_MS);

/* ---- Module exports (also allow `import`ing this file for unit tests) ---- */
export { DerivFeed, MarketAggregator, SSEHub, CONFIG, MAX_CANDLES };

// Only boot the HTTP server + upstream feed when this file is the entry point
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  server.listen(CONFIG.PORT, CONFIG.HOST, () => {
    log('========================================================');
    log('  PAPER TRADING STREAMING ENGINE');
    log('  Developed by Eng Hasan Mohamad');
    log('========================================================');
    log(`  Dashboard : http://localhost:${CONFIG.PORT}/`);
    log(`  SSE feed  : http://localhost:${CONFIG.PORT}/stream`);
    log(`  Health    : http://localhost:${CONFIG.PORT}/health`);
    log(`  Upstream  : ${feed.url}  (app_id=${CONFIG.DERIV.APP_ID})`);
    log(`  Pairs     : ${PAIRS.map((p) => p.display).join(', ')}`);
    log(`  Timeframes: ${TIMEFRAMES.map((t) => t.label).join(', ')}`);
    log(`  Store     : ${SERIES.length} series × ${MAX_CANDLES} candles (in-memory)`);
    log(`  Stream    : SSE every ${CONFIG.STREAM_INTERVAL_MS}ms`);
    log('========================================================');
    feed.start();
  });
}

/* ---- Graceful shutdown --------------------------------------------------- */
let shuttingDown = false;
function shutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  log(`\n${signal} received — shutting down gracefully…`);

  clearInterval(flushTimer);
  clearInterval(heartbeatTimer);
  clearInterval(statusTimer);
  feed.stop();
  sse.closeAll();

  server.close(() => {
    log('✔ HTTP server closed. Bye!');
    process.exit(0);
  });
  setTimeout(() => {
    warn('Forcing exit after 5s timeout');
    process.exit(1);
  }, 5_000).unref();
}

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('unhandledRejection', (e) => err('Unhandled rejection:', e?.message ?? e));
