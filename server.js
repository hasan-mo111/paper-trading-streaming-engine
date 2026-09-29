/* =============================================================================
 *  PAPER TRADING STREAMING ENGINE  —  server.js
 *  -----------------------------------------------------------------------------
 *  Developed by Eng Hasan Mohamad
 *  -----------------------------------------------------------------------------
 *  ARCHITECTURE
 *
 *   biquote.io tick feed  ──►  BiquoteFeed (SignalR over WebSocket,
 *        (wss://…/hubs/tick)        auto-reconnect + exponential backoff)
 *              │  REST /ohlc (100 candles)   │  live ticks
 *              ▼                             ▼
 *              MarketAggregator  (in-memory: exactly 100 closed candles
 *        │                    + 1 forming candle, per series)
 *        │  coalesced + throttled every 400ms
 *        ▼
 *              SSEHub ──►  GET /stream  ──►  Mobile / Web clients
 *                                                   │
 *                                                   ▼
 *                                        public/index.html
 *                             (TradingView Lightweight Charts dashboard)
 *
 *  UPSTREAM  : biquote.io — free real-time market data (MetaTrader 5 feed),
 *              no API key required.
 *  PAIRS     : XAUUSD, EURUSD, USDJPY, GBPUSD
 *  TIMEFRAMES: 5m, 15m, 1h, 4h
 *  SERIES    : 4 × 4 = 16
 * =============================================================================
 */

import http from 'node:http';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
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

  // ---- biquote.io upstream -------------------------------------------------
  BIQUOTE: {
    API: process.env.BIQUOTE_API ?? 'https://biquote.io',
    WS: process.env.BIQUOTE_WS ?? 'wss://biquote.io/hubs/tick',
    BACKOFF_BASE_MS: 1_000, // 1s → 2s → 4s → 8s …
    BACKOFF_MAX_MS: 30_000,
    // SignalR keepalive
    PING_EVERY_MS: 20_000,
    // Complete silence for this long ⇒ zombie socket. Deliberately generous:
    // forex and metals are closed at weekends, and a tight watchdog would
    // reconnect in a loop for the whole Saturday.
    WATCHDOG_MS: 300_000,
    REST_TIMEOUT_MS: 15_000,
  },

  // ---- In-memory store ----------------------------------------------------
  MAX_CANDLES: 100, // closed candles kept per series
};

/** Ring size for closed candles. */
const MAX_CANDLES = CONFIG.MAX_CANDLES;

/** The 4 traded pairs. `code` is the upstream symbol. */
export const PAIRS = [
  { code: 'XAUUSD', display: 'XAUUSD', name: 'Gold / US Dollar' },
  { code: 'EURUSD', display: 'EURUSD', name: 'Euro / US Dollar' },
  { code: 'USDJPY', display: 'USDJPY', name: 'US Dollar / Japanese Yen' },
  { code: 'GBPUSD', display: 'GBPUSD', name: 'British Pound / US Dollar' },
];

/**
 * The 4 supported timeframes.
 * `seconds` is the internal unit, `api` is the upstream interval string.
 */
export const TIMEFRAMES = [
  { seconds: 300, api: '5m', label: '5m', name: '5 Minutes' },
  { seconds: 900, api: '15m', label: '15m', name: '15 Minutes' },
  { seconds: 3600, api: '1h', label: '1h', name: '1 Hour' },
  { seconds: 14400, api: '4h', label: '4h', name: '4 Hours' },
];

/** Stable composite key used everywhere: `XAUUSD|900` */
export const seriesKey = (symbol, interval) => `${symbol}|${interval}`;

/** All 16 series the engine maintains, pre-declared. */
export const SERIES = PAIRS.flatMap((p) =>
  TIMEFRAMES.map((t) => ({
    key: seriesKey(p.code, t.seconds),
    symbol: p.code,
    display: p.display,
    pairName: p.name,
    interval: t.seconds,
    intervalApi: t.api,
    intervalLabel: t.label,
    intervalName: t.name,
  }))
);

/* ---------------------------------------------------------------------------
 * 2) SMALL HELPERS
 * ------------------------------------------------------------------------- */

const stamp = () => new Date().toISOString().replace('T', ' ').slice(0, 23);
const log = (...a) => console.log(`[${stamp()}]`, ...a);
const warn = (...a) => console.warn(`[${stamp()}] ⚠ `, ...a);
const err = (...a) => console.error(`[${stamp()}] ✖ `, ...a);

/** Floor an epoch (seconds) onto the timeframe grid. */
const floorEpoch = (epochSeconds, interval) =>
  Math.floor(epochSeconds / interval) * interval;

/** Upstream sends numbers as strings in some places — normalise. */
const toNum = (v) => (v == null || v === '' ? NaN : typeof v === 'number' ? v : parseFloat(v));

/** Short, readable id for log lines and SSE clients. */
const randomId = () => Math.random().toString(36).slice(2, 10);

/** fetch() with a timeout, so a hanging upstream cannot stall the boot. */
const fetchWithTimeout = async (url, timeoutMs) => {
  const ctl = new AbortController();
  const timer = setTimeout(() => ctl.abort(), timeoutMs);
  try {
    return await fetch(url, { signal: ctl.signal, headers: { accept: 'application/json' } });
  } finally {
    clearTimeout(timer);
  }
};

/* ---------------------------------------------------------------------------
 * 3) BIQUOTE FEED  —  upstream WebSocket (SignalR) with exponential reconnects
 * ------------------------------------------------------------------------- */

/** SignalR's record separator (0x1E) — every frame is suffixed with it. */
const RS = '\u001e';

/**
 * A resilient, auto-reconnecting client for the biquote.io tick hub.
 *
 * Protocol: SignalR over a raw WebSocket.
 *   1. send handshake   {"protocol":"json","version":1}
 *   2. receive ack      {}
 *   3. invoke           Subscribe with our symbol list
 *   4. receive          ReceiveTick / ReceiveSubscriptionState messages
 *   5. ping             {"type":6}  ↔  {"type":6}
 *
 * Emits only one thing upstream: `onTick(symbol, tick)`.
 */
class BiquoteFeed {
  constructor({ symbols = [], onTick = () => {}, onOpen = () => {}, onClose = () => {}, onError = () => {} } = {}) {
    this.url = CONFIG.BIQUOTE.WS;
    this.symbols = symbols;
    this.onTick = onTick;
    this.onOpen = onOpen;
    this.onClose = onClose;
    this.onError = onError;

    /** @type {WebSocket|null} */
    this.ws = null;
    this.connected = false;
    this.attempt = 0; // drives the exponential backoff
    this.lastMessageAt = 0;
    this.lastOpenAt = 0;
    this.stopped = false;
    this.invocationId = 0;
    this.subscribed = false;

    this.pingTimer = null;
    this.watchdogTimer = null;
    this.reconnectTimer = null;

    // Last failure seen — surfaced on /health so faults are diagnosable
    // at a glance without digging through the log.
    this.lastError = null;
  }

  /* ---------- public API ---------- */

  start() {
    this.stopped = false;
    this.connect();
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

  status() {
    return {
      connected: this.connected,
      subscribed: this.subscribed,
      url: this.url,
      reconnectAttempts: this.attempt,
      uptimeMs: this.lastOpenAt ? Date.now() - this.lastOpenAt : 0,
      secondsSinceLastTick: this.lastMessageAt
        ? Math.round((Date.now() - this.lastMessageAt) / 1000)
        : null,
      // Set when the upstream could not be reached
      lastError: this.lastError,
    };
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

    log(`→ Connecting to biquote tick feed … (attempt #${this.attempt + 1})`);

    let ws;
    try {
      ws = new WebSocket(this.url, { handshakeTimeout: 15_000 });
    } catch (e) {
      this.lastError = `construct: ${e.message}`;
      err('WebSocket construction failed:', e.message);
      this.onError?.(e);
      return this.#scheduleReconnect();
    }
    this.ws = ws;
    this.subscribed = false;

    ws.on('open', () => {
      // Step 1 — SignalR handshake
      ws.send(JSON.stringify({ protocol: 'json', version: 1 }) + RS);
    });

    ws.on('message', (raw) => {
      this.lastMessageAt = Date.now();
      for (const frame of raw.toString().split(RS)) {
        if (!frame) continue;
        let msg;
        try {
          msg = JSON.parse(frame);
        } catch {
          warn('Unparseable frame from biquote — ignored.');
          continue;
        }
        this.#handle(msg);
      }
    });

    ws.on('error', (e) => {
      this.lastError = e.message;
      err('WebSocket error:', e.message);
      this.onError?.(e);
    });

    ws.on('close', (code, reason) => {
      this.connected = false;
      this.subscribed = false;
      this.#clearTimers();
      log(`✖ Connection closed (code=${code}${reason ? `, reason="${reason}"` : ''})`);
      this.onClose?.(code, reason?.toString() || '');
      if (this.stopped || code === 1000) return; // deliberate shutdown
      this.#scheduleReconnect();
    });
  }

  #handle(msg) {
    // ---- Step 2: handshake acknowledgement (no `type` field) ----
    if (msg.type === undefined) {
      if (msg.error) {
        this.lastError = `handshake: ${msg.error}`;
        err('SignalR handshake rejected:', msg.error);
        return;
      }
      this.connected = true;
      this.attempt = 0; // a successful handshake resets the backoff
      this.lastOpenAt = Date.now();
      this.lastError = null;
      log('✔ Connected to biquote.io');

      // ---- Step 3: subscribe to our symbols ----
      this.#send({
        type: 1,
        invocationId: String(++this.invocationId),
        target: 'Subscribe',
        arguments: [this.symbols],
      });
      this.#startTimers();
      Promise.resolve(this.onOpen?.()).catch((e) => err('onOpen failed:', e.message));
      return;
    }

    switch (msg.type) {
      // ---- Invocation result / completion ----
      case 3:
        if (msg.error) {
          this.lastError = `subscribe: ${msg.error}`;
          err('Subscribe rejected:', msg.error);
        } else {
          log(`  ✓ Subscribe confirmed${msg.result ? `: ${JSON.stringify(msg.result).slice(0, 120)}` : ''}`);
          this.subscribed = true;
        }
        return;

      // ---- Stream messages ----
      case 1: {
        if (msg.target === 'ReceiveTick' && Array.isArray(msg.arguments)) {
          for (const tick of msg.arguments) {
            if (tick?.symbol) this.onTick(tick.symbol, tick);
          }
        } else if (msg.target === 'ReceiveSubscriptionState' && msg.arguments) {
          this.subscribed = true;
          log(`  ↻ subscription state: ${JSON.stringify(msg.arguments[0]).slice(0, 160)}`);
        }
        return;
      }

      // ---- Ping: keep the connection warm through proxies ----
      case 6:
        this.#send({ type: 6 });
        return;

      default:
        return; // close / other message types
    }
  }

  #send(obj) {
    if (this.ws?.readyState === WebSocket.OPEN) this.ws.send(JSON.stringify(obj) + RS);
  }

  /** Exponential backoff with jitter: 1s, 2s, 4s … capped at 30s. */
  #scheduleReconnect(extraDelay = 0) {
    if (this.stopped || this.reconnectTimer) return;
    this.attempt += 1;
    const cap = Math.min(
      CONFIG.BIQUOTE.BACKOFF_MAX_MS,
      CONFIG.BIQUOTE.BACKOFF_BASE_MS * 2 ** (this.attempt - 1)
    );
    // ±50% jitter prevents a thundering herd when many engines restart at once
    const delay = Math.round(cap * (0.5 + Math.random() * 0.5) + extraDelay);

    warn(`Reconnecting in ${(delay / 1000).toFixed(1)}s (backoff attempt ${this.attempt})`);
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.connect();
    }, delay);
  }

  #startTimers() {
    this.#clearTimers();

    // Application keepalive
    this.pingTimer = setInterval(() => this.#send({ type: 6 }), CONFIG.BIQUOTE.PING_EVERY_MS);

    // Watchdog — complete silence means a zombie socket. The window is long on
    // purpose: forex/metals are closed at weekends and we must not reconnect
    // in a loop while the market is shut.
    this.watchdogTimer = setInterval(() => {
      if (!this.connected) return;
      const silence = Date.now() - this.lastMessageAt;
      if (silence > CONFIG.BIQUOTE.WATCHDOG_MS) {
        warn(`No data for ${Math.round(silence / 1000)}s — forcing reconnect`);
        try {
          this.ws.terminate();
        } catch {
          /* ignore */
        }
      }
    }, CONFIG.BIQUOTE.WATCHDOG_MS / 2);
  }
}

/* ---------------------------------------------------------------------------
 * 4) MARKET AGGREGATOR  —  the in-memory candle store
 * ------------------------------------------------------------------------- */

/**
 * For every one of the 16 series we keep:
 *   • `candles` — the last MAX_CANDLES **closed** candles (oldest → newest)
 *   • `current` — the single *forming* candle, mutated live by incoming ticks
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

  /* ---------- history backfill (REST) ---------- */

  /**
   * Pull history for one series: `GET /api/{symbol}/ohlc?interval=…&limit=…`
   *
   * biquote returns bars newest-first and flags the currently-forming one with
   * `isOpen: true`, so there is no interval inference to calibrate here — we
   * pass the exact interval string and get exactly that back.
   */
  async backfill(slot) {
    const url =
      `${CONFIG.BIQUOTE.API}/api/${slot.symbol}/ohlc` +
      `?interval=${encodeURIComponent(slot.intervalApi)}&limit=${MAX_CANDLES + 1}`;

    const res = await fetchWithTimeout(url, CONFIG.BIQUOTE.REST_TIMEOUT_MS);
    if (!res.ok) {
      throw new Error(`HTTP ${res.status} ${(await res.text()).slice(0, 120)}`);
    }
    const body = await res.json();
    const bars = Array.isArray(body?.bars) ? body.bars : [];
    if (bars.length === 0) throw new Error(`no bars returned for ${slot.symbol}`);

    if (body.interval && body.interval !== slot.intervalApi) {
      warn(`${slot.key}: upstream used interval "${body.interval}", expected "${slot.intervalApi}"`);
    }

    const toCandle = (b) => ({
      time: Math.floor(new Date(b.openTime).getTime() / 1000),
      open: toNum(b.open),
      high: toNum(b.high),
      low: toNum(b.low),
      close: toNum(b.close),
      tickVolume: toNum(b.tickVolume ?? b.volume ?? 0) || 0,
    });

    // `isOpen` marks the forming candle; fall back to "newest bar" if absent
    const formingRaw = bars.find((b) => b.isOpen) ?? bars[0];
    const closed = bars
      .filter((b) => b !== formingRaw)
      .map(toCandle)
      .filter((c) => Number.isFinite(c.time) && Number.isFinite(c.close))
      .sort((a, b) => a.time - b.time);

    const forming = toCandle(formingRaw);
    if (!Number.isFinite(forming.time) || !Number.isFinite(forming.close)) {
      throw new Error(`unusable forming candle for ${slot.symbol}`);
    }

    slot.candles = closed.slice(-MAX_CANDLES); // exactly MAX_CANDLES closed
    slot.current = forming;
    slot.ready = true;
    slot.lastUpdate = Date.now();
    slot.tickCount = 0;

    log(
      `⬇ ${slot.key.padEnd(12)} ${slot.candles.length} closed + 1 forming ` +
        `(last ${forming.close} @ ${new Date(forming.time * 1000).toISOString()})`
    );
  }

  /* ---------- live tick path (the hot path) ---------- */

  /**
   * Fold one live tick into the series.
   *
   * A tick whose epoch is newer than the current candle opens a new candle
   * (closing the previous one); a tick inside the current candle updates its
   * close / high / low in place. This is what keeps the active candle live.
   *
   * @returns {boolean} true when the tick produced a state change
   */
  applyTick(symbol, tick) {
    // One tick can only belong to the timeframes of its own symbol
    const price = toNum(tick.mid ?? tick.ask ?? tick.bid);
    const epochSec = tick.timestamp ? new Date(tick.timestamp).getTime() / 1000 : Date.now() / 1000;
    if (!Number.isFinite(price) || !Number.isFinite(epochSec)) return false;

    let changed = false;

    for (const slot of this.store.values()) {
      if (slot.symbol !== symbol) continue;

      const epoch = floorEpoch(epochSec, slot.interval);

      // First tick ever, or the feed was down and we missed candles
      if (!slot.current) {
        slot.current = { time: epoch, open: price, high: price, low: price, close: price, tickVolume: 1 };
        this.#markDirty(slot, null, true);
        changed = true;
      } else if (epoch > slot.current.time) {
        // ── ROLLOVER: close the previous candle, open a new one ─────────────
        let closed = null;
        if (epoch === slot.current.time + slot.interval) {
          closed = slot.current; // the normal case
          this.#pushClosed(slot, closed);
        } else {
          // Gap (weekend / outage): carry the last close forward so the chart
          // never shows a hole, then open the new candle at the live price.
          warn(`${slot.key} gap of ${Math.round((epoch - slot.current.time) / slot.interval)} candles — filling`);
          let ghost = { ...slot.current, time: slot.current.time + slot.interval };
          while (ghost.time < epoch) {
            this.#pushClosed(slot, ghost);
            ghost = { ...ghost, time: ghost.time + slot.interval };
          }
        }
        slot.current = { time: epoch, open: price, high: price, low: price, close: price, tickVolume: 1 };
        this.#markDirty(slot, closed, true);
        changed = true;
      } else if (epoch === slot.current.time) {
        // ── Same candle: mutate the forming candle in place ─────────────────
        slot.current.close = price;
        slot.current.high = Math.max(slot.current.high, price);
        slot.current.low = Math.min(slot.current.low, price);
        slot.current.tickVolume += 1;
        this.#markDirty(slot, null, false);
        changed = true;
      }
      // Ticks older than the current candle are stale — deliberately ignored.

      slot.lastUpdate = Date.now();
      slot.tickCount += 1;
    }

    return changed;
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
  #markDirty(slot, justClosed, isNewCandle) {
    const existing = this.pending.get(slot.key);
    if (existing) {
      if (justClosed) existing.closed = justClosed;
      existing.isNewCandle = existing.isNewCandle || isNewCandle;
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
      source: 'biquote.io (MetaTrader 5)',
      pairs: PAIRS.map((p) => ({ code: p.code, display: p.display, name: p.name })),
      timeframes: TIMEFRAMES.map((t) => ({
        seconds: t.seconds,
        label: t.label,
        name: t.name,
      })),
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
   *   /stream?symbol=XAUUSD&interval=900
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
      id: randomId(),
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

const feed = new BiquoteFeed({
  symbols: PAIRS.map((p) => p.code),

  /**
   * Runs after every (re)connect: reload history for all 16 series and push
   * it straight to any client that is already attached (they would otherwise
   * keep showing the pre-disconnect data).
   */
  async onOpen() {
    log('Bootstrapping 16 series (4 pairs × 4 timeframes)…');

    const BATCH = 4; // small parallel batches stay well under any rate limit
    for (let i = 0; i < SERIES.length; i += BATCH) {
      const batch = SERIES.slice(i, i + BATCH);
      const results = await Promise.allSettled(batch.map((meta) => market.backfill(market.store.get(meta.key))));
      results.forEach((r, n) => {
        if (r.status === 'rejected') err(`backfill failed for ${batch[n].key}: ${r.reason?.message}`);
      });
    }

    market.markAllDirty();
    log(`✔ ${SERIES.length} series ready`);
  },

  onClose() {
    // Tell clients the data is now stale so the UI can warn / grey out
    sse.broadcast('status', {
      connected: false,
      serverTime: Date.now(),
      message: 'Upstream connection lost — reconnecting…',
    });
  },

  /** Every live tick, folded into the forming candle of all its timeframes. */
  onTick(symbol, tick) {
    market.applyTick(symbol, tick);
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

/* ---- Module exports (also allows `import` for unit tests) --------------- */
export { BiquoteFeed, MarketAggregator, SSEHub, CONFIG, MAX_CANDLES };

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
    log(`  Upstream  : ${CONFIG.BIQUOTE.WS}   REST ${CONFIG.BIQUOTE.API}`);
    log(`  Source    : biquote.io — MetaTrader 5 (free, no API key)`);
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
