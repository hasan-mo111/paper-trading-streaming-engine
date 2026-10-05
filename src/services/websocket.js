/* =============================================================================
 *  AETHERON CORE  —  src/services/websocket.js
 *  -----------------------------------------------------------------------------
 *  Upstream MT5 tick feed over WebSocket (SignalR JSON protocol).
 *
 *  Protocol walk-through
 *  --------------------
 *    1. client → server   {"protocol":"json","version":1}   handshake
 *    2. server → client   {}                                  handshake ack
 *    3. client → server   {type:1,target:"Subscribe",arguments:[[symbols]]}
 *    4. server → client   {type:3}                            completion
 *    5. server → client   {type:1,target:"ReceiveTick",arguments:[tick, …]}
 *    6. client ↔ server   {type:6}                            ping / pong
 *  Every frame is suffixed with the SignalR record separator 0x1E.
 *
 *  RESILIENCE
 *  ----------
 *  • exponential backoff  min(MAX, BASE × 2ⁿ) with ±50 % jitter, so a fleet of
 *    restarting instances does not stampede the upstream;
 *  • a liveness ping every PING_EVERY_MS to survive idle proxies;
 *  • a silence watchdog that terminates zombie sockets. The window is long on
 *    purpose — markets are closed at weekends and a tight watchdog would
 *    reconnect in a loop for the whole Saturday.
 * ============================================================================= */

import { WebSocket } from 'ws';
import { ENV } from '../config/env.js';
import { SYMBOLS } from '../config/db.js';
import { createLogger } from './logger.js';

const log = createLogger('feed');

/** SignalR record separator. */
const RS = '\u001e';

export class TickFeed {
  /**
   * @param {object} opts
   * @param {string[]} [opts.symbols]     symbols to subscribe to
   * @param {(symbol:string, tick:object)=>void} [opts.onTick]
   * @param {() => Promise<void>|void} [opts.onOpen]   fires after every (re)connect
   * @param {(code:number, reason:string)=>void} [opts.onClose]
   * @param {(err:Error)=>void} [opts.onError]
   * @param {object} [opts.env]           config override (tests)
   */
  constructor({
    symbols = [],
    onTick = () => {},
    onOpen = () => {},
    onClose = () => {},
    onError = () => {},
    env = ENV,
  } = {}) {
    this.env = env;
    this.url = env.upstream.WS;
    this.symbols = (symbols.length ? symbols : SYMBOLS).map((s) => String(s).toUpperCase());
    this.onTick = onTick;
    this.onOpen = onOpen;
    this.onClose = onClose;
    this.onError = onError;

    /** @type {WebSocket|null} */
    this.ws = null;
    this.connected = false;
    this.subscribed = false;
    this.attempt = 0; // drives the exponential backoff
    this.lastMessageAt = 0;
    this.lastOpenAt = 0;
    this.stopped = true;
    this.invocationId = 0;
    this.ticksReceived = 0;
    this.reconnects = 0;
    this.lastError = null;

    this.pingTimer = null;
    this.watchdogTimer = null;
    this.reconnectTimer = null;

    /** Latest tick seen per symbol — seeds prices right after a reconnect. */
    this.lastTicks = new Map();
  }

  /* ------------------------------- public API ------------------------------ */

  start() {
    this.stopped = false;
    this.connect();
  }

  stop() {
    this.stopped = true;
    this.#clearTimers();
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    this.reconnectTimer = null;
    try {
      this.ws?.close(1000, 'server shutting down');
    } catch {
      /* ignore */
    }
  }

  /** Latest tick seen per symbol — used to seed prices after a reconnect. */
  /** Diagnostics + latest tick per symbol, used to seed prices after a reconnect. */
  status() {
    const lastTicks = {};
    for (const [symbol, tick] of this.lastTicks) {
      lastTicks[symbol] = {
        bid: Number(tick.bid),
        ask: Number(tick.ask),
        mid: Number(tick.mid ?? tick.bid),
        timestamp: tick.timestamp,
      };
    }
    return {
      connected: this.connected,
      subscribed: this.subscribed,
      url: this.url,
      symbols: this.symbols,
      ticksReceived: this.ticksReceived,
      reconnects: this.reconnects,
      backoffAttempt: this.attempt,
      uptimeMs: this.lastOpenAt ? Date.now() - this.lastOpenAt : 0,
      secondsSinceLastTick: this.lastMessageAt
        ? Math.round((Date.now() - this.lastMessageAt) / 1000)
        : null,
      lastError: this.lastError,
      lastTicks,
    };
  }

  /* ------------------------------ internals ------------------------------- */

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
      return; // already open or opening — never stack sockets
    }

    log.info(`connecting to tick feed … (attempt #${this.attempt + 1})`);

    let ws;
    try {
      ws = new WebSocket(this.url, { handshakeTimeout: 15_000 });
    } catch (e) {
      this.lastError = `construct: ${e.message}`;
      log.error('WebSocket construction failed:', e.message);
      this.onError?.(e);
      this.#scheduleReconnect();
      return;
    }

    this.ws = ws;
    this.subscribed = false;

    ws.on('open', () => {
      // Step 1 — SignalR handshake
      this.#send({ protocol: 'json', version: 1 });
    });

    ws.on('message', (raw) => {
      this.lastMessageAt = Date.now();
      for (const frame of raw.toString().split(RS)) {
        if (!frame) continue;
        let msg;
        try {
          msg = JSON.parse(frame);
        } catch {
          log.warn('unparseable frame from upstream — ignored');
          continue;
        }
        try {
          this.#handle(msg);
        } catch (e) {
          // A bad frame must never kill the socket or the process.
          log.error('frame handler threw:', e.message);
        }
      }
    });

    ws.on('error', (e) => {
      this.lastError = e.message;
      log.error('WebSocket error:', e.message);
      this.onError?.(e);
    });

    ws.on('close', (code, reason) => {
      this.connected = false;
      this.subscribed = false;
      this.#clearTimers();
      log.warn(`connection closed (code=${code}${reason ? `, reason="${reason}"` : ''})`);
      this.onClose?.(code, reason?.toString() || '');
      if (this.stopped || code === 1000) return; // deliberate shutdown
      this.#scheduleReconnect();
    });
  }

  #handle(msg) {
    // ---- Step 2 — handshake acknowledgement (no `type` field) ----
    if (msg.type === undefined) {
      if (msg.error) {
        this.lastError = `handshake: ${msg.error}`;
        log.error('SignalR handshake rejected:', msg.error);
        return;
      }
      this.connected = true;
      this.attempt = 0; // a successful handshake resets the backoff
      this.lastOpenAt = Date.now();
      this.lastError = null;
      log.ok('connected to upstream tick feed');

      // ---- Step 3 — subscribe ----
      this.#send({
        type: 1,
        invocationId: String(++this.invocationId),
        target: 'Subscribe',
        arguments: [this.symbols],
      });
      this.#startTimers();

      // onOpen backfills history — awaited off the socket's critical path so a
      // slow REST call can never delay incoming ticks.
      Promise.resolve(this.onOpen?.()).catch((e) => log.error('onOpen failed:', e.message));
      return;
    }

    switch (msg.type) {
      // ---- invocation result ----
      case 3:
        if (msg.error) {
          this.lastError = `subscribe: ${msg.error}`;
          log.error('Subscribe rejected:', msg.error);
        } else {
          log.ok(`Subscribe confirmed${msg.result ? `: ${JSON.stringify(msg.result).slice(0, 120)}` : ''}`);
          this.subscribed = true;
        }
        return;

      // ---- stream messages ----
      case 1: {
        if (msg.target === 'ReceiveTick' && Array.isArray(msg.arguments)) {
          for (const tick of msg.arguments) {
            if (tick?.symbol) {
              this.ticksReceived += 1;
              this.lastTicks.set(String(tick.symbol).toUpperCase(), tick);
              this.onTick(String(tick.symbol).toUpperCase(), tick);
            }
          }
        } else if (msg.target === 'ReceiveSubscriptionState' && msg.arguments) {
          this.subscribed = true;
          log.debug(`subscription state: ${JSON.stringify(msg.arguments[0]).slice(0, 160)}`);
        }
        return;
      }

      // ---- ping / pong ----
      case 6:
        this.#send({ type: 6 });
        return;

      default:
        return; // close completion and anything else we do not consume
    }
  }

  #send(obj) {
    if (this.ws?.readyState === WebSocket.OPEN) {
      try {
        this.ws.send(JSON.stringify(obj) + RS);
      } catch (e) {
        log.warn('send failed:', e.message);
      }
    }
  }

  /** min(MAX, BASE × 2ⁿ) with ±50 % jitter. */
  #scheduleReconnect(extraDelay = 0) {
    if (this.stopped || this.reconnectTimer) return;
    this.attempt += 1;
    this.reconnects += 1;
    const { BACKOFF_BASE_MS: base, BACKOFF_MAX_MS: max } = this.env.upstream;
    const cap = Math.min(max, base * 2 ** (this.attempt - 1));
    const delay = Math.round(cap * (0.5 + Math.random() * 0.5) + extraDelay);

    log.warn(`reconnecting in ${(delay / 1000).toFixed(1)}s (backoff attempt ${this.attempt})`);
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.connect();
    }, delay);
    this.reconnectTimer.unref?.();
  }

  #startTimers() {
    this.#clearTimers();

    this.pingTimer = setInterval(() => this.#send({ type: 6 }), this.env.upstream.PING_EVERY_MS);
    this.pingTimer.unref?.();

    this.watchdogTimer = setInterval(
      () => {
        if (!this.connected || !this.lastMessageAt) return;
        const silence = Date.now() - this.lastMessageAt;
        if (silence > this.env.upstream.WATCHDOG_MS) {
          log.warn(`no data for ${Math.round(silence / 1000)}s — forcing reconnect`);
          try {
            this.ws.terminate();
          } catch {
            /* ignore */
          }
        }
      },
      Math.max(1_000, Math.floor(this.env.upstream.WATCHDOG_MS / 2))
    );
    this.watchdogTimer.unref?.();
  }
}

/** fetch() with a timeout, so a hanging upstream cannot stall the boot. */
export async function fetchWithTimeout(url, timeoutMs, init = {}) {
  const ctl = new AbortController();
  const timer = setTimeout(() => ctl.abort(), timeoutMs);
  try {
    return await fetch(url, {
      signal: ctl.signal,
      headers: { accept: 'application/json', ...(init.headers ?? {}) },
      ...init,
    });
  } finally {
    clearTimeout(timer);
  }
}

export default TickFeed;