/* =============================================================================
 *  MOCK DERIV WEBSOCKET SERVER  —  test/only, not part of the runtime
 *  Developed by Eng Hasan Mohamad
 * -----------------------------------------------------------------------------
 *  Speaks the exact Deriv API dialect the engine uses, so the whole pipeline
 *  (backfill → aggregation → rollover → SSE) can be verified without network
 *  access to Deriv.
 *
 *  Supports:
 *    • ping / pong
 *    • ticks_history: "candle"  (history, deterministic interval from start/end)
 *    • ticks_history: "candle" + subscribe: 1  → then a live `ohlc` stream
 *    • ticks: "tick" + subscribe: 1  → then a live `tick` stream
 *    • forced candle rollovers (so rollover/trim logic is testable in seconds)
 * ============================================================================= */

import { WebSocketServer } from 'ws';

const BASE = {
  frxXAUUSD: 2412.55,
  frxEURUSD: 1.0842,
  frxUSDJPY: 157.32,
  frxGBPUSD: 1.2718,
};
const DECIMALS = { frxXAUUSD: 2, frxEURUSD: 5, frxUSDJPY: 3, frxGBPUSD: 5 };

const round = (symbol, v) => Number(v.toFixed(DECIMALS[symbol] ?? 5));

/** The candle granularities Deriv supports, in seconds. */
const GRANULARITIES = [
  60, 120, 180, 300, 600, 900, 1800, 3600, 7200, 14400, 28800, 86400,
];
/** Snap an arbitrary span to the nearest supported granularity (like Deriv). */
const snapGranularity = (raw) =>
  GRANULARITIES.reduce((best, g) => (Math.abs(g - raw) < Math.abs(best - raw) ? g : best), 900);

/** Per-symbol random-walk price. */
const prices = { ...BASE };
const walk = (symbol) => {
  const step = BASE[symbol] * 0.0002 * (Math.random() - 0.5);
  prices[symbol] = prices[symbol] + step;
  return prices[symbol];
};

/**
 * @param {object} opts
 * @param {number} opts.port
 * @param {number} [opts.tickMs]        interval between live candle updates
 * @param {number} [opts.rolloverEvery] ms after which a candle rollover is forced
 */
export function startMockDeriv({
  port = 4599,
  tickMs = 250,
  rolloverEvery = 2500,
  onLog = () => {},
} = {}) {
  const wss = new WebSocketServer({ port, host: '127.0.0.1' });
  let subSeq = 0;
  const timers = new Set();

  wss.on('connection', (ws) => {
    /** symbol → interval remembered from the most recent history request */
    const lastInterval = new Map();
    /** active candle streams */
    const candleSubs = [];
    const tickSubs = [];
    const timersLocal = new Set();
    timers.add(timersLocal);

    const log = (...a) => onLog('[mock]', ...a);
    const send = (o) => ws.readyState === ws.OPEN && ws.send(JSON.stringify(o));

    /** The forming-candle epoch each subscription is currently on. */
    const formingEpoch = (interval, offset = 0) =>
      Math.floor((Date.now() / 1000 + offset) / interval) * interval;

    /* ---------------- request handling ---------------- */
    ws.on('message', (raw) => {
      let req;
      try {
        req = JSON.parse(raw.toString());
      } catch {
        return;
      }

      // keepalive
      if (req.ping) return send({ msg_type: 'pong', echo: req.ping, req_id: req.req_id });

      /* ---- tick subscription ---- */
      if (req.ticks === 'tick' && req.subscribe) {
        const id = `sub-t${++subSeq}`;
        const symbol = req.symbol;
        tickSubs.push({ id, symbol });
        log(`tick subscribe ${symbol} → ${id}`);
        return send({
          msg_type: 'tick',
          req_id: req.req_id,
          subscription: { id, symbol, subscription_type: 'ticks' },
          tick: makeTick(symbol),
        });
      }

      /* ---- candle history / subscription ---- */
      if (req.ticks_history === 'candle') {
        const symbol = req.symbol;

        // Deriv has no interval parameter: it derives the candle interval from
        // the requested window, (end - start) / (count - 1), snapped to the
        // nearest supported granularity.
        let interval = lastInterval.get(symbol) ?? 60;
        if (req.start != null && req.end != null) {
          const raw = (req.end - req.start) / Math.max(1, (req.count ?? 2) - 1);
          interval = snapGranularity(raw);
        }
        lastInterval.set(symbol, interval);

        const id = `sub-c${++subSeq}`;
        const times = [];
        const prices_ = [];
        const count = Math.max(1, req.count ?? 1);
        // Candle epochs are aligned to the interval grid, newest last
        const newest = Math.floor(Date.now() / 1000 / interval) * interval;
        for (let i = count; i >= 1; i--) {
          const t = newest - (i - 1) * interval;
          const p = walk(symbol);
          times.push({ epoch: t });
          prices_.push(String(round(symbol, p)));
        }

        const subscription = {
          id,
          symbol,
          subscription_type: 'ticks_history',
        };

        if (req.subscribe) {
          candleSubs.push({ id, symbol, interval, offset: 0, current: makeCandle(symbol, newest) });
          log(`candle subscribe ${symbol} @${interval}s → ${id}`);
          // Deriv answers a subscribe with the history it would have returned
          send({
            msg_type: 'history',
            req_id: req.req_id,
            subscription,
            history: { prices: prices_, times, tick_volumes: prices_.map(() => 42) },
          });
          return;
        }

        log(`history ${symbol} count=${count} interval=${interval}s`);
        return send({
          msg_type: 'history',
          req_id: req.req_id,
          subscription,
          history: { prices: prices_, times, tick_volumes: prices_.map(() => 42) },
        });
      }

      send({ error: { code: 'BadRequest', message: `unsupported: ${JSON.stringify(req)}` } });
    });

    /* ---------------- synthetic market data ---------------- */
    const makeTick = (symbol) => ({
      epoch: Math.floor(Date.now() / 1000),
      tick_display: null,
      bid: round(symbol, prices[symbol] * 0.99999),
      ask: round(symbol, prices[symbol] * 1.00001),
      quote: round(symbol, walk(symbol)),
    });

    const makeCandle = (symbol, epoch) => {
      const p = walk(symbol);
      return { epoch, open: p, high: p, low: p, close: p, tick_volume: 100 };
    };

    // Live candle stream
    const candleTimer = setInterval(() => {
      for (const sub of candleSubs) {
        const epoch = formingEpoch(sub.interval, sub.offset);
        if (sub.current.epoch !== epoch) sub.current = makeCandle(sub.symbol, epoch);
        const c = sub.current;
        const p = walk(sub.symbol);
        c.close = round(sub.symbol, p);
        c.high = Math.max(c.high, c.close);
        c.low = Math.min(c.low, c.close);
        c.tick_volume += 1;
        send({
          msg_type: 'ohlc',
          subscription: { id: sub.id, symbol: sub.symbol, subscription_type: 'ticks_history' },
          candle: c,
        });
      }
    }, tickMs);

    // Live tick stream
    const tickTimer = setInterval(() => {
      for (const sub of tickSubs) {
        send({
          msg_type: 'tick',
          subscription: { id: sub.id, symbol: sub.symbol, subscription_type: 'ticks' },
          tick: makeTick(sub.symbol),
        });
      }
    }, tickMs);

    // Force candle rollovers so the closed-candle push + 100-cap is testable
    const rollTimer = setInterval(() => {
      for (const sub of candleSubs) {
        sub.offset += sub.interval;
        sub.current = makeCandle(sub.symbol, formingEpoch(sub.interval, sub.offset));
      }
    }, rolloverEvery);

    const stop = () => {
      clearInterval(candleTimer);
      clearInterval(tickTimer);
      clearInterval(rollTimer);
      timers.delete(timersLocal);
    };
    ws.on('close', stop);
    ws.on('error', stop);
  });

  return {
    url: `ws://127.0.0.1:${port}/websockets/v3`,
    port,
    /** Simulate an upstream outage (used by the reconnect test). */
    closeAllClients: () => wss.clients.forEach((c) => c.terminate()),
    close: () =>
      new Promise((resolve) => {
        for (const t of timers) clearInterval(t);
        wss.clients.forEach((c) => c.terminate());
        wss.close(() => resolve());
      }),
  };
}

// Allow running standalone:  node test/mock-deriv.mjs
if (process.argv[1] && process.argv[1].endsWith('mock-deriv.mjs')) {
  const port = Number(process.env.MOCK_PORT ?? 4599);
  startMockDeriv({ port, onLog: (...a) => console.log(...a) });
  console.log(`[mock] Deriv-compatible server listening on ws://127.0.0.1:${port}/websockets/v3`);
}
