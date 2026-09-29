/* =============================================================================
 *  MOCK biquote.io SERVER  —  test/only, not part of the runtime
 *  Developed by Eng Hasan Mohamad
 * -----------------------------------------------------------------------------
 *  Speaks the same dialect the engine consumes, so the whole pipeline
 *  (REST backfill → tick aggregation → rollover → SSE) can be verified without
 *  touching the real upstream.
 *
 *  Implements:
 *    GET  /api/{symbol}                     latest tick
 *    GET  /api/{symbol}/ohlc?interval=&limit=   candles, newest first,
 *                                               `isOpen` on the forming one
 *    WS   /hubs/tick                        SignalR: handshake, Subscribe,
 *                                            ReceiveTick, ping
 *
 *  Run standalone:  node test/mock-biquote.mjs
 * ============================================================================= */

import { WebSocketServer } from 'ws';
import http from 'node:http';

const BASE = { XAUUSD: 4159.5, EURUSD: 1.1344, USDJPY: 157.43, GBPUSD: 1.3232 };
const DECIMALS = { XAUUSD: 2, EURUSD: 5, USDJPY: 3, GBPUSD: 5 };
const INTERVALS = { '1m': 60, '5m': 300, '15m': 900, '30m': 1800, '1h': 3600, '4h': 14400, '1d': 86400 };

const round = (s, v) => Number(v.toFixed(DECIMALS[s] ?? 5));

/** Per-symbol random-walk price. */
const prices = { ...BASE };
const walk = (symbol) => {
  prices[symbol] += BASE[symbol] * 0.00015 * (Math.random() - 0.5);
  return prices[symbol];
};

const isoOf = (epochSec) => new Date(epochSec * 1000).toISOString().replace('.000', '.000');

/** Build `count` candles ending at the currently-forming one (newest first). */
function buildBars(symbol, interval, count) {
  const now = Math.floor(Date.now() / 1000);
  const newest = Math.floor(now / interval) * interval;
  const bars = [];
  // Walk backwards so the newest bar contains the most recent movement
  for (let i = count - 1; i >= 0; i--) {
    const openTime = newest - i * interval;
    const drift = (Math.random() - 0.5) * BASE[symbol] * 0.0015;
    const o = openTime === newest ? prices[symbol] : prices[symbol] - drift;
    const c = openTime === newest ? walk(symbol) : o + drift * 0.6;
    const hi = Math.max(o, c) + Math.abs(drift) * 0.4;
    const lo = Math.min(o, c) - Math.abs(drift) * 0.4;
    bars.push({
      openTime: isoOf(openTime),
      open: round(symbol, o),
      high: round(symbol, hi),
      low: round(symbol, lo),
      close: round(symbol, c),
      volume: 0,
      tickVolume: 500 + Math.floor(Math.random() * 1500),
      isOpen: i === 0, // the newest bar is the forming one
    });
  }
  return bars.reverse(); // newest first, like the real API
}

const makeTick = (symbol) => {
  const mid = walk(symbol);
  return {
    symbol,
    bid: round(symbol, mid * 0.99999),
    ask: round(symbol, mid * 1.00001),
    last: 0,
    volume: 0,
    timestamp: new Date().toISOString(),
    source: 'MetaTrader 5 (Mock)',
    high: round(symbol, mid * 1.002),
    low: round(symbol, mid * 0.998),
    direction: Math.random() > 0.5 ? 'UP' : 'DOWN',
    mid: round(symbol, mid),
  };
};

/**
 * @param {object} opts
 * @param {number} opts.port
 * @param {number} [opts.tickMs]        interval between live ticks
 * @param {number} [opts.rolloverEvery] 0 = real time, >0 = force candle rollovers
 */
export async function startMockBiquote({ port = 4599, tickMs = 300, rolloverEvery = 0, onLog = () => {} } = {}) {
  /* ---------------- REST ---------------- */
  const rest = http.createServer(async (req, res) => {
    const url = new URL(req.url, `http://127.0.0.1:${port}`);
    const send = (code, body) => {
      res.writeHead(code, { 'content-type': 'application/json' });
      res.end(JSON.stringify(body));
    };
    const parts = url.pathname.split('/').filter(Boolean); // ['api', symbol, 'ohlc']
    const symbol = parts[1];
    if (parts[0] !== 'api' || !BASE[symbol]) return send(404, { message: 'unknown symbol' });

    if (parts[2] === 'ohlc') {
      const interval = url.searchParams.get('interval') ?? '1h';
      if (!INTERVALS[interval]) {
        return send(400, { message: `Invalid interval '${interval}'`, validIntervals: Object.keys(INTERVALS) });
      }
      const limit = Math.min(500, Number(url.searchParams.get('limit') ?? 100));
      onLog('[mock] ohlc', symbol, interval, `limit=${limit}`);
      return send(200, { symbol, interval, bars: buildBars(symbol, INTERVALS[interval], limit) });
    }

    return send(200, makeTick(symbol));
  });

  /* ---------------- WebSocket (SignalR) ---------------- */
  const wss = new WebSocketServer({ server: rest, path: '/hubs/tick' });
  const RS = '\u001e';
  const sockets = new Set();

  // Start listening once the WS server is attached to the HTTP server
  await new Promise((resolve, reject) => {
    rest.once('error', reject);
    rest.listen(port, '127.0.0.1', resolve);
  });

  wss.on('connection', (ws) => {
    onLog('[mock] client connected');
    let subscribed = [];
    let tickTimer = null;

    const send = (o) => ws.readyState === ws.OPEN && ws.send(JSON.stringify(o) + RS);

    ws.on('message', (raw) => {
      for (const frame of raw.toString().split(RS)) {
        if (!frame) continue;
        let msg;
        try {
          msg = JSON.parse(frame);
        } catch {
          continue;
        }

        // handshake
        if (msg.type === undefined) {
          onLog('[mock] handshake');
          send({});
          continue;
        }
        // ping
        if (msg.type === 6) {
          send({ type: 6 });
          continue;
        }
        // Subscribe
        if (msg.type === 1 && msg.target === 'Subscribe') {
          subscribed = (msg.arguments?.[0] ?? []).filter((s) => BASE[s]);
          onLog('[mock] Subscribe', subscribed.join(','));
          send({ type: 6 }); // completion for the invoke
          send({ type: 1, target: 'ReceiveSubscriptionState', arguments: [{ symbols: subscribed, count: subscribed.length, connected: true }] });
          if (tickTimer) clearInterval(tickTimer);
          tickTimer = setInterval(() => {
            for (const s of subscribed) {
              send({ type: 1, target: 'ReceiveTick', arguments: [makeTick(s)] });
            }
          }, tickMs);
          return;
        }
      }
    });

    const stop = () => {
      if (tickTimer) clearInterval(tickTimer);
      sockets.delete(ws);
    };
    ws.on('close', stop);
    ws.on('error', stop);
    sockets.add(ws);
  });

  /* Optional: force candle rollovers so the closed-candle path is testable
     in seconds instead of waiting for a real 5-minute boundary. */
  if (rolloverEvery > 0) {
    setInterval(() => {
      for (const s of Object.keys(BASE)) prices[s] = BASE[s] * (1 + (Math.random() - 0.5) * 0.004);
    }, rolloverEvery);
  }

  return {
    // Plain ws:// — the mock speaks no TLS, so the engine must be able to talk
    // to a non-wss upstream (the real biquote.io endpoint is wss://).
    url: `ws://127.0.0.1:${port}/hubs/tick`,
    api: `http://127.0.0.1:${port}`,
    port,
    /** Simulate an upstream outage (used by the reconnect test). */
    closeAllClients: () => sockets.forEach((c) => c.terminate()),
    close: () =>
      new Promise((resolve) => {
        sockets.forEach((c) => c.terminate());
        wss.close(() => rest.close(() => resolve()));
      }),
  };
}

// Run standalone:  node test/mock-biquote.mjs
if (process.argv[1] && process.argv[1].endsWith('mock-biquote.mjs')) {
  const port = Number(process.env.MOCK_PORT ?? 4599);
  await startMockBiquote({ port, onLog: (...a) => console.log(...a) });
  console.log(`[mock] biquote-compatible server on http/ws://127.0.0.1:${port}`);
}
