/* =============================================================================
 *  AETHERON CORE  —  src/routes/sse.js
 *  -----------------------------------------------------------------------------
 *  Mounts the three downstream SSE endpoints:
 *
 *    GET /stream                 legacy public chart feed (snapshot + update)
 *    GET /api/stream/prices      clean price ticks for the Android chart
 *    GET /api/stream/chat-events JSON chat-ops messages
 *
 *  AUTHENTICATION
 *  --------------
 *  `/stream` is public (it carries market data only — the same thing a browser
 *  dashboard can see). The two `/api/stream/*` endpoints require a token,
 *  because chat-events contains a user's own executions and quota state.
 *
 *  EventSource cannot set an Authorization header, so `?access_token=` is
 *  accepted as well. That is the standard SSE pattern, and the token is
 *  verified server-side exactly like a header — it is never trusted blindly.
 *
 *  WHY SSE AND NOT WEBSOCKET
 *  -------------------------
 *  The downstream need is one-way with occasional reconnects. SSE gets that
 *  over plain HTTP with automatic reconnection, proxy traversal and no framing
 *  code. Upstream, where latency and bidirectional control matter, stays a
 *  WebSocket.
 * ============================================================================= */

import express from 'express';
import { ENV } from '../config/env.js';
import { normaliseSymbol, normaliseInterval, getSymbolSpec, SYMBOLS, TIMEFRAMES } from '../config/db.js';
import { createLogger } from '../services/logger.js';

const log = createLogger('routes:sse');

/**
 * @param {object} deps
 * @param {import('express').Application} deps.app
 * @param {import('../services/sseHub.js').SSEHub} deps.hub
 * @param {import('../services/sseDispatcher.js').SSEDispatcher} deps.dispatcher
 * @param {import('../services/marketAggregator.js').MarketAggregator} deps.market
 * @param {ReturnType<import('../middleware/auth.js').createAuth>} deps.auth
 * @param {import('../middleware/tradeGuard.js').default} deps.tradeGuard
 * @param {() => object} deps.healthProvider
 */
export function mountSseRoutes({ app, hub, dispatcher, market, auth, tradeGuard, healthProvider }) {
  const router = express.Router();

  /* ====================================================================== */
  /*  LEGACY PUBLIC CHART FEED — unchanged contract, so the dashboard keeps */
  /*  working exactly as before the refactor.                              */
  /* ====================================================================== */
  router.get('/stream', tradeGuard, (req, res) => {
    const symbolFilter = normaliseSymbol(req.query.symbol);
    const intervalFilter = normaliseInterval(req.query.interval);

    const client = hub.add(req, res, {
      channels: ['market', '*'],
      name: 'market',
      filter: (item) => (!symbolFilter || item.symbol === symbolFilter) && (!intervalFilter || item.interval === intervalFilter),
    });

    const snapshot = market.snapshot();
    snapshot.upstream = healthProvider().upstream;
    hub.sendTo(client, 'snapshot', snapshot);

    const total = Object.values(snapshot.series).reduce((n, s) => n + s.candles.length, 0);
    log.raw(`snapshot → ${client.id}: ${total} candles across ${Object.keys(snapshot.series).length} series`);
  });

  /* ====================================================================== */
  /*  /api/stream/prices — clean price data for the TradingView chart      */
  /* ====================================================================== */
  router.get(
    '/api/stream/prices',
    tradeGuard,
    ENV.auth.REQUIRED ? auth.sse : auth.optionalSse,
    (req, res) => {
      // Filters are validated against the symbol registry before use. An
      // unrecognised symbol is a client error, not a silent "all symbols" —
      // otherwise a typo would quietly return 4× the expected bandwidth.
      const symbol = req.query.symbol ? normaliseSymbol(req.query.symbol) : null;
      if (req.query.symbol && !symbol) {
        return res.status(400).json({
          error: { code: 'bad_symbol', message: 'Unknown symbol', valid: SYMBOLS },
        });
      }
      const filters = {
        symbol: symbol ?? undefined,
        interval: normaliseInterval(req.query.interval) ?? 300,
      };

      dispatcher.openPrices(hub, req, res, { market, filters });
    }
  );

  /** REST twin of the price stream (useful for tests and polling clients). */
  router.get('/api/market/price', tradeGuard, (req, res) => {
    const raw = String(req.query.symbol ?? '');
    const symbol = normaliseSymbol(raw);
    if (!symbol) {
      return res.status(400).json({ error: { code: 'bad_symbol', message: 'Unknown symbol', valid: SYMBOLS } });
    }
    const interval = normaliseInterval(req.query.interval) ?? 300;
    const spec = getSymbolSpec(symbol);
    const price = market.priceOf(symbol, interval);
    if (price == null) {
      return res.status(503).json({ error: { code: 'no_price', message: 'No tick received yet for this series' } });
    }
    res.json({
      symbol,
      display: spec.display,
      digits: spec.digits,
      interval,
      price,
      candle: market.formingCandle(symbol, interval),
      serverTime: new Date().toISOString(),
    });
  });

  /** Lightweight REST twin of the chart feed (one series, ring-capped). */
  router.get('/api/market/candles', tradeGuard, (req, res) => {
    const symbol = normaliseSymbol(req.query.symbol);
    const interval = normaliseInterval(req.query.interval);
    if (!symbol || interval === null) {
      return res.status(400).json({
        error: { code: 'bad_series', message: 'symbol and interval are required', validSymbols: SYMBOLS, validIntervals: TIMEFRAMES.map((t) => t.seconds) },
      });
    }
    const slot = market.store.get(`${symbol}|${interval}`);
    res.json({
      symbol,
      display: slot.display,
      interval,
      intervalLabel: slot.intervalLabel,
      ready: slot.ready,
      restored: slot.restored,
      fresh: market.isFresh(slot),
      maxCandles: market.maxCandles,
      candles: slot.candles,
      current: slot.current,
    });
  });

  /** Full state as JSON — the original /api/snapshot endpoint. */
  router.get('/api/snapshot', tradeGuard, (_req, res) => {
    const snapshot = market.snapshot();
    snapshot.upstream = healthProvider().upstream;
    res.json(snapshot);
  });

  /* ====================================================================== */
  /*  /api/stream/chat-events — executions, signals, health                */
  /* ====================================================================== */
  router.get(
    '/api/stream/chat-events',
    tradeGuard,
    ENV.auth.REQUIRED ? auth.sse : auth.optionalSse,
    async (req, res) => {
      await dispatcher.openChat(hub, req, res, {
        user: req.user ?? null,
        requireAuth: ENV.auth.REQUIRED,
      });
    }
  );

  /** Connectors index — helps the Android developer discover the streams. */
  router.get('/api/stream', tradeGuard, (_req, res) => {
    res.json({
      streams: [
        {
          path: '/api/stream/prices',
          auth: ENV.auth.REQUIRED,
          events: ['hello', 'price'],
          description: 'Clean price ticks + live candle, coalesced every ' + ENV.stream.INTERVAL_MS + 'ms',
          query: ['symbol', 'interval'],
        },
        {
          path: '/api/stream/chat-events',
          auth: ENV.auth.REQUIRED,
          events: ['hello', 'backlog', 'message', 'auth'],
          description: 'Execution logs, trade confirmations, signal alerts, system health',
        },
        {
          path: '/stream',
          auth: false,
          events: ['snapshot', 'update', 'status'],
          description: 'Legacy public chart feed (dashboard)',
          query: ['symbol', 'interval'],
        },
      ],
      authHint: 'Pass `Authorization: Bearer <token>` or `?access_token=<token>`. EventSource cannot set headers.',
      active: hub.summary,
    });
  });

  app.use(router);
  log.raw('mounted /stream, /api/stream/prices, /api/stream/chat-events');
  return router;
}

export default mountSseRoutes;