/* =============================================================================
 *  AETHERON CORE  —  server.js
 *  -----------------------------------------------------------------------------
 *  HTTP entry point. Everything of substance lives in src/:
 *
 *    src/config     env.js (all tunables) · db.js (symbol + timeframe registry)
 *    src/services   websocket.js · supabase.js · marketAggregator.js
 *                   sseHub.js · sseDispatcher.js · brokerApi.js · logger.js
 *    src/engine     strategyEngine.js · indicatorCalc.js
 *    src/middleware auth.js · quotaManager.js · tradeGuard.js
 *    src/routes     sse.js · user.js · telegram.js
 *
 *  This file does three things and nothing else:
 *    1. builds the application (src/app.js),
 *    2. listens on PORT/HOST (injected by Render),
 *    3. shuts everything down cleanly on SIGINT/SIGTERM.
 *
 *  BOOT ORDER (see src/app.js for the full rationale)
 *  ------------------------------------------------
 *    Supabase → rehydrate 100 daily candles → HTTP up → upstream WS → engine
 *
 *  QUICK REFERENCE
 *  ---------------
 *    GET /                              dashboard
 *    GET /health                        liveness + diagnostics
 *    GET /stream                        legacy public chart feed
 *    GET /api/stream/prices             clean price ticks        (auth)
 *    GET /api/stream/chat-events        execution / signal chat  (auth)
 *    GET /api/user/me                   plan + quota + sizing    (auth)
 *    GET /api/user/positions            engine's open positions  (auth)
 *    GET /api/user/executions           audit trail             (auth)
 *    GET /api/user/engine               live strategy state      (auth)
 *
 *    There is no trade endpoint. The server decides when to trade.
 * ============================================================================= */

import http from 'node:http';
import { pathToFileURL } from 'node:url';

import { ENV } from './src/config/env.js';
import { createApplication } from './src/app.js';
import { createLogger } from './src/services/logger.js';

const log = createLogger('server');

/* ---- Re-exported for the test suite / any external importer --------------- */
export { ENV };
export { createApplication };

/* ---- Named re-exports so `import { X } from './server.js'` keeps working -- */
export { TickFeed as BiquoteFeed } from './src/services/websocket.js';
export { MarketAggregator } from './src/services/marketAggregator.js';
export { SSEHub } from './src/services/sseHub.js';
export { SSEDispatcher, CHAT_TYPES } from './src/services/sseDispatcher.js';
export { SupabaseService } from './src/services/supabase.js';
export { createBroker, PaperBroker, Mt5BridgeBroker } from './src/services/brokerApi.js';
export { AuthService, createAuth, tokenFromRequest } from './src/middleware/auth.js';
export { QuotaManager, createRateLimiter, PLANS } from './src/middleware/quotaManager.js';
export { tradeGuard, apiManifest } from './src/middleware/tradeGuard.js';
export { StrategyEngine, ENGINE_STATE } from './src/engine/strategyEngine.js';
export * as indicators from './src/engine/indicatorCalc.js';
export {
  SYMBOLS,
  SERIES,
  TIMEFRAMES,
  seriesKey,
  normaliseSymbol,
  normaliseLot,
  pointsValue,
} from './src/config/db.js';

/** Closed candles retained per series (kept for backwards compatibility). */
export const MAX_CANDLES = ENV.market.MAX_CANDLES;

/** The legacy CONFIG object shape, still imported by older tooling. */
export const CONFIG = Object.freeze({
  PORT: ENV.server.PORT,
  HOST: ENV.server.HOST,
  STREAM_INTERVAL_MS: ENV.stream.INTERVAL_MS,
  SSE_HEARTBEAT_MS: ENV.stream.HEARTBEAT_MS,
  STATUS_INTERVAL_MS: ENV.stream.STATUS_INTERVAL_MS,
  SSE_MAX_BUFFER_BYTES: ENV.stream.MAX_BUFFER_BYTES,
  MAX_CANDLES: ENV.market.MAX_CANDLES,
  BIQUOTE: { ...ENV.upstream },
});

/* =============================================================================
 *  START
 * ============================================================================= */

/** True only when this file was executed directly (`node server.js`). */
const isEntryPoint =
  Boolean(process.argv[1]) && import.meta.url === pathToFileURL(process.argv[1]).href;

if (isEntryPoint) {
  const ctx = await start();
  registerShutdown(ctx);
}

/**
 * Build the app, attach an HTTP listener and start the upstream feed.
 * @returns {Promise<{ctx:object, server:import('node:http').Server}>}
 */
export async function start({ env = ENV } = {}) {
  const ctx = await createApplication({ env });

  const server = http.createServer(ctx.app);

  // SSE connections are long-lived; the default 5s keep-alive timeout would cut
  // idle streams. This only affects the *request* keep-alive window, not the
  // stream itself, but it still matters for reconnect behaviour.
  server.keepAliveTimeout = 65_000;
  server.headersTimeout = 70_000;
  // Never let Render's request timeout kill a stream mid-flight.
  server.requestTimeout = 0;
  server.setTimeout(0);

  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(env.server.PORT, env.server.HOST, resolve);
  });

  log.ok(`listening on http://${env.server.HOST}:${env.server.PORT}`);
  log.raw(`  Dashboard  : http://localhost:${env.server.PORT}/`);
  log.raw(`  Prices SSE : /api/stream/prices`);
  log.raw(`  Chat SSE   : /api/stream/chat-events`);
  log.raw(`  Health     : /health`);

  // The upstream feed starts last so the HTTP surface is already answering.
  ctx.feed.start();

  return { ctx, server };
}

/* =============================================================================
 *  SHUTDOWN
 * ============================================================================= */

let shuttingDown = false;

function registerShutdown({ ctx, server }) {
  const graceMs = ENV.server.SHUTDOWN_GRACE_MS;

  const shutdown = async (signal) => {
    if (shuttingDown) return;
    shuttingDown = true;
    log.raw(`\n${signal} received — shutting down gracefully…`);

    // Flush candle writes BEFORE the process exits, so no closed bar is lost.
    await ctx.close().catch((e) => log.error('shutdown cleanup failed:', e.message));

    await new Promise((resolve) => {
      server.close(() => {
        log.ok('HTTP server closed. Bye!');
        resolve();
      });
      // SSE sockets keep the server open; the hub already ended them, so give
      // the OS a moment, then leave regardless.
      setTimeout(resolve, graceMs).unref?.();
    });

    process.exit(0);
  };

  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('unhandledRejection', (reason) => {
    log.error('Unhandled rejection:', reason?.message ?? reason);
  });
  process.on('uncaughtException', (err) => {
    // An uncaught exception leaves the process in an unknown state. Log it,
    // flush what we can, and let the platform restart us cleanly.
    log.error('Uncaught exception:', err?.stack ?? err?.message ?? err);
    ctx.close()
      .catch(() => {})
      .finally(() => process.exit(1));
  });
}

export default { start, isEntryPoint };