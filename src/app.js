/* =============================================================================
 *  AETHERON CORE  —  src/app.js
 *  -----------------------------------------------------------------------------
 *  Composition root. Builds every service, wires the event flow and returns a
 *  handle the HTTP entry point (server.js) can listen and shut down with.
 *
 *  THE EVENT FLOW
 *  --------------
 *    upstream WebSocket (services/websocket.js)
 *        │  onTick(symbol, tick)
 *        ▼
 *    MarketAggregator  ──► closed candle ──► SupabaseService (write-behind queue)
 *        │                    ───────────────► StrategyEngine.evaluateEntry()
 *        │  onFormingTick(price)                    │
 *        │                    │                    ▼
 *        │                    └──► StrategyEngine.evaluateExits()  (throttled)
 *        │                                    │            │
 *        │                                    │            ▼
 *        │                                    │      BrokerApi (server-only)
 *        │                                    ▼
 *        ▼                                     │            │
 *    SSEDispatcher ◄──────────────────────────┴────────────┘
 *        │  prices channel          chat channel (chat-ops messages)
 *        ▼                              ▼
 *    /api/stream/prices         /api/stream/chat-events
 *
 *  BOOT ORDER MATTERS
 *  ------------------
 *    1. Supabase client        — so the very first read can be a DB read
 *    2. rehydrate from Supabase — 100 daily candles in RAM before anything else
 *    3. HTTP listener up       — /health answers immediately, even if (4) fails
 *    4. WebSocket feed         — merges upstream history into what we restored
 *    5. strategy engine        — arms once the 50-day channel and ATR exist
 *
 *  Doing (2) before (4) is the point of the exercise: a restarted server can
 *  evaluate the 50-day Donchian immediately, instead of waiting for a
 *  100-candle REST backfill of a timeframe it may not even support.
 * ============================================================================= */

import express from 'express';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { ENV, auditEnv, publicEnv } from './config/env.js';
import { SYMBOLS, SERIES, TIMEFRAMES } from './config/db.js';
import { createLogger, uptimeLabel } from './services/logger.js';
import { SupabaseService } from './services/supabase.js';
import { TickFeed } from './services/websocket.js';
import { MarketAggregator } from './services/marketAggregator.js';
import { SSEHub } from './services/sseHub.js';
import { SSEDispatcher } from './services/sseDispatcher.js';
import { createBroker } from './services/brokerApi.js';
import { AuthService, createAuth } from './middleware/auth.js';
import { QuotaManager, createRateLimiter } from './middleware/quotaManager.js';
import { tradeGuard } from './middleware/tradeGuard.js';
import { StrategyEngine, ENGINE_STATE } from './engine/strategyEngine.js';
import { mountSseRoutes } from './routes/sse.js';
import { mountUserRoutes } from './routes/user.js';
import { mountAiRoutes } from './routes/ai.js';
import { mountMembershipRoutes, deviceGate } from './routes/membership.js';
import { mountAdminRoutes } from './routes/admin.js';
import { AdminAuth } from './middleware/adminAuth.js';
import { AppUserService } from './services/appUsers.js';
import { MembershipEvents } from './services/membershipEvents.js';
import { AiAnalystManager } from './services/aiAnalystManager.js';
import { EconomicNewsService } from './services/economicNews.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, '..');

/**
 * Build the whole application.
 *
 * @param {object} [opts]
 * @param {object} [opts.env]          config override (tests)
 * @param {object} [opts.userId]       the account the engine trades for
 * @returns {Promise<object>} handle with every service + `close()`
 */
export async function createApplication({ env = ENV, userId = null } = {}) {
  const log = createLogger('boot');
  const audit = auditEnv();

  /* ===================================================================== */
  /*  0. CONFIG AUDIT                                                      */
  /* ===================================================================== */
  for (const w of audit.warnings) log.warn(w);
  if (!audit.ready) {
    for (const p of audit.problems) log.error(p);
    throw new Error(`invalid configuration:\n  - ${audit.problems.join('\n  - ')}`);
  }

  /* ===================================================================== */
  /*  1. SUPABASE                                                          */
  /* ===================================================================== */
  const persistence = new SupabaseService({ env });
  const dbReady = await persistence.init();
  if (!dbReady && env.supabase.REQUIRED) throw new Error(persistence.loadError ?? 'Supabase is required');

  // Prove the credentials work NOW. `createClient()` accepts any non-empty
  // string, so without this the first sign of a bad key would be a wall of
  // failing writes minutes later — long after the deploy log stopped being
  // watched. Verified before anything else starts writing.
  const dbVerified = dbReady ? await persistence.verify() : { ok: false, error: 'not configured' };
  if (dbReady && !dbVerified.ok && env.supabase.REQUIRED) {
    throw new Error(`Supabase credentials rejected: ${dbVerified.error}\n  -> ${dbVerified.remedy}`);
  }

  /* ===================================================================== */
  /*  2. CORE SERVICES                                                     */
  /* ===================================================================== */
  const hub = new SSEHub({ env });
  const dispatcher = new SSEDispatcher({ hub, env, persistence });

  // `engine` is declared first and assigned a few lines below: the market
  // callbacks below reference it, and they only ever fire after construction
  // has finished (the feed is started by the caller).
  let engine = null;
  // The AI analyst is built after the market (it needs candles), but the tick
  // hook below must reach it — hold the reference in the closure.
  let aiSignal = null;

  // The aggregator is created after persistence so it can queue candle writes
  // from its very first tick.
  const market = new MarketAggregator({
    env,
    persistence,
    onClosed: (ev) => engine?.onClosedCandle(ev).catch((e) => log.error('engine.onClosedCandle failed:', e.message)),
    onFormingTick: (symbol, tick, price) => {
      dispatcher.onTick(symbol, tick);
      engine?.onTick(symbol, tick, price);
      // Let the analyst follow its running trade tick-by-tick: a stop has to be
      // honoured between the 30-minute analysis cycles, not at the next one.
      aiSignal?.onPrice(symbol, price);
    },
  });

  const broker = createBroker({ env });
  const auth = createAuth({ supabase: persistence, env });
  const quota = new QuotaManager({ supabase: persistence, auth: auth.service, env });

  // Device-based APK accounts, upgrade requests and the admin console. These
  // use the same Supabase service-role client; no credential ever reaches the
  // phone or the browser.
  const membership = new AppUserService({ persistence, env });
  const membershipEvents = new MembershipEvents();
  const adminAuth = new AdminAuth({ env });
  engine = new StrategyEngine({
    market,
    broker,
    quota,
    dispatcher,
    auth: auth.service,
    persistence,
    env,
    userId,
  });

  const guard = tradeGuard({ logger: log });
  const rateLimiter = createRateLimiter({ logger: log });

  // The AI analyst is fully independent of the strategy: it reads the same
  // candles but can never place an order. Its Telegram handle is attached
  // further down, once (and if) the gateway has mounted.
  //
  // The economic calendar feeds the analyst's prompt (see economicNews.js). It
  // is created here so both the analyst and /health can see it.
  const news = new EconomicNewsService({ env });
  aiSignal = new AiAnalystManager({ market, dispatcher, env, news, persistence });

  /* ===================================================================== */
  /*  3. REHYDRATE FROM SUPABASE                                           */
  /* ===================================================================== */
  // Runs BEFORE the upstream feed, so the 50-day Donchian channel is already
  // in memory when the strategy arms.
  const hydration = await market.hydrateFromDatabase(persistence);

  // Re-attach the AI analyst's open paper trades from the ledger BEFORE the
  // feed starts. The first tick then enforces their stop/targets, and a restart
  // can never open a second trade on the same symbol.
  const aiRestored = await aiSignal.restoreOpenTrades();

  // Recover positions the previous process left open. This runs before the
  // first evaluation so a restart can never double-expose a symbol.
  let restored = 0;
  if (userId) {
    try {
      const openRows = await persistence.listOpenPositions(userId);
      restored = engine.restorePositions(openRows);
    } catch (e) {
      log.error('position recovery failed:', e.message);
    }
  }

  /* ===================================================================== */
  /*  4. HTTP                                                              */
  /* ===================================================================== */
  const app = express();
  app.disable('x-powered-by');
  app.set('trust proxy', 1); // Render terminates TLS and forwards X-Forwarded-*

  // A small body cap. The API has no order endpoint, but an unauthenticated
  // client should never be able to make us buffer a megabyte.
  app.use(express.json({ limit: env.server.BODY_LIMIT }));
  app.use(express.urlencoded({ extended: false, limit: env.server.BODY_LIMIT }));

  /* ---- security headers (hand-rolled: one less dependency) ------------ */
  // Registered BEFORE the static mount so the dashboard, the admin console and
  // every API response share the same policy.
  app.use((_req, res, next) => {
    res.set('X-Content-Type-Options', 'nosniff');
    res.set('X-Frame-Options', 'SAMEORIGIN');
    res.set('Referrer-Policy', 'no-referrer');
    res.set('Permissions-Policy', 'geolocation=(), microphone=(), camera=()');
    // The dashboard loads Lightweight Charts from a CDN and talks only to this
    // origin, so a strict policy costs nothing and blocks injected scripts.
    res.set(
      'Content-Security-Policy',
      "default-src 'self'; script-src 'self' 'unsafe-inline' https://unpkg.com; " +
        "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; " +
        "font-src 'self' https://fonts.gstatic.com data:; img-src 'self' data:; connect-src 'self'"
    );
    next();
  });

  if (env.market.SERVE_PUBLIC) {
    app.use(
      express.static(path.join(ROOT, 'public'), {
        maxAge: env.server.IS_PRODUCTION ? '1h' : 0,
        etag: true,
        setHeaders: (res, filePath) => {
          // The console is a live operator tool: never let a deploy be hidden
          // by a one-hour browser cache.
          if (filePath.includes(`${path.sep}admin${path.sep}`)) {
            res.setHeader('Cache-Control', 'no-store');
            res.setHeader('Pragma', 'no-cache');
          }
        },
      })
    );
    app.get('/', (_req, res) => res.sendFile(path.join(ROOT, 'public', 'index.html')));
  }

  /* ---- the guard runs before every route ------------------------------ */
  app.use(guard);

  /* ===================================================================== */
  /*  5. UPSTREAM FEED                                                     */
  /* ===================================================================== */
  const feed = new TickFeed({
    symbols: env.upstream.SYMBOLS.length ? env.upstream.SYMBOLS : SYMBOLS,
    env,
    onTick: (symbol, tick) => market.applyTick(symbol, tick),

    /** Runs after every (re)connect. */
    onOpen: async () => {
      log.info('bootstrapping market history from upstream…');
      const r = await market.backfillAll();
      market.markAllDirty();
      if (r.failed > 0) log.warn(`${r.failed}/${r.total} series could not be backfilled — persisted candles are still in use`);
      engine.start();

      // Look at the market now instead of waiting for the next daily close —
      // the cooldown keeps this sweep from producing duplicate entries.
      const sweep = await engine.evaluateAll().catch((e) => {
        log.error('initial strategy sweep failed:', e.message);
        return [];
      });
      const armed = engine.phase === ENGINE_STATE.ARMED;
      dispatcher.system(
        armed
          ? `Strategy armed across ${sweep.length} symbol(s) — 50-day Donchian and ATR(20) are live.`
          : `Strategy warming up: ${engine.haltReason ?? 'awaiting history'}`,
        { state: engine.phase, symbols: sweep.length, reason: engine.haltReason }
      );
    },

    onClose: () => {
      // Tell every client the data is now stale so the UI can grey out.
      hub.broadcast('status', {
        type: 'status',
        connected: false,
        serverTime: Date.now(),
        message: 'Upstream connection lost — reconnecting…',
      });
      dispatcher.risk('Upstream market feed disconnected — reconnecting. Data is stale until it returns.', {
        component: 'feed',
      });
    },

    onError: (e) => dispatcher.risk(`Upstream feed error: ${e.message}`, { component: 'feed' }),
  });

  /* ===================================================================== */
  /*  6. HEALTH                                                            */
  /* ===================================================================== */
  const bootInfo = {
    startedAt: Date.now(),
    hydrated: hydration,
    restoredPositions: restored,
    aiRestored,
    dbReady,
    dbVerified,
    version: publicEnv().version,
  };

  const healthPayload = () => ({
    status: 'ok',
    service: 'Aetheron Core',
    version: bootInfo.version,
    uptimeSec: Math.round(process.uptime()),
    uptime: uptimeLabel(),
    upstream: feed.status(),
    // Cached probe — /health must stay fast and must never block on Supabase.
    database: {
      configured: persistence.configured,
      enabled: persistence.enabled,
      verified: bootInfo.dbVerified.ok,
      lastProbe: persistence.lastHealth,
      // When credentials are bad, surface the remedy on /health too — that is
      // often the first place anyone looks.
      remedy: bootInfo.dbVerified.remedy ?? null,
      queue: persistence.getQueueStats(),
    },
    market: market.summary(),
    engine: {
      state: engine.phase,
      reason: engine.haltReason,
      counters: engine.counters,
      openPositions: engine.positions.size,
    },
    quota: quota.stats(),
    auth: auth.service.stats(),
    membership: {
      ...membership.status(),
      restoredOpenTrades: aiRestored.restored,
    },
    admin: { enabled: adminAuth.enabled },
    broker: broker.status(),
    ai: aiSignal.status(),
    sse: { ...hub.summary, chat: dispatcher.stats(), membership: membershipEvents.totalClients },
    memoryMb: Math.round((process.memoryUsage().heapUsed / 1048576) * 10) / 10,
    hydration: bootInfo.hydrated,
  });

  app.get('/health', (_req, res) => res.json(healthPayload()));

  /* ===================================================================== */
  /*  7. ROUTES                                                            */
  /* ===================================================================== */

  /**
   * The Telegram gateway is mounted after the HTTP routes (it needs the same
   * app), but the admin notifier below must be able to reach it. The closure
   * reads this variable at notification time, by which point it is assigned.
   */
  let telegram = null;

  /**
   * Fan an account change out to the app and, when paired, to Telegram.
   *
   * This is deliberately SERVER-SIDE: the phone does not need to poll a
   * payment provider, and a plan cannot be forged by editing a local setting.
   * GET /api/membership/me remains the source of truth; the SSE frame and the
   * Telegram DM are wake-ups, not the authority.
   */
  async function notifyMembership(row, kind = 'plan', ctx = {}) {
    const view = membership.publicUser(row);
    if (!view) return { sse: 0, telegram: false, kind };

    const esc = (s) =>
      String(s ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    const until = view.planExpiresAt ? String(view.planExpiresAt).slice(0, 10) : null;
    const planLabel =
      view.plan === 'YEARLY' ? 'Yearly' : view.plan === 'MONTHLY' ? 'Monthly' : 'Free';

    let text;
    if (kind === 'status') {
      text =
        view.status === 'BANNED'
          ? `⛔ <b>Account suspended</b>\nDevice: <code>${esc(view.deviceId)}</code>\n\nContact support to appeal.`
          : `✅ <b>Account reactivated</b>\nDevice: <code>${esc(view.deviceId)}</code>\nPlan: <b>${planLabel}</b>`;
    } else if (kind === 'manual') {
      text = ctx.message
        ? `ℹ️ <b>Aetheron</b>\n\n${esc(ctx.message)}`
        : `ℹ️ <b>Aetheron</b>\nYour account was reviewed by support.`;
    } else {
      text =
        view.plan === 'FREE'
          ? `ℹ️ <b>Membership changed</b>\nPlan: <b>Free</b>\nDevice: <code>${esc(view.deviceId)}</code>`
          : `✅ <b>Membership upgraded</b>\nPlan: <b>${planLabel}</b>` +
            (until ? `\nValid until: <b>${until}</b>` : '') +
            `\nDevice: <code>${esc(view.deviceId)}</code>`;
    }

    const event = {
      type: 'membership',
      kind,
      user: view,
      message: text,
      by: ctx.by ?? null,
      at: new Date().toISOString(),
    };
    const sse = membershipEvents.publish(view.deviceId, 'membership', event);

    let sent = false;
    if (env.membership.TELEGRAM_UPGRADE && telegram?.notifyDevice) {
      try {
        sent = Boolean(await telegram.notifyDevice(view.deviceId, text));
      } catch (e) {
        log.warn('membership Telegram notify failed:', e.message);
      }
    }
    return { sse, telegram: sent, kind, plan: view.plan, status: view.status };
  }

  mountSseRoutes({
    app,
    hub,
    dispatcher,
    market,
    auth,
    tradeGuard: guard,
    deviceGate: deviceGate({ membership, sse: false }),
    healthProvider: healthPayload,
  });
  mountUserRoutes({ app, auth, quota, engine, broker, persistence, tradeGuard: guard, rateLimiter: rateLimiter.middleware });
  mountAiRoutes({
    app,
    ai: aiSignal,
    tradeGuard: guard,
    deviceGate: deviceGate({ membership, sse: false }),
  });
  mountMembershipRoutes({
    app,
    membership,
    events: membershipEvents,
    engine,
    tradeGuard: guard,
    rateLimiter: rateLimiter.middleware,
    env,
  });
  mountAdminRoutes({
    app,
    adminAuth,
    membership,
    persistence,
    env,
    ai: aiSignal,
    notifyMembership,
    healthProvider: healthPayload,
  });

  /* ---- optional Telegram chat-ops gateway ------------------------------ */
  // Mount when explicitly enabled OR when a bot token exists (the AI analyst
  // can then deliver signals to a configured chat even without device pairing).
  if (env.market.TELEGRAM_ENABLED || process.env.TELEGRAM_BOT_TOKEN) {
    try {
      const { mountTelegramGateway } = await import('./routes/telegram.js');
      telegram = mountTelegramGateway(app, { log: log.raw.bind(log), warn: log.warn.bind(log), err: log.error.bind(log) });
      // Hand the gateway to the analyst for server-originated broadcasts.
      aiSignal.attachTelegram(telegram);
    } catch (e) {
      log.warn('Telegram gateway unavailable:', e.message);
    }
  }

  /* ---- 404 + error handler (must be registered last) ------------------ */
  app.use((req, res) => {
    res.status(404).json({ error: { code: 'not_found', message: `No route for ${req.method} ${req.path}` } });
  });
  app.use((err, _req, res, _next) => {
    // A malformed JSON body is the client's fault, not a server fault.
    if (err?.type === 'entity.parse.failed') {
      return res.status(400).json({ error: { code: 'bad_request', message: 'Malformed JSON body' } });
    }
    if (err?.type === 'entity.too.large') {
      return res.status(413).json({ error: { code: 'payload_too_large', message: 'Request body is too large' } });
    }
    // Validation errors raised by services carry an explicit 4xx status. Pass
    // their message through instead of mislabelling every 400 as bad JSON.
    if (!res.headersSent && Number.isInteger(err?.status) && err.status >= 400 && err.status < 500) {
      return res.status(err.status).json({
        error: { code: err.code || 'bad_request', message: err.message || 'Bad request' },
      });
    }
    log.error('unhandled request error:', err?.message ?? err);
    if (res.headersSent) return undefined;
    return res.status(500).json({ error: { code: 'internal_error', message: 'Internal server error' } });
  });

  /* ===================================================================== */
  /*  8. BACKGROUND LOOPS                                                  */
  /* ===================================================================== */
  // Coalesced market updates for the legacy chart feed. The prices/chat
  // channels run their own timers inside the dispatcher.
  const marketFlushTimer = setInterval(() => {
    const payload = market.flush();
    if (payload) hub.publish('market', 'update', payload, { requiresFilter: true });
  }, env.stream.INTERVAL_MS);
  marketFlushTimer.unref?.();

  const statusTimer = setInterval(() => {
    if (hub.countFor('market') > 0) {
      hub.publish('market', 'status', {
        type: 'status',
        connected: feed.connected,
        serverTime: Date.now(),
        upstream: feed.status(),
        clients: hub.count,
      });
    }
    dispatcher.pushHealth(() => ({ status: 'ok', uptimeSec: Math.round(process.uptime()), engine: engine.phase, upstream: feed.connected }));
  }, env.stream.STATUS_INTERVAL_MS);
  statusTimer.unref?.();

  // Background database probe, so /health stays a fast, non-blocking read.
  const dbProbeTimer = setInterval(() => {
    persistence.health().catch((e) => log.error('database probe failed:', e.message));
  }, 30_000);
  dbProbeTimer.unref?.();
  persistence.health().catch(() => {});

  // The price flusher needs the aggregator to enrich frames with the live
  // candle; the aggregator needs the dispatcher for its tick hook. Attach
  // explicitly here, now that both exist.
  dispatcher.attachMarket(market);
  dispatcher.start();
  engine.start();
  aiSignal.start();
  news.start();

  /* ===================================================================== */
  /*  9. BANNER + RETURN                                                   */
  /* ===================================================================== */
  log.raw('');
  log.raw('========================================================');
  log.raw('  AETHERON CORE  ·  Quant Trading & Signal Backend');
  log.raw('  Paper Trading Streaming Engine — Eng Hasan Mohamad');
  log.raw('========================================================');
  log.raw(`  Database    : ${dbVerified.ok ? `Supabase OK (${persistence.schema})` : dbReady ? `CONFIGURED BUT BROKEN — ${dbVerified.error}` : 'LOCAL MODE - no Supabase'}`);
  if (dbReady && !dbVerified.ok) {
    log.warn(`  Database    : candles are NOT being persisted. ${dbVerified.remedy}`);
    log.warn(`               Engine keeps running on RAM; /health will show the error.`);
  }
  log.raw(`  Hydration   : ${hydration.series} series / ${hydration.candles} candles rehydrated in ${hydration.ms}ms`);
  if (restored > 0) log.warn(`  Recovery    : ${restored} open position(s) restored from the audit log`);
  if (aiRestored.restored.length) {
    log.raw(`  AI recovery : ${aiRestored.restored.join(', ')} open trade(s) restored from ai_trades`);
  }
  log.raw(`  Symbols     : ${SYMBOLS.join(', ')}`);
  log.raw(`  Timeframes  : ${TIMEFRAMES.map((t) => t.label).join(', ')}  →  ${SERIES.length} series`);
  log.raw(`  Broker      : ${env.broker.MODE}${env.strategy.EXECUTION_ENABLED ? '' : ' (execution DISABLED)'}`);
  log.raw(`  Strategy    : ${env.strategy.DONCHIAN_ENTRY}d entry · ${env.strategy.DONCHIAN_EXIT}d trail · ATR(${env.strategy.ATR_PERIOD}) sizing`);
  log.raw(`  Quota       : FREE ${env.quota.FREE_DAILY_EXECUTIONS}/day · PRO unlimited`);
  log.raw(
    `  AI analyst  : ${
      aiSignal.enabled
        ? `${env.ai.MODEL} on ${env.ai.PROVIDER_LABEL} · ` +
          `${env.ai.API_KEYS.length} key${env.ai.API_KEYS.length === 1 ? '' : 's'} · ` +
          `every ${Math.round(env.ai.INTERVAL_MS / 60_000)}m on ${aiSignal.symbols.join(', ')}`
        : env.ai.API_KEY
          ? 'disabled (AI_SIGNAL_ENABLED=false)'
          : `idle — ${env.ai.PROVIDER === 'groq' ? 'GROQ_API_KEY' : 'OPENROUTER_API_KEY'} not set`
    }`
  );
  log.raw(
    `  AI models   : ${[env.ai.MODEL, ...(env.ai.FALLBACK_MODELS || []), env.ai.FALLBACK_MODEL]
      .filter((m, i, all) => m && all.indexOf(m) === i)
      .join(' → ')}`
  );
  log.raw('  AI payload  : PLAINTEXT on /stream (same connection as the candles)');
  log.raw(`  AI tracking : ${env.ai.TRACK_ENABLED ? 'one trade at a time · Telegram status every cycle' : 'signal-only (no tracking)'}`);
  log.raw(`  Calendar    : ${news.enabled ? `${env.news.MIN_IMPACT}+ events within ${env.news.HORIZON_H}h` : 'disabled'}`);
  log.raw(
    `  Admin       : ${adminAuth.enabled ? `/admin/ · operator "${adminAuth.username}"` : '/admin/ DISABLED — set ADMIN_PASSWORD'}`
  );
  log.raw(
    `  Memberships : ${env.membership.ENABLED ? 'FREE / MONTHLY / YEARLY · history ' + membership.retentionFor('FREE') + '/' + membership.retentionFor('MONTHLY') + '/' + membership.retentionFor('YEARLY') + ' days' : 'disabled'}`
  );
  log.raw(`  Trade API   : /api/trades/open · /api/trades/history · /api/trades/stats`);
  log.raw(`  Streams     : /stream · /api/stream/prices · /api/stream/chat-events`);
  log.raw(`  Upstream    : ${env.upstream.WS}`);
  log.raw('========================================================');
  log.raw('');

  /* ===================================================================== */
  /*  10. SHUTDOWN                                                         */
  /* ===================================================================== */
  let closed = false;
  async function close() {
    if (closed) return;
    closed = true;

    clearInterval(marketFlushTimer);
    clearInterval(statusTimer);
    clearInterval(dbProbeTimer);
    dispatcher.stop();
    rateLimiter.stop();
    engine.stop();
    aiSignal.stop();
    news.stop();
    feed.stop();
    hub.closeAll();
    membershipEvents.closeAll();
    telegram?.stop?.();

    // Flush any queued candle writes so a restart does not lose a closed bar.
    try {
      const r = await persistence.flush();
      if (r.rows) log.ok(`flushed ${r.rows} pending candle row(s) to Supabase`);
    } catch (e) {
      log.warn('final candle flush failed:', e.message);
    }
    persistence.stop();
  }

  return {
    app,
    env,
    market,
    hub,
    dispatcher,
    persistence,
    broker,
    auth,
    quota,
    engine,
    feed,
    rateLimiter,
    telegram,
    aiSignal,
    news,
    membership,
    membershipEvents,
    adminAuth,
    health: healthPayload,
    close,
    // Re-exported for the e2e suite / unit tests.
    ENGINE_STATE,
  };
}

export default createApplication;