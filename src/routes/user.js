/* =============================================================================
 *  AETHERON CORE  —  src/routes/user.js
 *  -----------------------------------------------------------------------------
 *  The read-only user API. EVERY route here is a GET.
 *
 *  There is no POST /trade, no PUT /position, no "force close" endpoint, and
 *  the middleware in middleware/tradeGuard.js rejects any request that even
 *  carries execution-shaped parameters. A client can read what the engine
 *  decided; it can never tell the engine what to do. If you find yourself
 *  adding a mutating route here, the design has been broken.
 *
 *  ROUTES
 *  ------
 *    GET /api/user/me            identity, plan and sizing parameters
 *    GET /api/user/quota         today's execution budget
 *    GET /api/user/positions     what the engine currently holds
 *    GET /api/user/executions    audit trail (newest first)
 *    GET /api/user/engine        live strategy state + indicators
 *    GET /api                    read-only manifest
 * ============================================================================= */

import express from 'express';
import { ENV, publicEnv } from '../config/env.js';
import { apiManifest } from '../middleware/tradeGuard.js';
import { CHAT_TYPES } from '../services/sseDispatcher.js';
import { createLogger } from '../services/logger.js';

const log = createLogger('routes:user');

/**
 * @param {object} deps
 * @param {import('express').Application} deps.app
 * @param {import('../middleware/auth.js').createAuth extends Function} deps.auth
 * @param {import('../middleware/quotaManager.js').QuotaManager} deps.quota
 * @param {import('../engine/strategyEngine.js').StrategyEngine} deps.engine
 * @param {import('../services/brokerApi.js').PaperBroker} deps.broker
 * @param {import('../services/supabase.js').SupabaseService} deps.persistence
 * @param {Function} deps.tradeGuard
 * @param {Function} [deps.rateLimiter] express middleware
 */
export function mountUserRoutes({ app, auth, quota, engine, broker, persistence, tradeGuard, rateLimiter = null }) {
  const router = express.Router();

  // Every user route is authenticated (unless AUTH_REQUIRED is turned off for
  // local development) and rate limited per token/IP.
  const guard = [tradeGuard];
  if (rateLimiter) guard.unshift(rateLimiter);
  guard.push(ENV.auth.REQUIRED ? auth.require : auth.optional);

  /* ------------------------------------------------------------------ /api */
  router.get('/api', tradeGuard, (_req, res) => {
    res.json({ ...apiManifest(), config: publicEnv() });
  });

  /* --------------------------------------------------------------- /me --- */
  router.get('/api/user/me', ...guard, async (req, res) => {
    if (!req.user) {
      return res.status(401).json({ error: { code: 'unauthenticated', message: 'Token required' } });
    }
    const profile = req.profile ?? (await auth.service.getProfile(req.user.id));
    const ctx = await quota.contextFor(req.user.id, req.user.plan);
    const q = await quota.describe(req.user.id, ctx.plan);

    res.json({
      user: { id: req.user.id, email: req.user.email, dev: Boolean(req.user.dev) },
      plan: ctx.plan,
      tradingEnabled: ctx.tradingEnabled,
      // Sizing parameters — the client displays them; it cannot change them.
      sizing: {
        accountBalance: ctx.accountBalance,
        riskPercentage: ctx.riskPercentage,
        riskAmount: Number((ctx.accountBalance * ctx.riskPercentage).toFixed(2)),
      },
      symbols: ctx.symbols ?? 'all',
      quota: q,
    });
  });

  /* -------------------------------------------------------------- /quota -- */
  router.get('/api/user/quota', ...guard, async (req, res) => {
    if (!req.user) {
      return res.status(401).json({ error: { code: 'unauthenticated', message: 'Token required' } });
    }
    const ctx = await quota.contextFor(req.user.id, req.user.plan);
    const usage = await quota.describe(req.user.id, ctx.plan);

    res.json({
      ...usage,
      // Spelled out for the client so it can render "2 of 3 used" directly.
      message: usage.unlimited
        ? `PRO — unlimited automatic executions (${usage.used} today)`
        : `FREE — ${usage.used}/${usage.limit} automatic executions used today`,
      resetsAt: `${usage.day}T24:00:00Z`,
    });
  });

  /* ---------------------------------------------------------- /positions -- */
  router.get('/api/user/positions', ...guard, (req, res) => {
    if (!req.user) {
      return res.status(401).json({ error: { code: 'unauthenticated', message: 'Token required' } });
    }
    // Read the engine's own book. `broker.listPositions()` is not used here on
    // purpose: the engine's copy carries the leg split and the trailing stop,
    // which is what the app needs to render.
    res.json({
      count: engine.openPositions().length,
      positions: engine.openPositions(),
    });
  });

  /* --------------------------------------------------------- /executions -- */
  router.get('/api/user/executions', ...guard, async (req, res) => {
    if (!req.user) {
      return res.status(401).json({ error: { code: 'unauthenticated', message: 'Token required' } });
    }
    const limit = Math.min(100, Math.max(1, Number(req.query.limit) || 25));
    const rows = await persistence.listExecutions(req.user.id, limit);
    res.json({ count: rows.length, limit, items: rows });
  });

  /* ------------------------------------------------------------- /engine -- */
  router.get('/api/user/engine', ...guard, (_req, res) => {
    const status = engine.status();
    res.json({
      ...status,
      quotaPolicy: {
        freeDailyExecutions: ENV.quota.FREE_DAILY_EXECUTIONS,
        proUnlimited: ENV.quota.PRO_DAILY_EXECUTIONS === 0,
        note: 'Exits are never metered — an open position can always be closed.',
      },
      broker: broker.status(),
    });
  });

  /* ------------------------------------------------------ /chat taxonomy -- */
  router.get('/api/user/chat-types', tradeGuard, (_req, res) => {
    res.json({
      types: Object.values(CHAT_TYPES),
      note: 'Message envelope: { id, type, severity, message, timestamp, meta }',
    });
  });

  app.use(router);
  log.raw('mounted read-only user API');
  return router;
}

export default mountUserRoutes;