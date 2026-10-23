/* =============================================================================
 *  AETHERON CORE  —  src/routes/ai.js
 *  -----------------------------------------------------------------------------
 *  The read-only HTTP surface of the AI analyst:
 *
 *    GET /api/ai/signal    status + the most recent published signal
 *    GET /api/ai/analyze   trigger a fresh analysis now (rate-limited)
 *
 *  WHY GET, NOT POST
 *  -----------------
 *  This codebase has exactly one rule about its HTTP surface: no route accepts
 *  an order, and every user route is a GET. Triggering an analysis is a read of
 *  the market that happens to cost a model call — it is not a mutation of the
 *  trading state, so it stays a GET, and the tradeGuard middleware still strips
 *  any execution-shaped key a client might smuggle into the query string.
 *
 *  The manual trigger is cooldown-limited inside the service (see
 *  AiSignalService.run), so this endpoint cannot be used to burn API quota.
 * ============================================================================= */

import express from 'express';

/**
 * @param {object} deps
 * @param {import('express').Application} deps.app
 * @param {import('../services/aiSignal.js').AiSignalService} deps.ai
 * @param {Function} [deps.tradeGuard]  shared request guard
 * @param {Function} [deps.deviceGate]  optional APK membership gate
 */
export function mountAiRoutes({ app, ai, tradeGuard = null, deviceGate = null }) {
  const router = express.Router();
  const guard = tradeGuard ? [tradeGuard] : [];
  const member = deviceGate ? [deviceGate] : [];

  /** Current status + last signal. Cheap, safe to poll. */
  router.get('/api/ai/signal', ...guard, ...member, (_req, res) => {
    res.json(ai.status());
  });

  /** On-demand analysis. 429 while the cooldown is live, 503 when unconfigured. */
  router.get('/api/ai/analyze', ...guard, ...member, async (_req, res, next) => {
    try {
      const result = await ai.run('manual');
      if (result.ok) return res.json(result);

      const code =
        result.error === 'cooldown' || result.error === 'busy' || result.error === 'rate-limited'
          ? 429
          : result.error === 'disabled' || result.error === 'insufficient-data'
            ? 503
            : 502;
      return res.status(code).json(result);
    } catch (err) {
      return next(err);
    }
  });

  app.use(router);
  return router;
}

export default mountAiRoutes;
