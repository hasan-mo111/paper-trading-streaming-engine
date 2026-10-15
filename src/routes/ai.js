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
 */
export function mountAiRoutes({ app, ai, tradeGuard = null }) {
  const router = express.Router();
  const guard = tradeGuard ? [tradeGuard] : [];

  /** Current status + last signal. Cheap, safe to poll. */
  router.get('/api/ai/signal', ...guard, (_req, res) => {
    res.json(ai.status());
  });

  /** On-demand analysis. 429 while the cooldown is live, 503 when unconfigured. */
  router.get('/api/ai/analyze', ...guard, async (_req, res) => {
    const result = await ai.run('manual');
    if (result.ok) return res.json(result);

    const code =
      result.error === 'cooldown' || result.error === 'busy'
        ? 429
        : result.error === 'disabled' || result.error === 'insufficient-data'
          ? 503
          : 502;
    return res.status(code).json(result);
  });

  app.use(router);
  return router;
}

export default mountAiRoutes;
