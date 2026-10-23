/* =============================================================================
 *  AETHERON CORE  —  src/routes/admin.js
 *  -----------------------------------------------------------------------------
 *  The operator console: login, APK user management, upgrade requests and the
 *  permanent trade ledger.
 *
 *    GET    /admin/                     the single-page console
 *    GET    /admin/api/me               auth/config probe (no auth)
 *    POST   /admin/api/login            username + password → signed cookie
 *    POST   /admin/api/logout           clear the cookie
 *    GET    /admin/api/overview         counters + recent activity
 *    GET    /admin/api/users            search / filter / paginate
 *    GET    /admin/api/users/:id        one account
 *    PATCH  /admin/api/users/:id        plan, expiry, ban, contact, note
 *    POST   /admin/api/users/:id/notify resend the Telegram/SSE notice
 *    GET    /admin/api/requests         upgrade requests
 *    POST   /admin/api/requests/:id/resolve   approve / reject
 *    GET    /admin/api/trades           AI trade ledger
 *
 *  Every mutating endpoint is authenticated, same-origin checked and audit
 *  logged. The APK cannot reach any of them.
 * ============================================================================= */

import express from 'express';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { ENV } from '../config/env.js';
import { APP_PLANS } from '../services/appUsers.js';
import { createLogger } from '../services/logger.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ADMIN_PAGE = path.join(__dirname, '..', '..', 'public', 'admin', 'index.html');
const log = createLogger('routes:admin');

function fail(res, status, code, message, extra = {}) {
  return res.status(status).json({ error: { code, message, ...extra } });
}

function wrap(fn) {
  return (req, res, next) => Promise.resolve(fn(req, res, next)).catch(next);
}

function intParam(raw, fallback, min, max) {
  const n = Math.round(Number(raw));
  if (!Number.isFinite(n)) return fallback;
  return Math.min(max, Math.max(min, n));
}

/**
 * @param {object} deps
 * @param {import('express').Application} deps.app
 * @param {import('../middleware/adminAuth.js').AdminAuth} deps.adminAuth
 * @param {import('../services/appUsers.js').AppUserService} deps.membership
 * @param {import('../services/supabase.js').SupabaseService} deps.persistence
 * @param {object} [deps.env]
 * @param {object} [deps.ai]        AI manager (overview only)
 * @param {Function} [deps.notifyMembership]  (user, kind, ctx) → notification result
 * @param {Function} [deps.healthProvider]
 */
export function mountAdminRoutes({
  app,
  adminAuth,
  membership,
  persistence,
  env = ENV,
  ai = null,
  notifyMembership = null,
  healthProvider = null,
}) {
  const router = express.Router();

  const noStore = (_req, res, next) => {
    res.set('Cache-Control', 'no-store');
    res.set('Pragma', 'no-cache');
    next();
  };
  router.use('/admin', noStore);

  /* --------------------------------------------------------------- pages -- */
  // Express' default non-strict routing makes '/admin/' also match '/admin',
  // so a separate '/admin' → '/admin/' redirect would loop forever.
  router.get('/admin/', (_req, res) => res.sendFile(ADMIN_PAGE));
  router.get('/admin/index.html', (_req, res) => res.sendFile(ADMIN_PAGE));

  /* ---------------------------------------------------------------- probe -- */
  router.get('/admin/api/me', (req, res) => {
    const session = adminAuth.sessionFromRequest(req);
    res.json({
      ok: true,
      enabled: adminAuth.enabled,
      authenticated: Boolean(session),
      username: session ? adminAuth.username : null,
      database: {
        configured: persistence.configured,
        enabled: persistence.enabled,
        connected: Boolean(persistence.lastHealth?.connected),
      },
      plans: Object.values(APP_PLANS),
      retentionDays: membership.status().retentionDays,
      serverTime: new Date().toISOString(),
      message: adminAuth.enabled
        ? null
        : 'Set ADMIN_PASSWORD (or ADMIN_PASSWORD_HASH) in Render → Environment to enable the console.',
    });
  });

  /* ---------------------------------------------------------------- login -- */
  router.post(
    '/admin/api/login',
    wrap(async (req, res) => {
      if (!adminAuth.enabled) {
        return fail(res, 503, 'admin_disabled', 'Admin console is not configured on this server.');
      }
      if (!adminAuth.sameOrigin(req)) return fail(res, 403, 'bad_origin', 'Cross-site login is not allowed');

      const ip = req.ip ?? req.socket?.remoteAddress ?? 'unknown';
      if (!adminAuth.loginAllowed(ip)) {
        return fail(res, 429, 'login_locked', 'Too many failed attempts. Wait a few minutes and try again.');
      }

      const { username, password } = req.body ?? {};
      if (!adminAuth.verifyCredentials(username, password)) {
        adminAuth.recordFailure(ip);
        adminAuth.counters.failures += 1;
        log.warn(`failed admin login from ${ip}`);
        return fail(res, 401, 'bad_credentials', 'Wrong username or password');
      }

      adminAuth.clearFailures(ip);
      adminAuth.counters.logins += 1;
      const token = adminAuth.issueSession();
      res.setHeader('Set-Cookie', adminAuth.cookieHeader(token));
      log.ok(`admin login from ${ip}`);
      return res.json({ ok: true, username: adminAuth.username, expiresInMs: adminAuth.ttlMs });
    })
  );

  router.post('/admin/api/logout', (req, res) => {
    if (!adminAuth.sameOrigin(req)) return fail(res, 403, 'bad_origin', 'Cross-site logout is not allowed');
    res.setHeader('Set-Cookie', adminAuth.clearCookieHeader());
    res.json({ ok: true });
  });

  /* ------------------------------------------------ everything below = auth */
  const auth = adminAuth.middleware({ api: true });
  const csrf = (req, res, next) => {
    if (!['GET', 'HEAD', 'OPTIONS'].includes(req.method) && !adminAuth.sameOrigin(req)) {
      return fail(res, 403, 'bad_origin', 'Cross-site admin writes are not allowed');
    }
    return next();
  };

  /* ------------------------------------------------------------- overview -- */
  router.get(
    '/admin/api/overview',
    auth,
    wrap(async (_req, res) => {
      const stats = await membership.stats();
      const health = healthProvider ? healthProvider() : null;
      res.json({
        ok: true,
        stats,
        database: {
          configured: persistence.configured,
          enabled: persistence.enabled,
          connected: Boolean(persistence.lastHealth?.connected),
          error: persistence.lastHealth?.error ?? null,
        },
        membership: membership.status(),
        admin: { username: adminAuth.username, sessionTtlMs: adminAuth.ttlMs },
        ai: ai
          ? {
              enabled: ai.enabled,
              configured: ai.configured,
              model: ai.status?.().model ?? null,
              running: ai.status?.().running ?? false,
              openTrades: stats.trades.open,
              lastError: ai.status?.().lastError ?? null,
            }
          : null,
        health: health
          ? {
              status: health.status,
              uptimeSec: health.uptimeSec,
              engineState: health.engine?.state,
              upstream: health.upstream?.connected,
              aiErrors: health.ai?.errors ?? null,
            }
          : null,
        serverTime: new Date().toISOString(),
      });
    })
  );

  /* ---------------------------------------------------------------- users -- */
  router.get(
    '/admin/api/users',
    auth,
    wrap(async (req, res) => {
      const q = String(req.query.q ?? '').slice(0, 80);
      const status = req.query.status ? String(req.query.status).toUpperCase() : null;
      const plan = req.query.plan ? String(req.query.plan).toUpperCase() : null;
      const expired = req.query.expired === 'true' ? true : req.query.expired === 'false' ? false : null;
      const limit = intParam(req.query.limit, 50, 1, 200);
      const offset = intParam(req.query.offset, 0, 0, 100_000);
      const payload = await membership.listUsers({ q, status, plan, expired, limit, offset });
      res.json({ ok: true, ...payload, serverTime: new Date().toISOString() });
    })
  );

  router.get(
    '/admin/api/users/:id',
    auth,
    wrap(async (req, res) => {
      const user = await membership.getUser(req.params.id);
      if (!user) return fail(res, 404, 'not_found', 'User not found');
      const requests = (await membership.listRequestsForDevice(user.deviceId, 20)).slice(0, 10);
      res.json({ ok: true, user, requests });
    })
  );

  router.patch(
    '/admin/api/users/:id',
    auth,
    csrf,
    wrap(async (req, res) => {
      const before = await membership.getUser(req.params.id);
      if (!before) return fail(res, 404, 'not_found', 'User not found');

      const body = req.body ?? {};
      const updated = await membership.updateUser(req.params.id, body);
      const view = membership.publicUser(updated);

      const planChanged = view.storedPlan !== before.storedPlan || view.planExpiresAt !== before.planExpiresAt;
      const statusChanged = view.status !== before.status;
      let notification = null;
      if ((planChanged || statusChanged) && notifyMembership) {
        notification = await notifyMembership(updated, statusChanged ? 'status' : 'plan', {
          by: req.admin?.sub ?? 'admin',
          before,
        });
      }

      log.info(
        `admin ${req.admin?.sub ?? 'admin'} updated ${view.deviceId}: ` +
          `${before.storedPlan}/${before.status} → ${view.storedPlan}/${view.status}`
      );
      res.json({ ok: true, user: view, notification });
    })
  );

  router.post(
    '/admin/api/users/:id/notify',
    auth,
    csrf,
    wrap(async (req, res) => {
      const row = await persistence.getAppUser(req.params.id);
      if (!row) return fail(res, 404, 'not_found', 'User not found');
      if (!notifyMembership) return fail(res, 503, 'notifier_unavailable', 'No notifier is configured');
      const notification = await notifyMembership(row, 'manual', {
        by: req.admin?.sub ?? 'admin',
        message: String(req.body?.message ?? '').slice(0, 400),
      });
      res.json({ ok: true, notification });
    })
  );

  /* ------------------------------------------------------------- requests -- */
  router.get(
    '/admin/api/requests',
    auth,
    wrap(async (req, res) => {
      const status = req.query.status ? String(req.query.status).toUpperCase() : null;
      const limit = intParam(req.query.limit, 100, 1, 500);
      const offset = intParam(req.query.offset, 0, 0, 100_000);
      const payload = await membership.listRequests({ status, limit, offset });
      res.json({ ok: true, ...payload, serverTime: new Date().toISOString() });
    })
  );

  router.post(
    '/admin/api/requests/:id/resolve',
    auth,
    csrf,
    wrap(async (req, res) => {
      const body = req.body ?? {};
      const result = await membership.resolveRequest(req.params.id, {
        decision: body.decision,
        plan: body.plan,
        days: body.days,
        note: body.note,
        admin: req.admin?.sub ?? 'admin',
      });

      let notification = null;
      if (result.approved && result.user && notifyMembership) {
        notification = await notifyMembership(result.user, 'plan', {
          by: req.admin?.sub ?? 'admin',
          request: result.request,
        });
      }

      log.info(
        `admin ${req.admin?.sub ?? 'admin'} ${result.approved ? 'approved' : 'rejected'} request ` +
          `${req.params.id}`
      );
      res.json({
        ok: true,
        approved: result.approved,
        request: result.request,
        user: result.user ? membership.publicUser(result.user) : null,
        notification,
      });
    })
  );

  /* --------------------------------------------------------------- trades -- */
  router.get(
    '/admin/api/trades',
    auth,
    wrap(async (req, res) => {
      const status = req.query.status ? String(req.query.status).toUpperCase() : null;
      const symbol = req.query.symbol ? String(req.query.symbol).toUpperCase() : null;
      const limit = intParam(req.query.limit, 50, 1, 200);
      const offset = intParam(req.query.offset, 0, 0, 100_000);
      const payload = await membership.listTrades({ status, symbol, limit, offset });
      res.json({ ok: true, ...payload, serverTime: new Date().toISOString() });
    })
  );

  /* --------------------------------------------------------------- errors -- */
  router.use((err, req, res, next) => {
    if (!req.path.startsWith('/admin')) return next(err);
    const status = Number(err?.status) || 500;
    if (status >= 500) log.error('admin request failed:', err?.message ?? err);
    return res.status(status).json({
      error: {
        code: err?.code ?? 'admin_error',
        message: status >= 500 ? 'Admin request failed' : err?.message ?? 'Admin request failed',
      },
    });
  });

  app.use(router);
  log.raw('mounted /admin console + admin API');
  return router;
}

export default mountAdminRoutes;
