/* =============================================================================
 *  AETHERON CORE  —  src/routes/membership.js
 *  -----------------------------------------------------------------------------
 *  The APK-facing, device-identified API:
 *
 *    POST /api/membership/register   install → FREE account (idempotent)
 *    GET  /api/membership/me         plan, status, expiry, retention
 *    POST /api/membership/request    ask the admin for MONTHLY / YEARLY
 *    GET  /api/membership/requests   the device's own request history
 *    GET  /api/membership/stream     optional per-device SSE upgrade notice
 *    GET  /api/membership/plans      public plan + support-handle metadata
 *
 *    GET  /api/trades/open           open AI / strategy trades
 *    GET  /api/trades/history        closed AI trades, filtered by plan
 *    GET  /api/trades/stats          win rate / PnL for the plan window
 *
 *  IMPORTANT
 *  ---------
 *  The APK never receives Supabase credentials and never talks to the database
 *  directly. Every read goes through this process, which uses the service-role
 *  key server-side. A device id is an identity, not a secret; it authorizes
 *  read-only access to the shared signal history, not to another user's data.
 * ============================================================================= */

import express from 'express';
import { ENV } from '../config/env.js';
import { normaliseDeviceId, APP_PLANS } from '../services/appUsers.js';
import { createLogger } from '../services/logger.js';

const log = createLogger('routes:membership');
const HEARTBEAT_MS = 20_000;

/** Pull a device id from the header, query string or JSON body. */
export function deviceIdFromRequest(req) {
  const header = req.get?.('x-device-id') ?? req.headers?.['x-device-id'];
  const raw = header ?? req.query?.deviceId ?? req.query?.device_id ?? req.body?.deviceId;
  return normaliseDeviceId(raw);
}

function fail(res, status, code, message, extra = {}) {
  return res.status(status).json({ error: { code, message, ...extra } });
}

/** Wrap an async handler so a rejected promise becomes a JSON 500, not a hang. */
function wrap(fn) {
  return (req, res, next) => Promise.resolve(fn(req, res, next)).catch(next);
}

/**
 * Optional gate for the existing public surfaces. A request that carries a
 * device id is checked against the account; an anonymous browser/PWA request
 * is untouched. This is how a banned APK is stopped without breaking the site.
 *
 * @param {object} deps
 * @param {import('../services/appUsers.js').AppUserService} deps.membership
 * @param {boolean} [deps.required]
 * @param {boolean} [deps.sse]
 */
export function deviceGate({ membership, required = false, sse = false } = {}) {
  return async (req, res, next) => {
    try {
      const deviceId = deviceIdFromRequest(req);
      if (!deviceId) {
        if (!required) return next();
        return fail(res, 400, 'device_required', 'Send X-Device-Id (or ?deviceId=) with this request');
      }

      const user = await membership.getByDevice(deviceId, { create: false, touch: false });
      if (!user) {
        if (!required) return next();
        return fail(res, 401, 'unregistered', 'Register this device first: POST /api/membership/register');
      }
      if (user.status === 'BANNED') {
        if (sse) {
          try {
            if (!res.headersSent) {
              res.writeHead(403, {
                'content-type': 'text/event-stream; charset=utf-8',
                'cache-control': 'no-cache, no-transform',
                connection: 'keep-alive',
              });
              res.flushHeaders?.();
            }
            res.write(
              `event: auth\ndata: ${JSON.stringify({
                type: 'AUTH_ERROR',
                code: 'banned',
                message: 'This device is suspended. Contact support.',
              })}\n\n`
            );
            res.end();
          } catch {
            /* client gone */
          }
          return undefined;
        }
        return fail(res, 403, 'banned', 'This device is suspended. Contact support.');
      }

      req.appUser = user;
      return next();
    } catch (err) {
      // Express 4 does not await async middleware, so an error here would
      // otherwise become an unhandled rejection and leave the request hanging.
      return next(err);
    }
  };
}

/**
 * @param {object} deps
 * @param {import('express').Application} deps.app
 * @param {import('../services/appUsers.js').AppUserService} deps.membership
 * @param {import('../services/membershipEvents.js').MembershipEvents} deps.events
 * @param {object} [deps.engine]       strategy engine (open positions)
 * @param {Function} [deps.tradeGuard]
 * @param {Function} [deps.rateLimiter]
 */
export function mountMembershipRoutes({
  app,
  membership,
  events,
  engine = null,
  tradeGuard = null,
  rateLimiter = null,
  env = ENV,
}) {
  const router = express.Router();
  const guards = [];
  if (tradeGuard) guards.push(tradeGuard);
  if (rateLimiter) guards.push(rateLimiter);
  const membershipEnabled = membership.cfg?.ENABLED !== false;

  /** Membership endpoints are off when the operator disables the feature. */
  const requireMembership = (_req, res, next) => {
    if (!membershipEnabled) {
      return fail(res, 503, 'membership_disabled', 'Memberships are disabled');
    }
    return next();
  };

  const plansPayload = () => ({
    enabled: membershipEnabled,
    plans: [
      {
        id: APP_PLANS.FREE,
        label: 'Free',
        priceHint: null,
        historyDays: membership.retentionFor(APP_PLANS.FREE),
        defaultDays: 0,
      },
      {
        id: APP_PLANS.MONTHLY,
        label: 'Monthly',
        priceHint: null,
        historyDays: membership.retentionFor(APP_PLANS.MONTHLY),
        defaultDays: 30,
      },
      {
        id: APP_PLANS.YEARLY,
        label: 'Yearly',
        priceHint: null,
        historyDays: membership.retentionFor(APP_PLANS.YEARLY),
        defaultDays: 365,
      },
    ],
    support: {
      telegram: env.membership.SUPPORT_TELEGRAM || null,
      whatsapp: env.membership.SUPPORT_WHATSAPP || null,
      email: env.membership.SUPPORT_EMAIL || null,
    },
    note: 'Upgrades are approved manually after you contact support. No card data is stored in the app.',
  });

  /* --------------------------------------------------------------- register */
  router.post(
    '/api/membership/register',
    ...guards,
    wrap(async (req, res) => {
      if (!membershipEnabled) return fail(res, 503, 'membership_disabled', 'Memberships are disabled');
      const body = req.body ?? {};
      const deviceId = deviceIdFromRequest(req) ?? normaliseDeviceId(body.deviceId);
      if (!deviceId) return fail(res, 400, 'device_required', 'Send X-Device-Id (or deviceId in the body)');
      const { user, created } = await membership.register({ ...body, deviceId });
      return res.json({
        ok: true,
        created,
        user: membership.publicUser(user),
        ...plansPayload(),
        serverTime: new Date().toISOString(),
      });
    })
  );

  /* -------------------------------------------------------------------- me */
  router.get(
    '/api/membership/me',
    ...guards,
    requireMembership,
    wrap(async (req, res) => {
      const deviceId = deviceIdFromRequest(req);
      if (!deviceId) return fail(res, 400, 'device_required', 'Send X-Device-Id (or ?deviceId=)');
      const user = await membership.getByDevice(deviceId, { create: true, touch: true });
      if (!user) return fail(res, 503, 'storage_unavailable', 'Could not load this device account');
      return res.json({
        ok: true,
        user: membership.publicUser(user),
        ...plansPayload(),
        serverTime: new Date().toISOString(),
      });
    })
  );

  /* -------------------------------------------------------------- request */
  router.post(
    '/api/membership/request',
    ...guards,
    wrap(async (req, res) => {
      if (!membershipEnabled) return fail(res, 503, 'membership_disabled', 'Memberships are disabled');
      const body = req.body ?? {};
      const deviceId = deviceIdFromRequest(req) ?? normaliseDeviceId(body.deviceId);
      if (!deviceId) return fail(res, 400, 'device_required', 'deviceId is required');
      const { request, duplicate, user } = await membership.requestUpgrade({ ...body, deviceId });
      return res.status(duplicate ? 200 : 201).json({
        ok: true,
        duplicate,
        request,
        user: membership.publicUser(user),
        message: duplicate
          ? 'A request for this device is already waiting for the admin.'
          : 'Request saved. Contact support if you have not already done so.',
        support: plansPayload().support,
      });
    })
  );

  router.get(
    '/api/membership/requests',
    ...guards,
    requireMembership,
    wrap(async (req, res) => {
      const deviceId = deviceIdFromRequest(req);
      if (!deviceId) return fail(res, 400, 'device_required', 'Send X-Device-Id (or ?deviceId=)');
      const items = await membership.listRequestsForDevice(deviceId, 20);
      return res.json({ ok: true, count: items.length, items });
    })
  );

  router.get('/api/membership/plans', ...guards, (_req, res) => {
    res.json({ ok: true, ...plansPayload() });
  });

  /* --------------------------------------------------------------- SSE push */
  // Optional: the app can keep this open to learn about an upgrade instantly.
  // GET /api/membership/me remains the source of truth if the stream is off.
  router.get(
    '/api/membership/stream',
    ...guards,
    requireMembership,
    wrap(async (req, res) => {
      const deviceId = deviceIdFromRequest(req);
      if (!deviceId) return fail(res, 400, 'device_required', 'Send X-Device-Id (or ?deviceId=)');
      const user = await membership.getByDevice(deviceId, { create: false, touch: true });
      if (!user) return fail(res, 401, 'unregistered', 'Register this device first');
      if (user.status === 'BANNED') return fail(res, 403, 'banned', 'This device is suspended.');

      res.writeHead(200, {
        'content-type': 'text/event-stream; charset=utf-8',
        'cache-control': 'no-cache, no-transform',
        connection: 'keep-alive',
        'x-accel-buffering': 'no',
      });
      res.write(`retry: ${env.stream.RETRY_MS}\n\n`);
      res.flushHeaders?.();

      res.write(
        `event: hello\ndata: ${JSON.stringify({
          type: 'membership',
          user: membership.publicUser(user),
          serverTime: new Date().toISOString(),
        })}\n\n`
      );

      const subscription = events.subscribe(deviceId, res);
      const heartbeat = setInterval(() => {
        try {
          res.write(': keepalive\n\n');
        } catch {
          clearInterval(heartbeat);
        }
      }, HEARTBEAT_MS);
      heartbeat.unref?.();

      const close = () => {
        clearInterval(heartbeat);
        subscription.close();
      };
      req.on('close', close);
      req.on('aborted', close);
      res.on('close', close);
    })
  );

  /* ---------------------------------------------------------- open trades */
  router.get(
    '/api/trades/open',
    ...guards,
    requireMembership,
    deviceGate({ membership, required: true }),
    wrap(async (req, res) => {
      const positions = engine?.openPositions?.() ?? [];
      const payload = await membership.openTrades(req.appUser.device_id, { strategyPositions: positions });
      res.json({ ok: true, ...payload });
    })
  );

  /* ------------------------------------------------------- closed history */
  router.get(
    '/api/trades/history',
    ...guards,
    requireMembership,
    deviceGate({ membership, required: true }),
    wrap(async (req, res) => {
      const limit = Math.min(200, Math.max(1, Number(req.query.limit) || 100));
      const offset = Math.max(0, Number(req.query.offset) || 0);
      const symbol = req.query.symbol ? String(req.query.symbol).toUpperCase() : null;
      const payload = await membership.history(req.appUser.device_id, { symbol, limit, offset });
      res.json({ ok: true, ...payload, serverTime: new Date().toISOString() });
    })
  );

  /* ---------------------------------------------------------- trade stats */
  router.get(
    '/api/trades/stats',
    ...guards,
    requireMembership,
    deviceGate({ membership, required: true }),
    wrap(async (req, res) => {
      const payload = await membership.history(req.appUser.device_id, { limit: 200, offset: 0 });
      const wins = payload.items.filter((t) => Number(t.pnlPoints) >= 0).length;
      const pnlPoints = payload.items.reduce((sum, t) => sum + (Number(t.pnlPoints) || 0), 0);
      res.json({
        ok: true,
        plan: payload.plan,
        retentionDays: payload.retentionDays,
        since: payload.since,
        total: payload.items.length,
        wins,
        losses: payload.items.length - wins,
        winRate: payload.items.length ? Math.round((wins / payload.items.length) * 1_000) / 10 : 0,
        pnlPoints: Math.round(pnlPoints * 100) / 100,
        bySymbol: payload.items.reduce((acc, t) => {
          const key = t.symbol;
          const row = (acc[key] ??= { total: 0, wins: 0, losses: 0, pnlPoints: 0 });
          row.total += 1;
          if (Number(t.pnlPoints) >= 0) row.wins += 1;
          else row.losses += 1;
          row.pnlPoints += Number(t.pnlPoints) || 0;
          return acc;
        }, {}),
        serverTime: new Date().toISOString(),
      });
    })
  );

  app.use(router);
  log.raw('mounted APK membership + trade history API');
  return router;
}

export default mountMembershipRoutes;
