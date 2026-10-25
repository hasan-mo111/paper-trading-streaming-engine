/* =============================================================================
 *  AETHERON CORE  —  src/services/appUsers.js
 *  -----------------------------------------------------------------------------
 *  Business rules for the APK's device-based accounts, the upgrade requests the
 *  admin console resolves, and the trade-history views the app reads.
 *
 *  WHY DEVICE IDS, NOT PASSWORDS
 *  -----------------------------
 *  The Android app already generates a stable installation id for Telegram
 *  pairing. Reusing it keeps the signup flow at zero: install → register →
 *  FREE. A paid upgrade is a human decision made in /admin/ after the user
 *  contacts the operator, so there is no password or payment credential in the
 *  app to steal.
 *
 *  PLANS
 *  -----
 *    FREE      history window only (default 7 days)
 *    MONTHLY   30-day plan, 90-day history
 *    YEARLY    365-day plan, 10-year history
 *
 *  A plan is a label + a retention window. It NEVER changes what the strategy
 *  engine or the AI analyst may execute — those remain server-side decisions.
 * ============================================================================= */

import { ENV } from '../config/env.js';
import { createLogger } from './logger.js';

const log = createLogger('membership');

export const APP_PLANS = Object.freeze({ FREE: 'FREE', MONTHLY: 'MONTHLY', YEARLY: 'YEARLY' });
export const APP_STATUS = Object.freeze({ ACTIVE: 'ACTIVE', BANNED: 'BANNED' });
export const PLAN_DAYS = Object.freeze({ MONTHLY: 30, YEARLY: 365 });
export const CONTACT_METHODS = Object.freeze(['telegram', 'whatsapp', 'email', 'other']);

const DEVICE_RE = /^[A-Z0-9._:-]{4,64}$/;
const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]{2,}$/;
const DAY_MS = 86_400_000;

/** Coerce and validate a device id. Returns null for junk. */
export function normaliseDeviceId(raw) {
  if (raw == null) return null;
  const value = String(raw).trim().toUpperCase();
  return DEVICE_RE.test(value) ? value : null;
}

/**
 * Canonical email identity. Lower-casing is deliberate: `Alice@Example.com`
 * and `alice@example.com` are the same person and must map to one account.
 * Returns null for blank/invalid input.
 */
export function normaliseEmail(raw) {
  if (raw == null) return null;
  const value = String(raw).trim().toLowerCase();
  if (!value || value.length > 254) return null;
  return EMAIL_RE.test(value) ? value : null;
}

/** Allowed plan or the supplied fallback. */
export function normalisePlan(raw, fallback = APP_PLANS.FREE) {
  const value = String(raw ?? '').trim().toUpperCase();
  return Object.values(APP_PLANS).includes(value) ? value : fallback;
}

/** Allowed account status or ACTIVE. */
export function normaliseStatus(raw) {
  return String(raw ?? '').trim().toUpperCase() === APP_STATUS.BANNED ? APP_STATUS.BANNED : APP_STATUS.ACTIVE;
}

/** Trim + hard-cap a free-text field. */
function text(raw, max = 500) {
  if (raw == null) return null;
  const value = String(raw).trim();
  return value ? value.slice(0, max) : null;
}

function parseMs(value) {
  if (!value) return 0;
  const ms = Date.parse(value);
  return Number.isFinite(ms) ? ms : 0;
}

/** True when a paid plan's expiry is in the past. */
export function planExpired(user, now = Date.now()) {
  if (!user) return false;
  if (user.plan !== APP_PLANS.MONTHLY && user.plan !== APP_PLANS.YEARLY) return false;
  return parseMs(user.plan_expires_at) <= now;
}

/**
 * The plan actually in force right now. A paid plan with no/failed expiry is
 * treated as FREE, so a broken date can never grant permanent access.
 */
export function effectivePlan(user, now = Date.now()) {
  if (!user) return APP_PLANS.FREE;
  if (user.status === APP_STATUS.BANNED) return APP_STATUS.BANNED;
  const stored = normalisePlan(user.plan, APP_PLANS.FREE);
  if (stored === APP_PLANS.FREE) return APP_PLANS.FREE;
  return parseMs(user.plan_expires_at) > now ? stored : APP_PLANS.FREE;
}

/** Default number of days a plan adds when the admin does not supply one. */
export function defaultPlanDays(plan) {
  const value = normalisePlan(plan, APP_PLANS.FREE);
  return PLAN_DAYS[value] ?? 0;
}

export class AppUserService {
  /**
   * @param {object} opts
   * @param {import('./supabase.js').SupabaseService} opts.persistence
   * @param {object} [opts.env]
   * @param {() => number} [opts.now]  injected clock for tests
   */
  constructor({ persistence, env = ENV, now = () => Date.now() } = {}) {
    this.persistence = persistence;
    this.env = env;
    this.cfg = env.membership ?? {};
    this.now = now;

    /** `${deviceId}|${plan}` → epoch ms of the last accepted request. */
    this.requestCooldowns = new Map();
  }

  /* ------------------------------------------------------------- retention */

  retentionFor(plan) {
    const value = normalisePlan(plan, APP_PLANS.FREE);
    if (value === APP_PLANS.MONTHLY) return Number(this.cfg.MONTHLY_HISTORY_DAYS ?? 90);
    if (value === APP_PLANS.YEARLY) return Number(this.cfg.YEARLY_HISTORY_DAYS ?? 3_650);
    return Number(this.cfg.FREE_HISTORY_DAYS ?? 7);
  }

  planFeatures(plan) {
    const value = normalisePlan(plan, APP_PLANS.FREE);
    return {
      label: value === APP_PLANS.FREE ? 'Free' : value === APP_PLANS.MONTHLY ? 'Monthly' : 'Yearly',
      historyDays: this.retentionFor(value),
      // Everything else is intentionally identical: a plan cannot widen the
      // engine's execution permissions.
      aiSignals: true,
      liveCandles: true,
    };
  }

  /**
   * What the app is allowed to see for this account.
   *
   * `deviceId` is overridden with the installation that made the request when
   * one account is linked to several devices. `primaryDeviceId` remains the
   * canonical row value used by the admin console.
   */
  publicUser(user, { deviceId = null } = {}) {
    if (!user) return null;
    const plan = effectivePlan(user, this.now());
    return {
      id: user.id,
      deviceId: deviceId ?? user.device_id,
      primaryDeviceId: user.device_id,
      email: user.email ?? null,
      displayName: user.display_name ?? null,
      contactMethod: user.contact_method ?? null,
      contactValue: user.contact_value ?? null,
      plan,
      storedPlan: user.plan,
      status: user.status,
      planStartedAt: user.plan_started_at ?? null,
      planExpiresAt: user.plan_expires_at ?? null,
      expired: planExpired(user, this.now()),
      retentionDays: this.retentionFor(plan),
      features: this.planFeatures(plan),
      lastSeenAt: user.last_seen_at ?? null,
      createdAt: user.created_at ?? null,
    };
  }

  /** Minimal identity for an anonymous/public client. */
  anonymous(deviceId = null) {
    return {
      id: null,
      deviceId,
      primaryDeviceId: deviceId,
      email: null,
      plan: APP_PLANS.FREE,
      storedPlan: APP_PLANS.FREE,
      status: APP_STATUS.ACTIVE,
      expired: false,
      retentionDays: this.retentionFor(APP_PLANS.FREE),
      features: this.planFeatures(APP_PLANS.FREE),
    };
  }

  /* ---------------------------------------------------------------- lookups */

  /**
   * Resolve a device id to its row.
   *
   * @param {string} deviceId
   * @param {object} [opts]
   * @param {boolean} [opts.create]  create a FREE account when missing
   * @param {boolean} [opts.touch]   update last_seen_at (throttled to 60s)
   */
  async getByDevice(deviceId, { create = false, touch = false } = {}) {
    const id = normaliseDeviceId(deviceId);
    if (!id) return null;

    let user = await this.persistence.getAppUserByDevice(id);
    if (!user && create) {
      user = await this.persistence.createAppUser({ deviceId: id });
    }
    if (!user) return null;

    if (touch) {
      const last = parseMs(user.last_seen_at);
      if (!last || this.now() - last > 60_000) {
        const updated = await this.persistence.updateAppUser(user.id, {
          last_seen_at: new Date(this.now()).toISOString(),
        });
        if (updated) user = updated;
      }
    }
    return user;
  }

  /** One account by primary key, in the public API shape. */
  async getUser(id) {
    const row = await this.persistence.getAppUser(id);
    if (!row) return null;
    // `note` is operator-only; publicUser() is also used by /api/membership/me,
    // so it must not leak the internal note into the APK response.
    let devices = null;
    try {
      devices = await this.persistence.listAppUserDevices?.(row.id);
    } catch {
      devices = null;
    }
    return {
      ...this.publicUser(row),
      note: row.note ?? null,
      devices: Array.isArray(devices) && devices.length ? devices : [row.device_id],
    };
  }

  /** Every installation linked to one account. */
  async devicesForUser(id) {
    if (!id) return [];
    let row = null;
    let devices = null;
    try {
      row = await this.persistence.getAppUser?.(id);
    } catch {
      row = null;
    }
    try {
      devices = await this.persistence.listAppUserDevices?.(id);
    } catch {
      devices = null;
    }
    const out = Array.isArray(devices) ? devices.filter(Boolean) : [];
    if (row?.device_id && !out.includes(row.device_id)) out.unshift(row.device_id);
    return [...new Set(out)];
  }

  /**
   * Idempotent registration from the APK.
   *
   * A stable device id is still the transport identity, but an email is a
   * stronger account identity: two installations that present the same address
   * resolve to ONE app_users row and share plan/status/history. The second
   * device is recorded in app_user_devices, so both continue to work.
   */
  async register({
    deviceId,
    email = null,
    displayName = null,
    contactMethod = null,
    contactValue = null,
  } = {}) {
    const id = normaliseDeviceId(deviceId);
    if (!id) {
      const error = new Error('deviceId must be 4–64 characters (A–Z, 0–9, . _ : -)');
      error.status = 400;
      error.code = 'bad_device';
      throw error;
    }

    const method = contactMethod && CONTACT_METHODS.includes(String(contactMethod).toLowerCase())
      ? String(contactMethod).toLowerCase()
      : null;
    const contact = text(contactValue, 200);
    const name = text(displayName, 80);

    // Accept an explicit `email`, or derive it from the contact fields. The
    // APK can therefore deduplicate without a new endpoint or field.
    const rawEmail = email ?? (method === 'email' ? contact : null);
    const canonicalEmail = rawEmail == null || String(rawEmail).trim() === '' ? null : normaliseEmail(rawEmail);
    if (rawEmail != null && String(rawEmail).trim() && !canonicalEmail) {
      const error = new Error('email must be a valid address');
      error.status = 400;
      error.code = 'bad_email';
      throw error;
    }

    let user = null;
    let created = false;
    let linked = false;

    if (canonicalEmail) {
      user = await this.persistence.getAppUserByEmail(canonicalEmail);
      if (user) {
        const sameDevice = await this.persistence.getAppUserByDevice(id);
        if (sameDevice && sameDevice.id !== user.id) {
          const error = new Error('this device is already linked to another account');
          error.status = 409;
          error.code = 'email_conflict';
          throw error;
        }
        linked = await this.persistence.linkAppUserDevice(user.id, id);
        if (!linked) {
          const error = new Error('this device is already linked to another account');
          error.status = 409;
          error.code = 'email_conflict';
          throw error;
        }
      }
    }

    if (!user) user = await this.persistence.getAppUserByDevice(id);

    if (user) {
      if (
        canonicalEmail &&
        user.email &&
        user.email !== canonicalEmail
      ) {
        const error = new Error('this device is already linked to another email');
        error.status = 409;
        error.code = 'email_conflict';
        throw error;
      }
      const patch = { last_seen_at: new Date(this.now()).toISOString() };
      if (name) patch.display_name = name;
      if (method) patch.contact_method = method;
      if (contact) patch.contact_value = contact;
      if (canonicalEmail && !user.email) patch.email = canonicalEmail;
      user = (await this.persistence.updateAppUser(user.id, patch)) ?? user;
      if (canonicalEmail) await this.persistence.linkAppUserDevice(user.id, id);
    } else {
      user = await this.persistence.createAppUser({
        deviceId: id,
        email: canonicalEmail,
        displayName: name,
        contactMethod: method,
        contactValue: contact,
      });
      created = Boolean(user);
      // A concurrent registration can race us to the same email; make sure this
      // device is linked even when createAppUser returned the winner.
      if (user && canonicalEmail) await this.persistence.linkAppUserDevice(user.id, id);
    }

    if (!user) {
      const error = new Error('could not register this device');
      error.status = 503;
      error.code = 'storage_unavailable';
      throw error;
    }

    return { user, created, linked };
  }

  /* -------------------------------------------------------------- requests */

  /**
   * Record an upgrade request. The user contacts the operator out-of-band;
   * this row is what the admin console sees.
   */
  async requestUpgrade({
    deviceId,
    plan,
    email = null,
    contactMethod = null,
    contactValue = null,
    message = null,
  } = {}) {
    const id = normaliseDeviceId(deviceId);
    if (!id) {
      const error = new Error('deviceId is required');
      error.status = 400;
      error.code = 'bad_device';
      throw error;
    }

    const requestedPlan = normalisePlan(plan, '');
    if (requestedPlan !== APP_PLANS.MONTHLY && requestedPlan !== APP_PLANS.YEARLY) {
      const error = new Error('plan must be MONTHLY or YEARLY');
      error.status = 400;
      error.code = 'bad_plan';
      throw error;
    }

    // Registration is the single place that resolves email/device identity, so
    // a user can ask for an upgrade from either linked installation without
    // creating a duplicate account.
    const registered = await this.register({ deviceId: id, email, contactMethod, contactValue });
    let user = registered.user;
    if (user && !registered.created) {
      user = await this.getByDevice(id, { create: false, touch: true });
    }
    if (!user) {
      const error = new Error('could not create the device account');
      error.status = 503;
      error.code = 'storage_unavailable';
      throw error;
    }
    if (user.status === APP_STATUS.BANNED) {
      const error = new Error('this device is suspended');
      error.status = 403;
      error.code = 'banned';
      throw error;
    }

    // One pending request per account is enough, whichever linked device sent
    // it. Return it instead of letting a double tap create a second row.
    const pending = await this.persistence.listMembershipRequests({ status: 'PENDING', limit: 500 });
    const existing = pending.find((r) => r.user_id === user.id || r.device_id === id);
    if (existing) return { request: existing, duplicate: true, user };

    const cooldownKey = `${id}|${requestedPlan}`;
    const last = this.requestCooldowns.get(cooldownKey) ?? 0;
    const wait = Number(this.cfg.REQUEST_COOLDOWN_MS ?? 5 * 60_000);
    if (this.now() - last < wait) {
      const error = new Error('please wait before sending another request');
      error.status = 429;
      error.code = 'request_cooldown';
      error.retryInMs = wait - (this.now() - last);
      throw error;
    }

    const method = contactMethod && CONTACT_METHODS.includes(String(contactMethod).toLowerCase())
      ? String(contactMethod).toLowerCase()
      : user.contact_method ?? null;
    const contact = text(contactValue, 200) ?? user.contact_value ?? null;
    const note = text(message, Number(this.cfg.MAX_MESSAGE_CHARS ?? 500));

    const request = await this.persistence.createMembershipRequest({
      user_id: user.id,
      device_id: id,
      requested_plan: requestedPlan,
      contact_method: method,
      contact_value: contact,
      message: note,
    });

    if (!request) {
      const error = new Error('could not save the request — try again');
      error.status = 503;
      error.code = 'storage_unavailable';
      throw error;
    }

    // Keep the account's contact details fresh so the admin has a way to reply
    // even if the user later changes devices.
    if (method || contact) {
      user =
        (await this.persistence.updateAppUser(user.id, {
          contact_method: method ?? user.contact_method,
          contact_value: contact ?? user.contact_value,
          last_seen_at: new Date(this.now()).toISOString(),
        })) ?? user;
    }

    this.requestCooldowns.set(cooldownKey, this.now());
    if (this.requestCooldowns.size > 2_000) {
      const oldest = this.requestCooldowns.keys().next().value;
      if (oldest) this.requestCooldowns.delete(oldest);
    }

    log.info(`upgrade request ${request.id}: ${id} → ${requestedPlan}`);
    return { request, duplicate: false, user };
  }

  async listRequestsForDevice(deviceId, limit = 20) {
    const id = normaliseDeviceId(deviceId);
    if (!id) return [];
    const user = await this.getByDevice(id, { create: false, touch: false });
    const rows = await this.persistence.listMembershipRequests({ limit: 500 });
    return rows
      .filter((r) => r.device_id === id || (user?.id && r.user_id === user.id))
      .slice(0, limit);
  }

  /* ------------------------------------------------------------ admin views */

  async listUsers({ q = '', status = null, plan = null, expired = null, limit = 50, offset = 0 } = {}) {
    const rows = await this.persistence.listAppUsers(1_000);
    const needle = String(q || '').trim().toLowerCase();

    let items = rows.map((u) => this.publicUser(u)).filter(Boolean);
    if (needle) {
      items = items.filter((u) =>
        [u.deviceId, u.primaryDeviceId, u.email, u.displayName, u.contactValue, u.contactMethod]
          .filter(Boolean)
          .some((v) => String(v).toLowerCase().includes(needle))
      );
    }
    if (status) items = items.filter((u) => u.status === normaliseStatus(status));
    if (plan) items = items.filter((u) => u.plan === normalisePlan(plan, APP_PLANS.FREE));
    if (expired === true) items = items.filter((u) => u.expired);
    if (expired === false) items = items.filter((u) => !u.expired);

    const total = items.length;
    const page = items.slice(Math.max(0, offset), Math.max(0, offset) + Math.max(1, limit));
    return { items: page, total, limit, offset };
  }

  async listRequests({ status = null, limit = 100, offset = 0 } = {}) {
    const rows = await this.persistence.listMembershipRequests({ status, limit: 1_000 });
    const total = rows.length;
    return {
      items: rows.slice(Math.max(0, offset), Math.max(0, offset) + Math.max(1, limit)),
      total,
      limit,
      offset,
    };
  }

  async listTrades({ status = null, symbol = null, limit = 50, offset = 0 } = {}) {
    const rows = await this.persistence.listAiTrades({ status: null, limit: 1_000 });
    let items = rows.map((r) => this.toPublicTrade(r)).filter(Boolean);
    if (status) items = items.filter((t) => t.status === String(status).toUpperCase());
    if (symbol) items = items.filter((t) => t.symbol === String(symbol).toUpperCase());

    const total = items.length;
    return {
      items: items.slice(Math.max(0, offset), Math.max(0, offset) + Math.max(1, limit)),
      total,
      limit,
      offset,
    };
  }

  /** Dashboard counters. Kept in one place so the UI cannot disagree with itself. */
  async stats() {
    const users = await this.persistence.listAppUsers(1_000);
    const requests = await this.persistence.listMembershipRequests({ limit: 1_000 });
    const trades = await this.persistence.listAiTrades({ status: null, limit: 1_000 });

    const byPlan = { FREE: 0, MONTHLY: 0, YEARLY: 0 };
    let banned = 0;
    let expired = 0;
    for (const row of users) {
      const view = this.publicUser(row);
      if (!view) continue;
      if (view.status === APP_STATUS.BANNED) banned += 1;
      if (view.expired) expired += 1;
      byPlan[view.plan === APP_STATUS.BANNED ? APP_PLANS.FREE : view.plan] += 1;
    }

    const pending = requests.filter((r) => r.status === 'PENDING').length;
    const approved = requests.filter((r) => r.status === 'APPROVED').length;
    const rejected = requests.filter((r) => r.status === 'REJECTED').length;

    const closed = trades.filter((t) => t.status === 'CLOSED');
    const open = trades.filter((t) => t.status === 'OPEN');
    const wins = closed.filter((t) => Number(t.pnl_points) >= 0).length;
    const losses = closed.length - wins;
    const pnlPoints = closed.reduce((sum, t) => sum + (Number(t.pnl_points) || 0), 0);

    const bySymbol = {};
    for (const t of trades) {
      const s = (bySymbol[t.symbol] ??= { total: 0, open: 0, closed: 0, wins: 0, losses: 0, pnlPoints: 0 });
      s.total += 1;
      if (t.status === 'OPEN') s.open += 1;
      else {
        s.closed += 1;
        if (Number(t.pnl_points) >= 0) s.wins += 1;
        else s.losses += 1;
        s.pnlPoints += Number(t.pnl_points) || 0;
      }
    }

    return {
      users: {
        total: users.length,
        active: users.length - banned,
        banned,
        expired,
        free: byPlan.FREE,
        monthly: byPlan.MONTHLY,
        yearly: byPlan.YEARLY,
        paid: byPlan.MONTHLY + byPlan.YEARLY,
      },
      requests: { total: requests.length, pending, approved, rejected },
      trades: {
        total: trades.length,
        open: open.length,
        closed: closed.length,
        wins,
        losses,
        winRate: closed.length ? Math.round((wins / closed.length) * 1_000) / 10 : 0,
        pnlPoints: Math.round(pnlPoints * 100) / 100,
        bySymbol,
      },
      recentRequests: requests.slice(0, 6),
      openTrades: open.map((t) => this.toPublicTrade(t)),
    };
  }

  /* ------------------------------------------------------------ admin edits */

  /**
   * Change plan/expiry/status/contact. This is the only mutation path; the APK
   * itself can never upgrade or unban an account.
   */
  async updateUser(id, patch = {}) {
    const user = await this.persistence.getAppUser(id);
    if (!user) {
      const error = new Error('user not found');
      error.status = 404;
      error.code = 'not_found';
      throw error;
    }

    const out = {};
    if (patch.displayName !== undefined) out.display_name = text(patch.displayName, 80);
    if (patch.contactMethod !== undefined) {
      const method = String(patch.contactMethod ?? '').trim().toLowerCase();
      if (method && !CONTACT_METHODS.includes(method)) {
        const error = new Error('contactMethod must be telegram, whatsapp, email, other or empty');
        error.status = 400;
        error.code = 'bad_contact';
        throw error;
      }
      out.contact_method = method || null;
    }
    if (patch.contactValue !== undefined) out.contact_value = text(patch.contactValue, 200);
    if (patch.email !== undefined) {
      const raw = text(patch.email, 254);
      const email = raw ? normaliseEmail(raw) : null;
      if (raw && !email) {
        const error = new Error('email must be a valid address');
        error.status = 400;
        error.code = 'bad_email';
        throw error;
      }
      if (email) {
        const owner = await this.persistence.getAppUserByEmail(email);
        if (owner && owner.id !== user.id) {
          const error = new Error('that email already belongs to another account');
          error.status = 409;
          error.code = 'email_in_use';
          throw error;
        }
      }
      out.email = email;
    }
    if (patch.note !== undefined) out.note = text(patch.note, 1_000);
    if (patch.status !== undefined) {
      const status = String(patch.status ?? '').trim().toUpperCase();
      if (status !== APP_STATUS.ACTIVE && status !== APP_STATUS.BANNED) {
        const error = new Error('status must be ACTIVE or BANNED');
        error.status = 400;
        error.code = 'bad_status';
        throw error;
      }
      out.status = status;
    }

    const requestedPlan = patch.plan === undefined ? null : normalisePlan(patch.plan, '');
    if (patch.plan !== undefined && !requestedPlan) {
      const error = new Error('plan must be FREE, MONTHLY or YEARLY');
      error.status = 400;
      error.code = 'bad_plan';
      throw error;
    }

    let extendDays = 0;
    if (patch.extendDays !== undefined && patch.extendDays !== null && patch.extendDays !== '') {
      extendDays = Math.round(Number(patch.extendDays));
      if (!Number.isFinite(extendDays) || extendDays < 1 || extendDays > 3_650) {
        const error = new Error('extendDays must be between 1 and 3650');
        error.status = 400;
        error.code = 'bad_days';
        throw error;
      }
    }

    const targetPlan = requestedPlan ?? normalisePlan(user.plan, APP_PLANS.FREE);
    const planChanged = requestedPlan !== null && requestedPlan !== user.plan;
    const now = this.now();
    const nowIso = new Date(now).toISOString();

    if (targetPlan === APP_PLANS.FREE) {
      if (planChanged || requestedPlan !== null || extendDays) {
        out.plan = APP_PLANS.FREE;
        out.plan_started_at = null;
        out.plan_expires_at = null;
      }
      if (extendDays) {
        const error = new Error('cannot extend a FREE plan — choose MONTHLY or YEARLY');
        error.status = 400;
        error.code = 'free_no_expiry';
        throw error;
      }
    } else {
      if (planChanged) {
        out.plan = targetPlan;
        out.plan_started_at = user.plan_started_at ?? nowIso;
      }

      if (patch.expiresAt !== undefined) {
        if (patch.expiresAt === null || patch.expiresAt === '') {
          out.plan_expires_at = null;
        } else {
          const ms = parseMs(patch.expiresAt);
          if (!ms) {
            const error = new Error('expiresAt must be an ISO date');
            error.status = 400;
            error.code = 'bad_expiry';
            throw error;
          }
          out.plan_expires_at = new Date(ms).toISOString();
        }
      } else if (extendDays) {
        const base = Math.max(now, parseMs(out.plan_expires_at ?? user.plan_expires_at));
        out.plan_expires_at = new Date(base + extendDays * DAY_MS).toISOString();
        if (!out.plan_started_at && !user.plan_started_at) out.plan_started_at = nowIso;
      } else if (planChanged) {
        out.plan_expires_at = new Date(now + defaultPlanDays(targetPlan) * DAY_MS).toISOString();
      } else if (requestedPlan !== null && parseMs(user.plan_expires_at) <= now) {
        // Re-selecting the same paid plan on an expired row must renew it,
        // otherwise "set MONTHLY" would look like a no-op in the console.
        out.plan_started_at = user.plan_started_at ?? nowIso;
        out.plan_expires_at = new Date(now + defaultPlanDays(targetPlan) * DAY_MS).toISOString();
      } else if (!parseMs(user.plan_expires_at)) {
        // A paid row with no expiry would otherwise be treated as FREE forever.
        out.plan_expires_at = new Date(now + defaultPlanDays(targetPlan) * DAY_MS).toISOString();
      }
    }

    if (out.plan_expires_at && !out.plan_started_at && !user.plan_started_at) {
      out.plan_started_at = nowIso;
    }

    const updated = await this.persistence.updateAppUser(id, out);
    if (!updated) {
      const error = new Error('could not update the user');
      error.status = 503;
      error.code = 'storage_unavailable';
      throw error;
    }
    log.info(
      `admin updated ${user.device_id}: plan=${updated.plan} status=${updated.status}` +
        (updated.plan_expires_at ? ` until=${updated.plan_expires_at}` : '')
    );
    return updated;
  }

  /**
   * Approve or reject one request. Approval applies the plan atomically enough
   * for this use case (the request is marked resolved afterwards).
   */
  async resolveRequest(id, { decision = 'approve', plan = null, days = null, note = null, admin = 'admin' } = {}) {
    const rows = await this.persistence.listMembershipRequests({ limit: 1_000 });
    const request = rows.find((r) => r.id === id);
    if (!request) {
      const error = new Error('request not found');
      error.status = 404;
      error.code = 'not_found';
      throw error;
    }
    if (request.status !== 'PENDING') {
      const error = new Error(`request is already ${request.status.toLowerCase()}`);
      error.status = 409;
      error.code = 'already_resolved';
      throw error;
    }

    const decisionValue = String(decision ?? 'approve').trim().toLowerCase();
    if (decisionValue !== 'approve' && decisionValue !== 'reject') {
      const error = new Error('decision must be approve or reject');
      error.status = 400;
      error.code = 'bad_decision';
      throw error;
    }
    const approve = decisionValue === 'approve';
    let targetPlan = null;
    let extra = 0;

    if (approve) {
      targetPlan = normalisePlan(plan ?? request.requested_plan, '');
      if (targetPlan !== APP_PLANS.MONTHLY && targetPlan !== APP_PLANS.YEARLY) {
        const error = new Error('approved plan must be MONTHLY or YEARLY');
        error.status = 400;
        error.code = 'bad_plan';
        throw error;
      }
      extra = days == null ? defaultPlanDays(targetPlan) : Math.round(Number(days));
      if (!Number.isFinite(extra) || extra < 1 || extra > 3_650) {
        const error = new Error('days must be between 1 and 3650');
        error.status = 400;
        error.code = 'bad_days';
        throw error;
      }
    }

    // Claim the request first, conditionally on it still being PENDING. This
    // is the concurrency gate: two admins (or two tabs) cannot both grant the
    // same request, because only the first conditional update returns a row.
    const resolved = await this.persistence.resolveMembershipRequest(
      id,
      {
        status: approve ? 'APPROVED' : 'REJECTED',
        admin_note: text(note, 1_000),
        resolved_by: text(admin, 80) ?? 'admin',
        resolved_at: new Date(this.now()).toISOString(),
      },
      { expectedStatus: 'PENDING' }
    );
    if (!resolved) {
      const error = new Error('this request was already resolved or is unavailable');
      error.status = 409;
      error.code = 'already_resolved';
      throw error;
    }

    let updatedUser = null;
    if (approve) {
      try {
        updatedUser = await this.updateUser(request.user_id, { plan: targetPlan, extendDays: extra });
      } catch (err) {
        // Put the request back in the queue so the operator can retry; do not
        // leave an approved request that never granted a plan.
        try {
          await this.persistence.resolveMembershipRequest(id, {
            status: 'PENDING',
            admin_note: null,
            resolved_by: null,
            resolved_at: null,
          });
        } catch {
          /* best effort */
        }
        throw err;
      }
    }

    return { request: resolved, user: updatedUser, approved: approve };
  }

  /* ---------------------------------------------------------- app history */

  /**
   * Closed AI trades the device's plan is allowed to read.
   * The server filters by closed_at — never by a client-supplied date.
   */
  async history(deviceId, { symbol = null, limit = 100, offset = 0 } = {}) {
    const user = await this.getByDevice(deviceId, { create: false, touch: true });
    if (!user) {
      const error = new Error('register this device first');
      error.status = 401;
      error.code = 'unregistered';
      throw error;
    }
    if (user.status === APP_STATUS.BANNED) {
      const error = new Error('this device is suspended');
      error.status = 403;
      error.code = 'banned';
      throw error;
    }

    const plan = effectivePlan(user, this.now());
    const days = this.retentionFor(plan);
    const since = new Date(this.now() - days * DAY_MS).toISOString();
    const safeLimit = Math.min(200, Math.max(1, Math.round(Number(limit) || 100)));
    const safeOffset = Math.max(0, Math.round(Number(offset) || 0));
    const rows = await this.persistence.listAiTrades({
      status: 'CLOSED',
      symbol: symbol ? String(symbol).toUpperCase() : null,
      since,
      limit: safeLimit,
      offset: safeOffset,
    });

    return {
      plan,
      retentionDays: days,
      since,
      limit: safeLimit,
      offset: safeOffset,
      hasMore: rows.length === safeLimit,
      items: rows.map((r) => this.toPublicTrade(r)).filter(Boolean),
    };
  }

  /** Open AI trades + the engine's own strategy positions. */
  async openTrades(deviceId, { strategyPositions = [] } = {}) {
    const user = await this.getByDevice(deviceId, { create: false, touch: true });
    if (!user) {
      const error = new Error('register this device first');
      error.status = 401;
      error.code = 'unregistered';
      throw error;
    }
    if (user.status === APP_STATUS.BANNED) {
      const error = new Error('this device is suspended');
      error.status = 403;
      error.code = 'banned';
      throw error;
    }

    const rows = await this.persistence.listOpenAiTrades();
    return {
      plan: effectivePlan(user, this.now()),
      ai: rows.map((r) => this.toPublicTrade(r)).filter(Boolean),
      // The strategy engine's positions are server-side paper positions; they
      // are included so the app can show both books in one screen.
      strategy: Array.isArray(strategyPositions) ? strategyPositions : [],
      serverTime: new Date(this.now()).toISOString(),
    };
  }

  /** Map the snake_case ledger row to the camelCase contract used by the app. */
  toPublicTrade(row) {
    if (!row?.id) return null;
    const openedMs = Number(row.opened_ms ?? 0) || parseMs(row.opened_at) || null;
    const closedMs = Number(row.closed_ms ?? 0) || parseMs(row.closed_at) || null;
    return {
      id: row.id,
      symbol: row.symbol,
      action: row.action,
      status: row.status,
      stage: row.stage,
      entry: row.entry_price ?? null,
      stopLoss: row.stop_loss ?? null,
      takeProfit1: row.take_profit_1 ?? null,
      takeProfit2: row.take_profit_2 ?? null,
      rr: row.rr ?? null,
      confidence: row.confidence ?? null,
      reasoning: row.reasoning ?? null,
      model: row.model ?? null,
      openedAt: row.opened_at ?? null,
      closedAt: row.closed_at ?? null,
      exitPrice: row.exit_price ?? null,
      exitReason: row.exit_reason ?? null,
      pnlPoints: row.pnl_points ?? null,
      pnlPct: row.pnl_pct ?? null,
      lastPrice: row.last_price ?? null,
      mfe: row.mfe ?? 0,
      mae: row.mae ?? 0,
      durationMs: openedMs && closedMs ? Math.max(0, closedMs - openedMs) : null,
    };
  }

  /** Small diagnostics block for /health and the admin header. */
  status() {
    return {
      enabled: Boolean(this.cfg.ENABLED !== false),
      persistence: Boolean(this.persistence?.client),
      plans: { ...APP_PLANS },
      retentionDays: {
        FREE: this.retentionFor(APP_PLANS.FREE),
        MONTHLY: this.retentionFor(APP_PLANS.MONTHLY),
        YEARLY: this.retentionFor(APP_PLANS.YEARLY),
      },
      pendingCooldowns: this.requestCooldowns.size,
    };
  }
}

export default AppUserService;
