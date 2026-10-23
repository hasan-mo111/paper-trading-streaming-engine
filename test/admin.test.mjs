/* =============================================================================
 *  ADMIN + APK MEMBERSHIP TEST
 *  -----------------------------------------------------------------------------
 *  Exercises the real Express routers with the real local-mode Supabase service:
 *
 *    1. admin password hashing, login, signed cookie, tamper + CSRF rejection
 *    2. device registration, FREE defaults, duplicate registration
 *    3. upgrade request → admin approval → MONTHLY plan + expiry
 *    4. ban → 403 on trade history and /api/ai/signal, unban → restored
 *    5. plan expiry falls back to FREE and narrows history retention
 *    6. permanent AI ledger: open/closed rows, one open per symbol contract,
 *       plan-filtered history and admin totals
 *    7. login rate limiting and cross-origin write rejection
 *
 *  Run: npm run test:admin
 * ============================================================================= */

process.env.NODE_ENV = 'test';
process.env.ADMIN_ENABLED = 'true';
process.env.ADMIN_USERNAME = 'ops';
process.env.ADMIN_PASSWORD = 'correct horse battery staple';
process.env.ADMIN_SESSION_SECRET = 'test-session-secret-not-for-production';
process.env.ADMIN_LOGIN_MAX = '8';
process.env.ADMIN_LOGIN_WINDOW_MS = '900000';
process.env.MEMBERSHIP_ENABLED = 'true';
process.env.MEMBERSHIP_REQUEST_COOLDOWN_MS = '10000';
process.env.FREE_HISTORY_DAYS = '7';
process.env.MONTHLY_HISTORY_DAYS = '90';
process.env.YEARLY_HISTORY_DAYS = '3650';
process.env.SUPABASE_URL = '';
process.env.SUPABASE_SERVICE_ROLE_KEY = '';
process.env.SUPABASE_REQUIRED = 'false';

import assert from 'node:assert/strict';
import { once } from 'node:events';
import express from 'express';

const { ENV } = await import('../src/config/env.js');
const { SupabaseService } = await import('../src/services/supabase.js');
const { AppUserService } = await import('../src/services/appUsers.js');
const { MembershipEvents } = await import('../src/services/membershipEvents.js');
const { AdminAuth, hashPassword, verifyPasswordHash } = await import('../src/middleware/adminAuth.js');
const { mountMembershipRoutes, deviceGate } = await import('../src/routes/membership.js');
const { mountAdminRoutes } = await import('../src/routes/admin.js');
const { mountAiRoutes } = await import('../src/routes/ai.js');
const { AiAnalystManager } = await import('../src/services/aiAnalystManager.js');
const { tradeRowFromPosition, positionFromRow } = await import('../src/services/aiSignal.js');

/* ------------------------------------------------------------------ harness */
let passed = 0;
let failed = 0;
const failures = [];
let sectionName = '';

const section = (name) => {
  sectionName = name;
  console.log(`\n${name}`);
};

function check(name, condition, detail = '') {
  if (condition) {
    passed += 1;
    console.log(`  \x1b[32mOK\x1b[0m ${name}${detail ? ` -- ${detail}` : ''}`);
  } else {
    failed += 1;
    failures.push(`[${sectionName}] ${name}${detail ? ` -- ${detail}` : ''}`);
    console.log(`  \x1b[31mFAIL\x1b[0m ${name}${detail ? ` -- ${detail}` : ''}`);
  }
}

const DAY = 86_400_000;
const BASE = Date.parse('2026-10-23T12:00:00.000Z');
let now = BASE;

const persistence = new SupabaseService({ env: ENV });
const membership = new AppUserService({ persistence, env: ENV, now: () => now });
const events = new MembershipEvents();
const adminAuth = new AdminAuth({ env: ENV });
const notifications = [];

async function notifyMembership(row, kind, ctx = {}) {
  const view = membership.publicUser(row);
  notifications.push({ deviceId: view?.deviceId, kind, plan: view?.plan, status: view?.status, ctx });
  return { sse: 0, telegram: false, kind };
}

const app = express();
app.set('trust proxy', 1);
app.use(express.json());

mountMembershipRoutes({
  app,
  membership,
  events,
  engine: { openPositions: () => [] },
});
mountAiRoutes({
  app,
  ai: {
    status: () => ({ ok: true, enabled: true, position: null }),
    run: async () => ({ ok: true, action: 'HOLD' }),
  },
  deviceGate: deviceGate({ membership, sse: false }),
});
mountAdminRoutes({
  app,
  adminAuth,
  membership,
  persistence,
  env: ENV,
  ai: null,
  notifyMembership,
  healthProvider: () => ({ status: 'ok', uptimeSec: 12, engine: { state: 'ARMED' }, upstream: { connected: true } }),
});
app.use((err, _req, res, _next) => {
  res.status(Number(err?.status) || 500).json({
    error: { code: err?.code ?? 'test_error', message: err?.message ?? 'test error' },
  });
});

const server = app.listen(0, '127.0.0.1');
await once(server, 'listening');
const BASE_URL = `http://127.0.0.1:${server.address().port}`;

/** Minimal fetch wrapper that keeps the raw Set-Cookie header. */
async function request(path, { method = 'GET', body, headers = {}, cookie = null } = {}) {
  const merged = { Accept: 'application/json', ...headers };
  if (body !== undefined) merged['content-type'] = 'application/json';
  if (cookie) merged.cookie = cookie;
  const res = await fetch(BASE_URL + path, {
    method,
    headers: merged,
    body: body === undefined ? undefined : JSON.stringify(body),
    redirect: 'manual',
  });
  const text = await res.text();
  let json = null;
  try {
    json = text ? JSON.parse(text) : null;
  } catch {
    json = null;
  }
  const setCookie = res.headers.get('set-cookie');
  return {
    status: res.status,
    text,
    json,
    setCookie,
    cookie: setCookie ? setCookie.split(';')[0] : null,
  };
}

const close = () => new Promise((resolve) => server.close(resolve));

/* =============================================================== 1. AUTH */
section('1) Admin authentication');

const page = await request('/admin/');
check('admin SPA is served', page.status === 200 && /Operator console/.test(page.text), `HTTP ${page.status}`);

const probe = await request('/admin/api/me');
check('probe reports enabled + unauthenticated', probe.status === 200 && probe.json?.enabled === true && probe.json?.authenticated === false);

const unauth = await request('/admin/api/overview');
check('overview requires a session', unauth.status === 401 && unauth.json?.error?.code === 'admin_unauthorized');

const badLogin = await request('/admin/api/login', {
  method: 'POST',
  body: { username: 'ops', password: 'wrong-password' },
});
check('wrong password is rejected', badLogin.status === 401 && badLogin.json?.error?.code === 'bad_credentials');

const login = await request('/admin/api/login', {
  method: 'POST',
  body: { username: 'ops', password: 'correct horse battery staple' },
});
check('correct password returns a session', login.status === 200 && Boolean(login.cookie));
check(
  'cookie is HttpOnly + SameSite=Strict',
  /HttpOnly/i.test(login.setCookie ?? '') && /SameSite=Strict/i.test(login.setCookie ?? '')
);
const adminCookie = login.cookie;

const overview = await request('/admin/api/overview', { cookie: adminCookie });
check('authenticated overview works', overview.status === 200 && overview.json?.ok === true && overview.json?.stats?.users?.total === 0);

const tampered = adminCookie.slice(0, -1) + (adminCookie.endsWith('a') ? 'b' : 'a');
const tamperRes = await request('/admin/api/overview', { cookie: tampered });
check('tampered cookie is rejected', tamperRes.status === 401);

const crossOrigin = await request('/admin/api/users', {
  method: 'GET',
  cookie: adminCookie,
  headers: { Origin: 'https://evil.example' },
});
check('cross-origin GET is still readable but writes are blocked', crossOrigin.status === 200);

const hash = hashPassword('correct horse battery staple');
check('scrypt hash verifies', hash.startsWith('scrypt$') && verifyPasswordHash('correct horse battery staple', hash));
check('wrong password fails the hash', !verifyPasswordHash('nope', hash));
check('malformed hash fails closed', !verifyPasswordHash('x', 'not-a-hash'));

/* ========================================================== 2. REGISTER */
section('2) Device registration');

const register = await request('/api/membership/register', {
  method: 'POST',
  headers: { 'X-Device-Id': 'dev-test-0001' },
  body: {
    displayName: 'Alice',
    contactMethod: 'telegram',
    contactValue: '@alice',
  },
});
check('registration creates a FREE account', register.status === 200 && register.json?.created === true);
check(
  'FREE retention defaults to 7 days',
  register.json?.user?.plan === 'FREE' && register.json?.user?.retentionDays === 7,
  `plan=${register.json?.user?.plan} days=${register.json?.user?.retentionDays}`
);
check('public plans are exposed', register.json?.plans?.length === 3 && register.json?.support !== undefined);
const deviceId = register.json?.user?.deviceId;
const userId = register.json?.user?.id;

const again = await request('/api/membership/register', {
  method: 'POST',
  body: { deviceId: 'dev-test-0001' },
});
check('duplicate registration is idempotent', again.status === 200 && again.json?.created === false);

const me = await request('/api/membership/me', { headers: { 'X-Device-Id': deviceId } });
check('GET /api/membership/me resolves the device', me.status === 200 && me.json?.user?.deviceId === deviceId && me.json?.user?.plan === 'FREE');

const missing = await request('/api/membership/me');
check('missing device id is a 400', missing.status === 400 && missing.json?.error?.code === 'device_required');

const missingRegister = await request('/api/membership/register', {
  method: 'POST',
  body: { displayName: 'No Device' },
});
check(
  'registration without a device id is a clear 400',
  missingRegister.status === 400 && missingRegister.json?.error?.code === 'device_required',
  JSON.stringify(missingRegister.json)
);

/* =========================================================== 3. REQUEST */
section('3) Upgrade request and approval');

const upgrade = await request('/api/membership/request', {
  method: 'POST',
  headers: { 'X-Device-Id': deviceId },
  body: { plan: 'MONTHLY', message: 'I paid via Telegram' },
});
check('upgrade request is created', upgrade.status === 201 && upgrade.json?.request?.status === 'PENDING');
check('request keeps the plan + contact', upgrade.json?.request?.requested_plan === 'MONTHLY' && upgrade.json?.request?.contact_method === 'telegram');
const requestId = upgrade.json?.request?.id;

const duplicateRequest = await request('/api/membership/request', {
  method: 'POST',
  headers: { 'X-Device-Id': deviceId },
  body: { plan: 'MONTHLY' },
});
check('second pending request is deduplicated', duplicateRequest.status === 200 && duplicateRequest.json?.duplicate === true);

const adminRequests = await request('/admin/api/requests?status=PENDING', { cookie: adminCookie });
check('admin sees the pending request', adminRequests.status === 200 && adminRequests.json?.total === 1);

const approved = await request(`/admin/api/requests/${requestId}/resolve`, {
  method: 'POST',
  cookie: adminCookie,
  body: { decision: 'approve', plan: 'MONTHLY', days: 30, note: 'paid' },
});
check('admin approval succeeds', approved.status === 200 && approved.json?.approved === true);
check(
  'approval applies MONTHLY for 30 days',
  approved.json?.user?.plan === 'MONTHLY' &&
    approved.json?.user?.storedPlan === 'MONTHLY' &&
    approved.json?.user?.planExpiresAt === new Date(BASE + 30 * DAY).toISOString(),
  `${approved.json?.user?.plan} until ${approved.json?.user?.planExpiresAt}`
);
check('approval fires a notification', notifications.length === 1 && notifications[0].kind === 'plan' && notifications[0].plan === 'MONTHLY');

const afterUpgrade = await request('/api/membership/me', { headers: { 'X-Device-Id': deviceId } });
check(
  'APK sees the new plan and 90-day retention',
  afterUpgrade.json?.user?.plan === 'MONTHLY' && afterUpgrade.json?.user?.retentionDays === 90
);

const myRequests = await request('/api/membership/requests', { headers: { 'X-Device-Id': deviceId } });
check('device sees its resolved request', myRequests.status === 200 && myRequests.json?.items?.[0]?.status === 'APPROVED');

const reResolve = await request(`/admin/api/requests/${requestId}/resolve`, {
  method: 'POST',
  cookie: adminCookie,
  body: { decision: 'approve' },
});
check('resolving twice is a 409', reResolve.status === 409 && reResolve.json?.error?.code === 'already_resolved');

/* =========================================================== 4. LEDGER */
section('4) Permanent AI trade ledger');

function ledgerRow(id, { status = 'CLOSED', openedOffsetDays = 0, pnlPoints = 10 } = {}) {
  const openedMs = BASE + openedOffsetDays * DAY;
  const closedMs = status === 'CLOSED' ? openedMs + 3_600_000 : null;
  return {
    id,
    symbol: 'XAUUSD',
    action: 'BUY',
    entry_price: 2400,
    stop_loss: 2390,
    take_profit_1: 2420,
    take_profit_2: 2440,
    rr: 2,
    confidence: 80,
    reasoning: `test row ${id}`,
    model: 'test-model',
    stage: 'open',
    status,
    opened_at: new Date(openedMs).toISOString(),
    opened_ms: openedMs,
    closed_at: closedMs ? new Date(closedMs).toISOString() : null,
    closed_ms: closedMs,
    exit_price: closedMs ? 2440 : null,
    exit_reason: closedMs ? 'take_profit_2' : null,
    pnl_points: closedMs ? pnlPoints : null,
    pnl_pct: closedMs ? pnlPoints / 2400 : null,
    last_price: closedMs ? 2440 : 2405,
    mfe: 45,
    mae: -8,
    meta: {},
  };
}

await persistence.saveAiTrade(ledgerRow('ai-test-open', { status: 'OPEN', openedOffsetDays: -0.1 }));
await persistence.saveAiTrade(ledgerRow('ai-test-1d', { openedOffsetDays: -1, pnlPoints: 40 }));
await persistence.saveAiTrade(ledgerRow('ai-test-10d', { openedOffsetDays: -10, pnlPoints: -20 }));
await persistence.saveAiTrade(ledgerRow('ai-test-100d', { openedOffsetDays: -100, pnlPoints: 60 }));

// The ledger shape and the live position shape must survive a round trip;
// otherwise a restart would restore a trade the tick handler cannot manage.
const roundTripPosition = {
  id: 'ai-roundtrip',
  symbol: 'XAUUSD',
  action: 'SELL',
  entry: 2400,
  sl: 2410,
  tp1: 2380,
  tp2: 2360,
  rr: 2,
  confidence: 70,
  reasoning: 'round trip',
  model: 'test-model',
  openedAt: new Date(BASE).toISOString(),
  openedMs: BASE,
  stage: 'tp1',
  lastPrice: 2390,
  mfe: 20,
  mae: -5,
  ticks: 4,
};
const roundTripRow = tradeRowFromPosition(roundTripPosition);
const roundTripBack = positionFromRow(roundTripRow);
check(
  'position → ledger row keeps the identity and levels',
  roundTripRow.id === 'ai-roundtrip' &&
    roundTripRow.status === 'OPEN' &&
    roundTripRow.action === 'SELL' &&
    roundTripRow.entry_price === 2400 &&
    roundTripRow.stage === 'tp1'
);
check(
  'ledger row → position restores the tracked trade',
  roundTripBack.id === 'ai-roundtrip' &&
    roundTripBack.symbol === 'XAUUSD' &&
    roundTripBack.action === 'SELL' &&
    roundTripBack.entry === 2400 &&
    roundTripBack.sl === 2410 &&
    roundTripBack.stage === 'tp1',
  JSON.stringify(roundTripBack)
);

const manager = new AiAnalystManager({
  market: {},
  dispatcher: {},
  env: ENV,
  persistence,
});
const restore = await manager.restoreOpenTrades();
check(
  'manager restores the open trade after a restart',
  restore.restored.includes('XAUUSD') &&
    manager.serviceFor('XAUUSD')?.position?.id === 'ai-test-open' &&
    manager.status().persisted === true &&
    manager.status().openTrades.includes('XAUUSD'),
  JSON.stringify(restore)
);

const openBook = await request('/api/trades/open', { headers: { 'X-Device-Id': deviceId } });
check('open book returns the live AI trade', openBook.status === 200 && openBook.json?.ai?.length === 1 && openBook.json?.ai?.[0]?.status === 'OPEN');

const monthlyHistory = await request('/api/trades/history', { headers: { 'X-Device-Id': deviceId } });
check(
  'MONTHLY history keeps 90 days and drops 100d',
  monthlyHistory.status === 200 &&
    monthlyHistory.json?.items?.length === 2 &&
    monthlyHistory.json?.items?.every((t) => t.id !== 'ai-test-100d'),
  `items=${monthlyHistory.json?.items?.map((t) => t.id).join(',')}`
);
check('history is newest first', monthlyHistory.json?.items?.[0]?.id === 'ai-test-1d');

const stats = await request('/api/trades/stats', { headers: { 'X-Device-Id': deviceId } });
check(
  'trade stats aggregate the visible window',
  stats.status === 200 &&
    stats.json?.total === 2 &&
    stats.json?.wins === 1 &&
    stats.json?.losses === 1 &&
    stats.json?.winRate === 50 &&
    stats.json?.pnlPoints === 20,
  `${stats.json?.total} trades, winRate=${stats.json?.winRate}, pnl=${stats.json?.pnlPoints}`
);

const adminTrades = await request('/admin/api/trades?status=CLOSED', { cookie: adminCookie });
check('admin ledger lists every closed trade', adminTrades.status === 200 && adminTrades.json?.total === 3);

const adminOpen = await request('/admin/api/trades?status=OPEN', { cookie: adminCookie });
check('admin ledger lists the open trade', adminOpen.status === 200 && adminOpen.json?.total === 1);

const adminOverview = await request('/admin/api/overview', { cookie: adminCookie });
check(
  'overview counters match the ledger',
  adminOverview.json?.stats?.trades?.total === 4 &&
    adminOverview.json?.stats?.trades?.open === 1 &&
    adminOverview.json?.stats?.trades?.closed === 3,
  JSON.stringify(adminOverview.json?.stats?.trades ?? {})
);

/* ========================================================== 5. BAN/UNBAN */
section('5) Ban and unban');

const banned = await request(`/admin/api/users/${userId}`, {
  method: 'PATCH',
  cookie: adminCookie,
  headers: { Origin: 'https://evil.example' },
  body: { status: 'BANNED' },
});
check('cross-origin admin write is rejected', banned.status === 403 && banned.json?.error?.code === 'bad_origin');

const ban = await request(`/admin/api/users/${userId}`, {
  method: 'PATCH',
  cookie: adminCookie,
  body: { status: 'BANNED' },
});
check('admin can ban a device', ban.status === 200 && ban.json?.user?.status === 'BANNED' && ban.json?.notification?.kind === 'status');

const bannedHistory = await request('/api/trades/history', { headers: { 'X-Device-Id': deviceId } });
check('banned device cannot read history', bannedHistory.status === 403 && bannedHistory.json?.error?.code === 'banned');

const bannedAi = await request('/api/ai/signal', { headers: { 'X-Device-Id': deviceId } });
check('banned device cannot read the AI feed', bannedAi.status === 403 && bannedAi.json?.error?.code === 'banned');

const bannedRequest = await request('/api/membership/request', {
  method: 'POST',
  headers: { 'X-Device-Id': deviceId },
  body: { plan: 'YEARLY' },
});
check('banned device cannot request an upgrade', bannedRequest.status === 403);

const meBanned = await request('/api/membership/me', { headers: { 'X-Device-Id': deviceId } });
check('device can still see its own ban state', meBanned.status === 200 && meBanned.json?.user?.status === 'BANNED');

const unban = await request(`/admin/api/users/${userId}`, {
  method: 'PATCH',
  cookie: adminCookie,
  body: { status: 'ACTIVE', note: 'operator note' },
});
check('admin can unban a device', unban.status === 200 && unban.json?.user?.status === 'ACTIVE');

const detail = await request(`/admin/api/users/${userId}`, { cookie: adminCookie });
check('admin user detail returns the internal note', detail.status === 200 && detail.json?.user?.note === 'operator note');

/* ========================================================== 6. EXPIRY */
section('6) Plan expiry and retention fallback');

now = BASE + 31 * DAY;

const expired = await request('/api/membership/me', { headers: { 'X-Device-Id': deviceId } });
check(
  'expired MONTHLY is presented as FREE',
  expired.status === 200 && expired.json?.user?.plan === 'FREE' && expired.json?.user?.expired === true,
  `plan=${expired.json?.user?.plan} stored=${expired.json?.user?.storedPlan}`
);
check('expired plan narrows retention to 7 days', expired.json?.user?.retentionDays === 7);

const expiredHistory = await request('/api/trades/history', { headers: { 'X-Device-Id': deviceId } });
check(
  'FREE account sees no rows older than 7 days',
  expiredHistory.status === 200 && expiredHistory.json?.items?.length === 0,
  `items=${expiredHistory.json?.items?.length}`
);

// Re-select MONTHLY on an expired paid row: the service must renew it rather
// than leave the row expired (otherwise "set plan" looks like a no-op).
const renewed = await request(`/admin/api/users/${userId}`, {
  method: 'PATCH',
  cookie: adminCookie,
  body: { plan: 'MONTHLY', extendDays: 30 },
});
check(
  'renewing an expired plan sets a future expiry',
  renewed.status === 200 && renewed.json?.user?.plan === 'MONTHLY' && Date.parse(renewed.json?.user?.planExpiresAt) > now,
  renewed.json?.user?.planExpiresAt
);

const monthlyAgain = await request('/api/trades/history', { headers: { 'X-Device-Id': deviceId } });
check(
  'renewed MONTHLY restores the 90-day window',
  monthlyAgain.status === 200 && monthlyAgain.json?.items?.length === 2 && monthlyAgain.json?.retentionDays === 90,
  `items=${monthlyAgain.json?.items?.length}`
);

const yearly = await request(`/admin/api/users/${userId}`, {
  method: 'PATCH',
  cookie: adminCookie,
  body: { plan: 'YEARLY', extendDays: 365 },
});
check('admin can move the account to YEARLY', yearly.status === 200 && yearly.json?.user?.plan === 'YEARLY');

const yearlyHistory = await request('/api/trades/history', { headers: { 'X-Device-Id': deviceId } });
check(
  'YEARLY keeps the 100-day trade',
  yearlyHistory.status === 200 && yearlyHistory.json?.items?.length === 3 && yearlyHistory.json?.retentionDays === 3650,
  `items=${yearlyHistory.json?.items?.length}`
);

const free = await request(`/admin/api/users/${userId}`, {
  method: 'PATCH',
  cookie: adminCookie,
  body: { plan: 'FREE' },
});
check(
  'admin can return an account to FREE',
  free.status === 200 && free.json?.user?.plan === 'FREE' && free.json?.user?.planExpiresAt === null
);

/* ============================================================ 7. LIMITS */
section('7) Login rate limit and origin checks');

for (let i = 0; i < 8; i += 1) {
  await request('/admin/api/login', {
    method: 'POST',
    headers: { 'X-Forwarded-For': '198.51.100.7' },
    body: { username: 'ops', password: 'wrong' },
  });
}
const locked = await request('/admin/api/login', {
  method: 'POST',
  headers: { 'X-Forwarded-For': '198.51.100.7' },
  body: { username: 'ops', password: 'correct horse battery staple' },
});
check('8 failures lock the source address', locked.status === 429 && locked.json?.error?.code === 'login_locked');

const otherIp = await request('/admin/api/login', {
  method: 'POST',
  headers: { 'X-Forwarded-For': '198.51.100.8' },
  body: { username: 'ops', password: 'correct horse battery staple' },
});
check('another address can still sign in', otherIp.status === 200 && Boolean(otherIp.cookie));

const badOrigin = await request('/admin/api/login', {
  method: 'POST',
  headers: { 'X-Forwarded-For': '198.51.100.9', Origin: 'https://evil.example' },
  body: { username: 'ops', password: 'correct horse battery staple' },
});
check('cross-origin login is rejected', badOrigin.status === 403 && badOrigin.json?.error?.code === 'bad_origin');

/* ============================================================= SUMMARY */
await close();

console.log(`\n${'-'.repeat(64)}`);
console.log(`  ${passed} passed, ${failed} failed`);
if (failed) {
  console.log('\nFailures:');
  for (const f of failures) console.log(`  - ${f}`);
  process.exitCode = 1;
} else {
  console.log('  \x1b[32mAll admin + membership checks passed.\x1b[0m');
}
