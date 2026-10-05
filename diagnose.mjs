/* =============================================================================
 *  UPSTREAM DIAGNOSTIC  —  "can this machine reach the tick feed at all?"
 *  Developed by Eng Hasan Mohamad
 * -----------------------------------------------------------------------------
 *  Run it in the SAME place the server runs (your PC, or the Render shell):
 *
 *      node diagnose.mjs
 *
 *  Answers three questions that account for almost every "it works at home but
 *  not on Render" report:
 *
 *    1. Is Supabase configured, and does the REST endpoint actually answer?
 *    2. Can this host open the WebSocket and complete the SignalR handshake?
 *    3. Does the OHLC REST backfill return bars for every timeframe?
 *
 *  Every probe prints PASS / FAIL with the real error, so a network block can
 *  be told apart from a code problem.
 * ============================================================================= */

import { WebSocket } from 'ws';
import { ENV } from './src/config/env.js';
import { SYMBOLS, TIMEFRAMES } from './src/config/db.js';

const c = {
  g: '\x1b[32m',
  r: '\x1b[31m',
  y: '\x1b[33m',
  d: '\x1b[90m',
  b: '\x1b[1m',
  x: '\x1b[0m',
};
const ok = (m) => console.log(`${c.g}  PASS${c.x}  ${m}`);
const bad = (m) => console.log(`${c.r}  FAIL${c.x}  ${m}`);
const warn = (m) => console.log(`${c.y}  WARN${c.x}  ${m}`);
const info = (m) => console.log(`${c.d}        ${m}${c.x}`);
const head = (m) => console.log(`\n${c.b}${m}${c.x}`);
const RS = '\u001e';

console.log(`${c.b}AETHERON CORE — deployment diagnostic${c.x}`);
console.log(`${c.d}node ${process.version} · ${process.platform}/${process.arch} · ${process.env.NODE_ENV ?? 'development'}${c.x}`);

/* =============================================================================
 *  1. SUPABASE
 * ============================================================================= */
head('1. Supabase');
if (!ENV.supabase.URL || !ENV.supabase.SERVICE_ROLE_KEY) {
  warn('SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY are not set');
  info('→ persistence is OFF; candles live in RAM only and the 50-day Donchian');
  info('  channel will be empty after a restart until the REST backfill runs.');
} else {
  info(`project  : ${ENV.supabase.URL}`);
  info(`key      : ${ENV.supabase.SERVICE_ROLE_KEY.slice(0, 12)}… (${ENV.supabase.SERVICE_ROLE_KEY.length} chars)`);
  info(`schema   : ${ENV.supabase.SCHEMA}`);

  try {
    const ctl = new AbortController();
    const timer = setTimeout(() => ctl.abort(), ENV.supabase.TIMEOUT_MS);
    const res = await fetch(`${ENV.supabase.URL}/rest/v1/`, {
      headers: { apikey: ENV.supabase.SERVICE_ROLE_KEY, Authorization: `Bearer ${ENV.supabase.SERVICE_ROLE_KEY}` },
      signal: ctl.signal,
    }).finally(() => clearTimeout(timer));

    if (res.ok) ok(`REST endpoint reachable (HTTP ${res.status})`);
    else bad(`REST endpoint returned HTTP ${res.status} — check the project URL and key`);
  } catch (e) {
    bad(`cannot reach Supabase: ${e.message}`);
    info('→ a DNS/TLS problem here looks identical to a bad key; test in a browser first.');
  }

  // Which tables exist? Missing ones are the usual cause of silent failures.
  for (const table of ['candles', 'profiles', 'daily_executions', 'executions', 'chat_events']) {
    try {
      const res = await fetch(`${ENV.supabase.URL}/rest/v1/${table}?select=*&limit=1`, {
        headers: { apikey: ENV.supabase.SERVICE_ROLE_KEY, Authorization: `Bearer ${ENV.supabase.SERVICE_ROLE_KEY}` },
      });
      if (res.ok) ok(`table "${table}" exists`);
      else if (res.status === 404) bad(`table "${table}" is MISSING — run supabase/schema.sql`);
      else if (res.status === 401 || res.status === 403) bad(`table "${table}" denied (${res.status}) — bad key or RLS`);
      else warn(`table "${table}" returned HTTP ${res.status}`);
    } catch (e) {
      bad(`table "${table}": ${e.message}`);
    }
  }
}

/* =============================================================================
 *  2. UPSTREAM WEBSOCKET
 * ============================================================================= */
head('2. Upstream WebSocket (SignalR handshake)');
info(`url : ${ENV.upstream.WS}`);

await new Promise((resolve) => {
  let ws;
  let settled = false;
  const done = (fn) => {
    settled = true;
    try { ws?.terminate(); } catch { /* ignore */ }
    fn();
  };

  try {
    ws = new WebSocket(ENV.upstream.WS, { handshakeTimeout: 15_000 });
  } catch (e) {
    bad(`could not construct the socket: ${e.message}`);
    return resolve();
  }

  const timer = setTimeout(() => {
    bad('timed out after 20s — no handshake');
    info('→ a firewall or proxy is most likely blocking the WebSocket upgrade.');
    done(resolve);
  }, 20_000);

  ws.on('open', () => {
    ws.send(JSON.stringify({ protocol: 'json', version: 1 }) + RS);
    info('socket open, handshake sent');
  });

  ws.on('message', (raw) => {
    for (const frame of raw.toString().split(RS)) {
      if (!frame) continue;
      let msg;
      try { msg = JSON.parse(frame); } catch { continue; }

      if (msg.type === undefined) {
        if (msg.error) {
          bad(`handshake rejected: ${msg.error}`);
        } else {
          ok('SignalR handshake accepted');
          ws.send(JSON.stringify({ type: 1, invocationId: '1', target: 'Subscribe', arguments: [SYMBOLS] }) + RS);
          info(`Subscribe sent for ${SYMBOLS.join(', ')}`);
        }
        continue;
      }

      if (msg.type === 3) {
        if (msg.error) bad(`Subscribe rejected: ${msg.error}`);
        else ok('Subscribe confirmed — ticks should follow');
        clearTimeout(timer);
        return done(resolve);
      }

      if (msg.type === 1 && msg.target === 'ReceiveTick') {
        ok(`tick received: ${JSON.stringify(msg.arguments?.[0] ?? {}).slice(0, 90)}`);
        clearTimeout(timer);
        return done(resolve);
      }
    }
  });

  ws.on('error', (e) => {
    bad(`socket error: ${e.message}`);
    clearTimeout(timer);
    resolve();
  });

  // The socket is closed deliberately once a tick arrives, so a 1006 here is the
  // probe finishing its job rather than a failure — only early closes matter.
  ws.on('close', (code, reason) => {
    if (!settled && code !== 1000) bad(`socket closed unexpectedly (code=${code} ${reason?.toString() ?? ''})`);
    clearTimeout(timer);
    resolve();
  });
});

/* =============================================================================
 *  3. OHLC BACKFILL
 * ============================================================================= */
head('3. OHLC REST backfill');
info(`api : ${ENV.upstream.API}`);

const probeSymbol = SYMBOLS[0];
for (const tf of TIMEFRAMES) {
  const url = `${ENV.upstream.API}/api/${probeSymbol}/ohlc?interval=${encodeURIComponent(tf.api)}&limit=3`;
  try {
    const ctl = new AbortController();
    const timer = setTimeout(() => ctl.abort(), ENV.upstream.REST_TIMEOUT_MS);
    const res = await fetch(url, { signal: ctl.signal, headers: { accept: 'application/json' } }).finally(() =>
      clearTimeout(timer)
    );

    if (!res.ok) {
      bad(`${tf.label.padEnd(4)} HTTP ${res.status}`);
      continue;
    }
    const body = await res.json();
    const bars = Array.isArray(body?.bars) ? body.bars.length : 0;
    if (bars === 0) {
      warn(`${tf.label.padEnd(4)} 0 bars returned`);
    } else if (body.interval && body.interval !== tf.api) {
      warn(`${tf.label.padEnd(4)} ${bars} bars, but interval "${body.interval}" ≠ requested "${tf.api}"`);
    } else {
      ok(`${tf.label.padEnd(4)} ${bars} bars`);
    }
  } catch (e) {
    bad(`${tf.label.padEnd(4)} ${e.message}`);
  }
}

/* =============================================================================
 *  4. ENGINE CONFIG SANITY
 * ============================================================================= */
head('4. Engine configuration');
const s = ENV.strategy;
const rows = [
  ['entry channel', `${s.DONCHIAN_ENTRY}-day Donchian`],
  ['trailing channel', `${s.DONCHIAN_EXIT}-day Donchian`],
  ['ATR window', `ATR(${s.ATR_PERIOD}) on ${s.ATR_TIMEFRAME / 3600}h`],
  ['leg split', `${Math.round(s.LEG1_PCT * 100)}% at +${s.LEG1_ATR_MULT}×ATR, ${Math.round(s.LEG2_PCT * 100)}% runner`],
  ['broker mode', ENV.broker.MODE],
  ['execution', s.EXECUTION_ENABLED ? 'ENABLED' : 'DISABLED'],
  ['FREE quota', `${ENV.quota.FREE_DAILY_EXECUTIONS}/day`],
  ['PRO quota', ENV.quota.PRO_DAILY_EXECUTIONS === 0 ? 'unlimited' : `${ENV.quota.PRO_DAILY_EXECUTIONS}/day`],
  ['persistence', ENV.persistence.ENABLED ? 'enabled' : 'DISABLED'],
  ['auth required', String(ENV.auth.REQUIRED)],
  ['dev token', ENV.auth.DEV_TOKEN ? 'SET (never in production)' : 'not set'],
];
for (const [k, v] of rows) console.log(`  ${c.d}${k.padEnd(18)}${c.x} ${v}`);

const problems = [];
if (s.LEG1_PCT + s.LEG2_PCT > 1.0001) problems.push('leg percentages exceed 100%');
if (s.LEG1_PCT <= 0 || s.LEG2_PCT <= 0) problems.push('a leg percentage is zero');
if (s.DONCHIAN_EXIT >= s.DONCHIAN_ENTRY) problems.push('the trailing channel should be tighter than the entry channel');
if (ENV.broker.MODE === 'mt5' && (!ENV.broker.BASE_URL || !ENV.broker.TOKEN)) problems.push('BROKER_MODE=mt5 without BASE_URL/TOKEN');
if (ENV.market.MAX_CANDLES < s.DONCHIAN_ENTRY + 1) problems.push(`MAX_CANDLES (${ENV.market.MAX_CANDLES}) cannot hold a ${s.DONCHIAN_ENTRY}-day channel`);

head('Summary');
if (problems.length === 0) {
  console.log(`${c.g}No configuration problems found.${c.x}`);
} else {
  for (const p of problems) console.log(`${c.r}  - ${p}${c.x}`);
}
console.log('');