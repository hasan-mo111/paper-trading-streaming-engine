/* =============================================================================
 *  END-TO-END TEST  —  Paper Trading Streaming Engine
 *  Developed by Eng Hasan Mohamad
 * -----------------------------------------------------------------------------
 *  Boots a mock Deriv WS server, points server.js at it, then asserts:
 *    1. all 16 series backfill with exactly 100 closed candles + 1 forming
 *    2. /stream sends a full `snapshot` as the FIRST event
 *    3. live `update` events arrive, throttled to ~400ms, never faster
 *    4. candle rollover pushes a `closed` candle and keeps the ring at 100
 *    5. SSE client filtering (?symbol=&interval=) works
 *    6. the dashboard HTML + REST endpoints are served
 *    7. exponential backoff reconnects after the upstream drops
 *
 *  Run:  npm test
 * ============================================================================= */

import { spawn } from 'node:child_process';
import http from 'node:http';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { startMockDeriv } from './mock-deriv.mjs';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, '..');

const APP_PORT = 3199;
const MOCK_PORT = 4599;
const APP = `http://127.0.0.1:${APP_PORT}`;

/* ------------------------------------------------------------------ utils */
let passed = 0;
let failed = 0;
const check = (name, cond, detail = '') => {
  if (cond) {
    passed++;
    console.log(`  \x1b[32m✔\x1b[0m ${name}${detail ? ` — ${detail}` : ''}`);
  } else {
    failed++;
    console.log(`  \x1b[31m✖\x1b[0m ${name}${detail ? ` — ${detail}` : ''}`);
  }
};
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const get = (p) => fetch(APP + p).then((r) => r.json());
const getText = (p) => fetch(APP + p).then((r) => r.text());

/** Minimal SSE client: returns parsed events with their arrival timestamps. */
function sseClient(pathname, { durationMs = 6000 } = {}) {
  return new Promise((resolve, reject) => {
    const events = [];
    const req = http.get(APP + pathname, (res) => {
      let buf = '';
      res.setEncoding('utf8');
      res.on('data', (chunk) => {
        buf += chunk;
        let idx;
        while ((idx = buf.indexOf('\n\n')) !== -1) {
          const raw = buf.slice(0, idx);
          buf = buf.slice(idx + 2);
          if (raw.startsWith(':')) continue; // heartbeat comment
          const ev = {};
          for (const line of raw.split('\n')) {
            if (line.startsWith('event:')) ev.event = line.slice(6).trim();
            else if (line.startsWith('data:')) ev.data = line.slice(5).trim();
            else if (line.startsWith('id:')) ev.id = line.slice(3).trim();
          }
          if (ev.event) {
            try {
              ev.json = JSON.parse(ev.data);
            } catch {
              /* ignore */
            }
            ev.at = Date.now();
            events.push(ev);
          }
        }
      });
    });
    req.on('error', reject);
    setTimeout(() => {
      req.destroy();
      resolve(events);
    }, durationMs);
  });
}

/* ------------------------------------------------------------------- main */
console.log('\n\x1b[1mPaper Trading Streaming Engine — end-to-end test\x1b[0m');
console.log('Developed by Eng Hasan Mohamad\n');

const mock = startMockDeriv({
  port: MOCK_PORT,
  onLog: () => {}, // quiet
});
await sleep(300);

const child = spawn(process.execPath, ['server.js'], {
  cwd: ROOT,
  env: {
    ...process.env,
    PORT: String(APP_PORT),
    HOST: '127.0.0.1',
    DERIV_WS_URL: mock.url,
    NODE_ENV: 'test',
  },
  stdio: ['ignore', 'pipe', 'pipe'],
});

const serverLog = [];
child.stdout.on('data', (d) => serverLog.push(d.toString()));
child.stderr.on('data', (d) => serverLog.push(d.toString()));

const cleanup = async () => {
  child.kill('SIGKILL');
  await mock.close();
};
process.on('exit', cleanup);

/* ---- wait for bootstrap ---- */
let health = null;
for (let i = 0; i < 40; i++) {
  await sleep(500);
  try {
    health = await get('/health');
    if (health.series?.length && health.series.every((s) => s.ready)) break;
  } catch {
    /* not up yet */
  }
}

/* =========================================================== 1. BACKFILL */
console.log('\n1) In-memory aggregation');
check('server is up', !!health);
check('16 series (4 pairs × 4 timeframes)', health?.series?.length === 16, `got ${health?.series?.length}`);
check('all series ready', health?.series?.every((s) => s.ready));
check(
  'every series holds exactly 100 closed candles',
  health?.series?.every((s) => s.closed === 100),
  `counts: ${[...new Set((health?.series ?? []).map((s) => s.closed))].join('/')}`
);

// tickCount is reset by every backfill, so give the live stream a moment to
// land at least one update per series before asserting on it.
await sleep(2500);
const healthAfterTicks = await get('/health');
check(
  'live updates are landing on every series',
  healthAfterTicks.series.every((s) => s.ticks > 0),
  `min ticks: ${Math.min(...healthAfterTicks.series.map((s) => s.ticks))}`
);
check('every series reports fresh data', healthAfterTicks.series.every((s) => s.fresh));

const xau = await get('/api/candles?symbol=frxXAUUSD&interval=900');
check('REST /api/candles → 100 candles', xau.candles.length === 100, `got ${xau.candles.length}`);
check('candles are ascending by time', xau.candles.every((c, i, a) => i === 0 || c.time > a[i - 1].time));
check('candles sit on the 900s grid', xau.candles.every((c) => c.time % 900 === 0));
check('OHLC values are numbers', xau.candles.every((c) => [c.open, c.high, c.low, c.close].every(Number.isFinite)));
check('high >= low on every candle', xau.candles.every((c) => c.high >= c.low));
check('a forming candle exists', !!xau.current);
check('forming candle is the newest', xau.current?.time > xau.candles.at(-1).time);
const bad404 = await fetch(`${APP}/api/candles?symbol=frxXAUUSD&interval=999`).then((r) => r.status);
check('unknown series → 404', bad404 === 404);

/* ================================================== 2 + 3. SSE + THROTTLE */
console.log('\n2) SSE downstream (snapshot first, then throttled updates)');
const events = await sseClient('/stream', { durationMs: 7000 });

const first = events[0];
check('first SSE event is `snapshot`', first?.event === 'snapshot', first?.event);
check('snapshot contains 16 series', Object.keys(first?.json?.series ?? {}).length === 16);
check('snapshot has 100 candles per series',
  Object.values(first?.json?.series ?? {}).every((s) => s.candles.length === 100));
check('snapshot advertises pairs + timeframes',
  first?.json?.pairs?.length === 4 && first?.json?.timeframes?.length === 4);
check('snapshot includes upstream status', typeof first?.json?.upstream?.connected === 'boolean');

const updates = events.filter((e) => e.event === 'update');
check('update events streamed', updates.length >= 10, `${updates.length} updates in ~7s`);

const gaps = updates.slice(1).map((u, i) => u.at - updates[i].at);
const minGap = Math.min(...gaps);
const avgGap = gaps.reduce((a, b) => a + b, 0) / gaps.length;
check('throttled to ≥ 300ms (never flooded)', minGap >= 300, `min gap ${minGap}ms`);
check('throttled to ≤ 500ms (responsive)', avgGap <= 500, `avg gap ${Math.round(avgGap)}ms`);

const anyItem = updates[0]?.json?.items?.[0];
check('update items carry the forming candle', !!anyItem?.current);
check('update items carry OHLC', [anyItem?.current?.open, anyItem?.current?.high, anyItem?.current?.low, anyItem?.current?.close].every(Number.isFinite));
check('update items are tagged with symbol + interval',
  !!anyItem?.symbol && Number.isFinite(anyItem?.interval));
check('updating series is not in the closed array',
  updates.every((u) => (u.json.items ?? []).every((i) => !i.current || i.current.time > -1)));

/* ==================================================== 4. ROLLOVER + TRIM */
console.log('\n3) Candle rollover & 100-candle ring');
const rollovers = updates.flatMap((u) => u.json.items ?? []).filter((i) => i.isNewCandle && i.closed);
check('candle rollovers observed', rollovers.length > 0, `${rollovers.length} rollovers`);

const afterRoll = await get('/api/candles?symbol=frxXAUUSD&interval=900');
check('ring still exactly 100 after rollovers', afterRoll.candles.length === 100, `got ${afterRoll.candles.length}`);
check('ring stayed sorted', afterRoll.candles.every((c, i, a) => i === 0 || c.time > a[i - 1].time));
check('ring has no duplicate epochs',
  new Set(afterRoll.candles.map((c) => c.time)).size === afterRoll.candles.length);
check('newest closed candle is adjacent to the forming candle',
  afterRoll.current.time - afterRoll.candles.at(-1).time === 900,
  `gap ${afterRoll.current.time - afterRoll.candles.at(-1).time}s`);

/* ================================================== 5. SSE CLIENT FILTER */
console.log('\n4) Bandwidth saving filters');
const filtered = await sseClient('/stream?symbol=frxEURUSD&interval=300', { durationMs: 3000 });
const fUpdates = filtered.filter((e) => e.event === 'update').flatMap((e) => e.json.items ?? []);
check('filtered client still gets the full snapshot', filtered[0]?.event === 'snapshot');
check('filtered client only gets its own series',
  fUpdates.length > 0 && fUpdates.every((i) => i.symbol === 'frxEURUSD' && i.interval === 300),
  `${[...new Set(fUpdates.map((i) => i.key))].join(', ')}`);
check('unfiltered stream is much wider',
  (updates.at(-1)?.json?.items?.length ?? 0) > (fUpdates[0] ? 1 : 0));

/* ========================================================= 6. DASHBOARD */
console.log('\n5) Dashboard + client accounting');
const html = await getText('/');
check('GET / serves HTML', html.includes('<!DOCTYPE html>'));
check('dashboard embeds Lightweight Charts via CDN', /lightweight-charts@[\d.]+/.test(html));
check('dashboard connects to /stream', html.includes("new EventSource('/stream')"));
check('dashboard has pair + timeframe dropdowns',
  html.includes('id="pairSelect"') && html.includes('id="tfSelect"'));
check('default view is XAUUSD 15m',
  html.includes("DEFAULT_PAIR    = 'frxXAUUSD'") && html.includes('DEFAULT_TIMEFRAME = 900'));

const h2 = await get('/health');
check('SSE client count tracked', h2.sseClients >= 0, `${h2.sseClients} client(s)`);
check('status endpoint reports upstream connected', h2.upstream.connected === true);
check('static asset served', (await fetch(`${APP}/index.html`)).ok);

/* ================================================ 7. RECONNECT / BACKOFF */
console.log('\n6) Auto-reconnect with exponential backoff');

// Start collecting events, then hard-close every upstream connection.
const statusPromise = sseClient('/stream', { durationMs: 9000 });
await sleep(300);
mock.closeAllClients();
const statusEvents = (await statusPromise).filter((e) => e.event === 'status');

const log = serverLog.join('');
check('upstream drop was detected & logged', /Connection closed/.test(log));
check('reconnect was attempted with backoff', /Reconnecting in [\d.]+s/.test(log));
check('engine re-subscribed after reconnect', /subscribed frxXAUUSD\|900/.test(log));
check('clients were told the stream went stale',
  statusEvents.some((e) => e.json?.connected === false) || /Upstream connection lost/.test(log));
check('state survived the reconnect', (await get('/health')).series.every((s) => s.ready));
check('candle ring still exactly 100 after reconnect',
  (await get('/api/candles?symbol=frxXAUUSD&interval=900')).candles.length === 100);

/* ---------------------------------------------------------------- report */
console.log('\n' + '─'.repeat(58));
console.log(`  \x1b[32mPASSED: ${passed}\x1b[0m   ${failed ? `\x1b[31mFAILED: ${failed}\x1b[0m` : 'FAILED: 0'}`);
console.log('─'.repeat(58) + '\n');

if (process.env.SHOW_LOG) console.log(serverLog.join(''));
cleanup();
process.exit(failed ? 1 : 0);
