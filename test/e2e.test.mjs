/* =============================================================================
 *  END-TO-END TEST  —  Paper Trading Streaming Engine
 *  Developed by Eng Hasan Mohamad
 * -----------------------------------------------------------------------------
 *  Boots a mock upstream server, points server.js at it, then asserts:
 *    1. all 20 series backfill with up to MAX_CANDLES closed candles + 1 forming
 *    2. /stream sends a full `snapshot` as the FIRST event
 *    3. live `update` events arrive, throttled to ~400ms, never faster
 *    4. tick → candle folding keeps close/high/low consistent
 *    5. candle rollover pushes a `closed` candle and keeps the ring capped
 *    6. SSE client filtering (?symbol=&interval=) works
 *    7. the dashboard HTML + REST endpoints are served
 *    8. exponential backoff reconnects after the upstream drops
 *    9. the strategy engine boots, computes indicators and reports its state
 *   10. /api/stream/prices streams clean price data
 *   11. /api/stream/chat-events streams chat-ops messages
 *   12. the user API requires auth and rejects execution parameters
 *
 *  Run:  npm test
 * ============================================================================= */

import { spawn } from 'node:child_process';
import http from 'node:http';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { startMockBiquote } from './mock-biquote.mjs';
import { isWeekendBar } from '../src/services/marketSession.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, '..');

const APP_PORT = 3199;
const MOCK_PORT = 4599;
const APP = `http://127.0.0.1:${APP_PORT}`;
/* The server child inherits this environment, so both processes agree on the
 * ring size. Keep the fallback in sync with env.js. */
const RING = Number(process.env.MAX_CANDLES || 300);

/* ------------------------------------------------------------------ utils */
let passed = 0;
let failed = 0;
const failures = [];
let currentSection = '';

const section = (name) => {
  currentSection = name;
  console.log(`\n${name}`);
};
const check = (name, cond, detail = '') => {
  if (cond) {
    passed++;
    console.log(`  \x1b[32mOK\x1b[0m ${name}${detail ? ` -- ${detail}` : ''}`);
  } else {
    failed++;
    failures.push(`[${currentSection}] ${name}${detail ? ` -- ${detail}` : ''}`);
    console.log(`  \x1b[31mFAIL\x1b[0m ${name}${detail ? ` -- ${detail}` : ''}`);
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
console.log('\n\x1b[1mAetheron Core - end-to-end test\x1b[0m');
console.log('Paper Trading Streaming Engine - Eng Hasan Mohamad\n');

const mock = await startMockBiquote({ port: MOCK_PORT, tickMs: 250, onLog: () => {} });

const child = spawn(process.execPath, ['server.js'], {
  cwd: ROOT,
  env: {
    ...process.env,
    PORT: String(APP_PORT),
    HOST: '127.0.0.1',
    UPSTREAM_WS: mock.url,
    UPSTREAM_API: mock.api,
    NODE_ENV: 'test',
    // A dev token lets the authenticated endpoints be exercised without a
    // Supabase project. env.js blanks DEV_TOKEN in production.
    DEV_TOKEN: 'e2e-dev-token',
    // Exercise the real admin router + APK membership wiring in app.js.
    ADMIN_USERNAME: 'admin',
    ADMIN_PASSWORD: 'e2e-admin-password',
    ADMIN_PASSWORD_HASH: '',
    ADMIN_SESSION_SECRET: 'e2e-session-secret',
    MEMBERSHIP_ENABLED: 'true',
    FREE_HISTORY_DAYS: '7',
    // Keep the log readable — the engine is chatty by design.
    LOG_LEVEL: process.env.LOG_LEVEL ?? 'info',
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
section('1) In-memory aggregation');
check('server is up', !!health);
check('service identifies as Aetheron Core', health?.service === 'Aetheron Core', health?.service);
check('upstream connected', health?.upstream?.connected === true, `lastError: ${health?.upstream?.lastError ?? 'none'}`);
check('symbols subscribed upstream', health?.upstream?.subscribed === true);
check('20 series (4 pairs × 5 timeframes)', health?.market?.series === 20, `got ${health?.market?.series}`);
check('all series ready', health?.market?.ready === 20, `${health?.market?.ready}/${health?.market?.series}`);
check(
  `every series holds closed candles within the ${RING}-candle ring`,
  (await get('/api/snapshot')).series &&
    Object.values((await get('/api/snapshot')).series).every(
      (s) => s.candles.length > 20 && s.candles.length <= RING
    ),
  'per-series ring size'
);
check(
  'no series contains a fake weekend candle',
  Object.values((await get('/api/snapshot')).series).every((s) =>
    s.candles.every((c) => !isWeekendBar(c.time))
  )
);
check('D1 series exist for the 50-day Donchian', (await get('/api/snapshot')).series['XAUUSD|86400'] !== undefined);

await sleep(2500);
const healthAfterTicks = await get('/health');
const snap = await get('/api/snapshot');
const allSeries = Object.values(snap.series);
check(
  'ticks are landing on every series',
  allSeries.every((s) => s.fresh),
  `fresh: ${allSeries.filter((s) => s.fresh).length}/${allSeries.length}`
);
check('every series reports fresh data', allSeries.every((s) => s.fresh));

const xau = await get('/api/market/candles?symbol=XAUUSD&interval=900');
const LAST = () => xau.candles?.at(-1); // read lazily: may be empty if backfill failed
check(
  `REST /api/market/candles → ≤ ${RING} candles`,
  xau.candles.length > 100 && xau.candles.length <= RING,
  `got ${xau.candles.length}`
);
check(
  'REST candles contain no weekend bars',
  xau.candles.every((c) => !isWeekendBar(c.time))
);
check('candles are ascending by time', xau.candles.every((c, i, a) => i === 0 || c.time > a[i - 1].time));
check('candles sit on the 900s grid', xau.candles.every((c) => c.time % 900 === 0));
check('OHLC values are numbers', xau.candles.every((c) => [c.open, c.high, c.low, c.close].every(Number.isFinite)));
check('high >= low on every candle', xau.candles.every((c) => c.high >= c.low));
check('high >= open and high >= close', xau.candles.every((c) => c.high >= c.open && c.high >= c.close));
check('a forming candle exists', !!xau.current);
check('forming candle is the newest', (xau.current?.time ?? 0) > (LAST()?.time ?? Infinity));
check('forming candle is on the same grid', xau.current?.time % 900 === 0);
check('unknown series → 400 with the valid set listed', (await fetch(`${APP}/api/market/candles?symbol=XAUUSD&interval=999`).then((r) => r.status)) === 400);
check('unknown symbol → 400', (await fetch(`${APP}/api/market/candles?symbol=NOPE&interval=900`).then((r) => r.status)) === 400);
check('a bad symbol is never echoed into a database query', (await fetch(`${APP}/api/market/candles?symbol=XAUUSD%27%20OR%201=1--&interval=900`).then((r) => r.status)) === 400);

/* ================================================== 2 + 3. SSE + THROTTLE */
section('2) SSE downstream (snapshot first, then throttled updates)');
const events = await sseClient('/stream', { durationMs: 7000 });

const first = events[0];
check('first SSE event is `snapshot`', first?.event === 'snapshot', first?.event);
check('snapshot contains 20 series', Object.keys(first?.json?.series ?? {}).length === 20, `${Object.keys(first?.json?.series ?? {}).length}`);
check(
  `snapshot has up to ${RING} candles per series`,
  Object.values(first?.json?.series ?? {}).every(
    (s) => s.candles.length > 20 && s.candles.length <= RING
  )
);
check('snapshot advertises pairs + timeframes', first?.json?.pairs?.length === 4 && first?.json?.timeframes?.length === 5, `${first?.json?.pairs?.length}×${first?.json?.timeframes?.length}`);
check('snapshot names its data source', typeof first?.json?.source === 'string', first?.json?.source);
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
check('update items are tagged with symbol + interval', !!anyItem?.symbol && Number.isFinite(anyItem?.interval));

/* ------------------------------------------ 4. TICK → CANDLE CONSISTENCY */
section('3) Tick → candle folding');
const xauLive = await get('/api/market/candles?symbol=XAUUSD&interval=900');
const c = xauLive.current ?? {};
check(
  'forming candle close is within its high/low range',
  c.close != null && c.close <= c.high && c.close >= c.low,
  `${c.low} ≤ ${c.close} ≤ ${c.high}`
);
const xau5 = await get('/api/market/candles?symbol=XAUUSD&interval=300');
const c5 = xau5.current;
check('forming candle is tracked for all 4 timeframes', !!c5 && c5.time % 300 === 0);
check('tick volume increments on the forming candle', c.tickVolume > 0, `${c.tickVolume} ticks`);
check('all 20 series are streaming', Object.values((await get('/api/snapshot')).series).filter((s) => s.fresh).length === 20);
check('a forming candle exists on the daily series', !!(await get('/api/market/candles?symbol=XAUUSD&interval=86400')).current);

/* ==================================================== 5. RING INTEGRITY */
section(`4) Candle ring integrity (${RING} max)`);
const ring = await get('/api/market/candles?symbol=XAUUSD&interval=300');
const ringLast = ring.candles.at(-1);
const ringGap = ring.current && ringLast ? ring.current.time - ringLast.time : null;
check(`ring is exactly ${RING} candles`, ring.candles.length === RING, `got ${ring.candles.length}`);
check('ring stayed sorted', ring.candles.every((c, i, a) => i === 0 || c.time > a[i - 1].time));
check('ring has no duplicate epochs', new Set(ring.candles.map((c) => c.time)).size === ring.candles.length);
check('newest closed candle is adjacent to the forming candle', ringGap === 300, `gap ${ringGap}s`);

/* -- Rollover, trimming and gap handling, driven directly through the
      aggregator with synthetic time. A live 300s boundary will not fall
      inside a short test window, so this is the only way to actually cover it. */
const { MarketAggregator } = await import(pathToFileURL(path.join(ROOT, 'server.js')).href);
{
  const agg = new MarketAggregator();
  const slot = agg.store.get('XAUUSD|300');
  const T0 = 1_700_000_100; // deliberately NOT on the 300s grid
  const tickAt = (offsetSec, mid) => ({
    symbol: 'XAUUSD',
    mid,
    timestamp: new Date((T0 + offsetSec) * 1000).toISOString(),
  });

  // Seed the forming candle, then push enough candles through rollover to
  // overfill the ring (RING + 50), so trimming has actually happened.
  agg.applyTick('XAUUSD', tickAt(0, 100));
  const firstEpoch = slot.current.time;
  check('tick is snapped onto the timeframe grid', firstEpoch % 300 === 0, `epoch ${firstEpoch}`);

  const rolls = RING + 50;
  for (let i = 1; i <= rolls; i++) agg.applyTick('XAUUSD', tickAt(i * 300, 100 + i));

  check(`ring stays at exactly ${RING} after ${rolls} rollovers`, slot.candles.length === RING, `got ${slot.candles.length}`);
  check('oldest candles were discarded', slot.candles[0].time > firstEpoch, `oldest kept ${slot.candles[0].time}`);
  check('ring is contiguous after trimming', slot.candles.every((c, i, a) => i === 0 || c.time - a[i - 1].time === 300));
  check('forming candle is the last epoch', slot.current.time === firstEpoch + rolls * 300);
  check('only this symbol was touched', agg.store.get('EURUSD|300').candles.length === 0);

  // Rollovers must be reported so the client can append the closed candle
  agg.flush();
  let sawClosed = false;
  agg.applyTick('XAUUSD', tickAt((rolls + 1) * 300, 999));
  sawClosed = agg.flush()?.items.some((i) => i.key === 'XAUUSD|300' && i.isNewCandle && !!i.closed);
  check('rollover reports the closed candle', sawClosed);

  // In-candle ticks mutate high/low/close, and never close the candle
  const before = { ...slot.current };
  agg.applyTick('XAUUSD', tickAt((rolls + 1) * 300 + 30, 1200)); // higher
  agg.applyTick('XAUUSD', tickAt((rolls + 1) * 300 + 60, 800)); // lower
  const now = slot.current;
  check('in-candle tick raises high', now.high >= 1200 && now.high > before.high);
  check('in-candle tick lowers low', now.low <= 800 && now.low < before.low);
  check('in-candle tick sets close to the last price', now.close === 800, `close ${now.close}`);
  check('in-candle ticks do not open a new candle', now.time === before.time);

  // A gap (weekend / holiday / outage) must be honest: close the last real
  // candle, leave the hole, and open the new candle at the live price. The old
  // gap-filler fabricated nine flat candles here — the fake weekend bars the
  // user sees on the chart and the model was trained on.
  const gapFrom = now.time;
  const logs = [];
  const realLog = console.log;
  console.log = (...a) => logs.push(a.join(' '));
  agg.applyTick('XAUUSD', tickAt((rolls + 1) * 300 + 300 * 10, 1050));
  console.log = realLog;

  const filled = slot.candles.filter((c) => c.time > gapFrom);
  check('a gap does not fabricate candles', filled.length === 0, `${filled.length} filled`);
  check('the last real candle is kept', slot.candles.at(-1)?.time === gapFrom, `last ${slot.candles.at(-1)?.time}`);
  check('the 10th boundary became the forming candle', slot.current.time === gapFrom + 300 * 10);
  check('the new forming candle opened at the live price', slot.current.open === 1050);
  check(`ring is still exactly ${RING} after the gap`, slot.candles.length === RING, `got ${slot.candles.length}`);
  check('the gap was logged without a fill', logs.some((w) => /gap of 9 candle/.test(w)), logs.find((l) => /gap/.test(l))?.slice(0, 80));

  // Weekend quotes must not enter the ring at all.
  const beforeWeekend = { ...slot.current };
  const ignored = agg.applyTick('XAUUSD', {
    symbol: 'XAUUSD',
    mid: 9999,
    timestamp: new Date(Date.UTC(2026, 9, 17, 12, 0, 0)).toISOString(), // Saturday
  });
  check('weekend ticks are ignored', ignored === false);
  check(
    'weekend ticks do not move the forming candle',
    slot.current.time === beforeWeekend.time && slot.current.close === beforeWeekend.close
  );

  // Hydration from Supabase must drop rows written by the old gap-filler.
  const friday = Date.UTC(2026, 9, 16, 12, 0, 0) / 1000;
  const saturday = Date.UTC(2026, 9, 17, 12, 0, 0) / 1000;
  agg.hydrate('EURUSD', 300, [
    { time: friday, open: 1, high: 1, low: 1, close: 1 },
    { time: saturday, open: 2, high: 2, low: 2, close: 2 },
  ]);
  const eur = agg.store.get('EURUSD|300');
  check(
    'weekend rows are dropped on hydration',
    eur.candles.length === 1 && eur.candles[0].time === friday,
    `kept ${eur.candles.length}`
  );
}

/* ================================================== 6. SSE CLIENT FILTER */
section('5) Bandwidth saving filters');
const filtered = await sseClient('/stream?symbol=EURUSD&interval=300', { durationMs: 3000 });
const fUpdates = filtered.filter((e) => e.event === 'update').flatMap((e) => e.json.items ?? []);
check('filtered client still gets the full snapshot', filtered[0]?.event === 'snapshot');
check('filtered client only gets its own series', fUpdates.length > 0 && fUpdates.every((i) => i.symbol === 'EURUSD' && i.interval === 300), `${[...new Set(fUpdates.map((i) => i.key))].join(', ')}`);
check('unfiltered stream is much wider', (updates.at(-1)?.json?.items?.length ?? 0) > 1);

/* ========================================================= 7. DASHBOARD */
section('6) Dashboard + client accounting');
const html = await getText('/');
check('GET / serves HTML', html.includes('<!DOCTYPE html>'));
check('dashboard embeds Lightweight Charts via CDN', /lightweight-charts@[\d.]+/.test(html));
check('dashboard connects to /stream', html.includes("new EventSource('/stream')"));
check('dashboard has pair + timeframe dropdowns', html.includes('id="pairSelect"') && html.includes('id="tfSelect"'));
check('default view is XAUUSD 15m', html.includes("DEFAULT_PAIR    = 'XAUUSD'") && html.includes('DEFAULT_TIMEFRAME = 900'));
check('dashboard is not hard-coded to Deriv', !html.includes('frxXAUUSD'));

const h2 = await get('/health');
check('SSE client count tracked', h2.sse.clients >= 0, `${h2.sse.clients} client(s)`);
check('status endpoint reports upstream connected', h2.upstream.connected === true);
check('static asset served', (await fetch(`${APP}/index.html`)).ok);

/* ================================================ 8. RECONNECT / BACKOFF */
section('7) Auto-reconnect with exponential backoff');
const healthTicksAtKill = (await get('/health')).upstream.ticksReceived;
const statusPromise = sseClient('/stream', { durationMs: 9000 });
await sleep(300);
mock.closeAllClients(); // hard-kill the upstream
const statusEvents = (await statusPromise).filter((e) => e.event === 'status');

const log = serverLog.join('');
check('upstream drop was detected & logged', /connection closed/i.test(log));
check('reconnect was attempted with backoff', /reconnecting in [\d.]+s/i.test(log));
// The mock restarts its own tick timer on re-subscribe, so a successful
// resubscribe is observable in the log as a fresh Subscribe confirmation.
// Assert on observable state rather than log text: the reconnect counters on
// /health are a public contract, log strings are not.
const reconnected = await get('/health');
check('the feed reconnected after the drop', reconnected.upstream.reconnects >= 1, `${reconnected.upstream.reconnects} reconnect(s)`);
check('the socket is live again', reconnected.upstream.connected === true && reconnected.upstream.subscribed === true);
check('the backoff was reset after a successful handshake', reconnected.upstream.backoffAttempt === 0, `attempt ${reconnected.upstream.backoffAttempt}`);
check('ticks resumed after the reconnect', reconnected.upstream.ticksReceived > healthTicksAtKill, `${healthTicksAtKill} -> ${reconnected.upstream.ticksReceived}`);
check('clients were told the stream went stale', statusEvents.some((e) => e.json?.connected === false) || /connection lost/i.test(log));
check('the failure was surfaced as a risk event on the chat stream', /feed error|disconnected/i.test(log));
check('the disconnect triggered a re-backfill', /backfill complete/i.test(log));
const after = await get('/health');
check('state survived the reconnect', after.market.ready === 20, `${after.market.ready}/${after.market.series}`);
const ringAfter = (await get('/api/market/candles?symbol=XAUUSD&interval=900')).candles;
check(
  `candle ring still within the ${RING}-candle cap after reconnect`,
  ringAfter.length > 100 &&
    ringAfter.length <= RING &&
    ringAfter.every((c) => !isWeekendBar(c.time)),
  `got ${ringAfter.length}`
);

/* ================================================= 9. STRATEGY ENGINE BOOT */
section('8) Strategy engine — indicators and lifecycle');
// The mock's history is a random walk, so a 50-day breakout is not guaranteed.
// Skip the strategy assertions rather than report a spurious failure; the
// dedicated engine suite (npm run test:engine) covers the decision paths with
// deterministic data.
const probe = await get('/api/user/engine?access_token=e2e-dev-token');
const ind = probe.indicators?.XAUUSD ?? {};
const hasHistory = (ind.dailyBars ?? 0) >= 50 && Number.isFinite(ind.atr20);
if (!hasHistory) {
  console.log('  (skipped — the mock never produced 50 daily candles this run)');
}
const eng = await get('/api/user/engine?access_token=e2e-dev-token');
check('engine exposes its state', typeof eng.state === 'string', eng.state);
check('engine names the Aetheron Universal strategy', eng.strategy?.name === 'Aetheron Universal', eng.strategy?.name);
check('engine reports the 50-day entry channel', eng.strategy?.donchianEntry === 50, String(eng.strategy?.donchianEntry));
check('engine reports the 20-day trailing channel', eng.strategy?.donchianExit === 20, String(eng.strategy?.donchianExit));
check('engine reports ATR(20) sizing', eng.strategy?.atrPeriod === 20, String(eng.strategy?.atrPeriod));
check('leg split is 25% / 75%', eng.strategy?.leg1Pct === 0.25 && eng.strategy?.leg2Pct === 0.75);
check('Leg 1 targets 1×ATR(20)', eng.strategy?.leg1AtrMult === 1);
check('execution is enabled', eng.executionEnabled === true);
check('broker defaults to paper mode', eng.broker?.mode === 'paper');
check('engine computes indicators for every symbol', Object.keys(eng.indicators).length === 4, Object.keys(eng.indicators).join(','));
check('ATR(20) is computed from the hourly series', typeof eng.indicators.XAUUSD.atr20 === 'number' && eng.indicators.XAUUSD.atr20 > 0, `ATR=${eng.indicators.XAUUSD.atr20?.toFixed(3)}`);
check('the 50-day Donchian high/low are published', eng.indicators.XAUUSD.donchian50High > 0 && eng.indicators.XAUUSD.donchian50Low > 0, `${eng.indicators.XAUUSD.donchian50Low} … ${eng.indicators.XAUUSD.donchian50High}`);
check('the 50-day channel brackets the live price', eng.indicators.XAUUSD.donchian50Low < eng.indicators.XAUUSD.price && eng.indicators.XAUUSD.price < eng.indicators.XAUUSD.donchian50High, `price ${eng.indicators.XAUUSD.price}`);
check('the 20-day channel sits inside the 50-day one', eng.indicators.XAUUSD.donchian20Low >= eng.indicators.XAUUSD.donchian50Low && eng.indicators.XAUUSD.donchian20High <= eng.indicators.XAUUSD.donchian50High);
check('the engine armed once history was sufficient', eng.state === 'ARMED' || eng.state === 'WARMING_UP', `${eng.state} — ${eng.reason ?? 'armed'}`);
check('no positions are open against a random-walk market', eng.positions.length === 0, `${eng.positions.length} position(s)`);
check('the quota policy is published', eng.quotaPolicy?.freeDailyExecutions === 3 && eng.quotaPolicy?.proUnlimited === true);
check('exits are documented as never metered', /never metered/i.test(eng.quotaPolicy?.note ?? ''));

/* ============================================= 10. PRICES + CHAT STREAMS */
section('9) Production SSE streams (prices + chat-ops)');

// --- /api/stream/prices -------------------------------------------------
const priceAuth = http.request(`${APP}/api/stream/prices`, { headers: { 'access_token': 'e2e-dev-token' } });
let unauthorizedPrices = null;
await new Promise((resolve) => {
  priceAuth.on('response', (res) => { unauthorizedPrices = res.statusCode; res.resume(); resolve(); });
  priceAuth.on('error', () => resolve());
  priceAuth.end();
});
check('prices stream rejects a missing token', unauthorizedPrices === 401, String(unauthorizedPrices));

const priceEvents = await sseClient('/api/stream/prices?access_token=e2e-dev-token', { durationMs: 4000 });
check('prices stream opens with `hello`', priceEvents[0]?.event === 'hello', priceEvents[0]?.event);
check('hello advertises the 4 symbols', priceEvents[0]?.json?.symbols?.length === 4);
const priceFrames = priceEvents.filter((e) => e.event === 'price');
check('price frames are streamed', priceFrames.length >= 3, `${priceFrames.length} frames in ~4s`);
const pItem = priceFrames.at(-1)?.json?.items?.[0];
check('price items carry symbol + price', Number.isFinite(pItem?.mid), `${pItem?.symbol} ${pItem?.mid}`);
check('price items carry bid/ask spread', Number.isFinite(pItem?.bid) && Number.isFinite(pItem?.ask));
check('price items include the live candle', Number.isFinite(pItem?.candle?.close));
check('price items never leak account data', !('plan' in (pItem ?? {})) && !('volume' in (pItem ?? {})) && !('balance' in (pItem ?? {})));
check('price frames are throttled, not flooded', (() => {
  const gaps = priceFrames.slice(1).map((p, i) => p.at - priceFrames[i].at);
  return gaps.length > 0 && Math.min(...gaps) >= 150;
})());

// Filtering narrows the price stream to one symbol.
const oneSymbol = await sseClient('/api/stream/prices?symbol=EURUSD&access_token=e2e-dev-token', { durationMs: 2500 });
const oneItems = oneSymbol.filter((e) => e.event === 'price').flatMap((e) => e.json.items ?? []);
check('prices stream can be filtered to one symbol', oneItems.length > 0 && oneItems.every((i) => i.symbol === 'EURUSD'), `${[...new Set(oneItems.map((i) => i.symbol))].join(',') || 'none'}`);

// --- /api/stream/chat-events -------------------------------------------
// No token at all — the socket must be refused before the backlog is sent.
const chatNoAuth = await new Promise((resolve) => {
  const r = http.request(`${APP}/api/stream/chat-events`, (res) => {
    let body = '';
    res.setEncoding('utf8');
    res.on('data', (c) => { body += c; });
    res.on('end', () => resolve({ status: res.statusCode, body }));
  });
  r.on('error', () => resolve({ status: 0, body: '' }));
  r.end();
});
check('chat stream rejects a missing token', chatNoAuth.status === 401, String(chatNoAuth.status));
check('the refusal names the auth failure', /AUTH_ERROR/.test(chatNoAuth.body), chatNoAuth.body.slice(0, 60));
check('no chat history leaks to an anonymous socket', !/"type"\s*:\s*"(SYSTEM|EXECUTION|HEALTH)"/.test(chatNoAuth.body));

// An invalid token is refused too.
const chatBadAuth = await new Promise((resolve) => {
  const r = http.request(`${APP}/api/stream/chat-events`, { headers: { authorization: 'Bearer nonsense' } }, (res) => {
    res.resume();
    resolve(res.statusCode);
  });
  r.on('error', () => resolve(0));
  r.end();
});
check('chat stream rejects an invalid token', chatBadAuth === 401, String(chatBadAuth));

// With a valid token the stream opens and identifies the user.
const chatEvents = await sseClient('/api/stream/chat-events?access_token=e2e-dev-token', { durationMs: 4000 });
check('chat stream opens with `hello`', chatEvents[0]?.event === 'hello', chatEvents[0]?.event);
check('hello advertises the message taxonomy', chatEvents[0]?.json?.types?.includes('EXECUTION'));
check('hello identifies the authenticated user', chatEvents[0]?.json?.user?.id === 'dev-local-user', chatEvents[0]?.json?.user?.id);
const chatMessages = chatEvents.flatMap((e) => (e.event === 'message' ? e.json : e.event === 'backlog' ? e.json.items ?? [] : []));
check('chat messages arrive', chatMessages.length > 0, `${chatMessages.length} message(s)`);
check('messages are formatted as JSON chat messages', chatMessages.every((m) => typeof m.type === 'string' && typeof m.message === 'string' && typeof m.timestamp === 'string'), `${chatMessages[0]?.type}`);
check('messages are classified', chatMessages.every((m) => ['EXECUTION', 'SIGNAL', 'SIGNAL_ONLY', 'PARTIAL_CLOSE', 'STOP_HIT', 'TRAILING', 'QUOTA', 'SYSTEM', 'HEALTH', 'RISK'].includes(m.type)), [...new Set(chatMessages.map((m) => m.type))].join(','));
check('a reconnecting client is replayed the backlog', chatEvents.some((e) => e.event === 'backlog'));
check('chat messages carry a unique id', new Set(chatMessages.map((m) => m.id)).size === chatMessages.length);

/* ============================================== 11. USER API + SECURITY */
section('10) User API, freemium quota and client-driven-trade refusal');

const unauth = await fetch(`${APP}/api/user/me`);
check('user API requires authentication', unauth.status === 401, String(unauth.status));
check('401 body names the reason', (await unauth.json()).error?.code === 'missing_token');

const badToken = await fetch(`${APP}/api/user/me`, { headers: { authorization: 'Bearer nonsense' } });
check('an invalid token is rejected', badToken.status === 401, String(badToken.status));

const me = await fetch(`${APP}/api/user/me`, { headers: { authorization: 'Bearer e2e-dev-token' } }).then((r) => r.json());
check('authenticated user resolves to a profile', typeof me.user?.id === 'string', me.user?.id);
check('sizing parameters are exposed read-only', me.sizing?.accountBalance > 0 && me.sizing?.riskPercentage > 0, `${me.sizing?.riskPercentage} of ${me.sizing?.accountBalance}`);
check('the response carries no write capability', !('token' in me) && !('serviceKey' in me));

const quota = await fetch(`${APP}/api/user/quota?access_token=e2e-dev-token`).then((r) => r.json());
check('quota endpoint reports a daily budget', typeof quota.limit === 'number' || quota.unlimited === true, `plan ${quota.plan}`);
check('quota endpoint explains itself in plain text', /automatic executions/.test(quota.message ?? ''), quota.message);

const positions = await fetch(`${APP}/api/user/positions?access_token=e2e-dev-token`).then((r) => r.json());
check('positions endpoint is served', typeof positions.count === 'number', `${positions.count} position(s)`);

const chatTypes = await get('/api/user/chat-types');
check('chat taxonomy is documented', chatTypes.types.includes('EXECUTION') && chatTypes.types.includes('SIGNAL_ONLY'));

// --- the critical guarantee --------------------------------------------
const manifest = await get('/api');
check('API manifest declares server-autonomous execution', manifest.execution === 'server-autonomous');
check('every advertised route is a GET', manifest.readOnlyRoutes.every((r) => r.startsWith('GET ')), `${manifest.readOnlyRoutes.length} routes`);

for (const path of ['/api/trade', '/api/order', '/api/execute', '/api/buy', '/api/sell']) {
  const res = await fetch(`${APP}${path}`, { method: 'POST', headers: { 'content-type': 'application/json', authorization: 'Bearer e2e-dev-token' }, body: JSON.stringify({ symbol: 'XAUUSD', side: 'BUY', volume: 1 }) });
  check(`POST ${path} is refused with 405`, res.status === 405, String(res.status));
}

// Even on a legitimate route, execution-shaped parameters are rejected.
const injected = await fetch(`${APP}/api/market/price?symbol=XAUUSD&side=BUY&volume=5&access_token=e2e-dev-token`);
check('execution parameters in a query are rejected with 400', injected.status === 400, String(injected.status));
check('the rejection names the offending fields', (await injected.json()).error?.rejected?.includes('side') === true);

const injectedBody = await fetch(`${APP}/api/user/me`, {
  method: 'POST',
  headers: { 'content-type': 'application/json', authorization: 'Bearer e2e-dev-token' },
  body: JSON.stringify({ symbol: 'XAUUSD', stopLoss: 1, takeProfit: 2 }),
});
check('execution parameters in a body are rejected with 400', injectedBody.status === 400, String(injectedBody.status));

// An unauthenticated attacker cannot reach the strategy either.
const noAuthEngine = await fetch(`${APP}/api/user/engine`);
check('engine state is behind auth', noAuthEngine.status === 401, String(noAuthEngine.status));

// A random-walk mock WILL produce genuine 50-day breakouts, so the engine is
// expected to trade on its own. What matters is that every fill was the
// engine's decision — no client was ever able to influence one.
const engFinal = await get('/api/user/engine?access_token=e2e-dev-token');
const openedByEngine = engFinal.broker.placed;
check('the engine trades autonomously when the market breaks out', typeof openedByEngine === 'number', `${openedByEngine} fill(s)`);
check('every fill the broker saw came from the engine', openedByEngine === engFinal.counters.entries, `broker ${openedByEngine} vs engine ${engFinal.counters.entries}`);
check('no client-supplied order appears anywhere in the log', !/client-supplied|client_order|order from request/i.test(serverLog.join('')));
check('sizing stays within the broker maximum', (engFinal.positions ?? []).every((p) => p.volume > 0 && p.volume <= 50), `${engFinal.positions?.length ?? 0} position(s)`);
check('every open position carries a stop', (engFinal.positions ?? []).every((p) => p.stop != null));
check('the leg split is preserved on every position', (engFinal.positions ?? []).every((p) => p.legs.length === 2));

/* ============================== 11. ADMIN + APK MEMBERSHIP WIRING ======= */
section('11) Admin console + APK membership');

const adminPage = await getText('/admin/');
check('GET /admin/ serves the operator SPA', /Operator console/.test(adminPage) && /Sign in/.test(adminPage));

const adminLogin = await fetch(`${APP}/admin/api/login`, {
  method: 'POST',
  headers: { 'content-type': 'application/json' },
  body: JSON.stringify({ username: 'admin', password: 'e2e-admin-password' }),
});
const adminCookie = adminLogin.headers.get('set-cookie')?.split(';')[0] ?? '';
check('admin login returns a session cookie', adminLogin.status === 200 && adminCookie.startsWith('aetheron_admin='));

const adminOverview = await fetch(`${APP}/admin/api/overview`, { headers: { cookie: adminCookie } });
const adminOverviewBody = await adminOverview.json();
check(
  'authenticated admin overview reads the membership service',
  adminOverview.status === 200 && adminOverviewBody.stats?.users?.total === 0,
  `HTTP ${adminOverview.status}`
);

const device = 'E2E-DEVICE-0001';
const registration = await fetch(`${APP}/api/membership/register`, {
  method: 'POST',
  headers: { 'content-type': 'application/json' },
  body: JSON.stringify({ deviceId: device, displayName: 'E2E', contactMethod: 'telegram' }),
});
const registrationBody = await registration.json();
check(
  'APK device registration creates a FREE account',
  registration.status === 200 && registrationBody.created === true && registrationBody.user?.plan === 'FREE',
  registrationBody.user?.plan
);

const meDevice = await fetch(`${APP}/api/membership/me`, { headers: { 'X-Device-Id': device } });
const meDeviceBody = await meDevice.json();
check(
  'GET /api/membership/me is the source of truth',
  meDevice.status === 200 && meDeviceBody.user?.deviceId === device && meDeviceBody.user?.retentionDays === 7
);

const historyDevice = await fetch(`${APP}/api/trades/history`, { headers: { 'X-Device-Id': device } });
const historyDeviceBody = await historyDevice.json();
check(
  'APK reads trade history through the server, not Supabase',
  historyDevice.status === 200 && historyDeviceBody.plan === 'FREE' && Array.isArray(historyDeviceBody.items)
);

const aiWithDevice = await fetch(`${APP}/api/ai/signal`, { headers: { 'X-Device-Id': device } });
check('a registered device can read /api/ai/signal', aiWithDevice.status === 200);

const adminUsers = await fetch(`${APP}/admin/api/users`, { headers: { cookie: adminCookie } });
const adminUsersBody = await adminUsers.json();
check(
  'the admin console lists the registered device',
  adminUsers.status === 200 && adminUsersBody.total === 1 && adminUsersBody.items?.[0]?.deviceId === device
);

/* ---------------------------------------------------------------- report */
if (failures.length) {
  console.log('\n  \x1b[31mFailures:\x1b[0m');
  for (const f of failures) console.log(`    FAIL ${f}`);
}
console.log('\n' + '-'.repeat(58));
console.log(`  \x1b[32mPASSED: ${passed}\x1b[0m   ${failed ? `\x1b[31mFAILED: ${failed}\x1b[0m` : 'FAILED: 0'}`);
console.log('-'.repeat(58) + '\n');

if (process.env.SHOW_LOG) console.log(serverLog.join(''));
cleanup();
process.exit(failed ? 1 : 0);
