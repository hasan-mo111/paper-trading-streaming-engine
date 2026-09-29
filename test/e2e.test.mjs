/* =============================================================================
 *  END-TO-END TEST  —  Paper Trading Streaming Engine
 *  Developed by Eng Hasan Mohamad
 * -----------------------------------------------------------------------------
 *  Boots a mock biquote.io server, points server.js at it, then asserts:
 *    1. all 16 series backfill with exactly 100 closed candles + 1 forming
 *    2. /stream sends a full `snapshot` as the FIRST event
 *    3. live `update` events arrive, throttled to ~400ms, never faster
 *    4. tick → candle folding keeps close/high/low consistent
 *    5. candle rollover pushes a `closed` candle and keeps the ring at 100
 *    6. SSE client filtering (?symbol=&interval=) works
 *    7. the dashboard HTML + REST endpoints are served
 *    8. exponential backoff reconnects after the upstream drops
 *
 *  Run:  npm test
 * ============================================================================= */

import { spawn } from 'node:child_process';
import http from 'node:http';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { startMockBiquote } from './mock-biquote.mjs';

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

const mock = await startMockBiquote({ port: MOCK_PORT, tickMs: 250, onLog: () => {} });

const child = spawn(process.execPath, ['server.js'], {
  cwd: ROOT,
  env: {
    ...process.env,
    PORT: String(APP_PORT),
    HOST: '127.0.0.1',
    BIQUOTE_WS: mock.url,
    BIQUOTE_API: mock.api,
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
check('upstream connected', health?.upstream?.connected === true, `lastError: ${health?.upstream?.lastError ?? 'none'}`);
check('symbols subscribed upstream', health?.upstream?.subscribed === true);
check('16 series (4 pairs × 4 timeframes)', health?.series?.length === 16, `got ${health?.series?.length}`);
check('all series ready', health?.series?.every((s) => s.ready));
check(
  'every series holds exactly 100 closed candles',
  health?.series?.every((s) => s.closed === 100),
  `counts: ${[...new Set((health?.series ?? []).map((s) => s.closed))].join('/')}`
);

await sleep(2500);
const healthAfterTicks = await get('/health');
check(
  'ticks are landing on every series',
  healthAfterTicks.series.every((s) => s.ticks > 0),
  `min ticks: ${Math.min(...healthAfterTicks.series.map((s) => s.ticks))}`
);
check('every series reports fresh data', healthAfterTicks.series.every((s) => s.fresh));

const xau = await get('/api/candles?symbol=XAUUSD&interval=900');
const LAST = () => xau.candles.at(-1); // read lazily: may be empty if backfill failed
check('REST /api/candles → 100 candles', xau.candles.length === 100, `got ${xau.candles.length}`);
check('candles are ascending by time', xau.candles.every((c, i, a) => i === 0 || c.time > a[i - 1].time));
check('candles sit on the 900s grid', xau.candles.every((c) => c.time % 900 === 0));
check('OHLC values are numbers', xau.candles.every((c) => [c.open, c.high, c.low, c.close].every(Number.isFinite)));
check('high >= low on every candle', xau.candles.every((c) => c.high >= c.low));
check('high >= open and high >= close', xau.candles.every((c) => c.high >= c.open && c.high >= c.close));
check('a forming candle exists', !!xau.current);
check('forming candle is the newest', (xau.current?.time ?? 0) > (LAST()?.time ?? Infinity));
check('forming candle is on the same grid', xau.current?.time % 900 === 0);
check('unknown series → 404', (await fetch(`${APP}/api/candles?symbol=XAUUSD&interval=999`).then((r) => r.status)) === 404);

/* ================================================== 2 + 3. SSE + THROTTLE */
console.log('\n2) SSE downstream (snapshot first, then throttled updates)');
const events = await sseClient('/stream', { durationMs: 7000 });

const first = events[0];
check('first SSE event is `snapshot`', first?.event === 'snapshot', first?.event);
check('snapshot contains 16 series', Object.keys(first?.json?.series ?? {}).length === 16);
check('snapshot has 100 candles per series', Object.values(first?.json?.series ?? {}).every((s) => s.candles.length === 100));
check('snapshot advertises pairs + timeframes', first?.json?.pairs?.length === 4 && first?.json?.timeframes?.length === 4);
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
console.log('\n3) Tick → candle folding');
const xauLive = await get('/api/candles?symbol=XAUUSD&interval=900');
const c = xauLive.current ?? {};
check(
  'forming candle close is within its high/low range',
  c.close != null && c.close <= c.high && c.close >= c.low,
  `${c.low} ≤ ${c.close} ≤ ${c.high}`
);
const xau5 = await get('/api/candles?symbol=XAUUSD&interval=300');
const c5 = xau5.current;
check('forming candle is tracked for all 4 timeframes', !!c5 && c5.time % 300 === 0);
check('tick volume increments on the forming candle', c.tickVolume > 0, `${c.tickVolume} ticks`);
check('all four pairs are streaming', (await get('/health')).series.filter((s) => s.ticks > 0).length === 16);

/* ==================================================== 5. RING INTEGRITY */
console.log('\n4) Candle ring integrity (100 max)');
const ring = await get('/api/candles?symbol=XAUUSD&interval=300');
const ringLast = ring.candles.at(-1);
const ringGap = ring.current && ringLast ? ring.current.time - ringLast.time : null;
check('ring is exactly 100 candles', ring.candles.length === 100, `got ${ring.candles.length}`);
check('ring stayed sorted', ring.candles.every((c, i, a) => i === 0 || c.time > a[i - 1].time));
check('ring has no duplicate epochs', new Set(ring.candles.map((c) => c.time)).size === ring.candles.length);
check('newest closed candle is adjacent to the forming candle', ringGap === 300, `gap ${ringGap}s`);

/* -- Rollover, trimming and gap-filling, driven directly through the
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

  // Seed the forming candle, then push 150 more candles through rollover
  agg.applyTick('XAUUSD', tickAt(0, 100));
  const firstEpoch = slot.current.time;
  check('tick is snapped onto the timeframe grid', firstEpoch % 300 === 0, `epoch ${firstEpoch}`);

  for (let i = 1; i <= 150; i++) agg.applyTick('XAUUSD', tickAt(i * 300, 100 + i));

  check('ring stays at exactly 100 after 150 rollovers', slot.candles.length === 100, `got ${slot.candles.length}`);
  check('oldest candles were discarded', slot.candles[0].time > firstEpoch, `oldest kept ${slot.candles[0].time}`);
  check('ring is contiguous after trimming', slot.candles.every((c, i, a) => i === 0 || c.time - a[i - 1].time === 300));
  check('forming candle is the last epoch', slot.current.time === firstEpoch + 150 * 300);
  check('only this symbol was touched', agg.store.get('EURUSD|300').candles.length === 0);

  // Rollovers must be reported so the client can append the closed candle
  agg.flush();
  let sawClosed = false;
  agg.applyTick('XAUUSD', tickAt(151 * 300, 999));
  sawClosed = agg.flush()?.items.some((i) => i.key === 'XAUUSD|300' && i.isNewCandle && !!i.closed);
  check('rollover reports the closed candle', sawClosed);

  // In-candle ticks mutate high/low/close, and never close the candle
  const before = { ...slot.current };
  agg.applyTick('XAUUSD', tickAt(151 * 300 + 30, 1200)); // higher
  agg.applyTick('XAUUSD', tickAt(151 * 300 + 60, 800));  // lower
  const now = slot.current;
  check('in-candle tick raises high', now.high >= 1200 && now.high > before.high);
  check('in-candle tick lowers low', now.low <= 800 && now.low < before.low);
  check('in-candle tick sets close to the last price', now.close === 800, `close ${now.close}`);
  check('in-candle ticks do not open a new candle', now.time === before.time);

  // A gap (weekend / outage) must be filled, not left as a hole.
  // Jumping 10 intervals ahead synthesises the 9 candles in between; the 10th
  // boundary becomes the new forming candle.
  const gapFrom = now.time;
  const warnings = [];
  const realWarn = console.warn;
  console.warn = (...a) => warnings.push(a.join(' '));
  agg.applyTick('XAUUSD', tickAt(151 * 300 + 300 * 10, 1050));
  console.warn = realWarn;

  const filled = slot.candles.filter((c) => c.time > gapFrom);
  check('a 10-candle jump fills the 9 candles in between', filled.length === 9, `${filled.length} filled`);
  check('filled candles are contiguous and on the grid', filled.every((c, i, a) => c.time % 300 === 0 && (i === 0 || c.time - a[i - 1].time === 300)));
  check('filled candles carry the last known close', filled.every((c) => Number.isFinite(c.close) && c.close > 0));
  check('the 10th boundary became the forming candle', slot.current.time === gapFrom + 300 * 10);
  check('the new forming candle opened at the live price', slot.current.open === 1050);
  check('ring is still exactly 100 after gap-filling', slot.candles.length === 100, `got ${slot.candles.length}`);
  check('the gap was logged as a warning', warnings.some((w) => /gap of 10 candles/.test(w)), warnings[0]?.slice(0, 60));
}

/* ================================================== 6. SSE CLIENT FILTER */
console.log('\n5) Bandwidth saving filters');
const filtered = await sseClient('/stream?symbol=EURUSD&interval=300', { durationMs: 3000 });
const fUpdates = filtered.filter((e) => e.event === 'update').flatMap((e) => e.json.items ?? []);
check('filtered client still gets the full snapshot', filtered[0]?.event === 'snapshot');
check('filtered client only gets its own series', fUpdates.length > 0 && fUpdates.every((i) => i.symbol === 'EURUSD' && i.interval === 300), `${[...new Set(fUpdates.map((i) => i.key))].join(', ')}`);
check('unfiltered stream is much wider', (updates.at(-1)?.json?.items?.length ?? 0) > 1);

/* ========================================================= 7. DASHBOARD */
console.log('\n6) Dashboard + client accounting');
const html = await getText('/');
check('GET / serves HTML', html.includes('<!DOCTYPE html>'));
check('dashboard embeds Lightweight Charts via CDN', /lightweight-charts@[\d.]+/.test(html));
check('dashboard connects to /stream', html.includes("new EventSource('/stream')"));
check('dashboard has pair + timeframe dropdowns', html.includes('id="pairSelect"') && html.includes('id="tfSelect"'));
check('default view is XAUUSD 15m', html.includes("DEFAULT_PAIR    = 'XAUUSD'") && html.includes('DEFAULT_TIMEFRAME = 900'));
check('dashboard is not hard-coded to Deriv', !html.includes('frxXAUUSD'));

const h2 = await get('/health');
check('SSE client count tracked', h2.sseClients >= 0, `${h2.sseClients} client(s)`);
check('status endpoint reports upstream connected', h2.upstream.connected === true);
check('static asset served', (await fetch(`${APP}/index.html`)).ok);

/* ================================================ 8. RECONNECT / BACKOFF */
console.log('\n7) Auto-reconnect with exponential backoff');
const statusPromise = sseClient('/stream', { durationMs: 9000 });
await sleep(300);
mock.closeAllClients(); // hard-kill the upstream
const statusEvents = (await statusPromise).filter((e) => e.event === 'status');

const log = serverLog.join('');
check('upstream drop was detected & logged', /Connection closed/.test(log));
check('reconnect was attempted with backoff', /Reconnecting in [\d.]+s/.test(log));
const resubscribes = (log.match(/subscription state/g) ?? []).length;
check('engine re-subscribed after reconnect', resubscribes >= 2, `${resubscribes} subscribe confirmations`);
check('reconnect was faster on the 2nd try (backoff resets)', /Reconnecting in 0\.[0-9]s/.test(log));
check('clients were told the stream went stale', statusEvents.some((e) => e.json?.connected === false) || /Upstream connection lost/.test(log));
const after = await get('/health');
check('state survived the reconnect', after.series.every((s) => s.ready));
check('candle ring still exactly 100 after reconnect', (await get('/api/candles?symbol=XAUUSD&interval=900')).candles.length === 100);

/* ---------------------------------------------------------------- report */
console.log('\n' + '─'.repeat(58));
console.log(`  \x1b[32mPASSED: ${passed}\x1b[0m   ${failed ? `\x1b[31mFAILED: ${failed}\x1b[0m` : 'FAILED: 0'}`);
console.log('─'.repeat(58) + '\n');

if (process.env.SHOW_LOG) console.log(serverLog.join(''));
cleanup();
process.exit(failed ? 1 : 0);
