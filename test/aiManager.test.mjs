/* =============================================================================
 *  AETHERON CORE  —  test/aiManager.test.mjs
 *  -----------------------------------------------------------------------------
 *  Unit tests for the multi-symbol manager. The per-symbol analyst is already
 *  covered by aiSignal.test.mjs; what is tested HERE is the three things the
 *  manager adds on top of it:
 *
 *    1. ONE CLOCK, MANY SYMBOLS — a cycle walks every configured instrument,
 *       each with its own trade book, and unknown symbols are dropped.
 *    2. QUOTA BACKOFF — a 429 abandons the rest of the cycle, multiplies the
 *       interval, holds a cooldown, and decays back after a good cycle.
 *    3. CHEAP CYCLES WHILE A TRADE IS OPEN — the model is only consulted every
 *       MANAGEMENT_EVERY cycles, the rest are free tick-level statuses.
 *
 *  The feed is plaintext: every frame and every status payload must carry the
 *  exact levels a client acts on, because the trade rides the same public SSE
 *  connection as the candles.
 *
 *  Run:  node test/aiManager.test.mjs
 * ============================================================================= */

import assert from 'node:assert/strict';

import { AiAnalystManager } from '../src/services/aiAnalystManager.js';
import { CHAT_TYPES } from '../src/services/sseDispatcher.js';

let passed = 0;
let failed = 0;
async function test(name, fn) {
  try {
    await fn();
    passed += 1;
    console.log(`  ✓ ${name}`);
  } catch (e) {
    failed += 1;
    console.error(`  ✗ ${name}\n      ${e.message}`);
  }
}

/* ------------------------------------------------------------ test harness */

const PRICES = { XAUUSD: 2400, EURUSD: 1.08, USDJPY: 150, GBPUSD: 1.27 };
const PAIRS = [
  ['XAU/USD', 'XAUUSD'],
  ['EUR/USD', 'EURUSD'],
  ['USD/JPY', 'USDJPY'],
  ['GBP/USD', 'GBPUSD'],
];

function baseCfg(over = {}) {
  return {
    ENABLED: true,
    OPENROUTER_API_KEY: 'sk-test',
    MODEL: 'openrouter/free',
    SYMBOL: 'XAUUSD',
    SYMBOLS: ['XAUUSD', 'EURUSD', 'USDJPY', 'GBPUSD'],
    STAGGER_MS: 0,
    RATE_LIMIT_FACTOR_MAX: 4,
    RATE_LIMIT_COOLDOWN_MS: 600_000,
    MANAGEMENT_EVERY: 2,
    INTERVAL_MS: 1_800_000,
    MANUAL_COOLDOWN_S: 0,
    BOOT_DELAY_MS: 120_000,
    TIMEOUT_MS: 5_000,
    MAX_TOKENS: 2000,
    MIN_RR: 2,
    MAX_ENTRY_DRIFT: 0.02,
    BARS: { D1: 60, H4: 60, H1: 80, M15: 60, M5: 60 },
    TELEGRAM: false,
    TELEGRAM_CHAT_ID: '',
    MAX_ATTEMPTS: 2,
    FALLBACK_MODELS: [],
    CATCHUP_ENABLED: false,
    CATCHUP_DELAY_MS: 180_000,
    CATCHUP_MAX: 3,
    TRACK_ENABLED: true,
    STATUS_EVERY_CYCLE: true,
    ...over,
  };
}

/** A market stub that serves the same candle shape for every symbol. */
function marketStub() {
  const bars = (price, seconds) =>
    Array.from({ length: 80 }, (_, i) => ({
      time: 1_700_000_000 + i * seconds,
      open: price,
      high: price * 1.001,
      low: price * 0.999,
      close: price,
      tickVolume: 10,
    }));

  return {
    closedCandles: (symbol, seconds) => bars(PRICES[symbol] ?? 2400, seconds),
    formingCandle: (symbol) => {
      const p = PRICES[symbol] ?? 2400;
      return { time: 1_800_000_000, open: p, high: p, low: p, close: p };
    },
    priceOf: (symbol) => PRICES[symbol] ?? 2400,
    summary: () => ({ symbols: Object.keys(PRICES) }),
  };
}

function fakeDispatcher() {
  const pushed = [];
  return { pushed, push: (m) => pushed.push(m) };
}

/** Read the instrument the prompt was built for — user turn only, because the
 *  system prompt mentions XAU/USD as its worked example. */
function symbolFromBody(body) {
  const user = [...(body.messages ?? [])].reverse().find((m) => m.role === 'user');
  const text = String(user?.content ?? '');
  for (const [pair, code] of PAIRS) if (text.includes(pair)) return code;
  return 'XAUUSD';
}

/**
 * A model stub. `mode` is read at call time so a test can flip the tier from
 * "quota exhausted" to "healthy" without rebuilding the manager.
 */
function modelFetch(mode = () => 'ok') {
  const calls = [];
  const fn = async (url, opts) => {
    const body = JSON.parse(opts.body);
    calls.push({ url, body });

    if (mode() === 'limit') {
      return {
        ok: false,
        status: 429,
        text: async () =>
          JSON.stringify({ error: { message: 'Rate limit exceeded: free-models-per-day' } }),
      };
    }

    const symbol = symbolFromBody(body);
    const price = PRICES[symbol] ?? 2400;
    const content = JSON.stringify({
      action: 'BUY',
      symbol,
      entry_price: Number(price.toFixed(5)),
      stop_loss: Number((price * 0.995).toFixed(5)),
      take_profit_1: Number((price * 1.0075).toFixed(5)),
      take_profit_2: Number((price * 1.0125).toFixed(5)),
      confidence_score: 'High',
      reasoning: 'H4 break of structure, entry on the pullback.',
    });

    return {
      ok: true,
      status: 200,
      text: async () =>
        JSON.stringify({ choices: [{ finish_reason: 'stop', message: { content } }] }),
    };
  };
  fn.calls = calls;
  return fn;
}

const makeManager = (cfg, { fetchFn = modelFetch(), dispatcher = fakeDispatcher(), market = marketStub() } = {}) =>
  new AiAnalystManager({ market, dispatcher, env: { ai: cfg }, fetchFn });

console.log('\nAI analyst manager — multi-symbol + quota unit tests\n');

/* ---- 1. one clock, many symbols ------------------------------------------ */

await test('one cycle walks every configured symbol and books a trade in each', async () => {
  const cfg = baseCfg();
  const dispatcher = fakeDispatcher();
  const fetchFn = modelFetch();
  const mgr = makeManager(cfg, { fetchFn, dispatcher });

  const res = await mgr.run('manual');
  assert.equal(res.ok, true);
  assert.equal(res.results.length, 4, 'one result per symbol');
  assert.equal(fetchFn.calls.length, 4, 'exactly one model call per symbol');
  assert.deepEqual(res.results.map((r) => r.symbol), ['XAUUSD', 'EURUSD', 'USDJPY', 'GBPUSD']);
  assert.ok(res.results.every((r) => r.ok), 'every symbol should have produced a signal');

  const status = mgr.status();
  assert.equal(status.multiSymbol, true);
  assert.deepEqual(status.symbols, ['XAUUSD', 'EURUSD', 'USDJPY', 'GBPUSD']);
  assert.deepEqual(status.openTrades, ['XAUUSD', 'EURUSD', 'USDJPY', 'GBPUSD']);
  assert.equal(status.perSymbol.XAUUSD.position.action, 'BUY');
  assert.equal(status.perSymbol.USDJPY.position.action, 'BUY');

  // Four separate trades, four separate Telegram cards, one chat message each.
  const signals = dispatcher.pushed.filter((m) => m.type === CHAT_TYPES.AI_SIGNAL);
  assert.equal(signals.length, 4);
  assert.deepEqual(
    signals.map((m) => m.meta.symbol),
    ['XAUUSD', 'EURUSD', 'USDJPY', 'GBPUSD']
  );
  assert.equal(status.opened, 4);
});

await test('unknown or duplicated symbols are dropped instead of failing cycles', async () => {
  const mgr = makeManager(baseCfg({ SYMBOLS: ['xauusd', 'NOPE', 'XAUUSD', 'EURUSD'] }));
  assert.deepEqual(mgr.status().symbols, ['XAUUSD', 'EURUSD']);
});

await test('a disabled analyst refuses to run its cycle', async () => {
  const mgr = makeManager(baseCfg({ ENABLED: false }));
  const res = await mgr.run('manual');
  assert.equal(res.ok, false);
  assert.equal(res.error, 'disabled');
});

await test('a tick only reaches the analyst that owns the symbol', async () => {
  const mgr = makeManager(baseCfg());
  await mgr.run('manual');
  const gold = mgr.serviceFor('XAUUSD');

  // Drive the gold trade into its stop: 2380 is below the 0.5% stop (2388).
  mgr.onPrice('XAUUSD', 2380);
  mgr.onPrice('EURUSD', 1.08); // no-op, far from any level

  assert.equal(gold.position, null, 'gold should have been stopped out');
  assert.ok(mgr.serviceFor('EURUSD').position, 'eur/usd must be untouched');
  assert.equal(mgr.status().openTrades.length, 3);
});

/* ---- 2. quota backoff ---------------------------------------------------- */

await test('a quota rejection abandons the cycle, stretches the interval and cools down', async () => {
  const cfg = baseCfg();
  const fetchFn = modelFetch(() => 'limit');
  const mgr = makeManager(cfg, { fetchFn });

  const res = await mgr.run('schedule');
  assert.equal(res.ok, false);
  assert.equal(res.results.length, 1, 'the remaining symbols must not be tried');
  assert.equal(res.results[0].error, 'rate-limited');
  assert.equal(fetchFn.calls.length, 1, 'a spent quota must not be retried');
  assert.equal(res.factor, 2, 'interval must double');
  assert.equal(res.intervalMs, cfg.INTERVAL_MS * 2);
  assert.equal(mgr.cooldownRemainingMs() > 0, true);

  const status = mgr.status();
  assert.equal(status.rateLimitFactor, 2);
  assert.equal(status.rateLimits, 1);
  assert.ok(status.cooldownUntil, 'cooldownUntil should be published');
});

await test('a cooldown blocks scheduled cycles but not an explicit manual ask', async () => {
  const fetchFn = modelFetch(() => 'limit');
  const mgr = makeManager(baseCfg(), { fetchFn });

  await mgr.run('schedule');
  const calls = fetchFn.calls.length;

  const blocked = await mgr.run('schedule');
  assert.equal(blocked.error, 'rate-limited');
  assert.equal(fetchFn.calls.length, calls, 'a cooling analyst must not call the model');

  await mgr.run('manual'); // operator override
  assert.equal(fetchFn.calls.length, calls + 1);
});

await test('the interval grows per rejection up to the cap, then decays after a good cycle', async () => {
  let mode = 'limit';
  const fetchFn = modelFetch(() => mode);
  const mgr = makeManager(baseCfg(), { fetchFn });

  await mgr.run('manual');
  assert.equal(mgr.factor, 2);
  await mgr.run('manual');
  assert.equal(mgr.factor, 4);
  await mgr.run('manual');
  assert.equal(mgr.factor, 4, 'the factor must be capped');

  mode = 'ok';
  await mgr.run('manual');
  assert.equal(mgr.factor, 2, 'one good cycle buys back one step');
  await mgr.run('manual');
  assert.equal(mgr.factor, 1);
  assert.equal(mgr.cooldownRemainingMs(), 0);
});

/* ---- 3. cheap cycles while a trade is open ------------------------------- */

await test('an open trade is only re-analysed every MANAGEMENT_EVERY cycles', async () => {
  const cfg = baseCfg({ SYMBOLS: ['XAUUSD'], MANAGEMENT_EVERY: 2 });
  const dispatcher = fakeDispatcher();
  const fetchFn = modelFetch();
  const mgr = makeManager(cfg, { fetchFn, dispatcher });
  const svc = mgr.serviceFor('XAUUSD');

  await mgr.run('schedule'); // cycle 1 — opens the trade
  assert.equal(fetchFn.calls.length, 1);
  assert.ok(svc.position, 'cycle 1 should have opened a trade');

  await mgr.run('schedule'); // cycle 2 — reversal check
  assert.equal(fetchFn.calls.length, 2);

  await mgr.run('schedule'); // cycle 3 — free status only
  assert.equal(fetchFn.calls.length, 2, 'the model must not be called on a management cycle');
  const last = dispatcher.pushed.at(-1);
  assert.equal(last.type, CHAT_TYPES.AI_SIGNAL);
  assert.equal(last.meta.kind, 'management');

  await mgr.run('schedule'); // cycle 4 — reversal check again
  assert.equal(fetchFn.calls.length, 3);
});

await test('without an open trade every cycle still asks the model', async () => {
  const cfg = baseCfg({ SYMBOLS: ['XAUUSD'], MANAGEMENT_EVERY: 2, TRACK_ENABLED: false });
  const fetchFn = modelFetch();
  const mgr = makeManager(cfg, { fetchFn });

  await mgr.run('schedule');
  await mgr.run('schedule');
  await mgr.run('schedule');
  assert.equal(fetchFn.calls.length, 3, 'signal-only mode must not skip cycles');
  assert.equal(mgr.status().openTrades.length, 0);
});

/* ---- 4. plaintext feed --------------------------------------------------- */

await test('the published frame carries the levels in the clear', async () => {
  const dispatcher = fakeDispatcher();
  const mgr = makeManager(baseCfg({ SYMBOLS: ['XAUUSD'] }), { dispatcher });

  await mgr.run('manual');
  const frame = dispatcher.pushed.find((m) => m.type === CHAT_TYPES.AI_SIGNAL);
  assert.ok(frame, 'a signal frame should have been published');

  // The trade rides the same public socket as the candles: no ciphertext, no
  // redaction, the exact numbers a client is meant to act on.
  assert.equal(frame.meta.action, 'BUY');
  assert.equal(Math.round(frame.meta.entry_price), 2400);
  assert.equal(Math.round(frame.meta.stop_loss), 2388);
  assert.equal(Math.round(frame.meta.take_profit_1), 2418);
  assert.equal(typeof frame.meta.ts, 'number');
  assert.equal(frame.meta.enc, undefined, 'the wire must no longer carry ciphertext');

  // /api/ai/signal replays the same trade, not a redacted shell.
  const status = mgr.status();
  assert.equal(status.perSymbol.XAUUSD.lastSignal.action, 'BUY');
  assert.equal(status.perSymbol.XAUUSD.positionOpen, true);
  assert.equal(status.signals, undefined);
  assert.equal(status.encrypted, undefined);
});

/* ---------------------------------------------------------------- summary */

console.log(`\n${failed === 0 ? '✅' : '❌'} AI manager: ${passed} passed, ${failed} failed\n`);
process.exit(failed === 0 ? 0 : 1);
