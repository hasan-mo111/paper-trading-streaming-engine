/* =============================================================================
 *  AETHERON CORE  —  test/aiSignal.test.mjs
 *  -----------------------------------------------------------------------------
 *  Unit tests for the AI analyst. No network, no key, no Supabase: the market
 *  aggregator, the dispatcher and fetch are all injected / stubbed, so the
 *  parsing and validation core is exercised deterministically in milliseconds.
 *
 *  COVERED
 *  -------
 *    1. prompt/snapshot   five frames, compact rows, live price included
 *    2. model call        headers + body, JSON-mode request, empty-reply guard
 *    3. parsing           plain JSON, ```json fences, prose-wrapped objects
 *    4. validation        BUY/SELL ordering, R:R floor, entry-drift guard,
 *                         NO_TRADE → HOLD, unknown action → HOLD
 *    5. publishing        AI_SIGNAL chat message, Telegram only for valid trades
 *    6. lifecycle         run() guards: disabled, busy, cooldown, insufficient
 *
 *  Run:  npm run test:ai
 * ============================================================================= */

import assert from 'node:assert/strict';

import { AiSignalService, SYSTEM_PROMPT, formatChat, formatTelegram, JSON_CORRECTION } from '../src/services/aiSignal.js';
import { CHAT_TYPES } from '../src/services/sseDispatcher.js';

/* ------------------------------------------------------------ test harness */

let passed = 0;
let failed = 0;
function test(name, fn) {
  try {
    fn();
    passed += 1;
    console.log(`  ✓ ${name}`);
  } catch (e) {
    failed += 1;
    console.error(`  ✗ ${name}\n      ${e.message}`);
  }
}
async function testAsync(name, fn) {
  try {
    await fn();
    passed += 1;
    console.log(`  ✓ ${name}`);
  } catch (e) {
    failed += 1;
    console.error(`  ✗ ${name}\n      ${e.message}`);
  }
}

/* ------------------------------------------------------------- fake deps --- */

/** A market stub that serves `n` closed candles per timeframe. */
function fakeMarket({ symbol = 'XAUUSD', price = 2400, bars = 80, omit = [] } = {}) {
  const frames = {
    D1: 86400,
    H4: 14400,
    H1: 3600,
    M15: 900,
    M5: 300,
  };
  const out = {};
  for (const [bucket, seconds] of Object.entries(frames)) {
    const n = omit.includes(bucket) ? 3 : bars;
    out[seconds] = Array.from({ length: n }, (_, i) => ({
      time: 1_700_000_000 + i * seconds,
      open: price,
      high: price + 1,
      low: price - 1,
      close: price,
      tickVolume: 10,
    }));
  }
  return {
    env: { ai: baseCfg() },
    closedCandles: (_s, interval) => out[interval] ?? [],
    formingCandle: (_s, interval) => (omit.includes(bucketOf(interval)) ? null : { time: 1_800_000_000, open: price, high: price, low: price, close: price }),
    priceOf: (_s, interval) => (out[interval]?.length ? price : null),
    summary: () => ({ symbol }),
  };
}

const bucketOf = (seconds) =>
  ({ 86400: 'D1', 14400: 'H4', 3600: 'H1', 900: 'M15', 300: 'M5' })[seconds];

function baseCfg(over = {}) {
  return {
    ENABLED: true,
    API_KEY: 'sk-test',
    MODEL: 'llama-3.3-70b-versatile',
    SYMBOL: 'XAUUSD',
    INTERVAL_MS: 1_800_000,
    MANUAL_COOLDOWN_S: 60,
    BOOT_DELAY_MS: 120_000,
    TIMEOUT_MS: 5_000,
    MAX_TOKENS: 2000,
    MIN_RR: 2,
    MAX_ENTRY_DRIFT: 0.02,
    BARS: { D1: 60, H4: 60, H1: 80, M15: 60, M5: 60 },
    TELEGRAM: true,
    TELEGRAM_CHAT_ID: '',
    ...over,
  };
}

function fakeDispatcher() {
  const pushed = [];
  return {
    pushed,
    push: (m) => pushed.push(m),
  };
}

function fakeTelegram() {
  const sent = [];
  return {
    sent,
    notifyAll: async (text) => {
      sent.push(text);
      return 1;
    },
  };
}

/** A fetch stub that answers with a given model content string. */
function fakeFetch(content, { ok = true, status = 200 } = {}) {
  const calls = [];
  const fn = async (url, opts) => {
    calls.push({ url, opts, body: JSON.parse(opts.body) });
    return {
      ok,
      status,
      text: async () =>
        ok
          ? JSON.stringify({ choices: [{ message: { content } }] })
          : JSON.stringify({ error: { message: 'boom' } }),
    };
  };
  fn.calls = calls;
  return fn;
}

/** A fetch stub that returns a SEQUENCE of raw message objects, one per call.
 *  Used to exercise the fallback path: first call empty, second call valid. */
function sequenceFetch(messages) {
  const calls = [];
  const fn = async (url, opts) => {
    calls.push({ url, opts, body: JSON.parse(opts.body) });
    const msg = messages[Math.min(calls.length - 1, messages.length - 1)];
    return { ok: true, status: 200, text: async () => JSON.stringify({ choices: [{ finish_reason: 'stop', message: msg }] }) };
  };
  fn.calls = calls;
  return fn;
}

const EMPTY = { content: '' };

const buySignal = (entry = 2400, sl = 2390, tp1 = 2415, tp2 = 2425) =>
  JSON.stringify({
    action: 'BUY',
    symbol: 'XAU/USD',
    entry_price: entry,
    stop_loss: sl,
    take_profit_1: tp1,
    take_profit_2: tp2,
    confidence_score: 'High',
    reasoning: 'BOS on H4, liquidity swept, entry on the 15m pullback.',
  });

/* ================================================================== tests */

console.log('\nAI analyst — service unit tests\n');

/* ---- 1. snapshot + prompt ------------------------------------------------ */

await test('service reports its configuration through status()', () => {
  const market = fakeMarket({ price: 2400 });
  const svc = new AiSignalService({ market, dispatcher: fakeDispatcher(), fetchFn: fakeFetch(buySignal()) });
  const s = svc.status();
  assert.equal(s.enabled, true);
  assert.equal(s.symbol, 'XAUUSD');
  assert.equal(s.model, 'llama-3.3-70b-versatile');
  assert.equal(s.provider, 'groq');
  assert.equal(s.maxTokens, 2000);
  assert.equal(s.lastSignal, null);
});

await testAsync('run() sends the system prompt and a compact user turn to Groq', async () => {
  const market = fakeMarket({ price: 2400 });
  const fetchFn = fakeFetch(buySignal());
  const svc = new AiSignalService({ market, dispatcher: fakeDispatcher(), telegram: fakeTelegram(), fetchFn });

  const res = await svc.run('manual');
  assert.equal(res.ok, true, 'run should succeed');

  const [call] = fetchFn.calls;
  assert.match(call.url, /api\.groq\.com\/openai\/v1\/chat\/completions/);
  assert.equal(call.opts.headers.Authorization, 'Bearer sk-test');
  assert.equal(call.body.model, 'llama-3.3-70b-versatile');
  assert.equal(call.body.messages[0].role, 'system');
  assert.equal(call.body.messages[0].content, SYSTEM_PROMPT);
  assert.equal(call.body.response_format.type, 'json_object');

  const user = JSON.parse(call.body.messages[1].content);
  assert.equal(user.current_price, 2400);
  assert.deepEqual(Object.keys(user.timeframes), ['D1', 'H4', 'H1', 'M15', 'M5']);
  assert.equal(user.timeframes.D1.rows.length, 61); // 60 closed + forming
  assert.deepEqual(user.row_format, ['epoch_seconds', 'open', 'high', 'low', 'close']);
});

/* ---- 2. parsing ---------------------------------------------------------- */

await testAsync('parses a fenced ```json reply', async () => {
  const market = fakeMarket();
  const content = '```json\n' + buySignal() + '\n```';
  const svc = new AiSignalService({ market, dispatcher: fakeDispatcher(), telegram: fakeTelegram(), fetchFn: fakeFetch(content) });
  const res = await svc.run('manual');
  assert.equal(res.ok, true);
  assert.equal(res.signal.action, 'BUY');
});

await testAsync('parses a reply wrapped in prose', async () => {
  const market = fakeMarket();
  const content = `Here is the signal:\n${buySignal()}\nGood luck.`;
  const svc = new AiSignalService({ market, dispatcher: fakeDispatcher(), telegram: fakeTelegram(), fetchFn: fakeFetch(content) });
  const res = await svc.run('manual');
  assert.equal(res.ok, true);
  assert.equal(res.signal.action, 'BUY');
});

await testAsync('unparseable output is rejected, not published', async () => {
  const market = fakeMarket();
  const dispatcher = fakeDispatcher();
  const env = { ai: baseCfg({ MAX_ATTEMPTS: 1 }) };
  const svc = new AiSignalService({ market, dispatcher, env, fetchFn: fakeFetch('not json at all') });
  const res = await svc.run('manual');
  assert.equal(res.ok, false);
  assert.equal(res.error, 'bad-model-output');
  assert.equal(dispatcher.pushed.length, 0);
});

/* ---- 3. validation ------------------------------------------------------- */

await testAsync('valid BUY passes with correct R:R', async () => {
  const market = fakeMarket({ price: 2400 });
  const svc = new AiSignalService({ market, dispatcher: fakeDispatcher(), telegram: fakeTelegram(), fetchFn: fakeFetch(buySignal()) });
  const { signal } = await svc.run('manual');
  assert.equal(signal.action, 'BUY');
  assert.equal(signal.valid, true);
  assert.ok(signal.rr >= 2, `expected R:R ≥ 2, got ${signal.rr}`);
});

await testAsync('R:R below the floor is downgraded to HOLD', async () => {
  const market = fakeMarket({ price: 2400 });
  // risk 10; TP1 = +1R (2410) passes the first-target rule,
  // TP2 = +1.5R (2415) fails the 1:2 floor → downgrade.
  const svc = new AiSignalService({ market, dispatcher: fakeDispatcher(), telegram: fakeTelegram(), fetchFn: fakeFetch(buySignal(2400, 2390, 2410, 2415)) });
  const { signal } = await svc.run('manual');
  assert.equal(signal.action, 'HOLD');
  assert.equal(signal.valid, false);
  assert.match(signal.note, /reward:risk/);
});

await testAsync('stop on the wrong side of entry is downgraded', async () => {
  const market = fakeMarket({ price: 2400 });
  const svc = new AiSignalService({ market, dispatcher: fakeDispatcher(), telegram: fakeTelegram(), fetchFn: fakeFetch(buySignal(2400, 2410, 2420, 2430)) });
  const { signal } = await svc.run('manual');
  assert.equal(signal.action, 'HOLD');
  assert.match(signal.note, /wrong side/);
});

await testAsync('entry far from the live quote is downgraded', async () => {
  const market = fakeMarket({ price: 2400 });
  // entry 2500 = +4.1% away
  const svc = new AiSignalService({ market, dispatcher: fakeDispatcher(), telegram: fakeTelegram(), fetchFn: fakeFetch(buySignal(2500, 2480, 2530, 2560)) });
  const { signal } = await svc.run('manual');
  assert.equal(signal.action, 'HOLD');
  assert.equal(signal.valid, false);
  assert.match(signal.note, /away from live price/);
});

await testAsync('NO_TRADE is normalised to HOLD and stays valid', async () => {
  const market = fakeMarket();
  const content = JSON.stringify({ action: 'NO_TRADE', symbol: 'XAU/USD', confidence_score: 'Low', reasoning: 'Consolidation.' });
  const svc = new AiSignalService({ market, dispatcher: fakeDispatcher(), fetchFn: fakeFetch(content) });
  const { signal } = await svc.run('manual');
  assert.equal(signal.action, 'HOLD');
  assert.equal(signal.valid, true);
});

await testAsync('SELL with correct ordering and R:R is accepted', async () => {
  const market = fakeMarket({ price: 2400 });
  const content = JSON.stringify({
    action: 'SELL',
    symbol: 'XAU/USD',
    entry_price: 2400,
    stop_loss: 2410,
    take_profit_1: 2385,
    take_profit_2: 2375,
    confidence_score: 'Medium',
    reasoning: 'Lower highs on H1, rejected at premium.',
  });
  const svc = new AiSignalService({ market, dispatcher: fakeDispatcher(), telegram: fakeTelegram(), fetchFn: fakeFetch(content) });
  const { signal } = await svc.run('manual');
  assert.equal(signal.action, 'SELL');
  assert.equal(signal.valid, true);
});

/* ---- 4. publishing ------------------------------------------------------- */

await testAsync('valid trade publishes an AI_SIGNAL chat message and a Telegram alert', async () => {
  const market = fakeMarket({ price: 2400 });
  const dispatcher = fakeDispatcher();
  const telegram = fakeTelegram();
  const svc = new AiSignalService({ market, dispatcher, telegram, fetchFn: fakeFetch(buySignal()) });
  await svc.run('manual');

  assert.equal(dispatcher.pushed.length, 1);
  const msg = dispatcher.pushed[0];
  assert.equal(msg.type, CHAT_TYPES.AI_SIGNAL);
  assert.equal(msg.severity, 'success');
  assert.equal(msg.meta.source, 'ai-analyst');
  assert.equal(msg.meta.action, 'BUY');
  assert.match(msg.message, /AI XAUUSD · BUY/);

  assert.equal(telegram.sent.length, 1);
  assert.match(telegram.sent[0], /AI SIGNAL/);
});

await testAsync('HOLD is published to chat but never sent to Telegram', async () => {
  const market = fakeMarket();
  const dispatcher = fakeDispatcher();
  const telegram = fakeTelegram();
  const content = JSON.stringify({ action: 'HOLD', symbol: 'XAU/USD', confidence_score: 'Low', reasoning: 'Noise.' });
  const env = { ai: baseCfg({ STATUS_EVERY_CYCLE: false }) };
  const svc = new AiSignalService({ market, dispatcher, telegram, env, fetchFn: fakeFetch(content) });
  await svc.run('manual');
  assert.equal(dispatcher.pushed.length, 1);
  assert.equal(dispatcher.pushed[0].severity, 'info');
  assert.equal(telegram.sent.length, 0);
});

await testAsync('an unchanged trade is not re-sent to Telegram', async () => {
  const market = fakeMarket({ price: 2400 });
  const telegram = fakeTelegram();
  const env = { ai: baseCfg({ STATUS_EVERY_CYCLE: false }) };
  const svc = new AiSignalService({ market, dispatcher: fakeDispatcher(), telegram, env, fetchFn: fakeFetch(buySignal()) });
  await svc.run('manual');
  await svc.run('manual');
  assert.equal(telegram.sent.length, 1, 'second identical signal must be suppressed');
});

/* ---- 5. lifecycle guards ------------------------------------------------- */

await testAsync('unconfigured service idles (no key)', async () => {
  const market = fakeMarket();
  const fetchFn = fakeFetch(buySignal());
  const svc = new AiSignalService({ market, dispatcher: fakeDispatcher(), env: { ai: baseCfg({ API_KEY: '' }) }, fetchFn });
  const res = await svc.run('manual');
  assert.equal(res.ok, false);
  assert.equal(res.error, 'disabled');
  assert.equal(fetchFn.calls.length, 0);
  assert.equal(svc.enabled, false);
});

await testAsync('manual cooldown returns retryInMs without calling the model', async () => {
  const market = fakeMarket({ price: 2400 });
  const fetchFn = fakeFetch(buySignal());
  const svc = new AiSignalService({ market, dispatcher: fakeDispatcher(), telegram: fakeTelegram(), fetchFn });
  await svc.run('manual');
  const res = await svc.run('manual');
  assert.equal(res.ok, false);
  assert.equal(res.error, 'cooldown');
  assert.ok(res.retryInMs > 0);
  assert.equal(fetchFn.calls.length, 1);
});

await testAsync('insufficient candles refuses to call the model', async () => {
  const market = fakeMarket({ omit: ['D1'] }); // D1 has only 3 bars
  const fetchFn = fakeFetch(buySignal());
  const svc = new AiSignalService({ market, dispatcher: fakeDispatcher(), fetchFn });
  const res = await svc.run('manual');
  assert.equal(res.ok, false);
  assert.equal(res.error, 'insufficient-data');
  assert.equal(fetchFn.calls.length, 0);
});

await testAsync('a slow model call cannot be overlapped (busy guard)', async () => {
  const market = fakeMarket({ price: 2400 });
  const dispatcher = fakeDispatcher();
  const slow = async () => {
    await new Promise((r) => setTimeout(r, 30));
    return { ok: true, status: 200, text: async () => JSON.stringify({ choices: [{ message: { content: buySignal() } }] }) };
  };
  const svc = new AiSignalService({ market, dispatcher, telegram: fakeTelegram(), fetchFn: slow });
  const [a, b] = await Promise.all([svc.run('schedule'), svc.run('schedule')]);
  assert.equal(a.ok, true);
  assert.equal(b.ok, false);
  assert.equal(b.error, 'busy');
});

/* ---- 5b. fallback + reasoning ------------------------------------------- */

await test('an empty completion from the primary model falls back to the provider fallback', async () => {
  const market = fakeMarket({ price: 2400 });
  const fetchFn = sequenceFetch([EMPTY, { content: buySignal() }]);
  const env = { ai: baseCfg({ MODEL: 'openai/gpt-oss-120b' }) };
  const svc = new AiSignalService({ market, dispatcher: fakeDispatcher(), telegram: fakeTelegram(), env, fetchFn });
  const res = await svc.run('manual');
  assert.equal(res.ok, true, 'fallback should recover');
  assert.equal(fetchFn.calls.length, 2, 'primary + fallback = 2 calls');
  // First call is the configured model, second is the fallback with more room.
  assert.equal(fetchFn.calls[0].body.model, 'openai/gpt-oss-120b');
  assert.equal(fetchFn.calls[1].body.model, 'llama-3.1-8b-instant');
  assert.ok(fetchFn.calls[1].body.max_tokens >= 8000, 'fallback gets extra headroom');
  assert.equal(svc.stats.fallbacks, 1);
});

await test('reasoning_content is used when content is empty', async () => {
  const market = fakeMarket({ price: 2400 });
  const fetchFn = sequenceFetch([{ content: '', reasoning_content: buySignal() }]);
  const svc = new AiSignalService({ market, dispatcher: fakeDispatcher(), telegram: fakeTelegram(), fetchFn });
  const res = await svc.run('manual');
  assert.equal(res.ok, true, 'reasoning_content should satisfy the parser');
  assert.equal(res.signal.action, 'BUY');
  assert.equal(fetchFn.calls.length, 1, 'no fallback needed');
});

await test('empty completion on the fallback model surfaces a diagnostic', async () => {
  const market = fakeMarket({ price: 2400 });
  const fetchFn = sequenceFetch([EMPTY, EMPTY]);
  // Force a non-fallback primary so the fallback is actually attempted.
  const env = { ai: baseCfg({ MODEL: 'nvidia/nemotron-3-ultra-550b-a55b:free' }) };
  const svc = new AiSignalService({ market, dispatcher: fakeDispatcher(), env, fetchFn });
  const res = await svc.run('manual');
  assert.equal(res.ok, false);
  assert.equal(res.error, 'exception');
  assert.match(res.detail, /empty completion/);
  assert.match(res.detail, /AI_SIGNAL_MAX_TOKENS|AI_SIGNAL_MODEL/);
  assert.equal(fetchFn.calls.length, 2);
});

await test('no second attempt when the primary IS the fallback model', async () => {
  const market = fakeMarket({ price: 2400 });
  const fetchFn = sequenceFetch([EMPTY]);
  const env = { ai: baseCfg({ MODEL: 'llama-3.1-8b-instant', MAX_ATTEMPTS: 1 }) };
  const svc = new AiSignalService({ market, dispatcher: fakeDispatcher(), env, fetchFn });
  const res = await svc.run('manual');
  assert.equal(res.ok, false);
  assert.equal(fetchFn.calls.length, 1, 'maxAttempts=1 → single attempt');
});

/* ---- 5c. prose replies get one corrected retry --------------------------- */

await test('a prose reply with no JSON triggers a corrected retry that recovers', async () => {
  const market = fakeMarket({ price: 2400 });
  const dispatcher = fakeDispatcher();
  const telegram = fakeTelegram();
  const fetchFn = sequenceFetch([
    { content: "Here's the setup I see on gold right now — momentum looks constructive, so..." },
    { content: buySignal() },
  ]);
  const env = { ai: baseCfg({ MAX_ATTEMPTS: 2 }) };
  const svc = new AiSignalService({ market, dispatcher, telegram, env, fetchFn });
  const res = await svc.run('manual');

  assert.equal(res.ok, true, 'the corrected retry should recover the cycle');
  assert.equal(res.signal.action, 'BUY');
  assert.equal(fetchFn.calls.length, 2, 'prose + corrected retry = 2 calls');

  // The retry re-sends the original turn and appends the correction, with room.
  const retry = fetchFn.calls[1].body;
  assert.equal(retry.messages.length, 3);
  assert.equal(retry.messages[2].role, 'user');
  assert.equal(retry.messages[2].content, JSON_CORRECTION);
  assert.ok(retry.max_tokens >= 4000, 'retry gets extra headroom');

  assert.equal(dispatcher.pushed.length, 1, 'the recovered signal is published');
  assert.equal(telegram.sent.length, 1);
  assert.equal(svc.stats.fallbacks, 1);
  assert.equal(svc.stats.errors, 0, 'a recovered cycle is not counted as an error');
});

await test('a prose reply on a non-fallback model retries on the provider fallback', async () => {
  const market = fakeMarket({ price: 2400 });
  const fetchFn = sequenceFetch([{ content: 'Sure, let me walk you through it.' }, { content: buySignal() }]);
  const env = { ai: baseCfg({ MODEL: 'some/chatty-model:free', MAX_ATTEMPTS: 2 }) };
  const svc = new AiSignalService({ market, dispatcher: fakeDispatcher(), telegram: fakeTelegram(), env, fetchFn });
  const res = await svc.run('manual');

  assert.equal(res.ok, true);
  assert.equal(fetchFn.calls[0].body.model, 'some/chatty-model:free');
  assert.equal(fetchFn.calls[1].body.model, 'llama-3.1-8b-instant', 'retry uses the fallback model');
});

await test('prose on both attempts fails cleanly and publishes nothing', async () => {
  const market = fakeMarket({ price: 2400 });
  const dispatcher = fakeDispatcher();
  const fetchFn = sequenceFetch([{ content: 'no json in here' }, { content: 'still no json, sorry' }]);
  const env = { ai: baseCfg({ MAX_ATTEMPTS: 2 }) };
  const svc = new AiSignalService({ market, dispatcher, env, fetchFn });
  const res = await svc.run('manual');

  assert.equal(res.ok, false);
  assert.equal(res.error, 'bad-model-output');
  assert.equal(dispatcher.pushed.length, 0);
  assert.equal(fetchFn.calls.length, 2);
  assert.equal(svc.stats.errors, 1);
});

/* ---- 6. formatters ------------------------------------------------------- */

await test('formatChat renders a compact trade line', () => {
  const line = formatChat({
    action: 'BUY',
    symbol: 'XAUUSD',
    entry_price: 2400,
    stop_loss: 2390,
    take_profit_1: 2415,
    take_profit_2: 2425,
    rr: 2.5,
    confidence_score: 'High',
    valid: true,
    note: null,
    reasoning: 'x',
  });
  assert.match(line, /AI XAUUSD · BUY @ 2400/);
  assert.match(line, /R:R 1:2\.50/);
});

await test('formatTelegram escapes untrusted reasoning', () => {
  const html = formatTelegram({
    action: 'SELL',
    symbol: 'XAUUSD',
    entry_price: 2400,
    stop_loss: 2410,
    take_profit_1: 2385,
    take_profit_2: 2375,
    rr: 2.5,
    confidence_score: 'Medium',
    reasoning: '<script>alert(1)</script>',
  });
  assert.ok(!html.includes('<script>'), 'raw tag must not survive');
  assert.match(html, /&lt;script&gt;/);
});

/* ------------------------------------------------------------------ report */

console.log(`\n${failed === 0 ? '✅' : '❌'} AI analyst: ${passed} passed, ${failed} failed\n`);
process.exit(failed === 0 ? 0 : 1);
