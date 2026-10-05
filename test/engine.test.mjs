/* =============================================================================
 *  AETHERON CORE  —  test/engine.test.mjs
 *  -----------------------------------------------------------------------------
 *  Unit tests for the quant layer and the freemium gate. No network, no
 *  Supabase, no broker: every dependency is injected, so this suite runs in
 *  milliseconds and is deterministic.
 *
 *  COVERED
 *  -------
 *    1. indicators    ATR (Wilder), Donchian, trailing channel, breakout
 *    2. sizing        LotSize = (Balance × Risk%) / (ATR × PointValue) + clamps
 *    3. entry         50-day breakout fires only on a genuine break
 *    4. exits         Leg 1 at +1×ATR, Leg 2 rider, trailing stop
 *    5. quota         FREE = 3/day then SIGNAL_ONLY; PRO unlimited; exits free
 *    6. security      the API refuses execution parameters from a client
 *
 *  Run:  npm run test:engine
 * ============================================================================= */

import assert from 'node:assert/strict';
import { ENV } from '../src/config/env.js';
import {
  SYMBOL_SPECS,
  normaliseLot,
  pointsValue,
  seriesKey,
  getSymbolSpec,
} from '../src/config/db.js';
import {
  atr,
  donchian,
  trailingChannel,
  breakoutSignal,
  normaliseSeries,
  profitPoints,
  roundToStep,
} from '../src/engine/indicatorCalc.js';
import { MarketAggregator } from '../src/services/marketAggregator.js';
import { PaperBroker } from '../src/services/brokerApi.js';
import { StrategyEngine, ENGINE_STATE } from '../src/engine/strategyEngine.js';
import { SupabaseService } from '../src/services/supabase.js';
import { SSEHub } from '../src/services/sseHub.js';
import { SSEDispatcher } from '../src/services/sseDispatcher.js';
import { QuotaManager, createRateLimiter } from '../src/middleware/quotaManager.js';
import { AuthService } from '../src/middleware/auth.js';

/* ------------------------------------------------------------------ harness */
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
    console.log(`  \x1b[32m+\x1b[0m ${name}${detail ? ` — ${detail}` : ''}`);
  } else {
    failed++;
    failures.push(`[${currentSection}] ${name}${detail ? ` — ${detail}` : ''}`);
    console.log(`  \x1b[31mX\x1b[0m ${name}${detail ? ` — ${detail}` : ''}`);
  }
};
const near = (a, b, tol = 1e-6) => Math.abs(a - b) <= tol;

/* =============================================================================
 *  SYNTHETIC MARKET BUILDER
 * ============================================================================= */

/**
 * Build a deterministic candle series. `count` candles of `size` price units
 * each, arranged so the Donchian channel and the ATR are known in advance.
 */
function buildCandles({ count, start = 100, step = 1, timeStart = 1_700_000_000, interval = 86_400 }) {
  const out = [];
  let price = start;
  for (let i = 0; i < count; i++) {
    const o = price;
    const c = price + step;
    out.push({
      time: timeStart + i * interval,
      open: o,
      high: Math.max(o, c) + step * 0.5,
      low: Math.min(o, c) - step * 0.5,
      close: c,
      tickVolume: 100,
    });
    price = c;
  }
  return out;
}

/**
 * Assemble a fully wired engine against in-memory services.
 * `plan` decides which quota tier the synthetic profile reports.
 */
function harness({
  plan = 'PRO',
  balance = 10_000,
  riskPct = 0.01,
  dailyCount = 120,
  hourlyCount = 120,
  strategyOverrides = {},
  brokerMode = 'paper',
} = {}) {
  const env = {
    ...ENV,
    strategy: { ...ENV.strategy, ...strategyOverrides },
    broker: { ...ENV.broker, MODE: brokerMode, PAPER_LATENCY_MS: 0, DEFAULT_BALANCE: balance, DEFAULT_RISK_PCT: riskPct },
  };

  const store = new SupabaseService({ env }); // no client → local mode
  const auth = new AuthService({ supabase: store, env: { ...env, auth: { ...env.auth, DEV_TOKEN: '' } } });
  // Force a deterministic profile so the tests never touch a database.
  store.local.profiles.set('test-user', {
    user_id: 'test-user',
    email: 't@e.st',
    plan,
    account_balance: balance,
    risk_percentage: riskPct,
    enabled: true,
    symbols: [],
  });

  const quota = new QuotaManager({ supabase: store, auth, env });
  const hub = new SSEHub({ env });
  const dispatcher = new SSEDispatcher({ hub, env, persistence: null });
  const broker = new PaperBroker({ env });

  // Silence the chat stream during tests, but keep every message for assertions.
  const chat = [];
  for (const fn of ['system', 'risk', 'quota', 'execution', 'signalOnly', 'signal', 'partialClose', 'trailing', 'stopHit']) {
    dispatcher[fn] = (payload) => {
      const record = typeof payload === 'string' ? { message: payload } : payload;
      chat.push({ fn, ...record });
      return record;
    };
  }

  const market = new MarketAggregator({ env, persistence: null });
  const engine = new StrategyEngine({ market, broker, quota, dispatcher, auth, persistence: null, env, userId: 'test-user' });

  return { env, store, auth, quota, hub, dispatcher, broker, market, engine, chat, balance, riskPct };
}

/**
 * Push a candle series into the aggregator's ring for one series and open a
 * forming bar one interval later. `lastUpdate` is stamped so the series counts
 * as fresh — a stale series is (correctly) refused by the engine.
 */
function seed(market, symbol, interval, candles) {
  market.mergeHistory(symbol, interval, candles);
  const slot = market.store.get(seriesKey(symbol, interval));
  const lastClosed = slot.candles.at(-1);
  slot.current = {
    time: lastClosed.time + interval,
    open: lastClosed.close,
    high: lastClosed.close,
    low: lastClosed.close,
    close: lastClosed.close,
    tickVolume: 0,
  };
  slot.lastUpdate = Date.now();
  return slot;
}

/* =============================================================================
 *  1. INDICATORS
 * ============================================================================= */
console.log('\n\x1b[1mAetheron Core — engine, quota & security tests\x1b[0m');
console.log('Developed by Eng Hasan Mohamad\n');

section('1) indicatorCalc — ATR(20), Donchian, trailing channel');
{
  // Constant-range candles → ATR converges to the true range.
  // bar: high = close + step/2, low = open - step/2 ⇒ high-low = 2×step
  const candles = buildCandles({ count: 60, start: 100, step: 2 });
  const value = atr(candles, 20);
  check('ATR(20) of a constant-range series equals its true range', near(value, 4, 1e-9), `atr=${value}`);

  check('ATR refuses to guess from a short sample', atr(buildCandles({ count: 10 }), 20) === null);
  check('ATR needs period+1 candles (one for the first TR)', atr(buildCandles({ count: 20 }), 20) === null);
  check('ATR works with exactly period+1', atr(buildCandles({ count: 21 }), 20) !== null);
  check('ATR rejects garbage', atr([{ time: 1, open: 1, high: 1, low: 1, close: 1 }], 20) === null);

  // Donchian over 50 rising candles: the last high is the highest.
  const rising = buildCandles({ count: 60, start: 100, step: 1 });
  const ch = donchian(rising, 50);
  const expectedHigh = Math.max(...rising.slice(-50).map((c) => c.high));
  const expectedLow = Math.min(...rising.slice(-50).map((c) => c.low));
  check('Donchian(50) high is the max high of the last 50', near(ch.high, expectedHigh), `${ch.high}`);
  check('Donchian(50) low is the min low of the last 50', near(ch.low, expectedLow), `${ch.low}`);
  check('Donchian refuses a partial channel', donchian(buildCandles({ count: 30 }), 50) === null);
  check('Donchian window is exactly `period` bars', ch.count === 50 && ch.to - ch.from === 49 * 86_400);

  // THE critical contract: the ENTRY channel is built from CLOSED candles only,
  // so a live bar's own high is never part of the level price must exceed.
  // `donchian()` is a dumb pure function — the exclusion lives in the engine,
  // which reads `closedCandles()` (never the forming bar) for the breakout test.
  {
    const closed = buildCandles({ count: 120, start: 2000, step: 1 });
    const hugeForming = { time: closed.at(-1).time + 86_400, open: 0, high: 999_999, low: 0, close: 0 };
    const agg = new MarketAggregator({ env: ENV, persistence: null });
    const slot = seed(agg, 'XAUUSD', 86_400, closed);
    slot.current = hugeForming;
    check(
      'closedCandles() excludes the forming bar from the entry channel',
      donchian(agg.closedCandles('XAUUSD', 86_400), 50).high < 999_999,
      `channel high ${donchian(agg.closedCandles('XAUUSD', 86_400), 50).high}`
    );
    check(
      'the forming bar IS visible to the trailing channel',
      trailingChannel(agg.closedCandles('XAUUSD', 86_400), agg.formingCandle('XAUUSD', 86_400), 20).high > 999_000,
      'live bar folded in'
    );
  }

  // The trailing channel is the opposite: it must follow the live bar.
  const closed60 = buildCandles({ count: 60, start: 100, step: 1 });
  const formingBar = { time: closed60.at(-1).time + 86_400, open: 0, high: 9999, low: 0, close: 0 };
  const t1 = trailingChannel(closed60, formingBar, 20);
  check('trailing channel folds in the forming bar', near(t1.high, 9999), `high=${t1.high}`);
  const t2 = trailingChannel(closed60, null, 20);
  check('trailing channel falls back to closed bars when there is no live bar', t2 !== null && near(t2.high, Math.max(...closed60.slice(-20).map((c) => c.high))), `high=${t2?.high}`);
  check('trailing channel refuses a series shorter than its window', trailingChannel(buildCandles({ count: 5 }), null, 20) === null);

  check('normaliseSeries drops invalid rows and sorts', (() => {
    const out = normaliseSeries([{ time: 3, open: 1, high: 2, low: 0, close: 1 }, { time: 1, open: 1, high: 2, low: 0, close: 1 }, null, { time: 2, open: 5, high: 1, low: 9, close: 1 }]);
    return out.length === 2 && out[0].time === 1 && out[1].time === 3;
  })(), 'malformed candle high<low dropped');

  check('breakoutSignal fires only beyond the channel', breakoutSignal(1_000_010, { high: 1_000_000, low: 990_000 }, { side: 'BUY' }) === 'LONG');
  check('breakoutSignal stays quiet inside the channel', breakoutSignal(995_000, { high: 1_000_000, low: 990_000 }, { side: 'BUY' }) === 'NONE');
  check('breakout confirm buffer suppresses grazes', breakoutSignal(1_000_005, { high: 1_000_000, low: 990_000 }, { side: 'BUY', confirmPct: 0.0001 }) === 'NONE');
  check('profitPoints respects direction', profitPoints('BUY', 100, 105) === 5 && profitPoints('SELL', 100, 95) === 5);
  check('roundToStep floors onto the grid', roundToStep(0.037, 0.01) === 0.03);
}

/* =============================================================================
 *  2. SIZING
 * ============================================================================= */
section('2) Volatility-targeted position sizing');
{
  const { engine, balance, riskPct } = harness();

  // ATR = $3.00 of price movement on gold.
  //   pointSize 0.01  → atrPoints = 300 points
  //   pointValue $1/point/lot → riskPerLot = $300
  //   risk = 10,000 × 1% = $100 → 100 / 300 = 0.33 lots → floored to 0.33
  const size = engine.calculateLotSize('XAUUSD', { atr: 3, balance: 10_000, riskPct: 0.01 });
  check('ATR is converted from price units into points', near(size.atrPoints, 300, 1e-6), `${size.atrPoints} points`);
  check('risk per lot = atrPoints × pointValue', near(size.riskPerLot, 300, 1e-6), `$${size.riskPerLot}/lot`);
  // LotSize = (Balance × Risk%) / (ATR(20) × PointValue)
  // (`requested` is reported rounded to 4 decimals, hence the 1e-4 tolerance.)
  check('lot size follows (Balance × Risk%) / (ATR × PointValue)', near(size.requested, 100 / 300, 1e-4), `requested ${size.requested}`);
  check('the formula reduces to riskAmount / riskPerLot', near(size.requested, size.riskAmount / size.riskPerLot, 1e-4));
  check('lot size is floored onto the broker lot step', near(size.lots, 0.33, 1e-9), `lots ${size.lots}`);
  check('risk amount is balance × risk%', near(size.riskAmount, 100, 1e-9));
  check('point value comes from the instrument registry', near(size.pointValue, SYMBOL_SPECS.XAUUSD.pointSize * SYMBOL_SPECS.XAUUSD.contractSize));

  // A volatile market must shrink the position — that is the whole point.
  const calm = engine.calculateLotSize('XAUUSD', { atr: 1, balance: 10_000, riskPct: 0.01 });
  const wild = engine.calculateLotSize('XAUUSD', { atr: 10, balance: 10_000, riskPct: 0.01 });
  check('a 10× volatility increase shrinks the lot by 10×', near(calm.lots / wild.lots, 10, 0.02), `${calm.lots} vs ${wild.lots}`);

  // The dollars actually at risk must equal the target. This is the assertion
  // that catches a unit mismatch between price deltas, points and contract size.
  const riskDollars = pointsValue('XAUUSD', size.atr, size.lots);
  check('sized position risks ≈ the target amount', near(riskDollars, 100, 1), `${riskDollars.toFixed(2)} USD at risk`);

  // The same invariant on a second instrument with a different point size.
  // EURUSD: ATR 0.0018 = 18 points × $10 = $180 per lot → $100 risk → 0.55 lots.
  const eur = engine.calculateLotSize('EURUSD', { atr: 0.0018, balance: 10_000, riskPct: 0.01 });
  check('EURUSD ATR converts to the right number of points', near(eur.atrPoints, 18, 1e-6), `${eur.atrPoints} points`);
  check('EURUSD risk per lot is $180', near(eur.riskPerLot, 180, 1e-6), `$${eur.riskPerLot}`);
  check('EURUSD lot size ≈ 0.55', near(eur.lots, 0.55, 0.01), `${eur.lots}`);
  check('EURUSD position risks ≈ $100', near(pointsValue('EURUSD', 0.0018, eur.lots), 100, 1), `${pointsValue('EURUSD', 0.0018, eur.lots).toFixed(2)} USD`);

  // Clamping to the broker maximum.
  const huge = engine.calculateLotSize('XAUUSD', { atr: 0.05, balance: 10_000_000, riskPct: 0.5 });
  check('lot size is clamped to the broker maximum', huge.lots <= SYMBOL_SPECS.XAUUSD.maxLot, `lots ${huge.lots}`);
  check('clamping is reported', huge.clamped === true);
  check('the clamped size still respects the broker maximum', huge.lots === SYMBOL_SPECS.XAUUSD.maxLot || huge.lots < huge.requested);

  // Refusals — an engine that cannot size must not trade.
  check('refuses when ATR is unavailable', engine.calculateLotSize('XAUUSD', { atr: 0, balance, riskPct }).lots === 0);
  check('refuses when the balance is unknown', engine.calculateLotSize('XAUUSD', { atr: 3, balance: 0, riskPct }).lots === 0);
  check('refuses when risk is configured to zero', engine.calculateLotSize('XAUUSD', { atr: 3, balance, riskPct: 0 }).lots === 0);
  check('refuses an unknown instrument', engine.calculateLotSize('FAKEUSD', { atr: 3, balance, riskPct }).lots === 0);
  check('refuses when the size falls under the broker minimum', (() => {
    const s = engine.calculateLotSize('XAUUSD', { atr: 100_000, balance: 10, riskPct: 0.01 });
    return s.lots === 0 && typeof s.reason === 'string';
  })());

  check('normaliseLot rejects dust', normaliseLot('XAUUSD', 0.001) === 0);
  check('normaliseLot removes float dust', normaliseLot('XAUUSD', 0.01 * 3) === 0.03);
  check('EURUSD point value is $10/pip/lot', near(getSymbolSpec('EURUSD').pointSize * getSymbolSpec('EURUSD').contractSize, 10));
}

/* =============================================================================
 *  3. ENTRY — 50-day Donchian breakout
 * ============================================================================= */
section('3) Entry rules — 50-day Donchian breakout');
{
  // ---- 3a. inside the channel → no trade ---------------------------------
  const h = harness();
  seed(h.market, 'XAUUSD', 86_400, buildCandles({ count: 120, start: 2000, step: 1 }));
  seed(h.market, 'XAUUSD', 3600, buildCandles({ count: 120, start: 2000, step: 1 }));
  let d = await h.engine.evaluateEntry('XAUUSD');
  check('inside the channel the engine holds', d.action === 'HOLD', `${d.action}: ${d.reason}`);
  check('hold reason names the channel', /50-day channel/.test(d.reason ?? ''), d.reason);
  check('no position was opened', h.engine.openPositions().length === 0);

  // ---- 3b. genuine breakout above the 50-day high → LONG ------------------
  const h2 = harness();
  const daily = buildCandles({ count: 120, start: 2000, step: 1 });
  seed(h2.market, 'XAUUSD', 86_400, daily);
  seed(h2.market, 'XAUUSD', 3600, buildCandles({ count: 120, start: 2000, step: 1 }));

  const chHigh = donchian(daily, 50).high;
  // Push the live price above the channel high.
  const hourlySlot = h2.market.store.get(seriesKey('XAUUSD', 3600));
  hourlySlot.current.close = chHigh + 5;
  hourlySlot.lastUpdate = Date.now();

  d = await h2.engine.evaluateEntry('XAUUSD');
  check('price above the 50-day high opens a LONG', d.side === 'LONG', `${d.side} @ ${d.breakLevel}`);
  check('a PRO user gets an executed order', d.action === 'EXECUTED', `${d.action}${d.reason ? ` — ${d.reason}` : ''}`);
  check('the order has no fixed take profit', d.takeProfit === null && d.action === 'EXECUTED');
  check('a position was opened', h2.engine.openPositions().length === 1);

  const pos = h2.engine.openPositions()[0];
  check('position side is LONG', pos.side === 'LONG');
  check('position volume is the ATR-sized lot', pos.volume > 0 && pos.volume <= SYMBOL_SPECS.XAUUSD.maxLot, `${pos.volume}`);
  check('the two legs sum to the total volume', near(pos.legs[0].volume + pos.legs[1].volume, pos.volume, 0.011), `${pos.legs[0].volume} + ${pos.legs[1].volume}`);
  check('leg 1 is 25% of the position', near(pos.legs[0].pct, 0.25) && pos.legs[0].volume < pos.volume);
  check('leg 2 is the 75% runner', near(pos.legs[1].pct, 0.75) && pos.legs[1].volume > pos.legs[0].volume);

  // The initial stop is the 20-day channel low for a long.
  const expectedStop = trailingChannel(h2.market.closedCandles('XAUUSD', 86_400), h2.market.formingCandle('XAUUSD', 86_400), 20).low;
  check('initial stop is the 20-day low (long)', pos.stop != null && pos.stop < pos.entryPrice, `stop ${pos.stop} vs entry ${pos.entryPrice}`);
  check('stop sits at the 20-day channel edge', Math.abs(pos.stop - expectedStop) <= 0.02, `${pos.stop} ≈ ${expectedStop.toFixed(2)}`);
  check('a signal chat message was published', h2.chat.some((c) => c.fn === 'signal'));
  check('an execution chat message was published', h2.chat.some((c) => c.fn === 'execution'));

  // ---- 3c. no pyramiding on the same symbol ------------------------------
  d = await h2.engine.evaluateEntry('XAUUSD');
  check('the engine refuses to double up on one symbol', d.reason === 'position already open', d.reason);
  check('still exactly one position', h2.engine.openPositions().length === 1);

  // ---- 3d. breakout below the 50-day low → SHORT --------------------------
  const h3 = harness();
  const daily3 = buildCandles({ count: 120, start: 2000, step: 1 });
  seed(h3.market, 'XAUUSD', 86_400, daily3);
  seed(h3.market, 'XAUUSD', 3600, buildCandles({ count: 120, start: 2000, step: 1 }));
  const chLow = donchian(daily3, 50).low;
  const hs = h3.market.store.get(seriesKey('XAUUSD', 3600));
  hs.current.close = chLow - 5;
  hs.lastUpdate = Date.now();
  d = await h3.engine.evaluateEntry('XAUUSD');
  check('price below the 50-day low opens a SHORT', d.side === 'SHORT' && d.action === 'EXECUTED', `${d.side}/${d.action}${d.reason ? ` — ${d.reason}` : ''}`);
  const shortPos = h3.engine.openPositions()[0];
  check('a short position was opened', Boolean(shortPos));
  check('a short stop sits above the entry (20-day high)', shortPos && shortPos.stop > shortPos.entryPrice, `stop ${shortPos?.stop} vs entry ${shortPos?.entryPrice}`);
  check('the broker recorded the canonical SELL direction', h3.broker.listPositions()[0]?.side === 'SELL', h3.broker.listPositions()[0]?.side);
  check('the broker remembers the strategy\'s LONG/SHORT vocabulary', h3.broker.listPositions()[0]?.requestedSide === 'SHORT');
}

/* =============================================================================
 *  4. SPLIT EXITS
 * ============================================================================= */
section('4) Split exit logic — Leg 1 at +1×ATR(20), Leg 2 on the trail');
{
  const h = harness();
  const daily = buildCandles({ count: 120, start: 2000, step: 1 });
  seed(h.market, 'XAUUSD', 86_400, daily);
  const hourly = buildCandles({ count: 120, start: 2000, step: 1 });
  seed(h.market, 'XAUUSD', 3600, hourly);

  // Force a breakout so there is a position to manage.
  const chHigh = donchian(daily, 50).high;
  const slot = h.market.store.get(seriesKey('XAUUSD', 3600));
  slot.current.close = chHigh + 5;
  slot.lastUpdate = Date.now();
  await h.engine.evaluateEntry('XAUUSD');

  let pos = h.engine.openPositions()[0];
  check('position opened for the exit tests', Boolean(pos));
  const atrAtEntry = pos.atr;
  const entry = pos.entryPrice;
  check('the engine reports a usable ATR', atrAtEntry > 0, `ATR=${atrAtEntry?.toFixed(4)}`);

  /**
   * The exit loop is throttled to EXIT_THROTTLE_MS per symbol (it is driven by
   * the tick hot path), so tests must clear the throttle before each call.
   */
  const exits = (price) => {
    h.engine.symbolState('XAUUSD').lastExitCheckAt = 0;
    return h.engine.evaluateExits('XAUUSD', price);
  };

  /* ---- 4a. Leg 1 closes at +1 × ATR(20) -------------------------------- */
  const before1 = pos.volume;
  const target = entry + atrAtEntry * 1.0; // exactly +1 ATR
  const res1 = await exits(target);
  check('Leg 1 closes when profit reaches 1×ATR(20)', Boolean(res1), res1 ? `exit @ ${res1.exitPrice}` : 'no exit');
  check('Leg 1 is the 25% slice', res1 && near(res1.closedVolume, before1 * 0.25, 0.011), `${res1?.closedVolume} of ${before1}`);
  pos = h.engine.openPositions()[0];
  check('the runner stays open after Leg 1', Boolean(pos), `${pos?.volume} lots left`);
  check('Leg 1 is marked closed', pos.legs[0].closed === true);
  check('Leg 2 remains open', pos.legs[1].closed === false);
  check('a partial-close chat message was published', h.chat.some((c) => c.fn === 'partialClose'));

  // Just short of +1 ATR nothing must close.
  const hNothing = harness();
  seed(hNothing.market, 'XAUUSD', 86_400, buildCandles({ count: 120, start: 2000, step: 1 }));
  seed(hNothing.market, 'XAUUSD', 3600, buildCandles({ count: 120, start: 2000, step: 1 }));
  const nSlot = hNothing.market.store.get(seriesKey('XAUUSD', 3600));
  nSlot.current.close = donchian(hNothing.market.closedCandles('XAUUSD', 86_400), 50).high + 5;
  nSlot.lastUpdate = Date.now();
  await hNothing.engine.evaluateEntry('XAUUSD');
  const nPos = hNothing.engine.openPositions()[0];
  hNothing.engine.symbolState('XAUUSD').lastExitCheckAt = 0;
  const noExit = await hNothing.engine.evaluateExits('XAUUSD', nPos.entryPrice + nPos.atr * 0.95);
  check('just below +1×ATR nothing is closed', noExit === null && hNothing.engine.openPositions()[0].legs[0].closed === false);

  /* ---- 4b. Leg 2 has no fixed take profit ------------------------------ */
  const runner = h.engine.openPositions()[0];
  check('the runner carries no TP', h.engine.broker.listPositions()[0].tp === null);
  check('the runner was never assigned a take profit', runner.legs[1].purpose.includes('no fixed TP'));

  /* ---- 4c. the 20-day trailing stop ratchets upward ------------------- */
  // A Donchian low can only rise when an older bar leaves the window, so the
  // stop needs 20 fresh daily closes to move. That is the strategy working as
  // designed, not a bug — and the test proves it.
  const stopBefore = runner.stop;
  const rollPrice = stopBefore + 20;
  const d1Slot = h.market.store.get(seriesKey('XAUUSD', 86_400));
  const newDaily = buildCandles({
    count: 20,
    start: rollPrice,
    step: 0.5,
    interval: 86_400,
    timeStart: d1Slot.candles.at(-1).time + 86_400,
  });
  h.market.mergeHistory('XAUUSD', 86_400, newDaily);
  const nextBar = newDaily.at(-1);
  d1Slot.current = { time: nextBar.time + 86_400, open: nextBar.close, high: nextBar.close, low: nextBar.close, close: nextBar.close, tickVolume: 0 };
  d1Slot.lastUpdate = Date.now();

  const afterRoll = await exits(rollPrice + 1);
  const ratcheted = h.engine.openPositions()[0];
  check('the trailing stop only ever moves in favour', ratcheted.stop > stopBefore, `${stopBefore} → ${ratcheted.stop}`);
  check(
    'the ratcheted stop equals the engine\'s own 20-day channel low',
    near(ratcheted.stop, h.engine.trailingReference('XAUUSD').low, 1e-9),
    `${ratcheted.stop} = channel ${h.engine.trailingReference('XAUUSD').low}`
  );
  check('a trailing chat message was published', h.chat.some((c) => c.fn === 'trailing'));
  check('the ratchet did not close anything', afterRoll === null, afterRoll ? `closed ${afterRoll.closedVolume}` : 'nothing closed');
  check('the broker stop was actually moved', h.engine.broker.listPositions()[0].sl === ratcheted.stop, `broker sl ${h.engine.broker.listPositions()[0].sl}`);

  /* ---- 4d. trailing stop takes the runner out ------------------------- */
  const remaining = ratcheted.volume;
  const stopLevel = ratcheted.stop;
  const res2 = await exits(stopLevel - 0.01);
  check('the trailing stop closes the runner', Boolean(res2), res2 ? `exit @ ${res2.exitPrice}` : 'no exit');
  check('the whole remaining size was closed', res2 && near(res2.closedVolume, remaining, 0.011), `${res2?.closedVolume} of ${remaining}`);
  check('the position is fully closed', h.engine.openPositions().length === 0);
  check('a stop-hit chat message was published', h.chat.some((c) => c.fn === 'stopHit'));

  /* ---- 4e. exits never consume the daily quota ------------------------ */
  const q = await h.quota.usageFor('test-user', 'FREE');
  check('the entry used 1 execution', q.used === 1, `${q.used}/${q.limit}`);
  check('risk exits were never metered', h.quota.counters.exitsBypassed === 2, `${h.quota.counters.exitsBypassed} leg closures went through the free exit gate`);
}

/* =============================================================================
 *  5. FREEMIUM QUOTA
 * ============================================================================= */
section('5) Freemium quota — FREE 3/day, PRO unlimited');
{
  // ---- FREE user: exactly 3 automatic executions -------------------------
  const h = harness({ plan: 'FREE' });
  const daily = buildCandles({ count: 120, start: 2000, step: 1 });
  seed(h.market, 'XAUUSD', 86_400, daily);
  seed(h.market, 'XAUUSD', 3600, buildCandles({ count: 120, start: 2000, step: 1 }));
  const slot = h.market.store.get(seriesKey('XAUUSD', 3600));
  const chHigh = donchian(daily, 50).high;

  const decisions = [];
  for (let i = 0; i < 5; i++) {
    // Each iteration stands for a separate trading day. The entry cooldown is
    // what would otherwise legitimately block a second entry on the same
    // symbol, so simulate the passage of time rather than deleting state.
    h.engine.symbolState('XAUUSD').lastEntryAt = Date.now() - h.env.strategy.COOLDOWN_MS - 1;
    h.engine.positions.delete('XAUUSD');
    h.broker.positions.clear();
    slot.current.close = chHigh + 5 + i;
    slot.lastUpdate = Date.now();
    decisions.push(await h.engine.evaluateEntry('XAUUSD'));
  }

  const executed = decisions.filter((x) => x.action === 'EXECUTED');
  const signalOnly = decisions.filter((x) => x.action === 'SIGNAL_ONLY');
  check('FREE is executed exactly 3 times', executed.length === 3, `${executed.length} executions`);
  check('the 4th and 5th become SIGNAL_ONLY', signalOnly.length === 2, `${signalOnly.length} downgrades`);
  check('the downgrade reason is the daily quota', signalOnly.every((x) => x.reason === 'daily-quota-exhausted'), signalOnly[0]?.reason);
  check('no order reached the broker after the limit', h.broker.stats.placed === 3, `broker placed ${h.broker.stats.placed}`);
  check('a SIGNAL_ONLY chat message was published', h.chat.some((c) => c.fn === 'signalOnly'));
  check('a QUOTA chat message was published', h.chat.some((c) => c.fn === 'quota'));
  check('the signal was still published for the free user', h.chat.filter((c) => c.fn === 'signal').length === 5);

  const q = await h.quota.describe('test-user', 'FREE');
  check('the counter reports 3 used', q.used === 3, `${q.used}`);
  check('the counter reports exhausted', q.remaining === 0);
  check('the counter names the plan limit', q.limit === ENV.quota.FREE_DAILY_EXECUTIONS);

  // ---- the counter refuses further entries ------------------------------
  const verdict = await h.quota.canExecute('test-user', { plan: 'FREE', kind: 'ENTRY' });
  check('canExecute denies once exhausted', verdict.allowed === false && verdict.reason === 'daily-quota-exhausted');
  check('the denial carries a SIGNAL_ONLY downgrade payload', verdict.downgrade?.type === 'SIGNAL_ONLY');

  // ---- exits are always allowed ----------------------------------------
  const exitVerdict = await h.quota.canExecute('test-user', { plan: 'FREE', kind: 'EXIT' });
  check('an exhausted FREE user may still close a position', exitVerdict.allowed === true);

  // ---- PRO is unlimited -------------------------------------------------
  const hp = harness({ plan: 'PRO' });
  for (let i = 0; i < 12; i++) {
    await hp.quota.consume('test-user', { plan: 'PRO', kind: 'ENTRY' });
  }
  const pv = await hp.quota.canExecute('test-user', { plan: 'PRO', kind: 'ENTRY' });
  check('PRO is allowed after 12 executions', pv.allowed === true, `${pv.usage.used} today`);
  check('PRO reports unlimited', pv.usage.unlimited === true && pv.usage.limit === 0);
  check('PRO is allowed because the plan is unlimited', pv.reason === 'pro-unlimited', pv.reason);

  // ---- the day rolls over ----------------------------------------------
  const day = hp.store.dayKey(new Date('2026-03-01T23:59:59Z'));
  const day2 = hp.store.dayKey(new Date('2026-03-02T00:00:01Z'));
  check('the quota day advances at midnight UTC', day === '2026-03-01' && day2 === '2026-03-02', `${day} → ${day2}`);

  // ---- a different user has their own counter --------------------------
  await hp.quota.consume('other-user', { plan: 'FREE', kind: 'ENTRY' });
  await hp.quota.consume('other-user', { plan: 'FREE', kind: 'ENTRY' });
  const busy = await hp.quota.describe('other-user', 'FREE');
  const fresh = await hp.quota.describe('brand-new-user', 'FREE');
  check('counters are per user', busy.used === 2 && fresh.used === 0, `busy ${busy.used}, fresh ${fresh.used}`);
  check('one exhausted user does not block another', (await hp.quota.canExecute('brand-new-user', { plan: 'FREE' })).allowed === true);

  // ---- the rate limiter -------------------------------------------------
  const limiter = createRateLimiter({ windowMs: 60_000, max: 3, logger: { warn() {} } });
  const req = () => ({ ip: '10.0.0.1' });
  const results = [limiter.check(req()), limiter.check(req()), limiter.check(req()), limiter.check(req())];
  check('the rate limiter allows up to the limit', results.slice(0, 3).every((r) => r.allowed));
  check('the rate limiter blocks the request past the limit', results[3].allowed === false);
  check('a different IP is unaffected', limiter.check({ ip: '10.0.0.2' }).allowed === true);
  limiter.stop();
}

/* =============================================================================
 *  6. SECURITY — the client never instructs the server
 * ============================================================================= */
section('6) Security — the server decides when to trade');
{
  const { tradeGuard, apiManifest } = await import('../src/middleware/tradeGuard.js');

  // Minimal express-like doubles.
  const mkRes = () => {
    const res = {
      statusCode: 200,
      body: null,
      headers: {},
      status(c) { res.statusCode = c; return res; },
      json(b) { res.body = b; return res; },
      set(k, v) { res.headers[k] = v; return res; },
    };
    return res;
  };
  const mkReq = (method, path, { query = {}, body = {}, ip = '1.2.3.4' } = {}) => ({
    method,
    path,
    originalUrl: path,
    query: { ...query },
    body: { ...body },
    params: {},
    ip,
    headers: {},
    get: (h) => ({ authorization: req0.headers?.[h] })[h],
  });

  const run = (req) => {
    const res = mkRes();
    let nexted = false;
    tradeGuard({ logger: { warn() {} } })(req, res, () => { nexted = true; });
    return { res, nexted };
  };

  // A plain read request passes.
  check('a normal read request passes the guard', run(mkReq('GET', '/api/user/quota', { query: { symbol: 'XAUUSD' } })).nexted);

  // Execution parameters are rejected outright.
  const injected = mkReq('POST', '/api/user/anything', { body: { symbol: 'XAUUSD', side: 'BUY', volume: 5, sl: 1, tp: 2 } });
  const r1 = run(injected);
  check('a body carrying side/volume is rejected with 400', r1.res.statusCode === 400 && r1.nexted === false, r1.res.body?.error?.code);
  check('the rejection names the offending fields', /side/.test(r1.res.body?.error?.rejected?.join(',') ?? ''), r1.res.body?.error?.rejected?.join(','));

  const q2 = mkReq('GET', '/api/user/quota', { query: { lots: '10' } });
  const r2 = run(q2);
  check('execution parameters in the query string are rejected', r2.res.statusCode === 400 && r2.nexted === false, r2.res.body?.error?.code);

  const q3 = mkReq('GET', '/api/user/quota', { query: { nested: { autoExecute: true } } });
  check('nested execution parameters are rejected too', run(q3).res.statusCode === 400);

  const q4 = mkReq('GET', '/api/user/quota', { query: { SL: 1 } });
  check('parameter matching is case-insensitive', run(q4).res.statusCode === 400);

  // The guard also strips, so a careless handler downstream cannot read them.
  const strip = mkReq('POST', '/api/x', { body: { symbol: 'XAUUSD' } });
  run(strip);
  check('benign fields survive the sanitiser', strip.body.symbol === 'XAUUSD');

  // Order-shaped routes do not exist.
  check('POST /api/trade is blocked with 405', run(mkReq('POST', '/api/trade', { body: {} })).res.statusCode === 405);
  check('POST /order is blocked with 405', run(mkReq('POST', '/order', { body: {} })).res.statusCode === 405);
  check('GET /api/buy is blocked with 405', run(mkReq('GET', '/api/buy', { query: {} })).res.statusCode === 405);

  const manifest = apiManifest();
  check('the API manifest advertises server-autonomous execution', manifest.execution === 'server-autonomous');
  check('every manifest route is a GET', manifest.readOnlyRoutes.every((r) => r.startsWith('GET')), manifest.readOnlyRoutes.length + ' routes');

  // Auth: tokens are required on the user API and never trusted blindly.
  const store = new SupabaseService({ env: ENV });
  const auth = new AuthService({ supabase: store, env: { ...ENV, auth: { ...ENV.auth, DEV_TOKEN: '' } } });
  check('an absent token resolves to null', (await auth.resolveToken(null)) === null);
  check('a bogus token resolves to null (never trusted)', (await auth.resolveToken('not-a-jwt')) === null);
  const { tokenFromRequest } = await import('../src/middleware/auth.js');
  check('bearer header token is extracted', tokenFromRequest({ headers: { authorization: 'Bearer abc' } }) === 'abc');
  check('?access_token is extracted for EventSource', tokenFromRequest({ headers: {}, query: { access_token: 'xyz' } }) === 'xyz');
  check('x-access-token header is extracted', tokenFromRequest({ headers: { 'x-access-token': 'h' } }) === 'h');
  check('a missing token yields null', tokenFromRequest({ headers: {}, query: {} }) === null);

  // No route may import the broker. This is a static guarantee, checked here so
  // a future refactor cannot quietly wire an order endpoint up.
  const { readFileSync, readdirSync } = await import('node:fs');
  const { join } = await import('node:path');
  const routesDir = join(process.cwd(), 'src', 'routes');
  const offenders = readdirSync(routesDir)
    .filter((f) => f.endsWith('.js'))
    .filter((f) => {
      const src = readFileSync(join(routesDir, f), 'utf8');
      return /import[^;]*from\s*['"][^'"]*brokerApi/.test(src);
    });
  check('no route module imports the broker API', offenders.length === 0, offenders.join(', '));
}

/* =============================================================================
 *  7. ENGINE WARM-UP SAFETY
 * ============================================================================= */
section('7) Warm-up and fail-safe behaviour');
{
  const h = harness();
  seed(h.market, 'XAUUSD', 86_400, buildCandles({ count: 10, start: 2000, step: 1 })); // only 10 daily bars
  seed(h.market, 'XAUUSD', 3600, buildCandles({ count: 120, start: 2000, step: 1 }));

  check('the engine starts WARMING_UP', h.engine.phase === ENGINE_STATE.WARMING_UP, h.engine.phase);

  const d = await h.engine.evaluateEntry('XAUUSD');
  check('with 10 daily bars it refuses to trade', d.action === 'HOLD' && /need 50 daily candles/.test(d.reason), d.reason);
  check('the warm-up skip is counted', h.engine.counters.skippedNoHistory === 1);

  // Full history → arms.
  const h2 = harness();
  seed(h2.market, 'XAUUSD', 86_400, buildCandles({ count: 120, start: 2000, step: 1 }));
  seed(h2.market, 'XAUUSD', 3600, buildCandles({ count: 120, start: 2000, step: 1 }));
  const slot = h2.market.store.get(seriesKey('XAUUSD', 3600));
  slot.current.close = donchian(h2.market.closedCandles('XAUUSD', 86_400), 50).high + 5;
  slot.lastUpdate = Date.now();
  await h2.engine.evaluateEntry('XAUUSD');
  check('with full history the engine arms', h2.engine.phase === ENGINE_STATE.ARMED, h2.engine.phase);

  // Stale data must not open a position.
  const h3 = harness();
  seed(h3.market, 'XAUUSD', 86_400, buildCandles({ count: 120, start: 2000, step: 1 }));
  seed(h3.market, 'XAUUSD', 3600, buildCandles({ count: 120, start: 2000, step: 1 }));
  const s3 = h3.market.store.get(seriesKey('XAUUSD', 3600));
  s3.current.close = donchian(h3.market.closedCandles('XAUUSD', 86_400), 50).high + 5;
  // Freshness window for an H1 series is 3 hours, so go well past it.
  s3.lastUpdate = Date.now() - 4 * 3_600_000;
  const d3 = await h3.engine.evaluateEntry('XAUUSD');
  check('stale market data blocks an entry', d3.reason === 'market data is stale', d3.reason);

  // Execution disabled by configuration.
  const h4 = harness({ strategyOverrides: { EXECUTION_ENABLED: false } });
  seed(h4.market, 'XAUUSD', 86_400, buildCandles({ count: 120, start: 2000, step: 1 }));
  seed(h4.market, 'XAUUSD', 3600, buildCandles({ count: 120, start: 2000, step: 1 }));
  const s4 = h4.market.store.get(seriesKey('XAUUSD', 3600));
  s4.current.close = donchian(h4.market.closedCandles('XAUUSD', 86_400), 50).high + 5;
  s4.lastUpdate = Date.now();
  const d4 = await h4.engine.evaluateEntry('XAUUSD');
  check('a halted engine computes nothing', d4.action === 'HOLD' && /disabled/.test(d4.reason), d4.reason);
  check('the halted engine never calls the broker', h4.broker.stats.placed === 0);

  // Indicators exposed for observability.
  const st = h2.engine.status();
  check('status exposes per-symbol indicators', typeof st.indicators.XAUUSD.atr20 === 'number', `ATR ${st.indicators.XAUUSD.atr20?.toFixed(4)}`);
  check('status exposes the Donchian levels', st.indicators.XAUUSD.donchian50High > 0 && st.indicators.XAUUSD.donchian50Low > 0);
  check('status marks the symbol as having a position', st.indicators.XAUUSD.hasPosition === true);
}

/* ------------------------------------------------------------------ report */
if (failures.length) {
  console.log('\n  \x1b[31mFailures:\x1b[0m');
  for (const f of failures) console.log(`    X ${f}`);
}
console.log('\n' + '─'.repeat(58));
console.log(`  \x1b[32mPASSED: ${passed}\x1b[0m   ${failed ? `\x1b[31mFAILED: ${failed}\x1b[0m` : 'FAILED: 0'}`);
console.log('─'.repeat(58) + '\n');
process.exit(failed ? 1 : 0);