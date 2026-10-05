/* =============================================================================
 *  AETHERON CORE  —  src/engine/indicatorCalc.js
 *  -----------------------------------------------------------------------------
 *  Pure functions. No I/O, no state, no side effects — which makes the whole
 *  quant layer trivially unit-testable (see test/engine.test.mjs).
 *
 *  THE TWO INDICATORS THAT DEFINE "AETHERON UNIVERSAL"
 *  --------------------------------------------------
 *  • ATR(20)          volatility. Drives the lot size and the Leg-1 take profit.
 *  • Donchian(N)      the highest high / lowest low of the last N candles.
 *                     N=50 → the entry breakout channel.
 *                     N=20 → the trailing stop reference.
 *
 *  A NOTE ON WHICH CANDLES COUNT
 *  -----------------------------
 *  For the ENTRY channel the forming candle must be excluded, otherwise the
 *  current bar's own high is part of "the high we must break" and price can
 *  never exceed it. `donchian()` therefore takes already-closed candles only,
 *  and callers use `closedCandles()` (which excludes the forming bar) for the
 *  breakout test. The trailing stop does the opposite — it must follow the live
 *  bar — so `trailingChannel()` folds the forming candle in.
 * =============================================================================
 */

import { getSymbolSpec } from '../config/db.js';

/** A candle is usable when time/OHLC are finite and the bar is well formed. */
export function isValidCandle(c) {
  return Boolean(
    c &&
      Number.isFinite(c.time) &&
      Number.isFinite(c.open) &&
      Number.isFinite(c.high) &&
      Number.isFinite(c.low) &&
      Number.isFinite(c.close) &&
      c.high >= c.low
  );
}

/** Drop unusable rows, sort ascending by time, de-duplicate on time. */
export function normaliseSeries(candles) {
  const byTime = new Map();
  for (const c of candles ?? []) {
    if (isValidCandle(c)) byTime.set(c.time, c);
  }
  return [...byTime.values()].sort((a, b) => a.time - b.time);
}

export const last = (candles) => (candles?.length ? candles[candles.length - 1] : null);

export const closes = (candles) => (candles ?? []).map((c) => c.close);

/* ------------------------------------------------------------------ averages */

/** Simple moving average of the last `period` closes. */
export function sma(values, period) {
  if (!Array.isArray(values) || values.length < period || period <= 0) return null;
  let sum = 0;
  for (let i = values.length - period; i < values.length; i++) sum += values[i];
  return sum / period;
}

/** Exponential moving average, seeded with the first `period` values. */
export function ema(values, period) {
  if (!Array.isArray(values) || values.length < period || period <= 0) return null;
  const k = 2 / (period + 1);
  let avg = 0;
  for (let i = 0; i < period; i++) avg += values[i];
  avg /= period;
  for (let i = period; i < values.length; i++) avg = values[i] * k + avg * (1 - k);
  return avg;
}

/* ------------------------------------------------------------ volatility --- */

/** True Range of one candle. */
export function trueRange(current, previous) {
  if (!isValidCandle(current)) return null;
  const prevClose = previous && Number.isFinite(previous.close) ? previous.close : current.close;
  return Math.max(
    current.high - current.low,
    Math.abs(current.high - prevClose),
    Math.abs(current.low - prevClose)
  );
}

/**
 * Wilder's ATR — the industry standard smoothing, not a plain SMA of TR.
 *
 * Requires `period + 1` candles (one extra for the first TR, which needs the
 * previous close). Returns null rather than a short-sample guess: sizing a
 * position off an unstable ATR is worse than not trading.
 *
 * @param {Array<object>} candles oldest → newest, closed candles
 * @param {number} period
 * @returns {number|null}
 */
export function atr(candles, period = 20) {
  const series = normaliseSeries(candles);
  if (series.length < period + 1 || period < 1) return null;

  const trs = [];
  for (let i = 1; i < series.length; i++) {
    const tr = trueRange(series[i], series[i - 1]);
    if (tr == null) return null;
    trs.push(tr);
  }
  if (trs.length < period) return null;

  // Wilder seed = simple average of the first `period` true ranges
  let value = 0;
  for (let i = 0; i < period; i++) value += trs[i];
  value /= period;
  // Wilder smoothing for the remainder
  for (let i = period; i < trs.length; i++) value = (value * (period - 1) + trs[i]) / period;

  return Number.isFinite(value) && value > 0 ? value : null;
}

/** Population standard deviation of the last `period` values. */
export function stdev(values, period = 20) {
  if (!Array.isArray(values) || values.length < period || period < 2) return null;
  const slice = values.slice(-period);
  const mean = slice.reduce((a, b) => a + b, 0) / slice.length;
  const variance = slice.reduce((a, b) => a + (b - mean) ** 2, 0) / slice.length;
  return Math.sqrt(variance);
}

/* ---------------------------------------------------------------- Donchian -- */

/**
 * Donchian channel over the last `period` candles.
 *
 * @returns {{high:number, low:number, from:number, to:number, count:number}|null}
 *          null when there is not enough history — a breakout must never be
 *          evaluated against a partial channel.
 */
export function donchian(candles, period = 50) {
  const series = normaliseSeries(candles);
  if (series.length < period || period < 1) return null;

  const window = series.slice(-period);
  let high = -Infinity;
  let low = Infinity;
  for (const c of window) {
    if (c.high > high) high = c.high;
    if (c.low < low) low = c.low;
  }
  if (!Number.isFinite(high) || !Number.isFinite(low)) return null;

  return { high, low, from: window[0].time, to: window[window.length - 1].time, count: period };
}

/**
 * Trailing reference channel: the last `period` candles INCLUDING the live one.
 * This is what lets the stop ratchet with price instead of waiting for a close.
 *
 * When there is no live bar yet (fresh boot, weekend), the window falls back to
 * the last `period` CLOSED candles so the trailing stop still has a reference.
 */
export function trailingChannel(closedCandles, formingCandle, period = 20) {
  const series = normaliseSeries(closedCandles);
  const hasForming = Boolean(formingCandle) && isValidCandle(formingCandle);

  // Reserve one slot for the live bar only when it exists.
  const window = hasForming ? series.slice(-(period - 1)) : series.slice(-period);
  const bars = hasForming ? [...window, formingCandle] : window;
  if (bars.length < period) return null;

  let high = -Infinity;
  let low = Infinity;
  for (const c of bars) {
    if (c.high > high) high = c.high;
    if (c.low < low) low = c.low;
  }
  return Number.isFinite(high) && Number.isFinite(low) ? { high, low, count: period } : null;
}

/* ----------------------------------------------------------------- helpers -- */

/**
 * Breakout verdict against a Donchian channel.
 *
 * `confirmPct` requires the close to clear the level by a small buffer, which
 * filters the constant false breakouts produced by markets grinding along a
 * channel boundary.
 */
export function breakoutSignal(price, channel, { side, confirmPct = 0 } = {}) {
  if (!channel || !Number.isFinite(price) || !Number.isFinite(channel.high)) return 'NONE';

  const buffer = Math.abs(price) * confirmPct;
  if (price > channel.high + buffer) return side === 'SHORT' ? 'NONE' : 'LONG';
  if (price < channel.low - buffer) return side === 'LONG' ? 'NONE' : 'SHORT';
  return 'NONE';
}

/** Clamp a value into [min, max]; returns min when the bound is inverted. */
export function clamp(value, min, max) {
  if (min > max) return min;
  return Math.min(max, Math.max(min, value));
}

/** Round a number down onto a step grid (lot sizes). */
export function roundToStep(value, step) {
  if (!Number.isFinite(value) || !Number.isFinite(step) || step <= 0) return value;
  const decimals = Math.max(0, Math.ceil(-Math.log10(step)));
  return Number((Math.floor(value / step) * step).toFixed(decimals));
}

/**
 * Profit in price units for a position, from the broker's point of view.
 * Accepts either vocabulary (BUY/SELL or LONG/SHORT); longs profit when price
 * rises, shorts when it falls.
 */
export function profitPoints(side, entryPrice, exitPrice) {
  if (!Number.isFinite(entryPrice) || !Number.isFinite(exitPrice)) return 0;
  const s = String(side ?? '').toUpperCase();
  const long = s === 'BUY' || s === 'LONG';
  const short = s === 'SELL' || s === 'SHORT';
  if (!long && !short) return 0;
  return long ? exitPrice - entryPrice : entryPrice - exitPrice;
}

/** Convert price units into points for an instrument (UI-friendly). */
export function toPoints(symbol, priceDelta) {
  const spec = getSymbolSpec(symbol);
  if (!spec?.pointSize) return 0;
  return priceDelta / spec.pointSize;
}

/** Format a price with the instrument's precision. */
export function formatPrice(symbol, value, fallbackDigits = 2) {
  const spec = getSymbolSpec(symbol);
  const digits = spec?.digits ?? fallbackDigits;
  return Number.isFinite(value) ? value.toFixed(digits) : '—';
}