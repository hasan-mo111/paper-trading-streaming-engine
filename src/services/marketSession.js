/* =============================================================================
 *  AETHERON CORE  —  src/services/marketSession.js
 *  -----------------------------------------------------------------------------
 *  The weekly FX / metals session, in UTC.
 *
 *  WHY
 *  ---
 *  The feed keeps a socket open over the weekend, and some brokers replay the
 *  last quote or answer REST history with synthetic flat bars. Folding those
 *  bars into the ring puts fake candles on the chart and teaches the AI that the
 *  market traded in a straight line for two days. Neither is true.
 *
 *  Spot FX/metals open Sunday 21:00 UTC while the US is on daylight saving and
 *  22:00 UTC otherwise, and close at the same hour on Friday. A bar stamped
 *  inside that closed window did not come from a real session.
 *
 *  The predicate is deliberately conservative about the DST edge: one hour of
 *  legitimate Sunday-evening trading can be dropped in the changeover weeks.
 *  Keeping a synthetic flat candle in the AI prompt is the worse error.
 * ============================================================================= */

const HOUR_MS = 3_600_000;

/**
 * Test-only seam for the end-to-end harness. Unit tests exercise the real
 * weekend edges; the E2E suite needs the feed pipeline to run on any calendar
 * day, so it sets this flag in the spawned server. Production ignores it even
 * if someone exports it.
 */
const TEST_ALWAYS_OPEN =
  process.env.NODE_ENV === 'test' &&
  /^(1|true|open)$/i.test(process.env.MARKET_HOURS_OVERRIDE ?? '');

/** UTC timestamp of the nth Sunday of a month (n is 1-based). */
function nthSundayUtc(year, month, n) {
  const firstDow = new Date(Date.UTC(year, month, 1)).getUTCDay();
  const day = 1 + ((7 - firstDow) % 7) + (n - 1) * 7;
  return Date.UTC(year, month, day);
}

/** True while the US is on daylight saving (2nd Sun Mar 07:00 UTC → 1st Sun Nov 06:00 UTC). */
export function isUsDst(ms = Date.now()) {
  const year = new Date(ms).getUTCFullYear();
  const start = nthSundayUtc(year, 2, 2) + 7 * HOUR_MS;
  const end = nthSundayUtc(year, 10, 1) + 6 * HOUR_MS;
  return ms >= start && ms < end;
}

/**
 * UTC hour at which the weekly session opens on Sunday and closes on Friday.
 * 21:00 during US daylight saving, 22:00 during standard time.
 */
export function weeklySessionEdgeHour(ms = Date.now()) {
  return isUsDst(ms) ? 21 : 22;
}

/**
 * Does this bar timestamp fall inside the weekly close?
 *
 *   • Saturday                        → closed
 *   • Sunday before the opening hour  → closed
 *   • Friday at/after the closing hour → closed
 *
 * @param {number} timeSec epoch seconds (the bar's open time)
 * @returns {boolean}
 */
export function isWeekendBar(timeSec) {
  if (TEST_ALWAYS_OPEN) return false;
  const ms = Number(timeSec) * 1000;
  if (!Number.isFinite(ms)) return false;
  const d = new Date(ms);
  const day = d.getUTCDay();
  const hour = d.getUTCHours();
  const edge = weeklySessionEdgeHour(ms);

  if (day === 6) return true;
  if (day === 0 && hour < edge) return true;
  if (day === 5 && hour >= edge) return true;
  return false;
}

/**
 * Drop synthetic weekend bars from a candle list. Order is preserved and the
 * input array itself is never mutated.
 *
 * @param {Array<{time:number}>} candles
 * @returns {Array}
 */
export function filterTradableBars(candles) {
  if (!Array.isArray(candles)) return [];
  return candles.filter((c) => c && !isWeekendBar(c.time));
}

/**
 * Is the FX/metals market open right now?
 *
 * `isWeekendBar` is intentionally reused: the weekly close is the same window
 * for bars and for placing a trade, so there is only one source of truth for
 * "the weekend". Callers that evaluate entries or publish new trade ideas use
 * this predicate; exits keep running because a stop must never be abandoned.
 *
 * @param {number} ms epoch milliseconds
 * @returns {boolean}
 */
export function isMarketOpen(ms = Date.now()) {
  return !isWeekendBar(Math.floor(Number(ms) / 1000));
}

/**
 * First instant at/after `ms` at which the market reopens.
 *
 * Spot FX/metals reopen on Sunday at 21:00 UTC (DST) or 22:00 UTC (standard).
 * The search walks the UTC calendar rather than assuming a fixed day offset, so
 * it remains correct across DST change weekends.
 *
 * @param {number} ms epoch milliseconds
 * @returns {number} epoch milliseconds of the next open (or `ms` when open)
 */
export function nextMarketOpenAt(ms = Date.now()) {
  if (isMarketOpen(ms)) return Number(ms);

  let cursor = Math.floor(Number(ms) / HOUR_MS) * HOUR_MS;
  for (let i = 0; i < 24 * 9; i += 1) {
    const d = new Date(cursor);
    if (d.getUTCDay() === 0) {
      const edge = weeklySessionEdgeHour(cursor);
      const open = Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate(), edge, 0, 0, 0);
      if (open >= cursor && isMarketOpen(open)) return open;
    }
    cursor += HOUR_MS;
  }

  // Defensive fallback: never return a time in the past or return NaN.
  return Number(ms) + 7 * 24 * HOUR_MS;
}

export default {
  isUsDst,
  weeklySessionEdgeHour,
  isWeekendBar,
  filterTradableBars,
  isMarketOpen,
  nextMarketOpenAt,
};
