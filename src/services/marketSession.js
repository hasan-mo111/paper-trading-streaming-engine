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

export default { isUsDst, weeklySessionEdgeHour, isWeekendBar, filterTradableBars };
