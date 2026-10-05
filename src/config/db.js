/* =============================================================================
 *  AETHERON CORE  —  src/config/db.js
 *  -----------------------------------------------------------------------------
 *  The symbol / timeframe registry and the tiny "is this input safe to use"
 *  gate that protects both the Supabase queries and the strategy maths.
 *
 *  WHY A REGISTRY INSTEAD OF A CONSTANT
 *  ------------------------------------
 *  Point size and contract size decide how many dollars one "point" of price
 *  move is worth, and that number is what turns an ATR into a lot size. It is
 *  instrument-specific and broker-specific, so it is data — not a guess buried
 *  in a formula. Every number the sizing maths touches comes from here.
 *
 *  Deriving lot size:
 *      value of 1 point for 1 lot = contractSize × pointSize
 *      LotSize = (Balance × Risk%) / (ATR(20) × contractSize × pointSize)
 *  For XAUUSD: 100 oz × 0.01 = $1.00 per point per lot.
 *  For EURUSD: 100 000 × 0.0001 = $10.00 per pip per lot.
 * ============================================================================= */

/**
 * Instrument metadata.
 *  digits          – decimal places used when talking to the broker
 *  pointSize       – price increment of one "point" (tick)
 *  contractSize    – units per 1.0 lot
 *  minLot/maxLot   – broker clamps applied after sizing
 *  lotStep         – volume granularity the broker accepts
 *  quoteCurrency   – used in chat messages
 */
export const SYMBOL_SPECS = Object.freeze({
  XAUUSD: {
    code: 'XAUUSD',
    display: 'XAUUSD',
    name: 'Gold / US Dollar',
    digits: 2,
    pointSize: 0.01,
    contractSize: 100, // 1 lot = 100 troy ounces
    minLot: 0.01,
    maxLot: 50,
    lotStep: 0.01,
    quoteCurrency: 'USD',
    // $ value of a 1.00 price move per 1.0 lot — handy for reporting
    valuePerPoint: 100,
  },
  EURUSD: {
    code: 'EURUSD',
    display: 'EURUSD',
    name: 'Euro / US Dollar',
    digits: 5,
    pointSize: 0.0001,
    contractSize: 100_000,
    minLot: 0.01,
    maxLot: 100,
    lotStep: 0.01,
    quoteCurrency: 'USD',
    valuePerPoint: 10,
  },
  USDJPY: {
    code: 'USDJPY',
    display: 'USDJPY',
    name: 'US Dollar / Japanese Yen',
    digits: 3,
    pointSize: 0.001,
    contractSize: 100_000,
    minLot: 0.01,
    maxLot: 100,
    lotStep: 0.01,
    quoteCurrency: 'JPY',
    valuePerPoint: 100, // 0.001 × 100 000 = ¥100 per point per lot
  },
  GBPUSD: {
    code: 'GBPUSD',
    display: 'GBPUSD',
    name: 'British Pound / US Dollar',
    digits: 5,
    pointSize: 0.0001,
    contractSize: 100_000,
    minLot: 0.01,
    maxLot: 100,
    lotStep: 0.01,
    quoteCurrency: 'USD',
    valuePerPoint: 10,
  },
});

/** Symbols the platform offers, in display order. */
export const SYMBOLS = Object.freeze(Object.keys(SYMBOL_SPECS));

/** Stable composite key used everywhere: `XAUUSD|86400`. */
export const seriesKey = (symbol, interval) => `${symbol}|${interval}`;

/** Split a series key back into its parts, or null when malformed. */
export function parseSeriesKey(key) {
  const m = /^([A-Z]{6})\|(\d+)$/.exec(String(key ?? ''));
  if (!m) return null;
  return { symbol: m[1], interval: Number(m[2]) };
}

/**
 * Supported timeframes. `api` is the upstream interval string; `seconds` is
 * the internal unit and the database key.
 *
 * D1 (86400) exists because the strategy is a daily-breakout system: its
 * 50-day Donchian channel and 20-day trailing stop are both daily constructs.
 */
export const TIMEFRAMES = Object.freeze([
  { seconds: 300, api: '5m', label: '5m', name: '5 Minutes', bucket: 'M5' },
  { seconds: 900, api: '15m', label: '15m', name: '15 Minutes', bucket: 'M15' },
  { seconds: 3600, api: '1h', label: 'H1', name: '1 Hour', bucket: 'H1' },
  { seconds: 14400, api: '4h', label: '4h', name: '4 Hours', bucket: 'H4' },
  { seconds: 86_400, api: '1d', label: 'D1', name: '1 Day', bucket: 'D1' },
]);

/** Every series the engine maintains, pre-declared. */
export const SERIES = Object.freeze(
  SYMBOLS.flatMap((symbol) =>
    TIMEFRAMES.map((tf) => {
      const spec = SYMBOL_SPECS[symbol];
      return Object.freeze({
        key: seriesKey(symbol, tf.seconds),
        symbol,
        display: spec.display,
        pairName: spec.name,
        interval: tf.seconds,
        intervalApi: tf.api,
        intervalLabel: tf.label,
        intervalName: tf.name,
        bucket: tf.bucket,
      });
    })
  )
);

/** Fast lookup: `XAUUSD|300` → series descriptor. */
export const SERIES_BY_KEY = Object.freeze(
  Object.fromEntries(SERIES.map((s) => [s.key, s]))
);

/* ------------------------------------------------------- validation helpers */

/**
 * Symbols arrive from query strings and (potentially hostile) clients. Every
 * one of them is matched against this allow-list before it reaches a database
 * filter or a broker payload — that is the whole point of this section.
 */
export function normaliseSymbol(input) {
  const s = String(input ?? '').trim().toUpperCase();
  return Object.prototype.hasOwnProperty.call(SYMBOL_SPECS, s) ? s : null;
}

export function isValidSymbol(input) {
  return normaliseSymbol(input) !== null;
}

/** Timeframes must be an exact member of the registry (no free-form SQL). */
export function normaliseInterval(input) {
  const n = Number(input);
  if (!Number.isFinite(n)) return null;
  return TIMEFRAMES.some((tf) => tf.seconds === n) ? n : null;
}

export function isValidInterval(input) {
  return normaliseInterval(input) !== null;
}

/** Validate a symbol/timeframe pair in one call. */
export function normaliseSeries(input = {}) {
  const symbol = normaliseSymbol(input.symbol);
  const interval = normaliseInterval(input.inter ?? input.timeframe ?? input.interval);
  if (!symbol || interval === null) return null;
  return SERIES_BY_KEY[seriesKey(symbol, interval)] ?? null;
}

/** Full descriptor for a validated symbol, or null. */
export function getSymbolSpec(input) {
  const symbol = normaliseSymbol(input);
  return symbol ? SYMBOL_SPECS[symbol] : null;
}

/**
 * Direction normalisation.
 *
 * Two vocabularies meet in this system:
 *   • the STRATEGY reasons in LONG / SHORT, because that is how a breakout is
 *     described ("price broke the 50-day high ⇒ long");
 *   • the BROKER speaks BUY / SELL, because that is what MT5 order tickets use.
 *
 * Mixing them up silently opens a position in the opposite direction, so the
 * translation lives here and is applied at the broker boundary only.
 */
export function normaliseSide(input) {
  const s = String(input ?? '').trim().toUpperCase();
  if (s === 'BUY' || s === 'LONG') return 'BUY';
  if (s === 'SELL' || s === 'SHORT') return 'SELL';
  return null;
}

/** Inverse: what a chat message should say. */
export function sideLabel(input) {
  const s = normaliseSide(input);
  return s === 'BUY' ? 'BUY' : s === 'SELL' ? 'SELL' : String(input ?? '?').toUpperCase();
}

/** True when the direction profits from a rising price. */
export const isLong = (side) => normaliseSide(side) === 'BUY';

/** Round a price to the instrument's tick precision. */
export function roundPrice(symbol, price) {
  const spec = getSymbolSpec(symbol);
  if (!spec || !Number.isFinite(price)) return price;
  const f = 10 ** spec.digits;
  return Math.round(price * f) / f;
}

/**
 * Round a volume down onto the broker's lot step and clamp it into
 * [minLot, maxLot]. Returns 0 when the position is too small to trade — the
 * engine treats 0 as "do not trade" rather than sending an invalid order.
 */
export function normaliseLot(symbol, lot) {
  const spec = getSymbolSpec(symbol);
  if (!spec || !Number.isFinite(lot) || lot <= 0) return 0;
  const stepped = Math.floor(lot / spec.lotStep) * spec.lotStep;
  // Re-round to kill binary floating point dust (0.01 * 3 = 0.030000000000000002)
  const decimals = Math.max(0, Math.ceil(-Math.log10(spec.lotStep)));
  const rounded = Number(stepped.toFixed(decimals));
  if (rounded < spec.minLot) return 0;
  return Number(Math.min(rounded, spec.maxLot).toFixed(decimals));
}

/**
 * Money value of a price MOVE for `lots`, in the quote currency.
 *
 *   value = priceDelta × contractSize × lots
 *
 * `priceDelta` is a price difference (e.g. 3.00 on gold), NOT a point count.
 * Do not multiply by pointSize here: this function already converts a price
 * difference into money, and doing it twice understates P&L by a factor of
 * pointSize (100× for EURUSD). To go the other way — price delta → points —
 * divide by pointSize explicitly.
 *
 * Gold:  3.00 × 100 × 1.0 lot      = $300
 * EURUSD: 0.0018 × 100,000 × 0.55 = $99
 */
export function pointsValue(symbol, priceDelta, lots) {
  const spec = getSymbolSpec(symbol);
  if (!spec) return 0;
  if (!Number.isFinite(priceDelta) || !Number.isFinite(lots)) return 0;
  return priceDelta * spec.contractSize * lots;
}

export default {
  SYMBOL_SPECS,
  SYMBOLS,
  TIMEFRAMES,
  SERIES,
  SERIES_BY_KEY,
  seriesKey,
  parseSeriesKey,
  normaliseSymbol,
  normaliseInterval,
  normaliseSeries,
  getSymbolSpec,
  normaliseSide,
  sideLabel,
  isLong,
  roundPrice,
  normaliseLot,
  pointsValue,
};