/* =============================================================================
 *  AETHERON CORE  —  src/services/marketAggregator.js
 *  -----------------------------------------------------------------------------
 *  The candle store. For every series (pair × timeframe) we keep:
 *    • `candles` — the last MAX_CANDLES **closed** candles, oldest → newest
 *    • `current` — the single *forming* candle, mutated live by incoming ticks
 *
 *  Two responsibilities were added during the Aetheron refactor:
 *
 *  1. DB HYDRATION — `hydrate()` seeds a series from Supabase at boot so the
 *     50-day Donchian channel survives a restart. `mergeHistory()` then folds
 *     the upstream REST history into whatever was restored, without creating
 *     duplicates or holes.
 *
 *  2. WRITE-BEHIND PERSISTENCE — every closed candle (and the forming candle on
 *     a slower cadence) is handed to the Supabase service. The aggregator never
 *     awaits a write: the tick hot path stays allocation-light and lock-free.
 * ============================================================================= */

import { ENV } from '../config/env.js';
import { SERIES, TIMEFRAMES, seriesKey, getSymbolSpec } from '../config/db.js';
import { createLogger } from './logger.js';
import { fetchWithTimeout } from './websocket.js';

const log = createLogger('market');

/** Upstream occasionally sends numbers as strings. */
const toNum = (v) => (v == null || v === '' ? NaN : typeof v === 'number' ? v : parseFloat(v));

/** Floor an epoch (seconds) onto the timeframe grid. */
export const floorEpoch = (epochSeconds, interval) => Math.floor(epochSeconds / interval) * interval;

export class MarketAggregator {
  /**
   * @param {object} [opts]
   * @param {number} [opts.maxCandles]  ring size per series
   * @param {object} [opts.env]
   * @param {SupabaseService} [opts.persistence] persistence target (optional)
   * @param {(ev:object)=>void} [opts.onClosed]    fired once per closed candle
   * @param {(ev:object)=>void} [opts.onFormingTick] fired on the forming candle
   */
  constructor({
    maxCandles = ENV.market.MAX_CANDLES,
    env = ENV,
    persistence = null,
    onClosed = () => {},
    onFormingTick = () => {},
  } = {}) {
    this.maxCandles = maxCandles;
    this.env = env;
    /** NOTE: `store` is the slot map (kept for API compatibility). The
     *  persistence target is deliberately named `persistence` so the two can
     *  never be confused. */
    this.persistence = persistence;
    this.onClosed = onClosed;
    this.onFormingTick = onFormingTick;

    /** @type {Map<string, object>} seriesKey → slot */
    this.store = new Map(
      SERIES.map((s) => [
        s.key,
        {
          ...s,
          candles: [], // closed candles, oldest first, capped at maxCandles
          current: null, // the forming candle
          lastUpdate: 0, // ms epoch of the last accepted live update
          ready: false, // true once history is available (from DB or upstream)
          restored: false, // true when the ring was seeded from Supabase
          tickCount: 0,
          closedCount: 0,
        },
      ])
    );

    /** Coalesced changes awaiting the next SSE flush, keyed by series. */
    this.pending = new Map();

    /** Last persisted `current` epoch per series, so we do not re-write it. */
    this.lastFormingWrite = new Map();
  }

  /* ======================================================================== */
  /*  HISTORY — Supabase hydration                                            */
  /* ======================================================================== */

  /**
   * Seed a series from persisted candles. Safe to call before the upstream
   * backfill: `mergeHistory()` will reconcile the two afterwards.
   *
   * @returns {number} how many closed candles are now in the ring
   */
  hydrate(symbol, interval, candles, { source = 'db' } = {}) {
    const slot = this.store.get(seriesKey(symbol, interval));
    if (!slot) return 0;

    const clean = (candles ?? [])
      .filter((c) => c && Number.isFinite(c.time) && Number.isFinite(c.close) && Number.isFinite(c.high) && Number.isFinite(c.low))
      .map((c) => ({
        time: Math.floor(c.time),
        open: Number.isFinite(c.open) ? c.open : c.close,
        high: c.high,
        low: c.low,
        close: c.close,
        tickVolume: Number(c.tickVolume ?? 0) || 0,
      }))
      .sort((a, b) => a.time - b.time);

    slot.candles = clean.slice(-this.maxCandles);
    if (slot.candles.length > 0) {
      slot.ready = true;
      slot.restored = true;
      slot.restoredFrom = source;
    }

    // A forming candle must always sit exactly one interval after the newest
    // closed one; without it the strategy would treat the gap as a data error.
    if (slot.candles.length > 0) {
      const lastClosed = slot.candles.at(-1);
      const expected = lastClosed.time + slot.interval;
      const now = floorEpoch(Date.now() / 1000, slot.interval);
      if (!slot.current || slot.current.time < expected) {
        // Do NOT fabricate a forming bar when the newest closed bar is already
        // the current one (e.g. warm start in the middle of a session).
        const startAt = Math.max(expected, now);
        if (startAt === expected) {
          slot.current = {
            time: expected,
            open: lastClosed.close,
            high: lastClosed.close,
            low: lastClosed.close,
            close: lastClosed.close,
            tickVolume: 0,
          };
        }
      }
    }

    this.markDirty(slot.key);
    return slot.candles.length;
  }

  /**
   * Fold upstream REST history into the ring.
   *
   * Reconciliation rules:
   *   • rows already present are updated (upstream is authoritative on OHLC);
   *   • restored rows that upstream has not reached yet are kept — a cold start
   *     must not throw away the 50-day warm-up just because REST is slow;
   *   • the result is de-duplicated and kept at exactly maxCandles.
   */
  mergeHistory(symbol, interval, upstreamCandles) {
    const slot = this.store.get(seriesKey(symbol, interval));
    if (!slot) return 0;

    const byTime = new Map();
    for (const c of slot.candles) byTime.set(c.time, c); // restored rows first
    for (const c of upstreamCandles) {
      if (!c || !Number.isFinite(c.time) || !Number.isFinite(c.close)) continue;
      byTime.set(c.time, {
        time: c.time,
        open: Number.isFinite(c.open) ? c.open : c.close,
        high: Number.isFinite(c.high) ? c.high : c.close,
        low: Number.isFinite(c.low) ? c.low : c.close,
        close: c.close,
        tickVolume: Number(c.tickVolume ?? 0) || 0,
      });
    }

    slot.candles = [...byTime.values()].sort((a, b) => a.time - b.time).slice(-this.maxCandles);
    slot.ready = true;
    if (!this.store.get(seriesKey(symbol, interval))?.restored) slot.restored = false;
    this.markDirty(slot.key);
    return slot.candles.length;
  }

  /**
   * Load warm-up history from Supabase.
   * D1 is requested first with the largest window because the strategy's
   * 50-day channel is a daily construct; intraday series follow with the ring
   * size so the chart is populated before the first tick lands.
   *
   * @returns {Promise<{series:number, candles:number}>}
   */
  async hydrateFromDatabase(persistence, { dailyLimit, allLimit } = {}) {
    const db = persistence ?? this.persistence;
    if (!db?.loadRecentCandlesMany) return { series: 0, candles: 0, ms: 0 };

    const dailyLimitN = dailyLimit ?? this.env.supabase.WARMUP_DAILY_CANDLES;
    const allLimitN = allLimit ?? this.maxCandles;

    // Daily first — it is the critical path for the strategy.
    const requests = [];
    for (const symbol of new Set(SERIES.map((s) => s.symbol))) {
      requests.push({ symbol, interval: 86_400, limit: dailyLimitN });
    }
    for (const series of SERIES) {
      if (series.interval === 86_400) continue;
      requests.push({ symbol: series.symbol, interval: series.interval, limit: allLimitN });
    }

    log.info(`rehydrating ${requests.length} series from Supabase (D1 limit ${dailyLimitN})…`);
    const t0 = Date.now();
    const loaded = await db.loadRecentCandlesMany(requests);

    let candles = 0;
    let series = 0;
    for (const [key, list] of loaded) {
      const [symbol, intervalStr] = key.split('|');
      const n = this.hydrate(symbol, Number(intervalStr), list, { source: 'supabase' });
      if (n > 0) {
        series += 1;
        candles += n;
      }
    }

    log.ok(
      `hydrated ${series}/${requests.length} series (${candles} candles) in ${Date.now() - t0}ms` +
        (series === 0 ? ' — upstream backfill will supply everything' : '')
    );
    return { series, candles, ms: Date.now() - t0 };
  }

  /* ======================================================================== */
  /*  HISTORY — upstream REST backfill                                        */
  /* ======================================================================== */

  /**
   * Pull history for one series: `GET {API}/api/{symbol}/ohlc?interval=&limit=`
   * biquote returns bars newest-first and flags the forming bar with
   * `isOpen: true`, so the interval needs no inference.
   */
  async backfill(slotOrKey) {
    const slot = typeof slotOrKey === 'string' ? this.store.get(slotOrKey) : slotOrKey;
    if (!slot) throw new Error(`unknown series ${slotOrKey}`);

    const url =
      `${this.env.upstream.API}/api/${slot.symbol}/ohlc` +
      `?interval=${encodeURIComponent(slot.intervalApi)}&limit=${this.maxCandles + 1}`;

    const res = await fetchWithTimeout(url, this.env.upstream.REST_TIMEOUT_MS);
    if (!res.ok) throw new Error(`HTTP ${res.status} ${(await res.text()).slice(0, 120)}`);

    const body = await res.json();
    const bars = Array.isArray(body?.bars) ? body.bars : [];
    if (bars.length === 0) throw new Error(`no bars returned for ${slot.symbol}`);

    if (body.interval && body.interval !== slot.intervalApi) {
      log.warn(`${slot.key}: upstream used interval "${body.interval}", expected "${slot.intervalApi}"`);
    }

    const toCandle = (b) => ({
      time: Math.floor(new Date(b.openTime).getTime() / 1000),
      open: toNum(b.open),
      high: toNum(b.high),
      low: toNum(b.low),
      close: toNum(b.close),
      tickVolume: toNum(b.tickVolume ?? b.volume ?? 0) || 0,
    });

    const formingRaw = bars.find((b) => b.isOpen) ?? bars[0];
    const closed = bars
      .filter((b) => b !== formingRaw)
      .map(toCandle)
      .filter((c) => Number.isFinite(c.time) && Number.isFinite(c.close))
      .sort((a, b) => a.time - b.time);

    const merged = this.mergeHistory(slot.symbol, slot.interval, closed);

    // The forming candle comes from upstream — it is the live one. Only adopt
    // it when it is newer than whatever we had, so a stale REST snapshot
    // cannot rewind a live feed.
    const forming = toCandle(formingRaw);
    if (Number.isFinite(forming.time) && Number.isFinite(forming.close)) {
      if (!slot.current || forming.time >= slot.current.time) {
        slot.current = forming;
      } else if (slot.current.time === forming.time) {
        slot.current.high = Math.max(slot.current.high, forming.high);
        slot.current.low = Math.min(slot.current.low, forming.low);
      }
    }

    slot.ready = true;
    slot.lastUpdate = Date.now();
    slot.tickCount = 0;
    this.markDirty(slot.key);

    log.debug(
      `${slot.key.padEnd(12)} ${merged} closed + forming @ ${slot.current?.close ?? '—'}`
    );
    return merged;
  }

  /** Backfill every declared series in small parallel batches. */
  async backfillAll({ batch = 4 } = {}) {
    const keys = [...this.store.keys()];
    let ok = 0;
    let failed = 0;

    for (let i = 0; i < keys.length; i += batch) {
      const slice = keys.slice(i, i + batch);
      const results = await Promise.allSettled(
        slice.map((key) => this.backfill(key))
      );
      results.forEach((r, n) => {
        if (r.status === 'rejected') {
          failed += 1;
          log.error(`backfill failed for ${slice[n]}:`, r.reason?.message);
        } else {
          ok += 1;
        }
      });
    }

    this.markAllDirty();
    log.ok(`backfill complete — ${ok} ok, ${failed} failed`);
    return { ok, failed, total: keys.length };
  }

  /* ======================================================================== */
  /*  LIVE TICK PATH (hot path — keep this allocation-light)                  */
  /* ======================================================================== */

  /**
   * Fold one live tick into every timeframe of its symbol.
   *
   *  • tick newer than the current candle → close it (or gap-fill) and open a new one
   *  • tick inside the current candle  → mutate close/high/low in place
   *  • tick older than the current candle → stale, ignored
   *
   * @returns {boolean} true when the tick produced a state change
   */
  applyTick(symbol, tick) {
    const price = toNum(tick.mid ?? tick.ask ?? tick.bid);
    const epochSec = tick.timestamp ? new Date(tick.timestamp).getTime() / 1000 : Date.now() / 1000;
    if (!Number.isFinite(price) || !Number.isFinite(epochSec)) return false;

    let changed = false;

    for (const slot of this.store.values()) {
      if (slot.symbol !== symbol) continue;

      const epoch = floorEpoch(epochSec, slot.interval);

      if (!slot.current) {
        slot.current = { time: epoch, open: price, high: price, low: price, close: price, tickVolume: 1 };
        this.#markDirty(slot, null, true);
        changed = true;
      } else if (epoch > slot.current.time) {
        /* -------- ROLLOVER: close the previous candle, open a new one ------- */
        let closed = null;
        if (epoch === slot.current.time + slot.interval) {
          closed = slot.current; // the normal case
          this.#pushClosed(slot, closed);
        } else {
          // Gap (weekend / outage): carry the last close forward so the chart
          // never shows a hole, then open the new candle at the live price.
          const missing = Math.round((epoch - slot.current.time) / slot.interval);
          log.warn(`${slot.key} gap of ${missing} candles — filling`);
          let ghost = { ...slot.current, time: slot.current.time + slot.interval };
          while (ghost.time < epoch) {
            this.#pushClosed(slot, ghost);
            ghost = { ...ghost, time: ghost.time + slot.interval };
          }
        }
        slot.current = { time: epoch, open: price, high: price, low: price, close: price, tickVolume: 1 };
        this.#markDirty(slot, closed, true);
        changed = true;
      } else if (epoch === slot.current.time) {
        /* -------- same candle: mutate the forming candle in place ------------ */
        slot.current.close = price;
        if (price > slot.current.high) slot.current.high = price;
        if (price < slot.current.low) slot.current.low = price;
        slot.current.tickVolume += 1;
        this.#markDirty(slot, null, false);
        changed = true;
      }

      slot.lastUpdate = Date.now();
      slot.tickCount += 1;
    }

    if (changed) {
      // Forming-candle persistence is deliberately best-effort and throttled
      // by #persistForming() — closed candles already went to the queue above.
      this.#persistForming(symbol);
      try {
        this.onFormingTick(symbol, tick, price);
      } catch (e) {
        log.error('onFormingTick threw:', e.message);
      }
    }

    return changed;
  }

  /* ======================================================================== */
  /*  INTERNALS — ring + dirty tracking + persistence                         */
  /* ======================================================================== */

  /** Append a closed candle, keep the array at exactly maxCandles, persist it. */
  #pushClosed(slot, candle) {
    slot.candles.push(candle);
    if (slot.candles.length > this.maxCandles) {
      slot.candles.splice(0, slot.candles.length - this.maxCandles);
    }
    slot.closedCount += 1;

    // Closed bars are durable immediately — this is the whole point of the DB.
    this.#persist(slot.symbol, slot.interval, candle, true);

    try {
      this.onClosed({
        symbol: slot.symbol,
        display: slot.display,
        interval: slot.interval,
        intervalLabel: slot.intervalLabel,
        candle: { ...candle },
      });
    } catch (e) {
      log.error('onClosed threw:', e.message);
    }
  }

  /** Queue a candle for write-behind persistence (no-op without a store). */
  #persist(symbol, interval, candle, closed) {
    if (!this.persistence) return;
    try {
      this.persistence.enqueueCandle(symbol, interval, candle, closed);
    } catch (e) {
      log.error('enqueueCandle failed:', e.message);
    }
  }

  /**
   * Persist the forming candle, at most once per PERSIST_FORMING_FLUSH_MS per
   * series. Losing the current bar on a crash is acceptable; losing a closed
   * one is not — which is why they are queued immediately.
   */
  #persistForming(symbol) {
    if (!this.persistence) return;
    const now = Date.now();
    const cadence = this.env.persistence.FORMING_FLUSH_MS;

    for (const slot of this.store.values()) {
      if (slot.symbol !== symbol || !slot.current) continue;
      const key = slot.key;
      const last = this.lastFormingWrite.get(key) ?? 0;
      if (now - last < cadence) continue;
      this.lastFormingWrite.set(key, now);
      this.#persist(symbol, slot.interval, { ...slot.current }, false);
    }
  }

  /** Record a change for the next SSE flush (coalesced by series). */
  #markDirty(slot, justClosed, isNewCandle) {
    const existing = this.pending.get(slot.key);
    if (existing) {
      if (justClosed) existing.closed = justClosed;
      existing.isNewCandle = existing.isNewCandle || isNewCandle;
      return;
    }
    this.pending.set(slot.key, {
      key: slot.key,
      symbol: slot.symbol,
      display: slot.display,
      interval: slot.interval,
      intervalLabel: slot.intervalLabel,
      current: null, // filled at flush time — always the freshest copy
      closed: justClosed,
      isNewCandle,
    });
  }

  /** Mark a single series dirty (used after hydration). */
  markDirty(key) {
    const slot = this.store.get(key);
    if (slot) this.#markDirty(slot, null, false);
  }

  /** Mark everything dirty (used right after a reconnect backfill). */
  markAllDirty() {
    for (const slot of this.store.values()) this.#markDirty(slot, null, false);
  }

  /** Drain coalesced changes → the SSE `update` payload (null when idle). */
  flush() {
    if (this.pending.size === 0) return null;
    const items = [];
    for (const [key, change] of this.pending) {
      const slot = this.store.get(key);
      if (!slot) continue;
      change.current = slot.current ? { ...slot.current } : null;
      change.closed = change.closed ? { ...change.closed } : null;
      items.push(change);
    }
    this.pending.clear();
    return { type: 'update', serverTime: Date.now(), items };
  }

  /** Full historical payload — sent instantly to every new SSE client. */
  snapshot() {
    const series = {};
    for (const slot of this.store.values()) {
      series[slot.key] = {
        symbol: slot.symbol,
        display: slot.display,
        pairName: slot.pairName,
        interval: slot.interval,
        intervalLabel: slot.intervalLabel,
        intervalName: slot.intervalName,
        ready: slot.ready,
        restored: slot.restored,
        fresh: this.isFresh(slot),
        candles: slot.candles,
        current: slot.current,
      };
    }
    return {
      type: 'snapshot',
      serverTime: Date.now(),
      maxCandles: this.maxCandles,
      source: 'MT5 upstream feed',
      pairs: [...new Set(SERIES.map((s) => s.symbol))].map((symbol) => {
        const spec = getSymbolSpec(symbol);
        return { code: spec.code, display: spec.display, name: spec.name, digits: spec.digits };
      }),
      timeframes: TIMEFRAMES.map((t) => ({
        seconds: t.seconds,
        api: t.api,
        label: t.label,
        name: t.name,
      })),
      series,
    };
  }

  isFresh(slot) {
    return Date.now() - slot.lastUpdate < Math.max(30_000, slot.interval * 3000);
  }

  /** Convenience: newest close price for a series, or null. */
  priceOf(symbolInput, intervalInput) {
    const slot = this.store.get(seriesKey(symbolInput, intervalInput));
    return slot?.current?.close ?? null;
  }

  /** Closed candles for a series, copied out (callers may mutate safely). */
  closedCandles(symbolInput, intervalInput) {
    const slot = this.store.get(seriesKey(symbolInput, intervalInput));
    return slot ? slot.candles.map((c) => ({ ...c })) : [];
  }

  /**
   * The live slot for a series WITHOUT copying — for the hot paths.
   *
   * The returned `candles` array is the aggregator's own: read it, never
   * mutate or reorder it. Copying 100 candles several times a second per symbol
   * is exactly the kind of garbage the tick path cannot afford.
   *
   * @returns {{candles:Array, current:object|null}|null}
   */
  seriesRef(symbolInput, intervalInput) {
    const slot = this.store.get(seriesKey(symbolInput, intervalInput));
    if (!slot) return null;
    return { candles: slot.candles, current: slot.current, slot };
  }

  /** The forming candle for a series, copied out. */
  formingCandle(symbolInput, intervalInput) {
    const slot = this.store.get(seriesKey(symbolInput, intervalInput));
    return slot?.current ? { ...slot.current } : null;
  }

  /** Round a price using the instrument's precision. */
  static price(symbol, value) {
    const spec = getSymbolSpec(symbol);
    if (!spec) return value;
    const f = 10 ** spec.digits;
    return Math.round(value * f) / f;
  }

  stats() {
    return [...this.store.values()].map((s) => ({
      key: s.key,
      display: s.display,
      intervalLabel: s.intervalLabel,
      closed: s.candles.length,
      ready: s.ready,
      restored: s.restored,
      ticks: s.tickCount,
      closedTotal: s.closedCount,
      lastPrice: s.current?.close ?? null,
      fresh: this.isFresh(s),
    }));
  }

  /** Aggregate counts for /health. */
  summary() {
    let candles = 0;
    let ready = 0;
    let restored = 0;
    for (const s of this.store.values()) {
      candles += s.candles.length;
      if (s.ready) ready += 1;
      if (s.restored) restored += 1;
    }
    return { series: this.store.size, ready, restored, candles };
  }
}

export default MarketAggregator;