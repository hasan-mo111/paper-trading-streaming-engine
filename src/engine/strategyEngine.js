/* =============================================================================
 *  AETHERON CORE  —  src/engine/strategyEngine.js
 *  -----------------------------------------------------------------------------
 *  Autonomous execution loop for the "Aetheron Universal Strategy".
 *
 *  THE STRATEGY IN FULL
 *  --------------------
 *  a) VOLATILITY-TARGETED SIZING
 *        LotSize = (AccountBalance × RiskPercentage) / (ATR(20) × PointValue)
 *     where PointValue = contractSize × pointSize for the instrument (see
 *     config/db.js). A calm market → a large position, a violent one → a small
 *     position, and the *risk in dollars per trade stays constant*. The result
 *     is rounded onto the broker's lot step and clamped to its limits.
 *
 *  b) ENTRY — 50-day Donchian breakout
 *        LONG  when price > the highest high of the last 50 closed days
 *        SHORT when price < the lowest  low of the last 50 closed days
 *     The channel excludes the live bar; otherwise the current bar's own high
 *     would be part of the level price has to exceed.
 *
 *  c) SPLIT EXIT — 20-day trailing stop
 *        Initial stop: the opposite edge of the 20-day channel
 *                      (LONG → 20d low, SHORT → 20d high).
 *        Leg 1 (25%):  closes automatically at +1 × ATR(20) — enough to cover
 *                      spread and operational fees and leave a little behind.
 *        Leg 2 (75%):  NO fixed take profit. It rides the 20-day channel until
 *                      the trailing stop is hit, letting a breakout trend run.
 *
 *  AUTONOMY
 *  ---------
 *  Every input below is computed server-side. The Android client can observe
 *  the results on the SSE streams but cannot influence a decision: there is no
 *  code path from a request into `openPosition()`.
 *
 *  FAIL-SAFE POSTURE
 *  -----------------
 *  The engine refuses to trade — rather than trading badly — when:
 *    • history is too short for ATR(20) or the 50-day channel  → WARMING_UP
 *    • ATR is missing/zero                                        → no sizing
 *    • the profile disables trading                               → HALTED
 *    • the market data is stale                                   → skipped
 *  It also refuses to open a second position on a symbol it already holds.
 * ============================================================================= */

import { randomUUID } from 'node:crypto';
import { ENV } from '../config/env.js';
import {
  getSymbolSpec,
  normaliseLot,
  roundPrice,
  pointsValue,
  SYMBOLS,
} from '../config/db.js';
import { atr as calcAtr, donchian, trailingChannel, profitPoints } from './indicatorCalc.js';
import { PLANS } from '../middleware/quotaManager.js';
import { isMarketOpen } from '../services/marketSession.js';
import { createLogger } from '../services/logger.js';

const log = createLogger('strategy');

/** Engine lifecycle states, surfaced on /health and in chat. */
export const ENGINE_STATE = Object.freeze({
  WARMING_UP: 'WARMING_UP', // not enough history to evaluate the strategy
  ARMED: 'ARMED', // evaluating every tick
  HALTED: 'HALTED', // deliberately disabled (config or profile)
});

export class StrategyEngine {
  /**
   * @param {object} opts
   * @param {MarketAggregator} opts.market
   * @param {object} opts.broker          brokerApi instance (server-only)
   * @param {QuotaManager} opts.quota
   * @param {SSEDispatcher} opts.dispatcher
   * @param {AuthService} opts.auth
   * @param {SupabaseService} opts.persistence
   * @param {object} [opts.env]
   * @param {(msg:object)=>void} [opts.onDecision] test/observability hook
   */
  constructor({
    market,
    broker,
    quota,
    dispatcher,
    auth,
    persistence = null,
    env = ENV,
    onDecision = () => {},
    userId = null,
    marketOpenFn = isMarketOpen,
  }) {
    this.market = market;
    this.broker = broker;
    this.quota = quota;
    this.dispatcher = dispatcher;
    this.auth = auth;
    this.persistence = persistence;
    this.env = env;
    this.onDecision = onDecision;
    this.userId = userId;
    this.marketOpenFn = marketOpenFn;

    const s = env.strategy;
    this.symbols = (s.SYMBOLS.length ? s.SYMBOLS : SYMBOLS).map(String).map((x) => x.toUpperCase());

    /** symbol → open position (the engine never pyramids on a symbol). */
    this.positions = new Map();
    /** symbol → runtime state (indicators, throttle, cooldown). */
    this.state = new Map();

    /** Lifecycle: WARMING_UP | ARMED | HALTED. (Not `state` — that is the map.) */
    this.phase = s.EXECUTION_ENABLED ? ENGINE_STATE.WARMING_UP : ENGINE_STATE.HALTED;
    this.haltReason = s.EXECUTION_ENABLED ? 'warming up' : 'execution disabled by configuration';

    this.counters = {
      evaluations: 0,
      signals: 0,
      entries: 0,
      exits: 0,
      signalOnly: 0,
      skippedNoHistory: 0,
      skippedStale: 0,
      skippedCooldown: 0,
      skippedPosition: 0,
      skippedMarketClosed: 0,
      errors: 0,
      brokerErrors: 0,
    };

    /**
     * Plan of the last account the engine acted for. Used only to label the
     * (never-metered) exit permit. Defaults to PRO so a restored position on a
     * FREE account is still managed — that is the whole point of exempting exits.
     * @type {'FREE'|'PRO'}
     */
    this.plan = PLANS.PRO;
  }

  /* ======================================================================== */
  /*  STATE HELPERS                                                          */
  /* ======================================================================== */

  symbolState(symbol) {
    let st = this.state.get(symbol);
    if (!st) {
      st = {
        lastExitCheckAt: 0,
        lastEntryAt: 0,
        lastTrailSendAt: 0,
        lastTrailSl: null,
        lastSignal: null,
        indicators: null,
        warmupErrors: 0,
      };
      this.state.set(symbol, st);
    }
    return st;
  }

  setState(state, reason) {
    if (this.phase === state && this.haltReason === reason) return;
    this.phase = state;
    this.haltReason = reason ?? null;
    log.warn(`engine state → ${state}${reason ? ` (${reason})` : ''}`);
    this.dispatcher?.system(`Strategy engine ${state}${reason ? `: ${reason}` : ''}`, { state, reason });
  }

  start() {
    if (!this.env.strategy.EXECUTION_ENABLED) {
      this.setState(ENGINE_STATE.HALTED, 'execution disabled by configuration');
      return;
    }
    // Arming is decided by the first evaluation that finds enough history.
    this.setState(ENGINE_STATE.WARMING_UP, 'awaiting historical candles');
  }

  stop() {
    for (const symbol of this.positions.keys()) {
      log.warn(`shutting down with an open ${symbol} position — leaving it to the trailing stop`);
    }
  }

  /* ======================================================================== */
  /*  INDICATORS — the single place that reads market state for a symbol      */
  /* ======================================================================== */

  /**
   * Compute every indicator the strategy needs for one symbol.
   *
   * @returns {{
   *   ready: boolean, reason?: string,
   *   price: number|null, atr: number|null,
   *   entryChannel: object|null, trailChannel: object|null,
   *   donchian50High: number|null, donchian50Low: number|null,
   *   donchian20High: number|null, donchian20Low: number|null
   * }}
   */
  indicators(symbol) {
    const cfg = this.env.strategy;
    const entryInterval = cfg.ENTRY_TIMEFRAME; // daily
    const atrInterval = cfg.ATR_TIMEFRAME; // hourly

    const dailyClosed = this.market.closedCandles(symbol, entryInterval);
    const dailyForming = this.market.formingCandle(symbol, entryInterval);
    const atrClosed = this.market.closedCandles(symbol, atrInterval);

    const price = this.market.priceOf(symbol, atrInterval) ?? this.market.priceOf(symbol, 300);

    // ---- the 50-day entry channel: closed bars only ----------------------
    const entryChannel = donchian(dailyClosed, cfg.DONCHIAN_ENTRY);

    // ---- the 20-day trailing channel: live bar included ------------------
    const trailChannel = trailingChannel(dailyClosed, dailyForming, cfg.DONCHIAN_EXIT);

    // ---- ATR(20) on the execution timeframe ------------------------------
    const atrValue = calcAtr(atrClosed, cfg.ATR_PERIOD);

    const out = {
      ready: false,
      reason: null,
      price,
      atr: atrValue,
      entryChannel,
      trailChannel,
      donchian50High: entryChannel?.high ?? null,
      donchian50Low: entryChannel?.low ?? null,
      donchian20High: trailChannel?.high ?? null,
      donchian20Low: trailChannel?.low ?? null,
      dailyBars: dailyClosed.length,
      atrBars: atrClosed.length,
    };

    if (!Number.isFinite(price)) {
      out.reason = 'no live price';
    } else if (dailyClosed.length < cfg.DONCHIAN_ENTRY) {
      out.reason = `need ${cfg.DONCHIAN_ENTRY} daily candles, have ${dailyClosed.length}`;
    } else if (!entryChannel) {
      out.reason = '50-day channel unavailable';
    } else if (atrValue == null) {
      out.reason = `need ${cfg.ATR_PERIOD + 1} ${cfg.ATR_TIMEFRAME / 3600}h candles, have ${atrClosed.length}`;
    } else {
      out.ready = true;
    }

    return out;
  }

  /* ======================================================================== */
  /*  SIZING — volatility-targeted lot size                                    */
  /* ======================================================================== */

  /**
   * LotSize = (Balance × Risk%) / (ATR(20) × PointValue)
   *
   * UNITS MATTER HERE. `atr()` returns a price DELTA (e.g. 0.0018 for
   * EURUSD), while `PointValue` is dollars per POINT — dollars per tick, per
   * lot. Multiplying the two is meaningless: it is off by a factor of
   * pointSize, which for EURUSD is a 100× error and would over-size every
   * position tenfold. So the ATR is first converted into POINTS:
   *
   *     atrPoints  = atr / pointSize          (e.g. 0.0018 / 0.0001 = 18 points)
   *     riskPerLot = atrPoints × pointValue   (18 × $10 = $180 per lot)
   *     lots       = riskAmount / riskPerLot
   *
   * This keeps the published formula intact and makes the dollars-per-trade
   * assumption hold exactly.
   *
   * @returns {{lots:number, riskAmount:number, pointValue:number, atr:number,
   *            atrPoints:number, riskPerLot:number, requested:number,
   *            clamped:boolean, reason?:string}}
   */
  calculateLotSize(symbol, { atr, balance, riskPct }) {
    const spec = getSymbolSpec(symbol);
    const pointValue = spec ? spec.pointSize * spec.contractSize : 0; // $ per point, per lot

    const fail = (reason, extra = {}) => ({
      lots: 0,
      reason,
      riskAmount: 0,
      pointValue,
      atr,
      atrPoints: 0,
      riskPerLot: 0,
      requested: 0,
      balance,
      riskPct,
      ...extra,
    });

    if (!spec) return fail('unknown instrument');
    if (!(atr > 0)) return fail('ATR unavailable');
    if (!(balance > 0)) return fail('account balance unavailable');
    if (!(riskPct > 0)) return fail('risk percentage is zero');
    if (!(pointValue > 0)) return fail('point value is zero');
    if (!(spec.pointSize > 0)) return fail('instrument has no point size');

    const atrPoints = atr / spec.pointSize; // price delta → points
    const riskPerLot = atrPoints * pointValue; // dollars at risk for 1.0 lot
    const riskAmount = balance * riskPct; // dollars we are willing to lose

    const requested = riskAmount / riskPerLot;
    const lots = normaliseLot(symbol, requested);

    return {
      lots,
      requested: Number(requested.toFixed(4)),
      clamped: lots > 0 && lots < requested,
      riskAmount: Number(riskAmount.toFixed(2)),
      pointValue,
      atr,
      atrPoints: Number(atrPoints.toFixed(4)),
      riskPerLot: Number(riskPerLot.toFixed(4)),
      balance,
      riskPct,
      reason: lots > 0 ? undefined : `computed ${requested.toFixed(4)} lots is below the broker minimum`,
    };
  }

  /* ======================================================================== */
  /*  ENTRY EVALUATION                                                        */
  /* ======================================================================== */

  /**
   * The current 20-day trailing reference, read straight from the live series.
   *
   * Deliberately NOT cached: the whole point of a Donchian trailing stop is that
   * it follows the forming bar. It costs one pass over 20 candles, so there is
   * nothing to gain by memoising it.
   *
   * @returns {{high:number, low:number, count:number}|null}
   */
  trailingReference(symbol) {
    const daily = this.market.seriesRef(symbol, this.env.strategy.ENTRY_TIMEFRAME);
    if (!daily) return null;
    return trailingChannel(daily.candles, daily.current, this.env.strategy.DONCHIAN_EXIT);
  }

  /**
   * Decide whether to open a position on `symbol`.
   * Read-only: it reports the decision and (when enabled) acts on it.
   *
   * @returns {Promise<object>} the decision record
   */
  async evaluateEntry(symbol, { force = false } = {}) {
    this.counters.evaluations += 1;
    const st = this.symbolState(symbol);
    const cfg = this.env.strategy;

    const ind = this.indicators(symbol);
    st.indicators = ind;

    const decision = {
      symbol,
      at: Date.now(),
      action: 'HOLD',
      reason: null,
      indicators: ind,
      side: null,
      volume: null,
    };

    if (this.phase === ENGINE_STATE.HALTED) {
      decision.reason = this.haltReason;
      this.#record(decision);
      return decision;
    }

    if (!ind.ready) {
      decision.reason = ind.reason;
      this.counters.skippedNoHistory += 1;
      this.#maybeArm(ind);
      this.#record(decision);
      return decision;
    }

    // Warm-up complete — arm the engine.
    this.#maybeArm(ind, true);

    // Already exposed on this symbol: the trailing stop manages it. No
    // pyramiding — two correlated positions double the risk the sizing model
    // was built to keep constant.
    if (this.positions.has(symbol)) {
      decision.reason = 'position already open';
      this.counters.skippedPosition += 1;
      this.#record(decision);
      return decision;
    }

    // No new risk while the weekly session is closed. The feed may replay a
    // Friday quote or the engine may sweep on a Sunday restart; neither is a
    // reason to publish a trade idea or place an order.
    if (!this.marketOpenFn()) {
      decision.reason = 'market closed (weekend)';
      this.counters.skippedMarketClosed += 1;
      this.#record(decision);
      return decision;
    }

    // Cooldown after the last entry on this symbol.
    if (!force && Date.now() - st.lastEntryAt < cfg.COOLDOWN_MS) {
      decision.reason = 'entry cooldown active';
      this.counters.skippedCooldown += 1;
      this.#record(decision);
      return decision;
    }

    // Stale market data must never open a position.
    const slot = this.market.store.get(`${symbol}|${cfg.ATR_TIMEFRAME}`);
    if (slot && !this.market.isFresh(slot)) {
      decision.reason = 'market data is stale';
      this.counters.skippedStale += 1;
      this.#record(decision);
      return decision;
    }

    /* ---- the breakout test -------------------------------------------- */
    const price = ind.price;
    const ch = ind.entryChannel;
    let side = null;
    if (price > ch.high) side = 'LONG';
    else if (price < ch.low) side = 'SHORT';

    if (!side) {
      decision.reason = 'inside the 50-day channel';
      decision.action = 'HOLD';
      this.#record(decision);
      return decision;
    }

    decision.action = 'ENTER';
    decision.side = side;
    decision.breakLevel = side === 'LONG' ? ch.high : ch.low;
    this.counters.signals += 1;

    /* ---- profile: balance, risk %, plan, per-user trading switch -------- */
    const ctx = await this.quota.contextFor(this.userId ?? 'system', 'PRO');
    decision.plan = ctx.plan;
    this.plan = ctx.plan;

    if (!ctx.tradingEnabled) {
      decision.action = 'SKIP';
      decision.reason = 'trading disabled for this account';
      this.#record(decision);
      return decision;
    }

    /* ---- volatility-targeted sizing ------------------------------------ */
    const sizing = this.calculateLotSize(symbol, {
      atr: ind.atr,
      balance: ctx.accountBalance,
      riskPct: ctx.riskPercentage,
    });
    decision.sizing = sizing;

    if (sizing.lots <= 0) {
      decision.action = 'SKIP';
      decision.reason = sizing.reason ?? 'size below broker minimum';
      this.#record(decision);
      return decision;
    }
    decision.volume = sizing.lots;

    /* ---- initial stop: the opposite edge of the 20-day channel ---------- */
    const stop = this.#initialStop(symbol, side, ind);
    decision.stopLoss = stop;
    decision.takeProfit = null; // Leg 2 has no fixed TP by design

    /* ---- quota gate: may we execute automatically? --------------------- */
    const quotaVerdict = await this.quota.canExecute(this.userId ?? 'system', {
      plan: ctx.plan,
      kind: 'ENTRY',
    });
    decision.quota = quotaVerdict.usage;

    // One id per decision: it de-duplicates broker retries and keys the audit
    // row, so the same signal can never be filled twice.
    decision.signalId = signalId();

    const signalPayload = {
      signalId: decision.signalId,
      symbol,
      side,
      volume: sizing.lots,
      entryPrice: roundPrice(symbol, price),
      sl: stop,
      tp: null,
      atr: ind.atr,
      donchianHigh: ind.donchian50High,
      donchianLow: ind.donchian50Low,
      riskAmount: sizing.riskAmount,
      plan: ctx.plan,
      userId: this.userId ?? null,
    };

    // Always publish the signal — even when it cannot be executed. That is the
    // whole point of the SIGNAL_ONLY downgrade.
    this.dispatcher?.signal({ ...signalPayload, signalId: decision.signalId });

    if (!quotaVerdict.allowed) {
      decision.action = 'SIGNAL_ONLY';
      decision.reason = quotaVerdict.reason;
      decision.downgrade = quotaVerdict.downgrade;
      this.counters.signalOnly += 1;

      this.dispatcher?.signalOnly({ ...signalPayload, signalId: decision.signalId, reason: quotaVerdict.reason });
      this.dispatcher?.quota(quotaVerdict.downgrade.message, { ...quotaVerdict.usage, userId: this.userId ?? null });
      this.#auditExecution({ ...signalPayload, signalId: decision.signalId, status: 'SIGNAL_ONLY', reason: quotaVerdict.reason });
      this.#record(decision);
      return decision;
    }

    /* ---- execute ------------------------------------------------------- */
    const fill = await this.#execute(signalPayload, decision);

    if (fill.ok) {
      decision.action = 'EXECUTED';
      st.lastEntryAt = Date.now();
      st.lastSignal = decision;
      this.counters.entries += 1;
      // Spend the quota only after a real fill — a rejected order must not
      // cost the user one of their three daily trades.
      await this.quota.consume(this.userId ?? 'system', { plan: ctx.plan, kind: 'ENTRY' });
    } else {
      decision.action = 'FAILED';
      decision.reason = fill.reason;
      this.counters.brokerErrors += 1;
    }

    this.#record(decision);
    return decision;
  }

  /**
   * Evaluate every configured symbol once.
   *
   * Run after the initial backfill and after a reconnect: a daily-breakout
   * system would otherwise not look at the market until the next daily close,
   * which can be 20 hours away. The cooldown still prevents this sweep from
   * producing duplicate entries.
   */
  async evaluateAll({ force = false } = {}) {
    const results = [];
    for (const symbol of this.symbols) {
      try {
        results.push(await this.evaluateEntry(symbol, { force }));
      } catch (e) {
        this.counters.errors += 1;
        log.error(`evaluateAll failed for ${symbol}:`, e.message);
      }
    }
    const acted = results.filter((d) => d.action !== 'HOLD' && d.action !== 'SKIP');
    log.info(
      `sweep complete — ${results.length} symbol(s), ${acted.length} actionable` +
        (this.phase === ENGINE_STATE.WARMING_UP ? ', still warming up' : '')
    );
    return results;
  }

  /**
   * Rebuild the position book after a restart from the execution audit log.
   *
   * Without this a crash would make the engine forget it is exposed, and the
   * next daily breakout would open a SECOND position on the same symbol — the
   * exact doubling of risk the sizing model exists to prevent. An open trade
   * that survives the process keeps its trailing stop and its leg split.
   *
   * @param {Array<object>} records rows from SupabaseService.listOpenPositions()
   * @returns {number} how many positions were restored
   */
  restorePositions(records) {
    if (!Array.isArray(records) || records.length === 0) return 0;
    let restored = 0;

    for (const row of records) {
      const symbol = row.symbol;
      if (!this.symbols.includes(symbol)) continue;
      if (!getSymbolSpec(symbol)) continue;
      if (this.positions.has(symbol)) continue; // already tracked

      const legs =
        Array.isArray(row.legs) && row.legs.length
          ? row.legs.map((l) => ({ ...l }))
          : [
              { name: '1', pct: this.env.strategy.LEG1_PCT, volume: 0, closed: false, purpose: 'cover fees at +1×ATR(20)' },
              { name: '2', pct: this.env.strategy.LEG2_PCT, volume: Number(row.volume) || 0, closed: false, purpose: 'runner — no fixed TP, follows the 20-day trail' },
            ];

      // Mark Leg 1 closed if the audit log already recorded its take.
      if (row.leg1Closed && legs[0]) {
        legs[0].closed = true;
        legs[0].exitPrice = row.exit_price ?? null;
        legs[0].closedAt = Date.now();
      }

      const totalVolume = Number(row.volume) || 0;
      if (!(totalVolume > 0)) continue;

      const position = {
        signalId: row.signalId ?? row.signal_id ?? row.id,
        brokerId: String(row.broker_id ?? row.brokerId ?? row.id),
        symbol,
        side: row.side,
        volume: totalVolume,
        entryPrice: Number(row.entry_price ?? row.entryPrice),
        currentPrice: Number(row.entry_price ?? row.entryPrice),
        stop: row.stop_loss == null ? null : Number(row.stop_loss),
        atr: row.atr == null ? null : Number(row.atr),
        riskAmount: row.risk_amount == null ? null : Number(row.risk_amount),
        legs,
        openedAt: new Date(row.opened_at ?? row.created_at ?? Date.now()).getTime(),
        unrealised: 0,
        mode: row.mode ?? this.env.broker.MODE,
        restored: true,
      };
      this.positions.set(symbol, position);

      // Backfill the durable position ledger when this row came from the older
      // execution-audit recovery path. It is idempotent for rows that were
      // already loaded from strategy_trades.
      this.#persistStrategy(position);

      this.symbolState(symbol).lastEntryAt = Date.now();
      restored += 1;
      log.warn(`restored open ${symbol} ${row.side} ${totalVolume} from the position ledger`);
    }

    if (restored > 0) {
      this.dispatcher?.system(
        `Recovered ${restored} open position(s) after a restart — trailing stops are active again.`,
        { symbols: [...this.positions.keys()] }
      );
    }
    return restored;
  }

  /* ======================================================================== */
  /*  EXIT EVALUATION — runs on the tick hot path                             */
  /* ======================================================================== */

  /**
   * Manage an open position: ratchet the trailing stop, take Leg 1's profit,
   * and close whatever the stop takes out.
   *
   * Throttled per symbol (EXIT_THROTTLE_MS) because ticks arrive far faster
   * than any of these levels can change.
   *
   * @returns {Promise<object|null>} an action record, or null when idle
   */
  async evaluateExits(symbol, price) {
    const position = this.positions.get(symbol);
    if (!position) return null;
    if (!Number.isFinite(price)) return null;

    const st = this.symbolState(symbol);
    const now = Date.now();
    if (now - st.lastExitCheckAt < this.env.strategy.EXIT_THROTTLE_MS) return null;
    st.lastExitCheckAt = now;

    position.currentPrice = price;
    position.unrealised = Number(
      pointsValue(symbol, profitPoints(position.side, position.entryPrice, price), position.volume).toFixed(2)
    );

    const cfg = this.env.strategy;
    const spec = getSymbolSpec(symbol);
    const digits = spec?.digits ?? 2;

    /* ---- 1. ratchet the trailing stop --------------------------------- */
    // The trailing channel is recomputed from the LIVE series every time (it is
    // only 20 bars of work), because that is precisely what lets the stop
    // follow the market. The expensive parts — ATR(20) and the 50-day entry
    // channel — stay cached and are refreshed on every candle close.
    const trail = this.trailingReference(symbol);
    if (trail) {
      const candidate = position.side === 'LONG' ? trail.low : trail.high;
      const improved =
        position.side === 'LONG' ? candidate > (position.stop ?? -Infinity) : candidate < (position.stop ?? Infinity);

      // Only call the broker when the stop actually moved a meaningful amount:
      // a modify per tick would hammer the bridge API for no benefit.
      const movedPoints = position.stop == null ? Infinity : Math.abs(candidate - position.stop) / (spec?.pointSize || 1);
      if (improved && (movedPoints >= cfg.TRAIL_MIN_STEP_POINTS || position.stop == null)) {
        position.stop = roundPrice(symbol, candidate);
        const res = await this.broker.modifyPosition({ positionId: position.brokerId, sl: position.stop });
        if (res.ok) {
          st.lastTrailSendAt = now;
          st.lastTrailSl = position.stop;
          this.#persistStrategy(position);
          this.dispatcher?.trailing({
            symbol,
            side: position.side,
            sl: position.stop,
            distance: profitPoints(position.side, position.stop, price),
            userId: this.userId ?? null,
          });
        } else {
          this.counters.brokerErrors += 1;
          log.warn(`trailing modify failed for ${symbol}: ${res.reason}`);
        }
      }
    }

    /* ---- 2. Leg 1 take profit at +1 × ATR(20) ------------------------ */
    if (!position.legs[0].closed && position.atr > 0) {
      const target =
        position.side === 'LONG'
          ? position.entryPrice + position.atr * cfg.LEG1_ATR_MULT
          : position.entryPrice - position.atr * cfg.LEG1_ATR_MULT;

      const hit = position.side === 'LONG' ? price >= target : price <= target;
      if (hit) {
        const res = await this.#closeLeg(symbol, 0, target, 'leg1-atr-target');
        if (res) {
          this.counters.exits += 1;
          this.dispatcher?.partialClose({
            symbol,
            side: position.side,
            volume: res.closedVolume,
            exitPrice: res.exitPrice,
            profit: res.profit,
            leg: '1',
            reason: `+${cfg.LEG1_ATR_MULT}×ATR(20) — fees covered`,
            userId: this.userId ?? null,
          });
          // The remaining runner keeps the (now ratcheted) stop.
          this.#auditExecution({
            signalId: position.signalId,
            userId: this.userId ?? null,
            symbol,
            side: position.side,
            volume: res.closedVolume,
            entry_price: position.entryPrice,
            exit_price: res.exitPrice,
            status: 'CLOSED',
            reason: 'LEG1_ATR',
            profit: res.profit,
          });
          return res;
        }
      }
    }

    /* ---- 3. trailing stop hit ----------------------------------------- */
    if (position.stop != null) {
      const stopHit = position.side === 'LONG' ? price <= position.stop : price >= position.stop;
      if (stopHit) {
        const remaining = position.volume;
        const res = await this.#closeLeg(symbol, null, position.stop, 'trailing-stop');
        if (res) {
          this.counters.exits += 1;
          this.dispatcher?.stopHit({
            symbol,
            side: position.side,
            volume: res.closedVolume,
            exitPrice: res.exitPrice,
            profit: res.profit,
            leg: position.legs[0].closed ? '2' : '1+2',
            userId: this.userId ?? null,
          });
          this.dispatcher?.system(
            `${spec?.display ?? symbol} position closed at ${Number(res.exitPrice).toFixed(digits)} · ` +
              `net ${res.profit >= 0 ? '+' : ''}${Number(res.profit).toFixed(2)} USD`,
            { symbol, exitPrice: res.exitPrice, profit: res.profit, reason: 'TRAILING_STOP' }
          );
          this.#auditExecution({
            signalId: position.signalId,
            userId: this.userId ?? null,
            symbol,
            side: position.side,
            volume: remaining,
            entry_price: position.entryPrice,
            exit_price: res.exitPrice,
            status: 'CLOSED',
            reason: 'TRAILING_STOP',
            profit: res.profit,
          });
          return res;
        }
      }
    }

    return null;
  }

  /* ======================================================================== */
  /*  EVENT HOOKS — called by the market layer                               */
  /* ======================================================================== */

  /**
   * Every tick. Keeps the broker's mark price current and drives the throttled
   * exit loop. Entry evaluation is *not* run here — it is driven by candle
   * closes, which is the correct cadence for a daily-breakout system.
   */
  onTick(symbol, tick, price) {
    if (!this.symbols.includes(symbol)) return;
    this.broker.markPrice?.(symbol, price);

    // Exits are latency-sensitive (a trailing stop should fire promptly), so
    // they run on a promise the caller deliberately does not await.
    Promise.resolve()
      .then(() => this.evaluateExits(symbol, price))
      .catch((e) => {
        this.counters.errors += 1;
        log.error(`exit loop failed for ${symbol}:`, e.message);
      });
  }

  /**
   * A candle just closed.
   *  • on the entry timeframe (D1) → evaluate a new entry
   *  • on any timeframe            → refresh the indicator cache
   */
  async onClosedCandle({ symbol, interval, candle }) {
    if (!this.symbols.includes(symbol)) return;
    const cfg = this.env.strategy;

    // Cache fresh indicators so the exit loop uses up-to-date channels without
    // recomputing on every tick.
    const st = this.symbolState(symbol);
    st.indicators = this.indicators(symbol);

    log.debug(
      `${symbol} ${candle.close} closed on ${interval}s — ` +
        `ATR ${st.indicators.atr?.toFixed(4) ?? '—'} · ` +
        `50d ${st.indicators.donchian50High ?? '—'}/${st.indicators.donchian50Low ?? '—'}` +
        (interval === cfg.ENTRY_TIMEFRAME ? ' · entry evaluation' : '')
    );

    if (interval === cfg.ENTRY_TIMEFRAME) {
      try {
        const decision = await this.evaluateEntry(symbol);
        if (decision.action !== 'HOLD') {
          log.info(
            `${symbol}: ${decision.action}${decision.side ? ` ${decision.side}` : ''}` +
              `${decision.volume ? ` ${decision.volume}` : ''}` +
              `${decision.reason ? ` — ${decision.reason}` : ''}`
          );
        }
      } catch (e) {
        this.counters.errors += 1;
        log.error(`entry evaluation failed for ${symbol}:`, e.message);
      }
    }
  }

  /* ======================================================================== */
  /*  EXECUTION HELPERS                                                       */
  /* ======================================================================== */

  /** Initial stop: the opposite edge of the 20-day channel. */
  #initialStop(symbol, side, ind) {
    const cfg = this.env.strategy;
    const price = ind.price;
    let stop = null;

    if (side === 'LONG' && Number.isFinite(ind.donchian20Low)) {
      stop = ind.donchian20Low;
    } else if (side === 'SHORT' && Number.isFinite(ind.donchian20High)) {
      stop = ind.donchian20High;
    }

    // A channel edge that already sits on the wrong side of price would be
    // rejected by the broker; fall back to one ATR of room, which is also the
    // minimum sane distance.
    if (stop == null || (side === 'LONG' && stop >= price) || (side === 'SHORT' && stop <= price)) {
      stop = side === 'LONG' ? price - (ind.atr ?? 0) * cfg.LEG1_ATR_MULT : price + (ind.atr ?? 0) * cfg.LEG1_ATR_MULT;
      log.warn(`${symbol}: 20-day stop unusable — falling back to 1×ATR (${stop.toFixed(4)})`);
    }
    return roundPrice(symbol, stop);
  }

  /**
   * Place the order and register the two legs.
   *
   * `payload.side` is LONG/SHORT (the strategy's vocabulary); brokerApi
   * translates it to the BUY/SELL an MT5 ticket expects.
   */
  async #execute(payload, decision) {
    if (!this.env.strategy.EXECUTION_ENABLED) {
      return { ok: false, reason: 'execution disabled by configuration' };
    }

    const { signalId, symbol, side, volume, entryPrice, sl } = payload;
    const cfg = this.env.strategy;

    // Split the position up front so the leg volumes always sum to the total.
    const leg1Lots = normaliseLot(symbol, volume * cfg.LEG1_PCT);
    const leg2Lots = normaliseLot(symbol, volume - leg1Lots) || normaliseLot(symbol, volume);
    const legs = [
      { name: '1', pct: cfg.LEG1_PCT, volume: leg1Lots, closed: false, purpose: 'cover fees at +1×ATR(20)' },
      { name: '2', pct: cfg.LEG2_PCT, volume: leg2Lots, closed: false, purpose: 'runner — no fixed TP, follows the 20-day trail' },
    ];

    this.dispatcher?.system(
      `Executing ${symbol} ${side} ${volume} @ ${entryPrice} · SL ${sl} · TP none (Leg 2 runs on the 20-day trail)`,
      { symbol, side, volume, sl, tp: null, signalId }
    );

    let res;
    try {
      res = await this.broker.placeOrder({
        signalId,
        symbol,
        side,
        volume,
        entryPrice,
        sl,
        tp: null, // never a fixed take profit
        comment: `aetheron/${cfg.DONCHIAN_ENTRY}d-${cfg.LEG1_ATR_MULT}atr`,
        meta: { atr: payload.atr, riskAmount: payload.riskAmount, plan: payload.plan, legs },
      });
    } catch (e) {
      this.counters.brokerErrors += 1;
      this.counters.errors += 1;
      log.error(`broker.placeOrder threw for ${symbol}:`, e.message);
      this.dispatcher?.risk(`Order failed for ${symbol}: ${e.message}`, { symbol, side, error: e.message });
      return { ok: false, reason: e.message };
    }

    if (!res.ok) {
      this.counters.brokerErrors += 1;
      log.warn(`order rejected for ${symbol}: ${res.reason}`);
      this.dispatcher?.risk(`Order rejected for ${symbol} ${side}: ${res.reason}`, { symbol, side, reason: res.reason });
      return res;
    }

    const p = res.position;
    const position = {
      signalId,
      brokerId: p.id,
      symbol,
      side,
      volume,
      entryPrice: p.entryPrice,
      currentPrice: p.entryPrice,
      stop: p.sl ?? sl,
      atr: payload.atr,
      riskAmount: payload.riskAmount ?? null,
      legs,
      openedAt: Date.now(),
      unrealised: 0,
      mode: p.mode ?? this.env.broker.MODE,
    };
    this.positions.set(symbol, position);

    this.dispatcher?.execution({
      symbol,
      side,
      volume,
      entryPrice: p.entryPrice,
      positionId: p.id,
      mode: position.mode,
      userId: this.userId ?? null,
    });

    this.#auditExecution({
      signalId,
      userId: this.userId ?? null,
      symbol,
      side,
      volume,
      entry_price: p.entryPrice,
      stop_loss: position.stop,
      status: 'EXECUTED',
      reason: `DONCHIAN_${cfg.DONCHIAN_ENTRY}_BREAKOUT`,
      mode: position.mode,
      legs,
      atr: payload.atr,
      risk_amount: payload.riskAmount,
    });
    this.#persistStrategy(position);

    void decision;
    return { ok: true, position, brokerResult: p };
  }

  /**
   * Close one leg (or the whole position when `legIndex` is null).
   * `atPrice` is the reference price for the paper fill.
   *
   * The quota is consulted with `kind: 'EXIT'`, which is never metered — the
   * point of asking is that the guarantee is explicit and auditable rather than
   * an implicit absence. An open position must always be stoppable, whatever
   * the user's plan or the state of the daily counter.
   */
  async #closeLeg(symbol, legIndex, atPrice, reason) {
    const position = this.positions.get(symbol);
    if (!position) return null;

    const permit = await this.quota.canExecute(this.userId ?? 'system', {
      plan: this.plan,
      kind: 'EXIT',
    });
    if (!permit.allowed) {
      // Cannot currently happen (exits are never metered), but if the quota
      // policy ever changes this is the place that guarantees a position is
      // never left unstoppable.
      log.error(`refusing to close ${symbol}: ${permit.reason} — this would be unsafe`);
      this.dispatcher?.risk(`Refused to close ${symbol}: ${permit.reason}`, { symbol, reason: permit.reason });
      return null;
    }

    const leg = legIndex == null ? null : position.legs[legIndex];
    const volume = legIndex == null ? position.volume : leg.volume;
    if (!(volume > 0)) return null;

    if (Number.isFinite(atPrice)) this.broker.markPrice?.(symbol, atPrice);

    let res;
    try {
      res = await this.broker.closePosition({ positionId: position.brokerId, volume, reason });
    } catch (e) {
      this.counters.brokerErrors += 1;
      log.error(`closePosition threw for ${symbol}:`, e.message);
      return null;
    }
    if (!res.ok) {
      this.counters.brokerErrors += 1;
      log.warn(`close failed for ${symbol}: ${res.reason}`);
      return null;
    }

    if (leg) {
      leg.closed = true;
      leg.exitPrice = res.exitPrice;
      leg.profit = res.profit;
      leg.closedAt = Date.now();
    }
    position.volume = normaliseLot(symbol, Number((position.volume - res.closedVolume).toFixed(2)));

    if (!(position.volume > 0)) {
      this.positions.delete(symbol);
      const initialVolume =
        position.legs?.reduce((sum, l) => sum + (Number(l.volume) || 0), 0) || position.volume;
      this.#persistStrategy(
        {
          ...position,
          volume: initialVolume,
          exitPrice: res.exitPrice,
          exitReason: reason,
          profit: res.profit,
          closedAt: Date.now(),
        },
        { closed: true }
      );
      log.info(`${symbol} position fully closed · ${reason} · net ${res.profit}`);
    } else {
      this.#persistStrategy(position);
      log.info(`${symbol} leg ${leg?.name ?? 'all'} closed ${res.closedVolume} · ${res.remainingVolume} left`);
    }

    return res;
  }

  /* ======================================================================== */
  /*  DURABLE POSITION LEDGER                                                */
  /* ======================================================================== */

  /**
   * Mirror the live position book into `strategy_trades`.
   *
   * The execution audit remains append-only. This table is the *current* open
   * book, so a restart can restore the exact legs/stop it left behind without
   * reconstructing state from a sequence of audit rows. The write is
   * fire-and-forget: a database hiccup must never abort a trade the tick
   * handler is already managing.
   *
   * @param {object} position
   * @param {{closed?: boolean}} [opts]
   */
  #persistStrategy(position, { closed = false } = {}) {
    if (!this.persistence?.saveStrategyTrade || !position) return;
    const id = String(position.signalId ?? position.brokerId ?? '');
    if (!id) return;
    const openedMs = Number(position.openedAt) || Date.now();
    const closedMs = closed ? Number(position.closedAt) || Date.now() : null;

    const row = {
      id,
      signal_id: String(position.signalId ?? id),
      user_id: this.userId ?? null,
      symbol: position.symbol,
      side: position.side,
      volume: Number(position.volume) || 0,
      entry_price: Number(position.entryPrice),
      stop_loss: position.stop == null ? null : Number(position.stop),
      atr: position.atr == null ? null : Number(position.atr),
      risk_amount: position.riskAmount == null ? null : Number(position.riskAmount),
      legs: Array.isArray(position.legs) ? position.legs.map((l) => ({ ...l })) : [],
      status: closed ? 'CLOSED' : 'OPEN',
      exit_price: closed && position.exitPrice != null ? Number(position.exitPrice) : null,
      exit_reason: closed ? position.exitReason ?? null : null,
      profit: closed && position.profit != null ? Number(position.profit) : null,
      broker_id: position.brokerId == null ? null : String(position.brokerId),
      mode: position.mode ?? this.env.broker.MODE,
      opened_at: new Date(openedMs).toISOString(),
      opened_ms: openedMs,
      closed_at: closedMs ? new Date(closedMs).toISOString() : null,
      closed_ms: closedMs,
      meta: {},
    };

    Promise.resolve(this.persistence.saveStrategyTrade(row)).catch((e) =>
      log.warn('strategy position was not persisted:', e.message)
    );
  }

  /* ======================================================================== */
  /*  AUDIT + REPORTING                                                       */
  /* ======================================================================== */

  /** Append an immutable record of what the engine did. Never throws. */
  #auditExecution(row) {
    if (!this.persistence) return;
    Promise.resolve(
      this.persistence.recordExecution({
        id: row.signalId ?? randomUUID(),
        signal_id: row.signalId ?? null,
        user_id: row.userId ?? this.userId ?? null,
        symbol: row.symbol,
        side: row.side ?? null,
        volume: row.volume ?? null,
        entry_price: row.entry_price ?? null,
        exit_price: row.exit_price ?? null,
        stop_loss: row.stop_loss ?? null,
        status: row.status,
        reason: row.reason ?? null,
        mode: row.mode ?? this.env.broker.MODE,
        profit: row.profit ?? null,
        atr: row.atr ?? null,
        risk_amount: row.risk_amount ?? null,
        legs: row.legs ?? null,
        created_at: new Date().toISOString(),
      })
    ).catch((e) => log.error('audit write failed:', e.message));
  }

  /** Arm the engine once history is sufficient. */
  #maybeArm(ind, force = false) {
    if (this.phase === ENGINE_STATE.ARMED) return;
    if (this.phase === ENGINE_STATE.HALTED) return;
    if (ind.ready || force) this.setState(ENGINE_STATE.ARMED, null);
  }

  #record(decision) {
    try {
      this.onDecision(decision);
    } catch (e) {
      log.warn('onDecision hook threw:', e.message);
    }
  }

  /** Open positions, for /api/user/positions. */
  openPositions() {
    return [...this.positions.values()].map((p) => ({
      ...p,
      legs: p.legs.map((l) => ({ ...l })),
    }));
  }

  positionFor(symbol) {
    return this.positions.get(symbol) ?? null;
  }

  status() {
    return {
      state: this.phase,
      reason: this.haltReason,
      symbols: this.symbols,
      strategy: {
        name: 'Aetheron Universal',
        donchianEntry: this.env.strategy.DONCHIAN_ENTRY,
        donchianExit: this.env.strategy.DONCHIAN_EXIT,
        atrPeriod: this.env.strategy.ATR_PERIOD,
        leg1Pct: this.env.strategy.LEG1_PCT,
        leg2Pct: this.env.strategy.LEG2_PCT,
        leg1AtrMult: this.env.strategy.LEG1_ATR_MULT,
        entryTimeframe: this.env.strategy.ENTRY_TIMEFRAME,
        atrTimeframe: this.env.strategy.ATR_TIMEFRAME,
      },
      brokerMode: this.env.broker.MODE,
      executionEnabled: this.env.strategy.EXECUTION_ENABLED,
      marketOpen: this.marketOpenFn(),
      counters: { ...this.counters },
      positions: this.openPositions(),
      indicators: Object.fromEntries(
        this.symbols.map((s) => {
          const st = this.state.get(s);
          return [
            s,
            {
              price: st?.indicators?.price ?? null,
              atr20: st?.indicators?.atr ?? null,
              donchian50High: st?.indicators?.donchian50High ?? null,
              donchian50Low: st?.indicators?.donchian50Low ?? null,
              donchian20High: st?.indicators?.donchian20High ?? null,
              donchian20Low: st?.indicators?.donchian20Low ?? null,
              dailyBars: st?.indicators?.dailyBars ?? 0,
              ready: Boolean(st?.indicators?.ready),
              hasPosition: this.positions.has(s),
              lastEntryAt: st?.lastEntryAt ?? null,
            },
          ];
        })
      ),
    };
  }
}

/** Short, unique id for one strategy decision. */
function signalId() {
  return `sg_${randomUUID()}`;
}

export default StrategyEngine;