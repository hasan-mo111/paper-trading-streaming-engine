/* =============================================================================
 *  AETHERON CORE  —  src/services/brokerApi.js
 *  -----------------------------------------------------------------------------
 *  The execution gateway. This is the ONLY module in the codebase that can
 *  create, modify or close a position.
 *
 *  SECURITY INVARIANT  —  read this before changing anything here
 *  =============================================================================
 *  The broker is never mounted on an HTTP route and no route ever imports it.
 *  Its arguments are produced exclusively by src/engine/strategyEngine.js from
 *  server-side market data. There is deliberately no code path by which an
 *  Android client can influence `symbol`, `side`, `volume`, `sl` or `tp` —
 *  the API surface in src/routes/ has no trade endpoint at all, and
 *  `assertNoTradeParameters()` (middleware/guard.js) actively rejects requests
 *  that even *look* like an order instruction.
 *
 *  TWO IMPLEMENTATIONS
 *  -------------------
 *  • PaperBroker  — simulated fills in-process. Default. Deterministic, so the
 *                   engine can be exercised end-to-end without risking money.
 *  • Mt5BridgeBroker — HTTP to your MT5 bridge (an EA running on a Windows VPS).
 *                    Requests are signed with an HMAC so the bridge can reject
 *                    anything that did not come from this engine.
 * ============================================================================= */

import { randomUUID } from 'node:crypto';
import { ENV } from '../config/env.js';
import { getSymbolSpec, roundPrice, normaliseLot, normaliseSide, pointsValue } from '../config/db.js';
import { profitPoints } from '../engine/indicatorCalc.js';
import { createLogger } from './logger.js';

const log = createLogger('broker');

/** Sleep helper — only used by the paper broker to model execution latency. */
const sleep = (ms) => (ms > 0 ? new Promise((r) => setTimeout(r, ms)) : Promise.resolve());

/* =============================================================================
 *  PAPER BROKER — in-process simulated execution
 * ============================================================================= */
export class PaperBroker {
  constructor({ env = ENV, onFill = () => {} } = {}) {
    this.mode = 'paper';
    this.env = env;
    this.onFill = onFill;

    /** positionId → position */
    this.positions = new Map();
    /** Ordered log of every action, for /api/user/positions and the UI. */
    this.ledger = [];
    /** Last price seen per symbol, used to fill at something realistic. */
    this.lastPrice = new Map();

    this.stats = { placed: 0, modified: 0, closed: 0, rejected: 0, fees: 0 };
  }

  /** The engine pushes every tick price here so fills are realistic. */
  markPrice(symbol, price) {
    if (Number.isFinite(price)) this.lastPrice.set(symbol, price);
  }

  /** #fillPrice: buys lift the offer, sells hit the bid. */
  #fillPrice(symbol, side, referencePrice) {
    const spec = getSymbolSpec(symbol);
    if (!spec) return referencePrice;
    const half = spec.pointSize / 2; // half the spread
    return side === 'BUY' ? referencePrice + half : referencePrice - half;
  }

  #record(action) {
    this.ledger.push({ id: randomUUID(), at: Date.now(), ...action });
    if (this.ledger.length > 500) this.ledger.shift();
    return this.ledger.at(-1);
  }

  /**
   * Open a position. Every argument comes from the strategy engine.
   *
   * `side` accepts either vocabulary — the engine reasons in LONG/SHORT, MT5
   * order tickets use BUY/SELL — and both normalise to the same direction.
   *
   * @returns {Promise<{ok:boolean, position?:object, reason?:string}>}
   */
  async placeOrder({ signalId, symbol, side, volume, entryPrice, sl = null, tp = null, comment = '', meta = {} }) {
    const spec = getSymbolSpec(symbol);
    if (!spec) return { ok: false, reason: `unknown symbol ${symbol}` };

    const dir = normaliseSide(side);
    if (!dir) return { ok: false, reason: `invalid side ${side}` };

    const lots = normaliseLot(symbol, volume);
    if (lots <= 0) return { ok: false, reason: `volume ${volume} below broker minimum` };

    const reference = Number.isFinite(entryPrice) ? entryPrice : this.lastPrice.get(symbol);
    if (!Number.isFinite(reference)) return { ok: false, reason: 'no reference price available' };

    await sleep(this.env.broker.PAPER_LATENCY_MS);

    // Re-validate the stop: a stop on the wrong side of the market would be
    // rejected by a real broker, so catch it here instead of silently failing.
    const fill = roundPrice(symbol, this.#fillPrice(symbol, dir, reference));
    if (sl != null) {
      const wrongSide = dir === 'BUY' ? sl >= fill : sl <= fill;
      if (wrongSide) return { ok: false, reason: `stop ${sl} is on the wrong side of ${fill}` };
    }

    const id = `P-${randomUUID().slice(0, 8)}`;
    const position = {
      id,
      signalId,
      symbol,
      display: spec.display,
      side: dir, // canonical direction: BUY / SELL
      requestedSide: String(side).toUpperCase(), // what the strategy said: LONG / SHORT
      volume: lots,
      entryPrice: fill,
      currentPrice: fill,
      sl: sl == null ? null : roundPrice(symbol, sl),
      tp: tp == null ? null : roundPrice(symbol, tp), // Leg 2 runs with NO fixed TP
      openAt: Date.now(),
      comment,
      mode: 'paper',
      legs: [],
      unrealised: 0,
      meta,
    };

    this.positions.set(id, position);
    this.stats.placed += 1;
    this.#record({ type: 'OPEN', positionId: id, symbol, side: dir, volume: lots, price: fill });
    this.onFill({ action: 'open', position });
    return { ok: true, position: { ...position } };
  }

  /** Move the stop (the trailing stop calls this as the channel ratchets). */
  async modifyPosition({ positionId, sl, tp = undefined }) {
    const p = this.positions.get(positionId);
    if (!p) return { ok: false, reason: `unknown position ${positionId}` };

    if (sl != null) {
      const next = roundPrice(p.symbol, sl);
      const wrongSide = p.side === 'BUY' ? next >= p.currentPrice : next <= p.currentPrice;
      // A stop that would close at a loss *now* is still legal (it can only be
      // triggered below/above the market), but a stop beyond the far side of the
      // book is a bug — reject it loudly rather than silently.
      if (Number.isFinite(p.entryPrice) && p.side === 'BUY' && next > p.entryPrice * 1.5) {
        return { ok: false, reason: 'refusing implausible stop' };
      }
      p.sl = next;
      this.stats.modified += 1;
    }
    if (tp !== undefined) p.tp = tp == null ? null : roundPrice(p.symbol, tp);
    this.#record({ type: 'MODIFY', positionId, sl: p.sl, tp: p.tp });
    this.onFill({ action: 'modify', position: { ...p } });
    return { ok: true, position: { ...p } };
  }

  /**
   * Close all or part of a position.
   * @param {number} [volume] omit to close the whole position
   */
  async closePosition({ positionId, volume, reason = 'manual' }) {
    const p = this.positions.get(positionId);
    if (!p) return { ok: false, reason: `unknown position ${positionId}` };

    const reference = this.lastPrice.get(p.symbol) ?? p.currentPrice;
    const exit = roundPrice(p.symbol, this.#fillPrice(p.symbol, p.side === 'BUY' ? 'SELL' : 'BUY', reference));
    const closeLots = volume == null ? p.volume : normaliseLot(p.symbol, Math.min(volume, p.volume));
    if (closeLots <= 0) return { ok: false, reason: 'nothing left to close' };

    await sleep(Math.round(this.env.broker.PAPER_LATENCY_MS / 2));

    const points = profitPoints(p.side, p.entryPrice, exit);
    const profit = pointsValue(p.symbol, points, closeLots);

    p.volume = normaliseLot(p.symbol, Number((p.volume - closeLots).toFixed(2)));
    p.currentPrice = exit;

    const remaining = p.volume;
    if (remaining <= 0) this.positions.delete(positionId);

    this.stats.closed += 1;
    const record = this.#record({
      type: 'CLOSE',
      positionId,
      symbol: p.symbol,
      side: p.side,
      volume: closeLots,
      price: exit,
      profit: Number(profit.toFixed(2)),
      reason,
    });
    this.onFill({ action: 'close', position: { ...p, closedVolume: closeLots, exitPrice: exit, profit }, reason });

    return {
      ok: true,
      positionId,
      closedVolume: closeLots,
      remainingVolume: remaining,
      exitPrice: exit,
      profit: Number(profit.toFixed(2)),
      record,
    };
  }

  /** Open positions (or a single one). */
  listPositions(positionId = null) {
    const all = [...this.positions.values()].map((p) => ({ ...p }));
    return positionId ? all.filter((p) => p.id === positionId) : all;
  }

  account() {
    return { mode: 'paper', currency: 'USD', positions: this.positions.size };
  }

  status() {
    return { mode: 'paper', ...this.stats, openPositions: this.positions.size };
  }
}

/* =============================================================================
 *  MT5 BRIDGE BROKER — signed HTTP calls to an external bridge
 * ============================================================================= */

/**
 * Minimal HMAC-SHA256 signing (node:crypto only — no extra dependency):
 *   signature = hex(hmac(secret, `${timestamp}.${body}`))
 * The bridge recomputes it and rejects anything that does not match, which is
 * what stops a leaked bridge URL from being usable by a third party.
 */
async function signRequest(secret, timestamp, body) {
  const { createHmac } = await import('node:crypto');
  return createHmac('sha256', secret).update(`${timestamp}.${body}`).digest('hex');
}

export class Mt5BridgeBroker {
  constructor({ env = ENV, onFill = () => {} } = {}) {
    this.mode = 'mt5';
    this.env = env;
    this.onFill = onFill;
    this.positions = new Map(); // mirrored from the bridge's responses
    /** Last price seen per symbol — used to mirror the mark-to-market value. */
    this.lastPrice = new Map();
    this.stats = { placed: 0, modified: 0, closed: 0, rejected: 0 };
  }

  markPrice(symbol, price) {
    if (Number.isFinite(price)) this.lastPrice.set(symbol, price);
  }

  async #call(path, payload) {
    if (!this.env.broker.BASE_URL || !this.env.broker.TOKEN) {
      throw new Error('BROKER_MODE=mt5 requires BROKER_BASE_URL and BROKER_TOKEN');
    }
    const body = JSON.stringify(payload);
    const timestamp = String(Date.now());
    const signature = await signRequest(this.env.broker.TOKEN, timestamp, body);

    const ctl = new AbortController();
    const timer = setTimeout(() => ctl.abort(), this.env.broker.TIMEOUT_MS);
    try {
      const res = await fetch(`${this.env.broker.BASE_URL}${path}`, {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          'x-aetheron-timestamp': timestamp,
          'x-aetheron-signature': signature,
        },
        body,
        signal: ctl.signal,
      });
      const text = await res.text();
      if (!res.ok) throw new Error(`bridge ${path} → HTTP ${res.status}: ${text.slice(0, 200)}`);
      return text ? JSON.parse(text) : {};
    } finally {
      clearTimeout(timer);
    }
  }

  async placeOrder({ signalId, symbol, side, volume, entryPrice, sl = null, tp = null, comment = '', meta = {} }) {
    const lots = normaliseLot(symbol, volume);
    if (lots <= 0) return { ok: false, reason: `volume ${volume} below broker minimum` };

    const dir = normaliseSide(side);
    if (!dir) return { ok: false, reason: `invalid side ${side}` };

    // MT5 order tickets speak BUY/SELL, so translate LONG/SHORT here.
    const r = await this.#call('/order', {
      signalId,
      symbol,
      side: dir,
      volume: lots,
      type: 'market', // market entry — the engine decides when, never the client
      sl,
      tp,
      comment,
      meta,
    });
    if (!r?.ticket) return { ok: false, reason: r?.error ?? 'bridge returned no ticket' };

    const position = {
      id: String(r.ticket),
      signalId,
      symbol,
      side: dir,
      requestedSide: String(side).toUpperCase(),
      volume: lots,
      entryPrice: Number(r.price ?? entryPrice),
      currentPrice: Number(r.price ?? entryPrice),
      sl: r.sl ?? sl,
      tp: r.tp ?? tp,
      openAt: Date.now(),
      comment,
      mode: 'mt5',
      legs: [],
      unrealised: 0,
      meta,
    };
    this.positions.set(position.id, position);
    this.stats.placed += 1;
    this.onFill({ action: 'open', position: { ...position } });
    return { ok: true, position: { ...position } };
  }

  async modifyPosition({ positionId, sl, tp = undefined }) {
    const r = await this.#call('/modify', { ticket: positionId, sl, tp });
    if (r?.error) return { ok: false, reason: r.error };
    const p = this.positions.get(positionId);
    if (p) {
      if (sl != null) p.sl = sl;
      if (tp !== undefined) p.tp = tp;
    }
    this.stats.modified += 1;
    this.onFill({ action: 'modify', position: p ? { ...p } : { id: positionId, sl, tp } });
    return { ok: true, position: p ? { ...p } : { id: positionId, sl, tp } };
  }

  async closePosition({ positionId, volume, reason = 'manual' }) {
    const r = await this.#call('/close', { ticket: positionId, volume: volume ?? null, reason });
    if (r?.error) return { ok: false, reason: r.error };

    const p = this.positions.get(positionId);
    const remaining = normaliseLot(p?.symbol ?? 'XAUUSD', Number((p?.volume ?? 0) - (volume ?? p?.volume ?? 0)));
    if (p && remaining <= 0) this.positions.delete(positionId);
    this.stats.closed += 1;
    this.onFill({ action: 'close', position: { ...(p ?? { id: positionId }), exitPrice: r.price, profit: r.profit }, reason });
    return { ok: true, positionId, exitPrice: r.price, profit: r.profit, remainingVolume: remaining };
  }

  listPositions(positionId = null) {
    const all = [...this.positions.values()].map((p) => ({ ...p }));
    return positionId ? all.filter((p) => p.id === positionId) : all;
  }

  account() {
    return { mode: 'mt5', positions: this.positions.size };
  }

  status() {
    return { mode: 'mt5', ...this.stats, openPositions: this.positions.size, endpoint: this.env.broker.BASE_URL };
  }
}

/* =============================================================================
 *  FACTORY
 * ============================================================================= */

/**
 * Build the broker from configuration. An unknown mode fails closed to
 * `paper` — never to a live broker by accident.
 *
 * @returns {PaperBroker|Mt5BridgeBroker}
 */
export function createBroker({ env = ENV, onFill = () => {} } = {}) {
  const mode = String(env.broker.MODE).toLowerCase();
  if (mode === 'mt5') {
    if (!env.broker.BASE_URL || !env.broker.TOKEN) {
      log.warn('BROKER_MODE=mt5 without BROKER_BASE_URL/TOKEN — falling back to paper mode');
    } else {
      log.ok(`MT5 bridge broker → ${env.broker.BASE_URL}`);
      return new Mt5BridgeBroker({ env, onFill });
    }
  }
  log.info('paper broker (simulated fills)');
  return new PaperBroker({ env, onFill });
}

export default createBroker;