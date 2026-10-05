/* =============================================================================
 *  AETHERON CORE  —  src/services/sseHub.js
 *  -----------------------------------------------------------------------------
 *  Dependency-free Server-Sent Events hub, extended with CHANNELS so one module
 *  can serve three very different downstream feeds:
 *
 *    'market'  → snapshot + coalesced candle updates (public chart dashboard)
 *    'prices'  → clean price ticks for the Android TradingView chart
 *    'chat'    → JSON chat-ops messages (EXECUTION / SIGNAL / HEALTH / …)
 *    'chat:<userId>' → the same chat feed, scoped to one authenticated user
 *
 *  WHY CHANNELS MATTER
 *  -------------------
 *  A price tick every 400 ms and an execution confirmation every few hours have
 *  opposite traffic shapes. Mixing them on one socket means the chat client
 *  parses market data and the chart client wakes up for a trade message.
 *  Subscribing to a channel keeps each client on one payload shape — and lets a
 *  user's chat stream be private without a second hub instance.
 *
 *  LONGEVITY
 *  ---------
 *  SSE over Render/nginx dies when the connection looks idle, so a comment
 *  frame is emitted every HEARTBEAT_MS, `retry:` tells the client how fast to
 *  come back, and clients whose socket buffer overruns MAX_BUFFER_BYTES are
 *  skipped instead of being allowed to grow the heap.
 * ============================================================================= */

import { randomUUID } from 'node:crypto';
import { ENV } from '../config/env.js';

const stamp = () => new Date().toISOString().replace('T', ' ').slice(0, 23);
const log = (...a) => console.log(`[${stamp()}]`, ...a);
const warn = (...a) => console.warn(`[${stamp()}] ⚠ `, ...a);

export class SSEHub {
  /**
   * @param {object} [opts]
   * @param {number} [opts.retryMs]
   * @param {number} [opts.maxBufferBytes]
   */
  constructor({ retryMs = ENV.stream.RETRY_MS, maxBufferBytes = ENV.stream.MAX_BUFFER_BYTES, env = ENV } = {}) {
    /** @type {Set<object>} */
    this.clients = new Set();
    this.seq = 0;
    this.retryMs = retryMs;
    this.maxBufferBytes = env.stream.MAX_BUFFER_BYTES ?? maxBufferBytes;
    this.env = env;
    this.totalFrames = 0;
  }

  get count() {
    return this.clients.size;
  }

  /** Count of clients subscribed to at least one of `channels`. */
  countFor(channels) {
    const wanted = new Set([].concat(channels));
    let n = 0;
    for (const c of this.clients) {
      for (const ch of c.channels) if (wanted.has(ch)) { n += 1; break; }
    }
    return n;
  }

  /**
   * Register a client on one or more channels.
   *
   * @param {import('express').Request} req
   * @param {import('express').Response} res
   * @param {object} [opts]
   * @param {string[]} [opts.channels]   channels to subscribe to
   * @param {string} [opts.name]         label for /health
   * @param {object} [opts.user]         authenticated user, when applicable
   * @param {(item:object)=>boolean} [opts.filter]  per-item predicate
   * @param {object} [opts.meta]         free-form extra for /health
   */
  add(req, res, { channels = ['market'], name = 'client', user = null, filter = null, meta = {} } = {}) {
    res.writeHead(200, {
      'Content-Type': 'text/event-stream; charset=utf-8',
      'Cache-Control': 'no-cache, no-transform',
      Connection: 'keep-alive',
      'X-Accel-Buffering': 'no', // nginx buffering would defeat SSE entirely
    });
    res.flushHeaders?.();
    // Nagle would batch small frames for tens of ms — defeats the point.
    req.socket?.setNoDelay(true);
    req.socket?.setKeepAlive(true);

    const client = {
      id: randomUUID().slice(0, 8),
      res,
      req,
      name,
      user,
      channels: new Set([].concat(channels)),
      filter,
      connectedAt: Date.now(),
      sent: 0,
      dropped: 0,
      meta,
      filterLabel: '',
    };
    this.setFilterLabel(client);

    res.write(`retry: ${this.retryMs}\n\n`);
    this.clients.add(client);
    log(`＋ ${name} client ${client.id} connected (${[...client.channels].join(',')}) — total: ${this.count}`);

    const cleanup = () => this.remove(client);
    req.on('close', cleanup);
    req.on('error', cleanup);
    res.on('close', cleanup);
    res.on('error', cleanup);

    return client;
  }

  /** Refresh a client's filter description (shown on /health). */
  setFilterLabel(client) {
    const q = client.req?.query ?? {};
    const parts = [];
    if (q.symbol) parts.push(String(q.symbol).toUpperCase());
    if (q.interval) parts.push(`${q.interval}s`);
    client.filterLabel = parts.length ? parts.join('|') : 'all';
  }

  remove(client) {
    if (this.clients.delete(client)) {
      log(`－ ${client.name} client ${client.id} disconnected — total: ${this.count}`);
    }
  }

  /** Count clients whose channel list satisfies `predicate(ch)`. */
  countWhere(predicate) {
    let n = 0;
    for (const c of this.clients) {
      for (const ch of c.channels) {
        if (predicate(ch)) { n += 1; break; }
      }
    }
    return n;
  }

  /**
   * Write one SSE frame to a single client, with back-pressure protection.
   * A client that cannot keep up is skipped frame-by-frame rather than
   * disconnected — it catches up on the next snapshot.
   */
  sendTo(client, event, data) {
    if (client.res.writableEnded) return this.remove(client);
    if (client.res.writableLength > this.maxBufferBytes) {
      client.dropped += 1;
      if (client.dropped === 1 || client.dropped % 50 === 0) {
        warn(`${client.name} client ${client.id} is too slow — dropping frames (${client.dropped})`);
      }
      return;
    }
    this.seq += 1;
    this.totalFrames += 1;
    client.sent += 1;
    try {
      client.res.write(`id: ${this.seq}\nevent: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
    } catch (e) {
      warn(`write to ${client.id} failed: ${e.message}`);
      this.remove(client);
    }
  }

  /**
   * Publish to every client subscribed to one of `channels`.
   *
   * @param {string|string[]} channels
   * @param {string} event
   * @param {object} data
   * @param {object} [opts]
   * @param {(item:object)=>boolean} [opts.requiresFilter] only send when the
   *        client's filter accepts at least one item (mirrors the legacy
   *        `update` behaviour, which bandwides 16 series down to one)
   */
  publish(channels, event, data, { requiresFilter = null } = {}) {
    if (this.clients.size === 0) return 0;
    const wanted = new Set([].concat(channels));
    let delivered = 0;

    for (const client of this.clients) {
      // `*` (used by broadcast()) means "every client, whatever it subscribed to".
      if (!client.channels.has('*') && !this.isSubscribed(client, channels)) continue;

      // Item-level filtering (e.g. ?symbol=&interval=)
      if (requiresFilter && client.filter && Array.isArray(data.items)) {
        const items = data.items.filter((i) => client.filter(i));
        if (items.length === 0) continue;
        this.sendTo(client, event, { ...data, items });
        delivered += 1;
        continue;
      }

      this.sendTo(client, event, data);
      delivered += 1;
    }
    return delivered;
  }

  /**
   * Legacy helper: broadcast to every client regardless of channel.
   * Kept so the original single-channel `/stream` behaviour is unchanged.
   */
  broadcast(event, data, opts = {}) {
    return this.publish('*', event, data, opts);
  }

  /** `*` is a wildcard channel membership for `broadcast()`. */
  isSubscribed(client, channels) {
    if (client.channels.has('*')) return true;
    const wanted = new Set([].concat(channels));
    for (const ch of client.channels) if (wanted.has(ch)) return true;
    return false;
  }

  /** SSE comment frames — keeps idle connections alive through proxies. */
  heartbeat() {
    for (const client of this.clients) {
      if (client.res.writableEnded) continue;
      try {
        client.res.write(`: heartbeat ${Date.now()}\n\n`);
      } catch {
        this.remove(client);
      }
    }
  }

  closeAll() {
    for (const client of this.clients) {
      try {
        client.res.end();
      } catch {
        /* ignore */
      }
    }
    this.clients.clear();
  }

  stats() {
    return [...this.clients].map((c) => ({
      id: c.id,
      name: c.name,
      channels: [...c.channels],
      filter: c.filterLabel,
      user: c.user?.id ?? null,
      uptimeSec: Math.round((Date.now() - c.connectedAt) / 1000),
      framesSent: c.sent,
      framesDropped: c.dropped,
    }));
  }

  get summary() {
    const byChannel = {};
    for (const c of this.clients) {
      for (const ch of c.channels) byChannel[ch] = (byChannel[ch] ?? 0) + 1;
    }
    return { clients: this.count, byChannel, totalFrames: this.totalFrames };
  }
}

export default SSEHub;