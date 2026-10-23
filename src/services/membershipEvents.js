/* =============================================================================
 *  AETHERON CORE  —  src/services/membershipEvents.js
 *  -----------------------------------------------------------------------------
 *  A tiny per-device SSE fan-out. The public `/stream` is shared by every
 *  client, so publishing "MONTHLY activated for DEVICE-123" there would leak
 *  one user's account state to everyone. This hub keeps a separate set of
 *  listeners keyed by device id; only approved subscribers receive the event.
 *
 *  The stream is optional. The app can always poll GET /api/membership/me,
 *  which is the source of truth. Telegram is the immediate out-of-band nudge.
 * ============================================================================= */

export class MembershipEvents {
  constructor() {
    /** deviceId → Set<{ res, since }> */
    this.clients = new Map();
    this.counter = 0;
  }

  /**
   * Attach one SSE response to a device.
   * @param {string} deviceId
   * @param {import('http').ServerResponse} res
   * @returns {{ close: () => void }}
   */
  subscribe(deviceId, res) {
    const id = String(deviceId);
    let set = this.clients.get(id);
    if (!set) {
      set = new Set();
      this.clients.set(id, set);
    }
    const entry = { res, since: Date.now() };
    set.add(entry);
    this.counter += 1;

    const close = () => {
      set.delete(entry);
      if (set.size === 0) this.clients.delete(id);
    };
    return { close };
  }

  /** Send one named event to every live listener for this device. */
  publish(deviceId, event, data) {
    const set = this.clients.get(String(deviceId));
    if (!set || set.size === 0) return 0;
    const frame = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
    let sent = 0;
    for (const entry of set) {
      try {
        entry.res.write(frame);
        sent += 1;
      } catch {
        set.delete(entry);
      }
    }
    if (set.size === 0) this.clients.delete(String(deviceId));
    return sent;
  }

  countFor(deviceId) {
    return this.clients.get(String(deviceId))?.size ?? 0;
  }

  get totalClients() {
    let total = 0;
    for (const set of this.clients.values()) total += set.size;
    return total;
  }

  get devices() {
    return this.clients.size;
  }

  closeAll() {
    for (const set of this.clients.values()) {
      for (const entry of set) {
        try {
          entry.res.end();
        } catch {
          /* already gone */
        }
      }
    }
    this.clients.clear();
  }
}

export default MembershipEvents;
