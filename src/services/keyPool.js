/* =============================================================================
 *  AETHERON CORE  —  src/services/keyPool.js
 *  -----------------------------------------------------------------------------
 *  A rotating pool of provider API keys.
 *
 *  WHY
 *  ---
 *  One free-tier key is a single point of failure: when its daily budget is
 *  spent every analysis fails until midnight UTC. Two keys from two accounts
 *  double the budget, and the analyst should use the second one the moment the
 *  first answers 429 — automatically, without a redeploy.
 *
 *  RULES
 *  -----
 *    1. Keys are tried in order, wrapping around. `active` remembers the last
 *       slot used so every cycle does not start on the same key.
 *    2. A rate-limited key is parked in a cooldown. The duration is inferred
 *       from the provider's own words: daily limits sleep until 00:00 UTC,
 *       per-minute limits sleep a minute, anything else five minutes.
 *    3. `current()` throws when every key is cooling. That is a real quota
 *       wall, and the caller should back the whole cycle off.
 *    4. Key material never leaves this module through status(): callers get
 *       slot counts, never values.
 * ============================================================================= */

/** Milliseconds until the next 00:00 UTC — when free daily counters reset. */
export function msUntilUtcMidnight(now = Date.now()) {
  const d = new Date(now);
  return Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate() + 1) - now;
}

/**
 * How long a key should rest after a quota error, based on the provider message.
 *
 * Groq says "tokens per day (TPD)" / "requests per day (RPD)"; OpenRouter says
 * "free-models-per-day". Per-minute limits (TPM/RPM) recover in a minute and are
 * not worth parking a key for a whole day.
 *
 * @param {string} message
 * @param {number} [now]
 * @returns {number} milliseconds
 */
export function cooldownForMessage(message, now = Date.now()) {
  const m = String(message || '');
  if (/per.?day|daily|\bTPD\b|\bRPD\b|tokens per day|requests per day|free-models-per-day/i.test(m)) {
    return Math.max(60_000, msUntilUtcMidnight(now));
  }
  if (/per.?minute|\bTPM\b|\bRPM\b|tokens per minute|requests per minute/i.test(m)) return 60_000;
  return 5 * 60_000;
}

export class KeyPool {
  /** @param {string[]} keys */
  constructor(keys = []) {
    const list = Array.isArray(keys) ? keys : [keys];
    this.keys = [...new Set(list.map((k) => String(k || '').trim()).filter(Boolean))];
    /** slot index → epoch ms before which the key must not be used */
    this.cooldownUntil = new Map();
    /** slot index of the most recently used key */
    this.active = 0;
    /** how many times a key was parked because of a quota/credential error */
    this.rotations = 0;
  }

  get size() {
    return this.keys.length;
  }

  hasKeys() {
    return this.keys.length > 0;
  }

  #usable(index, now) {
    return (this.cooldownUntil.get(index) ?? 0) <= now;
  }

  /** Slot indices that are not cooling down, in order. */
  available(now = Date.now()) {
    return this.keys.map((_, i) => i).filter((i) => this.#usable(i, now));
  }

  hasAvailable(now = Date.now()) {
    return this.available(now).length > 0;
  }

  /**
   * The key to use next. Throws when every key is cooling down — the caller
   * treats that as "the whole account pool is out of quota".
   *
   * `force` is the operator override used by a manual analysis: wake the slot
   * that has been parked longest even though its cooldown has not expired. A
   * scheduled cycle never forces, so automatic traffic respects the park.
   *
   * @param {number} [now]
   * @param {{force?: boolean}} [opts]
   * @returns {{key: string, index: number, slot: number, forced?: boolean}}
   */
  current(now = Date.now(), { force = false } = {}) {
    if (!this.keys.length) throw new Error('no API key configured');
    const avail = this.available(now);
    if (!avail.length) {
      if (!force) throw new Error('all API keys are cooling down');
      // Longest-parked first: it is the most likely to have recovered.
      const parked = [...this.cooldownUntil.entries()].sort((a, b) => a[1] - b[1]);
      const index = parked.length ? parked[0][0] : this.active % this.keys.length;
      this.active = index;
      return { key: this.keys[index], index, slot: index + 1, forced: true };
    }
    // Prefer the first slot at/after the last one used, so a working key is not
    // re-tried before an untested one.
    const index = avail.find((i) => i >= this.active) ?? avail[0];
    this.active = index;
    return { key: this.keys[index], index, slot: index + 1 };
  }

  /**
   * Park `key` for `ms` and move the cursor past it.
   * Called for quota errors (429/402) and for rejected credentials (401/403).
   */
  markLimited(key, ms = 5 * 60_000, now = Date.now()) {
    const index = this.keys.indexOf(key);
    if (index === -1) return;
    this.cooldownUntil.set(index, now + Math.max(1_000, Number(ms) || 0));
    this.rotations += 1;
    this.active = index + 1;
  }

  /**
   * A successful call clears THIS key's park — whichever slot answered is
   * demonstrably alive. Other slots keep their own cooldowns untouched.
   */
  markOk(key) {
    const index = this.keys.indexOf(key);
    if (index !== -1) this.cooldownUntil.delete(index);
  }

  /** Observability — never includes key material. */
  status(now = Date.now()) {
    const available = this.available(now).length;
    return {
      total: this.keys.length,
      available,
      cooling: this.keys.length - available,
      rotations: this.rotations,
      activeSlot: this.keys.length ? (this.active % this.keys.length) + 1 : 0,
    };
  }
}

export default KeyPool;
