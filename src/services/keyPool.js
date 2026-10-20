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
 *    2. Cooldowns are tracked at two levels. A rejected credential (401/403) or
 *       an account-wide limit parks the whole KEY. A per-model limit — Groq's
 *       "Rate limit reached for model X on tokens per day" — parks only that
 *       (key, model) PAIR, because the same account can still serve the other
 *       models and a second account has its own bucket for the same model.
 *    3. The park duration is inferred from the provider's own words: daily
 *       limits sleep until 00:00 UTC, per-minute limits sleep a minute,
 *       anything else five minutes. A decommissioned model id is disabled for
 *       six hours so it is not re-probed every cycle.
 *    4. `current()` throws when no usable (key, model) pair remains. That is a
 *       real quota wall, and the caller should back the whole cycle off.
 *    5. Key material never leaves this module through status(): callers get
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
    /** slot index → epoch ms before which the key must not be used (all models) */
    this.cooldownUntil = new Map();
    /** `${slot}|${model}` → epoch ms before which that pair must not be used */
    this.modelCooldown = new Map();
    /** model id → epoch ms before which the model must not be used at all */
    this.modelDisabled = new Map();
    /** slot index of the most recently used key */
    this.active = 0;
    /** how many times a slot was parked because of a credential/quota error */
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

  #modelUsable(model, now) {
    if (!model) return true;
    return (this.modelDisabled.get(model) ?? 0) <= now;
  }

  #pairUsable(index, model, now) {
    if (!this.#usable(index, now)) return false;
    if (!model) return true;
    if (!this.#modelUsable(model, now)) return false;
    return (this.modelCooldown.get(`${index}|${model}`) ?? 0) <= now;
  }

  /** Slot indices that are not cooling down, in order. */
  available(now = Date.now()) {
    return this.keys.map((_, i) => i).filter((i) => this.#usable(i, now));
  }

  hasAvailable(now = Date.now()) {
    return this.available(now).length > 0;
  }

  /** Slot indices that can serve `model` (key park + pair park + model disable). */
  availableFor(model, now = Date.now()) {
    return this.keys.map((_, i) => i).filter((i) => this.#pairUsable(i, model, now));
  }

  hasModelAvailable(model, now = Date.now()) {
    return this.availableFor(model, now).length > 0;
  }

  /**
   * The key to use next. Throws when every key is cooling down — the caller
   * treats that as "the whole account pool is out of quota".
   *
   * `model` narrows the choice to a key that still has budget for THAT model.
   * Groq meters tokens per model per account, so key 1 can be exhausted on
   * llama-3.3 while still serving gpt-oss, and a second account has its own
   * bucket for the same model.
   *
   * `force` is the operator override used by a manual analysis: wake the slot
   * that has been parked longest even though its cooldown has not expired. A
   * scheduled cycle never forces, so automatic traffic respects the park.
   *
   * @param {number} [now]
   * @param {{force?: boolean, model?: string|null}} [opts]
   * @returns {{key: string, index: number, slot: number, forced?: boolean}}
   */
  current(now = Date.now(), { force = false, model = null } = {}) {
    if (!this.keys.length) throw new Error('no API key configured');
    const avail = this.availableFor(model, now);
    if (!avail.length) {
      if (!force) {
        throw new Error(
          model ? `every key is cooling down for ${model}` : 'all API keys are cooling down'
        );
      }
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
   * Park a credential or a (credential, model) pair.
   *
   *   • `model` given  → only that pair rests. Used for a per-model daily limit
   *     (Groq: "Rate limit reached for model X on tokens per day"). The key can
   *     still serve the other models, and a second account may serve this one.
   *   • `model` absent → the whole key rests. Used for a rejected credential or
   *     an account-wide limit.
   */
  markLimited(key, ms = 5 * 60_000, { model = null, now = Date.now() } = {}) {
    const index = this.keys.indexOf(key);
    if (index === -1) return;
    const until = now + Math.max(1_000, Number(ms) || 0);
    if (model) this.modelCooldown.set(`${index}|${model}`, until);
    else this.cooldownUntil.set(index, until);
    this.rotations += 1;
    this.active = index + 1;
  }

  /** Disable a model everywhere (a decommissioned id must not be retried daily). */
  markModelDisabled(model, ms = 6 * 3_600_000, now = Date.now()) {
    if (!model) return;
    this.modelDisabled.set(model, now + Math.max(1_000, Number(ms) || 0));
  }

  /**
   * A successful call clears THIS key's park (and the pair, when a model was
   * involved) — whichever slot answered is demonstrably alive. Other slots keep
   * their own cooldowns untouched.
   */
  markOk(key, { model = null } = {}) {
    const index = this.keys.indexOf(key);
    if (index === -1) return;
    this.cooldownUntil.delete(index);
    if (model) this.modelCooldown.delete(`${index}|${model}`);
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
      pairsCooling: [...this.modelCooldown.values()].filter((t) => t > now).length,
      modelsDisabled: [...this.modelDisabled.values()].filter((t) => t > now).length,
    };
  }
}

export default KeyPool;
