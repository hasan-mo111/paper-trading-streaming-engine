/* =============================================================================
 *  AETHERON CORE  —  src/services/economicNews.js
 *  -----------------------------------------------------------------------------
 *  Economic-calendar reader for the AI analyst.
 *
 *  WHY THIS EXISTS
 *  ---------------
 *  A pure price model cannot know that a 12:30 UTC Core CPI print is about to
 *  hit the tape, yet that single release is what turns a textbook breakout into
 *  a 40-dollar gold spike and back. Feeding the model the upcoming high-impact
 *  events gives its "do not force a trade" rule something concrete to act on.
 *
 *  SOURCE
 *  ------
 *  Forex Factory's public weekly calendar XML. No API key, no quota, one small
 *  document a week — the right trade-off for a free deployment. The feed is
 *  served as windows-1252, so it is decoded explicitly rather than trusting the
 *  default UTF-8 path (accented event titles would otherwise mangle).
 *
 *  TIME ZONE (important)
 *  ---------------------
 *  The feed's <time> column is UTC in the documents we receive — verified
 *  against releases whose real schedule is fixed (US CPI/Retail Sales/PPI all
 *  land at 12:30, i.e. 08:30 New York during EDT). If a future feed changes the
 *  reference zone, AI_NEWS_TZ_OFFSET_MIN corrects it without a code change.
 *
 *  FAILURE POLICY
 *  --------------
 *  The calendar is an ENHANCEMENT, never a dependency: every failure path keeps
 *  the previous events (or an empty list) and the analyst simply runs without
 *  news context. Nothing here can stall a signal cycle.
 * ============================================================================= */

import { ENV } from '../config/env.js';
import { createLogger } from './logger.js';

const log = createLogger('news');

/** Impact buckets the feed uses, ordered by how much they move a price. */
export const IMPACT_RANK = Object.freeze({
  HIGH: 3,
  MEDIUM: 2,
  LOW: 1,
  HOLIDAY: 0,
  'NON-ECONOMIC': 0,
});

/** Default source. Overridable so a mirror can be swapped in at runtime. */
export const DEFAULT_FEED_URL = 'https://nfs.faireconomy.media/ff_calendar_thisweek.xml';

/* ------------------------------------------------------------------ parsing */

/**
 * Pull one `<tag>…</tag>` value out of an `<event>` block, unwrapping CDATA.
 * Hand-rolled on purpose: the document is small, flat and stable, and pulling
 * in an XML parser for nine fields would dwarf the code that uses it.
 *
 * @param {string} block
 * @param {string} name
 * @returns {string} trimmed text, or '' when the tag is absent/self-closing
 */
function tag(block, name) {
  const m = new RegExp(`<${name}>([\\s\\S]*?)<\\/${name}>`, 'i').exec(block);
  if (!m) return '';
  return m[1].replace(/<!\[CDATA\[([\s\S]*?)\]\]>/g, '$1').trim();
}

/**
 * Turn the feed's `<date>MM-DD-YYYY</date>` + `<time>h:mmam</time>` pair into an
 * absolute instant.
 *
 * @param {string} date
 * @param {string} time
 * @param {number} tzOffsetMin minutes EAST of UTC that `time` is expressed in
 * @returns {Date|null} null for all-day markers ("All Day", "Tentative", "Day 1")
 */
export function parseWhen(date, time, tzOffsetMin = 0) {
  const d = /^(\d{2})-(\d{2})-(\d{4})$/.exec(String(date).trim());
  if (!d) return null;
  const t = /^(\d{1,2}):(\d{2})\s*(am|pm)$/i.exec(String(time).trim());
  if (!t) return null;

  let hours = Number(t[1]) % 12;
  if (/pm/i.test(t[3])) hours += 12;
  const minutes = Number(t[2]);

  const ms = Date.UTC(Number(d[3]), Number(d[1]) - 1, Number(d[2]), hours, minutes) - tzOffsetMin * 60_000;
  return new Date(ms);
}

/**
 * Parse the weekly calendar document into flat events.
 *
 * @param {string} xml
 * @param {object} [opts]
 * @param {number} [opts.tzOffsetMin] minutes east of UTC the feed times use
 * @returns {Array<{title:string,country:string,impact:string,at:Date,forecast:string,previous:string,url:string}>}
 */
export function parseCalendar(xml, { tzOffsetMin = 0 } = {}) {
  const events = [];
  const blocks = String(xml).match(/<event>[\s\S]*?<\/event>/gi) ?? [];

  for (const block of blocks) {
    const at = parseWhen(tag(block, 'date'), tag(block, 'time'), tzOffsetMin);
    if (!at || Number.isNaN(at.getTime())) continue;

    const impact = tag(block, 'impact').toUpperCase() || 'LOW';
    events.push({
      title: tag(block, 'title'),
      country: tag(block, 'country').toUpperCase(),
      impact,
      at,
      forecast: tag(block, 'forecast'),
      previous: tag(block, 'previous'),
      url: tag(block, 'url'),
    });
  }

  events.sort((a, b) => a.at - b.at);
  return events;
}

/* -------------------------------------------------------------------- service */

export class EconomicNewsService {
  /**
   * @param {object} [opts]
   * @param {object} [opts.env]       config override (tests)
   * @param {Function} [opts.fetchFn] fetch implementation (tests)
   */
  constructor({ env = ENV, fetchFn = null } = {}) {
    this.cfg = { ...NEWS_DEFAULTS, ...((env && env.news) || {}) };
    this.fetchFn = fetchFn ?? globalThis.fetch;
    this.enabled = Boolean(this.cfg.ENABLED);

    /** @type {Array<object>} last successfully parsed calendar */
    this.events = [];
    this.fetchedAt = 0;
    this.lastError = null;
    this.fetches = 0;
    this.failures = 0;
    /** In-flight promise so N callers share one HTTP request. */
    this.inflight = null;
    this.timer = null;
  }

  /* ------------------------------------------------------------- lifecycle */

  /** Refresh once at boot so the very first analysis already has context. */
  start() {
    if (!this.enabled) {
      log.info('economic calendar disabled (AI_NEWS_ENABLED=false)');
      return;
    }
    this.ensureFresh().catch(() => {});
    // A slow background refresh keeps the window fresh across the week without
    // any caller having to think about staleness.
    this.timer = setInterval(() => {
      this.ensureFresh(Date.now(), { force: true }).catch(() => {});
    }, this.cfg.TTL_MS);
    this.timer.unref?.();
    log.ok(`economic calendar armed — ${this.cfg.MIN_IMPACT}+ events within ${this.cfg.HORIZON_H}h`);
  }

  stop() {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
  }

  /* ---------------------------------------------------------------- fetching */

  /**
   * Make sure the cached window is fresh, fetching when it is not.
   * Concurrent callers share a single request; a failed fetch keeps the
   * previous events so the analyst never regresses to "no news".
   *
   * @returns {Promise<EconomicNewsService>}
   */
  async ensureFresh(now = Date.now(), { force = false } = {}) {
    if (!this.enabled) return this;
    const fresh = this.events.length > 0 && now - this.fetchedAt < this.cfg.TTL_MS;
    if (fresh && !force) return this;
    if (this.inflight) return this.inflight;

    this.inflight = this.#load()
      .catch((e) => {
        this.failures += 1;
        this.lastError = e.message;
        // Record the attempt so a dead feed is retried on the TTL cadence
        // rather than on every single cycle.
        this.fetchedAt = now;
        log.warn('economic calendar refresh failed:', e.message);
      })
      .finally(() => {
        this.inflight = null;
      });
    return this.inflight;
  }

  async #load() {
    const res = await this.fetchFn(this.cfg.URL, {
      headers: { accept: 'application/xml,text/xml,*/*' },
      signal: AbortSignal.timeout?.(this.cfg.TIMEOUT_MS),
    });
    if (!res.ok) throw new Error(`feed responded ${res.status}`);

    // windows-1252, not UTF-8 — decode explicitly, but never let a decoding
    // surprise kill the refresh.
    let xml;
    if (typeof res.arrayBuffer === 'function') {
      const buf = await res.arrayBuffer();
      try {
        xml = new TextDecoder('windows-1252').decode(buf);
      } catch {
        xml = new TextDecoder('utf-8').decode(buf);
      }
    } else {
      xml = await res.text();
    }

    const events = parseCalendar(xml, { tzOffsetMin: this.cfg.TZ_OFFSET_MIN });
    if (events.length === 0) throw new Error('calendar parsed to zero events');

    this.events = events;
    this.fetchedAt = Date.now();
    this.lastError = null;
    this.fetches += 1;
    log.ok(`economic calendar loaded — ${events.length} events this week`);
    return this;
  }

  /* --------------------------------------------------------------- querying */

  /**
   * Events matching the configured filter, soonest first.
   *
   * @param {object} [opts]
   * @param {number} [opts.now]        epoch ms
   * @param {number} [opts.withinHours]
   * @param {string} [opts.minImpact]
   * @returns {Array<object>}
   */
  upcoming({ now = Date.now(), withinHours = this.cfg.HORIZON_H, minImpact = this.cfg.MIN_IMPACT } = {}) {
    const floor = IMPACT_RANK[minImpact] ?? IMPACT_RANK.HIGH;
    const until = now + withinHours * 3_600_000;
    return this.events
      .filter((e) => e.at.getTime() >= now - 60_000 && e.at.getTime() <= until)
      .filter((e) => (IMPACT_RANK[e.impact] ?? 0) >= floor)
      .filter((e) => this.#countryWanted(e.country))
      .slice(0, this.cfg.MAX_EVENTS);
  }

  #countryWanted(country) {
    const want = this.cfg.COUNTRIES;
    if (!want.length) return true;
    if (want.includes('ALL')) return true;
    // The feed uses "All" for globally-relevant items (IMF meetings, summits).
    return country === 'ALL' || want.includes(country);
  }

  /**
   * Compact lines for the model prompt. Kept terse on purpose: every extra
   * token here is a token the free-tier model does not spend on the analysis.
   *
   * @returns {string[]}
   */
  forPrompt(opts) {
    return this.upcoming(opts).map((e) => {
      const when = e.at.toISOString().slice(0, 16).replace('T', ' ') + 'Z';
      const num = [e.forecast && `f/c ${e.forecast}`, e.previous && `prev ${e.previous}`]
        .filter(Boolean)
        .join(', ');
      return `${when} ${e.country} · ${e.title} (${e.impact}${num ? `; ${num}` : ''})`;
    });
  }

  /** Observability for /health. */
  status() {
    const next = this.upcoming({ withinHours: 24 * 7, minImpact: 'HIGH' })[0] ?? null;
    return {
      enabled: this.enabled,
      source: this.cfg.URL,
      events: this.events.length,
      fetches: this.fetches,
      failures: this.failures,
      lastError: this.lastError,
      ageMs: this.fetchedAt ? Date.now() - this.fetchedAt : null,
      freshnessMs: this.cfg.TTL_MS,
      horizonH: this.cfg.HORIZON_H,
      minImpact: this.cfg.MIN_IMPACT,
      countries: this.cfg.COUNTRIES,
      nextHighImpact: next ? { title: next.title, country: next.country, at: next.at.toISOString() } : null,
    };
  }
}

/** Defaults when a partial env is supplied (tests / missing config). */
export const NEWS_DEFAULTS = Object.freeze({
  ENABLED: true,
  URL: DEFAULT_FEED_URL,
  TTL_MS: 3 * 3_600_000,
  TIMEOUT_MS: 15_000,
  HORIZON_H: 24,
  MIN_IMPACT: 'HIGH',
  COUNTRIES: ['USD', 'ALL'],
  TZ_OFFSET_MIN: 0,
  MAX_EVENTS: 8,
});

export default EconomicNewsService;
