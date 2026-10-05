/* =============================================================================
 *  AETHERON CORE  —  src/services/logger.js
 *  -----------------------------------------------------------------------------
 *  Tiny levelled logger. Exists so that every module can log with a namespace
 *  without importing console formatting logic, and so that a single switch
 *  (LOG_LEVEL=warn) can quiet the tick hot path in production.
 * ============================================================================= */

const LEVELS = { debug: 10, info: 20, warn: 30, error: 40, silent: 99 };

const stamp = () => new Date().toISOString().replace('T', ' ').slice(0, 23);

/** Human-friendly elapsed time since boot: "3m 12s". */
export function uptimeLabel() {
  const s = Math.floor(process.uptime());
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  if (h) return `${h}h ${m}m`;
  if (m) return `${m}m ${s % 60}s`;
  return `${s}s`;
}

/**
 * Create a namespaced logger.
 * @param {string} name  prefix shown in every line, e.g. 'strategy'
 */
export function createLogger(name = 'aetheron') {
  const min = LEVELS[String(process.env.LOG_LEVEL ?? 'info').toLowerCase()] ?? LEVELS.info;

  const emit = (level, glyph, args) => {
    if (LEVELS[level] < min) return;
    const line = `[${stamp()}] ${glyph} [${name}]`;
    (level === 'error' ? console.error : level === 'warn' ? console.warn : console.log)(line, ...args);
  };

  return {
    debug: (...a) => emit('debug', '·', a),
    info: (...a) => emit('info', '›', a),
    /** Bare line without the namespace decoration (boot banners). */
    raw: (...a) => emit('info', ' ', a),
    ok: (...a) => emit('info', '✔', a),
    warn: (...a) => emit('warn', '⚠', a),
    error: (...a) => emit('error', '✖', a),
  };
}

/** Default logger for modules that are not given one. */
export const logger = createLogger('core');

export default logger;