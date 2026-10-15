/* =============================================================================
 *  AETHERON CORE  —  src/middleware/tradeGuard.js
 *  -----------------------------------------------------------------------------
 *  Defence in depth for the single most important rule in this system:
 *
 *      THE SERVER DECIDES WHEN TO TRADE. THE CLIENT NEVER DOES.
 *
 *  There is no trade endpoint — not a POST, not a GET, not a hidden one. That
 *  alone would be sufficient. This middleware exists because "sufficient" is
 *  not a property a financial system should rely on:
 *
 *    1. It rejects any request whose query string or JSON body contains
 *       execution-shaped keys (side, volume, lots, sl, tp, entryPrice,
 *       autoExecute, …), so a future route that accidentally starts reading a
 *       body cannot become an order injection point.
 *    2. It strips those keys from `req.query` / `req.body`, so even a careless
 *       handler downstream cannot see them.
 *    3. It advertises the read-only surface, so a client developer can see at
 *       a glance that the API has no order path.
 *
 *  Whitelisting beats blacklisting for the parser (it cannot be evaded with an
 *  obscure spelling), and blacklisting is kept as the second layer.
 * ============================================================================= */

/** Keys that must never arrive from a client. */
const FORBIDDEN_KEYS = new Set([
  'side',
  'direction',
  'action',
  'order',
  'ordertype',
  'volume',
  'lots',
  'lot',
  'qty',
  'quantity',
  'amount',
  'entryprice',
  'price',
  'sl',
  'stoploss',
  'stop_loss',
  'tp',
  'takeprofit',
  'take_profit',
  'tp1',
  'tp2',
  'leverage',
  'margin',
  'autoexecute',
  'auto_execute',
  'execute',
  'forceclose',
  'closeposition',
  'magic',
  'ticket',
]);

/** Top-level route names that would indicate an order endpoint. */
const FORBIDDEN_ROUTE_HINTS = ['/trade', '/order', '/execute', '/positions/close', '/buy', '/sell'];

/** Recursively scan an object for forbidden keys (depth-limited). */
function findForbidden(obj, depth = 0, hits = []) {
  if (!obj || typeof obj !== 'object' || depth > 4) return hits;
  for (const [key, value] of Object.entries(obj)) {
    if (FORBIDDEN_KEYS.has(String(key).toLowerCase())) hits.push(key);
    else if (value && typeof value === 'object') findForbidden(value, depth + 1, hits);
  }
  return hits;
}

/** Delete forbidden keys in place (the object is a fresh parse, so this is safe). */
function stripForbidden(obj, depth = 0) {
  if (!obj || typeof obj !== 'object' || depth > 4) return;
  for (const [key, value] of Object.entries(obj)) {
    if (FORBIDDEN_KEYS.has(key.toLowerCase())) {
      delete obj[key];
      continue;
    }
    if (value && typeof value === 'object') stripForbidden(value, depth + 1);
  }
}

/**
 * Express middleware factory.
 * @param {{logger?:object}} [opts]
 */
export function tradeGuard({ logger = console } = {}) {
  return function guard(req, res, next) {
    // ---- layer 1: the path itself must not look like an order endpoint ----
    const path = String(req.path ?? '').toLowerCase();
    if (FORBIDDEN_ROUTE_HINTS.some((hint) => path === hint || path.endsWith(hint))) {
      logger.warn?.(`blocked order-shaped route ${req.method} ${req.originalUrl}`);
      return res.status(405).json({
        error: {
          code: 'execution_not_client_driven',
          message:
            'Aetheron Core executes autonomously. The server decides when to trade; ' +
            'this API accepts no execution parameters.',
        },
      });
    }

    // ---- layer 2: nothing execution-shaped may be present at all ----
    const hits = [
      ...findForbidden(req.query),
      ...findForbidden(req.body),
      ...findForbidden(req.params),
    ];

    if (hits.length > 0) {
      logger.warn?.(`rejected execution parameters from ${req.ip}: ${[...new Set(hits)].join(', ')}`);
      return res.status(400).json({
        error: {
          code: 'execution_parameters_rejected',
          message:
            'Execution parameters (side/volume/stop/target) are server-owned and must not be sent.',
          rejected: [...new Set(hits)],
        },
      });
    }

    // ---- layer 3: sanitise, so nothing downstream can read them anyway ----
    stripForbidden(req.query);
    stripForbidden(req.body);

    return next();
  };
}

/** Machine-readable description of the read-only surface, served at /api. */
export function apiManifest() {
  return {
    execution: 'server-autonomous',
    note: 'No endpoint accepts an order. Side, volume, stop and target are computed by the strategy engine.',
    readOnlyRoutes: [
      'GET /api/user/me',
      'GET /api/user/quota',
      'GET /api/user/positions',
      'GET /api/user/executions',
      'GET /api/user/engine',
      'GET /api/market/candles',
      'GET /api/market/price',
      'GET /api/ai/signal',
      'GET /api/ai/analyze',
      'GET /api/stream/prices',
      'GET /api/stream/chat-events',
      'GET /stream',
      'GET /health',
    ],
  };
}

export default tradeGuard;