# Aetheron Core

**Autonomous Quant Trading & Signal Backend** — developed by Eng Hasan Mohamad.

An autonomous trading engine on top of the Paper Trading Streaming Engine: it
persists candles to Supabase, computes indicators server-side, decides *when to
trade* from its own analysis, enforces a freemium quota, and streams everything
to the Android client over SSE.

```
MT5 tick feed ──► MarketAggregator ──► Supabase (write-behind)
   (WebSocket)         │                     │
                      │                100 daily candles rehydrated
                      │                at startup (50-day Donchian)
                      ▼
                StrategyEngine ──► BrokerApi  (server-side only)
                 ATR(20) sizing        paper / MT5 bridge
                 50-day breakout
                 20-day trailing stop
                 25% @ +1×ATR, 75% runner
                      │
                      ├──► /api/stream/prices      clean price ticks
                      └──► /api/stream/chat-events executions, signals, health
```

**The client can observe the engine. It can never instruct it.** There is no
trade endpoint — see [Security](#security).

---

## Quick start

```bash
npm install
cp .env.example .env          # add Supabase URL + service-role key
node diagnose.mjs             # verify Supabase, the feed and the backfill
npm start                     # → http://localhost:3000
npm test                      # 291 assertions, no network required
```

Without Supabase credentials the engine still runs in **local mode**: candles
live in RAM, quota counters are in-process, and the strategy works — it just
rebuilds its history from the upstream REST backfill on every restart.

---

## The strategy — Aetheron Universal

### a) Volatility-targeted sizing

```
LotSize = (AccountBalance × RiskPercentage) / (ATR(20) × PointValue)
```

The dollars at risk stay constant while the position size adapts to volatility:
a calm market gets a large position, a violent one a small position.

`PointValue` comes from the instrument registry in `src/config/db.js`
(`pointSize × contractSize`), never from a hard-coded constant. Note the unit
handling — ATR is a **price delta**, PointValue is dollars **per point**, so
the ATR is converted to points first (`atrPoints = atr / pointSize`). Skipping
that conversion over-sizes a EURUSD position by exactly 100×.

### b) Entry — 50-day Donchian breakout

| Condition | Action |
| --- | --- |
| `price > highest high of the last 50 closed days` | **LONG** |
| `price < lowest low of the last 50 closed days` | **SHORT** |

The channel deliberately excludes the forming bar: if the live bar's own high
counted, price could never exceed the level it has to break.

### c) Split exit — 20-day trailing stop

| Leg | Size | Exit |
| --- | --- | --- |
| Initial stop | 100% | the opposite edge of the 20-day channel |
| **Leg 1** | 25% | closes automatically at **+1 × ATR(20)** — covers spread and operational fees |
| **Leg 2** | 75% | **no fixed take profit**; rides the 20-day trailing stop until it hits |

No pyramiding: one position per symbol, because two correlated positions would
double the risk the sizing model exists to keep constant.

### Fail-safe posture

The engine refuses to trade — rather than trade badly — when history is too
short for ATR(20) or the 50-day channel (`WARMING_UP`), when market data is
stale, when a position is already open, or when the profile disables trading
(`HALTED`). The lifecycle state is visible at `/health` and
`GET /api/user/engine`.

---

## Data persistence

`@supabase/supabase-js` stores every candle permanently.

**Startup order matters:**

```
1. create the Supabase client
2. rehydrate 100 daily candles into RAM   ← before the feed connects
3. start the HTTP listener
4. connect the upstream feed and merge its history in
5. start the strategy engine
```

Step 2 is the point of the exercise: after a restart the 50-day Donchian channel
and the ATR history are already in memory, so the engine arms immediately
instead of waiting for a 100-candle REST backfill.

Writes never block the tick path. Closed candles are queued and flushed in
batches (`PERSIST_FLUSH_MS`); the forming candle is upserted on a slower
cadence (`PERSIST_FORMING_FLUSH_MS`) because losing the current bar on a crash
is acceptable while losing a closed one is not.

### Schema

Run [`supabase/schema.sql`](supabase/schema.sql) once in the Supabase SQL editor
(it is idempotent). It creates `candles`, `profiles`, `daily_executions`,
`executions` and `chat_events`, enables **row-level security with zero
policies** — the service-role key in this process is the only thing that can
read or write them — and installs the atomic quota function plus a new-user
trigger that gives every signup a FREE profile automatically.

---

## Freemium quota

| Plan | Automatic executions | Behaviour at the limit |
| --- | --- | --- |
| `FREE` | **3 / day** | the signal is still published, downgraded to `SIGNAL_ONLY` — the server simply does not place the order |
| `PRO` | unlimited | executes normally |

```jsonc
// /api/stream/chat-events
{ "type": "SIGNAL_ONLY",
  "message": "XAUUSD BUY 0.05 -- SIGNAL ONLY, not executed (FREE daily limit reached 3/3)",
  "severity": "warning",
  "timestamp": "2026-10-05T05:47:03.018Z",
  "meta": { "symbol": "XAUUSD", "side": "LONG", "volume": 0.05 } }
```

Two design points worth stating explicitly:

- **Exits are never metered.** A position that has been opened must always be
  stoppable, whatever the plan or counter state. Charging the counter for a
  close would let a FREE user end up with an unstoppable trade. The engine
  still calls the quota with `kind: 'EXIT'` so the guarantee is explicit and
  auditable rather than an implicit absence.
- **The counter is spent only after a real fill**, so a rejected order does not
  cost the user one of their three trades. The increment itself is an atomic
  `aetheron_consume_execution` RPC (with a compare-and-swap `UPDATE` fallback)
  so two instances cannot race past the limit. If the counter is unreadable the
  engine **fails closed**.

---

## Endpoints

| Method | Path | Auth | Purpose |
| --- | --- | :---: | --- |
| `GET` | `/` | — | Lightweight Charts dashboard |
| `GET` | `/health` | — | liveness + diagnostics (never blocks on Supabase) |
| `GET` | `/stream` | — | legacy public chart feed (`snapshot`, `update`, `status`) |
| `GET` | `/api/stream/prices` | ✔ | clean price ticks + live candle (`hello`, `price`) |
| `GET` | `/api/stream/chat-events` | ✔ | executions, signals, health (`hello`, `backlog`, `message`) |
| `GET` | `/api/market/price` | — | REST twin of the price stream |
| `GET` | `/api/market/candles` | — | one series, ring-capped |
| `GET` | `/api/snapshot` | — | entire in-memory state |
| `GET` | `/api/user/me` | ✔ | identity, plan, sizing parameters, quota |
| `GET` | `/api/user/quota` | ✔ | today's execution budget |
| `GET` | `/api/user/positions` | ✔ | open positions with their leg split |
| `GET` | `/api/user/executions` | ✔ | audit trail |
| `GET` | `/api/user/engine` | ✔ | live strategy state + indicators |
| `GET` | `/api/stream` | — | connectors index |

Every user route is a `GET`. There is no mutating endpoint.

Authenticate with `Authorization: Bearer <token>`, `?access_token=<token>` or
`x-access-token` (EventSource cannot set headers, so the query form is the
standard SSE pattern — the token is verified server-side either way).

### Stream contracts

**`/api/stream/prices`** — coalesced to `STREAM_INTERVAL_MS`:

```jsonc
{ "type": "prices", "serverTime": "…", "count": 4,
  "items": [{ "symbol": "XAUUSD", "display": "XAUUSD",
              "bid": 4158.06, "ask": 4158.14, "mid": 4158.1, "spread": 0.08,
              "digits": 2, "ts": "…", "direction": "DOWN",
              "candle": { "time": 1791179100, "open": 4159.5, "high": 4160.75,
                          "low": 4158.09, "close": 4158.1, "tickVolume": 1702 },
              "interval": 300 }] }
```

Narrow it with `?symbol=XAUUSD&interval=300` — an unknown symbol is a `400`, not
a silent fallback to all four.

**`/api/stream/chat-events`** — chat-ops messages:

```jsonc
{ "id": "397f0c7b-…", "type": "EXECUTION", "severity": "success",
  "message": "XAUUSD BUY 0.33 executed at 4158.1 - ticket P-efe2dd78 - paper",
  "timestamp": "2026-10-05T05:47:03.018Z",
  "meta": { "symbol": "XAUUSD", "side": "LONG", "volume": 0.33, … } }
```

`type` is one of `EXECUTION`, `SIGNAL`, `SIGNAL_ONLY`, `PARTIAL_CLOSE`,
`STOP_HIT`, `TRAILING`, `QUOTA`, `SYSTEM`, `HEALTH`, `RISK`. A reconnecting
client is replayed the last `SSE_CHAT_BACKLOG` messages, so a phone that slept
through a trade still sees it.

---

## Security

**The server decides when to trade. The client never does.**

Three independent layers:

1. **No trade endpoint exists.** `POST /api/trade`, `/api/order`, `/api/execute`,
   `/api/buy` and `/api/sell` are all refused with `405`. There is no hidden one.
2. **`tradeGuard` middleware** rejects any request whose query, body or params
   contain execution-shaped keys (`side`, `volume`, `lots`, `sl`, `tp`,
   `autoExecute`, …) with `400`, and strips them so nothing downstream can read
   them. Blacklist as a second layer behind the whitelist in the route table.
3. **The broker is unreachable from the HTTP layer.** `src/services/brokerApi.js`
   is imported by the strategy engine only; a test asserts that no module in
   `src/routes/` imports it. Requests are signed to an MT5 bridge with
   HMAC-SHA256, so a leaked bridge URL is unusable by a third party.

Also in place: the Supabase service-role key stays in this process (never in the
app), RLS is enabled with no policies, tokens are verified server-side with a
short-lived cache, the API is rate limited per IP, security headers and a CSP are
set, and `DEV_TOKEN` is hard-disabled when `NODE_ENV=production`.

---

## Project layout

```
server.js                        HTTP entry point (listen + graceful shutdown)
src/
├── config/
│   ├── env.js                    every tunable, validated and clamped
│   └── db.js                    symbol registry, timeframes, input validation
├── services/
│   ├── websocket.js             upstream SignalR feed, exponential backoff
│   ├── supabase.js              persistence, profiles, atomic quota, audit
│   ├── marketAggregator.js      candle store, DB hydration, write-behind
│   ├── sseHub.js                channel-aware SSE hub, back-pressure
│   ├── sseDispatcher.js         prices + chat-ops streams
│   ├── brokerApi.js             paper + MT5 bridge execution (server-only)
│   └── logger.js
├── engine/
│   ├── strategyEngine.js        Aetheron Universal: entries, legs, trailing
│   └── indicatorCalc.js         ATR(20), Donchian, breakout (pure functions)
├── middleware/
│   ├── auth.js                  token → profile, SSE-aware
│   ├── quotaManager.js          daily counter, SIGNAL_ONLY downgrade, rate limit
│   └── tradeGuard.js            rejects client-supplied execution parameters
└── routes/
    ├── sse.js                   /stream + the two production streams
    ├── user.js                  read-only user API
    └── telegram.js              optional notifications
supabase/schema.sql              tables, RLS, atomic quota RPC, triggers
test/engine.test.mjs             140 assertions — quant, quota, security
test/e2e.test.mjs                151 assertions — full stack against a mock feed
```

---

## Configuration

Every knob lives in `src/config/env.js` with a working default; see
[`.env.example`](.env.example) for the annotated list. The ones that change
behaviour rather than tuning:

| Variable | Default | Notes |
| --- | --- | --- |
| `SUPABASE_URL` / `SUPABASE_SERVICE_ROLE_KEY` | — | without them: local mode |
| `SUPABASE_REQUIRED` | `false` | `true` aborts the boot instead |
| `BOOTSTRAP_DAILY_CANDLES` | `100` | lower it and the engine stays `WARMING_UP` |
| `BROKER_MODE` | `paper` | `mt5` needs `BROKER_BASE_URL` + `BROKER_TOKEN` |
| `STRATEGY_EXECUTION_ENABLED` | `true` | `false` publishes signals, places nothing |
| `FREE_DAILY_EXECUTIONS` | `3` | the freemium hook |
| `AUTH_REQUIRED` | `true` | gate on `/api/user/*` and `/api/stream/*` |
| `DEV_TOKEN` | — | dev only; blanked in production |

---

## Deploy to Render

1. Run `supabase/schema.sql` in Supabase.
2. Set `SUPABASE_URL` and `SUPABASE_SERVICE_ROLE_KEY` as secret env vars.
3. Apply the blueprint — [`render.yaml`](render.yaml) pins the strategy
   parameters and defaults to `BROKER_MODE=paper`.

Then confirm `/health` reports `database.lastProbe.connected: true`,
`market.restored` above zero (candles came back from the database), and
`engine.state: "ARMED"`.

### Render notes that matter

- **`starter` ($7/mo), not `free`.** The free plan sleeps after 15 minutes
  without traffic; a sleeping engine is not watching the 50-day channel.
- **Keep the instance count at 1.** The in-memory ring is per instance, so two
  instances would watch two different copies of the market and could each open a
  position on the same symbol. The quota counter *is* safe across instances
  (atomic in Postgres), but the position book is not.
- **`/health` returns 200 even when the feed or Supabase is down.** That is
  deliberate: a third-party outage should not make Render restart the service.
  The engine reconnects and fails over on its own.
- **SSE survives the proxy** thanks to a comment frame every 15 s, a `status`
  event every 5 s, and `X-Accel-Buffering: no`.

---

## Android client

```kotlin
// Kotlin / OkHttp — EventSource-equivalent over a raw socket
val req = Request.Builder()
    .url("$BASE/api/stream/prices?symbol=XAUUSD&access_token=$token")
    .header("Accept", "text/event-stream")
    .build()
```

```kotlin
// Chat-ops events
val req = Request.Builder()
    .url("$BASE/api/stream/chat-events?access_token=$token")
    .build()
// switch on the parsed "type": EXECUTION | SIGNAL_ONLY | STOP_HIT | …
```

Always render a connecting/reconnecting state: even on `starter` the first frame
can take a moment, and `retry:` (3 s) means EventSource recovers on its own.

---

## Notes and limits

- **One account per process.** `createApplication({ userId })` binds the engine
  to a single user. Multi-tenant means one instance per account, or moving the
  position book to Postgres.
- **The MT5 bridge is a separate service.** `BROKER_MODE=mt5` expects an EA or
  service exposing `POST /order`, `/modify`, `/close`. It is deliberately not
  bundled — test the paper path first.
- **Forex and metals are closed at weekends.** The silence watchdog is
  deliberately long (5 min) so the feed does not reconnect in a loop for the
  whole Saturday.
- **Candle gaps are filled, not hidden.** A weekend or outage produces synthetic
  flat candles carrying the last close, so the chart never shows a hole — which
  means ATR across such a gap understates real volatility.
- **`MAX_CANDLES` must stay above `DONCHIAN_ENTRY`.** `diagnose.mjs` checks this
  and fails loudly if it does not.