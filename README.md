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
npm test                      # engine + AI + admin/membership + e2e, no real network
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
`executions`, `chat_events`, `app_users`, `membership_requests` and `ai_trades`,
enables **row-level security with zero policies** — the service-role key in this
process is the only thing that can read or write them — and installs the atomic
quota function plus a new-user trigger that gives every signup a FREE profile
automatically.

Upgrading an existing deployment? Run
[`supabase/admin-membership.sql`](supabase/admin-membership.sql) once as well.
It adds only the three new tables, indexes and RLS, and is safe to re-run.

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

## APK membership and the operator console

The Android app has **no email/password signup and no database credentials**.
An installation is identified by the same stable device id it already uses for
Telegram pairing:

```
install → POST /api/membership/register → FREE account
launch/resume → GET /api/membership/me → plan, status, retention window
upgrade → POST /api/membership/request → operator sees it in /admin/
operator approves → plan_expires_at is set → APK is notified
```

### Plans

| Plan | History retention | Execution rights |
| --- | ---: | --- |
| `FREE` | 7 days | unchanged — the engine's normal freemium quota |
| `MONTHLY` | 90 days | unchanged |
| `YEARLY` | 3,650 days | unchanged |

A plan is a **label plus a trade-history window**. It never grants the APK any
execution ability: the strategy engine and the AI analyst remain the only
things that can open or close a position, and both run server-side.

Expiry is enforced on every request. A paid plan whose `plan_expires_at` is
missing or in the past is treated as `FREE` immediately, so a broken date can
never grant permanent access. In the console, **Extend by days** adds to the
current expiry (or starts from now if the plan already lapsed); changing the
plan uses that plan's default period unless you enter an explicit expiry.

### Upgrade notification — always server-side

When an operator approves a request or edits a plan, the **server**:

1. persists the new plan and expiry in `app_users`;
2. pushes a `membership` event to that device's
   `GET /api/membership/stream` listeners;
3. sends a Telegram DM if the device was paired with `/start <deviceId>`;
4. returns the notification counts to the admin console.

`GET /api/membership/me` remains the **source of truth**. The SSE frame and the
Telegram DM are wake-ups, not authority: if the phone was offline and unpaired,
it sees the new plan the next time it calls `/me`. There is no client-side plan
flag to edit and no payment credential in the APK.

### Bans

Setting an account to `BANNED` makes `deviceGate` return `403 banned` on
`/api/trades/*`, `/api/ai/signal`, `/api/ai/analyze`, `/api/membership/request`,
`/api/membership/stream`, `/stream`, `/api/stream/prices` and
`/api/stream/chat-events` whenever the APK sends its device id.
`GET /api/membership/me` still returns the account with `status: "BANNED"`, so
the app can show a clear suspension screen instead of a generic network error.
Unbanning restores access on the next request or reconnect. Public market
endpoints remain public for anonymous browsers; a ban is enforced on every
identified request.

### Admin console

`/admin/` is a self-contained page served by this process. It has an overview,
APK-user search/edit, the upgrade-request queue and the permanent AI ledger.
Authentication is a single operator account from the environment:

```bash
# Render → Environment
ADMIN_USERNAME=admin
ADMIN_PASSWORD_HASH=scrypt$...       # npm run hash:admin -- "long password"
ADMIN_SESSION_SECRET=<random hex>    # optional but recommended
```

The browser receives an `HttpOnly`, `SameSite=Strict`, HMAC-signed cookie.
Login attempts are rate-limited per IP; mutating admin calls also require a
same-origin `Origin`. No admin secret is stored in Supabase. Without
`ADMIN_PASSWORD` or `ADMIN_PASSWORD_HASH`, `/admin/` shows a setup message and
every admin API answers `503 admin_disabled`.

### Permanent AI trade ledger

Every AI trade is written to `ai_trades` when it opens, when TP1 moves the stop
to breakeven, and when it closes. A partial unique index enforces **one OPEN
row per symbol**, so a restart can never open a second trade on the same
instrument. At boot the analyst re-adopts those OPEN rows and resumes managing
their stop/targets; the APK reads them through the API, never through Supabase.

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
| `POST` | `/api/membership/register` | device | install → FREE account (idempotent) |
| `GET` | `/api/membership/me` | device | plan, status, expiry, retention, support handles |
| `POST` | `/api/membership/request` | device | ask the operator for MONTHLY / YEARLY |
| `GET` | `/api/membership/requests` | device | that device's own request history |
| `GET` | `/api/membership/plans` | — | public plan metadata + support handles |
| `GET` | `/api/membership/stream` | device | per-device SSE upgrade/ban notice |
| `GET` | `/api/trades/open` | device | live AI book + engine positions |
| `GET` | `/api/trades/history` | device | closed AI trades, filtered by plan |
| `GET` | `/api/trades/stats` | device | win rate / PnL for the plan window |
| `GET` | `/admin/` | cookie | operator console (login page + SPA) |
| `POST` | `/admin/api/login` | — | username/password → signed cookie |
| `GET` | `/admin/api/overview` | cookie | counters, system state, recent activity |
| `GET`/`PATCH` | `/admin/api/users…` | cookie | list, inspect, edit plan/ban/contact |
| `GET`/`POST` | `/admin/api/requests…` | cookie | queue and approve/reject upgrades |
| `GET` | `/admin/api/trades` | cookie | permanent AI ledger |
| `GET` | `/api/stream` | — | connectors index |

`device` means `X-Device-Id: <installation id>` (or `?deviceId=` for
EventSource). Every `/api/user/*` route is still a `GET`. The only mutating
endpoints are device registration / upgrade requests and the browser-only admin
console — none of them can place, size or close a trade.

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

The APK has no Supabase URL or key at all. Device membership reads go through
this process (`/api/trades/*`, `/api/membership/*`), which is why a plan cannot
be forged by editing a local setting and a leaked APK cannot query another
user's data. The admin console is a separate, environment-held credential with
an HttpOnly signed cookie; it is not a Supabase user, and its password never
enters the database.

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
│   ├── aiSignal.js              AI analyst + durable trade ledger writes
│   ├── aiAnalystManager.js      multi-symbol clock, quota backoff, restore
│   ├── appUsers.js              device accounts, plans, retention, requests
│   ├── membershipEvents.js      per-device SSE fan-out
│   ├── brokerApi.js             paper + MT5 bridge execution (server-only)
│   └── logger.js
├── engine/
│   ├── strategyEngine.js        Aetheron Universal: entries, legs, trailing
│   └── indicatorCalc.js         ATR(20), Donchian, breakout (pure functions)
├── middleware/
│   ├── auth.js                  token → profile, SSE-aware
│   ├── adminAuth.js             scrypt password, signed session cookie
│   ├── quotaManager.js          daily counter, SIGNAL_ONLY downgrade, rate limit
│   └── tradeGuard.js            rejects client-supplied execution parameters
└── routes/
    ├── sse.js                   /stream + the two production streams
    ├── user.js                  read-only user API
    ├── ai.js                    /api/ai/signal + /api/ai/analyze
    ├── membership.js            APK membership + trade-history API
    ├── admin.js                 /admin/ console + operator API
    └── telegram.js              optional notifications
public/admin/index.html          operator SPA (login, users, requests, ledger)
supabase/schema.sql              full idempotent schema
supabase/admin-membership.sql    migration for existing deployments
test/engine.test.mjs             140 assertions — quant, quota, security
test/aiSignal.test.mjs            38 assertions — analyst parsing/fallbacks
test/aiManager.test.mjs           10 assertions — multi-symbol/quota behaviour
test/admin.test.mjs               56 assertions — auth, plans, ledger, bans
test/e2e.test.mjs                163 assertions — full stack against a mock feed
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
| `ADMIN_USERNAME` / `ADMIN_PASSWORD(_HASH)` | `admin` / — | `/admin/` is disabled until a password is set |
| `ADMIN_SESSION_SECRET` | derived | set a separate random value so password rotation is clean |
| `MEMBERSHIP_ENABLED` | `true` | APK device accounts + trade-history API |
| `FREE_HISTORY_DAYS` | `7` | FREE trade-history window |
| `MONTHLY_HISTORY_DAYS` | `90` | MONTHLY window |
| `YEARLY_HISTORY_DAYS` | `3650` | YEARLY window |
| `SUPPORT_TELEGRAM` / `SUPPORT_WHATSAPP` / `SUPPORT_EMAIL` | — | shown by `GET /api/membership/plans` |

---

## Deploy to Render

1. Run `supabase/schema.sql` in Supabase. Existing projects also run
   `supabase/admin-membership.sql` once.
2. Set `SUPABASE_URL` and `SUPABASE_SERVICE_ROLE_KEY` as secret env vars.
3. Set `ADMIN_USERNAME`, `ADMIN_PASSWORD_HASH` (or `ADMIN_PASSWORD`) and
   `ADMIN_SESSION_SECRET`. Without a password the console stays disabled.
4. Set `SUPPORT_TELEGRAM` / `SUPPORT_WHATSAPP` / `SUPPORT_EMAIL` so the APK's
   upgrade screen has real contact buttons.
5. Apply the blueprint — [`render.yaml`](render.yaml) pins the strategy
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

### APK membership + trade history

Use the installation id already generated for Telegram pairing. It is an
identity, not a secret, and it must be stable across app restarts.

```kotlin
// 1) Register once (idempotent — safe to call on every launch)
// The device id may go in the header or the JSON body; the header is preferred.
val body = """
  {"displayName":"$name",
   "contactMethod":"telegram","contactValue":"$handle"}
""".trimIndent().toRequestBody("application/json".toMediaType())

val register = Request.Builder()
    .url("$BASE/api/membership/register")
    .header("X-Device-Id", deviceId)
    .post(body)
    .build()

// 2) Source of truth for plan / status / retention
val me = Request.Builder()
    .url("$BASE/api/membership/me")
    .header("X-Device-Id", deviceId)
    .build()

// 3) Ask the operator for an upgrade (after showing SUPPORT_* handles)
val request = Request.Builder()
    .url("$BASE/api/membership/request")
    .header("X-Device-Id", deviceId)
    .post("""{"plan":"MONTHLY","message":"paid via Telegram"}"""
        .toRequestBody("application/json".toMediaType()))
    .build()

// 4) Trade history — closed rows filtered by the server, not by the client
val history = Request.Builder()
    .url("$BASE/api/trades/history?limit=100&offset=0")
    .header("X-Device-Id", deviceId)
    .build()

// 5) Open book (AI + strategy positions)
val open = Request.Builder()
    .url("$BASE/api/trades/open")
    .header("X-Device-Id", deviceId)
    .build()
```

`GET /api/membership/me` returns `plan`, `storedPlan`, `status`, `expired`,
`planExpiresAt`, `retentionDays` and `features`. Handle the two important
errors explicitly:

- `401 unregistered` → call `/api/membership/register` first.
- `403 banned` → show the suspension screen; do not retry in a loop.

To receive the upgrade immediately instead of waiting for the next `/me`, keep
an EventSource open on
`$BASE/api/membership/stream?deviceId=$deviceId` and refresh `/me` when a
`membership` event arrives. The stream is optional; `/me` is authoritative.

**Do not put a Supabase URL, service-role key or database driver in the APK.**
The app cannot query `ai_trades` directly, by design. It sends its device id to
this Node process, which uses the service-role key server-side and returns only
the rows that device's plan is allowed to see. That is the complete answer to
"how does the app show the trade history without database access?" — it calls
`/api/trades/history`, not Supabase.

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
- **Candle gaps are left as gaps.** Weekend and outage bars are not fabricated:
  the chart and the indicators both see the real sequence, so ATR is not
  diluted by synthetic flat candles.
- **The AI ledger is permanent; the plan is not.** A FREE account that upgrades
  to YEARLY can read the older rows immediately because they were never
  deleted — retention is a read filter (`closed_at >= now - retentionDays`),
  not a purge job.
- **One OPEN AI trade per symbol** is enforced by a partial unique index in
  Postgres, not just by the in-memory map. A restart re-adopts the row and
  resumes managing it.
- **`MAX_CANDLES` must stay above `DONCHIAN_ENTRY`.** `diagnose.mjs` checks this
  and fails loudly if it does not.