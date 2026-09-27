# Paper Trading Streaming Engine

**Developed by Eng Hasan Mohamad**

Real-time market-data streaming engine for paper trading:

```
Deriv WebSocket API  →  in-memory candle aggregator  →  SSE  →  Mobile / Web
```

---

## Features

| Area | Detail |
| --- | --- |
| **Pairs** | `XAUUSD`, `EURUSD`, `USDJPY`, `GBPUSD` (Deriv `frx…` codes) |
| **Timeframes** | `5m`, `15m`, `1h`, `4h` → **16 concurrent series** |
| **History** | 100 candles per series, backfilled on connect **and** on every reconnect |
| **Live** | Forming candle's `close` / `high` / `low` mutated from `ohlc` + tick updates |
| **Downstream** | Server-Sent Events, coalesced + throttled to **400 ms** |
| **Dashboard** | TradingView Lightweight Charts with pair/timeframe dropdowns |
| **Resilience** | Exponential backoff + jitter, watchdog, rate-limit cool-down |

## Install & run

```bash
npm install
npm start          # → http://localhost:3000
npm run dev        # same, with --watch
npm test           # 47 end-to-end assertions against a mock Deriv server
```

## Endpoints

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/` | Live chart dashboard |
| `GET` | `/stream` | **SSE feed** — `snapshot` then `update` events |
| `GET` | `/stream?symbol=frxXAUUSD&interval=900` | Same, filtered to one series (bandwidth saving) |
| `GET` | `/api/candles?symbol=&interval=` | REST poll of one series |
| `GET` | `/api/snapshot` | Entire in-memory state as JSON |
| `GET` | `/health` | Upstream status, per-series stats, client list, memory |

### SSE contract

**`snapshot`** — sent *immediately* on connect, contains all 16 series:

```jsonc
{
  "type": "snapshot",
  "maxCandles": 100,
  "pairs": [...], "timeframes": [...],
  "series": {
    "frxXAUUSD|900": {
      "symbol": "frxXAUUSD", "display": "XAUUSD",
      "interval": 900, "intervalLabel": "15m",
      "ready": true, "fresh": true,
      "candles": [ { "time": 1750000000, "open": 1, "high": 1, "low": 1, "close": 1, "tickVolume": 0 } ],
      "current": { "time": 1750000500, "open": 1, "high": 1, "low": 1, "close": 1 }
    }
  }
}
```

**`update`** — every 400 ms, only the series that changed:

```jsonc
{
  "type": "update",
  "serverTime": 1750000500123,
  "items": [{
    "key": "frxXAUUSD|900", "symbol": "frxXAUUSD", "interval": 900,
    "current": { "time": 1750000500, "open": 1, "high": 1, "low": 1, "close": 1 },
    "closed":  { "time": 1750000200, ... },   // only when a candle just closed
    "isNewCandle": true
  }]
}
```

**`status`** — every 5 s, plus immediately when the upstream drops:

```jsonc
{ "connected": false, "message": "Upstream connection lost — reconnecting…" }
```

## Configuration (env vars)

| Variable | Default | Notes |
| --- | --- | --- |
| `PORT` / `HOST` | `3000` / `0.0.0.0` | HTTP listener |
| `DERIV_WS_URL` | `wss://ws.derivws.com/websockets/v3` | Upstream endpoint |
| `DERIV_APP_ID` | `1089` | Deriv application id |

Tunables live in the `CONFIG` object at the top of `server.js`
(stream interval, backoff, heartbeat, `STREAM_TICKS`, `MAX_CANDLES`, …).

## Implementation notes

**Interval pinning.** Deriv has no explicit candle-interval parameter — it
derives the interval from the requested window (`(end - start) / (count - 1)`).
Every history request therefore sends a window of exactly
`100 × interval` seconds with `end` floored onto the timeframe grid, and each
one is immediately followed by its own subscribe call (Deriv resolves a
subscription's interval from the most recent history request *for that symbol*,
which is what makes four timeframes per symbol work). On top of that the engine
*measures* the interval it receives and re-requests with a corrected `count`
until it matches — self-calibrating, not guessing.

**Back-pressure.** Live changes are coalesced into a `pending` map keyed by
series, so however fast ticks arrive the SSE layer emits at most one message per
400 ms. Clients whose socket buffer exceeds 1 MB are skipped rather than allowed
to grow unbounded.

**Feed gaps.** When a candle arrives later than `previous + interval` (weekend,
reconnect), the missing candles are synthesised from the last close so the chart
never shows a hole.

**Backoff.** `min(30s, 1s × 2ⁿ)` with ±50 % jitter, reset on a successful
handshake. A 60 s silence watchdog terminates zombie sockets, and a Deriv
`rate_limit` pushes the next attempt out by 30 s.

## Deploy to Render

The repo is Render-ready — `render.yaml`, `.nvmrc` and `.gitignore` are included.

```bash
git init
git add .
git commit -m "Paper Trading Streaming Engine"
git branch -M main
git remote add origin https://github.com/<you>/paper-trading-streaming-engine.git
git push -u origin main
```

Then on Render: **New → Blueprint** → select the repo → **Apply**. Render reads
`render.yaml` and does everything else. You get a URL like:

```
https://paper-trading-streaming-engine.onrender.com
```

| Where | URL |
| --- | --- |
| Dashboard | `https://<service>.onrender.com/` |
| SSE feed | `https://<service>.onrender.com/stream` |
| Health | `https://<service>.onrender.com/health` |

Or deploy manually with **New → Web Service**: build `npm ci --omit=dev`,
start `npm start`, health check `/health`.

### Render notes that matter

- **Plan.** `starter` ($7/mo) is what you want. The `free` plan sleeps after
  15 minutes with no traffic, and the next connect pays a 30–60 s cold start —
  a bad experience in a mobile app. Render also restarts free instances weekly.
- **Keep the instance count at 1.** The candle store is in-memory, so two
  instances would serve two different copies of the market. Never scale out
  without moving state to Redis.
- **`/health` returns 200 even when Deriv is down.** That is deliberate: a
  Deriv outage should not make Render kill and restart the service. The engine
  reconnects on its own.
- **SSE survives the proxy** because the hub writes a `: heartbeat` comment
  every 15 s and a `status` event every 5 s, so the connection is never idle.
- `PORT` / `HOST` are injected by Render automatically; `server.js` already
  binds `0.0.0.0`. Only `DERIV_APP_ID` needs setting.

### Pointing a mobile app at it

```
GET https://<service>.onrender.com/stream
Accept: text/event-stream
```

Three events arrive: `snapshot` (full 100-candle history for all 16 series,
sent immediately), `update` (coalesced live candles, every 400 ms) and `status`
(upstream health). Narrow the stream with
`?symbol=frxXAUUSD&interval=900` to cut bandwidth ~16×.

**Android (Kotlin / OkHttp):**

```kotlin
val req = Request.Builder()
    .url("https://<service>.onrender.com/stream?symbol=frxXAUUSD&interval=900")
    .header("Accept", "text/event-stream")
    .build()

client.newCall(req).execute().use { resp ->
    val source = resp.body!!.source()
    var event = ""
    while (!source.exhausted()) {
        val line = source.readUtf8Line() ?: continue
        when {
            line.startsWith("event:") -> event = line.removePrefix("event:").trim()
            line.startsWith("data:") && event == "update" ->
                // handle the JSON payload
            line.isEmpty() -> event = ""
        }
    }
}
```

**iOS (URLSession):**

```swift
var request = URLRequest(url: URL(string:
    "https://<service>.onrender.com/stream?symbol=frxXAUUSD&interval=900")!)
request.setValue("text/event-stream", forHTTPHeaderField: "Accept")
request.timeoutInterval = .infinity   // never time out a stream

let task = URLSession.shared.bytes(for: request) { bytes, response in
    for try await line in bytes.lines { /* parse event:/data: pairs */ }
}
task.resume()
```

Always render a "connecting / reconnecting" state in the app: on the free plan
the first frame can take a minute while the instance wakes up.

## Project layout

```
server.js              # feed, aggregator, SSE hub, HTTP server
public/index.html      # Lightweight Charts dashboard
test/mock-deriv.mjs    # Deriv-protocol mock server (test only)
test/e2e.test.mjs      # end-to-end suite (npm test)
```

## Notes

- Forex and metals are closed at weekends — the last candle stays flat and the
  dashboard shows `market closed`.
- State is in-memory by design: restarting the engine re-backfills from
  Deriv. There is no database, so nothing survives a restart.
