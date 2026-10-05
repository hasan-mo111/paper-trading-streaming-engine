/* =============================================================================
 *  TELEGRAM GATEWAY  —  server-side notification hub
 *  Developed by Eng Hasan Mohamad
 * -----------------------------------------------------------------------------
 *  Drop this into the Paper Trading Streaming Engine repo and mount it from
 *  server.js:
 *
 *      import { mountTelegramGateway } from './src/routes/telegram.js';
 *      mountTelegramGateway(app, { market, sse });
 *
 *  WHY A SERVER-SIDE GATEWAY?
 *  --------------------------
 *  A Telegram bot token is a password. If it ships inside a mobile app it can
 *  be extracted from the APK/IPA, and whoever extracts it can send messages as
 *  your bot indefinitely. Rotating the token does not help while the app is
 *  already in circulation. So the token never leaves this process.
 *
 *  FLOW
 *  -----
 *    1. App generates a device id and POSTs /api/telegram/register
 *    2. User sends  /start <deviceId>  to the bot
 *    3. This module sees the update, binds chatId -> deviceId
 *    4. App POSTs /api/telegram/notify  and we fan out to the bound chat
 *
 *  ENV VARS
 *  --------
 *    TELEGRAM_BOT_TOKEN   from @BotFather
 *    TELEGRAM_ENABLED     'false' to switch the whole thing off
 * ============================================================================= */

import { randomUUID } from 'node:crypto';

const DEFAULTS = {
  enabled: process.env.TELEGRAM_ENABLED !== 'false',
  token: process.env.TELEGRAM_BOT_TOKEN ?? '',
  apiBase: 'https://api.telegram.org',
  // Keep this in sync with the app's MEMORY cap
  maxDevices: 5_000,
  maxHistory: 30,
};

export function mountTelegramGateway(app, { log = console.log, warn = console.warn, err = console.error } = {}) {
  const cfg = { ...DEFAULTS };
  if (!cfg.enabled) log('📭 Telegram gateway disabled (TELEGRAM_ENABLED=false)');
  else if (!cfg.token) warn('⚠ TELEGRAM_BOT_TOKEN not set — the gateway will accept registrations but send nothing');
  else startPolling();

  /** deviceId -> { chatId, username, label, pairedAt, lastSentAt, sent } */
  const devices = new Map();
  let pollTimer = null;
  let lastUpdateId = 0;

  /* ------------------------------------------------------------------ bot -- */

  async function callTelegram(method, payload) {
    if (!cfg.token) return null;
    const res = await fetch(`${cfg.apiBase}/bot${cfg.token}/${method}`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(payload),
    });
    const json = await res.json().catch(() => null);
    if (!res.ok || !json?.ok) {
      err(`Telegram ${method} failed:`, json?.description ?? res.status);
      return null;
    }
    return json.result;
  }

  const sendMessage = (chatId, text) =>
    callTelegram('sendMessage', { chat_id: chatId, text, parse_mode: 'HTML', disable_web_page_preview: true });

  /** Long polling keeps this stateless — no inbound webhook or public HTTPS needed. */
  function startPolling() {
    if (!cfg.token || pollTimer) return;
    const tick = async () => {
      try {
        const url = `${cfg.apiBase}/bot${cfg.token}/getUpdates?timeout=25&offset=${lastUpdateId + 1}`;
        const res = await fetch(url);
        const json = await res.json().catch(() => null);
        if (json?.ok && Array.isArray(json.result) && json.result.length) {
          for (const update of json.result) {
            lastUpdateId = update.update_id;
            handleUpdate(update);
          }
        }
      } catch (e) {
        err('Telegram poll failed:', e.message);
      }
      if (pollTimer) setTimeout(tick, 1000); // re-arm only if still enabled
    };
    pollTimer = setTimeout(tick, 1000);
    log('🤖 Telegram bot polling started');
  }

  function handleUpdate(update) {
    const msg = update?.message ?? update?.edited_message;
    if (!msg?.text) return;
    const chatId = msg.chat?.id;
    const text = msg.text.trim();

    // "/start ABCD-1234" is the pairing handshake
    const m = /^\/start(?:@\w+)?\s+([A-Z0-9-]{4,32})$/i.exec(text);
    if (m) {
      const deviceId = m[1].toUpperCase();
      if (devices.size >= cfg.maxDevices && !devices.has(deviceId)) {
        return sendMessage(chatId, '⚠ Pairing limit reached. Contact the developer.');
      }
      devices.set(deviceId, {
        chatId,
        username: msg.chat?.username ?? null,
        label: 'mobile',
        pairedAt: Date.now(),
        lastSentAt: 0,
        sent: 0,
      });
      log(`🔗 Telegram paired: ${deviceId} → chat ${chatId} (@${msg.chat?.username ?? 'private'})`);
      return sendMessage(
        chatId,
        `✅ <b>Paired successfully</b>\n\nDevice ID: <code>${deviceId}</code>\n\n` +
          'You will be notified here when a trade opens or closes, when a stop loss or ' +
          'take profit is hit, and for AI signals.'
      );
    }

    if (/^\/start$/.test(text) || text === '/help') {
      return sendMessage(
        chatId,
        'To pair this device, send:\n\n<code>/start YOUR-DEVICE-ID</code>\n\n' +
          'The device ID is shown in the app under Settings → Telegram.'
      );
    }
    if (text === '/status') {
      const bound = [...devices.values()].filter((d) => d.chatId === chatId);
      return sendMessage(chatId, bound.length ? `✅ This chat is paired to <code>${bound[0].chatId === chatId ? 'a device' : ''}</code>` : 'Not paired yet.');
    }
  }

  /* -------------------------------------------------------------- routes -- */

  const json = (res, code, body) => {
    res.writeHead(code, { 'content-type': 'application/json' });
    res.end(JSON.stringify(body));
  };

  const readJson = (req) =>
    new Promise((resolve) => {
      let raw = '';
      req.on('data', (c) => {
        raw += c;
        if (raw.length > 64_000) req.destroy(); // 64 kB is generous for a trigger
      });
      req.on('end', () => {
        try {
          resolve(JSON.parse(raw || '{}'));
        } catch {
          resolve({});
        }
      });
      req.on('error', () => resolve({}));
    });

  app.post('/api/telegram/register', async (req, res) => {
    const { deviceId, label } = await readJson(req);
    if (!deviceId || !/^[A-Z0-9-]{4,32}$/i.test(deviceId)) {
      return json(res, 400, { error: 'A valid deviceId is required' });
    }
    const existing = devices.get(deviceId.toUpperCase());
    if (existing) {
      existing.label = label ?? existing.label;
      return json(res, 200, { ok: true, linked: true, chatId: existing.chatId });
    }
    // Registered but not yet paired — the user still has to send /start
    devices.set(deviceId.toUpperCase(), {
      chatId: null,
      label: label ?? 'mobile',
      pairedAt: Date.now(),
      lastSentAt: 0,
      sent: 0,
    });
    json(res, 200, { ok: true, linked: false, chatId: null });
  });

  app.get('/api/telegram/status', (req, res) => {
    const deviceId = String(req.query.deviceId ?? '').toUpperCase();
    const d = devices.get(deviceId);
    json(res, 200, {
      linked: !!d?.chatId,
      notificationsSent: d?.sent ?? 0,
      pairedAt: d?.pairedAt ?? null,
    });
  });

  app.post('/api/telegram/notify', async (req, res) => {
    const { deviceId, event, payload } = await readJson(req);
    const d = devices.get(String(deviceId ?? '').toUpperCase());
    if (!d?.chatId) return json(res, 200, { ok: false, reason: 'not-paired' });

    const text = format(event, payload);
    if (!text) return json(res, 400, { error: `Unknown event "${event}"` });

    const sent = await sendMessage(d.chatId, text);
    if (sent) {
      d.sent += 1;
      d.lastSentAt = Date.now();
    }
    json(res, 200, { ok: !!sent });
  });

  app.get('/api/telegram/devices', (_req, res) => {
    json(res, 200, {
      enabled: cfg.enabled,
      configured: !!cfg.token,
      paired: [...devices.values()].filter((d) => d.chatId).length,
      registered: devices.size,
    });
  });

  /* ---------------------------------------------------------- formatting -- */

  function format(event, p = {}) {
    const money = (n) => `${Number(n) >= 0 ? '+' : '-'}$${Math.abs(Number(n) || 0).toFixed(2)}`;
    const price = (v, s) => (v == null ? '—' : s === 'USDJPY' ? Number(v).toFixed(3) : Number(v).toFixed(s === 'XAUUSD' ? 2 : 5));

    switch (event) {
      case 'trade_opened':
        return (
          `🟢 <b>OPENED ${String(p.side).toUpperCase()}</b> ${p.symbol}\n` +
          `<b>${p.volume} lots</b> @ ${price(p.entryPrice, p.symbol)}\n` +
          `SL ${price(p.sl, p.symbol)} · TP ${price(p.tp, p.symbol)}`
        );
      case 'trade_closed':
        return (
          `⚪ <b>CLOSED ${String(p.side).toUpperCase()}</b> ${p.symbol}\n` +
          `${p.volume} lots · ${p.closeReason ?? 'closed'} @ ${price(p.closePrice, p.symbol)}\n` +
          `Result <b>${money(p.totalProfit)}</b>`
        );
      case 'sl_hit':
        return (
          `🔴 <b>STOP LOSS</b> ${p.symbol}\n` +
          `${String(p.side).toUpperCase()} ${p.volume} lots closed @ ${price(p.closePrice, p.symbol)}\n` +
          `Result <b>${money(p.totalProfit)}</b>`
        );
      case 'tp_hit':
        return (
          `🟢 <b>TAKE PROFIT</b> ${p.symbol}\n` +
          `${String(p.side).toUpperCase()} ${p.volume} lots closed @ ${price(p.closePrice, p.symbol)}\n` +
          `Result <b>${money(p.totalProfit)}</b>`
        );
      case 'margin_call':
        return `⚠️ <b>MARGIN CALL</b>\nEquity <b>${money(p.equity)}</b>\nMargin level <b>${p.marginLevel}%</b>`;
      case 'ai_signal':
        return `🧠 <b>AI READ</b> ${p.symbol ?? ''} ${p.intervalLabel ?? ''}\n\n${p.summary ?? ''}`;
      default:
        return null;
    }
  }

  return {
    config: cfg,
    devices,
    stop: () => {
      if (pollTimer) clearTimeout(pollTimer);
      pollTimer = null;
    },
  };
}
