/* =============================================================================
 *  UPSTREAM DIAGNOSTIC  —  does this machine can reach Deriv at all?
 *  Developed by Eng Hasan Mohamad
 * -----------------------------------------------------------------------------
 *  Run it in the SAME place the server runs (your PC, or the Render shell):
 *
 *      node diagnose.mjs
 *
 *  It tries several Deriv hosts and prints the real handshake result for each,
 *  so you can tell an IP/network block apart from a code problem.
 * ============================================================================= */

import { WebSocket } from 'ws';

const APP_ID = process.env.DERIV_APP_ID ?? '1089';
const HOSTS = [
  'ws.derivws.com',
  'ws.binaryws.com',
  'green.derivws.com',
  'blue.derivws.com',
  'red.derivws.com',
];

const c = { g: '\x1b[32m', r: '\x1b[31m', y: '\x1b[33m', d: '\x1b[90m', x: '\x1b[0m' };

console.log(`\nDeriv connectivity diagnostic   (app_id=${APP_ID})\n`);

const results = [];

for (const host of HOSTS) {
  const url = `wss://${host}/websockets/v3?app_id=${APP_ID}`;
  process.stdout.write(`  ${host.padEnd(22)} `);
  const r = await new Promise((resolve) => {
    const done = (verdict, detail) => resolve({ host, url, verdict, detail });
    let ws;
    try {
      ws = new WebSocket(url, { handshakeTimeout: 12_000 });
    } catch (e) {
      return done('ERROR', e.message);
    }
    const t = setTimeout(() => {
      try { ws.terminate(); } catch { /* ignore */ }
      done('TIMEOUT', 'no handshake within 12s');
    }, 12_000);

    ws.on('open', () => ws.send(JSON.stringify({ ping: 1, req_id: 1 })));
    ws.on('message', (m) => {
      clearTimeout(t);
      const body = m.toString().slice(0, 80);
      try { ws.close(); } catch { /* ignore */ }
      done('OK', body);
    });
    ws.on('error', (e) => {
      clearTimeout(t);
      done('ERROR', e.message);
    });
  });
  results.push(r);

  const colour = r.verdict === 'OK' ? c.g : r.verdict === 'TIMEOUT' ? c.y : c.r;
  const mark = r.verdict === 'OK' ? '✔ reachable' : `✖ ${r.verdict}`;
  console.log(`${colour}${mark}${c.x}  ${c.d}${r.detail}${c.x}`);
}

const ok = results.filter((r) => r.verdict === 'OK');
console.log('');

if (ok.length) {
  console.log(`${c.g}✔ ${ok.length}/${HOSTS.length} Deriv host(s) reachable from here.${c.x}`);
  console.log(`${c.d}  The upstream path works. If the dashboard still shows no data,`);
  console.log(`  look for a bug in server.js or a misconfigured environment variable.${c.x}\n`);
  console.log(`  Working host(s): ${c.x}${ok.map((r) => r.host).join(', ')}\n`);
  process.exit(0);
} else {
  console.log(`${c.r}✖ No Deriv host is reachable from this machine.${c.x}`);
  console.log(`
${c.y}This is a NETWORK / IP BLOCK, not a code problem.${c.x}
Deriv sits behind Cloudflare and rejects many datacenter and cloud-provider
IP ranges — which is exactly what Render, Railway, Fly.io, AWS and Azure use.

${c.d}Run this same script in two places to confirm:${c.x}
  ${c.d}•  on your PC  → if it works there, your ISP IP is allowed${c.x}
  ${c.d}•  on Render   → if it fails there, Render's datacenter IP is blocked${c.x}

${c.y}Fixes, roughly in order of effort:${c.x}
  1. Try a different Render region (see render.yaml) — sometimes one works.
  2. Deploy somewhere with a residential/clean IP:
       • a home server or VPS + Cloudflare Tunnel
       • a provider that hands out consumer-like IPs
  3. Put a small market-data proxy in front of Deriv (needs a clean IP too).
  4. Switch the data source away from Deriv (Twelve Data, Finnhub, Polygon…).

${c.d}Nothing in server.js will fix this — the TCP/TLS handshake never succeeds.${c.x}
`);
  process.exit(1);
}
