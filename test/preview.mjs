/* =============================================================================
 *  PREVIEW SERVER  —  run the whole engine without touching Deriv
 *  Developed by Eng Hasan Mohamad
 * -----------------------------------------------------------------------------
 *  Boots a mock Deriv WebSocket feed (same protocol, synthetic market) and
 *  starts server.js against it, so the dashboard, SSE stream and charts can be
 *  verified anywhere — including networks where Deriv is blocked.
 *
 *      npm run preview        → http://localhost:3000
 *
 *  For a live run against real Deriv instead:
 *      npm start
 * ============================================================================= */

import { spawn } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { startMockBiquote } from './mock-biquote.mjs';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, '..');

const PORT = Number(process.env.PORT ?? 3000);
const MOCK_PORT = Number(process.env.MOCK_PORT ?? 4599);

const mock = await startMockBiquote({
  port: MOCK_PORT,
  tickMs: 300,
  onLog: (...a) => console.log(...a),
});

const app = spawn(process.execPath, ['server.js'], {
  cwd: ROOT,
  env: {
    ...process.env,
    PORT: String(PORT),
    HOST: '127.0.0.1',
    BIQUOTE_WS: mock.url,   // ← the only difference from production
    BIQUOTE_API: mock.api,
  },
  stdio: 'inherit',
});

console.log('');
console.log('  ┌──────────────────────────────────────────────┐');
console.log('  │  PREVIEW MODE — synthetic market data         │');
console.log('  │  Dashboard: http://localhost:' + String(PORT).padEnd(9) + '        │');
console.log('  └──────────────────────────────────────────────┘');
console.log('');

const shutdown = async () => {
  app.kill('SIGINT');
  await mock.close();
  process.exit(0);
};
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
app.on('exit', (code) => {
  mock.close();
  process.exit(code ?? 0);
});
