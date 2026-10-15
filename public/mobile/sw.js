/* =============================================================================
 *  Aetheron Signals — service worker
 *  -----------------------------------------------------------------------------
 *  Minimal on purpose. It exists so the app is installable and so the shell
 *  still opens offline. It MUST NEVER touch the live streams:
 *      • /stream            (SSE — caching it would freeze the chart)
 *      • /api/*             (signal + market REST)
 *      • any non-GET        (never cache)
 *  Those fall straight through to the network. Only the app shell is cached,
 *  and the HTML is network-first so a redeploy is picked up immediately.
 * ============================================================================= */

const CACHE = 'aetheron-mobile-v1';
const SHELL = [
  './',
  './index.html',
  './manifest.webmanifest',
  './icon.svg',
  './icon-192.png',
  './icon-512.png',
];

self.addEventListener('install', (e) => {
  e.waitUntil(caches.open(CACHE).then((c) => c.addAll(SHELL)).then(() => self.skipWaiting()).catch(() => {}));
});

self.addEventListener('activate', (e) => {
  e.waitUntil(
    caches.keys()
      .then((keys) => Promise.all(keys.filter((k) => k !== CACHE).map((k) => caches.delete(k))))
      .then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', (e) => {
  const req = e.request;
  const url = new URL(req.url);

  // Never intercept: live streams, the API, cross-origin (CDN), or writes.
  if (req.method !== 'GET') return;
  if (url.origin !== self.location.origin) return;
  if (url.pathname.startsWith('/api/') || url.pathname === '/stream') return;

  // Navigations / the document: network-first, cache as the offline fallback.
  if (req.mode === 'navigate' || url.pathname.endsWith('/') || url.pathname.endsWith('index.html')) {
    e.respondWith(
      fetch(req)
        .then((res) => {
          const copy = res.clone();
          caches.open(CACHE).then((c) => c.put(req, copy)).catch(() => {});
          return res;
        })
        .catch(() => caches.match(req).then((r) => r || caches.match('./index.html')))
    );
    return;
  }

  // Static assets: cache-first.
  e.respondWith(caches.match(req).then((r) => r || fetch(req)));
});
