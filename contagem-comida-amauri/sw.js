/* Contagem de Comida — service worker */
const CACHE = 'contagem-comida-v1';
const ARQUIVOS = ['./', './index.html', './app.css', './app.js', './config.js', './logo.js', './manifest.webmanifest', './icone-192.png', './icone-512.png'];
self.addEventListener('install', (e) => {
  e.waitUntil(caches.open(CACHE).then((c) => c.addAll(ARQUIVOS)).then(() => self.skipWaiting()));
});
self.addEventListener('activate', (e) => {
  e.waitUntil(caches.keys().then((ks) => Promise.all(ks.filter((k) => k !== CACHE && k.startsWith('contagem-comida-')).map((k) => caches.delete(k)))).then(() => self.clients.claim()));
});
self.addEventListener('fetch', (e) => {
  const u = new URL(e.request.url);
  if (e.request.method !== 'GET') return;
  if (u.hostname.endsWith('supabase.co')) return;            // banco sempre online
  const guarda = (res) => { const copia = res.clone(); caches.open(CACHE).then((c) => c.put(e.request, copia)).catch(() => {}); return res; };
  if (u.origin === location.origin) {
    // arquivos do próprio app: primeiro a internet (pega a versão nova), cache só como reserva offline
    e.respondWith(fetch(e.request).then(guarda).catch(() => caches.match(e.request)));
    return;
  }
  if (u.hostname.endsWith('cdnjs.cloudflare.com') || u.hostname.endsWith('jsdelivr.net') || u.hostname.endsWith('gstatic.com') || u.hostname.endsWith('googleapis.com')) {
    e.respondWith(caches.match(e.request).then((hit) => hit || fetch(e.request).then(guarda)));
  }
});
