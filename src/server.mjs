import http from 'node:http';

// HTTP ingress. Reads the raw body (bounded), verifies via gateway.intake, and never
// echoes payloads. Per-client token-bucket rate limiting protects the database.
export function createServer({
  gateway, store,
  path = '/webhooks/intake',
  signatureHeader = 'x-gateway-signature',
  maxBodyBytes = 64 * 1024,
  rateLimit = { capacity: 60, refillPerSecond: 2 },
  deliveryMode = 'disabled',
  log = () => {},
  clock = () => Date.now()
}) {
  const buckets = new Map();

  function allow(key) {
    const now = clock();
    let b = buckets.get(key);
    if (!b) { b = { tokens: rateLimit.capacity, at: now }; buckets.set(key, b); }
    b.tokens = Math.min(rateLimit.capacity, b.tokens + ((now - b.at) / 1000) * rateLimit.refillPerSecond);
    b.at = now;
    if (buckets.size > 10000) for (const [k, v] of buckets) if (v.tokens >= rateLimit.capacity) buckets.delete(k);
    if (b.tokens < 1) return false;
    b.tokens -= 1;
    return true;
  }

  function send(res, status, body) {
    res.writeHead(status, { 'content-type': 'application/json', 'cache-control': 'no-store' });
    res.end(JSON.stringify(body));
  }

  function readBody(req) {
    return new Promise((resolve, reject) => {
      const declared = Number(req.headers['content-length']);
      if (Number.isFinite(declared) && declared > maxBodyBytes) return reject(Object.assign(Error('too_large'), { status: 413 }));
      const chunks = []; let size = 0;
      req.on('data', c => {
        size += c.length;
        if (size > maxBodyBytes) { req.destroy(); reject(Object.assign(Error('too_large'), { status: 413 })); }
        else chunks.push(c);
      });
      req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
      req.on('error', reject);
    });
  }

  const server = http.createServer(async (req, res) => {
    const started = clock();
    const url = new URL(req.url, 'http://local');
    try {
      if (req.method === 'GET' && url.pathname === '/healthz') {
        try { return send(res, 200, { ok: true, delivery: deliveryMode, events: await store.stats() }); }
        catch { return send(res, 503, { ok: false }); }
      }
      if (url.pathname !== path) return send(res, 404, { state: 'not_found' });
      if (req.method !== 'POST') return send(res, 405, { state: 'method_not_allowed' });
      if (!allow(req.socket.remoteAddress ?? 'unknown')) return send(res, 429, { state: 'rate_limited' });
      if (!String(req.headers['content-type'] ?? '').startsWith('application/json')) return send(res, 415, { state: 'unsupported_media_type' });
      const raw = await readBody(req);
      const result = await gateway.intake(raw, req.headers[signatureHeader]);
      log({ msg: 'http', status: result.status, state: result.state, ms: clock() - started });
      return send(res, result.status, { state: result.state });
    } catch (err) {
      const status = err?.status ?? 503; // 503 so the sender retries; the event was not stored.
      log({ msg: 'http', status, state: status === 413 ? 'too_large' : 'unavailable', ms: clock() - started, level: status >= 500 ? 'error' : 'warn' });
      if (!res.headersSent) send(res, status, { state: status === 413 ? 'too_large' : 'unavailable' });
    }
  });
  server.requestTimeout = 10000;
  server.headersTimeout = 10000;
  return server;
}
