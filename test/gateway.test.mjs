import test from 'node:test';
import assert from 'node:assert/strict';
import { createHmac } from 'node:crypto';
import { createGateway, classifyError } from '../src/gateway.mjs';
import { createServer } from '../src/server.mjs';
import { createWorker } from '../src/worker.mjs';
import { loadConfig } from '../src/config.mjs';
import { jsonLogger, sanitize } from '../src/log.mjs';

const secret = 'synthetic-only';
const sign = x => 'sha256=' + createHmac('sha256', secret).update(x).digest('hex');
const valid = { event_id: 'test-1', consent_granted: true, consent_timestamp: '2026-11-17T00:00:00Z', consent_source: 'synthetic', email: 'synthetic@example.invalid' };

// In-memory store with the same contract as postgres-store.mjs (attempts counted at claim).
function memoryStore() {
  const rows = new Map(); let clock = 0;
  const due = e => (['pending', 'retry'].includes(e.state) && e.nextAttemptAt <= clock) || (e.state === 'processing' && e.leaseUntil < clock);
  return {
    rows, advance(ms) { clock += ms; },
    async insertIfAbsent(e) { if (rows.has(e.id)) return false; rows.set(e.id, { ...e, state: 'pending', attempts: 0, nextAttemptAt: 0, leaseVersion: 0 }); return true; },
    async claim(id, token, { maxAttempts }) {
      const e = rows.get(id); if (!e || e.attempts >= maxAttempts || !due(e)) return null;
      Object.assign(e, { state: 'processing', token, leaseUntil: clock + 60000 }); e.leaseVersion++; e.attempts++;
      return { id, payload: e.payload, attempts: e.attempts, leaseVersion: e.leaseVersion };
    },
    async complete(id, token, remoteId, v) { const e = rows.get(id); if (e.state !== 'processing' || e.token !== token || e.leaseVersion !== v) return false; Object.assign(e, { state: 'complete', remoteId, token: null }); return true; },
    async fail(id, token, c, v) { const e = rows.get(id); if (e.state !== 'processing' || e.token !== token || e.leaseVersion !== v) return false; Object.assign(e, { state: c.state, nextAttemptAt: clock + c.delayMs, errorCode: c.errorCode, token: null }); return true; },
    async deadLetterExhausted(max) { const out = []; for (const e of rows.values()) if (e.attempts >= max && due(e)) { e.state = 'dead_letter'; e.errorCode = 'attempts_exhausted'; out.push(e.id); } return out; },
    async listDue(n) { return [...rows.values()].filter(due).slice(0, n).map(e => e.id); },
    async replay(id) { const e = rows.get(id); if (e?.state !== 'dead_letter') return false; Object.assign(e, { state: 'retry', attempts: 0, nextAttemptAt: clock }); return true; },
    async stats() { const s = {}; for (const e of rows.values()) s[e.state] = (s[e.state] ?? 0) + 1; return s; }
  };
}

function setup(remote = {}, opts = {}) {
  const store = memoryStore(); const logs = [];
  remote = { idempotencyGuaranteed: true, findByExternalId: async () => null, create: async () => ({ id: 'remote' }), ...remote };
  const gateway = createGateway({ store, secret, remote, maxAttempts: 3, log: f => logs.push(f), ...opts });
  return { store, rows: store.rows, gateway, logs };
}
async function submit(gateway, event = valid) { const b = JSON.stringify(event); return gateway.intake(b, sign(b)); }

test('invalid signature and malformed JSON rejected', async () => {
  const { gateway } = setup();
  assert.equal((await gateway.intake('{}', '')).status, 401);
  assert.equal((await gateway.intake('{}', 'sha256=' + '0'.repeat(64))).status, 401);
  assert.equal((await gateway.intake('{', sign('{'))).status, 400);
});
test('missing consent evidence rejected', async () => {
  const { gateway } = setup();
  assert.equal((await submit(gateway, { event_id: 'x', consent_granted: true })).status, 422);
});
test('consent denied suppressed', async () => {
  const { gateway, rows } = setup();
  assert.equal((await submit(gateway, { ...valid, consent_granted: false })).state, 'suppressed_no_consent');
  assert.equal(rows.size, 0);
});
test('concurrent duplicates delivered once', async () => {
  let n = 0; const { gateway, rows } = setup({ create: async () => { n++; return { id: 'remote' }; } });
  await Promise.all([submit(gateway), submit(gateway)]);
  await Promise.all([gateway.process(valid.event_id), gateway.process(valid.event_id)]);
  assert.equal(n, 1); assert.equal(rows.get(valid.event_id).state, 'complete');
});
test('existing remote reconciled without create', async () => {
  let n = 0; const { gateway, rows } = setup({ findByExternalId: async () => ({ id: 'existing' }), create: async () => { n++; return { id: 'new' }; } });
  await submit(gateway); await gateway.process(valid.event_id);
  assert.equal(n, 0); assert.equal(rows.get(valid.event_id).remoteId, 'existing');
});
test('transient failure retries after backoff', async () => {
  let n = 0; const { gateway, rows, store } = setup({ create: async () => { if (++n === 1) throw Object.assign(Error('t'), { code: 'remote_5xx' }); return { id: 'remote' }; } });
  await submit(gateway); await gateway.process(valid.event_id);
  const e = rows.get(valid.event_id);
  assert.equal(e.state, 'retry'); assert.equal(e.errorCode, 'remote_5xx');
  assert.equal(await gateway.process(valid.event_id), false, 'not due before backoff');
  store.advance(1000); await gateway.process(valid.event_id);
  assert.equal(e.state, 'complete'); assert.equal(e.attempts, 2);
});
test('unverified downstream idempotency fails closed', async () => {
  let n = 0; const { gateway, rows } = setup({ idempotencyGuaranteed: false, create: async () => { n++; return { id: 'remote' }; } });
  await submit(gateway);
  assert.equal(await gateway.process(valid.event_id), false);
  assert.equal(n, 0); assert.equal(rows.get(valid.event_id).state, 'pending'); assert.equal(rows.get(valid.event_id).attempts, 0);
});
test('worker whose lease was superseded mid-delivery cannot complete', async () => {
  // Worker A claims, then stalls inside remote.create; its lease expires and
  // worker B claims and completes. A's late completion must be rejected.
  let release; const stalled = new Promise(r => { release = r; });
  let call = 0;
  const { gateway, rows, store } = setup({ create: async () => (++call === 1 ? (await stalled, { id: 'late' }) : { id: 'fresh' }) });
  await submit(gateway);
  const a = gateway.process(valid.event_id);
  await new Promise(r => setImmediate(r));
  store.advance(61000);
  assert.equal(await gateway.process(valid.event_id), true);
  release();
  assert.equal(await a, false);
  const e = rows.get(valid.event_id);
  assert.equal(e.state, 'complete'); assert.equal(e.remoteId, 'fresh'); assert.equal(e.leaseVersion, 2);
});
test('permanent error dead-letters immediately without retry', async () => {
  let n = 0; const { gateway, rows } = setup({ create: async () => { n++; throw Object.assign(Error('bad'), { code: 'remote_4xx', permanent: true }); } });
  await submit(gateway); await gateway.process(valid.event_id);
  const e = rows.get(valid.event_id);
  assert.equal(e.state, 'dead_letter'); assert.equal(e.errorCode, 'remote_4xx'); assert.equal(n, 1);
});
test('retries are bounded then dead-lettered', async () => {
  let n = 0; const { gateway, rows, store } = setup({ create: async () => { n++; throw Object.assign(Error('x'), { code: 'rate_limited' }); } });
  await submit(gateway);
  for (let i = 0; i < 6; i++) { await gateway.process(valid.event_id); store.advance(3600000); }
  assert.equal(n, 3); assert.equal(rows.get(valid.event_id).state, 'dead_letter');
});
test('crash loop is dead-lettered by worker sweep', async () => {
  let n = 0; const { gateway, rows, store } = setup({ create: async () => { n++; return { id: 'r' }; } });
  await submit(gateway);
  for (let i = 0; i < 3; i++) { await store.claim(valid.event_id, 'crash-' + i, { maxAttempts: 3 }); store.advance(61000); }
  const worker = createWorker({ store, gateway, maxAttempts: 3 });
  const r = await worker.tick();
  assert.equal(r.deadLettered, 1); assert.equal(rows.get(valid.event_id).state, 'dead_letter'); assert.equal(n, 0);
});
test('dead letter can be replayed by operator', async () => {
  let fail = true; const { gateway, rows, store } = setup({ create: async () => { if (fail) throw Object.assign(Error('x'), { permanent: true }); return { id: 'ok' }; } });
  await submit(gateway); await gateway.process(valid.event_id);
  assert.equal(rows.get(valid.event_id).state, 'dead_letter');
  fail = false; assert.equal(await store.replay(valid.event_id), true);
  await gateway.process(valid.event_id);
  assert.equal(rows.get(valid.event_id).state, 'complete');
});
test('worker processes only due events', async () => {
  const { gateway, store, rows } = setup();
  await submit(gateway, { ...valid, event_id: 'a' }); await submit(gateway, { ...valid, event_id: 'b' });
  rows.get('b').nextAttemptAt = 5000;
  const r = await createWorker({ store, gateway }).tick();
  assert.equal(r.processed, 1); assert.equal(rows.get('a').state, 'complete'); assert.equal(rows.get('b').state, 'pending');
});
test('error codes are sanitized', () => {
  assert.equal(classifyError({ code: 'remote_5xx' }).code, 'remote_5xx');
  assert.equal(classifyError({ code: 'Bearer abc.def' }).code, 'delivery_failed');
  assert.equal(classifyError(null).code, 'delivery_failed');
});
test('logs never contain payload or consent details', async () => {
  const lines = []; const log = jsonLogger(l => lines.push(l));
  const { store } = setup();
  const gateway = createGateway({ store, secret, remote: { idempotencyGuaranteed: true, findByExternalId: async () => null, create: async () => ({ id: 'r' }) }, log });
  await submit(gateway); await gateway.process(valid.event_id);
  log({ msg: 'careless', payload: valid, email: valid.email, secret });
  const all = lines.join('\n');
  for (const s of ['synthetic@example.invalid', 'consent_source', secret]) assert.ok(!all.includes(s), s);
  assert.deepEqual(sanitize({ msg: 'x', email: 'y' }), { msg: 'x' });
});
test('config refuses unsupported or unsafe delivery modes', () => {
  const base = { DATABASE_URL: 'postgres://x', GATEWAY_WEBHOOK_SECRET: 'x'.repeat(32) };
  assert.equal(loadConfig(base).deliveryMode, 'disabled');
  assert.throws(() => loadConfig({ ...base, DELIVERY_MODE: 'ghl' }), /not supported/);
  assert.throws(() => loadConfig({ ...base, DELIVERY_MODE: 'synthetic' }), /SYNTHETIC_TEST_ONLY/);
  assert.throws(() => loadConfig({ ...base, GATEWAY_WEBHOOK_SECRET: 'short' }), /at least 32/);
  assert.equal(loadConfig({ ...base, DELIVERY_MODE: 'synthetic', SYNTHETIC_TEST_ONLY: 'true' }).deliveryMode, 'synthetic');
});

test('HTTP ingress: auth, size, media type, rate limit, health', async () => {
  const { gateway, store } = setup();
  const server = createServer({ gateway, store, maxBodyBytes: 1024, rateLimit: { capacity: 5, refillPerSecond: 0 } });
  await new Promise(r => server.listen(0, '127.0.0.1', r));
  const base = `http://127.0.0.1:${server.address().port}`;
  const post = (body, headers = {}) => fetch(base + '/webhooks/intake', { method: 'POST', body, headers: { 'content-type': 'application/json', ...headers } });
  try {
    const raw = JSON.stringify(valid);
    let r = await post(raw, { 'x-gateway-signature': sign(raw) });
    assert.equal(r.status, 202); assert.deepEqual(await r.json(), { state: 'pending' });
    r = await post(raw, { 'x-gateway-signature': sign(raw) }); assert.equal((await r.json()).state, 'duplicate');
    r = await post(raw); assert.equal(r.status, 401);
    r = await post('x'.repeat(2048)); assert.equal(r.status, 413);
    r = await fetch(base + '/webhooks/intake', { method: 'POST', body: raw, headers: { 'content-type': 'text/plain' } }); assert.equal(r.status, 415);
    r = await post(raw, { 'x-gateway-signature': sign(raw) }); assert.equal(r.status, 429, 'bucket exhausted after 5');
    r = await fetch(base + '/healthz'); assert.equal(r.status, 200); assert.equal((await r.json()).events.pending, 1);
    r = await fetch(base + '/other'); assert.equal(r.status, 404);
  } finally { server.close(); }
});
test('HTTP ingress returns 503 when storage fails so sender retries', async () => {
  const { gateway: _, store } = setup();
  store.insertIfAbsent = async () => { throw Error('db down'); };
  const gateway = createGateway({ store, secret, remote: {} });
  const server = createServer({ gateway, store });
  await new Promise(r => server.listen(0, '127.0.0.1', r));
  try {
    const raw = JSON.stringify(valid);
    const r = await fetch(`http://127.0.0.1:${server.address().port}/webhooks/intake`, { method: 'POST', body: raw, headers: { 'content-type': 'application/json', 'x-gateway-signature': sign(raw) } });
    assert.equal(r.status, 503);
  } finally { server.close(); }
});
