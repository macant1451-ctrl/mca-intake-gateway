// PostgreSQL integration suite. Maps to the scenarios in STAGING_TEST_GATE.md.
// Runs ONLY against a disposable database: requires SYNTHETIC_TEST_ONLY=true and a
// database name containing "synthetic" or "test". It truncates gateway tables.
import test from 'node:test';
import assert from 'node:assert/strict';
import { createHmac } from 'node:crypto';
import pg from 'pg';
import { createGateway } from '../src/gateway.mjs';
import { postgresStore } from '../src/postgres-store.mjs';
import { syntheticRemote } from '../src/synthetic-remote.mjs';
import { createWorker } from '../src/worker.mjs';
import { migrate } from '../src/migrate.mjs';
import { start } from '../src/main.mjs';
import { jsonLogger } from '../src/log.mjs';

const url = process.env.TEST_DATABASE_URL;
if (!url) throw Error('TEST_DATABASE_URL required: isolated disposable PostgreSQL database only');
if (process.env.SYNTHETIC_TEST_ONLY !== 'true') throw Error('SYNTHETIC_TEST_ONLY=true required');
if (!/synthetic|test/i.test(new URL(url).pathname)) throw Error('Refusing: database name must contain "synthetic" or "test"');

const { Pool } = pg;
const pool = new Pool({ connectionString: url, max: 25 });
const secret = 'synthetic-test-secret';
const MAX = 3;
const sign = raw => 'sha256=' + createHmac('sha256', secret).update(raw).digest('hex');
const payload = id => ({ event_id: id, consent_granted: true, consent_source: 'synthetic', consent_timestamp: '2026-11-17T00:00:00Z', email: `${id}@example.invalid` });
const store = (leaseSeconds = 1) => postgresStore(pool, { leaseSeconds });
const gateway = (remote = syntheticRemote(pool), opts = {}) => createGateway({ store: store(), remote, secret, maxAttempts: MAX, baseDelayMs: 50, ...opts });
async function send(g, id, body = payload(id)) { const raw = JSON.stringify(body); return g.intake(raw, sign(raw)); }
const id = label => 'synthetic-' + label + '-' + Date.now() + '-' + Math.random().toString(36).slice(2);
const row = async key => (await pool.query('SELECT * FROM intake_events WHERE id=$1', [key])).rows[0];
const remoteCount = async key => (await pool.query('SELECT count(*)::int AS n FROM synthetic_remote_records WHERE external_id=$1', [key])).rows[0].n;
const attemptsFor = async key => (await pool.query('SELECT lease_version::int AS v, result, error_code FROM intake_delivery_attempts WHERE event_id=$1 ORDER BY lease_version', [key])).rows;
const makeDue = key => pool.query("UPDATE intake_events SET next_attempt_at=now()-interval '1 second' WHERE id=$1", [key]);
const expireLease = key => pool.query("UPDATE intake_events SET lease_until=now()-interval '1 second' WHERE id=$1", [key]);
const failing = (err, counter = { n: 0 }) => ({ ...syntheticRemote(pool), counter, async create() { counter.n++; throw err; } });

test.before(async () => {
  await migrate(pool);
  await migrate(pool, new URL('../db/synthetic/', import.meta.url));
  await pool.query('TRUNCATE intake_delivery_attempts, intake_events, synthetic_remote_records');
});
test.after(async () => pool.end());

test('migrations are idempotent', async () => {
  assert.deepEqual(await migrate(pool), []);
  const names = (await pool.query('SELECT name FROM schema_migrations ORDER BY name')).rows.map(r => r.name);
  assert.ok(names.includes('001_intake.sql') && names.includes('002_recovery.sql'));
});

test('[gate 1] invalid HMAC, malformed JSON, missing consent: rejected, nothing stored', async () => {
  const g = gateway(), key = id('reject');
  const raw = JSON.stringify(payload(key));
  assert.equal((await g.intake(raw, sign(raw + 'x'))).status, 401);
  assert.equal((await g.intake('{', sign('{'))).status, 400);
  const { consent_source, ...noEvidence } = payload(key);
  assert.equal((await send(g, key, noEvidence)).status, 422);
  assert.equal(await row(key), undefined);
  assert.equal(await remoteCount(key), 0);
});

test('[gate 2] 20 concurrent intakes and workers create one remote record', async () => {
  const key = id('parallel'), g = gateway();
  const intakes = await Promise.all(Array.from({ length: 20 }, () => send(g, key)));
  assert.equal(intakes.filter(r => r.state === 'pending').length, 1);
  const results = await Promise.all(Array.from({ length: 20 }, () => gateway().process(key)));
  assert.equal(results.filter(Boolean).length, 1);
  assert.equal(await remoteCount(key), 1);
  const r = await row(key); assert.equal(r.state, 'complete'); assert.equal(r.attempts, 1);
});

test('[gate 3] worker killed after remote create, before completion: restart reconciles, no extra record', async () => {
  const key = id('kill'); await send(gateway(), key);
  // Simulate the dying worker: claim and create remotely, then never complete.
  const dying = await store().claim(key, '00000000-0000-4000-8000-0000000000aa', { maxAttempts: MAX });
  await syntheticRemote(pool).create({ ...dying.payload, external_id: key });
  await expireLease(key);
  // "Restart": a brand-new gateway + worker picks it up.
  const g = gateway(); await createWorker({ store: store(), gateway: g, maxAttempts: MAX }).tick();
  const r = await row(key);
  assert.equal(r.state, 'complete'); assert.equal(r.attempts, 2); assert.equal(await remoteCount(key), 1);
  assert.deepEqual((await attemptsFor(key)).map(a => a.result), ['lease_expired', 'complete']);
});

test('[gate 4] stale lease holder cannot complete after newer claim', async () => {
  const key = id('lease'), s = store(); await s.insertIfAbsent({ id: key, payload: payload(key), consent: { source: 'synthetic' } });
  const old = await s.claim(key, '00000000-0000-4000-8000-000000000001', { maxAttempts: MAX });
  await expireLease(key);
  const newer = await s.claim(key, '00000000-0000-4000-8000-000000000002', { maxAttempts: MAX });
  assert.ok(newer.leaseVersion > old.leaseVersion);
  assert.equal(await s.complete(key, '00000000-0000-4000-8000-000000000001', 'stale', old.leaseVersion), false);
  assert.equal(await s.fail(key, '00000000-0000-4000-8000-000000000001', { state: 'retry', delayMs: 0, errorCode: 'x' }, old.leaseVersion), false);
  assert.equal(await s.complete(key, '00000000-0000-4000-8000-000000000002', 'current', newer.leaseVersion), true);
  assert.equal((await row(key)).remote_id, 'current');
});

test('[gate 5] remote timeout with unknown commit: lookup before create, one record', async () => {
  const key = id('timeout'); let first = true;
  const remote = syntheticRemote(pool, { afterCommit: async () => { if (first) { first = false; throw Object.assign(Error('timeout'), { code: 'remote_timeout' }); } } });
  const g = gateway(remote); await send(g, key);
  assert.equal(await g.process(key), false);
  let r = await row(key); assert.equal(r.state, 'retry'); assert.equal(r.error_code, 'remote_timeout');
  await makeDue(key); assert.equal(await g.process(key), true);
  r = await row(key); assert.equal(r.state, 'complete'); assert.equal(await remoteCount(key), 1);
});

test('[gate 5] synthetic remote unique external_id rejects duplicate creates', async () => {
  const key = id('unique'), remote = syntheticRemote(pool);
  const [a, b] = await Promise.all([remote.create({ external_id: key }), remote.create({ external_id: key })]);
  assert.equal(String(a.id), String(b.id)); assert.equal(await remoteCount(key), 1);
});

test('[gate 6] pending events survive full restart and complete via new worker', async () => {
  const keys = [id('restart-a'), id('restart-b')];
  for (const k of keys) await send(gateway(), k);
  const g = gateway(); await createWorker({ store: store(), gateway: g, maxAttempts: MAX, batchSize: 100 }).tick();
  for (const k of keys) { assert.equal((await row(k)).state, 'complete'); assert.equal(await remoteCount(k), 1); }
});

test('[gate 7] 429/500 retries are bounded with DB-clock backoff, then dead-letter', async () => {
  const key = id('bounded'), counter = { n: 0 };
  const g = gateway(failing(Object.assign(Error('busy'), { code: 'rate_limited' }), counter)); await send(g, key);
  await g.process(key);
  let r = await row(key);
  assert.equal(r.state, 'retry');
  const gap = (await pool.query('SELECT extract(epoch FROM next_attempt_at - updated_at)*1000 AS ms FROM intake_events WHERE id=$1', [key])).rows[0].ms;
  assert.ok(gap >= 40 && gap <= 1000, `backoff scheduled on DB clock (${gap}ms)`);
  assert.equal(await g.process(key), false, 'not due before backoff elapses');
  for (let i = 0; i < 5; i++) { await makeDue(key); await g.process(key); }
  r = await row(key);
  assert.equal(r.state, 'dead_letter'); assert.equal(r.attempts, MAX); assert.equal(counter.n, MAX);
  assert.deepEqual((await attemptsFor(key)).map(a => a.result), ['retry', 'retry', 'dead_letter']);
});

test('[gate 7] terminal 4xx dead-letters on first attempt without repeated creates', async () => {
  const key = id('terminal'), counter = { n: 0 };
  const g = gateway(failing(Object.assign(Error('invalid'), { code: 'remote_4xx', permanent: true }), counter)); await send(g, key);
  await g.process(key); await makeDue(key); await g.process(key);
  const r = await row(key); assert.equal(r.state, 'dead_letter'); assert.equal(r.attempts, 1); assert.equal(counter.n, 1);
});

test('crash-looping event is dead-lettered by the worker sweep', async () => {
  const key = id('crashloop'); await send(gateway(), key);
  for (let i = 0; i < MAX; i++) { await store().claim(key, `00000000-0000-4000-8000-00000000010${i}`, { maxAttempts: MAX }); await expireLease(key); }
  assert.equal(await store().claim(key, '00000000-0000-4000-8000-0000000001ff', { maxAttempts: MAX }), null, 'no further claims once exhausted');
  const res = await createWorker({ store: store(), gateway: gateway(), maxAttempts: MAX }).tick();
  assert.ok(res.deadLettered >= 1);
  const r = await row(key); assert.equal(r.state, 'dead_letter'); assert.equal(r.error_code, 'attempts_exhausted');
  assert.equal(await remoteCount(key), 0);
  assert.ok((await attemptsFor(key)).every(a => a.result === 'lease_expired'));
});

test('operator replay of dead letter delivers exactly once', async () => {
  const key = id('replay');
  await send(gateway(), key);
  await gateway(failing(Object.assign(Error('x'), { permanent: true }))).process(key);
  assert.equal((await row(key)).state, 'dead_letter');
  assert.equal(await store().replay(key), true);
  assert.equal(await store().replay(key), false, 'replay only from dead_letter');
  assert.equal(await gateway().process(key), true);
  assert.equal(await remoteCount(key), 1); assert.equal((await row(key)).state, 'complete');
});

test('[gate 8] consent denied: no row, no remote write', async () => {
  const key = id('denied');
  assert.equal((await send(gateway(), key, { ...payload(key), consent_granted: false })).state, 'suppressed_no_consent');
  assert.equal(await row(key), undefined); assert.equal(await remoteCount(key), 0);
});

test('consent evidence is persisted with the event', async () => {
  const key = id('evidence'); await send(gateway(), key);
  assert.deepEqual((await row(key)).consent, { at: '2026-11-17T00:00:00Z', source: 'synthetic' });
});

test('delivery disabled: events persist, nothing is claimed or delivered', async () => {
  const key = id('disabled'), g = gateway({ idempotencyGuaranteed: false });
  await send(g, key); assert.equal(await g.process(key), false);
  const r = await row(key); assert.equal(r.state, 'pending'); assert.equal(r.attempts, 0);
  await pool.query("UPDATE intake_events SET state='complete' WHERE id=$1", [key]); // keep later sweeps clean
});

test('[gate 6+9] end-to-end through real HTTP runtime; logs carry no personal data or secrets', async () => {
  const lines = [];
  const app = await start({
    DATABASE_URL: url, GATEWAY_WEBHOOK_SECRET: 'e2e-synthetic-secret-0123456789abcdef', PORT: '0',
    DELIVERY_MODE: 'synthetic', SYNTHETIC_TEST_ONLY: 'true', WORKER_INTERVAL_MS: '100', LEASE_SECONDS: '5'
  }, jsonLogger(l => lines.push(l)));
  try {
    const base = `http://127.0.0.1:${app.server.address().port}`;
    const key = id('e2e'), raw = JSON.stringify(payload(key));
    const sig = 'sha256=' + createHmac('sha256', 'e2e-synthetic-secret-0123456789abcdef').update(raw).digest('hex');
    const res = await fetch(base + '/webhooks/intake', { method: 'POST', body: raw, headers: { 'content-type': 'application/json', 'x-gateway-signature': sig } });
    assert.equal(res.status, 202);
    for (let i = 0; i < 50 && (await row(key))?.state !== 'complete'; i++) await new Promise(r => setTimeout(r, 100));
    assert.equal((await row(key)).state, 'complete'); assert.equal(await remoteCount(key), 1);
    const health = await (await fetch(base + '/healthz')).json();
    assert.equal(health.ok, true); assert.equal(health.delivery, 'synthetic');
  } finally { await app.shutdown(); }
  const all = lines.join('\n');
  assert.ok(all.includes('"state":"complete"'));
  for (const s of ['@example.invalid', 'consent_source', 'e2e-synthetic-secret', 'postgresql://']) assert.ok(!all.includes(s), `log leaked ${s}`);
});
