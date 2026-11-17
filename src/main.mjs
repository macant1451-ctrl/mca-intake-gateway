import pg from 'pg';
import { createGateway } from './gateway.mjs';
import { postgresStore } from './postgres-store.mjs';
import { createServer } from './server.mjs';
import { createWorker } from './worker.mjs';
import { migrate } from './migrate.mjs';
import { syntheticRemote } from './synthetic-remote.mjs';
import { jsonLogger } from './log.mjs';
import { loadConfig } from './config.mjs';

export async function start(env = process.env, log = jsonLogger()) {
  const cfg = loadConfig(env);
  const pool = new pg.Pool({ connectionString: cfg.databaseUrl, max: 10 });
  const applied = await migrate(pool);
  if (cfg.deliveryMode === 'synthetic') applied.push(...await migrate(pool, new URL('../db/synthetic/', import.meta.url)));
  log({ msg: 'migrations', applied });

  const store = postgresStore(pool, { leaseSeconds: cfg.leaseSeconds });
  const remote = cfg.deliveryMode === 'synthetic' ? syntheticRemote(pool) : { idempotencyGuaranteed: false };
  const gateway = createGateway({ store, secret: cfg.secret, remote, maxAttempts: cfg.maxAttempts, log });
  const server = createServer({ gateway, store, signatureHeader: cfg.signatureHeader, deliveryMode: cfg.deliveryMode, log });
  const worker = cfg.deliveryMode === 'disabled' ? null
    : createWorker({ store, gateway, maxAttempts: cfg.maxAttempts, intervalMs: cfg.workerIntervalMs, log });

  await new Promise(r => server.listen(cfg.port, r));
  worker?.start();
  log({ msg: 'started', port: cfg.port, delivery: cfg.deliveryMode, worker: !!worker });

  const shutdown = async () => {
    log({ msg: 'stopping' });
    server.close();
    await worker?.stop();
    await pool.end();
  };
  return { server, worker, store, pool, shutdown };
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const log = jsonLogger();
  start(process.env, log).then(({ shutdown }) => {
    for (const s of ['SIGTERM', 'SIGINT']) process.once(s, () => shutdown().then(() => process.exit(0)));
  }).catch(err => {
    log({ msg: 'startup_failed', error: err.errors ?? err.code ?? 'unexpected', level: 'error' });
    process.exit(1);
  });
}
