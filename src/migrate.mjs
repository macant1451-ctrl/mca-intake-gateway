import { readFile, readdir } from 'node:fs/promises';

// Applies db/*.sql in filename order, once each, under an advisory lock so that
// concurrent replicas cannot race. Each file runs in its own transaction.
export async function migrate(pool, dir = new URL('../db/', import.meta.url)) {
  const client = await pool.connect();
  const applied = [];
  try {
    await client.query('SELECT pg_advisory_lock(727001)');
    await client.query(`CREATE TABLE IF NOT EXISTS schema_migrations (
      name text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())`);
    const done = new Set((await client.query('SELECT name FROM schema_migrations')).rows.map(r => r.name));
    for (const name of (await readdir(dir)).filter(f => f.endsWith('.sql')).sort()) {
      if (done.has(name)) continue;
      const sql = await readFile(new URL(name, dir), 'utf8');
      await client.query('BEGIN');
      try {
        await client.query(sql);
        await client.query('INSERT INTO schema_migrations(name) VALUES ($1)', [name]);
        await client.query('COMMIT');
        applied.push(name);
      } catch (err) { await client.query('ROLLBACK'); throw err; }
    }
  } finally {
    await client.query('SELECT pg_advisory_unlock(727001)').catch(() => {});
    client.release();
  }
  return applied;
}
