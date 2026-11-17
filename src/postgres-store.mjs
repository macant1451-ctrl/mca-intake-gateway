// PostgreSQL store for gateway.mjs. Requires a pg Pool injected by the caller.
// All mutations after claim are fenced with lease_token + lease_version, so a
// stale worker cannot commit. All scheduling uses the database clock.
//
// Attempts are counted when an event is CLAIMED, not when delivery fails. A
// worker that crashes mid-delivery therefore still consumes an attempt, and a
// crash-looping event is dead-lettered instead of being reclaimed forever.

const DUE = `((state IN ('pending','retry') AND next_attempt_at <= now())
              OR (state = 'processing' AND lease_until < now()))`;

export function postgresStore(pool, { leaseSeconds = 60 } = {}) {
  return {
    async insertIfAbsent(event) {
      const q = await pool.query(
        `INSERT INTO intake_events(id, payload, consent, state)
         VALUES ($1, $2::jsonb, $3::jsonb, 'pending')
         ON CONFLICT (id) DO NOTHING RETURNING id`,
        [event.id, JSON.stringify(event.payload), JSON.stringify(event.consent)]);
      return q.rowCount === 1;
    },

    // Atomically claim one due event, count the attempt, open an audit row,
    // and close any audit row left open by a worker whose lease expired.
    async claim(id, token, { maxAttempts }) {
      const q = await pool.query(
        `WITH c AS (
           UPDATE intake_events
              SET state = 'processing', lease_token = $2::uuid,
                  lease_version = lease_version + 1, attempts = attempts + 1,
                  lease_until = now() + ($3::int * interval '1 second'), updated_at = now()
            WHERE id = $1 AND attempts < $4 AND ${DUE}
            RETURNING id, payload, attempts, lease_version),
         expired AS (
           UPDATE intake_delivery_attempts a
              SET result = 'lease_expired', finished_at = now()
             FROM c
            WHERE a.event_id = c.id AND a.result IS NULL AND a.lease_version < c.lease_version),
         opened AS (
           INSERT INTO intake_delivery_attempts(event_id, lease_version)
           SELECT id, lease_version FROM c)
         SELECT * FROM c`,
        [id, token, leaseSeconds, maxAttempts]);
      if (!q.rowCount) return null;
      const e = q.rows[0];
      return { id: e.id, payload: e.payload, attempts: e.attempts, leaseVersion: Number(e.lease_version) };
    },

    async complete(id, token, remoteId, version) {
      const q = await pool.query(
        `WITH u AS (
           UPDATE intake_events
              SET state = 'complete', remote_id = $4, error_code = NULL,
                  lease_token = NULL, lease_until = NULL, updated_at = now()
            WHERE id = $1 AND lease_token = $2::uuid AND lease_version = $3 AND state = 'processing'
            RETURNING id, lease_version),
         a AS (
           UPDATE intake_delivery_attempts d SET result = 'complete', finished_at = now()
             FROM u WHERE d.event_id = u.id AND d.lease_version = u.lease_version)
         SELECT id FROM u`,
        [id, token, version, String(remoteId)]);
      return q.rowCount === 1;
    },

    // changes: { state: 'retry'|'dead_letter', delayMs, errorCode }
    async fail(id, token, changes, version) {
      const q = await pool.query(
        `WITH u AS (
           UPDATE intake_events
              SET state = $4, next_attempt_at = now() + ($5::bigint * interval '1 millisecond'),
                  error_code = $6, lease_token = NULL, lease_until = NULL, updated_at = now()
            WHERE id = $1 AND lease_token = $2::uuid AND lease_version = $3 AND state = 'processing'
            RETURNING id, lease_version),
         a AS (
           UPDATE intake_delivery_attempts d SET result = $4, error_code = $6, finished_at = now()
             FROM u WHERE d.event_id = u.id AND d.lease_version = u.lease_version)
         SELECT id FROM u`,
        [id, token, version, changes.state, Math.max(0, Math.floor(changes.delayMs ?? 0)), changes.errorCode]);
      return q.rowCount === 1;
    },

    // Events that are due but have no attempts left (e.g. crash loop) go to dead_letter.
    async deadLetterExhausted(maxAttempts) {
      const q = await pool.query(
        `WITH u AS (
           UPDATE intake_events
              SET state = 'dead_letter', error_code = 'attempts_exhausted',
                  lease_token = NULL, lease_until = NULL, updated_at = now()
            WHERE attempts >= $1 AND ${DUE}
            RETURNING id),
         a AS (
           UPDATE intake_delivery_attempts d SET result = 'lease_expired', finished_at = now()
             FROM u WHERE d.event_id = u.id AND d.result IS NULL)
         SELECT id FROM u`,
        [maxAttempts]);
      return q.rows.map(r => r.id);
    },

    async listDue(limit) {
      const q = await pool.query(
        `SELECT id FROM intake_events WHERE ${DUE} ORDER BY next_attempt_at LIMIT $1`, [limit]);
      return q.rows.map(r => r.id);
    },

    // Operator action: send a dead-lettered event back through delivery.
    async replay(id) {
      const q = await pool.query(
        `UPDATE intake_events
            SET state = 'retry', attempts = 0, next_attempt_at = now(), error_code = NULL, updated_at = now()
          WHERE id = $1 AND state = 'dead_letter' RETURNING id`, [id]);
      return q.rowCount === 1;
    },

    async stats() {
      const q = await pool.query(`SELECT state, count(*)::int AS n FROM intake_events GROUP BY state`);
      return Object.fromEntries(q.rows.map(r => [r.state, r.n]));
    }
  };
}
