# Staging integration gate — NO GO until all checks pass

## Preconditions
- Isolated PostgreSQL staging database; apply db/001_intake.sql after review.
- No production credentials, real applicants, live routing, or outreach.
- Configure a sandbox remote that enforces unique external_id or idempotency key atomically.
- Synthetic-only data; scrub request logs.
- Ensure runtime and PostgreSQL clocks are synchronized.

## Required executable end-to-end scenarios
1. Invalid HMAC, malformed JSON, missing consent evidence: reject, zero downstream writes.
2. Parallel duplicate event delivery from 20 independent workers: exactly one downstream record.
3. Kill worker immediately after remote create but before local completion: restart and reconcile, zero additional remote records.
4. Expire first worker lease, let second worker claim, then finish first worker: first worker's stale completion must fail.
5. Remote timeout with unknown commit status: lookup by external_id before any create; downstream unique external ID must reject duplicates.
6. Restart all workers after pending insert: pending event survives and completes.
7. Remote 429/500: bounded retry; terminal errors dead-letter without repeated creates.
8. Consent denied: no remote writes; consent evidence retained according to policy.
9. Verify no secrets or personal data in logs and production outreach remains off.

## Critical limitations
- The committed Node tests are synthetic unit tests; they are not evidence of these integration scenarios.
- A downstream adapter must demonstrate its idempotency guarantee. A pre-create lookup alone is NOT sufficient against concurrent writes.
- Gateway worker scheduling, HTTP server, secrets configuration, monitoring, and recovery of expired leases need implementation.
- Postgres store and gateway require a matching production runtime; do not deploy this draft as-is.
