# MCA Intake Gateway — operations runbook

## Configuration

| Variable | Required | Default | Notes |
|---|---|---|---|
| `DATABASE_URL` | yes | — | Dedicated PostgreSQL database for the gateway |
| `GATEWAY_WEBHOOK_SECRET` | yes | — | ≥ 32 chars; shared HMAC secret with the sender. Set in Railway, never in git |
| `DELIVERY_MODE` | no | `disabled` | `disabled` = accept and persist only. `synthetic` = test delivery (needs `SYNTHETIC_TEST_ONLY=true`). No other value is accepted |
| `SIGNATURE_HEADER` | no | `x-gateway-signature` | Header carrying `sha256=<hex HMAC of raw body>` |
| `PORT` | no | `8080` | |
| `MAX_ATTEMPTS` | no | `5` | 1–20 |
| `LEASE_SECONDS` | no | `60` | ≥ 5; must exceed worst-case delivery time |
| `WORKER_INTERVAL_MS` | no | `2000` | |

The process refuses to start on invalid configuration and logs which settings failed.

## Deploy (staging)
1. Create a **new** Railway service from this repo/branch using `Dockerfile`. Do not
   repoint the existing `apollo-*` services.
2. Attach a dedicated PostgreSQL database; set `DATABASE_URL` by reference.
3. Set `GATEWAY_WEBHOOK_SECRET`; leave `DELIVERY_MODE` unset (disabled).
4. Deploy. Migrations run on startup under an advisory lock.
5. Verify `GET /healthz` → `{"ok":true,"delivery":"disabled",...}`.
6. Send one signed synthetic event; confirm `202 pending` and the row in `intake_events`.

## Rollback
- Code: redeploy the previous Railway deployment (Deployments → previous → Redeploy).
- Schema: migrations only add tables, columns and indexes, so older code runs against
  the newer schema. Do not drop tables during rollback.
- Emergency stop of delivery: set `DELIVERY_MODE=disabled` and redeploy. Intake keeps
  persisting events; nothing is sent downstream; events resume when re-enabled.

## Operations
Counts by state: `GET /healthz`, or
`SELECT state, count(*) FROM intake_events GROUP BY state;`

Dead letters with reason:
`SELECT id, attempts, error_code, updated_at FROM intake_events WHERE state='dead_letter' ORDER BY updated_at DESC;`

Attempt history for one event:
`SELECT * FROM intake_delivery_attempts WHERE event_id = $1 ORDER BY lease_version;`

Replay a dead letter (after fixing the cause):
`UPDATE intake_events SET state='retry', attempts=0, next_attempt_at=now(), error_code=NULL WHERE id=$1 AND state='dead_letter';`
(same as `store.replay(id)`). The worker re-checks the downstream by external ID
before creating, so a replay does not duplicate an existing record.

Error codes: `remote_5xx`, `rate_limited`, `remote_timeout` (retried);
`remote_4xx` with `permanent` (dead-lettered immediately); `attempts_exhausted`
(crash loop or retries used up); `delivery_failed` (unclassified).

## Alerts to configure
- `/healthz` non-200.
- `dead_letter` count increasing.
- Oldest `pending`/`retry` event older than 15 minutes while delivery is enabled.
