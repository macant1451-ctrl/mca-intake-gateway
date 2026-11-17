# Staging integration gate

**Overall: NO-GO for live pilot.** Gateway engineering gates below are met with CI
evidence. Launch remains blocked on the downstream adapter, staging deployment, and
authorization items in "Open launch blockers".

Evidence source: GitHub Actions workflow `Synthetic gateway tests`, jobs `synthetic`
(unit) and `postgres-integration` (real PostgreSQL 16, disposable per job, plus a
container smoke test of the runtime image). Test names below are the literal names
in `test/postgres.integration.mjs`.

## Scenario status

| # | Scenario | Status | Evidence |
|---|---|---|---|
| 1 | Invalid HMAC, malformed JSON, missing consent: reject, zero downstream writes | PASS | `[gate 1] …` |
| 2 | 20 parallel duplicate intakes + 20 workers: exactly one downstream record | PASS | `[gate 2] …` |
| 3 | Worker dies after remote create, before local completion: restart reconciles, no extra record | PASS (simulated) | `[gate 3] …` — the dying worker is simulated by claiming, creating remotely and abandoning the lease; no OS-level process kill |
| 4 | Expired lease, second claim, stale first completion fails | PASS | `[gate 4] …` (also stale `fail` rejected) |
| 5 | Remote timeout with unknown commit: lookup before create; unique external ID | PASS (synthetic remote) | `[gate 5] …` ×2 |
| 6 | Restart all workers after pending insert: completes | PASS | `[gate 6] …`, `[gate 6+9] …` end-to-end over HTTP |
| 7 | 429/500 bounded retry; terminal errors dead-letter without repeated creates | PASS | `[gate 7] …` ×2, crash-loop sweep, operator replay |
| 8 | Consent denied: no remote writes; consent evidence retained | PASS | `[gate 8] …`, `consent evidence is persisted with the event` |
| 9 | No secrets or personal data in logs; outreach off | PASS (gateway logs) | `[gate 6+9] …`, container smoke test log scan; delivery disabled by default |

## Gateway capabilities now implemented
- HTTP ingress (`src/server.mjs`): raw-body HMAC, 64 KB body limit, JSON content type,
  per-client rate limit, `/healthz`, 503 on storage failure so senders retry.
- Polling worker (`src/worker.mjs`): due, retry-scheduled and lease-expired events;
  safe across replicas.
- Attempts counted at claim; crash loops dead-letter (`attempts_exhausted`).
- DB-clock exponential backoff; permanent errors dead-letter immediately.
- Operator replay of dead letters; per-attempt audit rows in `intake_delivery_attempts`.
- Migration runner with advisory lock (`src/migrate.mjs`).
- Allow-listed JSON logging (`src/log.mjs`).
- Delivery disabled by default; config rejects any mode other than `disabled` and
  `synthetic` (`src/config.mjs`).

## Open launch blockers
1. **Real downstream adapters do not exist.** There is no GHL or Trulli delivery mode.
   - GHL `POST /contacts/upsert` dedupes on email/phone per the location's
     "Allow Duplicate Contact" setting, not on our `external_id`, and its behaviour under
     concurrent upserts is not documented. Before a GHL mode is added: confirm the
     location setting (duplicates disallowed), store `external_id` in a custom field,
     implement `findByExternalId` against it, and prove one-contact-per-event with
     concurrent sandbox calls.
   - Trulli: API and idempotency behaviour not yet inspected.
2. **No staging deployment.** Railway template deploys in project
   `3rd Step Capital Ops Relay` are accepted but create nothing; Railway's GitHub
   integration is not linked. The runtime image is proven in CI only.
3. **Clock skew** between app and DB no longer affects scheduling (DB clock only).
4. **Monitoring/alerting:** `/healthz` reports counts by state; no external alert on
   `dead_letter` growth yet.
5. **Consent retention policy:** evidence is stored with each event; retention period
   and deletion procedure are not defined.
6. **Apollo sender contract:** header name and payload shape must be confirmed against
   what Apollo (or the relay in front of it) actually sends; defaults are
   `x-gateway-signature` and the fields validated in `src/gateway.mjs`.
7. **Authorization:** live pilot requires explicit written approval.
