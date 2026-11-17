# Pilot-safe gateway correction (draft)

This branch is isolated from `main`. No production routing is authorized.

## Implemented prototype and validation
A standalone Node.js prototype was validated locally against five synthetic tests:
1. Invalid signature and malformed payload rejected
2. Missing consent suppressed
3. Concurrent duplicate events delivered once (in-memory)
4. Existing remote record reconciled without re-creation
5. Transient delivery failure retried with backoff

## Required production architecture
- Authenticate incoming webhook using HMAC-SHA256 over raw request bytes; reject malformed signatures.
- Require stable event ID and consent grant, timestamp and source; redact sensitive data from logs.
- Store events in a durable transactional database with UNIQUE(event_id).
- Use an atomic lease claim and fencing token for each delivery attempt.
- Persist pending state BEFORE attempting a remote create.
- Before each retry, look up remote records by stable external ID; require downstream idempotency or unique external-ID enforcement.
- Handle ambiguous remote timeouts via reconciliation; never blindly create again.
- Use bounded exponential backoff and a dead-letter queue.
- Keep live outreach and production routing disabled pending approval.

## Limitations
Local synthetic tests used an in-memory store. They do NOT prove distributed atomicity, persistence across restarts, downstream idempotency, or a working Railway deployment. A production adapter and end-to-end staging tests remain blockers.
