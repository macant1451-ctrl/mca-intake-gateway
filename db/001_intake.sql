-- PostgreSQL durable intake state. Apply only to isolated staging database.
CREATE TABLE IF NOT EXISTS intake_events (
 id text PRIMARY KEY,
 payload jsonb NOT NULL,
 consent jsonb NOT NULL,
 state text NOT NULL DEFAULT 'pending' CHECK (state IN ('pending','processing','retry','complete','dead_letter','reconcile_required')),
 attempts integer NOT NULL DEFAULT 0,
 next_attempt_at timestamptz NOT NULL DEFAULT now(),
 lease_until timestamptz,
 lease_token uuid,
 lease_version bigint NOT NULL DEFAULT 0,
 remote_id text,
 error_code text,
 created_at timestamptz NOT NULL DEFAULT now(),
 updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS intake_due_idx ON intake_events(next_attempt_at) WHERE state IN ('pending','retry');
CREATE TABLE IF NOT EXISTS intake_delivery_attempts (
 event_id text NOT NULL REFERENCES intake_events(id),
 lease_version bigint NOT NULL,
 started_at timestamptz NOT NULL DEFAULT now(),
 result text,
 PRIMARY KEY(event_id,lease_version)
);
