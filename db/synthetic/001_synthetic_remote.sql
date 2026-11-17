-- Isolated staging only. NEVER run against a real GHL/Trulli database.
CREATE TABLE IF NOT EXISTS synthetic_remote_records (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 external_id text NOT NULL UNIQUE,
 payload jsonb NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now()
);
