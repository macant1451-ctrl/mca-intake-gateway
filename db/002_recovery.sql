-- Recovery and audit support. Apply only to isolated staging database until launch approval.
ALTER TABLE intake_delivery_attempts
  ADD COLUMN IF NOT EXISTS finished_at timestamptz,
  ADD COLUMN IF NOT EXISTS error_code text;
CREATE INDEX IF NOT EXISTS intake_expired_lease_idx ON intake_events(lease_until) WHERE state = 'processing';
CREATE INDEX IF NOT EXISTS intake_state_idx ON intake_events(state);
