// Structured JSON logs. Only allow-listed fields are written, so payloads, consent
// details, contact data and secrets cannot leak through a careless log call.
const ALLOWED = new Set(['msg', 'level', 'event_id', 'state', 'status', 'lease_version', 'attempt',
  'error_code', 'error', 'reconciled', 'ms', 'port', 'delivery', 'worker', 'applied']);

export function sanitize(fields) {
  const out = {};
  for (const [k, v] of Object.entries(fields)) if (ALLOWED.has(k)) out[k] = v;
  return out;
}

export function jsonLogger(write = line => process.stdout.write(line + '\n')) {
  return fields => write(JSON.stringify({ ts: new Date().toISOString(), level: 'info', ...sanitize(fields) }));
}
