// Runtime configuration. Delivery is OFF unless explicitly set to a supported mode.
//   DELIVERY_MODE=disabled  (default) accept, verify and persist events; deliver nothing.
//   DELIVERY_MODE=synthetic requires SYNTHETIC_TEST_ONLY=true; delivers to a test table.
// There is deliberately no GHL or Trulli mode yet: a real adapter must first prove
// atomic idempotency (see STAGING_TEST_GATE.md) and launch must be authorized.
export function loadConfig(env = process.env) {
  const errors = [];
  const cfg = {
    databaseUrl: env.DATABASE_URL,
    secret: env.GATEWAY_WEBHOOK_SECRET,
    port: Number(env.PORT ?? 8080),
    deliveryMode: env.DELIVERY_MODE ?? 'disabled',
    signatureHeader: (env.SIGNATURE_HEADER ?? 'x-gateway-signature').toLowerCase(),
    maxAttempts: Number(env.MAX_ATTEMPTS ?? 5),
    leaseSeconds: Number(env.LEASE_SECONDS ?? 60),
    workerIntervalMs: Number(env.WORKER_INTERVAL_MS ?? 2000)
  };
  if (!cfg.databaseUrl) errors.push('DATABASE_URL is required');
  if (!cfg.secret || cfg.secret.length < 32) errors.push('GATEWAY_WEBHOOK_SECRET must be at least 32 characters');
  if (!['disabled', 'synthetic'].includes(cfg.deliveryMode)) errors.push(`DELIVERY_MODE '${cfg.deliveryMode}' is not supported`);
  if (cfg.deliveryMode === 'synthetic' && env.SYNTHETIC_TEST_ONLY !== 'true') errors.push('DELIVERY_MODE=synthetic requires SYNTHETIC_TEST_ONLY=true');
  if (!(cfg.maxAttempts >= 1 && cfg.maxAttempts <= 20)) errors.push('MAX_ATTEMPTS must be 1-20');
  if (!(cfg.leaseSeconds >= 5)) errors.push('LEASE_SECONDS must be >= 5');
  if (errors.length) throw Object.assign(Error('Invalid configuration: ' + errors.join('; ')), { errors });
  return cfg;
}
