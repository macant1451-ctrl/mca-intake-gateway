import { createHmac, timingSafeEqual, randomUUID } from 'node:crypto';

const ERROR_CODE = /^[a-z0-9_]{1,40}$/;

// Remote adapters may throw errors carrying:
//   err.code      short machine code, e.g. 'rate_limited', 'remote_5xx', 'remote_4xx'
//   err.permanent true when retrying cannot succeed (validation / 4xx other than 408/429)
export function classifyError(err) {
  const code = typeof err?.code === 'string' && ERROR_CODE.test(err.code) ? err.code : 'delivery_failed';
  return { code, permanent: err?.permanent === true };
}

export function createGateway({
  store, secret, remote,
  maxAttempts = 5, baseDelayMs = 1000, maxDelayMs = 3600000,
  log = () => {}
}) {
  if (!store || !secret || !remote) throw Error('Missing dependencies');

  function signed(raw, signature) {
    if (typeof raw !== 'string' || typeof signature !== 'string' || !/^sha256=[a-f0-9]{64}$/.test(signature)) return false;
    const expected = createHmac('sha256', secret).update(raw).digest('hex');
    return timingSafeEqual(Buffer.from(signature.slice(7), 'hex'), Buffer.from(expected, 'hex'));
  }

  function validEvent(d) {
    return d && typeof d === 'object'
      && typeof d.event_id === 'string' && d.event_id.trim() && d.event_id.length <= 200
      && typeof d.consent_granted === 'boolean'
      && typeof d.consent_source === 'string' && d.consent_source.trim()
      && typeof d.consent_timestamp === 'string' && Number.isFinite(Date.parse(d.consent_timestamp));
  }

  async function intake(raw, signature) {
    if (!signed(raw, signature)) return { status: 401, state: 'unauthorized' };
    let data;
    try { data = JSON.parse(raw); } catch { return { status: 400, state: 'invalid_json' }; }
    if (!validEvent(data)) return { status: 422, state: 'invalid_event' };
    if (!data.consent_granted) {
      log({ msg: 'intake', event_id: data.event_id, state: 'suppressed_no_consent' });
      return { status: 202, state: 'suppressed_no_consent' };
    }
    const created = await store.insertIfAbsent({
      id: data.event_id, payload: data,
      consent: { at: data.consent_timestamp, source: data.consent_source }
    });
    const state = created ? 'pending' : 'duplicate';
    log({ msg: 'intake', event_id: data.event_id, state });
    return { status: 202, state };
  }

  function deliveryEnabled() {
    return remote.idempotencyGuaranteed === true
      && typeof remote.findByExternalId === 'function' && typeof remote.create === 'function';
  }

  async function process(id) {
    // Fail closed before claiming: never consume attempts without a verified remote guarantee.
    if (!deliveryEnabled()) return false;
    const token = randomUUID();
    const event = await store.claim(id, token, { maxAttempts });
    if (!event) return false;
    const ctx = { event_id: event.id, lease_version: event.leaseVersion, attempt: event.attempts };
    try {
      // Look up by external ID first so an ambiguous earlier create is reconciled, not repeated.
      const existing = await remote.findByExternalId(event.id);
      const record = existing ?? await remote.create({ ...event.payload, external_id: event.id });
      if (!record?.id) throw Object.assign(Error('remote_id_missing'), { code: 'remote_id_missing' });
      const completed = await store.complete(id, token, record.id, event.leaseVersion);
      log({ msg: 'delivery', ...ctx, state: completed ? 'complete' : 'stale_lease', reconciled: !!existing });
      return completed === true;
    } catch (err) {
      const { code, permanent } = classifyError(err);
      const state = permanent || event.attempts >= maxAttempts ? 'dead_letter' : 'retry';
      const delayMs = Math.min(maxDelayMs, baseDelayMs * 2 ** (event.attempts - 1));
      const recorded = await store.fail(id, token, { state, delayMs, errorCode: code }, event.leaseVersion);
      log({ msg: 'delivery', ...ctx, state: recorded ? state : 'stale_lease', error_code: code, level: 'warn' });
      return false;
    }
  }

  return { intake, process, deliveryEnabled };
}
