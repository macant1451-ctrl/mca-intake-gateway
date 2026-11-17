// Polls for due events (new, retry-scheduled, or lease-expired) and processes them.
// Safe to run in several replicas: store.claim is atomic, so each event is held by one worker.
export function createWorker({ store, gateway, maxAttempts = 5, batchSize = 25, intervalMs = 2000, log = () => {} }) {
  let timer = null, running = false, stopped = true;

  async function tick() {
    if (running) return { skipped: true };
    running = true;
    let processed = 0, exhausted = [];
    try {
      exhausted = await store.deadLetterExhausted(maxAttempts);
      for (const id of exhausted) log({ msg: 'delivery', event_id: id, state: 'dead_letter', error_code: 'attempts_exhausted', level: 'warn' });
      for (const id of await store.listDue(batchSize)) {
        try { if (await gateway.process(id)) processed++; }
        catch (err) { log({ msg: 'worker_error', event_id: id, error: err?.code ?? 'unexpected', level: 'error' }); }
      }
    } catch (err) {
      log({ msg: 'worker_error', error: err?.code ?? 'unexpected', level: 'error' });
    } finally { running = false; }
    return { processed, deadLettered: exhausted.length };
  }

  function loop() {
    if (stopped) return;
    timer = setTimeout(async () => { await tick(); loop(); }, intervalMs);
  }

  return {
    tick,
    start() { if (stopped) { stopped = false; loop(); } },
    async stop() { stopped = true; clearTimeout(timer); while (running) await new Promise(r => setTimeout(r, 20)); }
  };
}
