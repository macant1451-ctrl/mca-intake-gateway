import {createHmac,timingSafeEqual,randomUUID} from 'node:crypto';

// Prototype only. Inject a durable transactional store for any real deployment.
export function createGateway({store,secret,remote,now=()=>Date.now()}) {
  if(!store||!secret||!remote) throw Error('Missing dependencies');
  function signed(raw,signature) {
    if(typeof signature!=='string'||!/^sha256=[a-f0-9]{64}$/.test(signature)) return false;
    const digest=createHmac('sha256',secret).update(raw).digest('hex');
    return timingSafeEqual(Buffer.from(signature.slice(7)),Buffer.from(digest));
  }
  async function intake(raw,signature) {
    if(!signed(raw,signature)) return {status:401,state:'unauthorized'};
    let data;
    try {data=JSON.parse(raw)} catch {return {status:400,state:'invalid_json'}}
    if(typeof data?.event_id!=='string'||!data.event_id||typeof data.consent_granted!=='boolean'||!data.consent_source||!Number.isFinite(Date.parse(data.consent_timestamp))) return {status:422,state:'invalid_event'};
    if(!data.consent_granted) return {status:202,state:'suppressed_no_consent'};
    const created=await store.insertIfAbsent({id:data.event_id,payload:data,state:'pending',attempts:0,nextAttemptAt:0,consent:{at:data.consent_timestamp,source:data.consent_source}});
    return {status:202,state:created?'pending':'duplicate'};
  }
  async function process(id) {
    const token=randomUUID();
    const event=await store.claim(id,token,now());
    if(!event) return false;
    try {
      // A stable external ID alone is insufficient: downstream MUST enforce idempotency.\n      if(remote.idempotencyGuaranteed!==true) throw Error('downstream_idempotency_not_verified');\n      const existing=await remote.findByExternalId(event.id);
      const record=existing??await remote.create({...event.payload,external_id:event.id});
      if(!record?.id) throw Error('remote_id_missing');
      await store.complete(id,token,record.id,event.leaseVersion);
    } catch {
      const attempts=event.attempts+1;
      await store.fail(id,token,{attempts,state:attempts>=5?'dead_letter':'retry',nextAttemptAt:now()+Math.min(3600000,1000*2**attempts),errorCode:'delivery_failed'},event.leaseVersion);
    }
    return true;
  }
  return {intake,process};
}
