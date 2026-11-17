import {createHmac,timingSafeEqual,randomUUID} from 'node:crypto';

export function createGateway({store,secret,remote,now=()=>Date.now()}) {
 if(!store||!secret||!remote) throw Error('Missing dependencies');
 function signed(raw,signature) {
  if(typeof raw!=='string'||typeof signature!=='string'||!/^sha256=[a-f0-9]{64}$/.test(signature))return false;
  const expected=createHmac('sha256',secret).update(raw).digest('hex');
  return timingSafeEqual(Buffer.from(signature.slice(7),'hex'),Buffer.from(expected,'hex'));
 }
 async function intake(raw,signature) {
  if(!signed(raw,signature))return {status:401,state:'unauthorized'};
  let data;try{data=JSON.parse(raw)}catch{return {status:400,state:'invalid_json'}}
  if(!data||typeof data.event_id!=='string'||!data.event_id.trim()||data.event_id.length>200||typeof data.consent_granted!=='boolean'||typeof data.consent_source!=='string'||!data.consent_source.trim()||typeof data.consent_timestamp!=='string'||!Number.isFinite(Date.parse(data.consent_timestamp)))return {status:422,state:'invalid_event'};
  if(!data.consent_granted)return {status:202,state:'suppressed_no_consent'};
  const created=await store.insertIfAbsent({id:data.event_id,payload:data,consent:{at:data.consent_timestamp,source:data.consent_source}});
  return {status:202,state:created?'pending':'duplicate'};
 }
 async function process(id) {
  // Fail closed before claiming: never consume retries without a verified remote guarantee.
  if(remote.idempotencyGuaranteed!==true||typeof remote.findByExternalId!=='function'||typeof remote.create!=='function')return false;
  const token=randomUUID(),event=await store.claim(id,token,now());
  if(!event)return false;
  try {
   const existing=await remote.findByExternalId(event.id);
   const record=existing??await remote.create({...event.payload,external_id:event.id});
   if(!record?.id)throw Error('remote_id_missing');
   const completed=await store.complete(id,token,record.id,event.leaseVersion);
   return completed===true;
  }catch{
   const attempts=event.attempts+1;
   await store.fail(id,token,{attempts,state:attempts>=5?'dead_letter':'retry',nextAttemptAt:now()+Math.min(3600000,1000*2**attempts),errorCode:'delivery_failed'},event.leaseVersion);
   return false;
  }
 }
 return {intake,process};
}
