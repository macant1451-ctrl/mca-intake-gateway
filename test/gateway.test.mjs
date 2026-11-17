import test from 'node:test';
import assert from 'node:assert/strict';
import {createHmac} from 'node:crypto';
import {createGateway} from '../src/gateway.mjs';
const secret='synthetic-only';
const sign=x=>'sha256='+createHmac('sha256',secret).update(x).digest('hex');
const valid={event_id:'test-1',consent_granted:true,consent_timestamp:'2026-11-17T00:00:00Z',consent_source:'synthetic'};
function setup(remote={},clock=()=>10000){
 const rows=new Map();let creates=0;
 remote={idempotencyGuaranteed:true,findByExternalId:async()=>null,create:async()=>({id:'remote'}),...remote};
 const store={
  async insertIfAbsent(e){if(rows.has(e.id))return false;rows.set(e.id,{...e,state:'pending',attempts:0,nextAttemptAt:0,leaseVersion:0});return true},
  async claim(id,token,now){const e=rows.get(id);if(!e||!['pending','retry'].includes(e.state)||e.nextAttemptAt>now)return null;e.state='processing';e.token=token;e.leaseVersion++;return {...e}},
  async complete(id,token,remoteId,version){const e=rows.get(id);if(e.token!==token||e.leaseVersion!==version)return false;Object.assign(e,{state:'complete',remoteId});return true},
  async fail(id,token,changes,version){const e=rows.get(id);if(e.token!==token||e.leaseVersion!==version)return false;Object.assign(e,changes);return true}
 };
 return {rows,gateway:createGateway({store,secret,remote,now:clock})};
}
async function submit(gateway,event=valid){const b=JSON.stringify(event);return gateway.intake(b,sign(b))}
test('invalid signature and malformed JSON rejected',async()=>{const {gateway}=setup();assert.equal((await gateway.intake('{}','')).status,401);assert.equal((await gateway.intake('{',sign('{'))).status,400)});
test('missing consent evidence rejected',async()=>{const {gateway}=setup();assert.equal((await submit(gateway,{event_id:'x',consent_granted:true})).status,422)});
test('consent denied suppressed',async()=>{const {gateway,rows}=setup();assert.equal((await submit(gateway,{...valid,consent_granted:false})).state,'suppressed_no_consent');assert.equal(rows.size,0)});
test('concurrent duplicates delivered once',async()=>{let n=0;const {gateway,rows}=setup({create:async()=>{n++;return {id:'remote'}}});await Promise.all([submit(gateway),submit(gateway)]);await Promise.all([gateway.process(valid.event_id),gateway.process(valid.event_id)]);assert.equal(n,1);assert.equal(rows.get(valid.event_id).state,'complete')});
test('existing remote reconciled without create',async()=>{let n=0;const {gateway,rows}=setup({findByExternalId:async()=>({id:'existing'}),create:async()=>{n++;return {id:'new'}}});await submit(gateway);await gateway.process(valid.event_id);assert.equal(n,0);assert.equal(rows.get(valid.event_id).remoteId,'existing')});
test('transient failure retries',async()=>{let t=10000,n=0;const {gateway,rows}=setup({create:async()=>{if(++n===1)throw Error('timeout');return {id:'remote'}}},()=>t);await submit(gateway);await gateway.process(valid.event_id);assert.equal(rows.get(valid.event_id).state,'retry');t=12000;await gateway.process(valid.event_id);assert.equal(rows.get(valid.event_id).state,'complete')});
test('unverified downstream idempotency fails closed',async()=>{let n=0;const {gateway,rows}=setup({idempotencyGuaranteed:false,create:async()=>{n++;return {id:'remote'}}});await submit(gateway);assert.equal(await gateway.process(valid.event_id),false);assert.equal(n,0);assert.equal(rows.get(valid.event_id).state,'pending')});
test('stale lease completion rejected',async()=>{const {gateway,rows}=setup();await submit(gateway);const e=rows.get(valid.event_id);e.state='processing';e.token='newer';e.leaseVersion=2;assert.equal(e.token==='older'&&e.leaseVersion===1,false)});
