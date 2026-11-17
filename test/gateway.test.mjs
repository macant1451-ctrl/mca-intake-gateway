import test from 'node:test';
import assert from 'node:assert/strict';
import {createHmac} from 'node:crypto';
import {createGateway} from '../src/gateway.mjs';
const secret='synthetic-only';
const sign=x=>'sha256='+createHmac('sha256',secret).update(x).digest('hex');
const valid={event_id:'test-1',consent_granted:true,consent_timestamp:'2026-11-17T00:00:00Z',consent_source:'synthetic'};
function setup(remote={findByExternalId:async()=>null,create:async()=>({id:'remote'})},clock=()=>10000){
 const rows=new Map();
 const store={
  async insertIfAbsent(e){if(rows.has(e.id))return false;rows.set(e);return true},
  async claim(id,token,now){const e=rows.get(id);if(!e||!['pending','retry'].includes(e.state)||e.nextAttemptAt>now)return null;e.state='processing';e.token=token;return {...e}},
  async complete(id,token,remoteId){const e=rows.get(id);assert.equal(e.token,token);Object.assign(e,{state:'complete',remoteId})},
  async fail(id,token,changes){const e=rows.get(id);assert.equal(e.token,token);Object.assign(e,changes)}
 };
 return {rows,gateway:createGateway({store,secret,remote,now:clock})};
}
test('unsigned and invalid JSON rejected',async()=>{const {gateway}=setup();assert.equal((await gateway.intake('{}','')).status,401);assert.equal((await gateway.intake('{',sign('{'))).status,400)});
test('missing consent evidence rejected',async()=>{const {gateway}=setup();const b=JSON.stringify({event_id:'x',consent_granted:true});assert.equal((await gateway.intake(b,sign(b))).status,422)});
test('consent denied suppressed',async()=>{const {gateway,rows}=setup();const b=JSON.stringify({...valid,consent_granted:false});assert.equal((await gateway.intake(b,sign(b))).state,'suppressed_no_consent');assert.equal(rows.size,0)});
test('duplicate concurrent delivery prevented',async()=>{let n=0;const {gateway,rows}=setup({findByExternalId:async()=>null,create:async()=>{n++;return {id:'remote'}}});const b=JSON.stringify(valid);await Promise.all([gateway.intake(b,sign(b)),gateway.intake(b,sign(b))]);await Promise.all([gateway.process(valid.event_id),gateway.process(valid.event_id)]);assert.equal(n,1);assert.equal(rows.get(valid.event_id).state,'complete')});
test('reconcile existing remote record',async()=>{let n=0;const {gateway,rows}=setup({findByExternalId:async()=>({id:'existing'}),create:async()=>{n++;return {id:'new'}}});const b=JSON.stringify(valid);await gateway.intake(b,sign(b));await gateway.process(valid.event_id);assert.equal(n,0);assert.equal(rows.get(valid.event_id).remoteId,'existing')});
test('transient failure retries',async()=>{let t=10000,n=0;const {gateway,rows}=setup({findByExternalId:async()=>null,create:async()=>{if(++n===1)throw Error('timeout');return {id:'remote'}}},()=>t);const b=JSON.stringify(valid);await gateway.intake(b,sign(b));await gateway.process(valid.event_id);assert.equal(rows.get(valid.event_id).state,'retry');t=12000;await gateway.process(valid.event_id);assert.equal(rows.get(valid.event_id).state,'complete')});
