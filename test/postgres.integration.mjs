import test from 'node:test';
import assert from 'node:assert/strict';
import {createHmac} from 'node:crypto';
import pg from 'pg';
import {createGateway} from '../src/gateway.mjs';
import {postgresStore} from '../src/postgres-store.mjs';
import {syntheticRemote} from '../src/synthetic-remote.mjs';
import {readFile} from 'node:fs/promises';
const {Pool}=pg;
const url=process.env.TEST_DATABASE_URL;
if(!url)throw Error('TEST_DATABASE_URL required: isolated disposable PostgreSQL database only');
if(process.env.SYNTHETIC_TEST_ONLY!=='true')throw Error('SYNTHETIC_TEST_ONLY=true required');
const pool=new Pool({connectionString:url,max:25});
const secret='synthetic-test-secret';
const sign=raw=>'sha256='+createHmac('sha256',secret).update(raw).digest('hex');
const payload=id=>({event_id:id,consent_granted:true,consent_source:'synthetic',consent_timestamp:'2026-11-17T00:00:00Z'});
const gateway=(remote=syntheticRemote(pool))=>createGateway({store:postgresStore(pool,{leaseSeconds:1}),remote,secret});
async function send(g,id){const raw=JSON.stringify(payload(id));return g.intake(raw,sign(raw))}
await pool.query(await readFile(new URL('../db/001_intake.sql',import.meta.url),'utf8'));
await pool.query(await readFile(new URL('../db/002_synthetic_remote.sql',import.meta.url),'utf8'));
const id=label=>'synthetic-'+label+'-'+Date.now()+'-'+Math.random().toString(36).slice(2);
test('20 concurrent workers create one remote record',async()=>{
 const key=id('parallel'),g=gateway();await Promise.all(Array.from({length:20},()=>send(g,key)));
 await Promise.all(Array.from({length:20},()=>g.process(key)));
 const r=await pool.query('SELECT count(*)::int AS n FROM synthetic_remote_records WHERE external_id=$1',[key]);
 assert.equal(r.rows[0].n,1);
});
test('ambiguous commit then retry reconciles one remote record',async()=>{
 const key=id('timeout');let first=true;
 const remote=syntheticRemote(pool,{afterCommit:async()=>{if(first){first=false;throw Error('synthetic timeout after commit')}}});
 const g=gateway(remote);await send(g,key);await g.process(key);
 await pool.query("UPDATE intake_events SET next_attempt_at=now()-interval '1 second' WHERE id=$1",[key]);
 await g.process(key);
 const a=await pool.query('SELECT state FROM intake_events WHERE id=$1',[key]);
 const b=await pool.query('SELECT count(*)::int AS n FROM synthetic_remote_records WHERE external_id=$1',[key]);
 assert.equal(a.rows[0].state,'complete');assert.equal(b.rows[0].n,1);
});
test('stale lease holder cannot complete after newer claim',async()=>{
 const key=id('lease'),store=postgresStore(pool,{leaseSeconds:1});await store.insertIfAbsent({id:key,payload:payload(key),consent:{source:'synthetic'}});
 const old=await store.claim(key,'00000000-0000-4000-8000-000000000001');
 await pool.query("UPDATE intake_events SET lease_until=now()-interval '1 second' WHERE id=$1",[key]);
 const newer=await store.claim(key,'00000000-0000-4000-8000-000000000002');
 assert.ok(newer.leaseVersion>old.leaseVersion);
 assert.equal(await store.complete(key,'00000000-0000-4000-8000-000000000001','stale',old.leaseVersion),false);
 assert.equal(await store.complete(key,'00000000-0000-4000-8000-000000000002','current',newer.leaseVersion),true);
});
test('pending event survives fresh gateway instance',async()=>{
 const key=id('restart');await send(gateway(),key);
 await gateway().process(key);
 const r=await pool.query('SELECT state FROM intake_events WHERE id=$1',[key]);assert.equal(r.rows[0].state,'complete');
});
test.after(async()=>pool.end());
