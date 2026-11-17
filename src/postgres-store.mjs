// PostgreSQL store contract for gateway.mjs. Requires pg Pool injected by caller.
// All mutations are fenced with lease_token + lease_version; stale workers cannot commit.
export function postgresStore(pool,{leaseSeconds=60}={}) {
 return {
  async insertIfAbsent(event) {
   const q=await pool.query(`INSERT INTO intake_events(id,payload,consent,state)
     VALUES($1,$2::jsonb,$3::jsonb,'pending') ON CONFLICT(id) DO NOTHING RETURNING id`,
     [event.id,JSON.stringify(event.payload),JSON.stringify(event.consent)]);
   return q.rowCount===1;
  },
  async claim(id,token) {
   const q=await pool.query(`UPDATE intake_events SET state='processing', lease_token=$2::uuid,
    lease_version=lease_version+1,lease_until=now()+($3::int*interval '1 second'),
    updated_at=now() WHERE id=$1 AND
    ((state IN ('pending','retry') AND next_attempt_at<=now()) OR
     (state='processing' AND lease_until<now()))
    RETURNING id,payload,attempts,lease_version`,[id,token,leaseSeconds]);
   if(!q.rowCount)return null;
   const e=q.rows[0];
   return {id:e.id,payload:e.payload,attempts:e.attempts,leaseVersion:Number(e.lease_version)};
  },
  async complete(id,token,remoteId,version) {
   const q=await pool.query(`UPDATE intake_events SET state='complete',remote_id=$4,
     lease_token=NULL,lease_until=NULL,updated_at=now()
     WHERE id=$1 AND lease_token=$2::uuid AND lease_version=$3 AND state='processing'
     RETURNING id`,[id,token,version,remoteId]);
   return q.rowCount===1;
  },
  async fail(id,token,changes,version) {
   const q=await pool.query(`UPDATE intake_events SET state=$4,attempts=$5,
     next_attempt_at=to_timestamp($6/1000.0),error_code=$7,
     lease_token=NULL,lease_until=NULL,updated_at=now()
     WHERE id=$1 AND lease_token=$2::uuid AND lease_version=$3 AND state='processing'
     RETURNING id`,[id,token,version,changes.state,changes.attempts,changes.nextAttemptAt,changes.errorCode]);
   return q.rowCount===1;
  }
 };
}
