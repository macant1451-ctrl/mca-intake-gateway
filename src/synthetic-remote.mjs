// TEST ONLY: atomic unique external IDs, not a GHL/Trulli adapter.
export function syntheticRemote(pool,{afterCommit}={}) {
 return {
  idempotencyGuaranteed:true,
  async findByExternalId(id) {
   const r=await pool.query('SELECT id FROM synthetic_remote_records WHERE external_id=$1',[id]);
   return r.rows[0]??null;
  },
  async create(payload) {
   const r=await pool.query(`INSERT INTO synthetic_remote_records(external_id,payload)
    VALUES($1,$2::jsonb) ON CONFLICT(external_id) DO UPDATE
    SET external_id=EXCLUDED.external_id RETURNING id`,
    [payload.external_id,JSON.stringify(payload)]);
   if(afterCommit)await afterCommit();
   return r.rows[0];
  }
 };
}
