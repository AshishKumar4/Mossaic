import {
  OPERATION_CLAIM_COLUMNS_DDL,
  OPERATION_RETRY_COLUMNS_DDL,
} from "../../../lib/paged-operation";

export function applyChunkCleanupOutbox(sql: SqlStorage): void {
  // Durable outbox for primary file chunk-ref cleanup. CREATE TABLE/INDEX
  // IF NOT EXISTS makes this additive and safe to replay on existing DOs.
  // The ref-oriented schema can support additional cleanup producers later
  // without coupling this tranche to version, Yjs, or variant deletion.
  //
  // `state`/`provisional` are the domain's; the claim and retry columns are
  // the shared control-plane set from `lib/paged-operation`.
  sql.exec(`
      CREATE TABLE IF NOT EXISTS chunk_cleanup_intents (
        ref_id       TEXT NOT NULL,
        shard_index  INTEGER NOT NULL,
        cleanup_kind TEXT NOT NULL DEFAULT 'chunks',
        state        TEXT NOT NULL DEFAULT 'pending',
        ${OPERATION_CLAIM_COLUMNS_DDL},
        provisional  INTEGER NOT NULL DEFAULT 0,
        created_at   INTEGER NOT NULL,
        ${OPERATION_RETRY_COLUMNS_DDL},
        PRIMARY KEY (ref_id, shard_index)
      )
    `);
  sql.exec(`
      CREATE INDEX IF NOT EXISTS idx_chunk_cleanup_intents_eligible
        ON chunk_cleanup_intents(next_attempt_at, created_at, ref_id, shard_index)
    `);
}
