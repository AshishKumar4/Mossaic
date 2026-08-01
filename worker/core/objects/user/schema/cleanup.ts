export function applyChunkCleanupOutbox(sql: SqlStorage): void {
  // Durable outbox for primary file chunk-ref cleanup. CREATE TABLE/INDEX
  // IF NOT EXISTS makes this additive and safe to replay on existing DOs.
  // The ref-oriented schema can support additional cleanup producers later
  // without coupling this tranche to version, Yjs, or variant deletion.
  sql.exec(`
      CREATE TABLE IF NOT EXISTS chunk_cleanup_intents (
        ref_id       TEXT NOT NULL,
        shard_index  INTEGER NOT NULL,
        cleanup_kind TEXT NOT NULL DEFAULT 'chunks',
        state        TEXT NOT NULL DEFAULT 'pending',
        generation   INTEGER NOT NULL DEFAULT 0,
        provisional  INTEGER NOT NULL DEFAULT 0,
        created_at   INTEGER NOT NULL,
        updated_at   INTEGER NOT NULL,
        next_attempt_at INTEGER NOT NULL DEFAULT 0,
        attempts     INTEGER NOT NULL DEFAULT 0,
        last_error   TEXT,
        PRIMARY KEY (ref_id, shard_index)
      )
    `);
  sql.exec(`
      CREATE INDEX IF NOT EXISTS idx_chunk_cleanup_intents_eligible
        ON chunk_cleanup_intents(next_attempt_at, created_at, ref_id, shard_index)
    `);
}
