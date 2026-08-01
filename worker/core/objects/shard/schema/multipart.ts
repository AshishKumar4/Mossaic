import { applyMigrationOnce } from "../../../lib/migrations";

export function applyMultipartStaging(sql: SqlStorage): void {
  // ── multipart staging table ───────────────────────────────
  //
  // Records `(upload_id, chunk_index)` → `chunk_hash` for each chunk
  // landed during a multipart upload. The chunk bytes themselves
  // live in `chunks` and are referenced through `chunk_refs` exactly
  // as for a non-multipart write — the staging table is metadata
  // only, used by UserDO's finalize to verify that every chunk in
  // the client's hash list actually landed and matches.
  //
  // Written in the same DO turn as `chunk_refs` by `putChunkMultipart`,
  // so each per-chunk PUT costs zero extra subrequests.
  //
  // PRIMARY KEY (upload_id, chunk_index) makes re-PUT idempotent —
  // a retry under the same hash is `INSERT OR REPLACE` no-op; a
  // retry with different bytes overwrites and `putChunkMultipart`
  // takes the supersession branch (drops old ref, registers new
  // chunk, replaces this row).
  sql.exec(`
      CREATE TABLE IF NOT EXISTS upload_chunks (
        upload_id    TEXT NOT NULL,
        chunk_index  INTEGER NOT NULL,
        chunk_hash   TEXT NOT NULL,
        chunk_size   INTEGER NOT NULL,
        user_id      TEXT NOT NULL,
        created_at   INTEGER NOT NULL,
        PRIMARY KEY (upload_id, chunk_index)
      )
    `);
  sql.exec(`
      CREATE INDEX IF NOT EXISTS idx_upload_chunks_user
        ON upload_chunks(user_id, upload_id)
    `);
  sql.exec(`
      CREATE TABLE IF NOT EXISTS multipart_fences (
        upload_id  TEXT PRIMARY KEY,
        fence_id   TEXT NOT NULL,
        state      TEXT NOT NULL,
        updated_at INTEGER NOT NULL
      )
    `);

  // ── fence lifecycle ───────────────────────────────────────
  //
  // `expires_at` is the last instant at which a session token for this
  // upload can still be presented — the maximum `exp` this shard has
  // observed for the upload. It is what makes a fence row reclaimable:
  // until it passes (plus a grace margin) the row is the only thing
  // that rejects a straggler PUT against a finalizing, aborting, or
  // expired upload, so deleting it would let the straggler re-open the
  // fence and commit refs into a torn-down upload.
  //
  // Rows written before this column existed carry NULL and are never
  // reclaimed on that basis; `ShardDO.backfillMultipartFenceExpiry`
  // derives a safe deadline for them first.
  applyMigrationOnce(sql, "multipart_fences_add_expires_at", () =>
    sql.exec("ALTER TABLE multipart_fences ADD COLUMN expires_at INTEGER")
  );
  // Serves all three fence-lifecycle reads: the due-ordered reclaim
  // scan, the earliest-deadline probe, and the backfill's hunt for
  // NULL deadlines (SQLite sorts NULLs first, so that one is an index
  // seek rather than a table scan).
  sql.exec(`
      CREATE INDEX IF NOT EXISTS idx_multipart_fences_expires
        ON multipart_fences(expires_at)
    `);
}
