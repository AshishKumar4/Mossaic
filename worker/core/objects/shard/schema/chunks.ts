import { applyMigrationOnce } from "../../../lib/migrations";

export function applyChunkTables(sql: SqlStorage): void {
  sql.exec(`
      CREATE TABLE IF NOT EXISTS chunks (
        hash          TEXT PRIMARY KEY,
        data          BLOB NOT NULL,
        size          INTEGER NOT NULL,
        ref_count     INTEGER NOT NULL DEFAULT 1,
        created_at    INTEGER NOT NULL
      )
    `);

  sql.exec(`
      CREATE TABLE IF NOT EXISTS chunk_refs (
        chunk_hash    TEXT NOT NULL,
        file_id       TEXT NOT NULL,
        chunk_index   INTEGER NOT NULL,
        user_id       TEXT NOT NULL,
        PRIMARY KEY (chunk_hash, file_id, chunk_index)
      )
    `);

  sql.exec(`
      CREATE TABLE IF NOT EXISTS shard_meta (
        key           TEXT PRIMARY KEY,
        value         INTEGER NOT NULL
      )
    `);
}

export function applyChunkGcIndexes(sql: SqlStorage): void {
  // ── VFS GC bookkeeping (sdk-impl-plan §3.2, §8.3) ──────────────────────
  // deleted_at marks chunks pending hard-delete. Set when ref_count first
  // hits 0; the alarm sweeper hard-deletes after a grace
  // period. NULL = live.
  applyMigrationOnce(sql, "chunks_add_deleted_at", () =>
    sql.exec("ALTER TABLE chunks ADD COLUMN deleted_at INTEGER")
  );
  sql.exec(`
      CREATE INDEX IF NOT EXISTS idx_chunks_deleted
        ON chunks(deleted_at)
        WHERE deleted_at IS NOT NULL
    `);
  sql.exec(`
      CREATE INDEX IF NOT EXISTS idx_chunk_refs_file
        ON chunk_refs(file_id)
    `);
}
