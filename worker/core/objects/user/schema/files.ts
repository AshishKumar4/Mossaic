import { applyMigrationOnce } from "../../../lib/migrations";

export function applyFileTables(sql: SqlStorage): void {
  sql.exec(`
      CREATE TABLE IF NOT EXISTS files (
        file_id       TEXT PRIMARY KEY,
        user_id       TEXT NOT NULL,
        parent_id     TEXT,
        file_name     TEXT NOT NULL,
        file_size     INTEGER NOT NULL,
        file_hash     TEXT NOT NULL,
        mime_type     TEXT NOT NULL,
        chunk_size    INTEGER NOT NULL,
        chunk_count   INTEGER NOT NULL,
        pool_size     INTEGER NOT NULL,
        status        TEXT NOT NULL DEFAULT 'uploading',
        created_at    INTEGER NOT NULL,
        updated_at    INTEGER NOT NULL,
        deleted_at    INTEGER
      )
    `);

  sql.exec(`
      CREATE TABLE IF NOT EXISTS file_chunks (
        file_id       TEXT NOT NULL,
        chunk_index   INTEGER NOT NULL,
        chunk_hash    TEXT NOT NULL,
        chunk_size    INTEGER NOT NULL,
        shard_index   INTEGER NOT NULL,
        PRIMARY KEY (file_id, chunk_index)
      )
    `);
}

export function applyFilePosixColumns(sql: SqlStorage): void {
  // Backward compatibility: existing rows get default mode, NULL inline
  // data, node_kind='file'. The legacy app's reads keep working because
  // (a) new columns have defaults, (b) the manifest reader (files.ts)
  // continues to fall through to file_chunks when inline_data IS NULL.

  // file mode (POSIX), inline tier, symlink kind
  applyMigrationOnce(sql, "files_add_mode", () =>
    // 0o644
    sql.exec("ALTER TABLE files ADD COLUMN mode INTEGER NOT NULL DEFAULT 420")
  );
  applyMigrationOnce(sql, "files_add_inline_data", () =>
    sql.exec("ALTER TABLE files ADD COLUMN inline_data BLOB")
  );
  applyMigrationOnce(sql, "files_add_symlink_target", () =>
    sql.exec("ALTER TABLE files ADD COLUMN symlink_target TEXT")
  );
  applyMigrationOnce(sql, "files_add_node_kind", () =>
    sql.exec(
      "ALTER TABLE files ADD COLUMN node_kind TEXT NOT NULL DEFAULT 'file'"
    )
  );
}

export function applyFileSearchIndexColumns(sql: SqlStorage): void {
  // indexed_at marks files that have been search-indexed
  // (text+CLIP via `indexFile` in worker/app/routes/search.ts).
  // NULL = not yet indexed. The reconciler alarm
  // (`runIndexReconcile`) sweeps NULL rows on a periodic cadence
  // and re-queues them. Without it, if the SPA crashed between
  // `multipart/finalize` and `POST /api/index/file` the file
  // would land in canonical VFS but never indexed → silent search
  // miss for the lifetime of the file.
  applyMigrationOnce(sql, "files_add_indexed_at", () =>
    sql.exec("ALTER TABLE files ADD COLUMN indexed_at INTEGER")
  );
  // P1-8 — index_attempts column for reconciler retry cap.
  //
  // Pre-fix `reconcileUnindexedFiles` re-fired `indexFile` on
  // every alarm tick for any row with `indexed_at IS NULL`. A
  // permanently-failing file (corrupted source, unsupported
  // MIME, or any condition the indexer can't handle) would
  // retry forever, burning CPU + AI binding budget every alarm
  // and blocking a slot in the bounded `limit=25` reconciler.
  //
  // The new column tracks failed attempts; `appListUnindexedFiles`
  // filters `index_attempts < 5`. After the cap a single
  // `console.error` fires and the row is left dormant — operator
  // sees it via Logpush and can manually reconcile.
  applyMigrationOnce(sql, "files_add_index_attempts", () =>
    sql.exec(
      "ALTER TABLE files ADD COLUMN index_attempts INTEGER NOT NULL DEFAULT 0"
    )
  );
  // Sparse index — most files are indexed within seconds of finalize,
  // so the reconciler scan should be fast: the partial index makes
  // `WHERE indexed_at IS NULL` an index-only scan.
  sql.exec(`
      CREATE INDEX IF NOT EXISTS idx_files_indexed_at_null
        ON files(file_id)
        WHERE indexed_at IS NULL AND status = 'complete'
    `);
}

export function applyFileArchivedColumn(sql: SqlStorage): void {
  // ── Archive bit (three-tier delete API) ───────────────────────
  //
  // `archived = 1` hides a path from the default `listFiles` /
  // `fileInfo` results without destroying or tombstoning data.
  // Reads (`stat`, `readFile`, `readPreview`, `createReadStream`,
  // `openManifest`, `readChunk`, `listVersions`, `restoreVersion`)
  // are UNCHANGED — an archived file is fully readable by anyone
  // who knows its path. Only the listing-side filters apply.
  //
  // Three-tier delete model:
  //   - `archive(path)` — cosmetic; reversible via `unarchive`;
  //                       does NOT touch versions or chunks.
  //   - `unlink(path)`  — POSIX-style; versioning-on writes a
  //                       tombstone version (path becomes ENOENT
  //                       to reads); versioning-off hard-deletes.
  //   - `purge(path)`   — destructive; drops every version row +
  //                       decrements ShardDO chunk refs.
  //
  // Idempotent ALTER: try/catch swallows "duplicate column name"
  // when the migration runs on an already-migrated DO. NOT NULL
  // DEFAULT 0 means existing rows surface as not-archived without
  // a backfill.
  applyMigrationOnce(sql, "files_add_archived", () =>
    sql.exec(
      "ALTER TABLE files ADD COLUMN archived INTEGER NOT NULL DEFAULT 0"
    )
  );
}
