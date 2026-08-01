import { applyMigrationOnce } from "../../../lib/migrations";

export function applyVersionSchema(sql: SqlStorage): void {
  // ── file_versions table ─────────────────────────────────────
  // S3-style versioning. Each row is one historical snapshot of a
  // (path_id, version_id) pair. `path_id` is the stable `files.file_id`
  // (Design A: sticky path identity — the first writeFile creates a
  // `files` row + a v1 row; subsequent writes only add version rows
  // and update files.head_version_id). `version_id` is a fresh ULID
  // per write.
  //
  // Inline-tier (≤16KB): inline_data column on this table mirrors
  // files.inline_data semantics. No ShardDO call required.
  //
  // Chunked tier: chunk metadata lives in `version_chunks` (mirrors
  // file_chunks but keyed by version_id). ShardDO chunk_refs use a
  // synthetic file_id of `${path_id}#${version_id}` so per-version
  // refcount is independent — the alarm sweeper
  // reclaims chunks when the last version referencing them is
  // dropped. No new GC plumbing.
  //
  // Tombstones: deleted=1 + chunks=0; readFile(head) skips them and
  // returns ENOENT if no live version remains. unlink() inserts a
  // tombstone version (preserving history); chunks NOT decremented.
  sql.exec(`
      CREATE TABLE IF NOT EXISTS file_versions (
        path_id      TEXT NOT NULL,
        version_id   TEXT NOT NULL,
        user_id      TEXT NOT NULL,
        size         INTEGER NOT NULL,
        mode         INTEGER NOT NULL DEFAULT 420,
        mtime_ms     INTEGER NOT NULL,
        deleted      INTEGER NOT NULL DEFAULT 0,
        inline_data  BLOB,
        chunk_size   INTEGER NOT NULL DEFAULT 0,
        chunk_count  INTEGER NOT NULL DEFAULT 0,
        file_hash    TEXT NOT NULL DEFAULT '',
        mime_type    TEXT NOT NULL DEFAULT 'application/octet-stream',
        PRIMARY KEY (path_id, version_id)
      )
    `);
  // Newest-first index for listVersions over arbitrarily-large
  // history. SQLite uses this as a covering index for ORDER BY
  // mtime_ms DESC LIMIT N — sub-millisecond at 10k versions.
  sql.exec(`
      CREATE INDEX IF NOT EXISTS idx_file_versions_path_mtime
        ON file_versions(path_id, mtime_ms DESC)
    `);
  sql.exec(`
      CREATE INDEX IF NOT EXISTS idx_file_versions_user
        ON file_versions(user_id, path_id)
    `);

  // version_chunks: per-version chunk manifest. Mirrors file_chunks
  // but keyed by version_id.
  sql.exec(`
      CREATE TABLE IF NOT EXISTS version_chunks (
        version_id   TEXT NOT NULL,
        chunk_index  INTEGER NOT NULL,
        chunk_hash   TEXT NOT NULL,
        chunk_size   INTEGER NOT NULL,
        shard_index  INTEGER NOT NULL,
        PRIMARY KEY (version_id, chunk_index)
      )
    `);
  // Audit H4: secondary index on chunk_hash so placeChunkForVersion's
  // "have we placed this hash before?" probe is O(log N), not a full
  // scan. Without this, every chunked write under versioning-on
  // costs O(total_version_chunks_in_tenant) per chunk.
  sql.exec(`
      CREATE INDEX IF NOT EXISTS idx_version_chunks_hash
        ON version_chunks(chunk_hash)
    `);

  // Head-pointer column on `files`: when versioning is enabled, the
  // `files` row is just a stable identity for the path; the actual
  // head version lives in file_versions. Legacy / versioning-OFF
  // tenants leave this NULL and continue using files' own columns.
  applyMigrationOnce(sql, "files_add_head_version_id", () =>
    sql.exec("ALTER TABLE files ADD COLUMN head_version_id TEXT")
  );
}

export function applyVersionShardRefColumn(sql: SqlStorage): void {
  // Multipart × versioning consistency.
  //
  // Records the actual ShardDO chunk_refs file_id used at write
  // time for this version's chunks. The canonical versioned
  // write path (vfsWriteFileVersioned, restoreVersion, copy-file)
  // uses the synthetic `${pathId}#${versionId}` form
  // (`shardRefId(pathId, versionId)` in vfs-versions.ts:179) and
  // so the column is NULL for those rows — `dropVersionRows`
  // falls back to the synthetic form.
  //
  // Multipart-finalize-under-versioning writes chunks to ShardDOs
  // at upload time keyed by `refId = uploadId`. The
  // `file_versions` row created at finalize stamps
  // `shard_ref_id = uploadId` so a future `dropVersionRows` calls
  // ShardDO `deleteChunks(uploadId)` and finds the right
  // `chunk_refs` rows. Without this column, the canonical fan-out
  // would key off `${pathId}#${versionId}` and decrement nothing
  // — leaking chunk bytes forever.
  applyMigrationOnce(sql, "file_versions_add_shard_ref_id", () =>
    sql.exec(
      "ALTER TABLE file_versions ADD COLUMN shard_ref_id TEXT"
    )
  );
}
