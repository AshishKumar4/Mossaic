import { MULTIPART_LEGACY_PLACEMENT_VERSION } from "../../../../../shared/multipart";
import { applyMigrationOnce } from "../../../lib/migrations";

export function applyUploadSessionsTable(sql: SqlStorage): void {
  // ── multipart upload sessions ──────────────────────────────
  //
  // Per-tenant table tracking open / finalized / aborted multipart
  // upload sessions. The `upload_id` is the same value as the tmp
  // `files.file_id` minted at begin — that way a session row and
  // its tmp file share identity, and `commitRename` at finalize
  // doesn't need to bridge two id namespaces. The actual chunk
  // staging lives on each touched ShardDO (in `upload_chunks`); this
  // table holds only the manifest-level metadata and validated
  // commit-time payload (metadata/tags/version/encryption).
  //
  // CREATE TABLE IF NOT EXISTS is naturally idempotent; no migration
  // tag needed — additive table.
  sql.exec(`
      CREATE TABLE IF NOT EXISTS upload_sessions (
        upload_id            TEXT PRIMARY KEY,
        user_id              TEXT NOT NULL,
        parent_id            TEXT,
        leaf                 TEXT NOT NULL,
        total_size           INTEGER NOT NULL,
        total_chunks         INTEGER NOT NULL,
        chunk_size           INTEGER NOT NULL,
        pool_size            INTEGER NOT NULL,
        expires_at           INTEGER NOT NULL,
        status               TEXT NOT NULL,
        encryption_mode      TEXT,
        encryption_key_id    TEXT,
        metadata_blob        BLOB,
        tags_json            TEXT,
        version_label        TEXT,
        version_user_visible INTEGER,
        mode                 INTEGER NOT NULL,
        mime_type            TEXT NOT NULL,
        created_at           INTEGER NOT NULL
      )
    `);
}

export function applyUploadSessionIndexes(sql: SqlStorage): void {
  sql.exec(`
      CREATE INDEX IF NOT EXISTS idx_upload_sessions_open_expires
        ON upload_sessions(expires_at)
        WHERE status = 'open'
    `);
  sql.exec(`
      CREATE INDEX IF NOT EXISTS idx_upload_sessions_active_expires
        ON upload_sessions(expires_at)
        WHERE status IN ('open', 'finalizing', 'aborting')
    `);
  sql.exec(`
      CREATE INDEX IF NOT EXISTS idx_upload_sessions_user_status
        ON upload_sessions(user_id, status)
    `);
  // Local abort failures leave the session open and increment `attempts`.
  // Remote cleanup failures are tracked independently by the durable
  // cleanup outbox after the terminal local transaction commits.
  applyMigrationOnce(sql, "upload_sessions_add_attempts", () =>
    sql.exec(
      "ALTER TABLE upload_sessions ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0"
    )
  );
  applyMigrationOnce(sql, "upload_sessions_add_fence_id", () =>
    sql.exec("ALTER TABLE upload_sessions ADD COLUMN fence_id TEXT")
  );
  // Sessions that predate placement versioning already staged chunks
  // where rendezvous hashing put them, so they default to v1 and stay
  // there for the rest of their life.
  applyMigrationOnce(sql, "upload_sessions_add_placement_version", () =>
    sql.exec(
      `ALTER TABLE upload_sessions ADD COLUMN placement_version INTEGER NOT NULL
         DEFAULT ${MULTIPART_LEGACY_PLACEMENT_VERSION}`
    )
  );
}
