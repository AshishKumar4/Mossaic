// ── metadata + tags + version label/visibility ─────────────
//
// Schema-only additions delivered via the existing idempotent
// ensureInit path. NO new wrangler migration tag — additive
// ALTERs + CREATE TABLE/INDEX IF NOT EXISTS are safe to replay.
//
// - files.metadata: opaque JSON blob, ≤64 KB. NULL on legacy rows.
// - file_versions.user_visible: 0=compaction/internal,
//   1=writeFile/flush()/restore. Default 0; legacy versions
//   appear non-user-visible to listVersions(userVisibleOnly:true).
// - file_versions.label: optional human-readable ≤128-char label.
// - file_versions.metadata: snapshot of files.metadata at commit.
// - file_tags(path_id, tag): per-file tag set; (tag, mtime_ms DESC)
//   index drives listFiles-by-tag.
// - idx_files_parent_mtime / idx_files_parent_size: drive
//   listFiles-by-prefix in O(log N + K) seek+scan.
//
// Caps live in shared/metadata-caps.ts and are enforced in
// vfs-ops (validators throw VFSError("EINVAL", ...) before any
// SQL touches the row).

import { applyMigrationOnce } from "../../../lib/migrations";

export function applyMetadataColumns(sql: SqlStorage): void {
  applyMigrationOnce(sql, "files_add_metadata", () =>
    sql.exec("ALTER TABLE files ADD COLUMN metadata BLOB")
  );
  applyMigrationOnce(sql, "file_versions_add_user_visible", () =>
    sql.exec(
      "ALTER TABLE file_versions ADD COLUMN user_visible INTEGER NOT NULL DEFAULT 0"
    )
  );
  applyMigrationOnce(sql, "file_versions_add_label", () =>
    sql.exec("ALTER TABLE file_versions ADD COLUMN label TEXT")
  );
  applyMigrationOnce(sql, "file_versions_add_metadata", () =>
    sql.exec("ALTER TABLE file_versions ADD COLUMN metadata BLOB")
  );
}

export function applyTagsAndListingIndexes(sql: SqlStorage): void {
  sql.exec(`
      CREATE TABLE IF NOT EXISTS file_tags (
        path_id   TEXT NOT NULL,
        tag       TEXT NOT NULL,
        user_id   TEXT NOT NULL,
        mtime_ms  INTEGER NOT NULL,
        PRIMARY KEY (path_id, tag)
      )
    `);
  sql.exec(`
      CREATE INDEX IF NOT EXISTS idx_file_tags_tag_mtime
        ON file_tags(tag, mtime_ms DESC, path_id)
    `);

  sql.exec(`
      CREATE INDEX IF NOT EXISTS idx_files_parent_mtime
        ON files(IFNULL(parent_id, ''), updated_at DESC, file_name)
        WHERE status = 'complete'
    `);
  sql.exec(`
      CREATE INDEX IF NOT EXISTS idx_files_parent_size
        ON files(IFNULL(parent_id, ''), file_size DESC, file_name)
        WHERE status = 'complete'
    `);
}
