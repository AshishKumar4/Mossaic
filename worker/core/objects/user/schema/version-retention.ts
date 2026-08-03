import { applyMigrationOnce } from "../../../lib/migrations";

/**
 * Durable workspace for a bounded, resumable version-retention operation.
 *
 * Retention walks a path's history newest-first and drops what the policy does
 * not keep. Both halves of that walk have to be seekable, because a deep
 * history cannot be scanned in one invocation:
 *
 *   - `idx_file_versions_retention_seek` covers the ordering the scan uses,
 *     `(mtime_ms DESC, version_id DESC)` within a path, so resuming is a
 *     row-value range seek — `(mtime_ms, version_id) < (cursor…)` — instead of
 *     a rescan of everything newer than the cursor. Without `version_id` in
 *     the index the comparison would fall back to sorting the whole path's
 *     history on every step, which is what makes the naive form quadratic on
 *     the depth it is supposed to page over.
 *   - `version_retention_operations` is the operation itself: the policy it
 *     was started with, the plan fence it is walking under, the seek cursor,
 *     the running counts, and the one version whose manifest is mid-reap. The
 *     `status` / `plan_generation` pair is the control-plane tuple
 *     `lib/paged-operation` fences every transition on.
 *
 * Both are additive: `CREATE ... IF NOT EXISTS` for the new table and index,
 * and no existing table changes shape.
 */
export function applyVersionRetentionSchema(sql: SqlStorage): void {
  applyMigrationOnce(sql, "file_versions_retention_seek_idx", () =>
    sql.exec(
      `CREATE INDEX IF NOT EXISTS idx_file_versions_retention_seek
         ON file_versions(path_id, mtime_ms DESC, version_id DESC)`
    )
  );

  // `pending_version_id` is set only for a version whose metadata row is
  // already gone and whose manifest is still owed, so it is also the predicate
  // that keeps expiry from stranding one: an operation holding a pending
  // version is finished by the maintenance alarm, never pruned.
  sql.exec(`
      CREATE TABLE IF NOT EXISTS version_retention_operations (
        operation_id         TEXT PRIMARY KEY,
        user_id              TEXT NOT NULL,
        path_id              TEXT NOT NULL,
        policy_json          TEXT NOT NULL,
        status               TEXT NOT NULL,
        plan_generation      INTEGER NOT NULL,
        plan_head_version_id TEXT,
        cursor_mtime_ms      INTEGER,
        cursor_version_id    TEXT,
        remaining_keep       INTEGER NOT NULL,
        dropped              INTEGER NOT NULL DEFAULT 0,
        kept                 INTEGER NOT NULL DEFAULT 0,
        pending_version_id   TEXT,
        pending_mtime_ms     INTEGER,
        pending_ref_id       TEXT,
        created_at           INTEGER NOT NULL,
        updated_at           INTEGER NOT NULL
      )
    `);
  // Retention order for the prune, and the resume order for the alarm: both
  // read oldest `updated_at` first within a status.
  sql.exec(`
      CREATE INDEX IF NOT EXISTS idx_version_retention_operations_status
        ON version_retention_operations(status, updated_at)
    `);
}
