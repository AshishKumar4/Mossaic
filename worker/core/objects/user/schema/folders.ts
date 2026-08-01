import { applyMigrationOnce } from "../../../lib/migrations";

export function applyFoldersTable(sql: SqlStorage): void {
  sql.exec(`
      CREATE TABLE IF NOT EXISTS folders (
        folder_id     TEXT PRIMARY KEY,
        user_id       TEXT NOT NULL,
        parent_id     TEXT,
        name          TEXT NOT NULL,
        created_at    INTEGER NOT NULL,
        updated_at    INTEGER NOT NULL
      )
    `);
}

export function applyFolderModeColumn(sql: SqlStorage): void {
  applyMigrationOnce(sql, "folders_add_mode", () =>
    // 0o755
    sql.exec(
      "ALTER TABLE folders ADD COLUMN mode INTEGER NOT NULL DEFAULT 493"
    )
  );
}

export function applyFolderRevision(sql: SqlStorage): void {
  // folders.revision: monotonically-increasing per-folder counter
  // bumped by every mutation that changes that folder's
  // direct children (or the folder's own name/parent slot for
  // rename of the folder itself). Returned by `vfsListChildren`
  // so consumers (Seal etc.) can use it as an ETag — when revision
  // is unchanged across two reads, the directory contents are
  // guaranteed identical.
  //
  // Default 0 on existing rows; `bumpFolderRevision` does
  // `UPDATE folders SET revision = revision + 1 WHERE folder_id = ?`,
  // so the first bump moves any pre-migration row from 0 → 1.
  // Idempotent ALTER (try/catch on duplicate-column).
  applyMigrationOnce(sql, "folders_add_revision", () =>
    sql.exec(
      "ALTER TABLE folders ADD COLUMN revision INTEGER NOT NULL DEFAULT 0"
    )
  );
  // Root-folder revision counter.
  //
  // The root has no `folders` row (it's implicit: parent_id=NULL is
  // the root). To track its mutation revision we use a dedicated
  // single-row table keyed by `user_id`; the row is materialised
  // lazily by `bumpFolderRevision` on first root-level mutation.
  //
  // Rejected alternative: a synthetic `__root__` folder row inside
  // `folders` itself. That leaked into `vfsReaddir`'s `SELECT FROM
  // folders WHERE parent_id IS NULL`, surfacing as an empty-string
  // entry on every directory listing. This dedicated table avoids
  // the leak entirely without exclusion-clauses scattered across
  // every read site.
  sql.exec(`
      CREATE TABLE IF NOT EXISTS root_folder_revision (
        user_id  TEXT PRIMARY KEY,
        revision INTEGER NOT NULL DEFAULT 0
      )
    `);
}
