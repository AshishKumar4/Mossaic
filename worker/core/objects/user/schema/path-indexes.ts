export function applyPathIndexes(sql: SqlStorage): void {
  // POSIX uniqueness via partial indexes (SQLite cannot ALTER TABLE ADD
  // UNIQUE on existing tables). Scoped to non-deleted rows so prior
  // soft-deleted duplicates don't block migration.
  //
  // If existing data has live duplicates, this CREATE throws and is
  // swallowed; the admin dedupe route resolves them later.
  try {
    sql.exec(`
        CREATE UNIQUE INDEX IF NOT EXISTS uniq_files_parent_name
          ON files(user_id, IFNULL(parent_id, ''), file_name)
          WHERE status != 'deleted'
      `);
  } catch {
    // dupe live rows exist; admin dedupe is required before re-running
  }
  try {
    sql.exec(`
        CREATE UNIQUE INDEX IF NOT EXISTS uniq_folders_parent_name
          ON folders(user_id, IFNULL(parent_id, ''), name)
      `);
  } catch {
    // dupe folder rows exist; admin dedupe is required before re-running
  }

  // Lookup indexes (overdue per study §4)
  sql.exec(`
      CREATE INDEX IF NOT EXISTS idx_files_parent
        ON files(user_id, parent_id, status)
    `);
  sql.exec(`
      CREATE INDEX IF NOT EXISTS idx_folders_parent
        ON folders(user_id, parent_id)
    `);
}

export function verifyPathUniquenessIndexes(sql: SqlStorage): void {
  // ── Audit H6: surface UNIQUE INDEX failure on legacy data ────────────
  //
  // The previous code swallowed the throw silently when the file
  // table contained live (parent_id, file_name) duplicates. The DO
  // then ran WITHOUT the index and the central commit-rename
  // atomicity guarantee silently degraded.
  //
  // New behaviour: detect via PRAGMA whether the index is present
  // after the CREATE attempt; if absent, log via console.error AND
  // persist a `migration_state` row in vfs_meta so subsequent VFS
  // writes can refuse with EBUSY (see gateVfs). The recovery path
  // is the existing admin dedupe route, which the operator can
  // trigger manually; once dedupe completes, the next ensureInit
  // re-creates the index and clears the marker.
  checkAndRecordIndex(
    sql,
    "uniq_files_parent_name",
    "files_unique_index",
    "files"
  );
  checkAndRecordIndex(
    sql,
    "uniq_folders_parent_name",
    "folders_unique_index",
    "folders"
  );
}

/**
 * Audit H6 helper: verify the named UNIQUE INDEX exists, recording
 * a degraded marker in vfs_meta if not. Logs to console.error so
 * operators see the problem in wrangler tail / Logpush.
 *
 * sqlite_master rows for indexes have type='index'; missing index
 * means the CREATE was swallowed because of duplicate-row data.
 */
function checkAndRecordIndex(
  sql: SqlStorage,
  indexName: string,
  markerKey: string,
  table: string
): void {
  const present = sql
    .exec(
      "SELECT 1 FROM sqlite_master WHERE type='index' AND name = ? LIMIT 1",
      indexName
    )
    .toArray();
  if (present.length > 0) {
    // Index is healthy. Clear any stale marker (e.g. an admin run
    // dedupe and re-init).
    sql.exec(
      "DELETE FROM vfs_meta WHERE key = ?",
      markerKey
    );
    return;
  }
  // Degraded path: index missing because legacy data has live
  // duplicates. Record + log.
  const value = JSON.stringify({
    table,
    indexName,
    detectedAt: Date.now(),
    reason: "duplicate-rows-block-create-unique",
  });
  sql.exec(
    "INSERT OR REPLACE INTO vfs_meta (key, value) VALUES (?, ?)",
    markerKey,
    value
  );
  // eslint-disable-next-line no-console
  console.error(
    `[mossaic:H6] UNIQUE INDEX ${indexName} missing on ${table} — duplicate live rows block CREATE. ` +
      `VFS writes will refuse with EBUSY until \`POST /admin/dedupe-paths\` resolves the duplicates.`
  );
}
