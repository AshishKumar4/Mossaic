export function applyWriteStreamSessions(sql: SqlStorage): void {
  // Server-owned state for handle-based write streams. Public handles are
  // round-tripped by callers and therefore cannot be trusted to carry the
  // destination or commit policy. The tmp id is the only capability-like
  // lookup key; every other field is reloaded from this row at append and
  // commit time.
  sql.exec(`
      CREATE TABLE IF NOT EXISTS write_stream_sessions (
        tmp_id                TEXT PRIMARY KEY,
        user_id               TEXT NOT NULL,
        parent_id             TEXT,
        leaf                  TEXT NOT NULL,
        chunk_size            INTEGER NOT NULL,
        pool_size             INTEGER NOT NULL,
        metadata_present      INTEGER NOT NULL DEFAULT 0,
        metadata_blob         BLOB,
        tags_json             TEXT,
        version_label         TEXT,
        version_user_visible  INTEGER,
        encryption_mode       TEXT,
        encryption_key_id     TEXT,
        status                TEXT NOT NULL DEFAULT 'open',
        inflight_index        INTEGER,
        inflight_hash         TEXT,
        inflight_at           INTEGER,
        expires_at            INTEGER NOT NULL,
        created_at            INTEGER NOT NULL
      )
    `);
  sql.exec(
    `INSERT OR IGNORE INTO meta_schema (name, applied_at)
       VALUES ('write_stream_sessions_enabled', ?)`,
    Date.now()
  );
}
