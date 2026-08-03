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

/**
 * Durable workspace for a resumable multipart finalize.
 *
 * A finalize that has to survive Durable Object eviction cannot keep its
 * manifest, its running content hash, or the decisions it made about the
 * destination in memory. Three tables and one column group carry them:
 *
 *   - `upload_expected_chunks` is what the client declared, staged in pages
 *     before finalize starts. `upload_sessions.staged_hash_cursor` is the
 *     contiguous high-water mark, so a page checks its position in O(1)
 *     instead of counting what came before it.
 *   - `upload_verified_chunks` is what the shards actually hold, appended a
 *     page at a time as verification advances. The same page copies it into
 *     the destination manifest with set-based SQL, which is what leaves
 *     publication constant-size no matter how many chunks the upload has.
 *   - `upload_cleanup_routes` is inert routing: the shards a finished upload
 *     will have to clean, recorded while the rows that name them are still
 *     there. Publication turns them into executable outbox intents, and an
 *     abort discards them.
 *
 * The `finalize_*` columns are the control-plane tuple `lib/paged-operation`
 * fences on, plus the frozen context and terminal result. `finalize_context`
 * is the compare-and-set token for publication: a destination, versioning
 * flag, metadata, or tag change after the freeze makes publication fail
 * rather than silently apply a decision the operation never made.
 */
export function applyMultipartFinalizeSchema(sql: SqlStorage): void {
  sql.exec(`
      CREATE TABLE IF NOT EXISTS upload_expected_chunks (
        upload_id    TEXT NOT NULL,
        chunk_index  INTEGER NOT NULL,
        chunk_hash   TEXT NOT NULL,
        PRIMARY KEY (upload_id, chunk_index)
      )
    `);
  sql.exec(`
      CREATE TABLE IF NOT EXISTS upload_verified_chunks (
        upload_id    TEXT NOT NULL,
        chunk_index  INTEGER NOT NULL,
        chunk_hash   TEXT NOT NULL,
        chunk_size   INTEGER NOT NULL,
        shard_index  INTEGER NOT NULL,
        PRIMARY KEY (upload_id, chunk_index)
      )
    `);
  sql.exec(`
      CREATE TABLE IF NOT EXISTS upload_cleanup_routes (
        upload_id    TEXT NOT NULL,
        cleanup_kind TEXT NOT NULL,
        shard_index  INTEGER NOT NULL,
        PRIMARY KEY (upload_id, cleanup_kind, shard_index)
      )
    `);

  const addColumn = (name: string, definition: string): void =>
    applyMigrationOnce(sql, `upload_sessions_add_${name}`, () =>
      sql.exec(`ALTER TABLE upload_sessions ADD COLUMN ${name} ${definition}`)
    );

  addColumn("staged_hash_cursor", "INTEGER NOT NULL DEFAULT 0");
  addColumn("finalize_phase", "TEXT");
  addColumn("finalize_fence_cursor", "INTEGER NOT NULL DEFAULT 0");
  addColumn("finalize_chunk_cursor", "INTEGER NOT NULL DEFAULT 0");
  addColumn("finalize_verify_shard_cursor", "INTEGER NOT NULL DEFAULT 0");
  // Cursors that seek by a key which legitimately starts at zero begin one
  // step before it, so the first page selects `> -1` and takes row zero.
  addColumn("finalize_old_manifest_cursor", "INTEGER NOT NULL DEFAULT -1");
  addColumn("finalize_old_cleanup_cursor", "INTEGER NOT NULL DEFAULT -1");
  addColumn("finalize_cleanup_cursor", "INTEGER NOT NULL DEFAULT 0");
  addColumn("finalize_total_size", "INTEGER NOT NULL DEFAULT 0");
  addColumn("finalize_sha_state", "TEXT");
  addColumn("finalize_context", "TEXT");
  addColumn("finalize_result", "TEXT");

  // Sessions finalized before this schema existed have no phase, and the
  // control plane compares phases as text. Adopting them as terminal here is
  // what lets every later transition go through the shared guard instead of a
  // null-tolerant branch; their `finalize_result` stays null and is
  // reconstructed from the published rows on first replay.
  applyMigrationOnce(sql, "upload_sessions_adopt_terminal_finalize_phase", () =>
    sql.exec(
      `UPDATE upload_sessions
          SET finalize_phase = 'done', finalize_cleanup_cursor = total_chunks
        WHERE status = 'finalized' AND finalize_phase IS NULL`
    )
  );
}

/**
 * Control-plane columns of a resumable multipart abort.
 *
 * An abort is as large as the upload it undoes — a shard fence and a cleanup
 * intent per pool shard, a manifest row per chunk in up to four tables — so it
 * cannot be one transaction either. `abort_phase` and its four cursors are the
 * tuple `lib/paged-operation` fences those pages on; `abort_retry_at` is when
 * the sweep may look at the session again, which is what keeps a failing abort
 * backing off instead of spinning on the maintenance cadence.
 *
 * Every one of them is additive: an abort in flight when this schema arrives
 * is adopted below rather than restarted.
 */
export function applyMultipartAbortSchema(sql: SqlStorage): void {
  const addColumn = (name: string, definition: string): void =>
    applyMigrationOnce(sql, `upload_sessions_add_${name}`, () =>
      sql.exec(`ALTER TABLE upload_sessions ADD COLUMN ${name} ${definition}`)
    );

  addColumn("abort_phase", "TEXT");
  addColumn("abort_fence_cursor", "INTEGER NOT NULL DEFAULT 0");
  addColumn("abort_intent_cursor", "INTEGER NOT NULL DEFAULT 0");
  addColumn("abort_cleanup_cursor", "INTEGER NOT NULL DEFAULT 0");
  // Seeks by shard index, which starts at zero, so the first page selects
  // `> -1` and takes shard zero.
  addColumn("abort_old_intent_cursor", "INTEGER NOT NULL DEFAULT -1");
  addColumn("abort_retry_at", "INTEGER NOT NULL DEFAULT 0");

  // A session left mid-abort by a server that had no phase column has already
  // fenced some of its shards and staged none of its cleanup. Arming it at the
  // first phase is what the machine would do for it anyway: every page is
  // idempotent, so re-fencing a shard that already holds the fence and
  // re-staging an intent that already exists cost a replay, not a conflict.
  // Adopting the terminal rows too leaves `status = 'aborted'` and
  // `abort_phase = 'done'` the same fact, so no later reader has to special-
  // case a row this migration found.
  applyMigrationOnce(sql, "upload_sessions_adopt_abort_phase", () => {
    sql.exec(
      `UPDATE upload_sessions SET abort_phase = 'fencing'
        WHERE status = 'aborting' AND abort_phase IS NULL`
    );
    sql.exec(
      `UPDATE upload_sessions SET abort_phase = 'done'
        WHERE status IN ('aborted', 'poisoned') AND abort_phase IS NULL`
    );
  });
}
