import { applyMigrationOnce } from "../../../lib/migrations";

export function applyYjsSchema(sql: SqlStorage): void {
  // ── Yjs per-file mode ──────────────────────────────────────
  //
  // mode_yjs: opt-in per FILE bit. 0 = plain bytes (default);
  // 1 = the file is a Yjs CRDT op log. Storage is a
  // sequence of Yjs binary updates appended as chunks under the
  // existing refcounted machinery; readFile materializes the
  // Y.Doc and returns a serialised view; writeFile applies the
  // bytes via a Y.Text replacement transaction; live editors
  // connect via WebSocket.
  //
  // Set on per-file granularity, not per-tenant — a single
  // tenant can mix yjs-mode files with plain files freely.
  // Default 0 ⇒ no behavior change for any existing file.
  applyMigrationOnce(sql, "files_add_mode_yjs", () =>
    sql.exec(
      "ALTER TABLE files ADD COLUMN mode_yjs INTEGER NOT NULL DEFAULT 0"
    )
  );

  // yjs_oplog: append-only log of Yjs binary updates per file.
  // Each row is one update + a monotonic seq number per path_id
  // for ordering. Updates are ALSO chunked into ShardDOs via the
  // standard chunk_refs path (using a synthetic file_id of
  // `${pathId}#yjs#${seq}`) so refcount + GC come for free. The
  // SQL row carries a checkpoint flag — checkpoint rows are full
  // Y.Doc state snapshots that compaction creates so cold reads
  // don't replay the entire history.
  sql.exec(`
      CREATE TABLE IF NOT EXISTS yjs_oplog (
        path_id      TEXT NOT NULL,
        seq          INTEGER NOT NULL,
        kind         TEXT NOT NULL,
        chunk_hash   TEXT NOT NULL,
        chunk_size   INTEGER NOT NULL,
        shard_index  INTEGER NOT NULL,
        created_at   INTEGER NOT NULL,
        PRIMARY KEY (path_id, seq)
      )
    `);
  // Index by (path_id, seq DESC) for hot reads. Not strictly
  // needed since the PK already covers seq scans in either
  // direction on SQLite, but explicit + free.
  sql.exec(`
      CREATE INDEX IF NOT EXISTS idx_yjs_oplog_path_seq
        ON yjs_oplog(path_id, seq DESC)
    `);
  // yjs_meta: per-file Yjs state. Tracks the current seq counter,
  // the latest checkpoint seq (for cold-read replay bounds), and
  // whether a compaction is pending. One row per yjs-mode file.
  sql.exec(`
      CREATE TABLE IF NOT EXISTS yjs_meta (
        path_id            TEXT PRIMARY KEY,
        next_seq           INTEGER NOT NULL DEFAULT 0,
        last_checkpoint_seq INTEGER NOT NULL DEFAULT -1,
        op_count_since_ckpt INTEGER NOT NULL DEFAULT 0,
        last_compact_at    INTEGER NOT NULL DEFAULT 0,
        materialized_at    INTEGER NOT NULL DEFAULT 0
      )
    `);
}

export function applyYjsCompactionCounter(sql: SqlStorage): void {
  // Per-file encrypted-yjs op-log byte counter. Server-side
  // backpressure (see worker/core/objects/user/yjs.ts) consults this
  // alongside `op_count_since_ckpt` to decide whether to broadcast
  // the tag-4 compact-please advisory or hard-reject further appends
  // with EBUSY. Reset to 0 on every checkpoint commit.
  applyMigrationOnce(sql, "yjs_meta_add_bytes_since_last_compact", () =>
    sql.exec(
      "ALTER TABLE yjs_meta ADD COLUMN bytes_since_last_compact INTEGER NOT NULL DEFAULT 0"
    )
  );
}
