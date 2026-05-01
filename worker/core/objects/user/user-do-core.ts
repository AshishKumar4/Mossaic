import { DurableObject } from "cloudflare:workers";
import type { EnvCore as Env } from "../../../../shared/types";
import {
  hardDeleteFileRow,
  vfsAbortWriteStream,
  vfsAppendWriteStream,
  vfsBeginWriteStream,
  vfsChmod,
  vfsCommitWriteStream,
  vfsCreateReadStream,
  vfsCreateWriteStream,
  vfsExists,
  vfsLstat,
  vfsMkdir,
  vfsOpenManifest,
  vfsOpenReadStream,
  vfsPullReadStream,
  vfsReadChunk,
  vfsReadFile,
  vfsReadPreview,
  vfsReadlink,
  vfsReadManyStat,
  vfsReaddir,
  vfsRemoveRecursive,
  vfsRename,
  vfsRmdir,
  vfsStat,
  vfsSymlink,
  vfsUnlink,
  vfsPurge,
  vfsArchive,
  vfsUnarchive,
  vfsWriteFile,
  type VFSReadHandle,
  type VFSWriteFileOpts,
  type VFSWriteHandle,
} from "./vfs-ops";
import type {
  OpenManifestResult,
  VFSScope,
  VFSStatRaw,
} from "../../../../shared/vfs-types";
import type {
  ReadPreviewOpts,
  ReadPreviewResult,
} from "../../../../shared/preview-types";
import { VFSError } from "../../../../shared/vfs-types";
import { vfsShardDOName } from "../../lib/utils";
import { dedupePaths, type DedupeResult } from "./admin";
// type-only import. The YjsRuntime class is loaded
// lazily via `await import("./yjs")` inside `getYjsRuntime()` so
// non-collab consumers don't pay the ~250 KB yjs + y-protocols
// type-erase tax in the main bundle. The static type import is
// erased at runtime under `verbatimModuleSyntax`.
import type { YjsRuntime } from "./yjs";
import { enforceRateLimit } from "./rate-limit";
import {
  dropVersions,
  isVersioningEnabled,
  listVersions,
  resolvePathId,
  restoreVersion,
  setVersioningEnabled,
  type VersionRow,
} from "./vfs-versions";
export class UserDOCore extends DurableObject<Env> {
  sql: SqlStorage;
  /**
   * Public alias for the protected `env` from the DurableObject base
   * class. vfs-ops needs to dispatch ShardDO subrequests by binding
   * name; without this alias TS rejects external access. The base
   * class's `env` remains protected; we shadow it.
   */
  envPublic: Env;
  /**
   * Per-DO YjsRuntime cache. Lazily constructed on first access so
   * the import isn't evaluated for tenants that never use yjs-mode
   * files. Holds the in-memory `Y.Doc` cache + the live
   * WebSocket sets per pathId. State on disk (yjs_oplog, yjs_meta,
   * shard chunks) survives DO hibernation; the runtime instance does
   * not — it gets rebuilt cold from the op log on the first access
   * after wake. Sockets are restored via ctx.getWebSockets(pathId)
   * on first message after wake.
   */
  private _yjsRuntime: YjsRuntime | undefined;
  private initialized = false;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.sql = ctx.storage.sql;
    this.envPublic = env;
  }

  /**
   * Lazy YjsRuntime accessor — async because the class itself is
   * loaded via dynamic `import("./yjs")`. Non-collab
   * tenants never call this method; the entire yjs/y-protocols
   * graph is dead-code-eliminated from the consumer's bundle.
   *
   * On collab paths (`vfsOpenYjsSocket`, `vfsFlushYjs`,
   * `webSocketMessage` / `webSocketClose` / `webSocketError`) the
   * dynamic import resolves once per DO instance — subsequent
   * calls hit the in-memory cache.
   */
  async getYjsRuntime(): Promise<YjsRuntime> {
    if (this._yjsRuntime === undefined) {
      const { YjsRuntime } = await import("./yjs");
      this._yjsRuntime = new YjsRuntime(this);
    }
    return this._yjsRuntime;
  }

  /**
   * `protected` so the App subclass (`UserDO` in worker/app) can call
   * `this.ensureInit()` from its own `_legacyFetch` handler without
   * the schema migration silently being skipped on the legacy
   * /signup path.
   */
  protected ensureInit(): void {
    if (this.initialized) return;
    this.initialized = true;

    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS auth (
        user_id       TEXT PRIMARY KEY,
        email         TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        created_at    INTEGER NOT NULL,
        updated_at    INTEGER NOT NULL
      )
    `);

    this.sql.exec(`
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

    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS file_chunks (
        file_id       TEXT NOT NULL,
        chunk_index   INTEGER NOT NULL,
        chunk_hash    TEXT NOT NULL,
        chunk_size    INTEGER NOT NULL,
        shard_index   INTEGER NOT NULL,
        PRIMARY KEY (file_id, chunk_index)
      )
    `);

    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS folders (
        folder_id     TEXT PRIMARY KEY,
        user_id       TEXT NOT NULL,
        parent_id     TEXT,
        name          TEXT NOT NULL,
        created_at    INTEGER NOT NULL,
        updated_at    INTEGER NOT NULL
      )
    `);

    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS quota (
        user_id       TEXT PRIMARY KEY,
        storage_used  INTEGER NOT NULL DEFAULT 0,
        storage_limit INTEGER NOT NULL DEFAULT 107374182400,
        file_count    INTEGER NOT NULL DEFAULT 0,
        pool_size     INTEGER NOT NULL DEFAULT 32
      )
    `);

    // ── VFS schema migrations (sdk-impl-plan §3.1) ─────────────────────────
    // Each ALTER is idempotent via try/catch: SQLite throws "duplicate
    // column name" if the column already exists. The pattern matches
    // search-do.ts:59-68. CREATE TABLE/INDEX IF NOT EXISTS is naturally
    // idempotent.
    //
    // Backward compatibility: existing rows get default mode, NULL inline
    // data, node_kind='file'. The legacy app's reads keep working because
    // (a) new columns have defaults, (b) the manifest reader (files.ts)
    // continues to fall through to file_chunks when inline_data IS NULL.

    // file mode (POSIX), inline tier, symlink kind
    try {
      this.sql.exec(
        "ALTER TABLE files ADD COLUMN mode INTEGER NOT NULL DEFAULT 420"
      ); // 0o644
    } catch {
      // column already exists
    }
    try {
      this.sql.exec("ALTER TABLE files ADD COLUMN inline_data BLOB");
    } catch {
      // column already exists
    }
    try {
      this.sql.exec("ALTER TABLE files ADD COLUMN symlink_target TEXT");
    } catch {
      // column already exists
    }
    try {
      this.sql.exec(
        "ALTER TABLE files ADD COLUMN node_kind TEXT NOT NULL DEFAULT 'file'"
      );
    } catch {
      // column already exists
    }
    try {
      this.sql.exec(
        "ALTER TABLE folders ADD COLUMN mode INTEGER NOT NULL DEFAULT 493"
      ); // 0o755
    } catch {
      // column already exists
    }

    // POSIX uniqueness via partial indexes (SQLite cannot ALTER TABLE ADD
    // UNIQUE on existing tables). Scoped to non-deleted rows so prior
    // soft-deleted duplicates don't block migration.
    //
    // If existing data has live duplicates, this CREATE throws and is
    // swallowed; the admin dedupe route resolves them later.
    try {
      this.sql.exec(`
        CREATE UNIQUE INDEX IF NOT EXISTS uniq_files_parent_name
          ON files(user_id, IFNULL(parent_id, ''), file_name)
          WHERE status != 'deleted'
      `);
    } catch {
      // dupe live rows exist; admin dedupe is required before re-running
    }
    try {
      this.sql.exec(`
        CREATE UNIQUE INDEX IF NOT EXISTS uniq_folders_parent_name
          ON folders(user_id, IFNULL(parent_id, ''), name)
      `);
    } catch {
      // dupe folder rows exist; admin dedupe is required before re-running
    }

    // Lookup indexes (overdue per study §4)
    this.sql.exec(`
      CREATE INDEX IF NOT EXISTS idx_files_parent
        ON files(user_id, parent_id, status)
    `);
    this.sql.exec(`
      CREATE INDEX IF NOT EXISTS idx_folders_parent
        ON folders(user_id, parent_id)
    `);

    // ── per-tenant rate-limit state (token bucket) ──────────────
    //
    // Token-bucket limiter applied to VFS RPC methods (not the legacy
    // fetch handler). State persists across DO hibernation so the
    // bucket survives cold starts. Defaults: 100 ops/sec refill, 200
    // burst capacity. Operators can override per-tenant via direct
    // SQL or admin tooling. NULL columns inherit defaults at runtime.
    try {
      this.sql.exec(
        "ALTER TABLE quota ADD COLUMN rate_limit_per_sec INTEGER"
      );
    } catch {
      // column already exists
    }
    try {
      this.sql.exec(
        "ALTER TABLE quota ADD COLUMN rate_limit_burst INTEGER"
      );
    } catch {
      // column already exists
    }
    try {
      this.sql.exec("ALTER TABLE quota ADD COLUMN rl_tokens REAL");
    } catch {
      // column already exists
    }
    try {
      this.sql.exec("ALTER TABLE quota ADD COLUMN rl_updated_at INTEGER");
    } catch {
      // column already exists
    }

    // ── per-tenant versioning toggle (S3-style, opt-in) ─────────
    // versioning_enabled: NULL/0 = disabled (byte-equivalent
    // behavior); 1 = every writeFile/unlink creates a `file_versions`
    // row, readFile resolves the head version, and historical
    // readFile(path, {version: id}) becomes available. The default is
    // off; tenants opt-in via setTenantVersioning().
    try {
      this.sql.exec(
        "ALTER TABLE quota ADD COLUMN versioning_enabled INTEGER NOT NULL DEFAULT 0"
      );
    } catch {
      // column already exists
    }

    // Phase 32 Fix 5 — inline-tier graceful migration.
    //
    // Tracks per-tenant cumulative bytes stored in the inline tier
    // (`files.inline_data` BLOBs). `vfsWriteFile` consults this on
    // every write ≤ INLINE_LIMIT and falls through to the chunked
    // tier once `inline_bytes_used >= INLINE_TIER_CAP` (1 GiB) — the
    // soft ceiling prevents the inline tier from monopolizing the
    // UserDO's ~10 GiB SQLite quota.
    //
    // Maintained by `recordWriteUsage`'s `deltaInlineBytes`
    // parameter, called from `commitInlineTier` (positive delta)
    // and `hardDeleteFileRow` (negative delta when the deleted
    // row had `inline_data IS NOT NULL`).
    //
    // Defaults to 0; legacy rows behave as if no inline bytes are
    // accounted, so the cap is effectively only enforced for
    // POST-Phase-32 writes. That is correct behaviour: a tenant
    // already over the cap on legacy data continues to use inline
    // for the rows that already exist; new writes spill to chunked.
    try {
      this.sql.exec(
        "ALTER TABLE quota ADD COLUMN inline_bytes_used INTEGER NOT NULL DEFAULT 0"
      );
    } catch {
      // column already exists
    }

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
    this.sql.exec(`
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
    this.sql.exec(`
      CREATE INDEX IF NOT EXISTS idx_file_versions_path_mtime
        ON file_versions(path_id, mtime_ms DESC)
    `);
    this.sql.exec(`
      CREATE INDEX IF NOT EXISTS idx_file_versions_user
        ON file_versions(user_id, path_id)
    `);

    // version_chunks: per-version chunk manifest. Mirrors file_chunks
    // but keyed by version_id.
    this.sql.exec(`
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
    this.sql.exec(`
      CREATE INDEX IF NOT EXISTS idx_version_chunks_hash
        ON version_chunks(chunk_hash)
    `);

    // Head-pointer column on `files`: when versioning is enabled, the
    // `files` row is just a stable identity for the path; the actual
    // head version lives in file_versions. Legacy / versioning-OFF
    // tenants leave this NULL and continue using files' own columns.
    try {
      this.sql.exec(
        "ALTER TABLE files ADD COLUMN head_version_id TEXT"
      );
    } catch {
      // column already exists
    }

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
    try {
      this.sql.exec(
        "ALTER TABLE files ADD COLUMN mode_yjs INTEGER NOT NULL DEFAULT 0"
      );
    } catch {
      // column already exists
    }

    // yjs_oplog: append-only log of Yjs binary updates per file.
    // Each row is one update + a monotonic seq number per path_id
    // for ordering. Updates are ALSO chunked into ShardDOs via the
    // standard chunk_refs path (using a synthetic file_id of
    // `${pathId}#yjs#${seq}`) so refcount + GC come for free. The
    // SQL row carries a checkpoint flag — checkpoint rows are full
    // Y.Doc state snapshots that compaction creates so cold reads
    // don't replay the entire history.
    this.sql.exec(`
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
    this.sql.exec(`
      CREATE INDEX IF NOT EXISTS idx_yjs_oplog_path_seq
        ON yjs_oplog(path_id, seq DESC)
    `);
    // yjs_meta: per-file Yjs state. Tracks the current seq counter,
    // the latest checkpoint seq (for cold-read replay bounds), and
    // whether a compaction is pending. One row per yjs-mode file.
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS yjs_meta (
        path_id            TEXT PRIMARY KEY,
        next_seq           INTEGER NOT NULL DEFAULT 0,
        last_checkpoint_seq INTEGER NOT NULL DEFAULT -1,
        op_count_since_ckpt INTEGER NOT NULL DEFAULT 0,
        last_compact_at    INTEGER NOT NULL DEFAULT 0,
        materialized_at    INTEGER NOT NULL DEFAULT 0
      )
    `);

    // ── Audit H1: stale-upload sweeper bookkeeping ───────────────────────
    //
    // Plan §7 requires a UserDO `alarm()` that hard-deletes
    // `_vfs_tmp_*` rows abandoned by a crash mid-write. The alarm
    // runs without an HTTP/RPC scope, so it cannot synthesize
    // (ns, tenant, sub) from a request. We persist the DO's scope on
    // every gated VFS call so the alarm can reconstruct it.
    //
    // `vfs_meta` is a tiny key/value table that survives DO
    // hibernation. We store one row keyed `scope` whose value is a
    // JSON-encoded `{ ns, tenant, sub? }`. Writes are idempotent
    // (INSERT OR REPLACE) and hot-path-cheap (a single SQL UPSERT
    // bounded to one row). Pre-existing tenants without this row
    // keep working — the alarm becomes a no-op for them until the
    // first gated call records their scope.
    //
    // The same table also carries the H6 migration_state markers
    // ('files_unique_index', 'folders_unique_index') to surface a
    // failed CREATE UNIQUE INDEX rather than silently swallow it.
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS vfs_meta (
        key   TEXT PRIMARY KEY,
        value TEXT NOT NULL
      )
    `);

    // Phase 32 Fix 4 \u2014 skip-full-shard placement.
    //
    // Persistent per-shard byte-count cache. Refreshed every
    // ~30 min by `monitorShardCapacity` from the alarm path.
    // \`placeChunk\` reads this table to construct a `fullShards`
    // skip-set: rendezvous winners that are at-or-over the soft
    // cap fall over to the next-best score so writes never land
    // on a near-capacity shard. Backward-compat: empty cache
    // (no rows) \u2192 `placeChunk` is byte-equivalent to the
    // pre-Phase-32 deterministic top-1 winner; the test pool +
    // brand-new tenants exhibit identical placement until the
    // first capacity poll runs.
    //
    // Cold-cache scenarios (no entry for a particular shard)
    // are treated as \"not full\" \u2014 better to write to an
    // un-measured shard than to refuse the write under-spec'd.
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS shard_storage_cache (
        shard_index   INTEGER PRIMARY KEY,
        bytes_stored  INTEGER NOT NULL,
        refreshed_at  INTEGER NOT NULL
      )
    `);

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

    try {
      this.sql.exec("ALTER TABLE files ADD COLUMN metadata BLOB");
    } catch {
      // column already exists
    }
    try {
      this.sql.exec(
        "ALTER TABLE file_versions ADD COLUMN user_visible INTEGER NOT NULL DEFAULT 0"
      );
    } catch {
      // column already exists
    }
    try {
      this.sql.exec("ALTER TABLE file_versions ADD COLUMN label TEXT");
    } catch {
      // column already exists
    }
    try {
      this.sql.exec("ALTER TABLE file_versions ADD COLUMN metadata BLOB");
    } catch {
      // column already exists
    }

    // ── opt-in end-to-end encryption ───────────────────────────
    //
    // Two columns on `files` and two on `file_versions` carry the
    // per-file encryption mode + opaque keyId label. pre-encryption rows
    // get NULL by default — the SDK treats NULL as "plaintext" and
    // returns the bytes verbatim, preserving full backward compatibility.
    //
    // The server NEVER decrypts user data. These columns are pure
    // metadata used to (a) tell the SDK whether to attempt decryption
    // on read, and (b) reject mixed-mode writes within a path's
    // history with EBADF.
    //
    // No CHECK constraint — consistent with how `mode_yjs` was added
    // without one. The SDK validates the values it reads.
    //
    // No new wrangler migration tag — additive ALTERs are idempotent.
    try {
      this.sql.exec("ALTER TABLE files ADD COLUMN encryption_mode TEXT");
    } catch {
      // column already exists
    }
    try {
      this.sql.exec("ALTER TABLE files ADD COLUMN encryption_key_id TEXT");
    } catch {
      // column already exists
    }
    try {
      this.sql.exec(
        "ALTER TABLE file_versions ADD COLUMN encryption_mode TEXT"
      );
    } catch {
      // column already exists
    }
    try {
      this.sql.exec(
        "ALTER TABLE file_versions ADD COLUMN encryption_key_id TEXT"
      );
    } catch {
      // column already exists
    }

    // Phase 27 — multipart × versioning consistency.
    //
    // Records the actual ShardDO chunk_refs file_id used at write
    // time for this version's chunks. The legacy versioned write
    // path (vfsWriteFileVersioned, restoreVersion, copy-file) uses
    // the synthetic `${pathId}#${versionId}` form
    // (`shardRefId(pathId, versionId)` in vfs-versions.ts:179) and
    // so the column is NULL for those rows — `dropVersionRows`
    // falls back to the synthetic form.
    //
    // Multipart-finalize-under-versioning (Phase 27) writes chunks
    // to ShardDOs at upload time keyed by `refId = uploadId`. The
    // `file_versions` row created at finalize stamps
    // `shard_ref_id = uploadId` so a future `dropVersionRows` calls
    // ShardDO `deleteChunks(uploadId)` and finds the right
    // `chunk_refs` rows. Without this column, the canonical fan-out
    // would key off `${pathId}#${versionId}` and decrement nothing
    // — leaking chunk bytes forever.
    try {
      this.sql.exec(
        "ALTER TABLE file_versions ADD COLUMN shard_ref_id TEXT"
      );
    } catch {
      // column already exists
    }

    // Per-file encrypted-yjs op-log byte counter. Server-side
    // backpressure (see worker/core/objects/user/yjs.ts) consults this
    // alongside `op_count_since_ckpt` to decide whether to broadcast
    // the tag-4 compact-please advisory or hard-reject further appends
    // with EBUSY. Reset to 0 on every checkpoint commit.
    try {
      this.sql.exec(
        "ALTER TABLE yjs_meta ADD COLUMN bytes_since_last_compact INTEGER NOT NULL DEFAULT 0"
      );
    } catch {
      // column already exists
    }

    // Phase 23 Blindspot fix: indexed_at marks files that have been
    // search-indexed (text+CLIP via `indexFile` in worker/app/routes/
    // search.ts). NULL = not yet indexed. The reconciler alarm
    // (`runIndexReconcile`) sweeps NULL rows on a periodic cadence
    // and re-queues them. Pre-fix, if the SPA crashed between
    // `multipart/finalize` and `POST /api/index/file` the file
    // landed in canonical VFS but was never indexed → silent search
    // miss for the lifetime of the file.
    try {
      this.sql.exec("ALTER TABLE files ADD COLUMN indexed_at INTEGER");
    } catch {
      // column already exists
    }
    // Sparse index — most files are indexed within seconds of finalize,
    // so the reconciler scan should be fast: the partial index makes
    // `WHERE indexed_at IS NULL` an index-only scan.
    this.sql.exec(`
      CREATE INDEX IF NOT EXISTS idx_files_indexed_at_null
        ON files(file_id)
        WHERE indexed_at IS NULL AND status = 'complete'
    `);

    // ── Phase 29 — archive bit (three-tier delete API) ────────────
    //
    // `archived = 1` hides a path from the default `listFiles` /
    // `fileInfo` results without destroying or tombstoning data.
    // Reads (`stat`, `readFile`, `readPreview`, `createReadStream`,
    // `openManifest`, `readChunk`, `listVersions`, `restoreVersion`)
    // are UNCHANGED — an archived file is fully readable by anyone
    // who knows its path. Only the listing-side filters apply.
    //
    // Three-tier delete model after Phase 29:
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
    try {
      this.sql.exec(
        "ALTER TABLE files ADD COLUMN archived INTEGER NOT NULL DEFAULT 0"
      );
    } catch {
      // column already exists
    }

    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS file_tags (
        path_id   TEXT NOT NULL,
        tag       TEXT NOT NULL,
        user_id   TEXT NOT NULL,
        mtime_ms  INTEGER NOT NULL,
        PRIMARY KEY (path_id, tag)
      )
    `);
    this.sql.exec(`
      CREATE INDEX IF NOT EXISTS idx_file_tags_tag_mtime
        ON file_tags(tag, mtime_ms DESC, path_id)
    `);

    this.sql.exec(`
      CREATE INDEX IF NOT EXISTS idx_files_parent_mtime
        ON files(IFNULL(parent_id, ''), updated_at DESC, file_name)
        WHERE status = 'complete'
    `);
    this.sql.exec(`
      CREATE INDEX IF NOT EXISTS idx_files_parent_size
        ON files(IFNULL(parent_id, ''), file_size DESC, file_name)
        WHERE status = 'complete'
    `);

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
    this.sql.exec(`
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
    this.sql.exec(`
      CREATE INDEX IF NOT EXISTS idx_upload_sessions_open_expires
        ON upload_sessions(expires_at)
        WHERE status = 'open'
    `);
    this.sql.exec(`
      CREATE INDEX IF NOT EXISTS idx_upload_sessions_user_status
        ON upload_sessions(user_id, status)
    `);

    // ── Universal preview pipeline ───────────────────────────────────────
    //
    // `file_variants` records pre-generated and on-demand-cached preview
    // bytes (thumb / medium / lightbox + custom dimensions). Variant
    // bytes live on a ShardDO under the same `chunks` / `chunk_refs`
    // refcount machinery as primary file chunks; this row maps
    // (file_id, variant_kind, renderer_kind) → (chunk_hash, shard_index).
    //
    // - Composite PK lets the same file carry multiple renderer
    //   strategies (e.g. a video could have both a "video-poster" thumb
    //   AND a "waveform" medium).
    // - `chunk_hash` is content-addressed (SHA-256 of variant bytes).
    //   Per-shard dedup fires inside `writeChunkInternal`
    //   (`shard-do.ts:457`) when two writes land on the same shard for
    //   the same hash. Cross-user/cross-file dedup is NOT achieved by
    //   construction: `placeChunk` keys on `(userId, fileId,
    //   chunkIndex)` not on hash, so identical bytes from different
    //   files generally route to different shards. Same-file replay
    //   (idempotent retry) is the realistic dedup path.
    // - `ON DELETE CASCADE` removes variant rows when the parent
    //   `files` row is hard-deleted; chunk_refs cleanup is dispatched
    //   by `vfsUnlink` (see worker/core/objects/user/vfs/write-commit.ts).
    //
    // Idempotent CREATE TABLE; no migration tag.
    //
    // @lean-invariant Mossaic.Vfs.Preview.stepVariant_preserves_validState
    //   Lean proves all variant ops (insert / delete / cascadeFileDelete)
    //   preserve the PK uniqueness invariant. See
    //   `lean/Mossaic/Vfs/Preview.lean :: stepVariant_preserves_validState`.
    // @lean-invariant Mossaic.Vfs.Preview.cascade_delete_drops_all
    //   Lean proves CASCADE-delete drops every variant row for the
    //   deleted file_id. See
    //   `lean/Mossaic/Vfs/Preview.lean :: cascade_delete_drops_all`.
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS file_variants (
        file_id        TEXT NOT NULL,
        variant_kind   TEXT NOT NULL,
        renderer_kind  TEXT NOT NULL,
        chunk_hash     TEXT NOT NULL,
        shard_index    INTEGER NOT NULL,
        mime_type      TEXT NOT NULL,
        width          INTEGER NOT NULL,
        height         INTEGER NOT NULL,
        byte_size      INTEGER NOT NULL,
        created_at     INTEGER NOT NULL,
        PRIMARY KEY (file_id, variant_kind, renderer_kind),
        FOREIGN KEY (file_id) REFERENCES files(file_id) ON DELETE CASCADE
      )
    `);
    this.sql.exec(`
      CREATE INDEX IF NOT EXISTS idx_file_variants_hash
        ON file_variants(chunk_hash)
    `);
    this.sql.exec(`
      CREATE INDEX IF NOT EXISTS idx_file_variants_file
        ON file_variants(file_id)
    `);

    // Phase 28 Fix 1 — version-aware variant cache.
    //
    // Pre-fix, the cache key was `(file_id, variant_kind, renderer_kind)`.
    // After v2 supersedes v1 on a versioning-on tenant, the cache row
    // for v1 still matches a thumbnail lookup and the gallery serves
    // STALE bytes for the file's HEAD until the row is manually
    // invalidated. The bug surfaces as "I edited the photo but the
    // thumbnail shows the old image."
    //
    // Post-fix: store the head_version_id at render time on the
    // variant row. The reader (`findVariantRow` in
    // `worker/core/objects/user/preview-variants.ts`) gates on a match
    // with the file's CURRENT head_version_id; mismatch → cache miss
    // → re-render against the new head. Existing (legacy) rows have
    // version_id IS NULL — they remain valid for the
    // versioning-OFF / no-head-version case (where head_version_id
    // is NULL on `files` too). The legacy passthrough is a load-
    // bearing equivalence: NULL == NULL is the SQL convention we
    // adopt explicitly (`IS NULL` predicate, not `=`).
    try {
      this.sql.exec(
        "ALTER TABLE file_variants ADD COLUMN version_id TEXT"
      );
    } catch {
      // column already exists
    }

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
    this.checkAndRecordIndex(
      "uniq_files_parent_name",
      "files_unique_index",
      "files"
    );
    this.checkAndRecordIndex(
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
  private checkAndRecordIndex(
    indexName: string,
    markerKey: string,
    table: string
  ): void {
    const present = this.sql
      .exec(
        "SELECT 1 FROM sqlite_master WHERE type='index' AND name = ? LIMIT 1",
        indexName
      )
      .toArray();
    if (present.length > 0) {
      // Index is healthy. Clear any stale marker (e.g. an admin run
      // dedupe and re-init).
      this.sql.exec(
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
    this.sql.exec(
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

  /** Read H6 markers; returns the list of degraded index keys. */
  private readDegradedIndexes(): string[] {
    const rows = this.sql
      .exec(
        "SELECT key FROM vfs_meta WHERE key IN ('files_unique_index', 'folders_unique_index')"
      )
      .toArray() as { key: string }[];
    return rows.map((r) => r.key);
  }

  /**
   * Top-level fetch entry. Core's fetch handles the Yjs WebSocket
   * upgrade; every other request returns 404. The App subclass
   * (`UserDO` in `worker/app/objects/user/user-do.ts`) overrides
   * this method to delegate non-WS HTTP traffic to the legacy
   * photo-app handler whose body is byte-pinned.
   *
   * Service-mode deployments (deployments/service/wrangler.jsonc)
   * bind `class_name: "UserDOCore"` directly and never see the
   * App subclass — they serve VFS over typed RPC + WebSocket only.
   */
  override async fetch(request: Request): Promise<Response> {
    if (request.headers.get("Upgrade")?.toLowerCase() === "websocket") {
      return this._fetchWebSocketUpgrade(request);
    }
    // Core has no legacy HTTP surface. The App subclass (UserDO)
    // overrides fetch() to delegate non-WS requests to its own
    // `_legacyFetch` handler. Service-mode deployments do NOT bind
    // the App class and so this branch is the live path — they
    // serve VFS over typed RPC + WebSocket only.
    return new Response("not found", { status: 404 });
  }

  /**
   * WebSocket upgrade entry. Path-encoded params:
   *   /yjs/ws?path=<encoded path>&ns=<ns>&tenant=<tenant>[&sub=<sub>]
   *
   * Yjs binary frames flow over the WebSocket; the upgrade is the
   * one moment when the SDK pays a `fetch` round-trip rather than a
   * typed-RPC call. We avoid typed-RPC for the upgrade because
   * Cloudflare DO RPC currently can't serialize a Response that
   * carries a `webSocket` field across the RPC boundary — only
   * `fetch()` is permitted to return such a Response.
   */
  private async _fetchWebSocketUpgrade(request: Request): Promise<Response> {
    this.ensureInit();
    const url = new URL(request.url);
    if (url.pathname !== "/yjs/ws") {
      return new Response("not found", { status: 404 });
    }
    const path = url.searchParams.get("path");
    const ns = url.searchParams.get("ns");
    const tenant = url.searchParams.get("tenant");
    const sub = url.searchParams.get("sub") ?? undefined;
    if (!path || !ns || !tenant) {
      return new Response("missing required query params: path, ns, tenant", {
        status: 400,
      });
    }
    try {
      return await this.vfsOpenYjsSocket({ ns, tenant, sub }, path);
    } catch (err) {
      const code =
        err && typeof err === "object" && "code" in err
          ? (err as { code: string }).code
          : "EINTERNAL";
      const message = err instanceof Error ? err.message : "internal error";
      return Response.json(
        { error: message, code },
        { status: code === "ENOENT" ? 404 : 400 }
      );
    }
  }
  // ── VFS RPC surface (read-side) ───────────────────────────────
  //
  // Cloudflare DO RPC: any public async method on the DO class is callable
  // from a holder of the stub via `stub.methodName(args)`. The consumer
  // pays exactly one subrequest per call regardless of internal fan-out.
  // See sdk-impl-plan §5.3 for the full contract; these are the read-side
  // methods that land in. Write-side and streaming methods come
  // in Phases 3 and 4.
  //
  // Each method calls ensureInit() so the schema migrations
  // run before any VFS access on a DO that hasn't seen any legacy
  // /fetch traffic yet.

  /**
   * gate: ensureInit + per-tenant rate-limit check. Every
   * VFS RPC method calls this BEFORE delegating to vfs-ops. The
   * legacy fetch handler is unaffected — it has its own ensureInit
   * and is exempt from the new rate limiter (back-compat with the
   * existing user-facing app's traffic patterns).
   *
   * Audit H1: also persist the call scope so the stale-upload
   * sweeper alarm can reconstruct a (ns, tenant, sub) without an
   * RPC caller.
   */
  private gateVfs(scope: VFSScope): void {
    this.ensureInit();
    enforceRateLimit(this, scope);
    this.recordScope(scope);
  }

  /**
   * Audit H6: write-specific gate. Refuses with EBUSY when the
   * UNIQUE partial index on `files` is missing — legacy duplicate
   * rows would otherwise let two concurrent writeFiles to the same
   * path both insert their own `complete` row and corrupt the path
   * mapping silently. Reads bypass this gate (they tolerate dupes
   * by returning the first match).
   */
  private gateVfsWrite(scope: VFSScope): void {
    this.gateVfs(scope);
    const degraded = this.readDegradedIndexes();
    if (degraded.includes("files_unique_index")) {
      throw new VFSError(
        "EBUSY",
        "VFS writes refused: legacy duplicate rows block uniq_files_parent_name. " +
          "Run admin dedupe (`POST /admin/dedupe-paths`) and reload the DO."
      );
    }
  }

  /**
   * Persist the active scope into `vfs_meta` so alarm() can rehydrate
   * a VFSScope. Idempotent UPSERT bounded to one row. The DO is
   * already per-(ns, tenant, sub?) so this is mostly a "first-write
   * wins" lookup; we still UPSERT on every gated call because the
   * cost is one SQL statement and it self-heals if a row was wiped
   * by a manual SQL repair.
   */
  private recordScope(scope: VFSScope): void {
    const value = JSON.stringify({
      ns: scope.ns,
      tenant: scope.tenant,
      ...(scope.sub !== undefined ? { sub: scope.sub } : {}),
    });
    this.sql.exec(
      "INSERT OR REPLACE INTO vfs_meta (key, value) VALUES ('scope', ?)",
      value
    );
  }

  /** Read the scope persisted by gateVfs. Null if no VFS call has ever run. */
  private loadScope(): VFSScope | null {
    const row = this.sql
      .exec("SELECT value FROM vfs_meta WHERE key = 'scope'")
      .toArray()[0] as { value: string } | undefined;
    if (!row) return null;
    try {
      const parsed = JSON.parse(row.value) as {
        ns: string;
        tenant: string;
        sub?: string;
      };
      if (typeof parsed.ns !== "string" || typeof parsed.tenant !== "string") {
        return null;
      }
      return parsed;
    } catch {
      return null;
    }
  }

  /**
   * Public alias used by the alarm-sweep tests to invoke the
   * scheduling helper without introspecting private methods.
   * Production callers reach this via writeFile / beginWriteStream
   * which schedule the alarm as a side effect of the tmp insert.
   */
  scheduleStaleUploadSweep(): Promise<void> {
    return this.ensureStaleSweepScheduled();
  }

  /**
   * Ensure an alarm is scheduled within the next ~10 minutes.
   * Idempotent: leaves an existing earlier alarm in place. Called
   * from vfs-ops at every tmp-row insert so a crashing tenant gets
   * its sweep regardless of subsequent traffic.
   */
  private async ensureStaleSweepScheduled(): Promise<void> {
    const cur = await this.ctx.storage.getAlarm();
    const target = Date.now() + 10 * 60 * 1000; // 10 min cadence
    if (cur === null || cur > target) {
      await this.ctx.storage.setAlarm(target);
    }
  }

  /**
   * Audit H1: stale-upload sweeper.
   *
   * Reaps `_vfs_tmp_<id>` rows older than 1 hour with `status='uploading'`.
   * For each, runs the same hard-delete path as a synchronous abort:
   * UserDO metadata first (DELETE file_chunks + DELETE files), then
   * one ShardDO `deleteChunks` RPC per touched shard. The shard
   * refcount drops; the per-shard alarm reclaims the chunk blobs
   * after the 30s grace.
   *
   * Idempotent: re-running over an already-reaped tmp is a no-op
   * (DELETE matches zero rows; ShardDO `removeFileRefs` finds zero
   * chunk_refs). Cloudflare alarms have at-least-once semantics so
   * idempotence is load-bearing.
   *
   * Reschedules itself if the LIMIT batch was filled, on the same
   * 10-minute cadence as a fresh tmp-row insert.
   */
  async alarm(): Promise<void> {
    this.ensureInit();
    const scope = this.loadScope();
    if (!scope) {
      // No VFS call has ever run on this DO ⇒ no tmp rows could exist.
      // Be defensive though: an operator might wipe vfs_meta but
      // leave files behind. We still skip — without a scope we
      // cannot route deleteChunks to the right ShardDO instance.
      return;
    }

    const cutoff = Date.now() - 60 * 60 * 1000; // 1h staleness
    const rows = this.sql
      .exec(
        `SELECT file_id FROM files
          WHERE status = 'uploading'
            AND file_name LIKE '_vfs_tmp_%'
            AND created_at < ?
          LIMIT 200`,
        cutoff
      )
      .toArray() as { file_id: string }[];

    for (const { file_id } of rows) {
      // user_id encoding mirrors userIdFor(scope) in vfs-ops.
      const userId =
        scope.sub !== undefined ? `${scope.tenant}::${scope.sub}` : scope.tenant;
      try {
        await hardDeleteFileRow(this, userId, scope, file_id);
      } catch {
        // Best-effort: a transient ShardDO error leaves the row
        // intact; the next alarm fires on the same row. The 1h
        // staleness window is large enough that retry storms are
        // bounded.
      }
    }

    // sweep expired multipart sessions in the same alarm
    // cadence. The same `scope` works for every session row because
    // the scope is the persisted tenant identity for THIS DO instance
    // (tenant + optional sub) — multipart sessions can't span
    // tenants, only span uploads within one tenant. Idempotent.
    let multipartHasMore = false;
    try {
      const { sweepExpiredMultipartSessions } = await import(
        "./multipart-upload"
      );
      const r = await sweepExpiredMultipartSessions(this, () => scope);
      multipartHasMore = r.remaining;
    } catch {
      // Best-effort.
    }

    // Phase 28 Fix 4 — shard capacity warning poll. Throttled to
    // once per hour by the helper itself; reads `quota.pool_size`
    // and fans out a `getStorageBytes` RPC per shard. Logs a
    // structured warning for each shard that's >softCap (9 GB).
    // Best-effort: a transient shard failure or missing quota row
    // is swallowed so capacity monitoring never blocks the
    // primary alarm work.
    try {
      const poolRow = this.sql
        .exec(
          "SELECT pool_size FROM quota WHERE user_id = ?",
          scope.sub !== undefined
            ? `${scope.tenant}::${scope.sub}`
            : scope.tenant
        )
        .toArray()[0] as { pool_size: number } | undefined;
      if (poolRow) {
        const { monitorShardCapacity } = await import("./shard-capacity");
        await monitorShardCapacity(this, scope, poolRow.pool_size);
      }
    } catch {
      // Best-effort.
    }

    // Reschedule if the batch was capped, otherwise fall through —
    // the next gated VFS call will re-arm via ensureStaleSweepScheduled.
    if (rows.length === 200 || multipartHasMore) {
      await this.ctx.storage.setAlarm(Date.now() + 60_000);
    }
  }

  /** stat() — follows trailing symlinks. Throws ENOENT/ELOOP/ENOTDIR. */
  async vfsStat(scope: VFSScope, path: string): Promise<VFSStatRaw> {
    this.gateVfs(scope);
    return vfsStat(this, scope, path);
  }

  /** lstat() — does NOT follow trailing symlinks. */
  async vfsLstat(scope: VFSScope, path: string): Promise<VFSStatRaw> {
    this.gateVfs(scope);
    return vfsLstat(this, scope, path);
  }

  /** exists() — returns true iff the path resolves to a file/dir/symlink. */
  async vfsExists(scope: VFSScope, path: string): Promise<boolean> {
    this.gateVfs(scope);
    return vfsExists(this, scope, path);
  }

  /** readlink() — returns the symlink target string. EINVAL if not a symlink. */
  async vfsReadlink(scope: VFSScope, path: string): Promise<string> {
    this.gateVfs(scope);
    return vfsReadlink(this, scope, path);
  }

  /** readdir() — entry names under a directory. ENOTDIR/ENOENT if applicable. */
  async vfsReaddir(scope: VFSScope, path: string): Promise<string[]> {
    this.gateVfs(scope);
    return vfsReaddir(this, scope, path);
  }

  /** readManyStat() — batched lstat for git-style workloads. */
  async vfsReadManyStat(
    scope: VFSScope,
    paths: string[]
  ): Promise<(VFSStatRaw | null)[]> {
    this.gateVfs(scope);
    return vfsReadManyStat(this, scope, paths);
  }

  /**
   * readFile() — returns Uint8Array bytes. EISDIR/EFBIG/ENOENT/ELOOP.
   * pass `opts.versionId` to read a historical version
   * directly. Tombstone versions throw ENOENT.
   */
  async vfsReadFile(
    scope: VFSScope,
    path: string,
    opts?: { versionId?: string }
  ): Promise<Uint8Array> {
    this.gateVfs(scope);
    return vfsReadFile(this, scope, path, opts);
  }

  /** openManifest() — public, shard-index-stripped manifest for caller-orchestrated reads. */
  async vfsOpenManifest(
    scope: VFSScope,
    path: string
  ): Promise<OpenManifestResult> {
    this.gateVfs(scope);
    return vfsOpenManifest(this, scope, path);
  }

  /** readChunk() — fetch one chunk by (path, chunkIndex). */
  async vfsReadChunk(
    scope: VFSScope,
    path: string,
    chunkIndex: number
  ): Promise<Uint8Array> {
    this.gateVfs(scope);
    return vfsReadChunk(this, scope, path, chunkIndex);
  }

  /**
   * readPreview() — universal preview pipeline entry. Resolves
   * the file at `path`, dispatches the registered renderer for
   * its MIME, and returns variant bytes inline. Variant rows are
   * cached in `file_variants`; subsequent calls for the same
   * (file, variant) hit the cache.
   *
   * Encrypted files throw `ENOTSUP` — server cannot render
   * ciphertext. Custom variants render every call (no cache row).
   */
  async vfsReadPreview(
    scope: VFSScope,
    path: string,
    opts: ReadPreviewOpts = {}
  ): Promise<ReadPreviewResult> {
    this.gateVfs(scope);
    return vfsReadPreview(this, scope, path, opts);
  }

  // ── VFS RPC surface (write-side) ──────────────────────────────
  //
  // Atomic writes (temp-id-then-rename), hard delete with chunk GC fan-out,
  // and the supporting mutating ops. Each method runs inside a single
  // single-threaded DO invocation, so the supersede + rename sequence in
  // commitRename is atomic against concurrent reads/writes (sdk-impl-plan
  // §7). Chunks are reaped via ShardDO.deleteChunks RPC, which soft-marks
  // and lets the alarm-driven sweeper hard-delete after a 30s grace
  // (sdk-impl-plan §8.3).
  //
  // Inline tier (≤ INLINE_LIMIT) writes never touch ShardDO — the data
  // lives in files.inline_data and the entire write is one INSERT.

  /**
   * writeFile() — atomic, last-writer-wins. Inline tier ≤16KB;
   * chunked otherwise. extends the opts to carry metadata,
   * tags, and version flags; defaults preserve behavior.
   */
  async vfsWriteFile(
    scope: VFSScope,
    path: string,
    data: Uint8Array,
    opts?: {
      mode?: number;
      mimeType?: string;
      metadata?: Record<string, unknown> | null;
      tags?: readonly string[];
      version?: { label?: string; userVisible?: boolean };
      // optional encryption stamp. Server NEVER decrypts —
      // it just records `encryption_mode` + `encryption_key_id` on the
      // file row so the SDK knows what to do on read.
      encryption?: { mode: "convergent" | "random"; keyId?: string };
    }
  ): Promise<void> {
    this.gateVfsWrite(scope);
    return vfsWriteFile(this, scope, path, data, opts);
  }

  /** unlink() — hard-delete file/symlink + dispatch chunk GC. EISDIR for dirs. */
  async vfsUnlink(scope: VFSScope, path: string): Promise<void> {
    this.gateVfsWrite(scope);
    return vfsUnlink(this, scope, path);
  }

  /**
   * purge() — Phase 25 destructive cleanup.
   *
   * Drops every version row + the `files` row + decrements ShardDO
   * chunk refs for all versions' chunks. Independent of versioning
   * state. Idempotent — calling on a non-existent path is a no-op.
   */
  async vfsPurge(scope: VFSScope, path: string): Promise<void> {
    this.gateVfsWrite(scope);
    return vfsPurge(this, scope, path);
  }

  /**
   * Phase 29 — `archive(path)` / `unarchive(path)`.
   *
   * Hide a path from default `listFiles` / `fileInfo` results
   * without destroying or tombstoning data. Read surfaces (`stat`,
   * `readFile`, etc.) are unchanged — an archived file is fully
   * readable by anyone who knows the path.
   */
  async vfsArchive(scope: VFSScope, path: string): Promise<void> {
    this.gateVfsWrite(scope);
    vfsArchive(this, scope, path);
  }

  async vfsUnarchive(scope: VFSScope, path: string): Promise<void> {
    this.gateVfsWrite(scope);
    vfsUnarchive(this, scope, path);
  }

  /** mkdir() — create folder; recursive flag walks intermediates. */
  async vfsMkdir(
    scope: VFSScope,
    path: string,
    opts?: { recursive?: boolean; mode?: number }
  ): Promise<void> {
    this.gateVfsWrite(scope);
    vfsMkdir(this, scope, path, opts);
  }

  /** rmdir() — remove empty directory. ENOTEMPTY/ENOTDIR/ENOENT. */
  async vfsRmdir(scope: VFSScope, path: string): Promise<void> {
    this.gateVfsWrite(scope);
    vfsRmdir(this, scope, path);
  }

  /** rename() — atomic move/rename. Replace semantics for files, EEXIST for dirs. */
  async vfsRename(
    scope: VFSScope,
    src: string,
    dst: string
  ): Promise<void> {
    this.gateVfsWrite(scope);
    return vfsRename(this, scope, src, dst);
  }

  /** chmod() — update mode bits on a file/symlink/dir. */
  async vfsChmod(
    scope: VFSScope,
    path: string,
    mode: number
  ): Promise<void> {
    this.gateVfs(scope);
    vfsChmod(this, scope, path, mode);
  }

  /** symlink() — create a symlink at linkPath pointing to target. */
  async vfsSymlink(
    scope: VFSScope,
    target: string,
    linkPath: string
  ): Promise<void> {
    this.gateVfsWrite(scope);
    vfsSymlink(this, scope, target, linkPath);
  }

  /** removeRecursive() — paginated rm -rf on a directory subtree. */
  async vfsRemoveRecursive(
    scope: VFSScope,
    path: string,
    cursor?: string
  ): Promise<{ done: boolean; cursor?: string }> {
    this.gateVfsWrite(scope);
    return vfsRemoveRecursive(this, scope, path, cursor);
  }

  // ── streaming + handle-based stream primitives ───────────────
  //
  // Two shapes per stream direction:
  //
  //   Read:  vfsOpenReadStream + vfsPullReadStream (handle-based, works
  //          across separate consumer invocations — the escape hatch
  //          for files larger than one Worker invocation can fan out)
  //          and vfsCreateReadStream (returns a ReadableStream over RPC
  //          for in-the-same-invocation use cases).
  //
  //   Write: vfsBeginWriteStream + vfsAppendWriteStream +
  //          vfsCommitWriteStream / vfsAbortWriteStream (handle-based,
  //          chunk-by-chunk, resumable across consumer invocations)
  //          and vfsCreateWriteStream (returns a WritableStream that
  //          drives the same primitives internally).
  //
  // The handle-based primitives are the load-bearing surface — the
  // stream wrappers are convenience built on top. Both share the
  // commit-rename atomicity protocol.

  /** openReadStream — open a read handle. Caller pumps via vfsPullReadStream. */
  async vfsOpenReadStream(
    scope: VFSScope,
    path: string
  ): Promise<VFSReadHandle> {
    this.gateVfs(scope);
    return vfsOpenReadStream(this, scope, path);
  }

  /** pullReadStream — fetch one chunk from an open read handle. Optional byte range within the chunk. */
  async vfsPullReadStream(
    scope: VFSScope,
    handle: VFSReadHandle,
    chunkIndex: number,
    range?: { start?: number; end?: number }
  ): Promise<Uint8Array> {
    this.gateVfs(scope);
    return vfsPullReadStream(this, scope, handle, chunkIndex, range);
  }

  /** createReadStream — return a ReadableStream pulling chunks lazily. Optional byte-range over the file. */
  async vfsCreateReadStream(
    scope: VFSScope,
    path: string,
    range?: { start?: number; end?: number }
  ): Promise<ReadableStream<Uint8Array>> {
    this.gateVfs(scope);
    return vfsCreateReadStream(this, scope, path, range);
  }

  /** beginWriteStream — open a write handle. Caller pumps via vfsAppendWriteStream then commits. */
  async vfsBeginWriteStream(
    scope: VFSScope,
    path: string,
    opts?: VFSWriteFileOpts
  ): Promise<VFSWriteHandle> {
    this.gateVfsWrite(scope);
    const handle = vfsBeginWriteStream(this, scope, path, opts);
    // H1: schedule sweeper after the tmp row is in place. If the
    // caller never sends a commit / abort the alarm reclaims after
    // 1h. setAlarm is awaited but the latency is hidden behind the
    // existing await at the call site.
    await this.ensureStaleSweepScheduled();
    return handle;
  }

  /** appendWriteStream — push one chunk. chunkIndex must be sequential. Returns cumulative bytes. */
  async vfsAppendWriteStream(
    scope: VFSScope,
    handle: VFSWriteHandle,
    chunkIndex: number,
    data: Uint8Array
  ): Promise<{ bytesWritten: number }> {
    // Append doesn't insert into `files`; it INSERTs into file_chunks
    // and the tmp row already exists. The H6 EBUSY guard sits on
    // begin/commit (the pair that establishes new (parent, name)
    // claims). Append rate-limits and audits scope but skips the
    // index check.
    this.gateVfs(scope);
    return vfsAppendWriteStream(this, scope, handle, chunkIndex, data);
  }

  /** commitWriteStream — atomic supersede + rename (protocol). */
  async vfsCommitWriteStream(
    scope: VFSScope,
    handle: VFSWriteHandle
  ): Promise<void> {
    this.gateVfsWrite(scope);
    return vfsCommitWriteStream(this, scope, handle);
  }

  /** abortWriteStream — drop the tmp row + queue chunk GC. Idempotent. */
  async vfsAbortWriteStream(
    scope: VFSScope,
    handle: VFSWriteHandle
  ): Promise<void> {
    this.gateVfs(scope);
    return vfsAbortWriteStream(this, scope, handle);
  }

  /**
   * createWriteStream — return a WritableStream backed by the handle
   * primitives. Returns the wrapper { stream, handle } so callers that
   * need to surface the handle (for resumability or progress tracking)
   * can grab it.
   */
  async vfsCreateWriteStream(
    scope: VFSScope,
    path: string,
    opts?: VFSWriteFileOpts
  ): Promise<{ stream: WritableStream<Uint8Array>; handle: VFSWriteHandle }> {
    this.gateVfsWrite(scope);
    return vfsCreateWriteStream(this, scope, path, opts);
  }

  // ── multipart parallel transfer engine ─────────────────────
  //
  // Three RPCs forming the upload session boundary. Per-chunk PUTs do
  // NOT touch UserDO — they validate the session token in the route
  // handler (CPU-only, HMAC verify) and call ShardDO directly. This
  // is the load-bearing constraint that lets multipart saturate user
  // bandwidth without bottlenecking on UserDO single-thread.
  //
  // - vfsBeginMultipart: mints session, inserts tmp row + session row,
  //   returns HMAC token. Resume mode probes shards for landed[].
  // - vfsAbortMultipart: flips status, fans out chunk-ref drops + staging
  //   clears across the pool, hard-deletes tmp row.
  // - vfsFinalizeMultipart: verifies completeness, batch-inserts
  //   file_chunks, atomic supersede via commitRename.
  // - vfsGetMultipartStatus: read landed[] for resume / progress.
  //
  // See worker/core/objects/user/multipart-upload.ts for implementation
  // details; this file just wires the RPCs to gates.

  async vfsBeginMultipart(
    scope: VFSScope,
    path: string,
    opts: import("./multipart-upload").VFSBeginMultipartOpts
  ): Promise<import("../../../../shared/multipart").MultipartBeginResponse> {
    this.gateVfsWrite(scope);
    const { vfsBeginMultipart } = await import("./multipart-upload");
    const r = await vfsBeginMultipart(this, scope, path, opts);
    // Schedule the orphan-session sweep alarm (re-uses the existing
    // stale-write alarm). Idempotent if already scheduled.
    await this.ensureStaleSweepScheduled();
    return r;
  }

  async vfsAbortMultipart(
    scope: VFSScope,
    uploadId: string
  ): Promise<{ ok: true }> {
    this.gateVfs(scope);
    const { vfsAbortMultipart } = await import("./multipart-upload");
    return vfsAbortMultipart(this, scope, uploadId);
  }

  async vfsFinalizeMultipart(
    scope: VFSScope,
    uploadId: string,
    chunkHashList: readonly string[]
  ): Promise<import("../../../../shared/multipart").MultipartFinalizeResponse> {
    this.gateVfsWrite(scope);
    const { vfsFinalizeMultipart } = await import("./multipart-upload");
    return vfsFinalizeMultipart(this, scope, uploadId, chunkHashList);
  }

  async vfsGetMultipartStatus(
    scope: VFSScope,
    uploadId: string
  ): Promise<{
    landed: number[];
    total: number;
    bytesUploaded: number;
    expiresAtMs: number;
    status: string;
  }> {
    this.gateVfs(scope);
    const { vfsGetMultipartStatus } = await import("./multipart-upload");
    return vfsGetMultipartStatus(this, scope, uploadId);
  }

  // ── file-level versioning RPCs ───────────────────────────────
  //
  // Opt-in per tenant via `adminSetVersioning(tenant, enabled)`.
  // Subsequent writeFile/unlink calls insert file_versions rows;
  // readFile resolves the head version (or an explicit version_id).
  // Refcount-per-version is enforced via synthetic shard ref keys
  // `${pathId}#${versionId}`. The alarm sweeper reaps chunks
  // whose last reference was dropped.

  /** Newest-first list of versions for a path. ENOENT if path doesn't exist. */
  async vfsListVersions(
    scope: VFSScope,
    path: string,
    opts?: {
      limit?: number;
      userVisibleOnly?: boolean;
      includeMetadata?: boolean;
    }
  ): Promise<VersionRow[]> {
    this.gateVfs(scope);
    const userId = scope.sub
      ? `${scope.tenant}::${scope.sub}`
      : scope.tenant;
    const pathId = resolvePathId(this, userId, path);
    if (!pathId) {
      // Match the rest of the API: path-not-found surfaces as ENOENT
      // through mapServerError on the consumer side. We throw the
      // server-side VFSError shape directly here.
      const { VFSError } = await import("../../../../shared/vfs-types");
      throw new VFSError("ENOENT", `listVersions: path not found: ${path}`);
    }
    return listVersions(this, pathId, opts);
  }

  /**
   * mark a version's label and/or user-visible flag.
   * `userVisible:false` is rejected EINVAL — the bit is monotonic.
   */
  async vfsMarkVersion(
    scope: VFSScope,
    path: string,
    versionId: string,
    opts: { label?: string; userVisible?: boolean }
  ): Promise<void> {
    this.gateVfsWrite(scope);
    const userId = scope.sub
      ? `${scope.tenant}::${scope.sub}`
      : scope.tenant;
    const pathId = resolvePathId(this, userId, path);
    if (!pathId) {
      const { VFSError } = await import("../../../../shared/vfs-types");
      throw new VFSError("ENOENT", `markVersion: path not found: ${path}`);
    }
    if (opts.label !== undefined) {
      const { validateLabel } = await import("../../../../shared/metadata-validate");
      validateLabel(opts.label);
    }
    const { markVersion } = await import("./vfs-versions");
    markVersion(this, pathId, versionId, opts);
  }

  /**
   * explicit flush of a yjs-mode file. Triggers a Yjs
   * compaction whose checkpoint emits a user-visible version row
   * (when versioning is enabled for the tenant) and an optional
   * label. Returns the new version_id (or null if versioning is
   * off for the tenant — the checkpoint still happens, just
   * without a Mossaic version row).
   */
  async vfsFlushYjs(
    scope: VFSScope,
    path: string,
    opts?: { label?: string }
  ): Promise<{ versionId: string | null; checkpointSeq: number }> {
    this.gateVfsWrite(scope);
    if (opts?.label !== undefined) {
      const { validateLabel } = await import("../../../../shared/metadata-validate");
      validateLabel(opts.label);
    }
    const userId = scope.sub
      ? `${scope.tenant}::${scope.sub}`
      : scope.tenant;
    const { resolvePathFollow } = await import("./path-walk");
    const r = resolvePathFollow(this, userId, path);
    if (r.kind !== "file") {
      const { VFSError } = await import("../../../../shared/vfs-types");
      throw new VFSError(
        "EINVAL",
        `flushYjs: not a regular file: ${path}`
      );
    }
    const { isYjsMode } = await import("./vfs-ops");
    if (!isYjsMode(this, userId, r.leafId)) {
      const { VFSError } = await import("../../../../shared/vfs-types");
      throw new VFSError(
        "EINVAL",
        `flushYjs: file is not in yjs mode: ${path}`
      );
    }
    const poolRow = this.sql
      .exec("SELECT pool_size FROM quota WHERE user_id = ?", userId)
      .toArray()[0] as { pool_size: number } | undefined;
    const poolSize = poolRow ? poolRow.pool_size : 32;
    const result = await (await this.getYjsRuntime()).compact(
      scope,
      userId,
      r.leafId,
      poolSize,
      { userVisible: true, label: opts?.label }
    );
    return {
      versionId: result.versionId ?? null,
      checkpointSeq: result.checkpointSeq,
    };
  }

  /**
   * client-driven compaction for encrypted Yjs files.
   *
   * The server CANNOT decrypt the oplog, so the client builds the
   * checkpoint locally (decrypt all ops → apply → encode state →
   * encrypt) and submits it via this RPC. CAS-on-`next_seq` ensures
   * exactly-one-wins between concurrent compactors / writers.
   *
   * Throws `EBUSY` on CAS failure — caller retries against the new
   * tip.
   */
  async vfsCompactEncryptedYjs(
    scope: VFSScope,
    path: string,
    checkpointEnvelope: Uint8Array,
    expectedNextSeq: number
  ): Promise<{ checkpointSeq: number; opsReaped: number }> {
    this.gateVfsWrite(scope);
    const userId = scope.sub
      ? `${scope.tenant}::${scope.sub}`
      : scope.tenant;
    const { resolvePathFollow } = await import("./path-walk");
    const r = resolvePathFollow(this, userId, path);
    if (r.kind !== "file") {
      const { VFSError } = await import("../../../../shared/vfs-types");
      throw new VFSError(
        "EINVAL",
        `compactEncryptedYjs: not a regular file: ${path}`
      );
    }
    const { isYjsMode } = await import("./vfs-ops");
    if (!isYjsMode(this, userId, r.leafId)) {
      const { VFSError } = await import("../../../../shared/vfs-types");
      throw new VFSError(
        "EINVAL",
        `compactEncryptedYjs: file is not in yjs mode: ${path}`
      );
    }
    const poolRow = this.sql
      .exec("SELECT pool_size FROM quota WHERE user_id = ?", userId)
      .toArray()[0] as { pool_size: number } | undefined;
    const poolSize = poolRow ? poolRow.pool_size : 32;
    return await (await this.getYjsRuntime()).compactEncryptedYjs(
      scope,
      userId,
      r.leafId,
      poolSize,
      checkpointEnvelope,
      expectedNextSeq
    );
  }

  /**
   * read raw oplog rows (envelope bytes) for a yjs-mode
   * file. Used by the client-side compactor: it fetches all ops
   * since `last_checkpoint_seq`, decrypts them, and rebuilds the
   * checkpoint locally.
   *
   * Returns rows ordered by seq ASC. Caller may stream-read for
   * very large oplogs (the server caps at 1000 rows per call —
   * pagination via `afterSeq` cursor).
   */
  async vfsReadYjsOplog(
    scope: VFSScope,
    path: string,
    opts?: { afterSeq?: number; limit?: number }
  ): Promise<{
    rows: { seq: number; kind: "op" | "checkpoint"; envelope: Uint8Array }[];
    nextSeq: number;
    hasMore: boolean;
  }> {
    this.gateVfs(scope);
    const userId = scope.sub
      ? `${scope.tenant}::${scope.sub}`
      : scope.tenant;
    const { resolvePathFollow } = await import("./path-walk");
    const r = resolvePathFollow(this, userId, path);
    if (r.kind !== "file") {
      const { VFSError } = await import("../../../../shared/vfs-types");
      throw new VFSError(
        "EINVAL",
        `readYjsOplog: not a regular file: ${path}`
      );
    }
    const { isYjsMode } = await import("./vfs-ops");
    if (!isYjsMode(this, userId, r.leafId)) {
      const { VFSError } = await import("../../../../shared/vfs-types");
      throw new VFSError(
        "EINVAL",
        `readYjsOplog: file is not in yjs mode: ${path}`
      );
    }
    const limit = Math.min(opts?.limit ?? 1000, 1000);
    const afterSeq = opts?.afterSeq ?? -1;
    const oprows = this.sql
      .exec(
        `SELECT seq, kind, chunk_hash, shard_index
           FROM yjs_oplog WHERE path_id = ? AND seq > ?
          ORDER BY seq ASC LIMIT ?`,
        r.leafId,
        afterSeq,
        limit + 1
      )
      .toArray() as {
      seq: number;
      kind: string;
      chunk_hash: string;
      shard_index: number;
    }[];
    const hasMore = oprows.length > limit;
    if (hasMore) oprows.pop();
    // Resolve each row to its envelope bytes via the ShardDO.
    const env = this.envPublic;
    const shardNs = env.MOSSAIC_SHARD as unknown as DurableObjectNamespace;
    const rows: {
      seq: number;
      kind: "op" | "checkpoint";
      envelope: Uint8Array;
    }[] = [];
    for (const row of oprows) {
      const shardName = vfsShardDOName(scope.ns, scope.tenant, scope.sub, row.shard_index);
      const stub = shardNs.get(shardNs.idFromName(shardName));
      // Read via the HTTP chunk endpoint. The ShardDO's GET /chunk/:hash
      // route serves the raw bytes (which are envelopes for encrypted
      // yjs files). No userId / refId needed for read — content-addressed.
      const resp = await stub.fetch(
        `http://internal/chunk/${encodeURIComponent(row.chunk_hash)}`,
        { method: "GET" }
      );
      if (!resp.ok) {
        const { VFSError } = await import("../../../../shared/vfs-types");
        throw new VFSError(
          "ENOENT",
          `readYjsOplog: chunk ${row.chunk_hash} not on shard (status ${resp.status})`
        );
      }
      const bytes = await resp.arrayBuffer();
      rows.push({
        seq: row.seq,
        kind: row.kind as "op" | "checkpoint",
        envelope: new Uint8Array(bytes),
      });
    }
    const nextSeq =
      rows.length > 0 ? rows[rows.length - 1]!.seq : afterSeq;
    return { rows, nextSeq, hasMore };
  }

  /**
   * Restore a historical version: creates a NEW version row whose
   * content matches the source. Source must not be a tombstone.
   */
  async vfsRestoreVersion(
    scope: VFSScope,
    path: string,
    sourceVersionId: string
  ): Promise<{ versionId: string }> {
    this.gateVfsWrite(scope);
    const userId = scope.sub
      ? `${scope.tenant}::${scope.sub}`
      : scope.tenant;
    const pathId = resolvePathId(this, userId, path);
    if (!pathId) {
      const { VFSError } = await import("../../../../shared/vfs-types");
      throw new VFSError(
        "ENOENT",
        `restoreVersion: path not found: ${path}`
      );
    }
    return restoreVersion(this, scope, userId, pathId, sourceVersionId);
  }

  /**
   * Drop versions per a retention policy. Head version is always
   * preserved (S3 invariant). Returns counts. Chunks whose last
   * version reference was dropped are reaped by the alarm
   * sweeper after its 30s grace.
   */
  async vfsDropVersions(
    scope: VFSScope,
    path: string,
    policy: {
      olderThan?: number;
      keepLast?: number;
      exceptVersions?: string[];
    }
  ): Promise<{ dropped: number; kept: number }> {
    this.gateVfs(scope);
    const userId = scope.sub
      ? `${scope.tenant}::${scope.sub}`
      : scope.tenant;
    const pathId = resolvePathId(this, userId, path);
    if (!pathId) {
      const { VFSError } = await import("../../../../shared/vfs-types");
      throw new VFSError(
        "ENOENT",
        `dropVersions: path not found: ${path}`
      );
    }
    return dropVersions(this, scope, userId, pathId, policy);
  }

  /**
   * Operator-only: toggle versioning for a tenant. Affects only
   * future writes; existing files / versions are unchanged. Pass
   * `userId` directly (matches admin convention; not scope-derived
   * because the caller may not have a token-scoped session).
   */
  async adminSetVersioning(
    userId: string,
    enabled: boolean
  ): Promise<{ enabled: boolean }> {
    this.ensureInit();
    setVersioningEnabled(this, userId, enabled);
    return { enabled };
  }

  /** Operator-only: read the versioning flag for a tenant. */
  async adminGetVersioning(userId: string): Promise<{ enabled: boolean }> {
    this.ensureInit();
    return { enabled: isVersioningEnabled(this, userId) };
  }

  // ── admin tooling ────────────────────────────────────────────
  //
  // Operator-only RPC. Not exposed through public /api/* routes and
  // not surfaced on the SDK's VFS class. Holders of the binding can
  // call it directly via `stub.adminDedupePaths(userId, scope)` when
  // migrating data that pre-dates the UNIQUE partial index.

  /**
   * Resolve duplicate (parent_id, name) rows for a user. Returns counts
   * + index status. See worker/objects/user/admin.ts for the algorithm
   * and atomicity properties.
   */
  async adminDedupePaths(
    userId: string,
    scope: VFSScope
  ): Promise<DedupeResult> {
    this.ensureInit();
    return dedupePaths(this, userId, scope);
  }

  /**
   * Phase 25 — recovery primitive for tombstoned-head rows.
   *
   * Scans `files` rows whose `head_version_id` points at a
   * `deleted=1` `file_versions` row and either drops them
   * (`mode: "hardDelete"`, default for cleanup) or repoints head
   * at the newest live predecessor (`mode: "walkBack"`, for
   * recovery from accidental unlinks). Defaults to `dryRun: true`
   * — pass `dryRun: false` explicitly to write.
   *
   * Idempotent. Safe to re-run after partial completion.
   */
  async adminReapTombstonedHeads(
    userId: string,
    scope: VFSScope,
    opts: { mode: "hardDelete" | "walkBack"; dryRun?: boolean; limit?: number }
  ): Promise<{
    scanned: number;
    hardDeleted: number;
    walkedBack: number;
    samplePathIds: string[];
    dryRun: boolean;
  }> {
    this.ensureInit();
    const { reapTombstonedHeads } = await import("./admin-tombstones");
    return reapTombstonedHeads(this, userId, scope, opts);
  }

  /**
   * Pre-generate standard preview variants (`thumb`, `medium`,
   * `lightbox`) for a freshly-finalized file. Intended to run
   * inside the route layer's `c.executionCtx.waitUntil(...)` so
   * pre-gen latency doesn't extend the finalize response.
   *
   * Best-effort: per-variant failures are logged and swallowed.
   * Skips empty + encrypted files. Idempotent (content-addressed).
   */
  async adminPreGenerateStandardVariants(
    scope: VFSScope,
    args: {
      fileId: string;
      path: string;
      mimeType: string;
      fileName: string;
      fileSize: number;
      isEncrypted: boolean;
      /**
       * Phase 28 Fix 1 — head_version_id at finalize time. Optional
       * for backward compat with route callers that haven't been
       * updated; resolved from the `files` row when omitted.
       */
      headVersionId?: string | null;
    }
  ): Promise<void> {
    this.ensureInit();
    // Resolve head_version_id from `files` when the caller didn't
    // pass it. Pre-generated variants stamp this value so a
    // subsequent write that flips the head invalidates them
    // (Phase 28 Fix 1).
    let headVersionId: string | null = args.headVersionId ?? null;
    if (args.headVersionId === undefined) {
      const row = this.sql
        .exec(
          "SELECT head_version_id FROM files WHERE file_id = ?",
          args.fileId
        )
        .toArray()[0] as { head_version_id: string | null } | undefined;
      headVersionId = row?.head_version_id ?? null;
    }
    const { preGenerateStandardVariants } = await import(
      "./preview-variants"
    );
    await preGenerateStandardVariants(this, scope, {
      ...args,
      headVersionId,
    });
  }

  // ── metadata + tags primitives ──────────────────────────────

  /**
   * Deep-merge a metadata patch into the path's metadata blob,
   * optionally adding/removing tags atomically. See
   * `vfsPatchMetadata` in vfs-ops.ts for full semantics.
   */
  async vfsPatchMetadata(
    scope: VFSScope,
    path: string,
    patch: Record<string, unknown> | null,
    opts?: { addTags?: readonly string[]; removeTags?: readonly string[] }
  ): Promise<void> {
    this.gateVfsWrite(scope);
    const { vfsPatchMetadata } = await import("./vfs-ops");
    return vfsPatchMetadata(this, scope, path, patch, opts);
  }

  /**
   * same-tenant copyFile. Manifest-only copy for chunked +
   * versioned tiers; bytes-only copy for inline tier; bytes-snapshot
   * fork for yjs-mode src. See `copy-file.ts` for the refcount and
   * atomicity contracts.
   */
  async vfsCopyFile(
    scope: VFSScope,
    src: string,
    dest: string,
    opts?: {
      metadata?: Record<string, unknown> | null;
      tags?: readonly string[];
      version?: { label?: string; userVisible?: boolean };
      overwrite?: boolean;
    }
  ): Promise<void> {
    this.gateVfsWrite(scope);
    const { vfsCopyFile } = await import("./copy-file");
    return vfsCopyFile(this, scope, src, dest, opts);
  }

  /**
   * indexed listFiles. Drives an HMAC-signed cursor for
   * stable pagination. Tag intersection capped at 8 tags/query.
   * See `list-files.ts` for index selection and cursor semantics.
   */
  async vfsListFiles(
    scope: VFSScope,
    opts?: {
      prefix?: string;
      tags?: readonly string[];
      metadata?: Record<string, unknown>;
      limit?: number;
      cursor?: string;
      orderBy?: "mtime" | "name" | "size";
      direction?: "asc" | "desc";
      includeStat?: boolean;
      includeMetadata?: boolean;
      includeTombstones?: boolean;
      includeArchived?: boolean;
    }
  ): Promise<{
    items: import("./list-files").ListFilesItemRaw[];
    cursor?: string;
  }> {
    this.gateVfs(scope);
    const { vfsListFiles } = await import("./list-files");
    return vfsListFiles(this, scope, opts);
  }

  async vfsFileInfo(
    scope: VFSScope,
    path: string,
    opts?: {
      includeStat?: boolean;
      includeMetadata?: boolean;
      includeTombstones?: boolean;
      includeArchived?: boolean;
    }
  ): Promise<import("./list-files").ListFilesItemRaw> {
    this.gateVfs(scope);
    const { vfsFileInfo } = await import("./list-files");
    return vfsFileInfo(this, scope, path, opts);
  }

  // ── yjs-mode primitives ─────────────────────────────────────

  /**
   * Toggle the per-file `mode_yjs` bit. Currently only 0 → 1 is
   * permitted (downgrade is rejected to avoid losing CRDT history).
   * Path must point to an existing regular file. See vfs-ops.ts for
   * full semantics.
   */
  async vfsSetYjsMode(
    scope: VFSScope,
    path: string,
    enabled: boolean
  ): Promise<void> {
    this.gateVfsWrite(scope);
    const { vfsSetYjsMode } = await import("./vfs-ops");
    vfsSetYjsMode(this, scope, path, enabled);
  }

  /**
   * Phase 38 — return the full `Y.encodeStateAsUpdate(doc)` bytes
   * for a yjs-mode file so SDK consumers can decode arbitrary
   * named shared types (`Y.XmlFragment`, `Y.Map`, `Y.Array`,
   * multiple `Y.Text`s — Tiptap/ProseMirror, Notion-style block
   * editors).
   *
   * Pairs with the SDK's `vfs.readYjsSnapshot(path)`. The path
   * MUST be a yjs-mode file; non-yjs paths (mode_yjs=0) throw
   * EINVAL because the bytes wouldn't parse via `Y.applyUpdate`.
   *
   * Encryption-aware: encrypted yjs files have NO server-side
   * materialised doc (the server doesn't hold the key); this RPC
   * therefore throws EACCES. Encrypted-tenant consumers should
   * round-trip via `openYDoc` + decrypted op-log replay.
   */
  async vfsReadYjsSnapshot(
    scope: VFSScope,
    path: string
  ): Promise<Uint8Array> {
    this.gateVfs(scope);
    const { isYjsMode } = await import("./vfs-ops");
    const { resolvePathFollow } = await import("./path-walk");
    const userId =
      scope.sub !== undefined
        ? `${scope.tenant}::${scope.sub}`
        : scope.tenant;
    const r = resolvePathFollow(this, userId, path);
    // Phase 38 sub-agent (a) finding — distinguish ENOENT /
    // ENOTDIR / ELOOP / EISDIR / EINVAL on path resolution. Pre-fix
    // every non-"file" kind collapsed to EINVAL with the misleading
    // message "not a regular file", breaking the standard fs-style
    // error contract a Tiptap consumer expects.
    if (r.kind === "ENOENT") {
      throw new VFSError(
        "ENOENT",
        `readYjsSnapshot: path not found: ${path}`
      );
    }
    if (r.kind === "ENOTDIR") {
      throw new VFSError(
        "ENOTDIR",
        `readYjsSnapshot: path component is not a directory: ${path}`
      );
    }
    if (r.kind === "ELOOP") {
      throw new VFSError(
        "ELOOP",
        `readYjsSnapshot: too many symbolic links: ${path}`
      );
    }
    if (r.kind === "dir") {
      throw new VFSError(
        "EISDIR",
        `readYjsSnapshot: path is a directory: ${path}`
      );
    }
    if (r.kind !== "file") {
      throw new VFSError(
        "EINVAL",
        `readYjsSnapshot: not a regular file: ${path}`
      );
    }
    if (!isYjsMode(this, userId, r.leafId)) {
      throw new VFSError(
        "EINVAL",
        `readYjsSnapshot: path is not in yjs-mode: ${path}`
      );
    }
    // Encryption-aware: server cannot materialise an encrypted
    // doc. Surface as EACCES so the SDK can fall back to a
    // client-side `openYDoc` + state-vector dance.
    const encRow = this.sql
      .exec(
        "SELECT encryption_mode FROM files WHERE file_id=? AND user_id=?",
        r.leafId,
        userId
      )
      .toArray()[0] as { encryption_mode: string | null } | undefined;
    if (encRow?.encryption_mode != null) {
      throw new VFSError(
        "EACCES",
        `readYjsSnapshot: encrypted yjs files cannot be materialised server-side; use openYDoc instead: ${path}`
      );
    }
    const { readYjsSnapshotBytes } = await import("./yjs");
    return readYjsSnapshotBytes(this, scope, r.leafId);
  }

  /**
   * Open a Yjs WebSocket session against `path`. The path MUST be a
   * yjs-mode file. The returned Response carries the client side
   * of a WebSocketPair (status 101); the server side is accepted
   * via the Hibernation API (`ctx.acceptWebSocket`) so idle
   * connections cost $0.
   *
   * Per-socket state (scope, userId, pathId, poolSize) is stashed
   * via `ws.serializeAttachment` so the hibernation handlers can
   * reconstitute it without an in-memory map (which would not
   * survive eviction).
   */
  async vfsOpenYjsSocket(
    scope: VFSScope,
    path: string
  ): Promise<Response> {
    this.gateVfs(scope);
    const { isYjsMode } = await import("./vfs-ops");
    const { resolvePathFollow } = await import("./path-walk");
    // Resolve the path → pathId. Use the same tenant-scoped userId
    // as the rest of vfs-ops; reject anything that isn't a yjs-mode
    // regular file BEFORE we burn an upgrade.
    const userId = ((): string => {
      if (scope.sub !== undefined) return `${scope.tenant}::${scope.sub}`;
      return scope.tenant;
    })();
    const r = resolvePathFollow(this, userId, path);
    if (r.kind !== "file") {
      throw new VFSError(
        "EINVAL",
        `openYjsSocket: not a regular file: ${path}`
      );
    }
    if (!isYjsMode(this, userId, r.leafId)) {
      throw new VFSError(
        "EINVAL",
        `openYjsSocket: file is not in yjs mode: ${path}`
      );
    }
    // Phase 25 Fix 12 — refuse the WS upgrade for a tombstoned-head
    // file. Pre-fix, a yjs-mode path that had been `unlink`ed under
    // versioning-on still accepted incoming WS connections; clients
    // could read AND WRITE into a path the SDK reported as
    // gone. Now an explicit head-tombstone shortcuts to ENOENT,
    // matching `vfsStat` / `vfsReadFile`.
    const yjsHead = this.sql
      .exec(
        `SELECT f.head_version_id, fv.deleted AS head_deleted
           FROM files f
           LEFT JOIN file_versions fv
             ON fv.path_id = f.file_id AND fv.version_id = f.head_version_id
          WHERE f.file_id = ? AND f.user_id = ?`,
        r.leafId,
        userId
      )
      .toArray()[0] as
      | { head_version_id: string | null; head_deleted: number | null }
      | undefined;
    if (
      yjsHead !== undefined &&
      yjsHead.head_version_id !== null &&
      yjsHead.head_deleted === 1
    ) {
      throw new VFSError(
        "ENOENT",
        `openYjsSocket: head version is a tombstone for ${path}`
      );
    }

    // Look up the per-tenant pool size now so we don't have to
    // re-query on every socket message.
    const poolRow = this.sql
      .exec("SELECT pool_size FROM quota WHERE user_id = ?", userId)
      .toArray()[0] as { pool_size: number } | undefined;
    const poolSize = poolRow ? poolRow.pool_size : 32;

    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];

    // Tag with the pathId so we can rebuild the in-memory `sockets`
    // Map after a hibernation cycle via ctx.getWebSockets(pathId).
    this.ctx.acceptWebSocket(server, [r.leafId]);
    server.serializeAttachment({
      scope,
      userId,
      pathId: r.leafId,
      poolSize,
    });
    (await this.getYjsRuntime()).registerSocket(r.leafId, server);

    return new Response(null, { status: 101, webSocket: client });
  }

  /**
   * Hibernation API hook. Called by the runtime for each incoming
   * frame on an accepted WebSocket. The DO does NOT need to be in
   * memory between frames — workerd will instantiate, dispatch,
   * then evict. Idle WebSockets cost $0.
   *
    * Design notes (after surveying @cloudflare/agents + capnweb):
   *
   * - Yjs sync-protocol frames are BINARY (Uint8Array). agents-sdk's
   *   `@callable` JSON-RPC pattern only carries text frames; capnweb
   *   serializes Uint8Array as base64 strings inside a JSON envelope
   *   (~33% size penalty + per-frame CPU). Both are non-starters for
   *   the hot path. We keep the hand-rolled 1-byte-tag + payload
   *   framing — `decodeYjsMessage` in yjs.ts.
   *
   * - The single useful idiom we adopt from agents-sdk is the
   *   "ensure rehydrated" pattern: at the top of every hibernation
   *   handler, read `ws.deserializeAttachment()` (which DOES survive
   *   eviction) and re-populate the in-memory `YjsRuntime.sockets`
   *   set via `registerSocket` (idempotent — `Set.add` is a no-op on
   *   the second call). The runtime's `docs` Map is rebuilt lazily
   *   on the next `getDoc` call against this pathId.
   *
   * - Why we don't need a separate JSON control-plane envelope: the
   *   only "control" call clients make is `vfsOpenYjsSocket` itself,
   *   which is already a typed Cloudflare DO RPC method (no extra
   *   wire format). Once the WS is open, every frame is Yjs.
   */
  async webSocketMessage(
    ws: WebSocket,
    message: string | ArrayBuffer
  ): Promise<void> {
    if (typeof message === "string") {
      // We never send text frames; ignore.
      return;
    }
    const att = ws.deserializeAttachment() as {
      scope: VFSScope;
      userId: string;
      pathId: string;
      poolSize: number;
    } | null;
    if (!att) {
      // No attachment — socket from a different protocol. Drop it.
      ws.close(1011, "missing yjs attachment");
      return;
    }

    // Re-register the socket in the live map (no-op if already
    // present; idempotent set add). Cheap and keeps broadcast paths
    // correct after wake.
    (await this.getYjsRuntime()).registerSocket(att.pathId, ws);

    const bytes = new Uint8Array(message);
    const { decodeYjsMessage, encodeSyncStep2 } = await import("./yjs");
    const decoded = decodeYjsMessage(bytes);

    try {
      switch (decoded.kind) {
        case "syncStep1": {
          // encrypted yjs files cannot be materialised
          // server-side (the oplog rows are AES-GCM envelopes the
          // server cannot decrypt). Send an empty sync_step_2 so the
          // client unblocks its `await synced` and the doc starts
          // empty; connected peers will broadcast their updates via
          // the relay path. For new encrypted yjs files this is
          // correct (no prior state). For files with prior state, a
          // peer that has the master key must be connected for
          // bootstrap — otherwise the doc starts blank.
          const { isPathEncryptedYjs } = await import("./yjs");
          if (isPathEncryptedYjs(this, att.pathId)) {
            ws.send(encodeSyncStep2(new Uint8Array(0)));
            return;
          }
          // Plaintext path — original behaviour.
          const Y = await import("yjs");
          const doc = await (await this.getYjsRuntime()).getDoc(att.scope, att.pathId);
          const diff = Y.encodeStateAsUpdate(doc, decoded.stateVector);
          ws.send(encodeSyncStep2(diff));
          // Also send our state vector so they reciprocate (the
          // standard Yjs sync handshake is symmetric).
          const reply = await (await this.getYjsRuntime()).syncStep1Reply(
            att.scope,
            att.pathId
          );
          ws.send(reply);
          return;
        }
        case "syncStep2": {
          await (await this.getYjsRuntime()).applyRemoteUpdate(
            att.scope,
            att.userId,
            att.pathId,
            att.poolSize,
            decoded.diff,
            ws
          );
          return;
        }
        case "update": {
          await (await this.getYjsRuntime()).applyRemoteUpdate(
            att.scope,
            att.userId,
            att.pathId,
            att.poolSize,
            decoded.update,
            ws
          );
          return;
        }
        case "awareness": {
          // relay awareness frames; never persisted.
          await (await this.getYjsRuntime()).relayAwareness(
            att.scope,
            att.pathId,
            decoded.update,
            ws
          );
          return;
        }
        case "unknown":
        default: {
          // Unknown tag — ignore for forward compat.
          return;
        }
      }
    } catch (err) {
      // Don't crash the handler — close with the error reason so
      // the client knows to retry.
      try {
        ws.close(1011, err instanceof Error ? err.message : "internal error");
      } catch {
        /* already closed */
      }
    }
  }

  /**
   * Hibernation API hook: called when a peer closes the socket OR
   * when workerd drops it. Drop our in-memory tracking; SQL state
   * is unaffected.
   */
  async webSocketClose(
    ws: WebSocket,
    _code: number,
    _reason: string,
    _wasClean: boolean
  ): Promise<void> {
    const att = ws.deserializeAttachment() as
      | { pathId: string }
      | null;
    if (att) (await this.getYjsRuntime()).removeSocket(att.pathId, ws);
  }

  /**
   * Hibernation API hook: error path mirrors close. We don't try to
   * recover the connection — clients reconnect on their own.
   */
  async webSocketError(ws: WebSocket, _err: unknown): Promise<void> {
    const att = ws.deserializeAttachment() as
      | { pathId: string }
      | null;
    if (att) (await this.getYjsRuntime()).removeSocket(att.pathId, ws);
  }
}
