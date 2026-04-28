import { DurableObject } from "cloudflare:workers";
import type { Env } from "@shared/types";
import {
  hardDeleteFileRowExternal,
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
  vfsReadlink,
  vfsReadManyStat,
  vfsReaddir,
  vfsRemoveRecursive,
  vfsRename,
  vfsRmdir,
  vfsStat,
  vfsSymlink,
  vfsUnlink,
  vfsWriteFile,
  type VFSReadHandle,
  type VFSWriteHandle,
} from "./vfs-ops";
import type {
  OpenManifestResult,
  VFSScope,
  VFSStatRaw,
} from "@shared/vfs-types";
import { VFSError } from "@shared/vfs-types";
import { dedupePaths, type DedupeResult } from "./admin";
import { YjsRuntime } from "./yjs";
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
   * class. Phase 2 vfs-ops needs to dispatch ShardDO subrequests by
   * binding name; without this alias TS rejects external access. The
   * base class's `env` remains protected; we shadow it.
   */
  envPublic: Env;
  /**
   * Phase 10: per-DO YjsRuntime cache. Lazily constructed on first
   * access so the import isn't evaluated for tenants that never use
   * yjs-mode files. Holds the in-memory `Y.Doc` cache + the live
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

  /** Phase 10: lazy YjsRuntime accessor. */
  get yjsRuntime(): YjsRuntime {
    if (this._yjsRuntime === undefined) {
      this._yjsRuntime = new YjsRuntime(this);
    }
    return this._yjsRuntime;
  }

  /**
   * Phase 11: `protected` so the App subclass (`UserDO` in
   * worker/app) can call `this.ensureInit()` from its own
   * `_legacyFetch` handler without the schema migration silently
   * being skipped on the legacy /signup path. Body is unchanged
   * from Phase 10.
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
    // swallowed; the admin dedupe route (Phase 6) resolves them later.
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

    // ── Phase 8: per-tenant rate-limit state (token bucket) ──────────────
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

    // ── Phase 9: per-tenant versioning toggle (S3-style, opt-in) ─────────
    // versioning_enabled: NULL/0 = disabled (Phase 8 byte-equivalent
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

    // ── Phase 9: file_versions table ─────────────────────────────────────
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
    // refcount is independent — the alarm sweeper from Phase 3
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

    // ── Phase 10: Yjs per-file mode ──────────────────────────────────────
    //
    // mode_yjs: opt-in per FILE bit. 0 = plain bytes (Phase 1-9
    // behavior); 1 = the file is a Yjs CRDT op log. Storage is a
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
   * Phase 10 + 11: top-level fetch entry. Core's fetch handles the
   * Yjs WebSocket upgrade; every other request returns 404. The
   * App subclass (`UserDO` in `worker/app/objects/user/user-do.ts`)
   * overrides this method to delegate non-WS HTTP traffic to the
   * legacy photo-app handler whose body is byte-pinned.
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
   * Phase 10: WebSocket upgrade entry. Path-encoded params:
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
  // ── VFS RPC surface (Phase 2: read-side) ───────────────────────────────
  //
  // Cloudflare DO RPC: any public async method on the DO class is callable
  // from a holder of the stub via `stub.methodName(args)`. The consumer
  // pays exactly one subrequest per call regardless of internal fan-out.
  // See sdk-impl-plan §5.3 for the full contract; these are the read-side
  // methods that land in Phase 2. Write-side and streaming methods come
  // in Phases 3 and 4.
  //
  // Each method calls ensureInit() so the schema migrations (Phase 1)
  // run before any VFS access on a DO that hasn't seen any legacy
  // /fetch traffic yet.

  /**
   * Phase 8 gate: ensureInit + per-tenant rate-limit check. Every
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
        await hardDeleteFileRowExternal(this, userId, scope, file_id);
      } catch {
        // Best-effort: a transient ShardDO error leaves the row
        // intact; the next alarm fires on the same row. The 1h
        // staleness window is large enough that retry storms are
        // bounded.
      }
    }

    // Reschedule if the batch was capped, otherwise fall through —
    // the next gated VFS call will re-arm via ensureStaleSweepScheduled.
    if (rows.length === 200) {
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
   * Phase 9: pass `opts.versionId` to read a historical version
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

  // ── VFS RPC surface (Phase 3: write-side) ──────────────────────────────
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

  /** writeFile() — atomic, last-writer-wins. Inline tier ≤16KB; chunked otherwise. */
  async vfsWriteFile(
    scope: VFSScope,
    path: string,
    data: Uint8Array,
    opts?: { mode?: number; mimeType?: string }
  ): Promise<void> {
    this.gateVfsWrite(scope);
    return vfsWriteFile(this, scope, path, data, opts);
  }

  /** unlink() — hard-delete file/symlink + dispatch chunk GC. EISDIR for dirs. */
  async vfsUnlink(scope: VFSScope, path: string): Promise<void> {
    this.gateVfsWrite(scope);
    return vfsUnlink(this, scope, path);
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

  // ── Phase 4: streaming + handle-based stream primitives ───────────────
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
  // commit-rename atomicity protocol from Phase 3.

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
    opts?: { mode?: number; mimeType?: string }
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

  /** commitWriteStream — atomic supersede + rename (Phase 3 protocol). */
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
    opts?: { mode?: number; mimeType?: string }
  ): Promise<{ stream: WritableStream<Uint8Array>; handle: VFSWriteHandle }> {
    this.gateVfsWrite(scope);
    return vfsCreateWriteStream(this, scope, path, opts);
  }

  // ── Phase 9: file-level versioning RPCs ───────────────────────────────
  //
  // Opt-in per tenant via `adminSetVersioning(tenant, enabled)`.
  // Subsequent writeFile/unlink calls insert file_versions rows;
  // readFile resolves the head version (or an explicit version_id).
  // Refcount-per-version is enforced via synthetic shard ref keys
  // `${pathId}#${versionId}`. The Phase 3 alarm sweeper reaps chunks
  // whose last reference was dropped.

  /** Newest-first list of versions for a path. ENOENT if path doesn't exist. */
  async vfsListVersions(
    scope: VFSScope,
    path: string,
    opts?: { limit?: number }
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
      const { VFSError } = await import("@shared/vfs-types");
      throw new VFSError("ENOENT", `listVersions: path not found: ${path}`);
    }
    return listVersions(this, pathId, opts);
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
      const { VFSError } = await import("@shared/vfs-types");
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
   * version reference was dropped are reaped by the Phase 3 alarm
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
      const { VFSError } = await import("@shared/vfs-types");
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

  // ── Phase 6: admin tooling ────────────────────────────────────────────
  //
  // Operator-only RPC. Not exposed through the legacy /api/* routes
  // and not surfaced on the SDK's VFS class. Holders of the binding
  // can call it directly via stub.adminDedupePaths(userId, scope?)
  // when migrating legacy data that pre-dates the Phase 1 UNIQUE
  // partial index.

  /**
   * Resolve legacy duplicate (parent_id, name) rows for a user.
   * Returns counts + index status. See worker/objects/user/admin.ts
   * for the algorithm and atomicity properties.
   */
  async adminDedupePaths(
    userId: string,
    scope: VFSScope | null = null
  ): Promise<DedupeResult> {
    this.ensureInit();
    return dedupePaths(this, userId, scope);
  }

  // ── Phase 10: yjs-mode primitives ─────────────────────────────────────

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
    this.yjsRuntime.registerSocket(r.leafId, server);

    return new Response(null, { status: 101, webSocket: client });
  }

  /**
   * Hibernation API hook. Called by the runtime for each incoming
   * frame on an accepted WebSocket. The DO does NOT need to be in
   * memory between frames — workerd will instantiate, dispatch,
   * then evict. Idle WebSockets cost $0.
   *
   * Design notes (after surveying @cloudflare/agents + capnweb for
   * Phase 10):
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
    this.yjsRuntime.registerSocket(att.pathId, ws);

    const bytes = new Uint8Array(message);
    const { decodeYjsMessage, encodeSyncStep2 } = await import("./yjs");
    const decoded = decodeYjsMessage(bytes);

    try {
      switch (decoded.kind) {
        case "syncStep1": {
          // Client sent us their state vector; reply with the diff
          // they need (sync-step-2) AND prompt them with our own
          // state vector (sync-step-1) for full bidirectional sync.
          const Y = await import("yjs");
          const doc = await this.yjsRuntime.getDoc(att.scope, att.pathId);
          const diff = Y.encodeStateAsUpdate(doc, decoded.stateVector);
          ws.send(encodeSyncStep2(diff));
          // Also send our state vector so they reciprocate (the
          // standard Yjs sync handshake is symmetric).
          const reply = await this.yjsRuntime.syncStep1Reply(
            att.scope,
            att.pathId
          );
          ws.send(reply);
          return;
        }
        case "syncStep2": {
          await this.yjsRuntime.applyRemoteUpdate(
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
          await this.yjsRuntime.applyRemoteUpdate(
            att.scope,
            att.userId,
            att.pathId,
            att.poolSize,
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
    if (att) this.yjsRuntime.removeSocket(att.pathId, ws);
  }

  /**
   * Hibernation API hook: error path mirrors close. We don't try to
   * recover the connection — clients reconnect on their own.
   */
  async webSocketError(ws: WebSocket, _err: unknown): Promise<void> {
    const att = ws.deserializeAttachment() as
      | { pathId: string }
      | null;
    if (att) this.yjsRuntime.removeSocket(att.pathId, ws);
  }
}