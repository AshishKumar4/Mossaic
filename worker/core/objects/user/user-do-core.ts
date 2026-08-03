import { DurableObject } from "cloudflare:workers";
import type { EnvCore as Env } from "../../../../shared/types";
import {
  drainChunkCleanupIntents,
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
  vfsResolveCacheKey,
  vfsRmdir,
  vfsStat,
  vfsSymlink,
  vfsUnlink,
  vfsPurge,
  vfsArchive,
  vfsUnarchive,
  vfsWriteFile,
  type CacheResolveResult,
  type PatchMetadataIfHeadResult,
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
  PreviewInfo,
  PreviewInfoBatchEntry,
  ReadPreviewOpts,
  ReadPreviewResult,
  Variant,
} from "../../../../shared/preview-types";
import { VFSError } from "../../../../shared/vfs-types";
import { vfsShardDOName } from "../../lib/utils";
import { logError, logInfo } from "../../lib/logger";
import { ensureMigrationsTable } from "../../lib/migrations";
import { USER_SCHEMA_STEPS } from "./schema";
import {
  signPreviewToken,
  PREVIEW_TOKEN_DEFAULT_TTL_MS,
} from "../../lib/preview-token";
import { dedupePaths, type DedupeResult } from "./admin";
import {
  encodeVariantKey,
  findVariantRow,
  renderAndStoreVariant,
} from "./preview-variants";
import { defaultRegistry } from "../../lib/preview-pipeline";
import { resolvePath } from "./path-walk";
import {
  vfsFileInfo,
  vfsFileInfoByPathId,
  vfsListChildren,
  vfsListFiles,
  type ListChildrenResult,
  type ListFilesItemRaw,
  type ListFilesResult,
} from "./list-files";
import {
  userIdFor,
  FILE_HEAD_JOIN,
  assertHeadNotTombstoned,
} from "./vfs/helpers";
import {
  insertAuditLog,
  loadAuditLogMaxRows,
  reapAuditLog,
} from "./vfs/audit-log";
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
import {
  scheduleAlarmAt,
  scheduleStaleUploadSweep,
  transactionSync,
} from "./internal-storage";

/**
 * P1-7 — hard cap on concurrent Yjs WebSocket clients PER pathId.
 *
 * `YjsRuntime.broadcast` does a synchronous loop over connected
 * sockets, sending the encoded frame to each. The DO single-thread
 * holds the event loop during the loop; with N clients connected
 * to one pathId, every frame burns N-1 sync sends. CPU on workerd
 * cliffs around 20-50 clients per pathId per Cloudflare colo
 * scheduling.
 *
 * 100 is the hard refusal point (BUSY surfaces to the upgrade
 * caller — they fall back to read-only). 80 is the warning
 * threshold; we log a `console.warn` so operators can spot
 * approaching-cap files before the cap fires.
 *
 * Per-path, not per-tenant: a tenant with many collaborative
 * files each at the cap is still fine — the bottleneck is
 * per-file fan-out, not aggregate connections.
 */
const YJS_WS_HARD_CAP = 100;
const YJS_WS_WARN_THRESHOLD = 80;

export class UserDOCore extends DurableObject<Env> {
  sql: SqlStorage;
  state: DurableObjectState;
  storage: DurableObjectStorage;
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
    this.state = ctx;
    this.storage = ctx.storage;
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

    transactionSync(this, () => this.initializeSchema());
    this.initialized = true;
  }

  private initializeSchema(): void {
    ensureMigrationsTable(this.sql);
    for (const applySchemaStep of USER_SCHEMA_STEPS) {
      applySchemaStep(this.sql);
    }
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
   * UserDO maintenance alarm: durable chunk cleanup plus stale-upload sweep.
   *
   * Cleanup intents replay through the scope persisted by `gateVfs`. Stale
   * `_vfs_tmp_<id>` rows older than one hour use the same transactional
   * local-delete/outbox path as a synchronous abort. Network awaits occur
   * only after each local SQL transaction commits.
   *
   * Idempotent: re-running over an already-reaped tmp is a no-op
   * (DELETE matches zero rows; ShardDO `removeFileRefs` finds zero
   * chunk_refs). Cloudflare alarms have at-least-once semantics so
   * idempotence is load-bearing.
   *
   * Reschedules after one minute while any bounded maintenance queue still
   * has work; normal VFS traffic also keeps the ten-minute alarm armed.
   */
  async alarm(): Promise<void> {
    this.ensureInit();
    const scope = this.loadScope();
    if (!scope) {
      // No VFS call has ever run on this DO, so no tmp rows or cleanup intents
      // could have been created through the supported paths.
      // Be defensive though: an operator might wipe vfs_meta but
      // leave files behind. We still skip — without a scope we
      // cannot route deleteChunks to the right ShardDO instance.
      return;
    }

    await drainChunkCleanupIntents(this, scope);

    const now = Date.now();
    const legacyCutoff = now - 60 * 60 * 1000;
    const rows = this.sql
      .exec(
        `SELECT f.file_id FROM files f
          LEFT JOIN write_stream_sessions ws ON ws.tmp_id = f.file_id
          LEFT JOIN upload_sessions us ON us.upload_id = f.file_id
         WHERE f.status = 'uploading'
           AND f.file_name LIKE '_vfs_tmp_%'
           AND (
             (ws.tmp_id IS NOT NULL AND ws.expires_at <= ?)
             OR (us.upload_id IS NOT NULL AND us.status IN ('open', 'finalizing', 'aborting') AND us.expires_at <= ?)
             OR (ws.tmp_id IS NULL AND us.upload_id IS NULL AND f.created_at < ?)
           )
          LIMIT 200`,
        now,
        now,
        legacyCutoff
      )
      .toArray() as { file_id: string }[];

    let staleSweepFailed = false;
    for (const { file_id } of rows) {
      // user_id encoding mirrors userIdFor(scope) in vfs-ops.
      const userId =
        scope.sub !== undefined ? `${scope.tenant}::${scope.sub}` : scope.tenant;
      try {
        await hardDeleteFileRow(this, userId, scope, file_id, {
          staleAt: now,
        });
      } catch (err) {
        staleSweepFailed = true;
        // Surface alarm-handler errors via structured log +
        // counter. A bare `catch {}` would eat every error
        // including permanent local failures (for example, a corrupted tmp
        // row). Remote cleanup failures remain in the durable outbox. Log
        // other failures via `logError`, bump the
        // `alarm_failures` counter in `vfs_meta`, and CONTINUE —
        // alarms have at-least-once retry; throwing would just get
        // the alarm replayed without progress on the remaining
        // batch.
        this.recordAlarmFailure(
          "stale_tmp_sweep",
          file_id,
          err
        );
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
    } catch (err) {
      // Visible failure (instead of a bare swallow).
      this.recordAlarmFailure("multipart_sweep", "", err);
    }

    // A multipart finalize that published still owes the bounded reaping its
    // head switch left behind, and its caller already has the result it came
    // for — so nothing but this alarm is guaranteed to come back for it.
    // Separate from the sweep above: a failed sweep must not strand it.
    let multipartCleaningHasMore = false;
    try {
      const { resumeCleaningMultipartSessions } = await import(
        "./multipart-upload"
      );
      const r = await resumeCleaningMultipartSessions(this, () => scope);
      multipartCleaningHasMore = r.remaining;
    } catch (err) {
      this.recordAlarmFailure("multipart_cleaning", "", err);
    }

    // Shard capacity warning poll. Throttled (once per cadence) by
    // the helper itself; reads `quota.pool_size` and fans out a
    // `getStorageBytes` RPC per shard. Logs a structured warning
    // for each shard that's >softCap (9 GB). Best-effort: a
    // transient shard failure or missing quota row is swallowed so
    // capacity monitoring never blocks the primary alarm work.
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
    } catch (err) {
      // Visible failure (instead of a bare swallow).
      this.recordAlarmFailure("shard_capacity_poll", "", err);
    }

    // Audit-log retention sweep. Trim oldest rows when count
    // exceeds the configured cap. Cheap (one COUNT + one bounded
    // DELETE); fires inline so retention pressure amortizes
    // across alarm ticks.
    try {
      const max = loadAuditLogMaxRows(this);
      const reaped = reapAuditLog(this, max);
      if (reaped > 0) {
        const tenantId =
          scope.sub !== undefined
            ? `${scope.ns}::${scope.tenant}::${scope.sub}`
            : `${scope.ns}::${scope.tenant}`;
        logInfo(
          "audit-log retention reaped rows",
          { tenantId },
          { event: "audit_log_reaped", reaped, max }
        );
      }
    } catch (err) {
      this.recordAlarmFailure("audit_log_reap", "", err);
    }

    const nextCleanupAttempt = this.sql
      .exec<{ next_attempt_at: number | null }>(
        `SELECT MIN(next_attempt_at) AS next_attempt_at
           FROM chunk_cleanup_intents
          WHERE state IN ('pending', 'in_flight')`
      )
      .toArray()[0]?.next_attempt_at;
    const nextUploadExpiry = this.sql
      .exec<{ deadline: number | null }>(
        `SELECT MIN(deadline) AS deadline FROM (
           SELECT expires_at AS deadline FROM write_stream_sessions
           UNION ALL
            SELECT expires_at AS deadline FROM upload_sessions WHERE status IN ('open', 'finalizing', 'aborting')
           UNION ALL
           SELECT f.created_at + 3600000 AS deadline
             FROM files f
             LEFT JOIN write_stream_sessions ws ON ws.tmp_id = f.file_id
             LEFT JOIN upload_sessions us ON us.upload_id = f.file_id
            WHERE f.status = 'uploading'
              AND f.file_name LIKE '_vfs_tmp_%'
              AND ws.tmp_id IS NULL AND us.upload_id IS NULL
         )`
      )
      .toArray()[0]?.deadline;

    // Reschedule while any bounded maintenance queue still has work.
    const maintenanceHasMore =
      rows.length === 200 ||
      staleSweepFailed ||
      multipartHasMore ||
      multipartCleaningHasMore;
    if (
      maintenanceHasMore ||
      (nextCleanupAttempt !== null && nextCleanupAttempt !== undefined) ||
      (nextUploadExpiry !== null && nextUploadExpiry !== undefined)
    ) {
      let target = maintenanceHasMore
        ? Date.now() + 60_000
        : Number.POSITIVE_INFINITY;
      if (nextCleanupAttempt !== null && nextCleanupAttempt !== undefined) {
        target = Math.min(
          target,
          Math.max(Date.now() + 1_000, nextCleanupAttempt)
        );
      }
      if (nextUploadExpiry !== null && nextUploadExpiry !== undefined) {
        target = Math.min(
          target,
          Math.max(Date.now() + 1_000, nextUploadExpiry)
        );
      }
      await scheduleAlarmAt(this, target);
    }
  }

  /**
   * Record an alarm-handler failure visibly. Logs the error via
   * `logError` (so Logpush surfaces it) AND bumps the persistent
   * `alarm_failures` counter in `vfs_meta`. Operators who notice
   * the counter rising can grep Logpush by
   * `event: "alarm_handler_failed"` for the specific stack.
   *
   * Never throws — alarm handlers swallow their own observability
   * failures so a failed log/counter doesn't compound into a
   * failed alarm.
   */
  private recordAlarmFailure(
    sweepKind: string,
    targetId: string,
    err: unknown
  ): void {
    const scope = this.loadScope();
    const tenantId = scope
      ? scope.sub !== undefined
        ? `${scope.ns}::${scope.tenant}::${scope.sub}`
        : `${scope.ns}::${scope.tenant}`
      : "unknown";
    logError(
      "alarm handler failure",
      { tenantId },
      err,
      { event: "alarm_handler_failed", sweepKind, targetId }
    );
    try {
      this.sql.exec(
        `INSERT INTO vfs_meta (key, value)
         VALUES ('alarm_failures', '1')
         ON CONFLICT(key) DO UPDATE SET value = CAST((CAST(value AS INTEGER) + 1) AS TEXT)`
      );
    } catch {
      // observability failure must not block alarm continuation.
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
   * readManyFile() — batched readFile for multi-file fetch.
   *
   * Mirrors `vfsReadManyStat`'s per-path try/catch null-on-ENOENT
   * shape and `previewInfoMany`'s 256-path soft cap. Inline-tier
   * files served from SQL; chunked files fan out per shard via
   * `getChunksBatch`. Encrypted bytes are returned as-is (envelope).
   */
  async vfsReadManyFile(
    scope: VFSScope,
    paths: string[]
  ): Promise<(Uint8Array | null)[]> {
    this.gateVfs(scope);
    if (paths.length === 0) return [];
    if (paths.length > 256) {
      throw new VFSError(
        "EINVAL",
        `vfsReadManyFile: max 256 paths per call (got ${paths.length})`
      );
    }
    const { vfsReadFile } = await import("./vfs/reads");
    const out: (Uint8Array | null)[] = [];
    for (const p of paths) {
      try {
        out.push(await vfsReadFile(this, scope, p));
      } catch (err) {
        if (err instanceof VFSError && err.code === "ENOENT") {
          out.push(null);
          continue;
        }
        throw err;
      }
    }
    return out;
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

  /**
   * Cheap pre-flight for cache-key construction. Returns the
   * bust state (fileId, headVersionId, updatedAt,
   * encryption stamp) for `path` in one SQL JOIN. Routes that
   * wrap reads in `caches.default` call this BEFORE the heavy
   * RPC so they can build a deterministic cache key.
   *
   * Throws ENOENT for missing paths; EISDIR for directories.
   * Symlinks are followed to their direct file target.
   */
  async vfsResolveCacheKey(
    scope: VFSScope,
    path: string
  ): Promise<CacheResolveResult> {
    this.gateVfs(scope);
    return vfsResolveCacheKey(this, scope, path);
  }

  /**
   * Cheap cache-bust oracle for folder-surface ops (`readdir` /
   * `listChildren` / `listFiles` / `stat` / `fileInfo` /
   * `readManyStat`). Returns the parent folder's `revision`
   * counter — bumped by every mutation that affects the listing.
   *
   * Resolves `path` as the FOLDER itself: `/` returns the root
   * counter; `/foo` returns the revision of folder `foo`. ENOTDIR
   * for non-folder paths.
   */
  async vfsFolderRevision(
    scope: VFSScope,
    path: string
  ): Promise<{ revision: number }> {
    this.gateVfs(scope);
    const { vfsFolderRevision } = await import("./vfs/folder-revision");
    return vfsFolderRevision(this, scope, path);
  }

  /**
   * Mint a signed preview-variant URL.
   *
   * Resolves `path` to a fileId + headVersionId, ensures the
   * variant cache row exists (rendering on demand if needed),
   * reads the chunkHash, and signs a JWT that the browser can
   * present to `GET /api/vfs/preview-variant/<token>`.
   *
   * The mint RPC \u2014 not the route handler \u2014 owns the auth
   * decision: callers prove they can read the path here, and
   * the resulting token grants a CDN-cacheable URL whose bytes
   * are content-addressed (immutable per contentHash). Subsequent
   * fetches for the same content hit Workers Cache + browser
   * cache without re-authenticating.
   *
   * Encrypted files throw `ENOTSUP` (server cannot render
   * ciphertext; client-side rendering is the path forward).
   * Tombstoned heads throw `ENOENT` (matches `vfsReadPreview`).
   *
   * @param ttlMs \u2014 token TTL clamped to
   *   [PREVIEW_TOKEN_MIN_TTL_MS, PREVIEW_TOKEN_MAX_TTL_MS] by
   *   `signPreviewToken`. Default 24h. The browser cache lives
   *   for the year-long max-age regardless of token TTL.
   */
  async vfsMintPreviewToken(
    scope: VFSScope,
    path: string,
    opts: {
      variant?: Variant;
      format?: ReadPreviewOpts["format"];
      renderer?: string;
      ttlMs?: number;
    } = {}
  ): Promise<PreviewInfo> {
    this.gateVfs(scope);
    return this.mintPreviewInfo(scope, path, opts);
  }

  /**
   * Read variant bytes by content hash, gated on the
   * variant cache row matching `(fileId, variantKind, rendererKind,
   * headVersionId, contentHash)`. Used by the preview-variant
   * route after `verifyPreviewToken` succeeds: the token claims
   * the bytes match `contentHash`; this RPC re-verifies the
   * row still has that hash, then streams bytes from ShardDO.
   *
   * Returns null when:
   *   - The variant row no longer exists (e.g. dropped + not
   *     re-rendered yet). Route returns 404.
   *   - The row exists but its `chunk_hash` no longer matches
   *     the token's `contentHash` (a re-render produced
   *     different bytes). Route returns 410 Gone (token stale).
   *   - The chunk has been reaped from the shard. Route
   *     returns 410.
   *
   * NOT auth-gated by the route's `vfsAuth` middleware (the
   * route validates the HMAC token instead). The DO-level
   * `gateVfs` is bypassed because the token IS the auth signal;
   * any caller with a valid token has already proved the mint
   * RPC verified them.
   */
  async vfsReadVariantByHash(
    scope: VFSScope,
    fileId: string,
    variantKind: string,
    rendererKind: string,
    headVersionId: string | null,
    contentHash: string
  ): Promise<{
    bytes: Uint8Array;
    mimeType: string;
    width: number;
    height: number;
  } | null> {
    this.ensureInit();
    const row = findVariantRow(
      this,
      fileId,
      variantKind,
      rendererKind,
      headVersionId
    );
    if (row === null) return null;
    if (row.chunkHash !== contentHash) return null;
    const env = this.envPublic;
    const shardName = vfsShardDOName(
      scope.ns,
      scope.tenant,
      scope.sub,
      row.shardIndex
    );
    const shardNs = env.MOSSAIC_SHARD as unknown as DurableObjectNamespace<
      import("../shard/shard-do").ShardDO
    >;
    const stub = shardNs.get(shardNs.idFromName(shardName));
    const bytes = await stub.getChunkBytes(row.chunkHash);
    if (bytes === null) return null;
    return {
      bytes,
      mimeType: row.mimeType,
      width: row.width,
      height: row.height,
    };
  }

  /**
   * Batched preview-info mint. Mirrors the
   * `/manifests` batched shape: one RPC per N paths instead of N
   * RPCs. Per-path failures are returned alongside successes so
   * a single missing file doesn't surface 4xx for the whole
   * batch.
   *
   * Cap at 256 paths per call (matches the manifests batch
   * cap) so a single request can't exhaust DO turn time.
   */
  async vfsPreviewInfoMany(
    scope: VFSScope,
    paths: readonly string[],
    opts: {
      variant?: Variant;
      format?: ReadPreviewOpts["format"];
      renderer?: string;
      ttlMs?: number;
    } = {}
  ): Promise<PreviewInfoBatchEntry[]> {
    this.gateVfs(scope);
    if (paths.length === 0) return [];
    if (paths.length > 256) {
      throw new VFSError(
        "EINVAL",
        `vfsPreviewInfoMany: max 256 paths per call (got ${paths.length})`
      );
    }
    const out: PreviewInfoBatchEntry[] = [];
    for (const p of paths) {
      try {
        const info = await this.mintPreviewInfo(scope, p, opts);
        out.push({ path: p, ok: true, info });
      } catch (err) {
        let code: string = "EBUSY";
        if (err instanceof VFSError) {
          code = err.code;
        } else if (err instanceof Error) {
          const maybeCoded = err as Error & { code?: unknown };
          if (typeof maybeCoded.code === "string") {
            code = maybeCoded.code;
          }
        }
        const message =
          err instanceof Error ? err.message : String(err);
        out.push({ path: p, ok: false, code, message });
      }
    }
    return out;
  }

  /**
   * Internal mint helper. Resolves the path, ensures
   * the variant cache row exists, signs the token. Used by both
   * `vfsMintPreviewToken` and `vfsPreviewInfoMany`.
   *
   * Mirrors the auth + routing flow of `vfsReadPreview` so the
   * mint decision matches the read decision exactly. Where
   * `vfsReadPreview` returns bytes, this helper returns a
   * signed URL pointing at the same content.
   */
  private async mintPreviewInfo(
    scope: VFSScope,
    path: string,
    opts: {
      variant?: Variant;
      format?: ReadPreviewOpts["format"];
      renderer?: string;
      ttlMs?: number;
    }
  ): Promise<PreviewInfo> {
    const userId = userIdFor(scope);
    const r = resolvePath(this, userId, path);
    if (r.kind === "ENOENT") {
      throw new VFSError("ENOENT", `previewUrl: no such file: ${path}`);
    }
    if (r.kind === "dir") {
      throw new VFSError("EISDIR", `previewUrl: is a directory: ${path}`);
    }
    if (r.kind !== "file") {
      throw new VFSError(
        "EINVAL",
        `previewUrl: not a regular file: ${path}`
      );
    }
    const fileId = r.leafId;

    // Pull file metadata + head_version state. Mirrors the SELECT
    // in `vfs/preview.ts:90-118` so the mint decision and the
    // read decision see the same row shape.
    const fileRowRaw = this.sql
      .exec(
        `SELECT f.file_name, f.file_size, f.mime_type, f.encryption_mode,
                f.head_version_id, fv.deleted AS head_deleted,
                fv.size AS head_size
           FROM files f
           ${FILE_HEAD_JOIN}
          WHERE f.file_id = ? AND f.user_id = ? AND f.status != 'deleted'`,
        fileId,
        userId
      )
      .toArray()[0] as
      | {
          file_name: string;
          file_size: number;
          mime_type: string | null;
          encryption_mode: string | null;
          head_version_id: string | null;
          head_deleted: number | null;
          head_size: number | null;
        }
      | undefined;
    const fileRow = assertHeadNotTombstoned(fileRowRaw, "previewUrl", path);
    if (fileRow.encryption_mode !== null) {
      throw new VFSError(
        "ENOTSUP",
        "previewUrl: encrypted files require client-side rendering"
      );
    }

    const mimeType = fileRow.mime_type ?? "application/octet-stream";
    const fileName = fileRow.file_name;
    const fileSize =
      fileRow.head_version_id !== null
        ? (fileRow.head_size ?? 0)
        : fileRow.file_size;
    const headVersionForCache = fileRow.head_version_id;

    const registry = defaultRegistry();
    const primaryRenderer = registry.dispatchByMime(mimeType);
    const variant: Variant = opts.variant ?? "thumb";
    const variantKey = encodeVariantKey(variant);

    // Try the variant cache. If hit, we know the chunkHash + dims
    // immediately. Same fallback chain as vfs/preview.ts so a
    // pre-rendered icon-card or image-passthrough row counts.
    let row = findVariantRow(
      this,
      fileId,
      variantKey,
      primaryRenderer.kind,
      headVersionForCache
    );
    let rowRendererKind = primaryRenderer.kind;
    if (row === null) {
      const fallbackKinds = mimeType.startsWith("image/")
        ? ["image-passthrough", "icon-card"]
        : ["icon-card"];
      for (const k of fallbackKinds) {
        const fallback = findVariantRow(
          this,
          fileId,
          variantKey,
          k,
          headVersionForCache
        );
        if (fallback !== null) {
          row = fallback;
          rowRendererKind = k;
          break;
        }
      }
    }

    // Cache miss \u2014 render + persist + re-lookup.
    if (row === null) {
      await renderAndStoreVariant(
        this,
        scope,
        fileId,
        path,
        mimeType,
        fileName,
        fileSize,
        variant,
        headVersionForCache
      );
      // Resolve which renderer kind was actually persisted (the
      // EMOSSAIC_UNAVAILABLE fallback in renderAndStoreVariant
      // could have chosen image-passthrough or icon-card).
      const persistedRow = findVariantRow(
        this,
        fileId,
        variantKey,
        primaryRenderer.kind,
        headVersionForCache
      );
      if (persistedRow !== null) {
        row = persistedRow;
        rowRendererKind = primaryRenderer.kind;
      } else {
        const fallbackKinds = mimeType.startsWith("image/")
          ? ["image-passthrough", "icon-card"]
          : ["icon-card"];
        for (const k of fallbackKinds) {
          const fallback = findVariantRow(
            this,
            fileId,
            variantKey,
            k,
            headVersionForCache
          );
          if (fallback !== null) {
            row = fallback;
            rowRendererKind = k;
            break;
          }
        }
      }
    }

    if (row === null) {
      // renderAndStoreVariant should have written exactly one row
      // (composite PK guards the race); if we still can't find it
      // here something is structurally wrong. Surface as EBUSY so
      // the SPA shows a transient failure (and retries) rather
      // than caching the bad state.
      throw new VFSError(
        "EBUSY",
        `previewUrl: variant row missing after render for ${path}`
      );
    }

    // Tenant id mirrors `userIdFor(scope)` shape so the route
    // can re-derive scope from the verified token's tenantId
    // claim.
    const tenantId =
      scope.sub !== undefined
        ? `${scope.ns}::${scope.tenant}::${scope.sub}`
        : `${scope.ns}::${scope.tenant}`;
    const format =
      typeof opts.format === "string" && opts.format.length > 0
        ? opts.format
        : "auto";
    const ttlMs = opts.ttlMs ?? PREVIEW_TOKEN_DEFAULT_TTL_MS;

    const { token, expiresAtMs } = await signPreviewToken(
      this.envPublic,
      {
        tenantId,
        fileId,
        headVersionId: headVersionForCache,
        variantKind: variantKey,
        rendererKind: rowRendererKind,
        format,
        contentHash: row.chunkHash,
      },
      ttlMs
    );
    const cacheControl = "public, max-age=31536000, immutable";
    return {
      token,
      url: `/api/vfs/preview-variant/${token}`,
      etag: `W/"${row.chunkHash}"`,
      mimeType: row.mimeType,
      width: row.width,
      height: row.height,
      rendererKind: rowRendererKind,
      versionId: headVersionForCache,
      cacheControl,
      contentHash: row.chunkHash,
      expiresAtMs,
    };
  }

  // ── VFS RPC surface (write-side) ──────────────────────────────
  //
  // Atomic writes (temp-id-then-rename), hard delete with durable chunk-GC
  // intents, and supporting mutating ops. `commitRename` uses an explicit
  // synchronous transaction for its complete local publication; network
  // awaits happen only while uploading or draining committed cleanup work.
  // ShardDO.deleteChunks soft-marks chunks, and its alarm sweeper hard-deletes
  // them after a 30s grace (sdk-impl-plan §8.3).
  //
  // Inline tier (≤ INLINE_LIMIT) writes never touch ShardDO; their temp and
  // published bytes live in files.inline_data.

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
   * purge() — destructive cleanup.
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
   * `archive(path)` / `unarchive(path)`.
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
    dst: string,
    opts?: { overwrite?: boolean }
  ): Promise<void> {
    this.gateVfsWrite(scope);
    return vfsRename(this, scope, src, dst, opts);
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
    await scheduleStaleUploadSweep(this);
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
  // The RPCs forming the upload session boundary. Per-chunk PUTs do
  // NOT touch UserDO — they validate the session token in the route
  // handler (CPU-only, HMAC verify) and call ShardDO directly. This
  // is the load-bearing constraint that lets multipart saturate user
  // bandwidth without bottlenecking on UserDO single-thread.
  //
  // - vfsBeginMultipart: mints session, inserts tmp row + session row,
  //   returns HMAC token. Resume mode probes shards for landed[].
  // - vfsAbortMultipart: flips status, fans out chunk-ref drops + staging
  //   clears across the pool, hard-deletes tmp row.
  // - vfsStageMultipartHashes: persists one bounded page of declared
  //   chunk hashes, advancing the session's contiguous staging cursor.
  // - vfsFinalizeMultipartStep: advances the durable finalize machine by
  //   one bounded page — fence, verify, or publish.
  // - vfsFinalizeMultipart: stages the declared manifest and drives that
  //   machine to completion in one turn.
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
    await scheduleStaleUploadSweep(this);
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

  async vfsStageMultipartHashes(
    scope: VFSScope,
    uploadId: string,
    startIndex: number,
    hashes: readonly string[]
  ): Promise<import("../../../../shared/multipart").MultipartHashPageResponse> {
    this.gateVfsWrite(scope);
    const { vfsStageMultipartHashes } = await import("./multipart-upload");
    return vfsStageMultipartHashes(this, scope, uploadId, startIndex, hashes);
  }

  async vfsFinalizeMultipartStep(
    scope: VFSScope,
    uploadId: string
  ): Promise<import("../../../../shared/multipart").MultipartFinalizeProgress> {
    this.gateVfsWrite(scope);
    const { vfsFinalizeMultipartStep } = await import("./multipart-upload");
    return vfsFinalizeMultipartStep(this, scope, uploadId);
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
    // markVersion mutates file_versions.{label,user_visible}; both
    // are visible via listVersions feeding into folder-surface
    // tooling. Bump parent revision so cache invalidates.
    const { bumpFolderRevision } = await import("./vfs/helpers");
    const parentRow = this.sql
      .exec(
        "SELECT parent_id FROM files WHERE file_id=? AND user_id=?",
        pathId,
        userId,
      )
      .toArray()[0] as { parent_id: string | null } | undefined;
    bumpFolderRevision(this, userId, parentRow?.parent_id ?? null);
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
    expectedNextSeq: number,
    opts?: { userVisible?: boolean; label?: string }
  ): Promise<{
    checkpointSeq: number;
    opsReaped: number;
    versionId?: string;
  }> {
    this.gateVfsWrite(scope);
    if (opts?.label !== undefined) {
      const { validateLabel } = await import(
        "../../../../shared/metadata-validate"
      );
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
      expectedNextSeq,
      opts
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
   * Synthesize a VFSScope from a userId for the userId-only admin
   * RPCs (`adminSetVersioning`, `adminGetVersioning`). The App-side
   * tenant convention is `{ ns: "default", tenant: userId }` —
   * mirrors the alarm-path scope reconstruction at
   * `worker/app/objects/user/user-do.ts:79-84` and the App tenant
   * mapping at `worker/app/routes/auth.ts:155`. Per-tenant rate-
   * limit accounting under this scope hits the same bucket the
   * tenant's gated VFS RPCs use.
   */
  private adminScopeFor(userId: string): VFSScope {
    return { ns: "default", tenant: userId };
  }

  /**
   * Operator-only: toggle versioning for a tenant. Affects only
   * future writes; existing files / versions are unchanged. Pass
   * `userId` directly (matches admin convention; not scope-derived
   * because the caller may not have a token-scoped session).
   *
   * P1-6 fix — gated through the standard write path so the per-
   * tenant rate-limit bucket also bounds admin replay attempts.
   * Mirrors the prior fix that closed the corresponding gap on
   * the App-side `app*` surface (`worker/app/objects/user/gate.ts`).
   */
  async adminSetVersioning(
    userId: string,
    enabled: boolean
  ): Promise<{ enabled: boolean }> {
    this.gateVfsWrite(this.adminScopeFor(userId));
    setVersioningEnabled(this, userId, enabled);
    insertAuditLog(this, {
      op: "adminSetVersioning",
      actor: "operator",
      target: userId,
      payload: JSON.stringify({ enabled }),
    });
    return { enabled };
  }

  /** Operator-only: read the versioning flag for a tenant. */
  async adminGetVersioning(userId: string): Promise<{ enabled: boolean }> {
    this.gateVfs(this.adminScopeFor(userId));
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
   *
   * P1-6 — write-class gate. Dedupe materially mutates rows.
   */
  async adminDedupePaths(
    userId: string,
    scope: VFSScope
  ): Promise<DedupeResult> {
    this.gateVfsWrite(scope);
    const result = await dedupePaths(this, userId, scope);
    insertAuditLog(this, {
      op: "adminDedupePaths",
      actor: "operator",
      target: userId,
      payload: JSON.stringify(result),
    });
    return result;
  }

  /**
   * Recovery primitive for tombstoned-head rows.
   *
   * Scans `files` rows whose `head_version_id` points at a
   * `deleted=1` `file_versions` row and either drops them
   * (`mode: "hardDelete"`, default for cleanup) or repoints head
   * at the newest live predecessor (`mode: "walkBack"`, for
   * recovery from accidental unlinks). Defaults to `dryRun: true`
   * — pass `dryRun: false` explicitly to write.
   *
   * Idempotent. Safe to re-run after partial completion.
   *
   * P1-6 — write-class gate (mutates `files` rows when not in
   * dry-run mode). Dry-run still goes through the gate so its
   * rate-limit accounting matches the non-dry-run path; otherwise
   * an attacker could bypass the bucket by always passing
   * `dryRun: true` while still consuming SQL CPU.
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
    this.gateVfsWrite(scope);
    const { reapTombstonedHeads } = await import("./admin-tombstones");
    const result = await reapTombstonedHeads(this, userId, scope, opts);
    insertAuditLog(this, {
      op: "adminReapTombstonedHeads",
      actor: "operator",
      target: userId,
      payload: JSON.stringify({
        mode: opts.mode,
        dryRun: result.dryRun,
        scanned: result.scanned,
        hardDeleted: result.hardDeleted,
        walkedBack: result.walkedBack,
      }),
    });
    return result;
  }

  /**
   * Pre-generate standard preview variants (`thumb`, `medium`,
   * `lightbox`) for a freshly-finalized file. Intended to run
   * inside the route layer's `c.executionCtx.waitUntil(...)` so
   * pre-gen latency doesn't extend the finalize response.
   *
   * Best-effort: per-variant failures are logged and swallowed.
   * Skips empty + encrypted files. Idempotent (content-addressed).
   *
   * P1-6 — read-class gate. Pre-generates inserts NEW chunk_refs
   * but does NOT touch the partial-UNIQUE-INDEX-bearing
   * (parent_id, file_name) slot, so the H6 EBUSY refusal would
   * spuriously block legitimate pre-gen. Use the read gate
   * which still enforces per-tenant rate-limit + scope persistence.
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
       * head_version_id at finalize time. Optional for backward
       * compat with route callers that haven't been updated;
       * resolved from the `files` row when omitted.
       */
      headVersionId?: string | null;
    }
  ): Promise<void> {
    this.gateVfs(scope);
    // Resolve head_version_id from `files` when the caller didn't
    // pass it. Pre-generated variants stamp this value so a
    // subsequent write that flips the head invalidates them.
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
    insertAuditLog(this, {
      op: "adminPreGenerateStandardVariants",
      actor: "operator",
      target: args.fileId,
      payload: JSON.stringify({
        path: args.path,
        mimeType: args.mimeType,
        headVersionId,
        isEncrypted: args.isEncrypted,
      }),
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
   * Stable-id CAS metadata patch. Lets review systems key decisions by
   * (userId, pathId, versionId) and avoid a read-head-then-patch race.
   */
  async vfsPatchMetadataIfHead(
    scope: VFSScope,
    pathId: string,
    expectedHeadVersionId: string | null,
    patch: Record<string, unknown> | null,
    opts?: { addTags?: readonly string[]; removeTags?: readonly string[] }
  ): Promise<PatchMetadataIfHeadResult> {
    this.gateVfsWrite(scope);
    const { vfsPatchMetadataIfHead } = await import("./vfs-ops");
    return vfsPatchMetadataIfHead(
      this,
      scope,
      pathId,
      expectedHeadVersionId,
      patch,
      opts,
    );
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
      includeContentHash?: boolean;
    }
  ): Promise<ListFilesResult> {
    this.gateVfs(scope);
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
      includeContentHash?: boolean;
    }
  ): Promise<ListFilesItemRaw> {
    this.gateVfs(scope);
    return vfsFileInfo(this, scope, path, opts);
  }

  async vfsFileInfoByPathId(
    scope: VFSScope,
    pathId: string,
    opts?: {
      includeStat?: boolean;
      includeMetadata?: boolean;
      includeTombstones?: boolean;
      includeArchived?: boolean;
      includeContentHash?: boolean;
    }
  ): Promise<ListFilesItemRaw> {
    this.gateVfs(scope);
    return vfsFileInfoByPathId(this, scope, pathId, opts);
  }

  /**
   * Batched directory listing. Returns folder revision + a single
   * page of merged folder/file/symlink entries with stat /
   * metadata / contentHash hydrated in one round-trip. Replaces a
   * naive `readdir + lstat × N` loop. See
   * `list-files.ts:vfsListChildren` for the merge / cursor
   * semantics.
   */
  async vfsListChildren(
    scope: VFSScope,
    opts: {
      path: string;
      orderBy?: "mtime" | "name" | "size";
      direction?: "asc" | "desc";
      limit?: number;
      cursor?: string;
      includeStat?: boolean;
      includeMetadata?: boolean;
      includeContentHash?: boolean;
      includeTombstones?: boolean;
      includeArchived?: boolean;
    }
  ): Promise<ListChildrenResult> {
    this.gateVfs(scope);
    return vfsListChildren(this, scope, opts);
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
   * Return the full `Y.encodeStateAsUpdate(doc)` bytes for a
   * yjs-mode file so SDK consumers can decode arbitrary
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
    // Distinguish ENOENT / ENOTDIR / ELOOP / EISDIR / EINVAL on
    // path resolution. Without this branching, every non-"file"
    // kind would collapse to EINVAL with the misleading message
    // "not a regular file", breaking the standard fs-style error
    // contract a Tiptap consumer expects.
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
    // Refuse the WS upgrade for a tombstoned-head file. Without
    // this gate, a yjs-mode path that had been `unlink`ed under
    // versioning-on would still accept incoming WS connections;
    // clients could read AND WRITE into a path the SDK reported as
    // gone. The explicit head-tombstone shortcut to ENOENT matches
    // `vfsStat` / `vfsReadFile`.
    const yjsHead = this.sql
      .exec(
        `SELECT f.head_version_id, fv.deleted AS head_deleted
           FROM files f
           ${FILE_HEAD_JOIN}
          WHERE f.file_id = ? AND f.user_id = ?`,
        r.leafId,
        userId
      )
      .toArray()[0] as
      | { head_version_id: string | null; head_deleted: number | null }
      | undefined;
    // Tolerant of a missing `files` row (a fully-purged file is
    // already gone — that's not a tombstone case). Only error
    // when the row exists AND its head is tombstoned.
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

    // P1-7 fix — hard cap on concurrent yjs sockets per path.
    //
    // Pre-fix the `broadcast` loop in YjsRuntime fanned out every
    // Yjs frame to every connected socket synchronously inside the
    // DO single-thread. With N=100 connected clients on one
    // pathId, a 10-byte update produces 99 sync `ws.send` calls
    // per write — DO CPU is bounded, so throughput cliffs at 20-50
    // collaborators per file in practice. Refusing the upgrade
    // beyond a hard cap forces clients to fall back to plaintext
    // polling rather than silently degrading the editing surface
    // for everyone connected.
    //
    // Cap is per-pathId; a tenant with N collaborative files each
    // at the cap is fine — the bottleneck is per-file fan-out.
    // ctx.getWebSockets(tag) returns sockets accepted under the
    // tag (the pathId) including those currently hibernated, so
    // the count reflects the steady-state population, not just
    // active-frame senders.
    const existing = this.ctx.getWebSockets(r.leafId).length;
    if (existing >= YJS_WS_HARD_CAP) {
      throw new VFSError(
        "EBUSY",
        `openYjsSocket: too many connected clients (${existing}/${YJS_WS_HARD_CAP}) on ${path}`
      );
    }
    if (existing >= YJS_WS_WARN_THRESHOLD) {
      // eslint-disable-next-line no-console
      console.warn(
        `[mossaic:P1-7] openYjsSocket near cap: ${existing}/${YJS_WS_HARD_CAP} on path=${path} tenant=${scope.tenant}`
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
