import type { EnvApp } from "@shared/types";
import type {
  UserStats,
  UserFilesByStatus,
  MimeDistribution,
  RecentUpload,
  ShardDistribution,
  GalleryPhoto,
} from "../../types";
import type { UserFile, Folder, QuotaInfo } from "@shared/types";
import { handleSignup, handleLogin, type AuthResult } from "./auth";
import { listFiles, deleteFile, getFile } from "./files";
import { createFolder, listFolders, getFolderPath } from "./folders";
import { getQuota, updateUsage } from "./quota";
import { UserDOCore } from "@core/objects/user/user-do-core";
import {
  appAuthScopeFor,
  appGate,
  appGateFromPersistedScope,
  appGateWrite,
  appScopeFor,
} from "./gate";

/**
 * Typed projection of a `files` table row. Exposed for callers of
 * {@link UserDO.appGetFile}; matches the snake_case column names of
 * the underlying SQLite schema so the App routes can pass it
 * straight through to wire shapes that expect the column naming
 * (e.g. the public share endpoint at `/api/shared/:token/photos`).
 */
export interface AppFileRow {
  file_id: string;
  user_id: string;
  parent_id: string | null;
  file_name: string;
  file_size: number;
  file_hash: string;
  mime_type: string;
  chunk_size: number;
  chunk_count: number;
  pool_size: number;
  status: string;
  created_at: number;
  updated_at: number;
  deleted_at: number | null;
}

/**
 * `UserDO` is the App-side subclass of `UserDOCore`. It inherits the
 * entire VFS RPC surface (vfsReadFile/Write/Stat/..., versioning, Yjs
 * WebSocket, alarm sweep, rate-limit gates, schema migrations) from
 * Core and ADDS the photo-app's app-only RPCs as typed methods.
 *
 * Class-name preservation: production wrangler still binds
 * `class_name: "UserDO"` and migration tags remain v1 (UserDO,
 * ShardDO) and v2 (SearchDO). The runtime DO namespace is keyed by
 * (script, class_name) so storage on the existing app at
 * mossaic.ashishkumarsingh.com is untouched.
 */
export class UserDO extends UserDOCore {
  /**
   * Override Core's `fetch` to surface the WebSocket upgrade path
   * through `super.fetch` (Yjs collab WS). Non-WS HTTP requests fall
   * through to Core's default 404.
   */
  override async fetch(request: Request): Promise<Response> {
    return super.fetch(request);
  }

  /**
   * Override Core's alarm to chain through the App-side search-index
   * reconciler (Phase 23 Blindspot fix). Core's alarm sweeps tmp
   * upload rows + expired multipart sessions; the App layer adds a
   * sweep of `indexed_at IS NULL` files to catch cases where the
   * SPA crashed between `multipart/finalize` and the index POST.
   *
   * The reconciler is best-effort: any error is logged and swallowed
   * so it never blocks Core's alarm reschedule logic.
   */
  override async alarm(): Promise<void> {
    await super.alarm();
    try {
      const { reconcileUnindexedFiles } = await import(
        "../../routes/search"
      );
      const scope = (this as unknown as {
        loadScope(): { tenant: string; sub?: string } | null;
      }).loadScope();
      if (scope === null) return;
      const userId =
        scope.sub !== undefined ? `${scope.tenant}::${scope.sub}` : scope.tenant;
      const env = this.envPublic as unknown as EnvApp;
      const { reconciled } = await reconcileUnindexedFiles(env, userId, 25);
      if (reconciled > 0) {
        // Re-arm a soonish alarm so we drain backlog without
        // waiting for the next ambient tick. Core's alarm
        // already sets a 60s reschedule when its own batches
        // fill, so this is the AT-LEAST cadence; it's safe to
        // also set here because the storage API takes the
        // earlier of competing alarms.
        const cur = await this.ctx.storage.getAlarm();
        const target = Date.now() + 60_000;
        if (cur === null || cur > target) {
          await this.ctx.storage.setAlarm(target);
        }
      }
    } catch (err) {
      console.warn(
        `index reconciler alarm hook failed: ${
          err instanceof Error ? err.message : String(err)
        }`
      );
    }
  }

  // ── App-only RPCs ────────────────────────────────────────────────
  //
  // All methods below are TYPED RPCs callable from App routes via
  // `stub.appXxx(...)` over the DO RPC binding. Each method runs a
  // gate function from `./gate` (`appGate` for reads, `appGateWrite`
  // for writes, `appGateFromPersistedScope` when the RPC signature
  // lacks userId) before any work — closing the gap where the
  // App-side RPCs ran ungated against the per-tenant rate limiter
  // that VFS tenants enjoyed for free. Route-layer `authMiddleware`
  // is the upstream auth gate; these are per-tenant rate limiting
  // + write-degraded refusal.

  /**
   * Create user, hash password, insert quota row. Returns userId+email.
   *
   * Gate: per-account (auth:<email> DO) bucket. Brute-force defense
   * for repeated signups against the same email — an attacker
   * hammering signup with credential variants hits EAGAIN around
   * attempt ~200 and refills at 100/sec.
   */
  async appHandleSignup(email: string, password: string): Promise<AuthResult> {
    appGateWrite(this, appAuthScopeFor(email));
    return handleSignup(this, email, password);
  }

  /**
   * Initialize the per-tenant data DO with a default quota row.
   *
   * Called by `POST /api/auth/signup` immediately after the auth row
   * lands on the `auth:<email>` DO. Without this, the canonical
   * `vfs:default:<userId>` data DO has no quota row and analytics +
   * `appGetQuota` return all-zero defaults forever — gallery still
   * works but `storage_used` never tracks reality.
   *
   * Idempotent (`INSERT OR IGNORE`).
   */
  async appInitTenant(userId: string): Promise<void> {
    // Write gate against the *target* tenant DO (not the auth DO);
    // the route calls this on `userStub(c.env, userId)` after the
    // signup lands on the auth DO. EBUSY would be alarming here but
    // the gate is structurally consistent with the rest.
    appGateWrite(this, appScopeFor(userId));
    this.sql.exec(
      `INSERT OR IGNORE INTO quota (user_id, storage_used, storage_limit, file_count, pool_size)
       VALUES (?, 0, 107374182400, 0, 32)`,
      userId
    );
  }

  /**
   * Verify credentials. Returns userId+email on success; throws on
   * failure.
   *
   * Gate: per-account (auth:<email> DO) bucket. Same brute-force
   * defense rationale as appHandleSignup. We deliberately use the
   * read-only gate here (no EBUSY check) — login does not write
   * to the `files`/`folders` tables, only reads `auth`.
   */
  async appHandleLogin(email: string, password: string): Promise<AuthResult> {
    appGate(this, appAuthScopeFor(email));
    return handleLogin(this, email, password);
  }

  /** Folder contents (files + sub-folders). */
  async appListFiles(
    userId: string,
    parentId: string | null
  ): Promise<{ files: UserFile[]; folders: Folder[] }> {
    appGate(this, appScopeFor(userId));
    const files = listFiles(this, userId, parentId);
    const folders = listFolders(this, userId, parentId);
    return { files, folders };
  }

  /** Soft-delete a file. Decrements quota by the file's size. */
  async appDeleteFile(
    fileId: string,
    userId: string
  ): Promise<{ ok: true } | { ok: false; reason: "not_found" }> {
    appGateWrite(this, appScopeFor(userId));
    const file = getFile(this, fileId);
    if (!file) return { ok: false, reason: "not_found" };
    deleteFile(this, fileId);
    const fileSize = (file.file_size as number) ?? 0;
    updateUsage(this, userId, -fileSize, -1);
    return { ok: true };
  }

  /**
   * Resolve an absolute VFS path to its `files` row. The canonical
   * write path inserts file rows keyed by (user_id, parent_id,
   * file_name); this is the inverse — used by the `/api/index/file`
   * SPA callback to translate the just-written path back to a fileId
   * for the search-index pipeline.
   */
  async appResolveFileByPath(
    userId: string,
    path: string
  ): Promise<AppFileRow | null> {
    appGate(this, appScopeFor(userId));
    if (!path.startsWith("/")) return null;

    // Walk segments — descend through `folders` for each non-leaf,
    // then read the `files` row for the leaf.
    const segments = path.split("/").filter((s) => s.length > 0);
    if (segments.length === 0) return null;
    const leafName = segments[segments.length - 1];
    const dirSegments = segments.slice(0, -1);

    let parentId: string | null = null;
    for (const dir of dirSegments) {
      const folderRow = this.sql
        .exec(
          "SELECT folder_id FROM folders WHERE user_id = ? AND IFNULL(parent_id, '') = IFNULL(?, '') AND name = ?",
          userId,
          parentId,
          dir
        )
        .toArray()[0] as { folder_id: string } | undefined;
      if (!folderRow) return null;
      parentId = folderRow.folder_id;
    }

    // Phase 25 — tombstone-consistency. The index callback resolves
    // a path → fileId then re-fires `indexFile`, which reads bytes.
    // If the head version is tombstoned the read would throw; just
    // return null here so the index POST 404s instead of triggering
    // a downstream byte-read that explodes.
    const fileRow = this.sql
      .exec(
        `SELECT f.*
           FROM files f
           LEFT JOIN file_versions fv
             ON fv.path_id = f.file_id AND fv.version_id = f.head_version_id
          WHERE f.user_id = ?
            AND IFNULL(f.parent_id, '') = IFNULL(?, '')
            AND f.file_name = ?
            AND f.status != 'deleted'
            AND (f.head_version_id IS NULL OR fv.deleted IS NULL OR fv.deleted = 0)`,
        userId,
        parentId,
        leafName
      )
      .toArray()[0] as Record<string, unknown> | undefined;
    if (!fileRow) return null;

    return {
      file_id: fileRow.file_id as string,
      user_id: fileRow.user_id as string,
      parent_id: (fileRow.parent_id as string | null) ?? null,
      file_name: fileRow.file_name as string,
      file_size: fileRow.file_size as number,
      file_hash: (fileRow.file_hash as string) ?? "",
      mime_type: fileRow.mime_type as string,
      chunk_size: (fileRow.chunk_size as number) ?? 0,
      chunk_count: (fileRow.chunk_count as number) ?? 0,
      pool_size: (fileRow.pool_size as number) ?? 32,
      status: fileRow.status as string,
      created_at: fileRow.created_at as number,
      updated_at: fileRow.updated_at as number,
      deleted_at: (fileRow.deleted_at as number | null) ?? null,
    };
  }

  /**
   * Reconstruct the absolute VFS path + mimeType for a `files.file_id`
   * by walking the `parent_id` chain through the `folders` table.
   * Returns null when the file row is missing or soft-deleted.
   *
   * Used by gallery + shared-album routes to translate the App's
   * fileId-keyed URLs (`GET /api/gallery/image/:fileId`) into the
   * `path` + `mimeType` that the canonical VFS read APIs need.
   */
  async appGetFilePath(
    fileId: string
  ): Promise<{
    path: string;
    mimeType: string;
    /**
     * Phase 36 \u2014 cache-key bust token. Returned so HTTP-cache layers
     * (gallery thumbnail / shared image / preview) can include this
     * in the cache-key path component. Every write that mutates
     * bytes-or-metadata bumps `files.updated_at` (via the various
     * UPDATE files SET ... updated_at = ? sites in vfs/* and
     * vfs-versions.ts), so a stale cached response is never served
     * after a write. Non-versioned and versioned paths both update
     * this column.
     */
    updatedAt: number;
  } | null> {
    // Pre-gate ensureInit so the persisted-scope lookup below sees
    // the schema. We can't pass userId in without breaking the route
    // caller signature; recover it from `vfs_meta.scope` (set by any
    // prior gated app* call) and fall back to deriving from the
    // file row itself when scope hasn't been persisted yet (first
    // call on a fresh DO whose only prior touch was an unmigrated
    // path).
    this.ensureInit();
    appGateFromPersistedScope(this);
    // Phase 25 — tombstone-consistency. Gallery/shared-album routes
    // call this then immediately read bytes via canonical VFS. A
    // tombstoned head would 404 with the wrong error class
    // (downstream "head version is a tombstone" instead of "no such
    // file"); returning null here gives the route a clean 404 path.
    const fileRow = this.sql
      .exec(
        `SELECT f.parent_id, f.file_name, f.mime_type, f.updated_at
           FROM files f
           LEFT JOIN file_versions fv
             ON fv.path_id = f.file_id AND fv.version_id = f.head_version_id
          WHERE f.file_id = ?
            AND f.status != 'deleted'
            AND (f.head_version_id IS NULL OR fv.deleted IS NULL OR fv.deleted = 0)`,
        fileId
      )
      .toArray()[0] as
      | {
          parent_id: string | null;
          file_name: string;
          mime_type: string;
          updated_at: number;
        }
      | undefined;
    if (!fileRow) return null;

    const segments: string[] = [fileRow.file_name];
    let cursor: string | null = fileRow.parent_id ?? null;
    // Bound the walk — practical hierarchies stay shallow; the cap
    // protects against pathological cycles in malformed rows.
    for (let i = 0; i < 256 && cursor !== null; i++) {
      const folderRow = this.sql
        .exec(
          "SELECT parent_id, name FROM folders WHERE folder_id = ?",
          cursor
        )
        .toArray()[0] as { parent_id: string | null; name: string } | undefined;
      if (!folderRow) return null; // dangling parent_id
      segments.unshift(folderRow.name);
      cursor = folderRow.parent_id ?? null;
    }
    return {
      path: "/" + segments.join("/"),
      mimeType: fileRow.mime_type ?? "application/octet-stream",
      updatedAt: fileRow.updated_at,
    };
  }

  /**
   * Raw file row read (used by the public `/api/shared/...` route).
   * Returns the snake_case columns unchanged so callers can pass it
   * straight through to wire shapes that expect the SQLite column
   * naming.
   */
  async appGetFile(fileId: string): Promise<AppFileRow | null> {
    // Same scope-recovery pattern as appGetFilePath — caller route
    // routes to the correct DO via `userStub(c.env, userId)` so the
    // DO instance is already tenant-scoped; we recover the userId
    // for rate-limit accounting from `vfs_meta.scope`.
    this.ensureInit();
    appGateFromPersistedScope(this);
    const row = getFile(this, fileId);
    if (!row) return null;
    // Project to the typed shape — DO RPC strips index signatures so
    // we can't return the raw `Record<string, unknown>` opaquely.
    return {
      file_id: row.file_id as string,
      user_id: row.user_id as string,
      parent_id: (row.parent_id as string | null) ?? null,
      file_name: row.file_name as string,
      file_size: row.file_size as number,
      file_hash: (row.file_hash as string) ?? "",
      mime_type: row.mime_type as string,
      chunk_size: (row.chunk_size as number) ?? 0,
      chunk_count: (row.chunk_count as number) ?? 0,
      pool_size: (row.pool_size as number) ?? 32,
      status: row.status as string,
      created_at: row.created_at as number,
      updated_at: row.updated_at as number,
      deleted_at: (row.deleted_at as number | null) ?? null,
    };
  }

  /** Insert a folder row. */
  async appCreateFolder(
    userId: string,
    name: string,
    parentId: string | null
  ): Promise<Folder> {
    appGateWrite(this, appScopeFor(userId));
    return createFolder(this, userId, name, parentId);
  }

  /**
   * Breadcrumb path from root to the given folderId.
   *
   * Signature does not carry userId (caller routes via per-userId
   * stub); rate-limit via the persisted scope.
   */
  async appGetFolderPath(folderId: string | null): Promise<Folder[]> {
    this.ensureInit();
    appGateFromPersistedScope(this);
    return getFolderPath(this, folderId);
  }

  /**
   * All files for a user, regardless of folder. Used by the search
   * reindex pipeline. `status='deleted'` rows are excluded; the
   * caller filters further on `status === 'complete'` if needed.
   *
   * Phase 25 — tombstone-consistency: rows whose `head_version_id`
   * points at a `deleted=1` `file_versions` row are EXCLUDED so
   * downstream consumers (e.g. `indexFile`) don't try to read bytes
   * for an unlinked path. Mirrors the canonical `vfsListFiles`
   * default (`includeTombstones=false`).
   */
  async appListAllFiles(userId: string): Promise<UserFile[]> {
    appGate(this, appScopeFor(userId));
    const rows = this.sql
      .exec(
        `SELECT f.*
           FROM files f
           LEFT JOIN file_versions fv
             ON fv.path_id = f.file_id AND fv.version_id = f.head_version_id
          WHERE f.user_id = ?
            AND f.status != 'deleted'
            AND (f.head_version_id IS NULL OR fv.deleted IS NULL OR fv.deleted = 0)
          ORDER BY f.created_at DESC`,
        userId
      )
      .toArray();
    return rows.map((r: Record<string, unknown>) => ({
      fileId: r.file_id as string,
      fileName: r.file_name as string,
      fileSize: r.file_size as number,
      fileHash: r.file_hash as string,
      mimeType: r.mime_type as string,
      chunkCount: r.chunk_count as number,
      status: r.status as UserFile["status"],
      parentId: (r.parent_id as string) || null,
      createdAt: r.created_at as number,
      updatedAt: r.updated_at as number,
    }));
  }

  /**
   * All complete image files across all folders, newest first.
   *
   * Phase 25 — tombstone-consistency: same filter as `appListAllFiles`.
   * The gallery surface absolutely cannot show unlinked-under-versioning
   * photos: those photos' chunks are still referenced (versioning
   * preserves history) but the user has expressed intent to hide them.
   */
  async appGetGalleryPhotos(userId: string): Promise<GalleryPhoto[]> {
    appGate(this, appScopeFor(userId));
    const rows = this.sql
      .exec(
        `SELECT f.file_id, f.file_name, f.file_size, f.mime_type,
                f.parent_id, f.created_at, f.updated_at
           FROM files f
           LEFT JOIN file_versions fv
             ON fv.path_id = f.file_id AND fv.version_id = f.head_version_id
          WHERE f.user_id = ?
            AND f.status = 'complete'
            AND f.mime_type LIKE 'image/%'
            AND (f.head_version_id IS NULL OR fv.deleted IS NULL OR fv.deleted = 0)
          ORDER BY f.created_at DESC`,
        userId
      )
      .toArray();

    return rows.map((r: Record<string, unknown>) => ({
      fileId: r.file_id as string,
      fileName: r.file_name as string,
      fileSize: r.file_size as number,
      mimeType: r.mime_type as string,
      parentId: (r.parent_id as string) || null,
      createdAt: r.created_at as number,
      updatedAt: r.updated_at as number,
    }));
  }

  /** Aggregated per-user analytics: files, mime distribution, shards. */
  async appGetUserStats(userId: string): Promise<UserStats> {
    appGate(this, appScopeFor(userId));
    return this.getUserStats(userId);
  }

  /** Read the user's quota row. */
  async appGetQuota(userId: string): Promise<QuotaInfo> {
    appGate(this, appScopeFor(userId));
    return getQuota(this, userId);
  }

  // ── Search-index reconciler (Phase 23 Blindspot fix) ──────────────
  //
  // The SPA's POST /api/index/file is fire-and-forget on
  // `executionCtx.waitUntil`. If the SPA crashes between
  // `multipart/finalize` and the index POST, OR if the worker isolate
  // is evicted mid-`indexFile`, the file lands in canonical VFS but
  // is never search-indexed → silent search miss for the file's
  // lifetime.
  //
  // The reconciler closes the gap: every committed file gets an
  // `indexed_at` timestamp on success; periodic alarm sweeps the
  // `indexed_at IS NULL` set and re-fires `indexFile`.

  /**
   * Mark a file as successfully indexed. Called from `indexFile`
   * after both text-space and CLIP-space (when applicable) upserts
   * complete without error.
   */
  async appMarkFileIndexed(fileId: string): Promise<void> {
    // Per-tenant rate limit via persisted scope (route caller routes
    // through per-userId stub but the signature only takes fileId).
    //
    // Read-class gate intentionally (not write-class): the operation
    // is `UPDATE files SET indexed_at = ?` on an existing row, which
    // does NOT risk the H6 partial-UNIQUE-INDEX collision class
    // (that's about `(parent_id, file_name)` insertion). The EBUSY
    // marker therefore does not need to gate this call.
    this.ensureInit();
    appGateFromPersistedScope(this);
    this.sql.exec(
      "UPDATE files SET indexed_at = ? WHERE file_id = ?",
      Date.now(),
      fileId
    );
  }

  /**
   * Return up to `limit` files that are committed (status='complete')
   * but have not yet been search-indexed. Ordered oldest-first to
   * give freshly-uploaded files some grace before the reconciler
   * picks them up; this keeps reconciler load low under steady-state
   * (where most files index promptly via the SPA callback).
   */
  async appListUnindexedFiles(
    userId: string,
    limit: number
  ): Promise<
    Array<{
      file_id: string;
      file_name: string;
      mime_type: string;
      file_size: number;
    }>
  > {
    appGate(this, appScopeFor(userId));
    // Phase 25 — tombstone-consistency. Skip files whose head version
    // is tombstoned: re-firing `indexFile` on them would attempt a
    // byte read that throws "head version is a tombstone" downstream.
    // The file's previously-indexed embeddings remain in the vector
    // store keyed by file_id (acceptable: an unlinked file
    // shouldn't surface in search anyway, and the next live write
    // would re-fire indexing under a fresh fileId).
    const rows = this.sql
      .exec(
        `SELECT f.file_id, f.file_name, f.mime_type, f.file_size
           FROM files f
           LEFT JOIN file_versions fv
             ON fv.path_id = f.file_id AND fv.version_id = f.head_version_id
          WHERE f.user_id = ?
            AND f.status = 'complete'
            AND f.indexed_at IS NULL
            AND (f.head_version_id IS NULL OR fv.deleted IS NULL OR fv.deleted = 0)
          ORDER BY f.created_at ASC
          LIMIT ?`,
        userId,
        Math.min(Math.max(1, limit), 100)
      )
      .toArray() as Array<{
        file_id: string;
        file_name: string;
        mime_type: string;
        file_size: number;
      }>;
    return rows;
  }

  // ── Account deletion ─────────────────────────────────────────────
  //
  // Two-DO purge: the data DO (`vfs:default:<userId>`) holds files,
  // folders, quota, yjs state, and dispatches chunk-GC RPCs to all
  // ShardDOs that hold this user's bytes. The auth DO (`auth:<email>`)
  // holds the password hash. The route layer is responsible for
  // calling BOTH — we expose the two halves as separate typed RPCs
  // so the route can sequence them.
  //
  // Idempotent: re-running on an already-wiped DO is a no-op (all
  // SELECTs return empty; the RPC reports 0 affected rows).

  /**
   * Wipe ALL data for a tenant from this UserDO. Hard-deletes every
   * file row through the canonical `hardDeleteFileRow` path so chunks
   * are dispatched to ShardDOs for refcount-decrement and eventual
   * sweep. Drops folders, file_versions, file_tags, file_variants,
   * yjs_oplog/yjs_meta, vfs_dirent, multipart staging, and zeroes out
   * the quota row.
   *
   * SAFETY: callable only with the userId already authenticated by
   * the App route. The DO has no concept of admin keys — it trusts
   * its caller (the App route's authMiddleware).
   *
   * Returns the count of file rows + folder rows + version rows
   * removed for verification by the caller.
   */
  async appWipeAccountData(userId: string): Promise<{
    filesRemoved: number;
    foldersRemoved: number;
    versionsRemoved: number;
    chunksRemovedFromShards: number;
  }> {
    // Admin-class call. Route at `worker/app/routes/auth.ts:196`
    // gates the public-facing endpoint with `authMiddleware()` which
    // verifies the bearer JWT before this RPC runs. The DO trusts
    // its caller (no separate admin-key system at the DO level —
    // adding one here would require plumbing a second secret across
    // every wipe call site, out of scope for this fix). The gate
    // here is per-tenant rate limit + write-degraded refusal:
    // defense against repeated/replayed wipe attempts hitting the
    // same DO faster than the bucket allows.
    appGateWrite(this, appScopeFor(userId));

    // Lazy import to keep the App-side bundle's Core dependency
    // explicit. `hardDeleteFileRow` lives in core; we call it with
    // the canonical (userId, scope) pair — scope is reconstructed
    // from the DO's persisted scope row.
    const { hardDeleteFileRow } = await import(
      "@core/objects/user/vfs/write-commit"
    );

    // Reconstruct the scope. For App tenants we know it's
    // (ns="default", tenant=userId, sub=undefined) — that's how
    // `auth-bridge` mints the VFS token (see
    // `worker/app/routes/auth.ts:155`). The `vfs_meta` row may not
    // exist if the DO is brand-new and never had a VFS call run; in
    // that case there's nothing to wipe and we short-circuit.
    const scope = { ns: "default", tenant: userId } as const;

    // Pull every file_id (including soft-deleted and uploading rows
    // — wipe means EVERYTHING). hardDeleteFileRow handles each.
    const fileRows = this.sql
      .exec(
        "SELECT file_id FROM files WHERE user_id = ?",
        userId
      )
      .toArray() as { file_id: string }[];

    let chunksRemoved = 0;
    for (const { file_id } of fileRows) {
      try {
        // Best-effort: count chunks before delete for telemetry.
        const c = this.sql
          .exec(
            "SELECT COUNT(*) AS n FROM file_chunks WHERE file_id = ?",
            file_id
          )
          .toArray()[0] as { n: number } | undefined;
        if (c) chunksRemoved += c.n;
        await hardDeleteFileRow(this, userId, scope, file_id);
      } catch {
        // Continue purging even if one file's shard fan-out fails;
        // worst case is a small chunk-orphan that the alarm sweeper
        // catches on the next tick.
      }
    }

    // Folder rows.
    const folderRowCount = (
      this.sql
        .exec("SELECT COUNT(*) AS n FROM folders WHERE user_id = ?", userId)
        .toArray()[0] as { n: number }
    ).n;
    this.sql.exec("DELETE FROM folders WHERE user_id = ?", userId);

    // Version rows.
    let versionRowCount = 0;
    try {
      versionRowCount = (
        this.sql
          .exec(
            "SELECT COUNT(*) AS n FROM file_versions WHERE user_id = ?",
            userId
          )
          .toArray()[0] as { n: number }
      ).n;
      this.sql.exec("DELETE FROM file_versions WHERE user_id = ?", userId);
    } catch {
      // file_versions may not exist on older schemas; ignore.
    }

    // Tags / variants / dirents — best-effort drops.
    for (const sql of [
      "DELETE FROM file_tags WHERE user_id = ?",
      "DELETE FROM file_variants WHERE user_id = ?",
      "DELETE FROM vfs_dirent WHERE user_id = ?",
      "DELETE FROM yjs_oplog WHERE user_id = ?",
      "DELETE FROM yjs_meta WHERE user_id = ?",
      "DELETE FROM upload_sessions WHERE user_id = ?",
    ]) {
      try {
        this.sql.exec(sql, userId);
      } catch {
        // Table may not exist (schema-version-dependent); ignore.
      }
    }

    // Quota: zero out (don't drop the row — keeps schema invariants
    // if the same userId ever signs up again on this DO instance,
    // though in practice the email→userId mapping makes that
    // impossible).
    this.sql.exec(
      "UPDATE quota SET storage_used = 0, file_count = 0, pool_size = 32 WHERE user_id = ?",
      userId
    );

    return {
      filesRemoved: fileRows.length,
      foldersRemoved: folderRowCount,
      versionsRemoved: versionRowCount,
      chunksRemovedFromShards: chunksRemoved,
    };
  }

  /**
   * Wipe the auth row on the auth DO. Called against the
   * `auth:<email>` DO; drops the password hash + auth identity.
   * Idempotent — returns false if no row matched.
   */
  async appWipeAuthRow(email: string): Promise<{ removed: boolean }> {
    // Admin-class call against the auth-keyed DO (`auth:<email>`).
    // Same rationale as appWipeAccountData: route gates the public
    // surface; this is the per-account rate limiter that bounds
    // repeated wipe attempts against a single email.
    //
    // Read-class gate intentionally (not write-class): the auth DO
    // has its own UNIQUE INDEX on `email`, not the H6 partial
    // `(parent_id, file_name)` UNIQUE INDEX on `files`/`folders`.
    // EBUSY-on-H6 therefore does not apply here.
    appGate(this, appAuthScopeFor(email));
    const before = (
      this.sql
        .exec("SELECT COUNT(*) AS n FROM auth WHERE email = ?", email)
        .toArray()[0] as { n: number }
    ).n;
    this.sql.exec("DELETE FROM auth WHERE email = ?", email);
    return { removed: before > 0 };
  }

  // ── Internal aggregator ──────────────────────────────────────────

  private getUserStats(userId: string): UserStats {
    // Total files and storage
    const quotaRows = this.sql
      .exec("SELECT * FROM quota WHERE user_id = ?", userId)
      .toArray();

    const quota =
      quotaRows.length > 0
        ? (quotaRows[0] as {
            storage_used: number;
            storage_limit: number;
            file_count: number;
            pool_size: number;
          })
        : {
            storage_used: 0,
            storage_limit: 107374182400,
            file_count: 0,
            pool_size: 32,
          };

    // Files by status
    const statusRows = this.sql
      .exec(
        "SELECT status, COUNT(*) as count FROM files WHERE user_id = ? GROUP BY status",
        userId
      )
      .toArray() as { status: string; count: number }[];

    const filesByStatus: UserFilesByStatus = {
      uploading: 0,
      complete: 0,
      failed: 0,
      deleted: 0,
    };
    for (const row of statusRows) {
      if (row.status in filesByStatus) {
        filesByStatus[row.status as keyof UserFilesByStatus] = row.count;
      }
    }

    // Derive totalFiles from actual files table for consistency —
    // the quota row may be stale or missing, so count non-deleted files directly
    const actualFileCount =
      filesByStatus.complete + filesByStatus.uploading + filesByStatus.failed;

    // Mime type distribution
    const mimeRows = this.sql
      .exec(
        "SELECT mime_type, COUNT(*) as count, COALESCE(SUM(file_size), 0) as total_size FROM files WHERE user_id = ? AND status != 'deleted' GROUP BY mime_type ORDER BY count DESC",
        userId
      )
      .toArray() as { mime_type: string; count: number; total_size: number }[];

    const mimeDistribution: MimeDistribution[] = mimeRows.map((r) => ({
      mimeType: r.mime_type,
      count: r.count,
      totalSize: r.total_size,
    }));

    // Recent uploads (last 10)
    const recentRows = this.sql
      .exec(
        "SELECT file_id, file_name, file_size, mime_type, status, created_at FROM files WHERE user_id = ? ORDER BY created_at DESC LIMIT 10",
        userId
      )
      .toArray() as {
      file_id: string;
      file_name: string;
      file_size: number;
      mime_type: string;
      status: string;
      created_at: number;
    }[];

    const recentUploads: RecentUpload[] = recentRows.map((r) => ({
      fileId: r.file_id,
      fileName: r.file_name,
      fileSize: r.file_size,
      mimeType: r.mime_type,
      status: r.status,
      createdAt: r.created_at,
    }));

    // Shard distribution
    const shardRows = this.sql
      .exec(
        "SELECT fc.shard_index, COUNT(*) as chunk_count FROM file_chunks fc JOIN files f ON fc.file_id = f.file_id WHERE f.user_id = ? GROUP BY fc.shard_index ORDER BY fc.shard_index",
        userId
      )
      .toArray() as { shard_index: number; chunk_count: number }[];

    const shardDistribution: ShardDistribution[] = shardRows.map((r) => ({
      shardIndex: r.shard_index,
      chunkCount: r.chunk_count,
    }));

    return {
      totalFiles: Math.max(actualFileCount, quota.file_count),
      totalStorageUsed: quota.storage_used,
      quotaLimit: quota.storage_limit,
      poolSize: quota.pool_size ?? 32,
      filesByStatus,
      mimeDistribution,
      recentUploads,
      shardDistribution,
    };
  }
}

// Reaffirm the EnvApp import is used (TS would warn unused otherwise).
export type { EnvApp };
