import type { EnvApp } from "@shared/types";
import type {
  UserStats,
  UserFilesByStatus,
  MimeDistribution,
  RecentUpload,
  ShardDistribution,
  GalleryPhoto,
} from "../../types";
import type { UserFile, FileManifest, Folder, QuotaInfo } from "@shared/types";
import { handleSignup, handleLogin, type AuthResult } from "./auth";
import {
  createFile,
  recordChunk,
  completeFile,
  getFileManifest,
  listFiles,
  deleteFile,
  getFile,
} from "./files";
import { createFolder, listFolders, getFolderPath } from "./folders";
import { getQuota, checkQuota, updateUsage } from "./quota";
import { UserDOCore } from "@core/objects/user/user-do-core";
// multipart upload helpers (App-side variants).
import {
  appBeginMultipart as appBeginMultipartImpl,
  appAbortMultipart as appAbortMultipartImpl,
  appFinalizeMultipart as appFinalizeMultipartImpl,
  appGetMultipartStatus as appGetMultipartStatusImpl,
  appOpenManifest as appOpenManifestImpl,
  type AppBeginMultipartOpts,
} from "./multipart";
import type {
  MultipartBeginResponse,
  MultipartFinalizeResponse,
} from "@shared/multipart";

/**
 * Shape returned by {@link UserDO.appCreateFile}. Mirrors the
 * legacy `/files/create` JSON response so the SPA's
 * `UploadInitResponse` shape is preserved end-to-end.
 */
export interface AppCreateFileResult {
  fileId: string;
  chunkSize: number;
  chunkCount: number;
  poolSize: number;
}

/**
 * Typed projection of a `files` table row. Exposed for callers of
 * {@link UserDO.appGetFile}; matches the snake_case column names of
 * the underlying SQLite schema so the App routes can pass it
 * straight through to legacy wire shapes (e.g. the public share
 * endpoint at `/api/shared/:token/photos`).
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
 *
 * ── `_legacyFetch` removal ───────────────────────────────
 *
 * Historically, all app-only operations were dispatched through a
 * hand-rolled JSON router named `_legacyFetch` (193 byte-pinned
 * lines, sha256 `4c6eb84925cd8b34298aa92a5201c6e8074defb4527c3bbb1d2c677f9f2c8e70`).
 * Routes called `stub.fetch("http://internal/files/create", ...)` and
 * parsed the JSON reply.
 *
 * That handler is gone. The typed-RPC methods on this class
 * (`appHandleSignup`, `appCreateFile`, etc.) replace it.
 * App routes call them directly as `stub.appCreateFile(...)`,
 * eliminating one JSON parse + one URL allocation per call and
 * making the surface type-checked end-to-end.
 *
 * The byte-pinned hash is intentionally retired because every caller
 * has been migrated; the unreachable code is removed rather than
 * preserved as dead bytes. Callers that still address the old
 * `http://internal/...` URLs (none exist in this tree) would now
 * receive 404 from `super.fetch(request)`.
 *
 * Production data is unaffected: the typed RPCs hit the same SQLite
 * tables (`auth`, `files`, `file_chunks`, `folders`, `quota`) on the
 * same DO instance via the same helpers.
 */
export class UserDO extends UserDOCore {
  /**
   * Override Core's `fetch` to surface the WebSocket upgrade path
   * through `super.fetch` (Yjs collab WS). Non-WS HTTP requests fall
   * through to Core's default 404 — the legacy JSON router that
   * handled them was retired with the App-on-SDK refactor.
   */
  override async fetch(request: Request): Promise<Response> {
    return super.fetch(request);
  }

  // ── App-only RPCs ────────────────────────────────────────────────
  //
  // All methods below are TYPED RPCs callable from App routes via
  // `stub.appXxx(...)` over the DO RPC binding. They replace the
  // legacy `_legacyFetch` JSON router 1:1 in semantics.
  //
  // Each method calls `this.ensureInit()` so first-touch on a fresh
  // DO instance materializes the schema, exactly like the legacy
  // path did (preserves first-signup behavior on a brand-new user).

  /** Create user, hash password, insert quota row. Returns userId+email. */
  async appHandleSignup(email: string, password: string): Promise<AuthResult> {
    this.ensureInit();
    return handleSignup(this, email, password);
  }

  /** Verify credentials. Returns userId+email on success; throws on failure. */
  async appHandleLogin(email: string, password: string): Promise<AuthResult> {
    this.ensureInit();
    return handleLogin(this, email, password);
  }

  /**
   * Insert a new uploading-state file row. Returns chunk spec + the
   * user's current pool size for placement. Throws on quota exceeded.
   */
  async appCreateFile(
    userId: string,
    fileName: string,
    fileSize: number,
    mimeType: string,
    parentId: string | null
  ): Promise<AppCreateFileResult> {
    this.ensureInit();
    if (!checkQuota(this, userId, fileSize)) {
      throw new Error("Quota exceeded");
    }
    return createFile(this, userId, fileName, fileSize, mimeType, parentId);
  }

  /** Record a successfully-uploaded chunk in `file_chunks`. */
  async appRecordChunk(
    fileId: string,
    chunkIndex: number,
    chunkHash: string,
    chunkSize: number,
    shardIndex: number
  ): Promise<void> {
    this.ensureInit();
    recordChunk(this, fileId, chunkIndex, chunkHash, chunkSize, shardIndex);
  }

  /**
   * Flip status='complete' on the file row, stamp file_hash, and bump
   * the user's quota row by the file size.
   */
  async appCompleteFile(
    fileId: string,
    fileHash: string,
    userId: string,
    fileSize: number
  ): Promise<void> {
    this.ensureInit();
    completeFile(this, fileId, fileHash);
    updateUsage(this, userId, fileSize, 1);
  }

  // ── multipart upload RPCs (legacy schema) ──────────────
  //
  // Mirror the canonical `vfsBeginMultipart`/`vfsFinalizeMultipart`/
  // `vfsAbortMultipart`/`vfsGetMultipartStatus`/`vfsOpenManifest`
  // surface but adapted to the App's legacy `files`/`file_chunks`
  // schema and `legacyAppPlacement` shard naming. The App-pinned
  // multipart route at `/api/upload/multipart/*` calls these via the
  // typed DO RPC binding.

  /** begin a multipart upload session against legacy schema. */
  async appBeginMultipart(
    userId: string,
    path: string,
    opts: AppBeginMultipartOpts
  ): Promise<MultipartBeginResponse> {
    this.ensureInit();
    return appBeginMultipartImpl(this, userId, path, opts);
  }

  /** abort a multipart upload session. Idempotent. */
  async appAbortMultipart(
    userId: string,
    uploadId: string
  ): Promise<{ ok: true }> {
    this.ensureInit();
    return appAbortMultipartImpl(this, userId, uploadId);
  }

  /** finalize a multipart upload — verify + flip to complete. */
  async appFinalizeMultipart(
    userId: string,
    uploadId: string,
    chunkHashList: readonly string[]
  ): Promise<MultipartFinalizeResponse> {
    this.ensureInit();
    return appFinalizeMultipartImpl(this, userId, uploadId, chunkHashList);
  }

  /** probe the status of an open multipart session. */
  async appGetMultipartStatus(
    userId: string,
    uploadId: string
  ): Promise<{
    landed: number[];
    total: number;
    bytesUploaded: number;
    expiresAtMs: number;
  }> {
    this.ensureInit();
    return appGetMultipartStatusImpl(this, userId, uploadId);
  }

  /** open a download manifest for a finalized App file. */
  async appOpenManifest(fileId: string): Promise<{
    fileId: string;
    size: number;
    chunkSize: number;
    chunkCount: number;
    chunks: Array<{ index: number; hash: string; size: number }>;
    inlined: boolean;
    mimeType: string;
  }> {
    this.ensureInit();
    return appOpenManifestImpl(this, fileId);
  }

  /** Read the file row + its chunks for download manifest. */
  async appGetFileManifest(fileId: string): Promise<FileManifest | null> {
    this.ensureInit();
    return getFileManifest(this, fileId);
  }

  /** Folder contents (files + sub-folders). */
  async appListFiles(
    userId: string,
    parentId: string | null
  ): Promise<{ files: UserFile[]; folders: Folder[] }> {
    this.ensureInit();
    const files = listFiles(this, userId, parentId);
    const folders = listFolders(this, userId, parentId);
    return { files, folders };
  }

  /** Soft-delete a file. Decrements quota by the file's size. */
  async appDeleteFile(
    fileId: string,
    userId: string
  ): Promise<{ ok: true } | { ok: false; reason: "not_found" }> {
    this.ensureInit();
    const file = getFile(this, fileId);
    if (!file) return { ok: false, reason: "not_found" };
    deleteFile(this, fileId);
    const fileSize = (file.file_size as number) ?? 0;
    updateUsage(this, userId, -fileSize, -1);
    return { ok: true };
  }

  /**
   * Raw file row read (used by the public `/api/shared/...` route
   * and the upload-complete handler). Returns the snake_case columns
   * unchanged (the DB rows are passed through with no rename so the
   * shape matches what the legacy SQL helpers produce).
   */
  async appGetFile(fileId: string): Promise<AppFileRow | null> {
    this.ensureInit();
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
    this.ensureInit();
    return createFolder(this, userId, name, parentId);
  }

  /** Folders sharing a parent. */
  async appListFolders(
    userId: string,
    parentId: string | null
  ): Promise<Folder[]> {
    this.ensureInit();
    return listFolders(this, userId, parentId);
  }

  /** Breadcrumb path from root to the given folderId. */
  async appGetFolderPath(folderId: string | null): Promise<Folder[]> {
    this.ensureInit();
    return getFolderPath(this, folderId);
  }

  /**
   * All files for a user, regardless of folder. Used by the search
   * reindex pipeline. `status='deleted'` rows are excluded; the
   * caller filters further on `status === 'complete'` if needed.
   */
  async appListAllFiles(userId: string): Promise<UserFile[]> {
    this.ensureInit();
    const rows = this.sql
      .exec(
        "SELECT * FROM files WHERE user_id = ? AND status != 'deleted' ORDER BY created_at DESC",
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

  /** All complete image files across all folders, newest first. */
  async appGetGalleryPhotos(userId: string): Promise<GalleryPhoto[]> {
    this.ensureInit();
    const rows = this.sql
      .exec(
        "SELECT file_id, file_name, file_size, mime_type, parent_id, created_at, updated_at FROM files WHERE user_id = ? AND status = 'complete' AND mime_type LIKE 'image/%' ORDER BY created_at DESC",
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
    this.ensureInit();
    return this.getUserStats(userId);
  }

  /** Read the user's quota row. */
  async appGetQuota(userId: string): Promise<QuotaInfo> {
    this.ensureInit();
    return getQuota(this, userId);
  }

  // ── Internal aggregator (was inline in legacy fetch) ─────────────

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
          })
        : { storage_used: 0, storage_limit: 107374182400, file_count: 0 };

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
      filesByStatus,
      mimeDistribution,
      recentUploads,
      shardDistribution,
    };
  }
}

// Reaffirm the EnvApp import is used (TS would warn unused otherwise).
export type { EnvApp };
