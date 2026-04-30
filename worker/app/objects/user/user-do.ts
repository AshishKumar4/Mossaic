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
  getFileManifest,
  listFiles,
  deleteFile,
  getFile,
} from "./files";
import { createFolder, listFolders, getFolderPath } from "./folders";
import { getQuota, updateUsage } from "./quota";
import { UserDOCore } from "@core/objects/user/user-do-core";

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

  // ── App-only RPCs ────────────────────────────────────────────────
  //
  // All methods below are TYPED RPCs callable from App routes via
  // `stub.appXxx(...)` over the DO RPC binding. Each calls
  // `this.ensureInit()` so first-touch on a fresh DO instance
  // materializes the schema.

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
    this.ensureInit();
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

    const fileRow = this.sql
      .exec(
        "SELECT * FROM files WHERE user_id = ? AND IFNULL(parent_id, '') = IFNULL(?, '') AND file_name = ? AND status != 'deleted'",
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
  ): Promise<{ path: string; mimeType: string } | null> {
    this.ensureInit();
    const fileRow = this.sql
      .exec(
        "SELECT parent_id, file_name, mime_type FROM files WHERE file_id = ? AND status != 'deleted'",
        fileId
      )
      .toArray()[0] as
      | { parent_id: string | null; file_name: string; mime_type: string }
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
    };
  }

  /**
   * Raw file row read (used by the public `/api/shared/...` route).
   * Returns the snake_case columns unchanged so callers can pass it
   * straight through to wire shapes that expect the SQLite column
   * naming.
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
