import { DurableObject } from "cloudflare:workers";
import type { EnvApp } from "@shared/types";
import type {
  UserStats,
  UserFilesByStatus,
  MimeDistribution,
  RecentUpload,
  ShardDistribution,
} from "../../types";
import { handleSignup, handleLogin } from "./auth";
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

/**
 * `UserDO` is the App-side subclass of `UserDOCore`. It
 * inherits the entire VFS RPC surface (vfsReadFile/Write/Stat/...,
 * versioning, Yjs WebSocket, alarm sweep, rate-limit gates, schema
 * migrations) from Core and ADDS the legacy photo-app HTTP routes
 * via `_legacyFetch`.
 *
 * Class-name preservation: production wrangler still binds
 * `class_name: "UserDO"` and migration tags remain v1
 * (UserDO, ShardDO) and v2 (SearchDO). The runtime DO namespace is
 * keyed by class name so storage on the existing app at
 * mossaic.ashishkumarsingh.com is untouched.
 *
 * The `_legacyFetch` body bytes are byte-pinned to a sha256 hash
 * — see the docstring on the function for the audit hash.
 */
export class UserDO extends UserDOCore {
  /**
   * Override Core's `fetch` to dispatch non-WS HTTP traffic to the
   * legacy handler. WebSocket upgrades fall through to Core's
   * implementation via `super.fetch`.
   */
  override async fetch(request: Request): Promise<Response> {
    if (request.headers.get("Upgrade")?.toLowerCase() === "websocket") {
      return super.fetch(request);
    }
    return this._legacyFetch(request);
  }

  /**
   * Legacy photo-app HTTP handler. The BODY bytes
   * (lines 70..263 inclusive — between the opening `{` and the
   * closing `}` of `_legacyFetch`) are byte-pinned at sha256
   * `4c6eb84925cd8b34298aa92a5201c6e8074defb4527c3bbb1d2c677f9f2c8e70`.
   *
   * Audit script:
   *
   *     awk 'NR>=70 && NR<=263' worker/app/objects/user/user-do.ts | sha256sum
   *
   * The pin exists because the legacy app at
   * mossaic.ashishkumarsingh.com speaks this exact JSON shape over
   * the DO's `fetch()` surface. Any change to the body — even a
   * cosmetic reorder — must update both the hash and this comment
   * AND be called out in the commit message.
   */
  private async _legacyFetch(request: Request): Promise<Response> {
    this.ensureInit();

    const url = new URL(request.url);
    const path = url.pathname;
    const method = request.method;

    try {
      // Auth routes
      if (path === "/signup" && method === "POST") {
        const { email, password } = (await request.json()) as {
          email: string;
          password: string;
        };
        const result = await handleSignup(this, email, password);
        return Response.json(result);
      }

      if (path === "/login" && method === "POST") {
        const { email, password } = (await request.json()) as {
          email: string;
          password: string;
        };
        const result = await handleLogin(this, email, password);
        return Response.json(result);
      }

      // File routes
      if (path === "/files/create" && method === "POST") {
        const body = (await request.json()) as {
          userId: string;
          fileName: string;
          fileSize: number;
          mimeType: string;
          parentId?: string | null;
        };
        if (!checkQuota(this, body.userId, body.fileSize)) {
          return Response.json({ error: "Quota exceeded" }, { status: 403 });
        }
        const result = createFile(
          this,
          body.userId,
          body.fileName,
          body.fileSize,
          body.mimeType,
          body.parentId ?? null
        );
        return Response.json(result);
      }

      if (path === "/files/chunk" && method === "POST") {
        const body = (await request.json()) as {
          fileId: string;
          chunkIndex: number;
          chunkHash: string;
          chunkSize: number;
          shardIndex: number;
        };
        recordChunk(
          this,
          body.fileId,
          body.chunkIndex,
          body.chunkHash,
          body.chunkSize,
          body.shardIndex
        );
        return Response.json({ ok: true });
      }

      if (path === "/files/complete" && method === "POST") {
        const body = (await request.json()) as {
          fileId: string;
          fileHash: string;
          userId: string;
          fileSize: number;
        };
        completeFile(this, body.fileId, body.fileHash);
        updateUsage(this, body.userId, body.fileSize, 1);
        return Response.json({ ok: true });
      }

      if (path.startsWith("/files/manifest/")) {
        const fileId = path.split("/")[3];
        const manifest = getFileManifest(this, fileId);
        if (!manifest) {
          return Response.json({ error: "File not found" }, { status: 404 });
        }
        return Response.json(manifest);
      }

      if (path === "/files/list" && method === "POST") {
        const body = (await request.json()) as {
          userId: string;
          parentId?: string | null;
        };
        const files = listFiles(this, body.userId, body.parentId ?? null);
        const folders = listFolders(this, body.userId, body.parentId ?? null);
        return Response.json({ files, folders });
      }

      if (path.startsWith("/files/delete/") && method === "DELETE") {
        const fileId = path.split("/")[3];
        const file = getFile(this, fileId);
        if (!file) {
          return Response.json({ error: "File not found" }, { status: 404 });
        }
        deleteFile(this, fileId);
        const userId = request.headers.get("X-User-Id") || "";
        updateUsage(this, userId, -(file.file_size as number), -1);
        return Response.json({ ok: true });
      }

      if (path.startsWith("/files/get/")) {
        const fileId = path.split("/")[3];
        const file = getFile(this, fileId);
        if (!file) {
          return Response.json({ error: "File not found" }, { status: 404 });
        }
        return Response.json(file);
      }

      // Folder routes
      if (path === "/folders/create" && method === "POST") {
        const body = (await request.json()) as {
          userId: string;
          name: string;
          parentId?: string | null;
        };
        const folder = createFolder(
          this,
          body.userId,
          body.name,
          body.parentId ?? null
        );
        return Response.json(folder);
      }

      if (path.startsWith("/folders/list") && method === "POST") {
        const body = (await request.json()) as {
          userId: string;
          parentId?: string | null;
        };
        const folders = listFolders(this, body.userId, body.parentId ?? null);
        return Response.json(folders);
      }

      if (path.startsWith("/folders/path/")) {
        const folderId = path.split("/")[3] || null;
        const folderPath = getFolderPath(this, folderId);
        return Response.json(folderPath);
      }

      // Gallery route — list all images across all folders
      if (path === "/gallery/photos" && method === "POST") {
        const body = (await request.json()) as { userId: string };
        const rows = this.sql
          .exec(
            "SELECT file_id, file_name, file_size, mime_type, parent_id, created_at, updated_at FROM files WHERE user_id = ? AND status = 'complete' AND mime_type LIKE 'image/%' ORDER BY created_at DESC",
            body.userId
          )
          .toArray();

        const photos = rows.map((r: Record<string, unknown>) => ({
          fileId: r.file_id as string,
          fileName: r.file_name as string,
          fileSize: r.file_size as number,
          mimeType: r.mime_type as string,
          parentId: (r.parent_id as string) || null,
          createdAt: r.created_at as number,
          updatedAt: r.updated_at as number,
        }));

        return Response.json({ photos });
      }

      // Stats route
      if (path === "/stats" && method === "POST") {
        const body = (await request.json()) as { userId: string };
        const stats = this.getUserStats(body.userId);
        return Response.json(stats);
      }

      // Quota routes
      if (path === "/quota" && method === "POST") {
        const body = (await request.json()) as { userId: string };
        const quota = getQuota(this, body.userId);
        return Response.json(quota);
      }

      return Response.json({ error: "Not found" }, { status: 404 });
    } catch (err) {
      const message = err instanceof Error ? err.message : "Internal error";
      return Response.json({ error: message }, { status: 500 });
    }
  }

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
