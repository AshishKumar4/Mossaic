import { DurableObject } from "cloudflare:workers";
import type {
  Env,
  UserStats,
  UserFilesByStatus,
  MimeDistribution,
  RecentUpload,
  ShardDistribution,
} from "@shared/types";
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
import { createFolder, listFolders, getFolder, getFolderPath } from "./folders";
import { getQuota, checkQuota, updateUsage } from "./quota";

export class UserDO extends DurableObject<Env> {
  sql: SqlStorage;
  private initialized = false;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.sql = ctx.storage.sql;
  }

  private ensureInit(): void {
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
  }

  async fetch(request: Request): Promise<Response> {
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
