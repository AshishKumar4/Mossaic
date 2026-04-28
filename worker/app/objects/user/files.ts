import type { UserDO } from "./user-do";
import type { UserFile, FileManifest, ChunkSpec } from "@shared/types";
import { generateId } from "@core/lib/utils";
import { computeChunkSpec } from "@shared/chunking";
import { placeChunk } from "@shared/placement";

/**
 * Create a new file record in uploading state. Returns the file ID.
 */
export function createFile(
  durableObject: UserDO,
  userId: string,
  fileName: string,
  fileSize: number,
  mimeType: string,
  parentId: string | null
): { fileId: string; chunkSize: number; chunkCount: number; poolSize: number } {
  const fileId = generateId();
  const { chunkSize, chunkCount } = computeChunkSpec(fileSize);
  const now = Date.now();

  // Get current pool size
  const quotaRows = durableObject.sql
    .exec("SELECT pool_size FROM quota WHERE user_id = ?", userId)
    .toArray();
  const poolSize = quotaRows.length > 0 ? (quotaRows[0] as { pool_size: number }).pool_size : 32;

  durableObject.sql.exec(
    `INSERT INTO files (file_id, user_id, parent_id, file_name, file_size, file_hash, mime_type, chunk_size, chunk_count, pool_size, status, created_at, updated_at)
     VALUES (?, ?, ?, ?, ?, '', ?, ?, ?, ?, 'uploading', ?, ?)`,
    fileId,
    userId,
    parentId,
    fileName,
    fileSize,
    mimeType,
    chunkSize,
    chunkCount,
    poolSize,
    now,
    now
  );

  return { fileId, chunkSize, chunkCount, poolSize };
}

/**
 * Record a chunk upload completion.
 */
export function recordChunk(
  durableObject: UserDO,
  fileId: string,
  chunkIndex: number,
  chunkHash: string,
  chunkSize: number,
  shardIndex: number
): void {
  durableObject.sql.exec(
    `INSERT OR REPLACE INTO file_chunks (file_id, chunk_index, chunk_hash, chunk_size, shard_index)
     VALUES (?, ?, ?, ?, ?)`,
    fileId,
    chunkIndex,
    chunkHash,
    chunkSize,
    shardIndex
  );
}

/**
 * Complete a file upload. Updates status and file hash.
 */
export function completeFile(
  durableObject: UserDO,
  fileId: string,
  fileHash: string
): void {
  const now = Date.now();
  durableObject.sql.exec(
    `UPDATE files SET status = 'complete', file_hash = ?, updated_at = ? WHERE file_id = ?`,
    fileHash,
    now,
    fileId
  );
}

/**
 * Get a file manifest for download.
 */
export function getFileManifest(
  durableObject: UserDO,
  fileId: string
): FileManifest | null {
  const fileRows = durableObject.sql
    .exec(
      "SELECT * FROM files WHERE file_id = ? AND status = 'complete'",
      fileId
    )
    .toArray();

  if (fileRows.length === 0) return null;

  const file = fileRows[0] as Record<string, unknown>;

  // VFS additions (sdk-impl-plan §11): when inline_data is non-NULL we
  // skip the file_chunks lookup entirely. Legacy rows have NULL/undefined
  // here so behavior is unchanged.
  const inlineData = (file.inline_data ?? null) as ArrayBuffer | null;
  const nodeKind =
    ((file.node_kind as string | undefined) ?? "file") === "symlink"
      ? "symlink"
      : "file";
  const symlinkTarget = (file.symlink_target as string | null | undefined) ?? null;
  const mode = (file.mode as number | undefined) ?? 420;

  let chunks: ChunkSpec[] = [];
  if (inlineData === null && nodeKind !== "symlink") {
    const chunkRows = durableObject.sql
      .exec(
        "SELECT chunk_index, chunk_hash, chunk_size, shard_index FROM file_chunks WHERE file_id = ? ORDER BY chunk_index",
        fileId
      )
      .toArray();

    chunks = chunkRows.map((c: Record<string, unknown>) => ({
      index: c.chunk_index as number,
      offset: (c.chunk_index as number) * (file.chunk_size as number),
      size: c.chunk_size as number,
      hash: c.chunk_hash as string,
      shardIndex: c.shard_index as number,
    }));
  }

  return {
    fileId: file.file_id as string,
    fileName: file.file_name as string,
    fileSize: file.file_size as number,
    fileHash: file.file_hash as string,
    mimeType: file.mime_type as string,
    chunkSize: file.chunk_size as number,
    chunkCount: file.chunk_count as number,
    poolSize: file.pool_size as number,
    chunks,
    createdAt: file.created_at as number,
    mode,
    nodeKind,
    symlinkTarget,
    inlineData,
  };
}

/**
 * List files in a folder.
 */
export function listFiles(
  durableObject: UserDO,
  userId: string,
  parentId: string | null
): UserFile[] {
  const rows = parentId
    ? durableObject.sql
        .exec(
          "SELECT * FROM files WHERE user_id = ? AND parent_id = ? AND status != 'deleted' ORDER BY created_at DESC",
          userId,
          parentId
        )
        .toArray()
    : durableObject.sql
        .exec(
          "SELECT * FROM files WHERE user_id = ? AND parent_id IS NULL AND status != 'deleted' ORDER BY created_at DESC",
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
 * Delete a file (soft delete).
 */
export function deleteFile(durableObject: UserDO, fileId: string): boolean {
  const now = Date.now();
  const rows = durableObject.sql
    .exec("SELECT file_size FROM files WHERE file_id = ?", fileId)
    .toArray();
  if (rows.length === 0) return false;

  durableObject.sql.exec(
    "UPDATE files SET status = 'deleted', deleted_at = ?, updated_at = ? WHERE file_id = ?",
    now,
    now,
    fileId
  );
  return true;
}

/**
 * Get file record by ID.
 */
export function getFile(
  durableObject: UserDO,
  fileId: string
): Record<string, unknown> | null {
  const rows = durableObject.sql
    .exec("SELECT * FROM files WHERE file_id = ?", fileId)
    .toArray();
  return rows.length > 0 ? (rows[0] as Record<string, unknown>) : null;
}
