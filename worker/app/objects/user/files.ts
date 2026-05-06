import type { UserDO } from "./user-do";
import type { UserFile } from "@shared/types";

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
