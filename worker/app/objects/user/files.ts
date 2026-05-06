import type { UserDO } from "./user-do";
import type { UserFile } from "@shared/types";
import { FILE_HEAD_JOIN } from "@core/objects/user/vfs/helpers";

/**
 * List files in a folder.
 *
 * Tombstone-consistency: rows whose `head_version_id` points at a
 * `deleted=1` `file_versions` row are EXCLUDED. This matches the
 * canonical `vfsListFiles` default. The user-visible App listing
 * surface (`/api/files`) must not surface unlinked-under-versioning
 * paths because every consumer would then call `vfsStat` on them
 * and hit the "head version is a tombstone" throw at
 * `helpers.ts:245`.
 */
export function listFiles(
  durableObject: UserDO,
  userId: string,
  parentId: string | null
): UserFile[] {
  const tombstoneFilter =
    "(f.head_version_id IS NULL OR fv.deleted IS NULL OR fv.deleted = 0)";
  const rows = parentId
    ? durableObject.sql
        .exec(
          `SELECT f.*
             FROM files f
             ${FILE_HEAD_JOIN}
            WHERE f.user_id = ? AND f.parent_id = ?
              AND f.status != 'deleted'
              AND ${tombstoneFilter}
            ORDER BY f.created_at DESC`,
          userId,
          parentId
        )
        .toArray()
    : durableObject.sql
        .exec(
          `SELECT f.*
             FROM files f
             ${FILE_HEAD_JOIN}
            WHERE f.user_id = ? AND f.parent_id IS NULL
              AND f.status != 'deleted'
              AND ${tombstoneFilter}
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
 *
 * Tombstone-consistency: returns `null` when the head version is
 * tombstoned, so all downstream consumers (`appGetFile`,
 * public-share routes, gallery image fetch) return a clean 404
 * instead of throwing the "head version is a tombstone" error from
 * a downstream byte read. Admin/recovery surfaces that need to see
 * tombstones can read `files` directly via SQL.
 */
export function getFile(
  durableObject: UserDO,
  fileId: string
): Record<string, unknown> | null {
  const rows = durableObject.sql
    .exec(
      `SELECT f.*
         FROM files f
         ${FILE_HEAD_JOIN}
        WHERE f.file_id = ?
          AND (f.head_version_id IS NULL OR fv.deleted IS NULL OR fv.deleted = 0)`,
      fileId
    )
    .toArray();
  return rows.length > 0 ? (rows[0] as Record<string, unknown>) : null;
}
