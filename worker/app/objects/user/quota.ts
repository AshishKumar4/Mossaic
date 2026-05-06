import type { UserDO } from "./user-do";
import type { QuotaInfo } from "@shared/types";
import { recordWriteUsage } from "@core/objects/user/vfs-ops";

/**
 * Get quota info for a user.
 */
export function getQuota(durableObject: UserDO, userId: string): QuotaInfo {
  const rows = durableObject.sql
    .exec("SELECT * FROM quota WHERE user_id = ?", userId)
    .toArray();

  if (rows.length === 0) {
    return {
      storageUsed: 0,
      storageLimit: 107374182400,
      fileCount: 0,
      poolSize: 32,
    };
  }

  const q = rows[0] as Record<string, unknown>;
  return {
    storageUsed: q.storage_used as number,
    storageLimit: q.storage_limit as number,
    fileCount: q.file_count as number,
    poolSize: q.pool_size as number,
  };
}

/**
 * Update storage usage after file upload/deletion.
 *
 * Thin App-side wrapper around canonical `recordWriteUsage` so the
 * App's `appDeleteFile` path stays accounting-consistent with the
 * canonical write paths (commitInlineTier / commitChunkedTier /
 * vfsFinalizeMultipart) which all go through the same primitive.
 *
 * Pool growth: `recordWriteUsage` recomputes `pool_size` from the
 * post-update `storage_used` total via `computePoolSize` and writes
 * the new value back if it grew.
 */
export function updateUsage(
  durableObject: UserDO,
  userId: string,
  deltaBytes: number,
  deltaFiles: number
): void {
  recordWriteUsage(durableObject, userId, deltaBytes, deltaFiles);
}
