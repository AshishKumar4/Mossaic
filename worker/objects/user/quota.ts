import type { UserDO } from "./user-do";
import type { QuotaInfo } from "@shared/types";
import { computePoolSize } from "@shared/placement";

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
 * Check if a user has enough quota for additional bytes.
 */
export function checkQuota(
  durableObject: UserDO,
  userId: string,
  additionalBytes: number
): boolean {
  const quota = getQuota(durableObject, userId);
  return quota.storageUsed + additionalBytes <= quota.storageLimit;
}

/**
 * Update storage usage after file upload/deletion.
 */
export function updateUsage(
  durableObject: UserDO,
  userId: string,
  deltaBytes: number,
  deltaFiles: number
): void {
  durableObject.sql.exec(
    `UPDATE quota SET storage_used = storage_used + ?, file_count = file_count + ? WHERE user_id = ?`,
    deltaBytes,
    deltaFiles,
    userId
  );

  // Check if pool needs to grow
  const rows = durableObject.sql
    .exec(
      "SELECT storage_used, pool_size FROM quota WHERE user_id = ?",
      userId
    )
    .toArray();

  if (rows.length > 0) {
    const q = rows[0] as { storage_used: number; pool_size: number };
    const newPoolSize = computePoolSize(q.storage_used);
    if (newPoolSize > q.pool_size) {
      durableObject.sql.exec(
        "UPDATE quota SET pool_size = ? WHERE user_id = ?",
        newPoolSize,
        userId
      );
    }
  }
}
