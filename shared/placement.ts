import { murmurhash3 } from "./hash";

/**
 * Build the score-key template for chunk placement. **This is NOT a
 * DO instance name** — actual DO addressing uses
 * `vfsUserDOName` / `vfsShardDOName` from `worker/core/lib/utils.ts`.
 *
 * The score-key template is forever-pinned: changing it orphans every
 * existing chunk because rendezvous hashing keys off this string.
 */
function scoreKey(userId: string, shardIndex: number): string {
  return `shard:${userId}:${shardIndex}`;
}

/**
 * Compute placement score for a chunk on a specific shard.
 * Higher score = higher priority for placement (rendezvous hashing).
 */
function placementScore(
  fileId: string,
  chunkIndex: number,
  shardId: string
): number {
  const key = `${fileId}:${chunkIndex}:${shardId}`;
  return murmurhash3(key);
}

/**
 * Determine which shard index holds a specific chunk.
 * FULLY DETERMINISTIC: depends only on (userId, fileId, chunkIndex, poolSize).
 * No network calls. No state lookups.
 */
export function placeChunk(
  userId: string,
  fileId: string,
  chunkIndex: number,
  poolSize: number
): number {
  let bestShard = 0;
  let bestScore = -1;

  for (let shard = 0; shard < poolSize; shard++) {
    const shardId = scoreKey(userId, shard);
    const score = placementScore(fileId, chunkIndex, shardId);
    if (score > bestScore) {
      bestScore = score;
      bestShard = shard;
    }
  }

  return bestShard;
}

/**
 * Place all chunks of a file and return a map of chunkIndex → shardIndex.
 */
export function placeFile(
  userId: string,
  fileId: string,
  chunkCount: number,
  poolSize: number
): Map<number, number> {
  const placement = new Map<number, number>();
  for (let i = 0; i < chunkCount; i++) {
    placement.set(i, placeChunk(userId, fileId, i, poolSize));
  }
  return placement;
}

/**
 * Compute pool size based on total storage used.
 */
export function computePoolSize(storageUsedBytes: number): number {
  const BASE_POOL = 32;
  const BYTES_PER_SHARD = 5 * 1024 * 1024 * 1024; // 5 GB
  const additional = Math.floor(storageUsedBytes / BYTES_PER_SHARD);
  return BASE_POOL + additional;
}
