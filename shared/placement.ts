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
 *
 * Phase 28.1 — TODO: cap-aware placement. The current implementation
 * picks the pure-rendezvous winner with no fullness check, so a
 * shard that's at the soft cap (9 GB) keeps receiving chunks until
 * pool growth (Phase 23 `recordWriteUsage`) widens the rendezvous
 * space. Pool growth IS the primary capacity mechanism, but in a
 * burst-write scenario where many chunks land before the next 5 GB
 * boundary trips, individual shards could approach the workerd
 * SQLite ceiling.
 *
 * The Phase 28 follow-up will accept an optional `skip?: Set<number>`
 * (shards over softCap, populated from the `monitorShardCapacity`
 * cache) and fall over to next-best rendezvous score. Backward
 * compat: when `skip` is absent or empty, behaviour is unchanged
 * (deterministic). For now, the warning at
 * `worker/core/objects/user/shard-capacity.ts` gives operators
 * visibility into approaching-cap conditions before any user-visible
 * write failure.
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
 * Compute pool size based on total storage used.
 */
export function computePoolSize(storageUsedBytes: number): number {
  const BASE_POOL = 32;
  const BYTES_PER_SHARD = 5 * 1024 * 1024 * 1024; // 5 GB
  const additional = Math.floor(storageUsedBytes / BYTES_PER_SHARD);
  return BASE_POOL + additional;
}
