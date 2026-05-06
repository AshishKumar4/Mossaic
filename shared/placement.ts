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
 * Sentinel returned by `placeChunk` when every shard in the pool
 * is in `fullShards`. Caller's contract: trigger pool growth and
 * retry placement with the new `poolSize`. Negative-int sentinel
 * keeps the return type `number` and avoids a Maybe wrapper on
 * the hot write path.
 */
export const POOL_FULL = -1;

/**
 * Determine which shard index holds a specific chunk.
 *
 * **Cap-aware placement.** The pure-rendezvous winner is checked
 * against `fullShards` (a snapshot of shards at-or-over the soft
 * cap, populated by `worker/core/objects/user/shard-capacity.ts`).
 * If the winner is full, the loop continues at the next-best score
 * until either a non-full shard is found OR every shard in the
 * pool is full (returns `POOL_FULL`). Caller responsibilities:
 *  - When `fullShards` is `undefined` or empty: behaviour is
 *    byte-equivalent to the pure-rendezvous deterministic top-1
 *    winner. ALL existing tests + readers depending on
 *    deterministic placement see the same result.
 *  - When `placeChunk` returns `POOL_FULL`: trigger pool growth
 *    via `recordWriteUsage` (writing one chunk normally would not
 *    advance pool_size by itself; the operator may need to bump
 *    quota.storage_used to force a 5 GiB-boundary cross), then
 *    retry placement with the new poolSize.
 *
 * Reads are UNCHANGED: every chunk's recorded `shard_index` in
 * `file_chunks` / `version_chunks` stays valid forever. A "full"
 * shard continues to serve reads; only writes are rerouted.
 *
 * FULLY DETERMINISTIC given identical (userId, fileId,
 * chunkIndex, poolSize, fullShards) tuple. The non-determinism
 * comes from the cache itself, which can change as shards fill
 * over time \u2014 that's expected.
 */
export function placeChunk(
  userId: string,
  fileId: string,
  chunkIndex: number,
  poolSize: number,
  fullShards?: ReadonlySet<number>
): number {
  // Fast path: no skip-set or empty skip-set — byte-equivalent to
  // the pure-rendezvous deterministic top-1 winner. The hot write
  // path (every chunk PUT) takes this branch when the cache is
  // cold or empty.
  if (!fullShards || fullShards.size === 0) {
    let bestShard = 0;
    let bestScore = -1;
    for (let shard = 0; shard < poolSize; shard++) {
      const score = placementScore(
        fileId,
        chunkIndex,
        scoreKey(userId, shard)
      );
      if (score > bestScore) {
        bestScore = score;
        bestShard = shard;
      }
    }
    return bestShard;
  }

  // Slow path: skip-aware. Score every shard in the pool, sort
  // descending, return the first non-full one. Bounded by
  // poolSize; placement work scales O(poolSize \xb7 log poolSize)
  // which is fine: poolSize is already small (\u2264 ~256 in any
  // realistic deployment) and chunk PUTs are not in the inner
  // loop of any per-byte iteration.
  const scored: { shard: number; score: number }[] = [];
  for (let shard = 0; shard < poolSize; shard++) {
    scored.push({
      shard,
      score: placementScore(fileId, chunkIndex, scoreKey(userId, shard)),
    });
  }
  scored.sort((a, b) => b.score - a.score);
  for (const s of scored) {
    if (!fullShards.has(s.shard)) return s.shard;
  }
  // Every shard in the pool is full. Caller triggers pool growth.
  return POOL_FULL;
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
