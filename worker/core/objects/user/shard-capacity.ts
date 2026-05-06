/**
 * Shard capacity monitor (Phase 28 Fix 4 — warning-only mode).
 *
 * Polls each ShardDO in the user's pool for `bytesStored` and logs
 * a structured warning when any shard's storage exceeds the soft
 * cap (9 GB by convention; the workerd SQLite-backed DO ceiling is
 * around 10 GB and we want operators to see the approach BEFORE
 * writes start failing).
 *
 * **Architectural status — DEFERRED to Phase 28.1**: this is the
 * back-pressure precursor. Cap-aware placement (skip full shards in
 * `placeChunk`, fall over to next-best rendezvous score) requires a
 * persistent per-shard capacity cache + alarm-driven refresh +
 * threading the cache through every callsite. That's substantial
 * cross-cutting plumbing; we ship the warning signal first so:
 *
 *   1. Operators get visibility into approaching-cap conditions
 *      via Workers logs / Logpush before any user-visible write
 *      failure.
 *   2. Phase 23's pool growth (`recordWriteUsage`) remains the
 *      primary capacity mechanism — pool grows by 1 ShardDO per
 *      5 GB stored, so a tenant approaching the soft cap on any
 *      individual shard is already at sub-cap on the rest of the
 *      pool. The warning is the operator's signal to verify pool
 *      growth is keeping pace.
 *   3. The eventual cap-aware placement (Phase 28.1) can read the
 *      same `bytesStored` telemetry without schema changes — we
 *      just add a cache table + refresh cadence.
 *
 * Invocation: from the UserDO alarm, throttled by a `vfs_meta` row
 * keyed `shard_capacity_last_check`. Default cadence: 1 hour.
 *
 * Cost: one ShardDO RPC per shard in the pool. For poolSize=32
 * that's 32 fan-out reads — well under the 50/1000 subrequest cap
 * and bounded to one alarm tick per hour.
 */

import type { UserDOCore } from "./user-do-core";
import type { ShardDO } from "../shard/shard-do";
import type { VFSScope } from "../../../../shared/vfs-types";
import { vfsShardDOName } from "../../lib/utils";
import { logWarn } from "../../lib/logger";

/** Soft cap matches `ShardDO.getStorageBytes`'s published value. */
const SOFT_CAP_BYTES = 9 * 1024 * 1024 * 1024;

/**
 * Min interval between capacity polls.
 *
 * Phase 32 Fix 4 \u2014 lowered from 1h \u2192 30 min so the cache
 * tracking shard fullness for `placeChunk`'s skip logic stays
 * fresh. A shard at 8.5 GB is still well below the soft cap on
 * one poll and over it on the next; tighter cadence shrinks the
 * window where a near-full shard receives extra writes that
 * could push it past the runtime SQLite ceiling.
 */
const CHECK_INTERVAL_MS = 30 * 60 * 1000;

export interface ShardCapacitySnapshot {
  shardIndex: number;
  bytesStored: number;
  uniqueChunks: number;
  exceedsCap: boolean;
}

/**
 * Poll every shard in the tenant's pool for bytesStored. Returns
 * snapshots for ALL shards (even non-exceeders) so the caller can
 * surface the full picture if needed; logs a structured warning for
 * each shard at-or-over `softCapBytes`.
 *
 * Honors the `vfs_meta` throttle: returns an empty array (no work
 * done) if `force === false` and the last check was less than
 * `CHECK_INTERVAL_MS` ago. Pass `force: true` from tests / admin
 * tooling to bypass the throttle.
 */
export async function monitorShardCapacity(
  durableObject: UserDOCore,
  scope: VFSScope,
  poolSize: number,
  opts: { force?: boolean } = {}
): Promise<ShardCapacitySnapshot[]> {
  const force = opts.force === true;
  const now = Date.now();

  if (!force) {
    const last = durableObject.sql
      .exec(
        "SELECT value FROM vfs_meta WHERE key = 'shard_capacity_last_check'"
      )
      .toArray()[0] as { value: string } | undefined;
    const lastMs = last ? Number(last.value) : 0;
    if (now - lastMs < CHECK_INTERVAL_MS) return [];
  }

  // Stamp the throttle timestamp BEFORE the fan-out so a partial
  // failure doesn't cause back-to-back retries on the next alarm.
  durableObject.sql.exec(
    "INSERT OR REPLACE INTO vfs_meta (key, value) VALUES ('shard_capacity_last_check', ?)",
    String(now)
  );

  const env = durableObject.envPublic;
  const shardNs = env.MOSSAIC_SHARD as unknown as DurableObjectNamespace<ShardDO>;

  const snapshots: ShardCapacitySnapshot[] = await Promise.all(
    Array.from({ length: poolSize }, async (_unused, sIdx) => {
      const shardName = vfsShardDOName(scope.ns, scope.tenant, scope.sub, sIdx);
      const stub = shardNs.get(shardNs.idFromName(shardName));
      try {
        const stats = await stub.getStorageBytes();
        const exceeds = stats.bytesStored >= SOFT_CAP_BYTES;
        return {
          shardIndex: sIdx,
          bytesStored: stats.bytesStored,
          uniqueChunks: stats.uniqueChunks,
          exceedsCap: exceeds,
        };
      } catch {
        // Transient shard failure — skip; the next poll will retry.
        return {
          shardIndex: sIdx,
          bytesStored: -1,
          uniqueChunks: -1,
          exceedsCap: false,
        };
      }
    })
  );

  // Phase 32 Fix 4 \u2014 persist into shard_storage_cache. Reachable
  // shards have their measured bytes recorded; transient
  // failures (bytesStored = -1 sentinel from the catch above) are
  // skipped \u2014 the next poll retries. INSERT OR REPLACE keeps
  // the row count bounded by poolSize; refreshed_at is updated
  // every poll so a stale-cache check could cull entries older
  // than CHECK_INTERVAL_MS \xd7 2 (not implemented this phase \u2014
  // pool_size shrinking is forbidden, so stale rows are at worst
  // cosmetic).
  for (const s of snapshots) {
    if (s.bytesStored < 0) continue;
    durableObject.sql.exec(
      `INSERT OR REPLACE INTO shard_storage_cache
         (shard_index, bytes_stored, refreshed_at)
       VALUES (?, ?, ?)`,
      s.shardIndex,
      s.bytesStored,
      now
    );
  }

  // Structured warning per offending shard. Logpush picks these up
  // with grep-friendly fields. We log per-shard rather than batched
  // so the operator can see the histogram (which shards are hot,
  // which aren't) at a glance.
  const tenantId = scope.sub
    ? `${scope.ns}::${scope.tenant}::${scope.sub}`
    : `${scope.ns}::${scope.tenant}`;
  for (const s of snapshots) {
    if (s.exceedsCap) {
      logWarn(
        "shard capacity soft cap exceeded",
        { tenantId },
        {
          event: "shard_capacity_soft_cap_exceeded",
          shardIndex: s.shardIndex,
          bytesStored: s.bytesStored,
          softCapBytes: SOFT_CAP_BYTES,
          uniqueChunks: s.uniqueChunks,
          // Phase 32 Fix 4 \u2014 placement skips this shard. Pool
          // growth (Phase 23 `recordWriteUsage`) is still the
          // primary capacity mechanism; this warning surfaces
          // shards that are at-cap so operators can verify
          // growth is keeping pace.
          phase: "32-cap-aware",
        }
      );
    }
  }

  return snapshots;
}

/**
 * Constant exported for tests so they can assert the soft-cap
 * value the warning fires at.
 */
export const SHARD_SOFT_CAP_BYTES = SOFT_CAP_BYTES;

/**
 * Phase 32 Fix 4 \u2014 read the persisted full-shard set from
 * `shard_storage_cache`. Each caller of `placeChunk` invokes
 * this once per write batch (one SQL query per batch \u2014
 * negligible) and threads the resulting set through every
 * `placeChunk` call in the batch. Cold cache (no rows) returns
 * an empty set, which preserves the deterministic placement
 * fast path in `placeChunk`.
 */
export function loadFullShards(
  durableObject: UserDOCore
): ReadonlySet<number> {
  const rows = durableObject.sql
    .exec(
      "SELECT shard_index FROM shard_storage_cache WHERE bytes_stored >= ?",
      SOFT_CAP_BYTES
    )
    .toArray() as { shard_index: number }[];
  if (rows.length === 0) return EMPTY_FULL_SHARDS;
  return new Set(rows.map((r) => r.shard_index));
}

const EMPTY_FULL_SHARDS: ReadonlySet<number> = new Set<number>();
