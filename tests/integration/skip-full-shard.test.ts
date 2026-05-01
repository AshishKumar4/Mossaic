import { describe, it, expect } from "vitest";
import { env, runInDurableObject } from "cloudflare:test";
import { vfsUserDOName } from "@core/lib/utils";
import type { ShardDO } from "@core/objects/shard/shard-do";
import {
  placeChunk,
  POOL_FULL,
} from "@shared/placement";

/**
 * Phase 32 Fix 4 — cap-aware placement.
 *
 * The pre-Phase-32 placement was pure rendezvous: a shard at the
 * soft cap kept receiving chunks until the next pool-growth
 * boundary. Post-fix:
 *   - `placeChunk(..., fullShards)` skips shards in `fullShards`
 *     and returns the next-best score.
 *   - All shards in `fullShards` → returns `POOL_FULL` sentinel.
 *   - Empty / undefined `fullShards` → byte-equivalent to
 *     pre-Phase-32 deterministic top-1 winner.
 *   - The UserDO `shard_storage_cache` table mirrors per-shard
 *     bytes; `loadFullShards` reads it.
 *
 * Cases:
 *   SF1. Empty fullShards → deterministic, identical to legacy.
 *   SF2. Full primary winner → fall through to next-best score.
 *   SF3. All shards full → POOL_FULL sentinel.
 *   SF4. Cache refresh updates the full-shard set.
 *   SF5. Reads still work for chunks pinned to a now-full shard
 *        (placement is a write-side concern, never a read-side
 *        one).
 *   SF6. Determinism: same (inputs, fullShards) ⇒ same output.
 */

interface E {
  MOSSAIC_USER: DurableObjectNamespace;
  MOSSAIC_SHARD: DurableObjectNamespace<ShardDO>;
}
const E = env as unknown as E;
const NS = "default";

describe("Phase 32 Fix 4 — cap-aware placement", () => {
  it("SF1 — empty fullShards is byte-equivalent to legacy placement", () => {
    const legacy = placeChunk("user-sf1", "file-1", 0, 32);
    const phase32 = placeChunk("user-sf1", "file-1", 0, 32, new Set<number>());
    const phase32Undef = placeChunk("user-sf1", "file-1", 0, 32);
    expect(phase32).toBe(legacy);
    expect(phase32Undef).toBe(legacy);
    // Sanity: result is in [0, poolSize).
    expect(legacy).toBeGreaterThanOrEqual(0);
    expect(legacy).toBeLessThan(32);
  });

  it("SF2 — full primary winner falls through to next-best score", () => {
    // Compute the legacy winner, then mark it full and verify a
    // different shard wins.
    const winner = placeChunk("user-sf2", "file-2", 0, 32);
    const skipped = placeChunk(
      "user-sf2",
      "file-2",
      0,
      32,
      new Set([winner])
    );
    expect(skipped).not.toBe(winner);
    expect(skipped).toBeGreaterThanOrEqual(0);
    expect(skipped).toBeLessThan(32);
  });

  it("SF3 — every shard full returns POOL_FULL sentinel", () => {
    const fullAll = new Set<number>();
    for (let s = 0; s < 8; s++) fullAll.add(s);
    const result = placeChunk("user-sf3", "file-3", 0, 8, fullAll);
    expect(result).toBe(POOL_FULL);
  });

  it("SF4 — cache refresh updates the full-shard set persistently", async () => {
    const tenant = "sf4-cache-refresh";
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
    );
    // Trigger ensureInit.
    await stub.vfsExists({ ns: NS, tenant }, "/");

    const SOFT_CAP = 9 * 1024 * 1024 * 1024;

    // Seed the cache: shard 5 is full, shard 7 is half.
    await runInDurableObject(stub, async (_inst, state) => {
      state.storage.sql.exec(
        "INSERT OR REPLACE INTO shard_storage_cache (shard_index, bytes_stored, refreshed_at) VALUES (?, ?, ?)",
        5,
        SOFT_CAP + 1,
        Date.now()
      );
      state.storage.sql.exec(
        "INSERT OR REPLACE INTO shard_storage_cache (shard_index, bytes_stored, refreshed_at) VALUES (?, ?, ?)",
        7,
        SOFT_CAP / 2,
        Date.now()
      );
    });

    // loadFullShards picks up shard 5 only.
    const { loadFullShards } = await import(
      "@core/objects/user/shard-capacity"
    );
    const fullSet = await runInDurableObject(stub, async (inst) => {
      return Array.from(loadFullShards(inst as never));
    });
    expect(fullSet).toEqual([5]);

    // Update shard 7 to full; loadFullShards reflects both.
    await runInDurableObject(stub, async (_inst, state) => {
      state.storage.sql.exec(
        "UPDATE shard_storage_cache SET bytes_stored = ? WHERE shard_index = 7",
        SOFT_CAP + 100
      );
    });
    const fullSet2 = await runInDurableObject(stub, async (inst) => {
      return Array.from(loadFullShards(inst as never)).sort();
    });
    expect(fullSet2).toEqual([5, 7]);
  });

  it("SF5 — reads still work for chunks pinned to a now-full shard", async () => {
    const tenant = "sf5-read-after-full";
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
    );
    const scope = { ns: NS, tenant };

    const bytes = new Uint8Array(20 * 1024).fill(7);
    await stub.vfsWriteFile(scope, "/old.bin", bytes);

    // Find which shard the chunk landed on.
    const recordedShard = await runInDurableObject(
      stub,
      async (_inst, state) => {
        const r = state.storage.sql
          .exec(
            `SELECT shard_index FROM file_chunks fc
              JOIN files f ON f.file_id = fc.file_id
             WHERE f.file_name = 'old.bin' LIMIT 1`
          )
          .toArray()[0] as { shard_index: number };
        return r.shard_index;
      }
    );

    // Mark that shard FULL in the cache. A subsequent read still
    // works \u2014 placement is write-side only; readers go to the
    // recorded `shard_index` regardless.
    const SOFT_CAP = 9 * 1024 * 1024 * 1024;
    await runInDurableObject(stub, async (_inst, state) => {
      state.storage.sql.exec(
        "INSERT OR REPLACE INTO shard_storage_cache (shard_index, bytes_stored, refreshed_at) VALUES (?, ?, ?)",
        recordedShard,
        SOFT_CAP + 1,
        Date.now()
      );
    });

    const back = await stub.vfsReadFile(scope, "/old.bin");
    expect(new Uint8Array(back)).toEqual(bytes);
  });

  it("SF6 — placement is deterministic given (inputs, fullShards)", () => {
    const full = new Set([1, 4, 7, 12]);
    const a = placeChunk("user-sf6", "file-6", 0, 32, full);
    const b = placeChunk("user-sf6", "file-6", 0, 32, full);
    expect(a).toBe(b);
    // And different from the no-skip result iff the no-skip
    // winner is in the skip set.
    const noSkip = placeChunk("user-sf6", "file-6", 0, 32);
    if (full.has(noSkip)) {
      expect(a).not.toBe(noSkip);
    } else {
      expect(a).toBe(noSkip);
    }
  });
});
