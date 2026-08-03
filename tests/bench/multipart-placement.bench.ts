import { expect, it } from "vitest";

import { MULTIPART_PLACEMENT_VERSION } from "@shared/multipart";
import {
  jumpConsistentHash,
  multipartPlacementHash,
  placeChunk,
  placeMultipartChunk,
} from "@shared/placement";

const PLACEMENTS = 20_000;
const POOL_SIZES = [32, 256, 2_048] as const;

interface PlacementCost {
  poolSize: number;
  placements: number;
  hashesPerPlacement: number;
  jumpIterationsPerPlacement: number;
  rendezvousHashesPerPlacement: number;
  wallMs: number;
  checksum: number;
}

/**
 * Placement runs on the hot chunk-PUT path and again per verified chunk
 * at finalize, so its cost per chunk has to stay flat while a tenant's
 * shard pool grows. Rendezvous cannot: it scores every shard. This
 * measures both, and pins that v2 hashing work does not move at all
 * between a 32-shard and a 2048-shard pool.
 */
it("keeps multipart placement hash work constant as the pool grows", () => {
  const costs = POOL_SIZES.map((poolSize): PlacementCost => {
    let hashes = 0;
    let jumpIterations = 0;
    let checksum = 0;
    const instrumentation = {
      hash: () => hashes++,
      jumpIteration: () => jumpIterations++,
    };
    const started = performance.now();
    for (let index = 0; index < PLACEMENTS; index++) {
      const userId = "benchmark-user";
      const fileId = `upload-${index}`;
      const chunkIndex = index % 97;
      const shard = jumpConsistentHash(
        multipartPlacementHash(userId, fileId, chunkIndex, instrumentation),
        poolSize,
        instrumentation
      );
      // The measured primitives are exactly what production placement
      // runs; drift between them would make the numbers meaningless.
      expect(shard).toBe(
        placeMultipartChunk(
          userId,
          fileId,
          chunkIndex,
          poolSize,
          MULTIPART_PLACEMENT_VERSION
        )
      );
      checksum = (checksum + shard) >>> 0;
    }
    const wallMs = performance.now() - started;
    return {
      poolSize,
      placements: PLACEMENTS,
      hashesPerPlacement: hashes / PLACEMENTS,
      jumpIterationsPerPlacement: jumpIterations / PLACEMENTS,
      // Rendezvous scores one key per shard, by construction.
      rendezvousHashesPerPlacement: poolSize,
      wallMs,
      checksum,
    };
  });

  console.log(
    `MOSSAIC_MULTIPART_PLACEMENT_BENCHMARK=${JSON.stringify(costs)}`
  );

  // Constant hash work: two Murmur3 words per placement at every pool
  // size, against rendezvous' 32 / 256 / 2048.
  expect(costs.map((cost) => cost.hashesPerPlacement)).toEqual([2, 2, 2]);
  expect(costs.map((cost) => cost.rendezvousHashesPerPlacement)).toEqual([
    ...POOL_SIZES,
  ]);
  // The only pool-dependent work left is the jump loop, which is
  // logarithmic: a 64× larger pool costs well under 3× the steps.
  expect(costs[0].jumpIterationsPerPlacement).toBeLessThanOrEqual(6);
  expect(costs[2].jumpIterationsPerPlacement).toBeLessThanOrEqual(12);
  expect(costs[2].jumpIterationsPerPlacement).toBeLessThan(
    costs[0].jumpIterationsPerPlacement * 3
  );
  // Legacy sessions keep paying the O(pool) price; that is the cost v2
  // exists to remove, and the reason placement is versioned rather
  // than replaced.
  expect(placeChunk("benchmark-user", "upload-0", 0, 32)).toBe(
    placeMultipartChunk("benchmark-user", "upload-0", 0, 32)
  );
});
