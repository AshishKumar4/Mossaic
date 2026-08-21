import { expect, it } from "vitest";

import { MULTIPART_PLACEMENT_VERSION } from "@shared/multipart";
import {
  placeMultipartChunk,
} from "@shared/placement";

it("benchmarks constant multipart placement hash work", () => {
  const placements = 25_000;
  const results = [256, 65_536].map((poolSize) => {
    let hashCalls = 0;
    let jumpIterations = 0;
    let checksum = 0;
    const started = performance.now();
    for (let index = 0; index < placements; index++) {
      const shard = placeMultipartChunk(
        "benchmark-user",
        `upload-${index}`,
        index % 97,
        poolSize,
        MULTIPART_PLACEMENT_VERSION,
        {
          hash: () => hashCalls++,
          jumpIteration: () => jumpIterations++,
        }
      );
      checksum = (checksum + shard) >>> 0;
    }
    return {
      poolSize,
      placements,
      hashCalls,
      hashesPerPlacement: hashCalls / placements,
      jumpIterations,
      jumpIterationsPerPlacement: jumpIterations / placements,
      wallMs: performance.now() - started,
      checksum,
    };
  });

  expect(results.map((result) => result.hashesPerPlacement)).toEqual([2, 2]);
  expect(results[0]!.jumpIterationsPerPlacement).toBeLessThanOrEqual(8);
  expect(results[1]!.jumpIterationsPerPlacement).toBeLessThanOrEqual(16);
  expect(results[1]!.jumpIterationsPerPlacement).toBeLessThan(
    results[0]!.jumpIterationsPerPlacement * 2.5
  );
  console.log(`MOSSAIC_MULTIPART_PLACEMENT_BENCHMARK=${JSON.stringify(results)}`);
});
