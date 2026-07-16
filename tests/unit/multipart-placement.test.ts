import { describe, expect, it } from "vitest";

import {
  MULTIPART_LEGACY_PLACEMENT_VERSION,
  MULTIPART_PLACEMENT_VERSION,
} from "@shared/multipart";
import {
  jumpConsistentHash,
  multipartPlacementHash,
  placeChunk,
  placeMultipartChunk,
} from "@shared/placement";

const GOLDEN_VECTORS = [
  {
    userId: "tenant-a",
    fileId: "upload-a",
    chunkIndex: 0,
    poolSize: 32,
    hash: "e89dcc24b3ec9591",
    legacy: 24,
    current: 13,
  },
  {
    userId: "tenant-a",
    fileId: "upload-a",
    chunkIndex: 1,
    poolSize: 32,
    hash: "bb863bcc4a7aec35",
    legacy: 22,
    current: 29,
  },
  {
    userId: "tenant-a",
    fileId: "upload-a",
    chunkIndex: 999,
    poolSize: 256,
    hash: "efec9c9bedc54836",
    legacy: 128,
    current: 226,
  },
  {
    userId: "tenant::sub",
    fileId: "01hzyx",
    chunkIndex: 42,
    poolSize: 2_048,
    hash: "ea4bc2e3c43d0844",
    legacy: 1_291,
    current: 1_612,
  },
  {
    userId: "unicode-user",
    fileId: "file:with:colon",
    chunkIndex: 7,
    poolSize: 17,
    hash: "bc7bad3676ce9c41",
    legacy: 9,
    current: 0,
  },
] as const;

describe("versioned multipart placement", () => {
  it("pins deterministic legacy and v2 golden vectors", () => {
    for (const vector of GOLDEN_VECTORS) {
      const hash = multipartPlacementHash(
        vector.userId,
        vector.fileId,
        vector.chunkIndex
      );
      expect(hash.toString(16).padStart(16, "0")).toBe(vector.hash);
      expect(
        placeMultipartChunk(
          vector.userId,
          vector.fileId,
          vector.chunkIndex,
          vector.poolSize
        )
      ).toBe(vector.legacy);
      expect(
        placeMultipartChunk(
          vector.userId,
          vector.fileId,
          vector.chunkIndex,
          vector.poolSize,
          MULTIPART_LEGACY_PLACEMENT_VERSION
        )
      ).toBe(vector.legacy);
      expect(
        placeChunk(
          vector.userId,
          vector.fileId,
          vector.chunkIndex,
          vector.poolSize
        )
      ).toBe(vector.legacy);
      expect(jumpConsistentHash(hash, vector.poolSize)).toBe(vector.current);
      expect(
        placeMultipartChunk(
          vector.userId,
          vector.fileId,
          vector.chunkIndex,
          vector.poolSize,
          MULTIPART_PLACEMENT_VERSION
        )
      ).toBe(vector.current);
    }
  });

  it("keeps v2 distribution bounded as the pool grows", () => {
    const samplesPerBucket = 128;
    for (const poolSize of [32, 256, 2_048]) {
      const counts = new Uint32Array(poolSize);
      for (let sample = 0; sample < poolSize * samplesPerBucket; sample++) {
        const shard = placeMultipartChunk(
          "distribution-user",
          `upload-${sample}`,
          sample % 17,
          poolSize,
          MULTIPART_PLACEMENT_VERSION
        );
        counts[shard]++;
      }
      const minimum = Math.min(...counts);
      const maximum = Math.max(...counts);
      expect(minimum).toBeGreaterThan(samplesPerBucket * 0.65);
      expect(maximum).toBeLessThan(samplesPerBucket * 1.5);
    }
  });

  it("retains the expected share of placements across pool growth", () => {
    const samples = 100_000;
    for (const [before, after] of [
      [32, 256],
      [256, 2_048],
    ] as const) {
      let retained = 0;
      for (let sample = 0; sample < samples; sample++) {
        const args = [
          "growth-user",
          `upload-${sample}`,
          sample % 23,
        ] as const;
        if (
          placeMultipartChunk(
            ...args,
            before,
            MULTIPART_PLACEMENT_VERSION
          ) ===
          placeMultipartChunk(...args, after, MULTIPART_PLACEMENT_VERSION)
        ) {
          retained++;
        }
      }
      expect(retained / samples).toBeCloseTo(before / after, 2);
    }
  });

  it("moves only to the new bucket when a pool grows by one", () => {
    for (let sample = 0; sample < 20_000; sample++) {
      const before = placeMultipartChunk(
        "monotonic-user",
        `upload-${sample}`,
        sample % 31,
        256,
        MULTIPART_PLACEMENT_VERSION
      );
      const after = placeMultipartChunk(
        "monotonic-user",
        `upload-${sample}`,
        sample % 31,
        257,
        MULTIPART_PLACEMENT_VERSION
      );
      if (after !== before) expect(after).toBe(256);
    }
  });
});
