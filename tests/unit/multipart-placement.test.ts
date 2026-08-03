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

/**
 * Placement is the one decision every multipart chunk depends on twice:
 * once when the PUT picks a shard and once when finalize looks for it
 * there. These vectors pin both algorithms byte-for-byte, because a
 * drift in either silently orphans stored chunks rather than failing.
 */
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

  it("keeps versionless and v1 callers on the original rendezvous result", () => {
    for (const vector of GOLDEN_VECTORS) {
      const rendezvous = placeChunk(
        vector.userId,
        vector.fileId,
        vector.chunkIndex,
        vector.poolSize
      );
      expect(rendezvous).toBe(vector.legacy);
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
    }
  });

  it("refuses to guess at an unknown placement version", () => {
    expect(() =>
      placeMultipartChunk("tenant-a", "upload-a", 0, 32, 3)
    ).toThrow(/unsupported multipart placement version 3/);
    expect(() => jumpConsistentHash(1n, 0)).toThrow(RangeError);
  });

  it("keeps v2 distribution bounded as the pool grows", () => {
    const samplesPerBucket = 128;
    for (const poolSize of [32, 256, 2_048]) {
      const counts = new Uint32Array(poolSize);
      for (let sample = 0; sample < poolSize * samplesPerBucket; sample++) {
        counts[
          placeMultipartChunk(
            "distribution-user",
            `upload-${sample}`,
            sample % 17,
            poolSize,
            MULTIPART_PLACEMENT_VERSION
          )
        ]++;
      }
      expect(Math.min(...counts)).toBeGreaterThan(samplesPerBucket * 0.65);
      expect(Math.max(...counts)).toBeLessThan(samplesPerBucket * 1.5);
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
        const key = multipartPlacementHash(
          "growth-user",
          `upload-${sample}`,
          sample % 23
        );
        if (
          jumpConsistentHash(key, before) === jumpConsistentHash(key, after)
        ) {
          retained++;
        }
      }
      // Jump consistent hashing moves all but `before / after` of the
      // keys when the pool grows — the minimum any correct remap can.
      expect(retained / samples).toBeCloseTo(before / after, 2);
    }
  });

  it("moves a placement only onto the new shard when the pool grows by one", () => {
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
