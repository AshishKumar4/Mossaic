import { describe, expect, it } from "vitest";
import { bytesToHex, hashChunk } from "@shared/crypto";
import {
  createSha256State,
  digestSha256,
  restoreSha256State,
  serializeSha256State,
  updateSha256,
} from "@shared/incremental-sha256";

const encoder = new TextEncoder();

async function expectMatches(
  bytes: Uint8Array,
  partitions: readonly number[]
): Promise<void> {
  let state = createSha256State();
  let offset = 0;
  for (const length of partitions) {
    updateSha256(state, bytes.subarray(offset, offset + length));
    offset += length;
    state = restoreSha256State(
      JSON.parse(JSON.stringify(serializeSha256State(state)))
    );
  }
  updateSha256(state, bytes.subarray(offset));
  expect(bytesToHex(digestSha256(state))).toBe(await hashChunk(bytes));
}

describe("incremental SHA-256", () => {
  it("matches hashChunk for empty input", async () => {
    const state = createSha256State();

    expect(bytesToHex(digestSha256(state))).toBe(
      await hashChunk(new Uint8Array())
    );
  });

  it.each([
    ["abc", "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"],
    [
      "The quick brown fox jumps over the lazy dog",
      "d7a8fbb307d7809469ca9abcb0082e4f8d5651e46d3cdb762d02d0bf37c9e592",
    ],
  ])("matches the standard vector %s", (input, expected) => {
    const state = createSha256State();
    updateSha256(state, encoder.encode(input));
    expect(bytesToHex(digestSha256(state))).toBe(expected);
  });

  it("matches every split around SHA-256 block and padding boundaries", async () => {
    for (const size of [1, 55, 56, 63, 64, 65, 119, 120, 127, 128, 129]) {
      const bytes = Uint8Array.from({ length: size }, (_, i) => (i * 31) & 0xff);
      for (let split = 0; split <= size; split++) {
        await expectMatches(bytes, [split]);
      }
    }
  });

  it("matches deterministic randomized page partitions", async () => {
    let seed = 0x9e3779b9;
    const random = (): number => {
      seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0;
      return seed;
    };
    for (let run = 0; run < 100; run++) {
      const size = random() % 8192;
      const bytes = Uint8Array.from({ length: size }, () => random() & 0xff);
      const partitions: number[] = [];
      let remaining = size;
      while (remaining > 0) {
        const length = Math.min(remaining, 1 + (random() % 257));
        partitions.push(length);
        remaining -= length;
      }
      await expectMatches(bytes, partitions);
    }
  });

  it("matches the exact multipart concatenated-hash encoding", async () => {
    const hashes = Array.from({ length: 1025 }, (_, index) =>
      index.toString(16).padStart(64, "0")
    );
    const bytes = encoder.encode(hashes.join(""));
    const state = createSha256State();
    for (let offset = 0; offset < hashes.length; offset += 256) {
      updateSha256(state, encoder.encode(hashes.slice(offset, offset + 256).join("")));
    }
    expect(bytesToHex(digestSha256(state))).toBe(await hashChunk(bytes));
  });

  it.each([
    null,
    {},
    { words: [], tail: [], totalBytes: 0 },
    { words: Array(8).fill(0), tail: Array(64).fill(0), totalBytes: 64 },
    { words: Array(8).fill(0), tail: [256], totalBytes: 1 },
    { words: Array(8).fill(0), tail: [1], totalBytes: 2 },
    { words: Array(8).fill(0), tail: [], totalBytes: -1 },
  ])("rejects malformed snapshot %#", (snapshot) => {
    expect(() => restoreSha256State(snapshot)).toThrow(
      /Invalid serialized SHA-256 state/
    );
  });
});
