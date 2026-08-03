import { describe, expect, it } from "vitest";
import { bytesToHex, computeFileHash, hashChunk } from "@shared/crypto";
import {
  createSha256State,
  digestSha256,
  restoreSha256State,
  serializeSha256State,
  updateSha256,
} from "@shared/incremental-sha256";

/**
 * The accumulator only earns its place if it is indistinguishable from
 * `crypto.subtle.digest` over the concatenation of everything fed to it —
 * including across a JSON round trip, which is what a Durable Object
 * eviction between two finalize pages amounts to. Every case here is
 * differential against the one-shot digest for that reason.
 */

const encoder = new TextEncoder();

/**
 * Feed `bytes` in the given slice lengths (remainder as a final update),
 * snapshotting and restoring between each one, then compare to the one-shot
 * digest of the whole input.
 */
async function expectMatchesOneShot(
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
  it("matches the one-shot digest of empty input", async () => {
    expect(bytesToHex(digestSha256(createSha256State()))).toBe(
      await hashChunk(new Uint8Array())
    );
  });

  it.each([
    ["abc", "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"],
    [
      "The quick brown fox jumps over the lazy dog",
      "d7a8fbb307d7809469ca9abcb0082e4f8d5651e46d3cdb762d02d0bf37c9e592",
    ],
    [
      "abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq",
      "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1",
    ],
  ])("matches the standard vector for %j", (input, expected) => {
    const state = createSha256State();
    updateSha256(state, encoder.encode(input));
    expect(bytesToHex(digestSha256(state))).toBe(expected);
  });

  it("matches every split around block and padding boundaries", async () => {
    // 55/56 straddle the length-field boundary of the final block, 63/64/65
    // the block boundary, and the 119..129 band both again one block later.
    for (const size of [1, 55, 56, 63, 64, 65, 119, 120, 127, 128, 129]) {
      const bytes = Uint8Array.from({ length: size }, (_, i) => (i * 31) & 0xff);
      for (let split = 0; split <= size; split++) {
        await expectMatchesOneShot(bytes, [split]);
      }
    }
  });

  it("matches 100 randomized page partitions", async () => {
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
      await expectMatchesOneShot(bytes, partitions);
    }
  });

  it("reproduces the multipart concatenated-hex file hash from 256-hash pages", async () => {
    // 1025 hashes = 65_600 bytes: four full pages plus a remainder, and a
    // per-page byte count that is not a multiple of the 64-byte SHA block.
    const hashes = Array.from({ length: 1025 }, (_, index) =>
      index.toString(16).padStart(64, "0")
    );
    let state = createSha256State();
    for (let offset = 0; offset < hashes.length; offset += 256) {
      updateSha256(
        state,
        encoder.encode(hashes.slice(offset, offset + 256).join(""))
      );
      state = restoreSha256State(
        JSON.parse(JSON.stringify(serializeSha256State(state)))
      );
    }
    expect(bytesToHex(digestSha256(state))).toBe(await computeFileHash(hashes));
  });

  it("leaves the accumulator usable after taking a digest", async () => {
    const state = createSha256State();
    updateSha256(state, encoder.encode("first"));
    const afterFirst = bytesToHex(digestSha256(state));
    updateSha256(state, encoder.encode("second"));

    expect(afterFirst).toBe(await hashChunk(encoder.encode("first")));
    expect(bytesToHex(digestSha256(state))).toBe(
      await hashChunk(encoder.encode("firstsecond"))
    );
  });

  it("copies the pending tail out of a caller-owned buffer", async () => {
    // A paged caller is free to reuse one scratch buffer per page, so the
    // bytes held back for the next block must not be a view onto it.
    const scratch = new Uint8Array(8).fill(0x61);
    const state = createSha256State();
    updateSha256(state, scratch);
    scratch.fill(0x62);
    updateSha256(state, scratch);

    expect(bytesToHex(digestSha256(state))).toBe(
      await hashChunk(encoder.encode("aaaaaaaabbbbbbbb"))
    );
  });

  it("snapshots without aliasing the live accumulator", async () => {
    // Both updates compress a whole block, so an aliased `words` array would
    // let the second one rewrite the compressed state the snapshot captured.
    const first = Uint8Array.from({ length: 64 }, (_, i) => i);
    const second = Uint8Array.from({ length: 64 }, (_, i) => 255 - i);
    const state = createSha256State();
    updateSha256(state, first);
    const snapshot = serializeSha256State(state);
    updateSha256(state, second);

    expect(snapshot.totalBytes).toBe(64);
    expect(bytesToHex(digestSha256(restoreSha256State(snapshot)))).toBe(
      await hashChunk(first)
    );
  });

  it.each([
    ["not an object", null],
    ["an array", []],
    ["a missing word list", {}],
    ["a short word list", { words: [], tail: [], totalBytes: 0 }],
    [
      "a non-integer word",
      { words: [0.5, 0, 0, 0, 0, 0, 0, 0], tail: [], totalBytes: 64 },
    ],
    [
      "an out-of-range word",
      { words: Array(8).fill(0x1_0000_0000), tail: [], totalBytes: 64 },
    ],
    [
      "a full block left in the tail",
      { words: Array(8).fill(0), tail: Array(64).fill(0), totalBytes: 64 },
    ],
    [
      "a non-byte tail value",
      { words: Array(8).fill(0), tail: [256], totalBytes: 1 },
    ],
    [
      "a tail length disagreeing with totalBytes",
      { words: Array(8).fill(0), tail: [1], totalBytes: 2 },
    ],
    [
      "a negative totalBytes",
      { words: Array(8).fill(0), tail: [], totalBytes: -1 },
    ],
    [
      "advanced words with no processed block",
      { words: Array(8).fill(0), tail: [1], totalBytes: 1 },
    ],
  ])("rejects a snapshot with %s", (_reason, snapshot) => {
    expect(() => restoreSha256State(snapshot)).toThrow(
      /Invalid serialized SHA-256 state/
    );
  });
});
