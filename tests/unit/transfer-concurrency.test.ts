import { describe, it, expect } from "vitest";

/**
 * Regression test: concurrent `parallelDownload` calls on the same
 * client must not contaminate each other's `path` argument.
 *
 * Background: prior to the fix, `parallelDownload` mutated a global
 * `client.fileIdToPath` resolver before each call and "restored" it
 * after. Two concurrent calls on the same client interleaved their
 * save-and-restore — call A's `path` could leak into call B's
 * chunk fetches, or call A's resolver could persist past A's
 * lifetime. The fix routes `path` through `fetchChunkByHash`
 * directly as a parameter, captured per-call inside the lane.
 *
 * This test stubs `multipartDownloadToken` and `fetchChunkByHash`
 * on a fake client and asserts each call's chunk fetches receive
 * the path the caller passed, even when the calls overlap in time.
 */

import { parallelDownload } from "../../sdk/src/transfer";
import type { HttpVFS } from "../../sdk/src/http";

interface ChunkRequest {
  callerLabel: string; // which parallelDownload call invoked us
  pathArg: string;     // the path argument that fetchChunkByHash received
  fileIdArg: string;   // the fileId argument
  idx: number;
}

/**
 * Build a stub HTTP client that records every fetchChunkByHash call.
 * Returns deterministic per-chunk bytes so verify-hash succeeds.
 */
function makeStubClient(label: string): {
  client: HttpVFS;
  requests: ChunkRequest[];
} {
  const requests: ChunkRequest[] = [];
  const client = {
    multipartDownloadToken: async (path: string) => {
      // Synthesize a manifest with 4 chunks of 4 bytes each.
      const TOTAL = 16;
      const CHUNK = 4;
      const fileId = `file:${path}`;
      const chunks: { index: number; hash: string; size: number }[] = [];
      for (let i = 0; i < 4; i++) {
        const bytes = new Uint8Array([i, i, i, i]);
        const hash = await sha256Hex(bytes);
        chunks.push({ index: i, hash, size: CHUNK });
      }
      return {
        token: `tok:${path}`,
        expiresAtMs: Date.now() + 60_000,
        manifest: {
          fileId,
          size: TOTAL,
          chunkSize: CHUNK,
          chunkCount: 4,
          chunks,
          inlined: false,
        },
      };
    },
    fetchChunkByHash: async (
      fileId: string,
      idx: number,
      _hash: string,
      _token: string,
      pathArg: string,
      _signal?: AbortSignal,
    ) => {
      requests.push({ callerLabel: label, pathArg, fileIdArg: fileId, idx });
      // Force a microtask boundary so concurrent calls actually interleave.
      await Promise.resolve();
      return new Uint8Array([idx, idx, idx, idx]);
    },
    readFile: async (_p: string) => new Uint8Array(0),
  } as unknown as HttpVFS;
  return { client, requests };
}

async function sha256Hex(bytes: Uint8Array): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return Array.from(new Uint8Array(digest))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

describe("transfer concurrency — fileIdToPath race regression", () => {
  it("two concurrent parallelDownload calls on the same client never cross paths", async () => {
    const { client, requests } = makeStubClient("shared");
    const [resA, resB] = await Promise.all([
      parallelDownload(client, "/alpha.bin", { concurrency: { initial: 4, max: 4, min: 1 } }),
      parallelDownload(client, "/beta.bin",  { concurrency: { initial: 4, max: 4, min: 1 } }),
    ]);
    // Both downloads complete and produce 16 bytes each.
    expect(resA.byteLength).toBe(16);
    expect(resB.byteLength).toBe(16);

    // Every chunk fetch's pathArg matches the parallelDownload call
    // that issued it. We tagged the request with its caller label
    // implicitly by the path string itself: any fetch with
    // pathArg='/alpha.bin' must have a fileIdArg of 'file:/alpha.bin',
    // and vice versa. If the resolver-mutation race were present,
    // we'd see fetches where pathArg='/beta.bin' but fileIdArg='file:/alpha.bin'
    // (or vice versa).
    expect(requests.length).toBe(8); // 4 chunks × 2 downloads
    for (const r of requests) {
      const expectedFileId = `file:${r.pathArg}`;
      expect(r.fileIdArg).toBe(expectedFileId);
    }

    // Specifically count alpha vs beta fetches.
    const alpha = requests.filter((r) => r.pathArg === "/alpha.bin");
    const beta = requests.filter((r) => r.pathArg === "/beta.bin");
    expect(alpha.length).toBe(4);
    expect(beta.length).toBe(4);
  });

  it("a single parallelDownload call passes its path on every chunk fetch", async () => {
    const { client, requests } = makeStubClient("solo");
    const out = await parallelDownload(client, "/solo.bin", {
      concurrency: { initial: 4, max: 4, min: 1 },
    });
    expect(out.byteLength).toBe(16);
    expect(requests.length).toBe(4);
    for (const r of requests) {
      expect(r.pathArg).toBe("/solo.bin");
      expect(r.fileIdArg).toBe("file:/solo.bin");
    }
  });
});
