import { describe, it, expect } from "vitest";

/**
 * Verifies that `parallelDownload` and `parallelDownloadStream` —
 * which both delegate to the shared `runAdaptiveDownloadEngine` —
 * produce byte-identical output for the same manifest.
 *
 * Background: prior to the engine extract, the two functions were
 * line-for-line near-duplicates (~280 LoC each). The endgame fix
 * had to be applied to both by hand. This test pins the parity
 * the extract delivers.
 */

import {
  parallelDownload,
  parallelDownloadStream,
} from "../../sdk/src/transfer";
import type { HttpVFS } from "../../sdk/src/http";

interface FakeChunk {
  hash: string;
  bytes: Uint8Array;
}

async function sha256Hex(bytes: Uint8Array): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return Array.from(new Uint8Array(digest))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

/**
 * Build a stub client that serves a fixed N-chunk file. Chunk bytes
 * are deterministic so every concurrency interleave produces the
 * same final bytes.
 */
async function makeStubClient(
  chunkSize: number,
  chunkCount: number
): Promise<{ client: HttpVFS; chunks: FakeChunk[]; totalBytes: number }> {
  const chunks: FakeChunk[] = [];
  let totalBytes = 0;
  for (let i = 0; i < chunkCount; i++) {
    const sz = chunkSize;
    const bytes = new Uint8Array(sz);
    for (let j = 0; j < sz; j++) bytes[j] = (i * 37 + j) & 0xff;
    chunks.push({ hash: await sha256Hex(bytes), bytes });
    totalBytes += sz;
  }
  const client = {
    multipartDownloadToken: async (path: string) => ({
      token: `tok:${path}`,
      expiresAtMs: Date.now() + 60_000,
      manifest: {
        fileId: `file:${path}`,
        size: totalBytes,
        chunkSize,
        chunkCount,
        mimeType: "application/octet-stream",
        chunks: chunks.map((c, i) => ({
          index: i,
          hash: c.hash,
          size: c.bytes.byteLength,
        })),
        inlined: false,
      },
    }),
    fetchChunkByHash: async (
      _fileId: string,
      idx: number,
      _hash: string,
      _token: string,
      _path: string,
      _signal?: AbortSignal,
    ) => {
      // Microtask boundary so concurrent lanes interleave.
      await Promise.resolve();
      return chunks[idx]!.bytes;
    },
    readFile: async () => new Uint8Array(0),
  } as unknown as HttpVFS;
  return { client, chunks, totalBytes };
}

async function readAll(stream: ReadableStream<Uint8Array>): Promise<Uint8Array> {
  const reader = stream.getReader();
  const parts: Uint8Array[] = [];
  let total = 0;
  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    if (value) {
      parts.push(value);
      total += value.byteLength;
    }
  }
  const out = new Uint8Array(total);
  let cursor = 0;
  for (const p of parts) {
    out.set(p, cursor);
    cursor += p.byteLength;
  }
  return out;
}

function expected(chunks: FakeChunk[]): Uint8Array {
  let total = 0;
  for (const c of chunks) total += c.bytes.byteLength;
  const out = new Uint8Array(total);
  let cursor = 0;
  for (const c of chunks) {
    out.set(c.bytes, cursor);
    cursor += c.bytes.byteLength;
  }
  return out;
}

describe("transfer engine parity — parallelDownload vs parallelDownloadStream", () => {
  it("4-chunk file: both functions return byte-identical bytes (concat order)", async () => {
    const { client, chunks } = await makeStubClient(4, 4);
    const want = expected(chunks);

    const got1 = await parallelDownload(client, "/four.bin", {
      concurrency: { initial: 2, max: 4, min: 1 },
    });
    expect(got1).toEqual(want);

    const stream = await parallelDownloadStream(client, "/four.bin", {
      concurrency: { initial: 2, max: 4, min: 1 },
    });
    const got2 = await readAll(stream);
    expect(got2).toEqual(want);

    // Both produce the same bytes.
    expect(got1).toEqual(got2);
  });

  it("16-chunk file with high concurrency: order preserved despite interleave", async () => {
    const { client, chunks } = await makeStubClient(8, 16);
    const want = expected(chunks);

    const got1 = await parallelDownload(client, "/sixteen.bin", {
      concurrency: { initial: 16, max: 16, min: 1 },
    });
    const stream = await parallelDownloadStream(client, "/sixteen.bin", {
      concurrency: { initial: 16, max: 16, min: 1 },
    });
    const got2 = await readAll(stream);

    expect(got1).toEqual(want);
    expect(got2).toEqual(want);
    expect(got1).toEqual(got2);
  });

  it("chunk-transform applied uniformly: both functions see post-transform bytes", async () => {
    const { client, chunks } = await makeStubClient(8, 4);
    // Transform: zero out the first byte of each chunk.
    const transform = (bytes: Uint8Array, _idx: number): Uint8Array => {
      const out = new Uint8Array(bytes);
      if (out.byteLength > 0) out[0] = 0;
      return out;
    };
    // Compute expected post-transform output.
    const want = new Uint8Array(chunks.length * 8);
    for (let i = 0; i < chunks.length; i++) {
      const t = transform(chunks[i]!.bytes, i);
      want.set(t, i * 8);
    }

    const got1 = await parallelDownload(client, "/xform.bin", {
      concurrency: { initial: 4, max: 4, min: 1 },
      chunkTransform: transform,
    });
    const stream = await parallelDownloadStream(client, "/xform.bin", {
      concurrency: { initial: 4, max: 4, min: 1 },
      chunkTransform: transform,
    });
    const got2 = await readAll(stream);

    expect(got1).toEqual(want);
    expect(got2).toEqual(want);
  });

  it("chunk events emitted by both functions in same per-index lifecycle", async () => {
    const { client, chunks } = await makeStubClient(4, 4);
    void chunks;

    const events1: Array<{ idx: number; state: string }> = [];
    await parallelDownload(client, "/events.bin", {
      concurrency: { initial: 2, max: 2, min: 1 },
      onChunkEvent: (e) => events1.push({ idx: e.index, state: e.state }),
    });

    const events2: Array<{ idx: number; state: string }> = [];
    const stream = await parallelDownloadStream(client, "/events.bin", {
      concurrency: { initial: 2, max: 2, min: 1 },
      onChunkEvent: (e) => events2.push({ idx: e.index, state: e.state }),
    });
    await readAll(stream);

    // Each chunk should emit started + completed exactly once for both
    // functions. Order across indices may differ due to concurrency,
    // but per-index ordering must be started → completed.
    function perIndexEvents(
      events: Array<{ idx: number; state: string }>,
      idx: number
    ): string[] {
      return events.filter((e) => e.idx === idx).map((e) => e.state);
    }
    for (let i = 0; i < 4; i++) {
      expect(perIndexEvents(events1, i)).toEqual(["started", "completed"]);
      expect(perIndexEvents(events2, i)).toEqual(["started", "completed"]);
    }
  });

  it("inlined manifest short-circuit: both functions return readFile bytes", async () => {
    const inlineBytes = new Uint8Array([1, 2, 3, 4, 5]);
    const client = {
      multipartDownloadToken: async (path: string) => ({
        token: `tok:${path}`,
        expiresAtMs: Date.now() + 60_000,
        manifest: {
          fileId: `file:${path}`,
          size: 5,
          chunkSize: 5,
          chunkCount: 0,
          mimeType: "text/plain",
          chunks: [],
          inlined: true,
        },
      }),
      readFile: async () => inlineBytes,
      fetchChunkByHash: async () => {
        throw new Error("should not be called for inlined");
      },
    } as unknown as HttpVFS;

    const got1 = await parallelDownload(client, "/inline.txt");
    expect(got1).toEqual(inlineBytes);

    const stream = await parallelDownloadStream(client, "/inline.txt");
    const got2 = await readAll(stream);
    expect(got2).toEqual(inlineBytes);
  });

  it("empty file (0 chunks): both functions return empty bytes", async () => {
    const client = {
      multipartDownloadToken: async (path: string) => ({
        token: `tok:${path}`,
        expiresAtMs: Date.now() + 60_000,
        manifest: {
          fileId: `file:${path}`,
          size: 0,
          chunkSize: 0,
          chunkCount: 0,
          mimeType: "application/octet-stream",
          chunks: [],
          inlined: false,
        },
      }),
      readFile: async () => new Uint8Array(0),
      fetchChunkByHash: async () => {
        throw new Error("should not be called for empty");
      },
    } as unknown as HttpVFS;

    const got1 = await parallelDownload(client, "/empty.bin");
    expect(got1.byteLength).toBe(0);

    const stream = await parallelDownloadStream(client, "/empty.bin");
    const got2 = await readAll(stream);
    expect(got2.byteLength).toBe(0);
  });
});
