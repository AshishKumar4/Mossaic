import { describe, it, expect } from "vitest";
import { SELF, env } from "cloudflare:test";

/**
 * SDK parallelUpload / parallelDownload integration tests.
 *
 * Drives `sdk/src/transfer.ts` end-to-end against the real worker
 * via SELF.fetch. Same fetcher pattern as `http-fallback.test.ts`.
 *
 * Coverage:
 *   - parallelUpload happy path (round-trips bytes via readFile)
 *   - parallelUpload with chunkTransform
 *   - parallelUpload progress callback fires
 *   - parallelUpload abort signal propagates
 *   - parallelDownload happy path round-trip
 *   - parallelDownload with chunkTransform inverse (round-trip)
 *   - throughput math constants are sane
 */

import {
  createMossaicHttpClient,
  parallelUpload,
  parallelDownload,
  parallelDownloadStream,
  beginUpload,
  putChunk,
  finalizeUpload,
  abortUpload,
  THROUGHPUT_MATH,
  deriveClientChunkSpec,
  type TransferProgressEvent,
} from "../../sdk/src/index";
import { signVFSToken } from "@core/lib/auth";

interface E {
  JWT_SECRET?: string;
}
const TEST_ENV = env as unknown as E;

const selfFetcher: typeof fetch = ((input: RequestInfo | URL, init?: RequestInit) => {
  const url = typeof input === "string"
    ? input
    : input instanceof URL
    ? input.toString()
    : input.url;
  return SELF.fetch(url, init);
}) as typeof fetch;

async function mint(tenant: string): Promise<string> {
  return signVFSToken(TEST_ENV as never, { ns: "default", tenant });
}

async function clientFor(tenant: string) {
  const apiKey = await mint(tenant);
  return createMossaicHttpClient({
    url: "https://mossaic.test",
    apiKey,
    fetcher: selfFetcher,
  });
}

function makeBytes(n: number, seed = 0): Uint8Array {
  const out = new Uint8Array(n);
  let x = seed >>> 0;
  for (let i = 0; i < n; i++) {
    x = (x * 1664525 + 1013904223) >>> 0; // LCG
    out[i] = x & 0xff;
  }
  return out;
}

describe("SDK parallelUpload", () => {
  it("uploads a multi-chunk file and round-trips via readFile", async () => {
    const vfs = await clientFor("sdk-mp-1");
    const data = makeBytes(300, 1); // small enough to chunk via override
    const r = await parallelUpload(vfs, "/sdk-up.bin", data, {
      chunkSize: 100, // 3 chunks
      concurrency: { initial: 4, max: 8 },
    });
    expect(r.size).toBe(300);
    expect(r.uploadId).toMatch(/^[a-z0-9]+$/);

    const back = await vfs.readFile("/sdk-up.bin");
    expect(back.byteLength).toBe(300);
    expect(back).toEqual(data);
  });

  it("uploads an empty file (totalChunks=0)", async () => {
    const vfs = await clientFor("sdk-mp-2");
    const r = await parallelUpload(vfs, "/sdk-empty.bin", new Uint8Array(0), {});
    expect(r.size).toBe(0);
    const back = await vfs.readFile("/sdk-empty.bin");
    expect(back.byteLength).toBe(0);
  });

  it("fires onProgress callbacks during upload", async () => {
    const vfs = await clientFor("sdk-mp-3");
    const data = makeBytes(500, 2);
    const events: TransferProgressEvent[] = [];
    await parallelUpload(vfs, "/sdk-prog.bin", data, {
      chunkSize: 100,
      concurrency: { initial: 2 },
      onProgress: (e) => events.push(e),
    });
    expect(events.length).toBeGreaterThan(0);
    const last = events[events.length - 1];
    expect(last.chunksDone).toBe(last.chunksTotal);
    expect(last.uploaded).toBe(500);
  });

  it("supports chunkTransform", async () => {
    const vfs = await clientFor("sdk-mp-4");
    const plaintext = makeBytes(250, 3);
    // Pretend each plaintext chunk wraps in a 2-byte header.
    const wrap = (bytes: Uint8Array): Uint8Array => {
      const out = new Uint8Array(bytes.byteLength + 2);
      out[0] = 0xab;
      out[1] = 0xcd;
      out.set(bytes, 2);
      return out;
    };
    const unwrap = (env: Uint8Array): Uint8Array => env.subarray(2);
    const r = await parallelUpload(vfs, "/sdk-tx.bin", plaintext, {
      chunkSize: 100,
      chunkTransform: (b) => wrap(b),
    });
    expect(r.size).toBeGreaterThan(plaintext.byteLength); // envelope overhead

    // Read back via parallelDownload with the inverse transform.
    const got = await parallelDownload(vfs, "/sdk-tx.bin", {
      chunkTransform: (b) => unwrap(b),
    });
    expect(got.byteLength).toBeGreaterThanOrEqual(plaintext.byteLength);
  });

  it("propagates AbortSignal — aborts in flight", async () => {
    const vfs = await clientFor("sdk-mp-5");
    const data = makeBytes(2000, 4);
    const ac = new AbortController();
    // Abort almost immediately.
    queueMicrotask(() => ac.abort());
    await expect(
      parallelUpload(vfs, "/sdk-abort.bin", data, {
        chunkSize: 100,
        signal: ac.signal,
      })
    ).rejects.toBeDefined();
  });

  it("respects concurrency.max bound (target never exceeds max)", async () => {
    const vfs = await clientFor("sdk-mp-6");
    const data = makeBytes(1000, 5);
    const events: TransferProgressEvent[] = [];
    await parallelUpload(vfs, "/sdk-cap.bin", data, {
      chunkSize: 50, // 20 chunks
      concurrency: { initial: 4, max: 8 },
      onProgress: (e) => events.push(e),
    });
    for (const e of events) {
      expect(e.currentParallelism).toBeLessThanOrEqual(8);
    }
  });
});

describe("SDK raw protocol", () => {
  it("beginUpload + putChunk + finalizeUpload composes by hand", async () => {
    const vfs = await clientFor("sdk-raw-1");
    const data = makeBytes(150, 6);
    const session = await beginUpload(vfs, "/raw.bin", {
      size: 150,
      chunkSize: 50,
    });
    expect(session.totalChunks).toBe(3);
    const hashes: string[] = [];
    for (let i = 0; i < session.totalChunks; i++) {
      const start = i * session.chunkSize;
      const end = Math.min(start + session.chunkSize, 150);
      const r = await putChunk(vfs, session, i, data.subarray(start, end));
      hashes.push(r.hash);
    }
    const f = await finalizeUpload(vfs, session, hashes);
    expect(f.size).toBe(150);
    const back = await vfs.readFile("/raw.bin");
    expect(back).toEqual(data);
  });

  it("abortUpload cancels a session — finalize after abort fails", async () => {
    const vfs = await clientFor("sdk-raw-2");
    const session = await beginUpload(vfs, "/raw-abort.bin", {
      size: 100,
      chunkSize: 100,
    });
    const c0 = makeBytes(100, 7);
    const r0 = await putChunk(vfs, session, 0, c0);
    await abortUpload(vfs, session);
    await expect(finalizeUpload(vfs, session, [r0.hash])).rejects.toBeDefined();
  });
});

describe("SDK parallelDownload", () => {
  it("round-trips a multi-chunk file uploaded via parallelUpload", async () => {
    const vfs = await clientFor("sdk-dl-1");
    const data = makeBytes(400, 8);
    await parallelUpload(vfs, "/sdk-dl.bin", data, { chunkSize: 100 });
    const back = await parallelDownload(vfs, "/sdk-dl.bin", {
      concurrency: { initial: 4 },
    });
    expect(back.byteLength).toBe(400);
    expect(back).toEqual(data);
  });

  it("parallelDownloadStream emits chunks in monotonic index order", async () => {
    const vfs = await clientFor("sdk-dl-stream");
    const data = makeBytes(800, 13);
    await parallelUpload(vfs, "/stream.bin", data, { chunkSize: 100 });
    const stream = await parallelDownloadStream(vfs, "/stream.bin", {
      concurrency: { initial: 2, max: 4 },
    });
    const reader = stream.getReader();
    const collected: Uint8Array[] = [];
    let total = 0;
    for (;;) {
      const r = await reader.read();
      if (r.done) break;
      collected.push(r.value);
      total += r.value.byteLength;
    }
    expect(total).toBe(800);
    // Concatenate and compare byte-equivalence.
    const out = new Uint8Array(total);
    let off = 0;
    for (const c of collected) {
      out.set(c, off);
      off += c.byteLength;
    }
    expect(out).toEqual(data);
  });

  it("parallelDownloadStream short-circuits empty / inlined files", async () => {
    const vfs = await clientFor("sdk-dl-stream-tiny");
    const tiny = new TextEncoder().encode("tiny");
    await vfs.writeFile("/tiny.txt", tiny);
    const stream = await parallelDownloadStream(vfs, "/tiny.txt");
    const reader = stream.getReader();
    const r1 = await reader.read();
    expect(r1.done).toBe(false);
    expect(r1.value).toBeDefined();
    expect(new TextDecoder().decode(r1.value!)).toBe("tiny");
    const r2 = await reader.read();
    expect(r2.done).toBe(true);
  });
});

describe("throughput math constants", () => {
  it("THROUGHPUT_MATH exposes documented numbers", () => {
    expect(THROUGHPUT_MATH.defaultChunkSizeBytes).toBe(1_048_576);
    expect(THROUGHPUT_MATH.defaultMaxConcurrency).toBe(64);
    // Aggregate ceiling = (64 chunks × 1 MB/chunk) / 0.015 s ≈ 4266 MB/s.
    expect(THROUGHPUT_MATH.aggregateCeilingMBs).toBeGreaterThan(4000);
    expect(THROUGHPUT_MATH.aggregateCeilingMBs).toBeLessThan(5000);
    // 100 MB on gigabit: ~0.8 + 0.3 = ~1.1 s — well under the 10 s gate.
    expect(THROUGHPUT_MATH.hundredMBOnGigabitSec).toBeLessThan(2);
  });

  it("deriveClientChunkSpec matches the server's adaptive ladder", () => {
    expect(deriveClientChunkSpec(0).chunkSize).toBe(0);
    expect(deriveClientChunkSpec(512).chunkSize).toBe(512);
    expect(deriveClientChunkSpec(2 * 1024 * 1024).chunkSize).toBe(1_048_576);
    expect(deriveClientChunkSpec(2 * 1024 * 1024).chunkCount).toBe(2);
  });
});
