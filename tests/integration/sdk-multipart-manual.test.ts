import { describe, it, expect } from "vitest";
import { SELF, env } from "cloudflare:test";

/**
 * Manual multipart upload — SDK surface.
 *
 * Exercises the four `*MultipartUpload` methods exposed on
 * `HttpVFS` against the real worker via `SELF.fetch`. Same fetcher
 * pattern as multipart-sdk.test.ts.
 *
 * The methods are thin wrappers over `multipartBegin` /
 * `multipartPutChunk` / `multipartFinalize` / `multipartAbort` —
 * `parallelUpload` (transfer.ts) drives the same wire endpoints
 * with an AIMD controller layered on. These tests ensure the
 * caller-facing wrappers preserve the same wire semantics
 * end-to-end.
 */

import {
  createMossaicHttpClient,
  createVFS,
  EFBIG,
  type MossaicEnv,
  type MultipartUploadHandle,
} from "../../sdk/src/index";
import { hashChunk } from "@shared/crypto";
import { MULTIPART_PAGED_CONTROL_CAPABILITY } from "@shared/multipart";
import { signVFSToken } from "@core/lib/auth";

interface E {
  MOSSAIC_USER: MossaicEnv["MOSSAIC_USER"];
  MOSSAIC_SHARD: MossaicEnv["MOSSAIC_SHARD"];
  JWT_SECRET?: string;
}
const TEST_ENV = env as unknown as E;

const selfFetcher: typeof fetch = ((
  input: RequestInfo | URL,
  init?: RequestInit
) => {
  const url =
    typeof input === "string"
      ? input
      : input instanceof URL
      ? input.toString()
      : input.url;
  return SELF.fetch(url, init);
}) as typeof fetch;

async function clientFor(tenant: string) {
  const apiKey = await signVFSToken(TEST_ENV as never, {
    ns: "default",
    tenant,
  });
  return createMossaicHttpClient({
    url: "https://mossaic.test",
    apiKey,
    fetcher: selfFetcher,
  });
}

function bindingClientFor(tenant: string) {
  return createVFS(
    {
      MOSSAIC_USER: TEST_ENV.MOSSAIC_USER,
      MOSSAIC_SHARD: TEST_ENV.MOSSAIC_SHARD,
    },
    { tenant }
  );
}

/** LCG-derived deterministic bytes; suitable for content roundtrip checks. */
function makeBytes(n: number, seed = 0): Uint8Array {
  const out = new Uint8Array(n);
  let x = seed >>> 0;
  for (let i = 0; i < n; i++) {
    x = (x * 1664525 + 1013904223) >>> 0;
    out[i] = x & 0xff;
  }
  return out;
}

/** Slice the source at `chunkSize` and return one Uint8Array per chunk. */
function sliceChunks(src: Uint8Array, chunkSize: number): Uint8Array[] {
  const chunks: Uint8Array[] = [];
  for (let i = 0; i < src.byteLength; i += chunkSize) {
    chunks.push(src.subarray(i, Math.min(i + chunkSize, src.byteLength)));
  }
  return chunks;
}

describe("SDK manual multipart", () => {
  it("M1 — round-trips bytes byte-for-byte", async () => {
    const vfs = await clientFor("mp-manual-1");
    const data = makeBytes(7000, 1);

    const handle = await vfs.beginMultipartUpload("/manual-rt.bin", {
      size: data.byteLength,
      chunkSize: 2048,
    });
    expect(handle.uploadId).toMatch(/^[a-z0-9]+$/);
    expect(handle.expectedChunks).toBe(Math.ceil(7000 / handle.chunkSize));

    const chunks = sliceChunks(data, handle.chunkSize);
    expect(chunks.length).toBe(handle.expectedChunks);
    const hashes: string[] = [];
    for (let i = 0; i < chunks.length; i++) {
      const r = await vfs.putMultipartChunk(handle, i, chunks[i]);
      expect(r.accepted).toBe(true);
      hashes.push(r.chunkHash);
    }

    const result = await vfs.finalizeMultipartUpload(handle, hashes);
    expect(result).not.toHaveProperty("operation");
    expect(result.size).toBe(7000);
    expect(result.path).toBe("/manual-rt.bin");
    expect(result.pathId).toMatch(/^[a-z0-9]+$/);
    expect(result.fileHash).toMatch(/^[a-f0-9]{64}$/);

    const back = await vfs.readFile("/manual-rt.bin");
    expect(back.byteLength).toBe(data.byteLength);
    expect(back).toEqual(data);
  });

  it("M2 — abort releases the session, no chunks survive", async () => {
    const vfs = await clientFor("mp-manual-2");
    const data = makeBytes(5000, 2);
    const handle = await vfs.beginMultipartUpload("/manual-abort.bin", {
      size: data.byteLength,
      chunkSize: 2048,
    });
    const chunks = sliceChunks(data, handle.chunkSize);
    // Upload partial — only first chunk.
    const r = await vfs.putMultipartChunk(handle, 0, chunks[0]);
    expect(r.accepted).toBe(true);

    const aborted = await vfs.abortMultipartUpload(handle);
    expect(aborted).not.toHaveProperty("operation");
    expect(aborted.aborted).toBe(true);

    // After abort, the path must NOT exist.
    await expect(vfs.readFile("/manual-abort.bin")).rejects.toThrow();
    await expect(vfs.putMultipartChunk(handle, 0, chunks[0])).rejects.toThrow(
      /EBUSY/
    );
  });

  it("M3 — abort is idempotent for already-finalised sessions", async () => {
    const vfs = await clientFor("mp-manual-3");
    const data = makeBytes(1024, 3);
    const handle = await vfs.beginMultipartUpload("/manual-fin.bin", {
      size: data.byteLength,
      chunkSize: 1024,
    });
    const r = await vfs.putMultipartChunk(handle, 0, data);
    await vfs.finalizeMultipartUpload(handle, [r.chunkHash]);

    // Now abort the already-finalised session — should report not-aborted.
    const aborted = await vfs.abortMultipartUpload(handle);
    expect(aborted).not.toHaveProperty("operation");
    expect(aborted.aborted).toBe(false);
    await expect(vfs.putMultipartChunk(handle, 0, data)).rejects.toThrow(/EBUSY/);
  });

  it("M4 — accepts ArrayBuffer chunks", async () => {
    const vfs = await clientFor("mp-manual-4");
    const data = makeBytes(3000, 4);
    const handle = await vfs.beginMultipartUpload("/manual-ab.bin", {
      size: data.byteLength,
      chunkSize: 1500,
    });
    const chunks = sliceChunks(data, handle.chunkSize);
    const hashes: string[] = [];
    for (let i = 0; i < chunks.length; i++) {
      // Slice and copy into a fresh ArrayBuffer.
      const ab = chunks[i].slice().buffer;
      const r = await vfs.putMultipartChunk(handle, i, ab);
      hashes.push(r.chunkHash);
    }
    await vfs.finalizeMultipartUpload(handle, hashes);
    const back = await vfs.readFile("/manual-ab.bin");
    expect(back).toEqual(data);
  });

  it("M5 — accepts Blob chunks", async () => {
    const vfs = await clientFor("mp-manual-5");
    const data = makeBytes(2000, 5);
    const handle = await vfs.beginMultipartUpload("/manual-blob.bin", {
      size: data.byteLength,
      chunkSize: 1000,
    });
    const chunks = sliceChunks(data, handle.chunkSize);
    const hashes: string[] = [];
    for (let i = 0; i < chunks.length; i++) {
      const blob = new Blob([chunks[i]]);
      const r = await vfs.putMultipartChunk(handle, i, blob);
      hashes.push(r.chunkHash);
    }
    await vfs.finalizeMultipartUpload(handle, hashes);
    const back = await vfs.readFile("/manual-blob.bin");
    expect(back).toEqual(data);
  });

  it("M6 — chunkHash returned matches client-side hashChunk", async () => {
    const vfs = await clientFor("mp-manual-6");
    const data = makeBytes(2048, 6);
    const handle = await vfs.beginMultipartUpload("/manual-hash.bin", {
      size: data.byteLength,
      chunkSize: 1024,
    });
    const chunks = sliceChunks(data, handle.chunkSize);
    for (let i = 0; i < chunks.length; i++) {
      const expected = await hashChunk(chunks[i]);
      const r = await vfs.putMultipartChunk(handle, i, chunks[i]);
      expect(r.chunkHash).toBe(expected);
    }
    await vfs.abortMultipartUpload(handle);
  });

  it("M7 — finalize with missing chunks throws (mismatched hash list length)", async () => {
    const vfs = await clientFor("mp-manual-7");
    const data = makeBytes(4000, 7);
    const handle = await vfs.beginMultipartUpload("/manual-missing.bin", {
      size: data.byteLength,
      chunkSize: 2000,
    });
    const chunks = sliceChunks(data, handle.chunkSize);
    // Upload chunk 0 only.
    const r0 = await vfs.putMultipartChunk(handle, 0, chunks[0]);

    // Finalize with only one hash — totalChunks should be 2; server
    // rejects the length mismatch before any chunk-by-chunk check.
    await expect(
      vfs.finalizeMultipartUpload(handle, [r0.chunkHash])
    ).rejects.toThrow();

    await vfs.abortMultipartUpload(handle);
  });

  it("M8 — handle is JSON-serialisable wire shape", async () => {
    const vfs = await clientFor("mp-manual-8");
    const handle = await vfs.beginMultipartUpload("/manual-ser.bin", {
      size: 100,
      chunkSize: 100,
    });
    const ser = JSON.stringify(handle);
    const round = JSON.parse(ser) as MultipartUploadHandle;
    expect(round.uploadId).toBe(handle.uploadId);
    expect(round.path).toBe(handle.path);
    expect(round.chunkSize).toBe(handle.chunkSize);
    expect(round.expectedChunks).toBe(handle.expectedChunks);
    expect(round.poolSize).toBe(handle.poolSize);
    expect(round.sessionToken).toBe(handle.sessionToken);
    expect(round.expiresAtMs).toBe(handle.expiresAtMs);

    // Resuming via the round-tripped handle still works.
    const data = new Uint8Array(100).fill(0x55);
    const r = await vfs.putMultipartChunk(round, 0, data);
    await vfs.finalizeMultipartUpload(round, [r.chunkHash]);
    const back = await vfs.readFile("/manual-ser.bin");
    expect(back.byteLength).toBe(100);
  });

  it("M9 — putMultipartChunk rejects non-binary input synchronously", async () => {
    const vfs = await clientFor("mp-manual-9");
    const handle = await vfs.beginMultipartUpload("/manual-bad.bin", {
      size: 10,
      chunkSize: 10,
    });
    // String is not a supported chunk type.
    await expect(
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      (vfs as any).putMultipartChunk(handle, 0, "hello there")
    ).rejects.toThrow(/unsupported chunk type/);

    await vfs.abortMultipartUpload(handle);
  });

  it("M10 — manual multipart rejects encryption without encrypted chunks", async () => {
    const vfs = await clientFor("mp-manual-10");
    await expect(vfs.beginMultipartUpload("/manual-enc.bin", {
      size: 2048,
      encryption: { mode: "convergent", keyId: "test-key" },
    })).rejects.toThrow(/EINVAL/);
  });

  it("M11 — finalize applies explicit metadata and tag clears", async () => {
    const vfs = await clientFor("mp-manual-11");
    const path = "/manual-clear.bin";
    await vfs.writeFile(path, new Uint8Array([1]), {
      metadata: { stale: true },
      tags: ["stale"],
    });
    const data = makeBytes(2_048, 11);
    const handle = await vfs.beginMultipartUpload(path, {
      size: data.byteLength,
      chunkSize: 1_024,
      metadata: null,
      tags: [],
    });
    const hashes: string[] = [];
    for (const [index, chunk] of sliceChunks(data, handle.chunkSize).entries()) {
      hashes.push((await vfs.putMultipartChunk(handle, index, chunk)).chunkHash);
    }
    await vfs.finalizeMultipartUpload(handle, hashes);

    const info = await vfs.fileInfo(path, { includeMetadata: true });
    expect(info.metadata).toBeNull();
    expect(info.tags).toEqual([]);
  });

  it("M12 — binding finalize completes overwrite preparation", async () => {
    const vfs = bindingClientFor("mp-manual-12");
    const path = "/manual-binding-overwrite.bin";
    await vfs.writeFile(path, new Uint8Array([1]), {
      metadata: { stale: true },
      tags: ["stale"],
    });
    const data = makeBytes(2_048, 12);
    const handle = await vfs.beginMultipartUpload(path, {
      size: data.byteLength,
      chunkSize: 1_024,
      metadata: null,
      tags: [],
    });
    const hashes: string[] = [];
    for (const [index, chunk] of sliceChunks(data, handle.chunkSize).entries()) {
      hashes.push((await vfs.putMultipartChunk(handle, index, chunk)).chunkHash);
    }

    await expect(vfs.finalizeMultipartUpload(handle, hashes)).resolves.toMatchObject({
      path,
      size: data.byteLength,
    });
    await expect(vfs.fileInfo(path, { includeMetadata: true })).resolves.toMatchObject({
      metadata: null,
      tags: [],
    });
  });

  it("M13 — binding and HTTP bounded operations have matching shapes and cancellation", async () => {
    const clients = [
      bindingClientFor("mp-manual-13-binding"),
      await clientFor("mp-manual-13-http"),
    ];

    for (const [index, vfs] of clients.entries()) {
      const data = makeBytes(2_048, 13 + index);
      const path = `/manual-parity-${index}.bin`;
      const handle = await vfs.beginMultipartUpload(path, {
        size: data.byteLength,
        chunkSize: 1_024,
      });
      const hashes: string[] = [];
      for (const [chunkIndex, chunk] of sliceChunks(
        data,
        handle.chunkSize,
      ).entries()) {
        hashes.push(
          (await vfs.putMultipartChunk(handle, chunkIndex, chunk)).chunkHash,
        );
      }

      const finalized = await vfs.startFinalizeMultipartUpload(handle, hashes);
      expect(finalized).toMatchObject({ path, size: data.byteLength });
      expect(finalized).not.toHaveProperty("operation");

      const cancellable = await vfs.beginMultipartUpload(
        `/manual-cancel-${index}.bin`,
        { size: data.byteLength, chunkSize: 1_024 },
      );
      const controller = new AbortController();
      await expect(
        vfs.startAbortMultipartUpload(cancellable, {
          signal: controller.signal,
          onProgress: () =>
            controller.abort(new DOMException("cancelled", "AbortError")),
        }),
      ).rejects.toMatchObject({ name: "AbortError" });
      await expect(vfs.abortMultipartUpload(cancellable)).resolves.toEqual({
        aborted: true,
      });
    }
  });

  it("M14 — binding defaults reject over-budget handles before an RPC", async () => {
    let requests = 0;
    const overBudgetEnv: MossaicEnv = {
      MOSSAIC_USER: {
        idFromName: (name) => TEST_ENV.MOSSAIC_USER.idFromName(name),
        get: () => ({
          vfsStageMultipartHashes: async () => {
            requests++;
            return { staged: 0, total: 0 };
          },
          vfsAbortMultipartStep: async () => {
            requests++;
            return { done: true };
          },
          vfsGetMultipartStatus: async () => {
            requests++;
            return {
              landed: [],
              total: 0,
              bytesUploaded: 0,
              expiresAtMs: Date.now() + 60_000,
            };
          },
          vfsBeginMultipart: async () => {
            requests++;
            throw new Error("unexpected resume RPC");
          },
        }),
      },
      MOSSAIC_SHARD: TEST_ENV.MOSSAIC_SHARD,
    };
    const vfs = createVFS(overBudgetEnv, { tenant: "mp-manual-14" });
    const handle = (expectedChunks: number): MultipartUploadHandle => ({
      uploadId: `binding-${expectedChunks}`,
      path: "/binding-boundary.bin",
      chunkSize: 1,
      expectedChunks,
      poolSize: 1,
      sessionToken: "session-token",
      expiresAtMs: Date.now() + 60_000,
      size: expectedChunks,
      capabilities: [MULTIPART_PAGED_CONTROL_CAPABILITY],
    });

    const finalizeError = await vfs
      .finalizeMultipartUpload(
        handle(1_025),
        Array.from({ length: 1_025 }, () => "a".repeat(64)),
      )
      .catch((error: Error) => error);
    expect(finalizeError).toBeInstanceOf(EFBIG);
    await expect(vfs.abortMultipartUpload(handle(3_329))).rejects.toBeInstanceOf(
      EFBIG,
    );
    await expect(
      vfs.getMultipartUploadStatus(handle(4_096)),
    ).rejects.toBeInstanceOf(EFBIG);
    await expect(vfs.resumeMultipartUpload(handle(4_096))).rejects.toBeInstanceOf(
      EFBIG,
    );
    expect(requests).toBe(0);
  });
});
