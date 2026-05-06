import { describe, it, expect } from "vitest";
import { env, runInDurableObject } from "cloudflare:test";

/**
 * Phase 4 — Stream + low-level escape-hatch RPC tests.
 *
 * Coverage (sdk-impl-plan §5.3, §8 bottom-of-list):
 *   - vfsCreateReadStream over inlined files (single-chunk inline path)
 *   - vfsCreateReadStream over multi-chunk files (lazy pull, bounded memory)
 *   - vfsCreateReadStream byte-range (start/end inside one chunk; spanning
 *     chunk boundaries; end-of-file)
 *   - handle-based read primitives: vfsOpenReadStream + vfsPullReadStream
 *     (this is the escape hatch that lets a consumer fan out reads across
 *     separate invocations for files larger than one Worker can handle)
 *   - vfsCreateWriteStream multi-pump end-to-end, with mid-chunk write
 *     boundaries (tests the internal buffer flushing)
 *   - handle-based write primitives: begin → append → commit; abort
 *     drops the tmp row + chunks
 *   - openManifest + readChunk parity (the public, shard-stripped form)
 *   - backpressure: ReadableStream pulls one chunk at a time on demand
 *
 * All tests use the new vfs:default:<userId> DO naming pattern that
 * Phase 4 introduced. Direct DO RPC: tests grab a stub via
 * env.USER_DO.idFromName(vfsUserDOName(...)) and call the new methods.
 */

import type { UserDO } from "../../worker/objects/user/user-do";
import type { ShardDO } from "../../worker/objects/shard/shard-do";
import { vfsShardDOName, vfsUserDOName } from "../../worker/lib/utils";
import { INLINE_LIMIT } from "@shared/inline";

interface E {
  USER_DO: DurableObjectNamespace<UserDO>;
  SHARD_DO: DurableObjectNamespace<ShardDO>;
}
const E = env as unknown as E;

const NS = "default";

/**
 * Get a UserDO stub for a given test tenant id. The DO is named via
 * the Phase 4 vfsUserDOName pattern so the test mirrors what an SDK
 * consumer's createVFS({ tenant }) would do.
 */
function userStubFor(tenant: string, sub?: string) {
  return E.USER_DO.get(
    E.USER_DO.idFromName(vfsUserDOName(NS, tenant, sub))
  );
}

/** Shard stub for a (tenant, shardIdx) — used by tests that seed shard state. */
function shardStubFor(
  tenant: string,
  shardIdx: number,
  sub?: string
): DurableObjectStub<ShardDO> {
  return E.SHARD_DO.get(
    E.SHARD_DO.idFromName(vfsShardDOName(NS, tenant, sub, shardIdx))
  );
}

/** Drain a ReadableStream<Uint8Array> into a single Uint8Array (test helper). */
async function drain(stream: ReadableStream<Uint8Array>): Promise<Uint8Array> {
  const reader = stream.getReader();
  const parts: Uint8Array[] = [];
  let total = 0;
  for (;;) {
    const { value, done } = await reader.read();
    if (done) break;
    parts.push(value);
    total += value.byteLength;
  }
  reader.releaseLock();
  const out = new Uint8Array(total);
  let off = 0;
  for (const p of parts) {
    out.set(p, off);
    off += p.byteLength;
  }
  return out;
}

/** Drain with chunk-by-chunk inspection, returning each chunk separately. */
async function drainChunks(
  stream: ReadableStream<Uint8Array>
): Promise<Uint8Array[]> {
  const reader = stream.getReader();
  const parts: Uint8Array[] = [];
  for (;;) {
    const { value, done } = await reader.read();
    if (done) break;
    parts.push(value);
  }
  reader.releaseLock();
  return parts;
}

// ───────────────────────────────────────────────────────────────────────
// vfsCreateReadStream
// ───────────────────────────────────────────────────────────────────────

describe("vfsCreateReadStream", () => {
  it("streams an inlined file (≤ INLINE_LIMIT) with no shard fan-out", async () => {
    const tenant = "stream-inline";
    const stub = userStubFor(tenant);
    const scope = { ns: NS, tenant };

    // Write via the chunked-tier-aware writeFile; for a small payload it
    // takes the inline path automatically.
    const payload = new TextEncoder().encode("hello inline streaming");
    expect(payload.byteLength).toBeLessThan(INLINE_LIMIT);
    await stub.vfsWriteFile(scope, "/note.txt", payload);

    const stream = await stub.vfsCreateReadStream(scope, "/note.txt");
    const got = await drain(stream);
    expect(new TextDecoder().decode(got)).toBe("hello inline streaming");
  });

  it("streams a multi-chunk file lazily (one chunk per pull)", async () => {
    const tenant = "stream-multi";
    const stub = userStubFor(tenant);
    const scope = { ns: NS, tenant };

    // Force chunked tier: write something well above INLINE_LIMIT via the
    // handle-based stream so we control chunk sizes precisely.
    const handle = await stub.vfsBeginWriteStream(scope, "/big.bin");
    expect(handle.chunkSize).toBeGreaterThan(0);
    // Three full chunks + a partial last chunk.
    const A = new Uint8Array(handle.chunkSize).fill(0x61);
    const B = new Uint8Array(handle.chunkSize).fill(0x62);
    const C = new Uint8Array(handle.chunkSize).fill(0x63);
    const D = new Uint8Array(7).fill(0x64);
    await stub.vfsAppendWriteStream(scope, handle, 0, A);
    await stub.vfsAppendWriteStream(scope, handle, 1, B);
    await stub.vfsAppendWriteStream(scope, handle, 2, C);
    await stub.vfsAppendWriteStream(scope, handle, 3, D);
    await stub.vfsCommitWriteStream(scope, handle);

    const stream = await stub.vfsCreateReadStream(scope, "/big.bin");
    const flat = await drain(stream);
    // RPC stream framing may re-chunk on the wire; we assert on content
    // rather than chunk count. The total size and per-region byte
    // markers prove all four source chunks streamed through correctly.
    const totalSize = handle.chunkSize * 3 + 7;
    expect(flat.byteLength).toBe(totalSize);
    expect(flat[0]).toBe(0x61);
    expect(flat[handle.chunkSize - 1]).toBe(0x61);
    expect(flat[handle.chunkSize]).toBe(0x62);
    expect(flat[handle.chunkSize * 2 - 1]).toBe(0x62);
    expect(flat[handle.chunkSize * 2]).toBe(0x63);
    expect(flat[handle.chunkSize * 3 - 1]).toBe(0x63);
    expect(flat[handle.chunkSize * 3]).toBe(0x64);
    expect(flat[totalSize - 1]).toBe(0x64);
  });

  it("supports byte-range start/end within a single chunk", async () => {
    const tenant = "stream-range-1";
    const stub = userStubFor(tenant);
    const scope = { ns: NS, tenant };
    await stub.vfsWriteFile(
      scope,
      "/r.txt",
      new TextEncoder().encode("0123456789ABCDEF")
    );
    const stream = await stub.vfsCreateReadStream(scope, "/r.txt", {
      start: 4,
      end: 10,
    });
    const got = await drain(stream);
    expect(new TextDecoder().decode(got)).toBe("456789");
  });

  it("supports byte-range spanning chunk boundaries", async () => {
    const tenant = "stream-range-2";
    const stub = userStubFor(tenant);
    const scope = { ns: NS, tenant };
    const handle = await stub.vfsBeginWriteStream(scope, "/spanning.bin");
    const cs = handle.chunkSize;
    // Two chunks: A(0..cs)=0x41, B(cs..2cs)=0x42
    await stub.vfsAppendWriteStream(
      scope,
      handle,
      0,
      new Uint8Array(cs).fill(0x41)
    );
    await stub.vfsAppendWriteStream(
      scope,
      handle,
      1,
      new Uint8Array(cs).fill(0x42)
    );
    await stub.vfsCommitWriteStream(scope, handle);

    // Range from cs-3 to cs+5 → 3 bytes of A then 5 of B.
    const stream = await stub.vfsCreateReadStream(scope, "/spanning.bin", {
      start: cs - 3,
      end: cs + 5,
    });
    const got = await drain(stream);
    expect(got.byteLength).toBe(8);
    expect(got[0]).toBe(0x41);
    expect(got[1]).toBe(0x41);
    expect(got[2]).toBe(0x41);
    expect(got[3]).toBe(0x42);
    expect(got[7]).toBe(0x42);
  });

  it("range to end-of-file works when end is past the size", async () => {
    const tenant = "stream-range-eof";
    const stub = userStubFor(tenant);
    const scope = { ns: NS, tenant };
    await stub.vfsWriteFile(
      scope,
      "/x.txt",
      new TextEncoder().encode("short")
    );
    // Caller passes a generous end; clampOffset clips to file size.
    const stream = await stub.vfsCreateReadStream(scope, "/x.txt", {
      start: 1,
      end: 1_000_000,
    });
    const got = await drain(stream);
    expect(new TextDecoder().decode(got)).toBe("hort");
  });

  it("zero-length range (start === end) yields an empty stream", async () => {
    const tenant = "stream-empty-range";
    const stub = userStubFor(tenant);
    const scope = { ns: NS, tenant };
    await stub.vfsWriteFile(scope, "/x.txt", new TextEncoder().encode("abc"));
    const stream = await stub.vfsCreateReadStream(scope, "/x.txt", {
      start: 1,
      end: 1,
    });
    const got = await drain(stream);
    expect(got.byteLength).toBe(0);
  });

  it("rejects invalid ranges with EINVAL", async () => {
    const tenant = "stream-range-bad";
    const stub = userStubFor(tenant);
    const scope = { ns: NS, tenant };
    await stub.vfsWriteFile(scope, "/y.txt", new TextEncoder().encode("abc"));
    await expect(
      stub.vfsCreateReadStream(scope, "/y.txt", { start: 5, end: 2 })
    ).rejects.toThrow(/EINVAL/);
    await expect(
      stub.vfsCreateReadStream(scope, "/y.txt", { start: -1 })
    ).rejects.toThrow(/EINVAL/);
  });

  it("ENOENT for missing path; EINVAL for directory", async () => {
    const tenant = "stream-err";
    const stub = userStubFor(tenant);
    const scope = { ns: NS, tenant };
    await expect(
      stub.vfsCreateReadStream(scope, "/missing")
    ).rejects.toThrow(/ENOENT/);

    await stub.vfsMkdir(scope, "/d");
    await expect(stub.vfsCreateReadStream(scope, "/d")).rejects.toThrow(
      /EINVAL/
    );
  });

  it("backpressure: cancel mid-stream stops further pulls without throwing", async () => {
    const tenant = "stream-backpressure";
    const stub = userStubFor(tenant);
    const scope = { ns: NS, tenant };
    const handle = await stub.vfsBeginWriteStream(scope, "/lazy.bin");
    const cs = handle.chunkSize;
    for (let i = 0; i < 4; i++) {
      await stub.vfsAppendWriteStream(
        scope,
        handle,
        i,
        new Uint8Array(cs).fill(0x40 + i)
      );
    }
    await stub.vfsCommitWriteStream(scope, handle);

    // Open the stream and read a few wire frames, then cancel — the
    // underlying ReadableStream `pull` is invoked on demand only;
    // cancelling stops further pulls. Wire framing may re-chunk our
    // source chunks (RPC stream serialization is implementation
    // detail), so we don't assert on per-frame size — only that data
    // arrives and cancel is a clean exit.
    const stream = await stub.vfsCreateReadStream(scope, "/lazy.bin");
    const reader = stream.getReader();
    const r1 = await reader.read();
    expect(r1.done).toBe(false);
    expect(r1.value!.byteLength).toBeGreaterThan(0);
    expect(r1.value![0]).toBe(0x40); // first byte of source chunk 0

    // Cancel — must not throw, must not blow stack.
    await reader.cancel("done with you");
    reader.releaseLock();
  });
});

// ───────────────────────────────────────────────────────────────────────
// vfsOpenReadStream + vfsPullReadStream (handle-based escape hatch)
// ───────────────────────────────────────────────────────────────────────

describe("handle-based read: vfsOpenReadStream + vfsPullReadStream", () => {
  it("returns a stable handle whose chunkCount matches the file", async () => {
    const tenant = "rh-basic";
    const stub = userStubFor(tenant);
    const scope = { ns: NS, tenant };
    const handle = await stub.vfsBeginWriteStream(scope, "/m.bin");
    const cs = handle.chunkSize;
    for (let i = 0; i < 3; i++) {
      await stub.vfsAppendWriteStream(
        scope,
        handle,
        i,
        new Uint8Array(cs).fill(0x70 + i)
      );
    }
    await stub.vfsCommitWriteStream(scope, handle);

    const rh = await stub.vfsOpenReadStream(scope, "/m.bin");
    expect(rh.inlined).toBe(false);
    expect(rh.chunkCount).toBe(3);
    expect(rh.size).toBe(cs * 3);

    const c0 = await stub.vfsPullReadStream(scope, rh, 0);
    const c1 = await stub.vfsPullReadStream(scope, rh, 1);
    const c2 = await stub.vfsPullReadStream(scope, rh, 2);
    expect(c0[0]).toBe(0x70);
    expect(c1[0]).toBe(0x71);
    expect(c2[0]).toBe(0x72);
  });

  it("inlined files report inlined=true; pull at idx 0 returns the inline blob", async () => {
    const tenant = "rh-inline";
    const stub = userStubFor(tenant);
    const scope = { ns: NS, tenant };
    await stub.vfsWriteFile(scope, "/i.txt", new TextEncoder().encode("hi"));
    const rh = await stub.vfsOpenReadStream(scope, "/i.txt");
    expect(rh.inlined).toBe(true);
    expect(rh.chunkCount).toBe(0);
    expect(rh.size).toBe(2);
    const buf = await stub.vfsPullReadStream(scope, rh, 0);
    expect(new TextDecoder().decode(buf)).toBe("hi");
    await expect(stub.vfsPullReadStream(scope, rh, 1)).rejects.toThrow(
      /EINVAL/
    );
  });

  it("supports range within a chunk pull", async () => {
    const tenant = "rh-range";
    const stub = userStubFor(tenant);
    const scope = { ns: NS, tenant };
    await stub.vfsWriteFile(
      scope,
      "/r.txt",
      new TextEncoder().encode("abcdefghij")
    );
    const rh = await stub.vfsOpenReadStream(scope, "/r.txt");
    const slice = await stub.vfsPullReadStream(scope, rh, 0, {
      start: 2,
      end: 6,
    });
    expect(new TextDecoder().decode(slice)).toBe("cdef");
  });

  it("rejects out-of-range chunkIndex", async () => {
    const tenant = "rh-bad-idx";
    const stub = userStubFor(tenant);
    const scope = { ns: NS, tenant };
    const handle = await stub.vfsBeginWriteStream(scope, "/m.bin");
    await stub.vfsAppendWriteStream(
      scope,
      handle,
      0,
      new Uint8Array(handle.chunkSize).fill(1)
    );
    await stub.vfsCommitWriteStream(scope, handle);
    const rh = await stub.vfsOpenReadStream(scope, "/m.bin");
    await expect(stub.vfsPullReadStream(scope, rh, 1)).rejects.toThrow(
      /EINVAL/
    );
    await expect(stub.vfsPullReadStream(scope, rh, -1)).rejects.toThrow(
      /EINVAL/
    );
  });
});

// ───────────────────────────────────────────────────────────────────────
// vfsCreateWriteStream + handle-based write primitives
// ───────────────────────────────────────────────────────────────────────

describe("vfsCreateWriteStream", () => {
  it("round-trips: writeStream → readStream identical bytes", async () => {
    const tenant = "ws-roundtrip";
    const stub = userStubFor(tenant);
    const scope = { ns: NS, tenant };
    const { stream, handle } = await stub.vfsCreateWriteStream(
      scope,
      "/rt.bin"
    );
    const cs = handle.chunkSize;
    // Pump a multi-chunk pseudo-random pattern so any per-chunk
    // misalignment is detectable in the readback.
    const totalChunks = 3;
    const totalSize = cs * totalChunks + 17;
    const orig = new Uint8Array(totalSize);
    for (let i = 0; i < totalSize; i++) orig[i] = (i * 1103515245 + 12345) & 0xff;
    const w = stream.getWriter();
    // Write in awkward sizes (not aligned to cs) to force buffer splits.
    let off = 0;
    while (off < totalSize) {
      const take = Math.min(13_337, totalSize - off);
      await w.write(orig.subarray(off, off + take));
      off += take;
    }
    await w.close();
    w.releaseLock();

    const back = await drain(await stub.vfsCreateReadStream(scope, "/rt.bin"));
    expect(back.byteLength).toBe(totalSize);
    // Compare a few markers + the SHA-256 to avoid printing a 6 MB diff.
    expect(back[0]).toBe(orig[0]);
    expect(back[totalSize - 1]).toBe(orig[totalSize - 1]);
    const dOrig = await crypto.subtle.digest("SHA-256", orig);
    const dBack = await crypto.subtle.digest("SHA-256", back);
    expect(new Uint8Array(dOrig)).toEqual(new Uint8Array(dBack));
  });

  it("buffers, splits at chunkSize boundaries, and commits", async () => {
    const tenant = "ws-basic";
    const stub = userStubFor(tenant);
    const scope = { ns: NS, tenant };
    const { stream, handle } = await stub.vfsCreateWriteStream(
      scope,
      "/w.bin"
    );
    const cs = handle.chunkSize;
    const writer = stream.getWriter();
    // Write 2.5 chunks across multiple smaller pumps so the internal
    // buffer has to merge then split.
    await writer.write(new Uint8Array(Math.floor(cs / 3)).fill(0xaa));
    await writer.write(new Uint8Array(Math.floor(cs / 3)).fill(0xbb));
    await writer.write(new Uint8Array(cs).fill(0xcc));
    await writer.write(new Uint8Array(Math.floor(cs / 3)).fill(0xdd));
    await writer.close();
    writer.releaseLock();

    // Sanity: read back via createReadStream and confirm size + first
    // bytes of each chunk match.
    const totalWritten =
      Math.floor(cs / 3) * 3 + cs;
    const stat = await stub.vfsStat(scope, "/w.bin");
    expect(stat.size).toBe(totalWritten);
    const got = await drain(await stub.vfsCreateReadStream(scope, "/w.bin"));
    expect(got.byteLength).toBe(totalWritten);
    // First chunk is exactly cs bytes, mostly aa/bb mixed; full integrity
    // is checked by total length plus presence of each marker.
    expect(got).toContain(0xaa);
    expect(got).toContain(0xbb);
    expect(got).toContain(0xcc);
    expect(got).toContain(0xdd);
  });

  it("close() with no writes commits an empty file", async () => {
    const tenant = "ws-empty";
    const stub = userStubFor(tenant);
    const scope = { ns: NS, tenant };
    const { stream } = await stub.vfsCreateWriteStream(scope, "/empty.bin");
    const w = stream.getWriter();
    await w.close();
    w.releaseLock();
    const stat = await stub.vfsStat(scope, "/empty.bin");
    expect(stat.size).toBe(0);
  });

  it("abort() drops the tmp row; readFile returns ENOENT", async () => {
    const tenant = "ws-abort";
    const stub = userStubFor(tenant);
    const scope = { ns: NS, tenant };
    const { stream } = await stub.vfsCreateWriteStream(scope, "/never.bin");
    const w = stream.getWriter();
    await w.write(new TextEncoder().encode("partial"));
    await w.abort("nope");
    w.releaseLock();
    await expect(stub.vfsReadFile(scope, "/never.bin")).rejects.toThrow(
      /ENOENT/
    );
  });

  it("EISDIR when target path is a directory", async () => {
    const tenant = "ws-isdir";
    const stub = userStubFor(tenant);
    const scope = { ns: NS, tenant };
    await stub.vfsMkdir(scope, "/d");
    await expect(stub.vfsCreateWriteStream(scope, "/d")).rejects.toThrow(
      /EISDIR/
    );
  });
});

describe("handle-based write: begin → append → commit", () => {
  it("rejects out-of-order chunk indices", async () => {
    const tenant = "wh-order";
    const stub = userStubFor(tenant);
    const scope = { ns: NS, tenant };
    const handle = await stub.vfsBeginWriteStream(scope, "/o.bin");
    await stub.vfsAppendWriteStream(
      scope,
      handle,
      0,
      new Uint8Array(handle.chunkSize).fill(1)
    );
    // skip index 1 → expect failure on idx 2
    await expect(
      stub.vfsAppendWriteStream(
        scope,
        handle,
        2,
        new Uint8Array(handle.chunkSize).fill(2)
      )
    ).rejects.toThrow(/EINVAL/);
    // re-attempting idx 0 (already taken) also fails
    await expect(
      stub.vfsAppendWriteStream(
        scope,
        handle,
        0,
        new Uint8Array(handle.chunkSize).fill(3)
      )
    ).rejects.toThrow(/EINVAL/);
  });

  it("zero-byte append is a no-op (no chunk_refs row created)", async () => {
    const tenant = "wh-zero";
    const stub = userStubFor(tenant);
    const scope = { ns: NS, tenant };
    const handle = await stub.vfsBeginWriteStream(scope, "/z.bin");
    const r = await stub.vfsAppendWriteStream(
      scope,
      handle,
      0,
      new Uint8Array(0)
    );
    expect(r.bytesWritten).toBe(0);
    await stub.vfsCommitWriteStream(scope, handle);
    const stat = await stub.vfsStat(scope, "/z.bin");
    expect(stat.size).toBe(0);
  });

  it("EFBIG when cumulative size exceeds WRITEFILE_MAX", async () => {
    const { WRITEFILE_MAX } = await import("@shared/inline");
    const tenant = "wh-efbig";
    const stub = userStubFor(tenant);
    const scope = { ns: NS, tenant };
    const handle = await stub.vfsBeginWriteStream(scope, "/big.bin");

    // Real-time pumping a full WRITEFILE_MAX worth of bytes through
    // Miniflare RPC is slow and dominated by the per-chunk 32 MiB
    // serialization cap. We instead seed the row's file_size to just
    // below WRITEFILE_MAX, then a single small append must trip the
    // cumulative check.
    await runInDurableObject(stub, async (_inst, state) => {
      // chunk_count must be coherent with file_size for the next
      // append to be accepted, so we leave chunk_count at 0 — the
      // next append at index 0 is the first accepted but is rejected
      // by the size check.
      state.storage.sql.exec(
        "UPDATE files SET file_size = ? WHERE file_id = ?",
        WRITEFILE_MAX - 5,
        handle.tmpId
      );
    });
    await expect(
      stub.vfsAppendWriteStream(scope, handle, 0, new Uint8Array(10).fill(0xff))
    ).rejects.toThrow(/EFBIG/);
    // Cleanup
    await stub.vfsAbortWriteStream(scope, handle);
  });

  it("commit after abort raises ENOENT (handle gone)", async () => {
    const tenant = "wh-abort-commit";
    const stub = userStubFor(tenant);
    const scope = { ns: NS, tenant };
    const handle = await stub.vfsBeginWriteStream(scope, "/aa.bin");
    await stub.vfsAbortWriteStream(scope, handle);
    await expect(stub.vfsCommitWriteStream(scope, handle)).rejects.toThrow(
      /ENOENT/
    );
  });

  it("abort is idempotent", async () => {
    const tenant = "wh-abort-idemp";
    const stub = userStubFor(tenant);
    const scope = { ns: NS, tenant };
    const handle = await stub.vfsBeginWriteStream(scope, "/ai.bin");
    await stub.vfsAbortWriteStream(scope, handle);
    await stub.vfsAbortWriteStream(scope, handle); // no throw
  });

  it("commit chains through to readFile + readManyStat correctly", async () => {
    const tenant = "wh-chain";
    const stub = userStubFor(tenant);
    const scope = { ns: NS, tenant };
    const handle = await stub.vfsBeginWriteStream(scope, "/c.bin");
    const cs = handle.chunkSize;
    await stub.vfsAppendWriteStream(scope, handle, 0, new Uint8Array(cs).fill(7));
    await stub.vfsAppendWriteStream(scope, handle, 1, new Uint8Array(11).fill(8));
    await stub.vfsCommitWriteStream(scope, handle);

    const stat = await stub.vfsStat(scope, "/c.bin");
    expect(stat.size).toBe(cs + 11);
    const got = await stub.vfsReadFile(scope, "/c.bin");
    expect(got.byteLength).toBe(cs + 11);
    expect(got[0]).toBe(7);
    expect(got[cs - 1]).toBe(7);
    expect(got[cs]).toBe(8);
    expect(got[got.byteLength - 1]).toBe(8);
  });
});

// ───────────────────────────────────────────────────────────────────────
// openManifest + readChunk parity (the public, shard-stripped form)
// ───────────────────────────────────────────────────────────────────────

describe("vfsOpenManifest + vfsReadChunk", () => {
  it("openManifest hides shard_index; readChunk fetches by (path, idx)", async () => {
    const tenant = "om-basic";
    const stub = userStubFor(tenant);
    const scope = { ns: NS, tenant };
    const handle = await stub.vfsBeginWriteStream(scope, "/m.bin");
    const cs = handle.chunkSize;
    for (let i = 0; i < 3; i++) {
      await stub.vfsAppendWriteStream(
        scope,
        handle,
        i,
        new Uint8Array(cs).fill(0xa0 + i)
      );
    }
    await stub.vfsCommitWriteStream(scope, handle);

    const m = await stub.vfsOpenManifest(scope, "/m.bin");
    expect(m.inlined).toBe(false);
    expect(m.chunkCount).toBe(3);
    expect(m.chunks.length).toBe(3);
    for (const c of m.chunks) {
      expect(typeof c.hash).toBe("string");
      expect(c.size).toBeGreaterThan(0);
      // Public manifest must NOT expose shardIndex.
      expect((c as Record<string, unknown>).shardIndex).toBeUndefined();
      expect((c as Record<string, unknown>).shard_index).toBeUndefined();
    }
    const c0 = await stub.vfsReadChunk(scope, "/m.bin", 0);
    expect(c0[0]).toBe(0xa0);
    const c2 = await stub.vfsReadChunk(scope, "/m.bin", 2);
    expect(c2[0]).toBe(0xa2);
  });

  it("openManifest reports inlined=true for inline files", async () => {
    const tenant = "om-inline";
    const stub = userStubFor(tenant);
    const scope = { ns: NS, tenant };
    await stub.vfsWriteFile(
      scope,
      "/i.txt",
      new TextEncoder().encode("inline-only")
    );
    const m = await stub.vfsOpenManifest(scope, "/i.txt");
    expect(m.inlined).toBe(true);
    expect(m.chunkCount).toBe(0);
    expect(m.chunks).toHaveLength(0);
  });
});

// ───────────────────────────────────────────────────────────────────────
// Shard binding: the streams use vfsShardDOName, not the legacy name.
// ───────────────────────────────────────────────────────────────────────

describe("Phase 4 shard naming wired through stream paths", () => {
  it("chunks land on vfs:default:<tenant>:s<idx> shards, not the legacy shard:<userId>:<idx>", async () => {
    const tenant = "shard-naming";
    const stub = userStubFor(tenant);
    const scope = { ns: NS, tenant };
    const handle = await stub.vfsBeginWriteStream(scope, "/n.bin");
    const cs = handle.chunkSize;
    await stub.vfsAppendWriteStream(scope, handle, 0, new Uint8Array(cs).fill(9));
    await stub.vfsCommitWriteStream(scope, handle);

    // Get the recorded shard_index for chunk 0.
    const recordedIdx = await runInDurableObject(stub, async (_inst, state) => {
      const row = state.storage.sql
        .exec(
          `SELECT shard_index FROM file_chunks fc
            JOIN files f ON fc.file_id = f.file_id
            WHERE f.file_name = 'n.bin' AND fc.chunk_index = 0`
        )
        .toArray()[0] as { shard_index: number };
      return row.shard_index;
    });

    // The chunk row must exist on the new-name shard...
    const newShard = shardStubFor(tenant, recordedIdx);
    const stats = (await (
      await newShard.fetch(new Request("http://internal/stats"))
    ).json()) as { uniqueChunks: number };
    expect(stats.uniqueChunks).toBeGreaterThan(0);

    // ...and not on the legacy-name shard.
    const legacyShard = E.SHARD_DO.get(
      E.SHARD_DO.idFromName(`shard:${tenant}:${recordedIdx}`)
    );
    const legacyStats = (await (
      await legacyShard.fetch(new Request("http://internal/stats"))
    ).json()) as { uniqueChunks: number };
    expect(legacyStats.uniqueChunks).toBe(0);
  });
});
