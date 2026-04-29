/**
 * E2E C — Streaming + 50MB SHA round-trip + Phase 13 stream metadata
 * (5 cases).
 *
 * The HTTP fallback does NOT support createReadStream/createWriteStream
 * (sdk/src/http.ts:351 — explicit EINVAL). Instead we drive
 * `openManifest` + `readChunk` for caller-orchestrated reads and use
 * the buffered `writeFile` path for writes (with the 100MB ceiling).
 */

import { describe, it, expect, beforeEach, afterEach, beforeAll } from "vitest";
import { createHash } from "node:crypto";
import { freshTenant, type TenantCtx } from "./helpers/tenant.js";
import { hasSecret, requireSecret } from "./helpers/env.js";

describe.skipIf(!hasSecret())("C — Streaming + 50MB SHA + Phase 13 metadata", () => {
  beforeAll(() => requireSecret());

  let ctx: TenantCtx;
  beforeEach(async () => { ctx = await freshTenant(); });
  afterEach(async () => { await ctx.teardown(); });

  it("C.1 — openManifest + readChunk loop reconstructs a 5MB file byte-equal", async () => {
    const N = 5 * 1024 * 1024;
    const payload = new Uint8Array(N);
    // Deterministic-ish pattern (cheaper than crypto random for 5MB).
    for (let i = 0; i < N; i++) payload[i] = (i * 1103515245 + 12345) & 0xff;
    await ctx.vfs.writeFile("/big5.bin", payload);
    const m = await ctx.vfs.openManifest("/big5.bin");
    expect(m.size).toBe(N);
    let assembled: Uint8Array;
    if (m.inlined) {
      assembled = await ctx.vfs.readFile("/big5.bin");
    } else {
      assembled = new Uint8Array(N);
      let off = 0;
      for (let i = 0; i < m.chunkCount; i++) {
        const c = await ctx.vfs.readChunk("/big5.bin", i);
        assembled.set(c, off);
        off += c.byteLength;
      }
    }
    const a = createHash("sha256").update(payload).digest("hex");
    const b = createHash("sha256").update(assembled).digest("hex");
    expect(b).toBe(a);
  }, 60_000);

  it("C.2 — partial chunk read: reading only chunk 0 matches the corresponding offset slice", async () => {
    const N = 3 * 1024 * 1024 + 511; // arbitrary mid-chunk size
    const payload = new Uint8Array(N);
    for (let i = 0; i < N; i++) payload[i] = (i ^ (i >> 7)) & 0xff;
    await ctx.vfs.writeFile("/c2.bin", payload);
    const m = await ctx.vfs.openManifest("/c2.bin");
    if (m.inlined) {
      // Skip — file inlined; no chunk-level access needed.
      expect(m.chunkCount).toBe(0);
      return;
    }
    const c0 = await ctx.vfs.readChunk("/c2.bin", 0);
    const slice = payload.slice(0, c0.byteLength);
    const a = createHash("sha256").update(slice).digest("hex");
    const b = createHash("sha256").update(c0).digest("hex");
    expect(b).toBe(a);
  }, 60_000);

  it("C.3 — large SHA round-trip via writeFile + openManifest/readChunk (under DO RPC cap)", async () => {
    // Cloudflare DO RPC caps a single argument at 32 MiB; the HTTP
    // fallback's writeFile forwards the entire body via one RPC, so
    // the live ceiling for `writeFile` over HTTP is < 32 MiB. We
    // exercise a 25 MiB round-trip here — large enough to span many
    // chunks (default 1 MiB chunk size → 25+ chunks) so the
    // openManifest/readChunk reassembly path is genuinely tested.
    // The build-spec's 50 MB target requires the binding-mode SDK
    // (createWriteStream) and is exercised by the SDK's
    // tests/integration/streaming.test.ts in-Worker.
    const N = 25 * 1024 * 1024;
    const payload = new Uint8Array(N);
    const { randomFillSync } = await import("node:crypto");
    const block = new Uint8Array(1024 * 1024);
    randomFillSync(block);
    for (let off = 0; off < N; off += block.length) {
      payload.set(block.subarray(0, Math.min(block.length, N - off)), off);
    }
    const expectedHash = createHash("sha256").update(payload).digest("hex");

    await ctx.vfs.writeFile("/big25.bin", payload);
    const m = await ctx.vfs.openManifest("/big25.bin");
    expect(m.size).toBe(N);

    const got = new Uint8Array(N);
    let off = 0;
    if (m.inlined) {
      const buf = await ctx.vfs.readFile("/big25.bin");
      got.set(buf, 0);
      off = buf.byteLength;
    } else {
      for (let i = 0; i < m.chunkCount; i++) {
        const c = await ctx.vfs.readChunk("/big25.bin", i);
        got.set(c, off);
        off += c.byteLength;
      }
    }
    expect(off).toBe(N);
    expect(createHash("sha256").update(got).digest("hex")).toBe(expectedHash);
    expect(m.chunkCount).toBeGreaterThanOrEqual(20);
  }, 600_000);

  it("C.4 — Phase 13 stream metadata commit (multipart envelope)", async () => {
    const payload = new TextEncoder().encode("phase 13 metadata payload");
    await ctx.vfs.writeFile("/c4.bin", payload, {
      mimeType: "application/octet-stream",
      metadata: { camera: "Pixel 8", iso: 200 },
      tags: ["nature", "2026"],
    });
    const stat = await ctx.vfs.stat("/c4.bin");
    expect(stat.size).toBe(payload.byteLength);
    const page = await ctx.vfs.listFiles({
      tags: ["nature"],
      includeMetadata: true,
    });
    expect(page.items.find((i) => i.path === "/c4.bin")).toBeDefined();
    const item = page.items.find((i) => i.path === "/c4.bin")!;
    expect(item.metadata).toMatchObject({ camera: "Pixel 8", iso: 200 });
    expect(item.tags.sort()).toEqual(["2026", "nature"]);
  });

  it("C.5 — write past Cloudflare DO RPC cap surfaces a structured error (EFBIG / EINVAL / unavailable)", async () => {
    // Cloudflare DO RPC caps args at 32 MiB. The build-spec target is
    // WRITEFILE_MAX (100 MB) which is the binding-mode ceiling; over
    // HTTP, we'll hit the RPC arg cap first. Either way the failure
    // is a clean structured error (not a process crash). We allocate
    // 33 MiB which is over the 32 MiB RPC cap and well under any
    // CI memory pressure.
    const N = 33 * 1024 * 1024;
    const payload = new Uint8Array(N);
    await expect(ctx.vfs.writeFile("/c5.bin", payload)).rejects.toMatchObject({
      code: expect.stringMatching(/^E(FBIG|INVAL|MOSSAIC_UNAVAILABLE)$/),
    });
  }, 300_000);
});
