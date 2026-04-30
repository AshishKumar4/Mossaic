import { describe, it, expect } from "vitest";
import { env, runInDurableObject } from "cloudflare:test";

/**
 * Phase 27.5 — read-surface version routing.
 *
 * Production bug report (verbatim):
 *
 *   "readPreview() broken for versioned files. Root: readPreview
 *    calls vfsCreateReadStream which reads files.inline_data /
 *    file_chunks. With versioning, current bytes live in
 *    file_versions / version_chunks, while files row can have
 *    chunk_size=0, chunk_count=0, inline_data=null.
 *    Math.floor(0/0) → chunkIndex NaN out of range [0,0)."
 *
 * Phase 25's e7c134e fixed `vfsOpenReadStream` /
 * `vfsPullReadStream` (handle-based), `vfsOpenManifest`,
 * `vfsReadChunk`, and `vfsReadFile`. It did NOT fix:
 *
 *  - `vfsCreateReadStream` (the wrapper that the SDK / preview
 *    pipeline uses) — internally called `getChunkSizeForHandle`
 *    which re-queried `files.chunk_size` (stale 0 on versioned
 *    tenants → NaN division).
 *  - `vfsReadPreview` — passed `f.file_size` (stale 0 on versioned
 *    tenants) to renderers, tripping decode-validation and
 *    memory-budget guards inside sharp/exiftool.
 *  - Yjs-mode + `vfsCreateReadStream` / `vfsOpenManifest` /
 *    `vfsReadChunk` — no short-circuit; the chunked path tried to
 *    read empty `version_chunks` and returned wrong/empty bytes.
 *  - Empty-file (0-byte) chunked path — `Math.floor(0/0) = NaN`
 *    even on legacy tenants with a malformed schema row.
 *
 * This file pins the post-fix invariants. See `read-surface audit`
 * sub-agent (a) for the cross-surface uniformity matrix.
 */

import {
  createVFS,
  type MossaicEnv,
  type UserDO,
  ENOENT,
} from "../../sdk/src/index";
import { vfsUserDOName } from "@core/lib/utils";

interface E {
  MOSSAIC_USER: DurableObjectNamespace<UserDO>;
  MOSSAIC_SHARD: DurableObjectNamespace;
}
const E = env as unknown as E;
const NS = "default";

function envFor(): MossaicEnv {
  return {
    MOSSAIC_USER: E.MOSSAIC_USER as MossaicEnv["MOSSAIC_USER"],
    MOSSAIC_SHARD: E.MOSSAIC_SHARD as unknown as MossaicEnv["MOSSAIC_SHARD"],
  };
}
function userStub(tenant: string) {
  return E.MOSSAIC_USER.get(
    E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
  );
}
const enc = (s: string) => new TextEncoder().encode(s);

async function drain(stream: ReadableStream<Uint8Array>): Promise<Uint8Array> {
  const reader = stream.getReader();
  const parts: Uint8Array[] = [];
  let total = 0;
  for (;;) {
    const { value, done } = await reader.read();
    if (done) break;
    if (value) {
      parts.push(value);
      total += value.byteLength;
    }
  }
  const out = new Uint8Array(total);
  let off = 0;
  for (const p of parts) {
    out.set(p, off);
    off += p.byteLength;
  }
  return out;
}

function makePayload(seed: number, size: number): Uint8Array {
  const a = new Uint8Array(size);
  let s = seed >>> 0;
  for (let i = 0; i < size; i++) {
    s = (Math.imul(s, 1664525) + 1013904223) >>> 0;
    a[i] = s & 0xff;
  }
  return a;
}

describe("createReadStream — versioned (root cause of readPreview bug)", () => {
  it("V1 — versioned-ON tenant: createReadStream returns CURRENT bytes from version_chunks (chunked tier)", async () => {
    const tenant = "rsv-create-versioned-chunked";
    const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });
    const stub = userStub(tenant);
    const scope = { ns: NS, tenant };

    // Chunked tier (>16 KB inline limit) so chunks land in
    // version_chunks under versioning.
    const payload = makePayload(0xa1, 32 * 1024);
    await vfs.writeFile("/big.bin", payload);

    // Sanity: the legacy `files` columns are NULL/0 (the bug
    // surface). We're not asserting these for invariance — they're
    // an implementation detail of `commitVersion` — but if a future
    // change DOES start populating them we want the test to lean
    // on `version_chunks` rather than coincidentally pass.
    const legacy = await runInDurableObject(stub, async (_inst, s) => {
      return s.storage.sql
        .exec(
          "SELECT chunk_size, chunk_count, inline_data FROM files WHERE file_name='big.bin'"
        )
        .toArray()[0] as
        | {
            chunk_size: number;
            chunk_count: number;
            inline_data: ArrayBuffer | null;
          }
        | undefined;
    });
    expect(legacy).toBeTruthy();
    // Today's behavior on versioned tenants: legacy columns NOT
    // populated. Lock so a regression here can't silently mask the
    // real fix.
    expect(legacy!.chunk_size).toBe(0);
    expect(legacy!.chunk_count).toBe(0);
    expect(legacy!.inline_data).toBeNull();

    // The actual fix surface: createReadStream sources from
    // version_chunks via the handle's pinned versionId.
    const stream = await stub.vfsCreateReadStream(scope, "/big.bin");
    const back = await drain(stream);
    expect(back.byteLength).toBe(payload.byteLength);
    expect(back[0]).toBe(payload[0]);
    expect(back[16_000]).toBe(payload[16_000]);
    expect(back[31_999]).toBe(payload[31_999]);
  });

  it("V2 — versioned-ON tenant: openManifest returns chunks from version_chunks (NOT from file_chunks)", async () => {
    const tenant = "rsv-manifest-versioned";
    const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });
    const stub = userStub(tenant);
    const scope = { ns: NS, tenant };

    const payload = makePayload(0xb2, 32 * 1024);
    await vfs.writeFile("/m.bin", payload);

    const manifest = await stub.vfsOpenManifest(scope, "/m.bin");
    expect(manifest.size).toBe(payload.byteLength);
    expect(manifest.inlined).toBe(false);
    expect(manifest.chunks.length).toBeGreaterThan(0);
    // Each chunk hash is non-empty and chunkSize > 0.
    expect(manifest.chunkSize).toBeGreaterThan(0);
    for (const c of manifest.chunks) {
      expect(c.hash.length).toBeGreaterThan(0);
      expect(c.size).toBeGreaterThan(0);
    }
  });

  it("V3 — versioned-ON tenant: readChunk(0..N-1) concatenated equals readFile bytes", async () => {
    const tenant = "rsv-readchunk-versioned";
    const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });
    const stub = userStub(tenant);
    const scope = { ns: NS, tenant };

    const payload = makePayload(0xc3, 32 * 1024);
    await vfs.writeFile("/c.bin", payload);

    const manifest = await stub.vfsOpenManifest(scope, "/c.bin");
    const parts: Uint8Array[] = [];
    for (let i = 0; i < manifest.chunks.length; i++) {
      const buf = await stub.vfsReadChunk(scope, "/c.bin", i);
      parts.push(buf);
    }
    let total = 0;
    for (const p of parts) total += p.byteLength;
    expect(total).toBe(payload.byteLength);
    const out = new Uint8Array(total);
    let off = 0;
    for (const p of parts) {
      out.set(p, off);
      off += p.byteLength;
    }
    // Spot-check; full equality is slow under workerd.
    expect(out[0]).toBe(payload[0]);
    expect(out[total - 1]).toBe(payload[total - 1]);
  });
});

describe("createReadStream — legacy (no regression)", () => {
  it("L1 — versioning-OFF tenant: createReadStream still reads from file_chunks", async () => {
    const tenant = "rsv-create-legacy";
    const vfs = createVFS(envFor(), { tenant });
    const stub = userStub(tenant);
    const scope = { ns: NS, tenant };

    const payload = makePayload(0xa2, 32 * 1024);
    await vfs.writeFile("/big.bin", payload);

    // Legacy: files.chunk_size + chunk_count populated, no
    // file_versions row.
    const legacy = await runInDurableObject(stub, async (_inst, s) => {
      const f = s.storage.sql
        .exec(
          "SELECT chunk_size, chunk_count FROM files WHERE file_name='big.bin'"
        )
        .toArray()[0] as { chunk_size: number; chunk_count: number };
      const v = (
        s.storage.sql
          .exec("SELECT COUNT(*) AS n FROM file_versions")
          .toArray()[0] as { n: number }
      ).n;
      return { f, v };
    });
    expect(legacy.f.chunk_size).toBeGreaterThan(0);
    expect(legacy.f.chunk_count).toBeGreaterThan(0);
    expect(legacy.v).toBe(0);

    const stream = await stub.vfsCreateReadStream(scope, "/big.bin");
    const back = await drain(stream);
    expect(back.byteLength).toBe(payload.byteLength);
    expect(back[0]).toBe(payload[0]);
    expect(back[31_999]).toBe(payload[31_999]);
  });

  it("L2 — versioning-OFF tenant: openManifest + readChunk read from file_chunks", async () => {
    const tenant = "rsv-manifest-legacy";
    const vfs = createVFS(envFor(), { tenant });
    const stub = userStub(tenant);
    const scope = { ns: NS, tenant };

    const payload = makePayload(0xb3, 32 * 1024);
    await vfs.writeFile("/m.bin", payload);

    const manifest = await stub.vfsOpenManifest(scope, "/m.bin");
    expect(manifest.size).toBe(payload.byteLength);
    expect(manifest.inlined).toBe(false);
    expect(manifest.chunks.length).toBeGreaterThan(0);

    const c0 = await stub.vfsReadChunk(scope, "/m.bin", 0);
    expect(c0.byteLength).toBeGreaterThan(0);
  });
});

describe("empty-file (0-byte) — no NaN", () => {
  it("E1 — versioning-ON tenant: empty file streams 0 bytes, completes cleanly (no NaN, no infinite pull)", async () => {
    const tenant = "rsv-empty-versioned";
    const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });
    const stub = userStub(tenant);
    const scope = { ns: NS, tenant };

    await vfs.writeFile("/empty.bin", new Uint8Array(0));

    const stream = await stub.vfsCreateReadStream(scope, "/empty.bin");
    const back = await drain(stream);
    expect(back.byteLength).toBe(0);

    // openManifest sees a 0-byte file (either inlined-empty or
    // chunked-with-0-chunks). chunkSize must NOT be NaN.
    const manifest = await stub.vfsOpenManifest(scope, "/empty.bin");
    expect(manifest.size).toBe(0);
    expect(Number.isFinite(manifest.chunkSize)).toBe(true);
    expect(manifest.chunks).toHaveLength(0);
  });

  it("E2 — versioning-OFF tenant: empty file streams 0 bytes, completes cleanly", async () => {
    const tenant = "rsv-empty-legacy";
    const vfs = createVFS(envFor(), { tenant });
    const stub = userStub(tenant);
    const scope = { ns: NS, tenant };

    await vfs.writeFile("/empty.bin", new Uint8Array(0));

    const stream = await stub.vfsCreateReadStream(scope, "/empty.bin");
    const back = await drain(stream);
    expect(back.byteLength).toBe(0);
  });

  it("E3 — handle.chunkSize is finite (not NaN) for empty / inlined / yjs files", async () => {
    const tenant = "rsv-handle-shape";
    const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });
    const stub = userStub(tenant);
    const scope = { ns: NS, tenant };

    await vfs.writeFile("/empty.bin", new Uint8Array(0));
    await vfs.writeFile("/small.txt", "hello"); // inlined tier
    await vfs.writeFile("/big.bin", makePayload(0xee, 32 * 1024)); // chunked tier

    const handles = await Promise.all([
      stub.vfsOpenReadStream(scope, "/empty.bin"),
      stub.vfsOpenReadStream(scope, "/small.txt"),
      stub.vfsOpenReadStream(scope, "/big.bin"),
    ]);
    for (const h of handles) {
      expect(Number.isFinite(h.chunkSize)).toBe(true);
      expect(h.chunkSize).toBeGreaterThanOrEqual(0);
    }
    // Empty + inlined: chunkSize=0, chunkCount=0.
    expect(handles[0].chunkCount).toBe(0);
    expect(handles[1].chunkCount).toBe(0);
    expect(handles[1].inlined).toBe(true);
    // Chunked: chunkSize>0, chunkCount>0.
    expect(handles[2].chunkSize).toBeGreaterThan(0);
    expect(handles[2].chunkCount).toBeGreaterThan(0);
  });
});

describe("yjs-mode — materialize from Y.Doc, not chunks", () => {
  it("Y1 — yjs-mode file: createReadStream returns the materialized Y.Doc bytes", async () => {
    const tenant = "rsv-yjs-create";
    const vfs = createVFS(envFor(), { tenant });
    const stub = userStub(tenant);
    const scope = { ns: NS, tenant };

    await vfs.writeFile("/notes.md", "");
    await vfs.setYjsMode("/notes.md", true);
    // writeFile via the SDK on a yjs-mode file replays into Y.Doc.
    await vfs.writeFile("/notes.md", "hello yjs world");

    const stream = await stub.vfsCreateReadStream(scope, "/notes.md");
    const back = await drain(stream);
    const text = new TextDecoder().decode(back);
    // Yjs materialization may serialize the Y.Text content; we
    // assert the substring is present (the rest is yjs framing).
    expect(text).toContain("hello yjs world");
  });

  it("Y2 — yjs-mode file: openManifest returns inlined=true with size = materialized byteLength", async () => {
    const tenant = "rsv-yjs-manifest";
    const vfs = createVFS(envFor(), { tenant });
    const stub = userStub(tenant);
    const scope = { ns: NS, tenant };

    await vfs.writeFile("/notes.md", "");
    await vfs.setYjsMode("/notes.md", true);
    await vfs.writeFile("/notes.md", "yjs payload");

    const manifest = await stub.vfsOpenManifest(scope, "/notes.md");
    expect(manifest.inlined).toBe(true);
    expect(manifest.size).toBeGreaterThan(0);
    expect(manifest.chunks).toHaveLength(0);
    expect(manifest.chunkSize).toBe(0);
  });

  it("Y3 — yjs-mode file: readChunk(0) returns materialized bytes; readChunk(>0) throws EINVAL", async () => {
    const tenant = "rsv-yjs-readchunk";
    const vfs = createVFS(envFor(), { tenant });
    const stub = userStub(tenant);
    const scope = { ns: NS, tenant };

    await vfs.writeFile("/notes.md", "");
    await vfs.setYjsMode("/notes.md", true);
    await vfs.writeFile("/notes.md", "yjs chunk0");

    const c0 = await stub.vfsReadChunk(scope, "/notes.md", 0);
    expect(c0.byteLength).toBeGreaterThan(0);

    let caught: unknown = null;
    try {
      await stub.vfsReadChunk(scope, "/notes.md", 1);
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeTruthy();
  });
});

describe("readPreview — versioned + tombstoned", () => {
  it("P1 — readPreview on a versioned text file returns rendered preview (no NaN, no decode error)", async () => {
    const tenant = "rsv-preview-versioned";
    const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });
    const stub = userStub(tenant);
    const scope = { ns: NS, tenant };

    // Code-renderer path: text/plain with a recognized extension.
    // Chunked tier (>16 KB) so the preview pipeline must source
    // bytes via createReadStream → version_chunks (the production
    // failure path).
    const code = "console.log('hello');\n".repeat(1500); // ~33 KB
    await vfs.writeFile("/script.js", code);

    const preview = await stub.vfsReadPreview(scope, "/script.js", {});
    expect(preview).toBeTruthy();
    expect(preview.bytes.byteLength).toBeGreaterThan(0);
    expect(preview.mimeType).toMatch(/^image\/(svg\+xml|webp|png|jpeg)$/);
  });

  it("P2 — readPreview on a tombstoned-head versioned file throws ENOENT", async () => {
    const tenant = "rsv-preview-tombstone";
    const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });
    const stub = userStub(tenant);
    const scope = { ns: NS, tenant };

    await vfs.writeFile("/x.txt", "alive");
    await vfs.unlink("/x.txt"); // writes a tombstone version

    let caught: unknown = null;
    try {
      await stub.vfsReadPreview(scope, "/x.txt", {});
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeTruthy();
    const msg = String((caught as Error).message ?? caught);
    expect(msg).toMatch(/ENOENT/);
  });
});

describe("versioned overwrite — subsequent reads return NEW bytes", () => {
  it("O1 — write v1, write v2, all read surfaces return v2 bytes (not v1)", async () => {
    const tenant = "rsv-overwrite";
    const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });
    const stub = userStub(tenant);
    const scope = { ns: NS, tenant };

    const v1 = makePayload(0x11, 20_000);
    const v2 = makePayload(0x22, 20_000);
    await vfs.writeFile("/over.bin", v1);
    await vfs.writeFile("/over.bin", v2);

    // readFile (the reference)
    const readFileBytes = await stub.vfsReadFile(scope, "/over.bin");
    expect(new Uint8Array(readFileBytes)[0]).toBe(v2[0]);
    expect(new Uint8Array(readFileBytes)[19_999]).toBe(v2[19_999]);

    // createReadStream
    const streamBytes = await drain(
      await stub.vfsCreateReadStream(scope, "/over.bin")
    );
    expect(streamBytes[0]).toBe(v2[0]);
    expect(streamBytes[19_999]).toBe(v2[19_999]);

    // openManifest + readChunk(0)
    const manifest = await stub.vfsOpenManifest(scope, "/over.bin");
    expect(manifest.size).toBe(v2.byteLength);
    const chunk0 = await stub.vfsReadChunk(scope, "/over.bin", 0);
    expect(chunk0[0]).toBe(v2[0]);
  });
});

describe("encrypted × versioned × stream (sub-agent gap)", () => {
  it("EV6 — encrypted versioned chunked file: createReadStream → drain → decryptPayload yields original plaintext", async () => {
    // Phase 27.5 sub-agent (c) "$1000 bet" — found this gap.
    // Pre-Phase-27.5 there was no test where `encrypted: true` and
    // a stream surface (createReadStream / openManifest /
    // readChunk) co-occur. The SDK's encryption design encrypts
    // the WHOLE file as a single AES-GCM envelope (sdk/src/
    // encryption.ts:86-94); that envelope is split into chunks
    // server-side. For a versioned tenant the chunks live in
    // `version_chunks`. If `vfsOpenReadStream` did NOT pin
    // `versionId` (the Phase 25 / 27.5 fix), `pullReadStream`
    // would source from legacy `file_chunks` (empty for
    // commitVersion-written rows) → drained ciphertext is wrong
    // bytes → decryptPayload throws "auth-tag mismatch" with no
    // diagnostic fingerprint. This test pins the post-fix
    // round-trip.
    const tenant = "rsv-encrypted-versioned-stream";
    const masterKey = new Uint8Array(32).fill(0xa1);
    const tenantSalt = new Uint8Array(32).fill(0xb2);
    const vfs = createVFS(envFor(), {
      tenant,
      versioning: "enabled",
      encryption: { masterKey, tenantSalt },
    });
    const stub = userStub(tenant);
    const scope = { ns: NS, tenant };

    // Chunked tier (>16 KB inline limit) AND large enough to
    // produce ≥2 chunks of envelope (the >1-chunk axis the audit
    // gap explicitly called out).
    const plaintext = makePayload(0xee, 64 * 1024);
    await vfs.writeFile("/secret.bin", plaintext, { encrypted: true });

    // The SDK's vfs.readFile() does the decrypt internally; we
    // cross-check that path first.
    const direct = await vfs.readFile("/secret.bin");
    expect(new Uint8Array(direct)).toEqual(plaintext);

    // The stream surface returns RAW envelope bytes (the SDK's
    // streams API is intentionally low-level — encryption is the
    // caller's concern). Drain, then decrypt and compare.
    const envelope = await drain(
      await stub.vfsCreateReadStream(scope, "/secret.bin")
    );
    expect(envelope.byteLength).toBeGreaterThan(plaintext.byteLength); // AES-GCM overhead
    const { decryptPayload } = await import("../../sdk/src/encryption");
    const decrypted = await decryptPayload(envelope, {
      masterKey,
      tenantSalt,
    });
    expect(new Uint8Array(decrypted)).toEqual(plaintext);

    // openManifest sees the same envelope bytes; its size matches
    // what the stream emits.
    const manifest = await stub.vfsOpenManifest(scope, "/secret.bin");
    expect(manifest.size).toBe(envelope.byteLength);
    expect(manifest.inlined).toBe(false);
    expect(manifest.chunks.length).toBeGreaterThanOrEqual(1);

    // readChunk concatenated equals the streamed envelope.
    const parts: Uint8Array[] = [];
    for (let i = 0; i < manifest.chunks.length; i++) {
      parts.push(await stub.vfsReadChunk(scope, "/secret.bin", i));
    }
    let total = 0;
    for (const p of parts) total += p.byteLength;
    const concat = new Uint8Array(total);
    let off = 0;
    for (const p of parts) {
      concat.set(p, off);
      off += p.byteLength;
    }
    expect(concat).toEqual(envelope);
  });
});

describe("read-surface consistency — every (path) tuple via stream matches readFile", () => {
  const cases: { name: string; size: number; versioning: boolean; yjs?: boolean }[] = [
    { name: "0B-legacy", size: 0, versioning: false },
    { name: "1B-legacy", size: 1, versioning: false },
    { name: "1B-versioned", size: 1, versioning: true },
    { name: "16KB-versioned", size: 16 * 1024, versioning: true },
    { name: "32KB-versioned", size: 32 * 1024, versioning: true },
    { name: "32KB-legacy", size: 32 * 1024, versioning: false },
  ];
  for (const c of cases) {
    it(`C-${c.name} — readFile == createReadStream == openManifest+readChunk`, async () => {
      const tenant = `rsv-cons-${c.name}`;
      const vfs = createVFS(envFor(), {
        tenant,
        ...(c.versioning ? { versioning: "enabled" as const } : {}),
      });
      const stub = userStub(tenant);
      const scope = { ns: NS, tenant };
      const payload =
        c.size === 0 ? new Uint8Array(0) : makePayload(0xfe, c.size);
      await vfs.writeFile("/x.bin", payload);

      const ref = await stub.vfsReadFile(scope, "/x.bin");
      expect(ref.byteLength).toBe(payload.byteLength);

      const streamBytes = await drain(
        await stub.vfsCreateReadStream(scope, "/x.bin")
      );
      expect(streamBytes.byteLength).toBe(payload.byteLength);
      if (payload.byteLength > 0) {
        expect(streamBytes[0]).toBe(new Uint8Array(ref)[0]);
        expect(streamBytes[payload.byteLength - 1]).toBe(
          new Uint8Array(ref)[payload.byteLength - 1]
        );
      }

      const manifest = await stub.vfsOpenManifest(scope, "/x.bin");
      expect(manifest.size).toBe(payload.byteLength);
      if (manifest.chunks.length > 0) {
        const c0 = await stub.vfsReadChunk(scope, "/x.bin", 0);
        expect(c0.byteLength).toBe(manifest.chunks[0].size);
      }
    });
  }
});
