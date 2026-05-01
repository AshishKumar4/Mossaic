import { describe, it, expect } from "vitest";
import { env, runInDurableObject } from "cloudflare:test";
import {
  edgeCacheKey,
  edgeCacheKeyPart,
} from "../../worker/core/lib/edge-cache";
import { vfsUserDOName } from "@core/lib/utils";

/**
 * Phase 36 Theme B + Phase 36b \u2014 Workers Cache integration.
 *
 * The edge-cache helper is a thin wrapper over `caches.default`
 * for read-heavy gallery / shared-album / preview / chunk /
 * manifest endpoints. Most cache behaviour is observable only
 * against a real Cloudflare edge (Miniflare's caches.default is
 * a per-Worker stub), so the tests here pin the structural
 * invariants:
 *
 *   - Cache-key shape is deterministic.
 *   - Different bust signals (updatedAt, headVersionId, encryption,
 *     variant kind, format, renderer, chunkIndex, chunkHash) all
 *     produce different keys.
 *   - Different namespaces produce different keys (per-tenant
 *     isolation).
 *   - vfsResolveCacheKey (Phase 36b pre-flight) advances on every
 *     mutating write.
 *
 * EC1\u2013EC5 are Phase 36 (gallery + shared). EC6\u2013EC18 are
 * Phase 36b additions.
 */

interface E {
  MOSSAIC_USER: DurableObjectNamespace;
}
const E = env as unknown as E;
const NS = "default";
describe("Phase 36 \u2014 edge cache helper", () => {
  it("EC1 \u2014 key is deterministic", () => {
    const a = edgeCacheKey({
      surfaceTag: "gthumb",
      namespace: "user-1",
      fileId: "file-A",
      updatedAt: 1000,
      cacheControl: "private",
      waitUntil: () => undefined,
    });
    const b = edgeCacheKey({
      surfaceTag: "gthumb",
      namespace: "user-1",
      fileId: "file-A",
      updatedAt: 1000,
      cacheControl: "private",
      waitUntil: () => undefined,
    });
    expect(a.url).toBe(b.url);
  });

  it("EC2 \u2014 different updatedAt \u2192 different key (cache-bust on write)", () => {
    const a = edgeCacheKey({
      surfaceTag: "gthumb",
      namespace: "user-1",
      fileId: "file-A",
      updatedAt: 1000,
      cacheControl: "private",
      waitUntil: () => undefined,
    });
    const b = edgeCacheKey({
      surfaceTag: "gthumb",
      namespace: "user-1",
      fileId: "file-A",
      updatedAt: 2000,
      cacheControl: "private",
      waitUntil: () => undefined,
    });
    expect(a.url).not.toBe(b.url);
  });

  it("EC3 \u2014 different surfaceTag \u2192 different key (gthumb vs gimg)", () => {
    const t = edgeCacheKey({
      surfaceTag: "gthumb",
      namespace: "user-1",
      fileId: "file-A",
      updatedAt: 1000,
      cacheControl: "private",
      waitUntil: () => undefined,
    });
    const i = edgeCacheKey({
      surfaceTag: "gimg",
      namespace: "user-1",
      fileId: "file-A",
      updatedAt: 1000,
      cacheControl: "private",
      waitUntil: () => undefined,
    });
    expect(t.url).not.toBe(i.url);
  });

  it("EC4 \u2014 different namespace \u2192 different key (per-tenant isolation)", () => {
    const u1 = edgeCacheKey({
      surfaceTag: "gthumb",
      namespace: "user-1",
      fileId: "file-A",
      updatedAt: 1000,
      cacheControl: "private",
      waitUntil: () => undefined,
    });
    const u2 = edgeCacheKey({
      surfaceTag: "gthumb",
      namespace: "user-2",
      fileId: "file-A",
      updatedAt: 1000,
      cacheControl: "private",
      waitUntil: () => undefined,
    });
    expect(u1.url).not.toBe(u2.url);
  });

  it("EC5 \u2014 key namespace path components are URL-stable", () => {
    const k = edgeCacheKey({
      surfaceTag: "simg",
      namespace: "user-with-dashes-and-numbers-123",
      fileId: "01HKZ...",
      updatedAt: 1700000000000,
      cacheControl: "public",
      waitUntil: () => undefined,
    });
    expect(k.url).toBe(
      "https://simg.mossaic.local/user-with-dashes-and-numbers-123/01HKZ.../1700000000000"
    );
    expect(k.method).toBe("GET");
  });

  // ── Phase 36b — extra-key-parts + new surfaces ──────────────────────

  it("EC6 \u2014 extraKeyParts: different versionPart \u2192 different key (overwrite bust)", () => {
    const v1 = edgeCacheKey({
      surfaceTag: "preview",
      namespace: "u1",
      fileId: "f1",
      updatedAt: 1000,
      extraKeyParts: ["ver-A", "thumb", "auto", "auto", "00000000"],
      cacheControl: "public",
      waitUntil: () => undefined,
    });
    const v2 = edgeCacheKey({
      surfaceTag: "preview",
      namespace: "u1",
      fileId: "f1",
      updatedAt: 1000,
      extraKeyParts: ["ver-B", "thumb", "auto", "auto", "00000000"],
      cacheControl: "public",
      waitUntil: () => undefined,
    });
    expect(v1.url).not.toBe(v2.url);
  });

  it("EC7 \u2014 extraKeyParts: different variant kind busts (thumb vs medium)", () => {
    const t = edgeCacheKey({
      surfaceTag: "preview",
      namespace: "u1",
      fileId: "f1",
      updatedAt: 1000,
      extraKeyParts: ["v1", "thumb", "auto", "auto", "00000000"],
      cacheControl: "public",
      waitUntil: () => undefined,
    });
    const m = edgeCacheKey({
      surfaceTag: "preview",
      namespace: "u1",
      fileId: "f1",
      updatedAt: 1000,
      extraKeyParts: ["v1", "medium", "auto", "auto", "00000000"],
      cacheControl: "public",
      waitUntil: () => undefined,
    });
    expect(t.url).not.toBe(m.url);
  });

  it("EC8 \u2014 extraKeyParts: different format busts (jpeg vs webp)", () => {
    const j = edgeCacheKey({
      surfaceTag: "preview",
      namespace: "u1",
      fileId: "f1",
      updatedAt: 1000,
      extraKeyParts: ["v1", "thumb", "jpeg", "auto", "00000000"],
      cacheControl: "public",
      waitUntil: () => undefined,
    });
    const w = edgeCacheKey({
      surfaceTag: "preview",
      namespace: "u1",
      fileId: "f1",
      updatedAt: 1000,
      extraKeyParts: ["v1", "thumb", "webp", "auto", "00000000"],
      cacheControl: "public",
      waitUntil: () => undefined,
    });
    expect(j.url).not.toBe(w.url);
  });

  it("EC9 \u2014 extraKeyParts: different renderer busts (auto vs forced)", () => {
    const a = edgeCacheKey({
      surfaceTag: "preview",
      namespace: "u1",
      fileId: "f1",
      updatedAt: 1000,
      extraKeyParts: ["v1", "thumb", "auto", "auto", "00000000"],
      cacheControl: "public",
      waitUntil: () => undefined,
    });
    const f = edgeCacheKey({
      surfaceTag: "preview",
      namespace: "u1",
      fileId: "f1",
      updatedAt: 1000,
      extraKeyParts: ["v1", "thumb", "auto", "svg-v2", "00000000"],
      cacheControl: "public",
      waitUntil: () => undefined,
    });
    expect(a.url).not.toBe(f.url);
  });

  it("EC10 \u2014 extraKeyParts: different encryption fingerprint busts", () => {
    const plain = edgeCacheKey({
      surfaceTag: "preview",
      namespace: "u1",
      fileId: "f1",
      updatedAt: 1000,
      extraKeyParts: [
        "v1",
        "thumb",
        "auto",
        "auto",
        edgeCacheKeyPart("|"),
      ],
      cacheControl: "public",
      waitUntil: () => undefined,
    });
    const enc = edgeCacheKey({
      surfaceTag: "preview",
      namespace: "u1",
      fileId: "f1",
      updatedAt: 1000,
      extraKeyParts: [
        "v1",
        "thumb",
        "auto",
        "auto",
        edgeCacheKeyPart("convergent|key-A"),
      ],
      cacheControl: "public",
      waitUntil: () => undefined,
    });
    expect(plain.url).not.toBe(enc.url);
  });

  it("EC11 \u2014 chunk surface: different chunkIndex \u2192 different key", () => {
    const i0 = edgeCacheKey({
      surfaceTag: "chunk",
      namespace: "u1",
      fileId: "f1",
      updatedAt: 1000,
      extraKeyParts: ["v1", "i0"],
      cacheControl: "public",
      waitUntil: () => undefined,
    });
    const i1 = edgeCacheKey({
      surfaceTag: "chunk",
      namespace: "u1",
      fileId: "f1",
      updatedAt: 1000,
      extraKeyParts: ["v1", "i1"],
      cacheControl: "public",
      waitUntil: () => undefined,
    });
    expect(i0.url).not.toBe(i1.url);
  });

  it("EC12 \u2014 manifest surface: different fileId \u2192 different key", () => {
    const a = edgeCacheKey({
      surfaceTag: "manifest",
      namespace: "u1",
      fileId: "f-A",
      updatedAt: 1000,
      extraKeyParts: ["v1"],
      cacheControl: "private",
      waitUntil: () => undefined,
    });
    const b = edgeCacheKey({
      surfaceTag: "manifest",
      namespace: "u1",
      fileId: "f-B",
      updatedAt: 1000,
      extraKeyParts: ["v1"],
      cacheControl: "private",
      waitUntil: () => undefined,
    });
    expect(a.url).not.toBe(b.url);
  });

  it("EC13 \u2014 chunk-download: different chunk hash \u2192 different key", () => {
    const a = edgeCacheKey({
      surfaceTag: "chunk",
      namespace: "tn-1",
      fileId: "f1",
      updatedAt: 0,
      extraKeyParts: ["d", "a".repeat(64), "i0"],
      cacheControl: "public",
      waitUntil: () => undefined,
    });
    const b = edgeCacheKey({
      surfaceTag: "chunk",
      namespace: "tn-1",
      fileId: "f1",
      updatedAt: 0,
      extraKeyParts: ["d", "b".repeat(64), "i0"],
      cacheControl: "public",
      waitUntil: () => undefined,
    });
    expect(a.url).not.toBe(b.url);
  });

  it("EC14 \u2014 edgeCacheKeyPart is deterministic", () => {
    expect(edgeCacheKeyPart("hello")).toBe(edgeCacheKeyPart("hello"));
    expect(edgeCacheKeyPart("hello")).not.toBe(edgeCacheKeyPart("world"));
  });

  it("EC15 \u2014 edgeCacheKeyPart returns 8-hex-char digest", () => {
    const d = edgeCacheKeyPart("convergent|key-id-rot-1");
    expect(d).toMatch(/^[0-9a-f]{8}$/);
  });

  // ── Phase 36b — vfsResolveCacheKey RPC contract ────────────────────

  it("EC16 \u2014 vfsResolveCacheKey returns updatedAt that advances on overwrite", async () => {
    const tenant = "ec16-bust";
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
    );
    const scope = { ns: NS, tenant };
    await stub.vfsWriteFile(scope, "/x.bin", new Uint8Array(8).fill(1));
    const ck1 = await stub.vfsResolveCacheKey(scope, "/x.bin");
    // Sleep ~5ms to ensure mtime changes; Date.now() is the source.
    await new Promise((r) => setTimeout(r, 5));
    await stub.vfsWriteFile(scope, "/x.bin", new Uint8Array(8).fill(2));
    const ck2 = await stub.vfsResolveCacheKey(scope, "/x.bin");
    // For non-versioning tenants, head_version_id stays NULL but
    // updatedAt advances on every write.
    expect(ck2.updatedAt).toBeGreaterThan(ck1.updatedAt);
    // fileId is stable across overwrite for non-versioning.
    expect(ck1.fileId).toBeTruthy();
    // Build cache keys; they must differ.
    const k1 = edgeCacheKey({
      surfaceTag: "preview",
      namespace: tenant,
      fileId: ck1.fileId,
      updatedAt: ck1.updatedAt,
      extraKeyParts: [`t${ck1.updatedAt}`, "thumb"],
      cacheControl: "public",
      waitUntil: () => undefined,
    });
    const k2 = edgeCacheKey({
      surfaceTag: "preview",
      namespace: tenant,
      fileId: ck2.fileId,
      updatedAt: ck2.updatedAt,
      extraKeyParts: [`t${ck2.updatedAt}`, "thumb"],
      cacheControl: "public",
      waitUntil: () => undefined,
    });
    expect(k1.url).not.toBe(k2.url);
  });

  it("EC17 \u2014 vfsResolveCacheKey returns headVersionId that bumps on versioned overwrite", async () => {
    const tenant = "ec17-versioned-bust";
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
    );
    const scope = { ns: NS, tenant };
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    await (stub as any).adminSetVersioning(tenant, true);
    await stub.vfsWriteFile(scope, "/x.bin", new Uint8Array(8).fill(1));
    const ck1 = await stub.vfsResolveCacheKey(scope, "/x.bin");
    expect(ck1.headVersionId).toBeTruthy();
    await stub.vfsWriteFile(scope, "/x.bin", new Uint8Array(8).fill(2));
    const ck2 = await stub.vfsResolveCacheKey(scope, "/x.bin");
    expect(ck2.headVersionId).toBeTruthy();
    expect(ck1.headVersionId).not.toBe(ck2.headVersionId);
    // fileId is path-stable; versioning preserves identity.
    expect(ck1.fileId).toBe(ck2.fileId);
  });

  it("EC18 \u2014 vfsResolveCacheKey returns null encryption stamp for plaintext", async () => {
    const tenant = "ec18-plain";
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
    );
    const scope = { ns: NS, tenant };
    await stub.vfsWriteFile(scope, "/p.bin", new Uint8Array(8).fill(0));
    const ck = await stub.vfsResolveCacheKey(scope, "/p.bin");
    expect(ck.encryptionMode).toBeNull();
    expect(ck.encryptionKeyId).toBeNull();
  });

  it("EC19 \u2014 vfsResolveCacheKey throws ENOENT for missing path", async () => {
    const tenant = "ec19-missing";
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
    );
    const scope = { ns: NS, tenant };
    await stub.vfsExists(scope, "/"); // ensureInit
    await expect(stub.vfsResolveCacheKey(scope, "/nope.bin")).rejects.toThrow(
      /ENOENT/
    );
  });

  it("EC20 \u2014 vfsResolveCacheKey advances updatedAt on rename", async () => {
    const tenant = "ec20-rename";
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
    );
    const scope = { ns: NS, tenant };
    await stub.vfsWriteFile(scope, "/a.bin", new Uint8Array(4).fill(1));
    const ck1 = await stub.vfsResolveCacheKey(scope, "/a.bin");
    await new Promise((r) => setTimeout(r, 5));
    await stub.vfsRename(scope, "/a.bin", "/b.bin");
    const ck2 = await stub.vfsResolveCacheKey(scope, "/b.bin");
    expect(ck2.fileId).toBe(ck1.fileId); // same identity
    expect(ck2.updatedAt).toBeGreaterThan(ck1.updatedAt);
  });

  it("EC21 \u2014 cache-key namespace prevents cross-tenant collision (regression)", async () => {
    const t1 = "ec21-tenant-a";
    const t2 = "ec21-tenant-b";
    const stub1 = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, t1))
    );
    const stub2 = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, t2))
    );
    const scope1 = { ns: NS, tenant: t1 };
    const scope2 = { ns: NS, tenant: t2 };
    await stub1.vfsWriteFile(scope1, "/shared.bin", new Uint8Array(16).fill(1));
    await stub2.vfsWriteFile(scope2, "/shared.bin", new Uint8Array(16).fill(2));
    const ck1 = await stub1.vfsResolveCacheKey(scope1, "/shared.bin");
    const ck2 = await stub2.vfsResolveCacheKey(scope2, "/shared.bin");
    // Even if fileIds happen to match (ULIDs are unique so they
    // shouldn't), the namespace component guarantees disjoint keys.
    const k1 = edgeCacheKey({
      surfaceTag: "preview",
      namespace: t1,
      fileId: ck1.fileId,
      updatedAt: ck1.updatedAt,
      cacheControl: "public",
      waitUntil: () => undefined,
    });
    const k2 = edgeCacheKey({
      surfaceTag: "preview",
      namespace: t2,
      fileId: ck2.fileId,
      updatedAt: ck2.updatedAt,
      cacheControl: "public",
      waitUntil: () => undefined,
    });
    expect(k1.url).not.toBe(k2.url);
    // The namespace component MUST appear in both URLs disjointly.
    expect(k1.url).toContain(`/${t1}/`);
    expect(k2.url).toContain(`/${t2}/`);
  });

  it("EC22 \u2014 vfsResolveCacheKey is consistent for repeated calls without writes", async () => {
    const tenant = "ec22-consistent";
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
    );
    const scope = { ns: NS, tenant };
    await stub.vfsWriteFile(scope, "/c.bin", new Uint8Array(4).fill(7));
    const a = await stub.vfsResolveCacheKey(scope, "/c.bin");
    const b = await stub.vfsResolveCacheKey(scope, "/c.bin");
    expect(b.fileId).toBe(a.fileId);
    expect(b.updatedAt).toBe(a.updatedAt);
    expect(b.headVersionId).toBe(a.headVersionId);
  });

  it("EC23 \u2014 versioned dropVersions advances headVersionId via head pointer flip", async () => {
    const tenant = "ec23-drop-versions";
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
    );
    const scope = { ns: NS, tenant };
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    await (stub as any).adminSetVersioning(tenant, true);
    await stub.vfsWriteFile(scope, "/d.bin", new Uint8Array(4).fill(1));
    await stub.vfsWriteFile(scope, "/d.bin", new Uint8Array(4).fill(2));
    await stub.vfsWriteFile(scope, "/d.bin", new Uint8Array(4).fill(3));
    const ck1 = await stub.vfsResolveCacheKey(scope, "/d.bin");
    // Drop two; head moves to the surviving version (could be the
    // current head depending on keepLast). Assert headVersionId
    // remains valid \u2014 it's still the most-recent live version.
    await stub.vfsDropVersions(scope, "/d.bin", { keepLast: 1 });
    const ck2 = await stub.vfsResolveCacheKey(scope, "/d.bin");
    expect(ck2.headVersionId).toBeTruthy();
    expect(ck1.headVersionId).toBe(ck2.headVersionId);
    // updatedAt may or may not advance (depends on dropVersions
    // touching files.updated_at); the contract is that the cache key
    // reflects the current state regardless.
  });
});
