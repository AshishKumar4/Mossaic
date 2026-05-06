import { describe, it, expect } from "vitest";
import { edgeCacheKey } from "../../worker/app/lib/edge-cache";

/**
 * Phase 36 Theme B \u2014 Workers Cache integration.
 *
 * The edge-cache helper is a thin wrapper over `caches.default`
 * for read-heavy gallery / shared-album endpoints. Most cache
 * behaviour is observable only against a real Cloudflare edge
 * (Miniflare's caches.default is a per-Worker stub), so the
 * tests here pin only the structural invariants:
 *
 *   - Cache-key shape is deterministic for (surfaceTag,
 *     namespace, fileId, updatedAt).
 *   - Different updatedAt produces different keys (cache-bust on
 *     write).
 *   - Different surfaceTag produces different keys (gthumb !=
 *     gimg even for same fileId).
 *   - Different namespace produces different keys (per-user
 *     isolation).
 */
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
});
