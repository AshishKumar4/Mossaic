/**
 * Validator for `CacheResolveResult` JSON shipped over the HTTP fallback
 * (`apps/mossaic/shared/vfs-types.ts:parseCacheResolveResult`).
 *
 * The validator is a boundary check at the place an external JSON
 * response is parsed back into a typed SDK value. Tests pin:
 *   - Valid happy-path round-trip preserves every field.
 *   - Each field's narrow allowed shape (`null | string`, finite number,
 *     etc.) rejects with a precise field path in the message so a
 *     contract drift surfaces at the boundary instead of corrupting a
 *     downstream cache key.
 *   - Top-level non-object input rejects with a descriptive type name.
 *
 * RFC-009: every external boundary must validate, not cast.
 */
import { describe, expect, it } from "vitest";
import { parseCacheResolveResult } from "../../shared/vfs-types";

describe("parseCacheResolveResult", () => {
  it("accepts a full plaintext payload", () => {
    const r = parseCacheResolveResult({
      fileId: "f-1",
      headVersionId: "v-1",
      updatedAt: 1_700_000_000_000,
      encryptionMode: null,
      encryptionKeyId: null,
    });
    expect(r).toEqual({
      fileId: "f-1",
      headVersionId: "v-1",
      updatedAt: 1_700_000_000_000,
      encryptionMode: null,
      encryptionKeyId: null,
    });
  });

  it("accepts a null headVersionId (versioning-OFF tenants)", () => {
    const r = parseCacheResolveResult({
      fileId: "f-1",
      headVersionId: null,
      updatedAt: 1,
      encryptionMode: null,
      encryptionKeyId: null,
    });
    expect(r.headVersionId).toBeNull();
  });

  it("accepts an encrypted payload with both encryption fields set", () => {
    const r = parseCacheResolveResult({
      fileId: "f-1",
      headVersionId: "v-1",
      updatedAt: 1,
      encryptionMode: "convergent",
      encryptionKeyId: "k-1",
    });
    expect(r.encryptionMode).toBe("convergent");
    expect(r.encryptionKeyId).toBe("k-1");
  });

  it("rejects non-object top-level shapes", () => {
    expect(() => parseCacheResolveResult(null)).toThrowError(/expected object, got null/);
    expect(() => parseCacheResolveResult(42)).toThrowError(/expected object, got number/);
    expect(() => parseCacheResolveResult("oops")).toThrowError(/expected object, got string/);
  });

  it("rejects missing or empty fileId", () => {
    expect(() =>
      parseCacheResolveResult({
        headVersionId: null,
        updatedAt: 1,
        encryptionMode: null,
        encryptionKeyId: null,
      }),
    ).toThrowError(/fileId/);
    expect(() =>
      parseCacheResolveResult({
        fileId: "",
        headVersionId: null,
        updatedAt: 1,
        encryptionMode: null,
        encryptionKeyId: null,
      }),
    ).toThrowError(/fileId/);
  });

  it("rejects non-finite updatedAt", () => {
    expect(() =>
      parseCacheResolveResult({
        fileId: "f-1",
        headVersionId: null,
        updatedAt: Number.NaN,
        encryptionMode: null,
        encryptionKeyId: null,
      }),
    ).toThrowError(/updatedAt/);
    expect(() =>
      parseCacheResolveResult({
        fileId: "f-1",
        headVersionId: null,
        updatedAt: "1700000000000",
        encryptionMode: null,
        encryptionKeyId: null,
      }),
    ).toThrowError(/updatedAt/);
  });

  it("rejects unexpected types on headVersionId / encryptionMode / encryptionKeyId", () => {
    const base = {
      fileId: "f-1",
      headVersionId: null,
      updatedAt: 1,
      encryptionMode: null,
      encryptionKeyId: null,
    };
    expect(() => parseCacheResolveResult({ ...base, headVersionId: 5 })).toThrowError(
      /headVersionId/,
    );
    expect(() => parseCacheResolveResult({ ...base, encryptionMode: 5 })).toThrowError(
      /encryptionMode/,
    );
    expect(() => parseCacheResolveResult({ ...base, encryptionKeyId: 5 })).toThrowError(
      /encryptionKeyId/,
    );
  });
});
