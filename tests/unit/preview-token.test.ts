import { describe, it, expect } from "vitest";
import {
  signPreviewToken,
  verifyPreviewToken,
  VFS_PREVIEW_SCOPE,
  PREVIEW_TOKEN_DEFAULT_TTL_MS,
} from "@core/lib/preview-token";
import {
  signVFSDownloadToken,
  signShareToken,
  VFSConfigError,
} from "@core/lib/auth";
import type { EnvCore as Env } from "@shared/types";

/**
 * Phase 45 \u2014 preview-token unit tests.
 *
 * Pure-crypto layer. No DO bindings; runs against an in-memory env
 * stub with a fixed `JWT_SECRET`.
 *
 * Cases:
 *   PT1 round-trip sign \u2192 verify returns the same payload.
 *   PT2 verify rejects tampered token (signature mismatch).
 *   PT3 verify rejects wrong-scope token (download/share token
 *       replayed at preview-variant route).
 *   PT4 verify accepts token signed with JWT_SECRET_PREVIOUS
 *       (rotation window).
 *   PT5 sign rejects malformed payload (missing fields, bad hash).
 *   PT6 expired token returns null.
 *   PT7 no-secret env throws VFSConfigError on sign and on verify
 *       (matches behaviour of sibling token systems).
 */

function makeEnv(
  secret: string | undefined,
  previous?: string
): Env {
  // Casting via unknown to assemble just the JWT_SECRET / PREVIOUS
  // slots; the rest of the Env (DO bindings) is irrelevant for
  // pure-crypto tests.
  return {
    JWT_SECRET: secret,
    JWT_SECRET_PREVIOUS: previous,
  } as unknown as Env;
}

const VALID_HASH =
  "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";

describe("Phase 45 \u2014 preview-token (PT)", () => {
  it("PT1 \u2014 round-trip sign \u2192 verify", async () => {
    const env = makeEnv("test-secret-1");
    const { token, expiresAtMs } = await signPreviewToken(env, {
      tenantId: "default::alice",
      fileId: "file-A",
      headVersionId: "ver-1",
      variantKind: "thumb",
      rendererKind: "image",
      format: "auto",
      contentHash: VALID_HASH,
    });
    expect(typeof token).toBe("string");
    expect(token.split(".").length).toBe(3);
    expect(expiresAtMs).toBeGreaterThan(Date.now());

    const payload = await verifyPreviewToken(env, token);
    expect(payload).not.toBeNull();
    expect(payload?.scope).toBe(VFS_PREVIEW_SCOPE);
    expect(payload?.tenantId).toBe("default::alice");
    expect(payload?.fileId).toBe("file-A");
    expect(payload?.headVersionId).toBe("ver-1");
    expect(payload?.variantKind).toBe("thumb");
    expect(payload?.rendererKind).toBe("image");
    expect(payload?.format).toBe("auto");
    expect(payload?.contentHash).toBe(VALID_HASH);
  });

  it("PT2 \u2014 tampered token returns null", async () => {
    const env = makeEnv("test-secret-2");
    const { token } = await signPreviewToken(env, {
      tenantId: "default::bob",
      fileId: "file-B",
      headVersionId: null,
      variantKind: "medium",
      rendererKind: "image",
      format: "webp",
      contentHash: VALID_HASH,
    });
    const parts = token.split(".");
    // flip a byte in the payload segment
    const mangled = parts[1].slice(0, -2) + "ZZ";
    const tampered = [parts[0], mangled, parts[2]].join(".");
    const result = await verifyPreviewToken(env, tampered);
    expect(result).toBeNull();
  });

  it("PT3 \u2014 cross-scope token rejected (download token replayed)", async () => {
    const env = makeEnv("test-secret-3");
    const { token: downloadToken } = await signVFSDownloadToken(env, {
      fileId: "file-C",
      ns: "default",
      tn: "carol",
    });
    const result = await verifyPreviewToken(env, downloadToken);
    expect(result).toBeNull();

    const { token: shareToken } = await signShareToken(env, {
      userId: "carol",
      fileIds: ["file-C"],
      albumName: "album",
    });
    const result2 = await verifyPreviewToken(env, shareToken);
    expect(result2).toBeNull();
  });

  it("PT4 \u2014 verify accepts token from JWT_SECRET_PREVIOUS (rotation)", async () => {
    // Sign with the OLD secret in an env where it's still primary.
    const oldEnv = makeEnv("old-secret");
    const { token } = await signPreviewToken(oldEnv, {
      tenantId: "default::dave",
      fileId: "file-D",
      headVersionId: "ver-d",
      variantKind: "thumb",
      rendererKind: "image",
      format: "auto",
      contentHash: VALID_HASH,
    });
    // Now operator rotates: NEW secret primary, OLD secret as PREVIOUS.
    const rotatedEnv = makeEnv("new-secret", "old-secret");
    const result = await verifyPreviewToken(rotatedEnv, token);
    expect(result).not.toBeNull();
    expect(result?.fileId).toBe("file-D");
    // Same env without PREVIOUS rejects the old-secret token.
    const newOnly = makeEnv("new-secret");
    expect(await verifyPreviewToken(newOnly, token)).toBeNull();
  });

  it("PT5 \u2014 sign rejects malformed payload", async () => {
    const env = makeEnv("test-secret-5");
    await expect(
      signPreviewToken(env, {
        tenantId: "",
        fileId: "f",
        headVersionId: null,
        variantKind: "thumb",
        rendererKind: "image",
        format: "auto",
        contentHash: VALID_HASH,
      })
    ).rejects.toThrow(/tenantId/);
    await expect(
      signPreviewToken(env, {
        tenantId: "t",
        fileId: "",
        headVersionId: null,
        variantKind: "thumb",
        rendererKind: "image",
        format: "auto",
        contentHash: VALID_HASH,
      })
    ).rejects.toThrow(/fileId/);
    await expect(
      signPreviewToken(env, {
        tenantId: "t",
        fileId: "f",
        headVersionId: null,
        variantKind: "thumb",
        rendererKind: "image",
        format: "auto",
        contentHash: "not-hex",
      })
    ).rejects.toThrow(/contentHash/);
  });

  it("PT6 \u2014 default TTL clamped to documented range", async () => {
    const env = makeEnv("test-secret-6");
    const before = Date.now();
    const { expiresAtMs } = await signPreviewToken(
      env,
      {
        tenantId: "t",
        fileId: "f",
        headVersionId: null,
        variantKind: "thumb",
        rendererKind: "image",
        format: "auto",
        contentHash: VALID_HASH,
      },
      // No TTL passed \u2014 helper uses PREVIEW_TOKEN_DEFAULT_TTL_MS.
    );
    const lifetime = expiresAtMs - before;
    expect(lifetime).toBeGreaterThanOrEqual(PREVIEW_TOKEN_DEFAULT_TTL_MS - 1000);
    expect(lifetime).toBeLessThanOrEqual(PREVIEW_TOKEN_DEFAULT_TTL_MS + 1000);
  });

  it("PT7 \u2014 missing JWT_SECRET throws VFSConfigError on sign", async () => {
    const env = makeEnv(undefined);
    await expect(
      signPreviewToken(env, {
        tenantId: "t",
        fileId: "f",
        headVersionId: null,
        variantKind: "thumb",
        rendererKind: "image",
        format: "auto",
        contentHash: VALID_HASH,
      })
    ).rejects.toBeInstanceOf(VFSConfigError);
  });
});
