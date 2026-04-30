import { describe, it, expect } from "vitest";
import { jwtVerify, decodeProtectedHeader } from "jose";
import { mintToken } from "../../src/jwt.js";

describe("mintToken", () => {
  const SECRET = "test-secret-for-vitest-only-32-chars-min!";

  it("produces an HS256 JWT verifiable with the same secret", async () => {
    const tok = await mintToken({
      secret: SECRET,
      ns: "default",
      tenant: "team-acme",
    });
    expect(tok.split(".").length).toBe(3);
    const header = decodeProtectedHeader(tok);
    expect(header.alg).toBe("HS256");
    const { payload } = await jwtVerify(tok, new TextEncoder().encode(SECRET));
    expect(payload.scope).toBe("vfs");
    expect(payload.ns).toBe("default");
    expect(payload.tn).toBe("team-acme");
    expect(payload.sub).toBeUndefined();
    expect(payload.iat).toBeTypeOf("number");
    expect(payload.exp).toBeTypeOf("number");
  });

  it("includes sub when provided", async () => {
    const tok = await mintToken({
      secret: SECRET,
      ns: "prod",
      tenant: "team-acme",
      sub: "alice",
    });
    const { payload } = await jwtVerify(tok, new TextEncoder().encode(SECRET));
    expect(payload.sub).toBe("alice");
  });

  it("rejects empty secret with a clear error", async () => {
    await expect(
      mintToken({ secret: "", ns: "default", tenant: "t" }),
    ).rejects.toThrow(/missing JWT secret/i);
  });

  it("rejects empty ns / tenant", async () => {
    await expect(
      mintToken({ secret: SECRET, ns: "", tenant: "t" }),
    ).rejects.toThrow(/ns must be a non-empty string/);
    await expect(
      mintToken({ secret: SECRET, ns: "x", tenant: "" }),
    ).rejects.toThrow(/tenant must be a non-empty string/);
  });

  it("respects ttlMs and emits seconds-since-epoch exp (matches signVFSToken)", async () => {
    // RFC 7519 `exp` is seconds-since-epoch. jose's
    // `setExpirationTime(numeric)` stores the value verbatim, so
    // the caller MUST pass seconds. The Mossaic worker
    // (`worker/core/lib/auth.ts:239`) does
    // `Math.floor((Date.now() + ttlMs) / 1000)`; the CLI was
    // fixed to match. A previous version of this assertion checked
    // an ms-shaped exp ~Date.now()+ttlMs (year-57000 expiry —
    // tokens never expired); post-fix exp is ~(Date.now()+ttlMs)/1000.
    const tNowSec = Math.floor(Date.now() / 1000);
    const tok = await mintToken({
      secret: SECRET,
      ns: "default",
      tenant: "t",
      ttlMs: 5_000,
    });
    const { payload } = await jwtVerify(tok, new TextEncoder().encode(SECRET));
    expect(payload.exp).toBeTypeOf("number");
    const expSec = payload.exp as number;
    // exp should be ~ (now + 5s) in seconds. Allow ±2s for clock
    // skew / test scheduling jitter.
    const diffSec = expSec - tNowSec;
    expect(diffSec).toBeGreaterThanOrEqual(3);
    expect(diffSec).toBeLessThanOrEqual(7);
    // Sanity: must NOT be ms-shaped (which would be ~Date.now() ≈
    // 1.7e12 today; would dwarf seconds-since-epoch ≈ 1.7e9).
    expect(expSec).toBeLessThan(1e11); // ~year 5138 ceiling
  });

  it("token signed with one secret fails verification with another", async () => {
    const tok = await mintToken({
      secret: SECRET,
      ns: "default",
      tenant: "t",
    });
    await expect(
      jwtVerify(tok, new TextEncoder().encode("different-secret-value")),
    ).rejects.toThrow();
  });
});
