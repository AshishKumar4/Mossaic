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

  it("respects ttlMs (matches the worker-side signVFSToken shape)", async () => {
    // jose's setExpirationTime(numeric) stores the value as-is (the
    // claim is meant to be seconds-since-epoch; passing ms produces
    // a "ms-shaped" exp claim). The Mossaic worker
    // (worker/core/lib/auth.ts:signVFSToken) uses the same pattern,
    // so we match it byte-for-byte. jwtVerify accepts both shapes
    // when the value is in the future. We assert the diff between
    // exp and iso-equivalent of Date.now() is in the ttl ballpark.
    const t = Date.now();
    const tok = await mintToken({
      secret: SECRET,
      ns: "default",
      tenant: "t",
      ttlMs: 5_000,
    });
    const { payload } = await jwtVerify(tok, new TextEncoder().encode(SECRET));
    // payload.exp will be approximately Date.now() + ttlMs (ms-shaped).
    expect(payload.exp).toBeTypeOf("number");
    const diff = (payload.exp as number) - t;
    expect(diff).toBeGreaterThan(4_000);
    expect(diff).toBeLessThan(7_000);
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
