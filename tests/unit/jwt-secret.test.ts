import { describe, it, expect } from "vitest";
import {
  signJWT,
  signVFSToken,
  verifyJWT,
  verifyVFSToken,
  VFSConfigError,
} from "@core/lib/auth";
import type { EnvCore as Env } from "@shared/types";

/**
 * C1 (audit-report.md) — JWT_SECRET fallback removal.
 *
 * The module no longer carries a hard-coded development fallback string.
 * Any sign- or verify-path called with `env.JWT_SECRET` missing/empty
 * MUST throw `VFSConfigError` rather than silently signing with a
 * known-public string. Testing the throw is the regression gate.
 *
 * NOTE: this test does not need the workers pool — it exercises pure
 * functions of the env shape. It runs in the workers pool anyway because
 * the rest of the suite does, but it asserts behavior that is provider-
 * independent.
 */

function makeEnv(secret: string | undefined): Env {
  // Casting through unknown is the cheapest way to assemble a stub Env
  // that only fills the JWT_SECRET slot. We never touch DO bindings here.
  return { JWT_SECRET: secret } as unknown as Env;
}

describe("JWT_SECRET handling (C1)", () => {
  it("signJWT throws VFSConfigError when JWT_SECRET is undefined", async () => {
    const env = makeEnv(undefined);
    await expect(
      signJWT(env, { userId: "u", email: "e@example.com" })
    ).rejects.toBeInstanceOf(VFSConfigError);
  });

  it("signJWT throws VFSConfigError when JWT_SECRET is empty string", async () => {
    const env = makeEnv("");
    await expect(
      signJWT(env, { userId: "u", email: "e@example.com" })
    ).rejects.toBeInstanceOf(VFSConfigError);
  });

  it("signVFSToken throws VFSConfigError when JWT_SECRET is undefined", async () => {
    const env = makeEnv(undefined);
    await expect(
      signVFSToken(env, { ns: "default", tenant: "t" })
    ).rejects.toBeInstanceOf(VFSConfigError);
  });

  it("verifyJWT propagates VFSConfigError when JWT_SECRET is undefined", async () => {
    const env = makeEnv(undefined);
    await expect(verifyJWT(env, "anything")).rejects.toBeInstanceOf(
      VFSConfigError
    );
  });

  it("verifyVFSToken propagates VFSConfigError when JWT_SECRET is undefined", async () => {
    const env = makeEnv(undefined);
    await expect(verifyVFSToken(env, "anything")).rejects.toBeInstanceOf(
      VFSConfigError
    );
  });

  it("VFSConfigError carries discriminator code", async () => {
    const env = makeEnv(undefined);
    try {
      await signJWT(env, { userId: "u", email: "e@example.com" });
      expect.fail("should have thrown");
    } catch (err) {
      expect(err).toBeInstanceOf(VFSConfigError);
      expect((err as VFSConfigError).code).toBe("VFS_CONFIG_ERROR");
      expect((err as VFSConfigError).name).toBe("VFSConfigError");
      expect((err as VFSConfigError).message).toMatch(/JWT_SECRET/);
    }
  });

  it("with a configured secret signing succeeds (positive control)", async () => {
    const env = makeEnv("a-secret-of-some-length");
    const token = await signJWT(env, { userId: "u", email: "e@example.com" });
    expect(typeof token).toBe("string");
    expect(token.length).toBeGreaterThan(0);
    const verified = await verifyJWT(env, token);
    expect(verified).toEqual({ userId: "u", email: "e@example.com" });
  });

  /**
   * Regression: jose's `setExpirationTime(input)` interprets numeric
   * input as seconds-since-epoch. Earlier code passed milliseconds so
   * tokens carried `exp` ~year 57000 (effectively never-expiring).
   * Decode the JWT body and assert `exp` is in seconds and within
   * sane bounds of "now + ~24h" (default JWT_EXPIRATION_MS).
   */
  it("signJWT emits `exp` as seconds-since-epoch (not milliseconds)", async () => {
    const env = makeEnv("a-secret-of-some-length");
    const beforeS = Math.floor(Date.now() / 1000);
    const token = await signJWT(env, { userId: "u", email: "e@example.com" });
    // Decode the body without verifying — we just want the exp claim.
    const body = JSON.parse(
      atob(token.split(".")[1]!.replace(/-/g, "+").replace(/_/g, "/"))
    ) as { exp?: number; iat?: number };
    expect(typeof body.exp).toBe("number");
    // Sanity: exp is within ~25 hours of now (default JWT_EXPIRATION_MS = 30d
    // per env at time of writing — but at minimum > 0 and < year 2200).
    expect(body.exp!).toBeGreaterThan(beforeS);
    expect(body.exp!).toBeLessThan(beforeS + 366 * 24 * 60 * 60);
    expect(body.iat!).toBeGreaterThanOrEqual(beforeS - 1);
    expect(body.iat!).toBeLessThan(body.exp!);
  });

  it("signVFSToken emits `exp` as seconds-since-epoch", async () => {
    const env = makeEnv("a-secret-of-some-length");
    const beforeS = Math.floor(Date.now() / 1000);
    const ttlMs = 15 * 60 * 1000;
    const token = await signVFSToken(
      env,
      { ns: "default", tenant: "alice" },
      ttlMs
    );
    const body = JSON.parse(
      atob(token.split(".")[1]!.replace(/-/g, "+").replace(/_/g, "/"))
    ) as { exp?: number; iat?: number };
    expect(typeof body.exp).toBe("number");
    // 15-min TTL ⇒ exp ≈ iat + 900s. Allow ±5s slop.
    expect(body.exp!).toBeGreaterThanOrEqual(beforeS + 895);
    expect(body.exp!).toBeLessThanOrEqual(beforeS + 905);
  });

  it("verifyVFSToken accepts the seconds-encoded exp from signVFSToken", async () => {
    const env = makeEnv("a-secret-of-some-length");
    const token = await signVFSToken(env, { ns: "default", tenant: "alice" });
    const verified = await verifyVFSToken(env, token);
    expect(verified).not.toBeNull();
    expect(verified!.tn).toBe("alice");
    expect(verified!.ns).toBe("default");
  });
});
