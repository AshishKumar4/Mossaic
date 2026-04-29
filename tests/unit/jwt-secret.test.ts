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
});
