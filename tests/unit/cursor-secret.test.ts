import { describe, it, expect } from "vitest";
import { env, runInDurableObject } from "cloudflare:test";
import {
  getCursorSecret,
  VFSConfigError,
} from "@core/lib/auth";
import { encodeCursor, decodeCursor } from "@core/lib/cursor";
import { vfsUserDOName } from "@core/lib/utils";

/**
 * B-1 (final-audit.md) — cursor HMAC dev-fallback removal.
 *
 * Mirror of `tests/unit/jwt-secret.test.ts` (C1). The `vfsListFiles`
 * primitive used to read `env.JWT_SECRET ?? "mossaic-cursor-dev-secret-do-not-use-in-prod"`,
 * which silently signs cursors with a public string baked into the
 * open-source repo when the operator forgets to run
 * `wrangler secret put JWT_SECRET`. The fix routes the read through
 * `getCursorSecret(env)` which throws `VFSConfigError` on
 * missing/empty — same shape as `getSecret` in auth.ts.
 *
 * Coverage:
 *   1. `getCursorSecret` throws on undefined/empty (the unit-level
 *      regression).
 *   2. `encodeCursor` / `decodeCursor` round-trip with an explicit
 *      test secret (positive control — exercises the cursor codec
 *      without relying on env, proving the codec itself is unchanged).
 *   3. `vfsListFiles` throws `VFSConfigError` when the DO's
 *      `envPublic.JWT_SECRET` is wiped — covers the call site
 *      (B-1's actual defect was at the call site, not in the codec).
 *
 * Without the fix, test (3) would silently succeed by signing with
 * the public dev string. With the fix, it throws `VFSConfigError` —
 * the test fails-loud iff the regression returns.
 */

describe("getCursorSecret (B-1 regression unit)", () => {
  it("throws VFSConfigError when JWT_SECRET is undefined", () => {
    expect(() => getCursorSecret({ JWT_SECRET: undefined })).toThrow(
      VFSConfigError
    );
  });

  it("throws VFSConfigError when JWT_SECRET is empty string", () => {
    expect(() => getCursorSecret({ JWT_SECRET: "" })).toThrow(VFSConfigError);
  });

  it("VFSConfigError carries discriminator code + name", () => {
    try {
      getCursorSecret({ JWT_SECRET: undefined });
      expect.fail("should have thrown");
    } catch (err) {
      expect(err).toBeInstanceOf(VFSConfigError);
      expect((err as VFSConfigError).code).toBe("VFS_CONFIG_ERROR");
      expect((err as VFSConfigError).name).toBe("VFSConfigError");
      expect((err as VFSConfigError).message).toMatch(/JWT_SECRET/);
      expect((err as VFSConfigError).message).toMatch(/listFiles cursor/);
    }
  });

  it("returns the configured secret string when set", () => {
    expect(getCursorSecret({ JWT_SECRET: "abc123" })).toBe("abc123");
  });
});

describe("cursor codec encode/decode (positive control)", () => {
  // The codec itself takes a `secret: string` and is unchanged by
  // B-1. Pin it with an explicit test secret so a future refactor
  // of the codec can't quietly break the cursor round-trip.
  const SECRET = "test-cursor-secret-for-vitest-only";

  it("round-trips a cursor payload with an explicit secret", async () => {
    const encoded = await encodeCursor(
      { v: 1, ob: "mtime", d: "desc", ov: 1735689600000, pid: "file-123" },
      SECRET
    );
    expect(typeof encoded).toBe("string");
    expect(encoded.length).toBeGreaterThan(0);

    const decoded = await decodeCursor(encoded, SECRET, "mtime", "desc");
    expect(decoded).toEqual({
      v: 1,
      ob: "mtime",
      d: "desc",
      ov: 1735689600000,
      pid: "file-123",
    });
  });

  it("rejects a cursor signed with a different secret (EINVAL)", async () => {
    const encoded = await encodeCursor(
      { v: 1, ob: "mtime", d: "desc", ov: 100, pid: "f" },
      SECRET
    );
    await expect(
      decodeCursor(encoded, "different-secret", "mtime", "desc")
    ).rejects.toMatchObject({ code: "EINVAL" });
  });
});

interface E {
  USER_DO: DurableObjectNamespace;
}
const E = env as unknown as E;

describe("vfsListFiles refuses cursor ops when JWT_SECRET unset (B-1)", () => {
  it("throws VFSConfigError when envPublic.JWT_SECRET is missing", async () => {
    // The test runner sets JWT_SECRET via tests/wrangler.test.jsonc;
    // wipe it on this DO instance to simulate a deploy where the
    // operator forgot to run `wrangler secret put JWT_SECRET`.
    const tenant = "b1-jwt-secret-missing";
    const stub = E.USER_DO.get(
      E.USER_DO.idFromName(vfsUserDOName("default", tenant))
    );

    // Trigger ensureInit + record scope so vfsListFiles reaches the
    // cursor-secret read path (skipping it on a never-written tenant
    // would also surface a different error and miss the regression).
    await stub.vfsExists({ ns: "default", tenant }, "/");

    const caught = await runInDurableObject(stub, async (inst) => {
      const original = inst.envPublic.JWT_SECRET;
      // Mutate envPublic directly — the DO holds a live ref to
      // env, so this simulates the missing-secret deploy state.
      // Cast through `unknown` to clear the optional field.
      (inst.envPublic as { JWT_SECRET?: string }).JWT_SECRET = undefined;
      let err: unknown = null;
      try {
        await inst.vfsListFiles({ ns: "default", tenant }, { limit: 5 });
      } catch (e) {
        err = e;
      } finally {
        // Restore so subsequent tests aren't poisoned.
        (inst.envPublic as { JWT_SECRET?: string }).JWT_SECRET = original;
      }
      return err;
    });

    expect(caught).toBeInstanceOf(VFSConfigError);
    expect((caught as VFSConfigError).code).toBe("VFS_CONFIG_ERROR");
    expect((caught as VFSConfigError).message).toMatch(/JWT_SECRET/);
  });

  it("with JWT_SECRET configured, vfsListFiles succeeds (positive control)", async () => {
    const tenant = "b1-jwt-secret-present";
    const stub = E.USER_DO.get(
      E.USER_DO.idFromName(vfsUserDOName("default", tenant))
    );
    // Default test env DOES set JWT_SECRET, so this should just work.
    const r = await stub.vfsListFiles({ ns: "default", tenant }, { limit: 5 });
    expect(r.items).toEqual([]);
    expect(r.cursor).toBeUndefined();
  });
});
