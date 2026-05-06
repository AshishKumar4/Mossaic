import { describe, it, expect } from "vitest";
import { env, runInDurableObject, SELF } from "cloudflare:test";
import { vfsUserDOName } from "@core/lib/utils";

/**
 * App* RPC gate parity.
 *
 * The Core+App split moved Core's per-tenant rate limiter
 * (`gateVfs`) onto every Core RPC method but none of the App-side
 * `app*` RPCs gained the equivalent gate. Audit `N1` called out
 * 18 unrate-limited methods. The fix wires `appGate` /
 * `appGateWrite` (in `worker/app/objects/user/gate.ts`) into every
 * such method, reproducing `gateVfs`/`gateVfsWrite` semantics from
 * outside the (private) Core method using `enforceRateLimit`
 * (exported) + `ensureInit` (protected on Core, accessed via
 * structural cast).
 *
 * This file pins:
 *   1. Public-route requests without an auth token return 401 —
 *      `authMiddleware` is upstream of the DO; this test guards
 *      against any future "expose an app* RPC publicly" mistake.
 *   2. Per-tenant rate-limit triggers on app* methods (read class).
 *   3. Per-tenant rate-limit triggers on app* methods (write class).
 *   4. Per-tenant rate-limit triggers on auth methods (signup/login)
 *      against the `auth:<email>` DO — brute-force defense.
 *   5. Cross-tenant isolation: tenant A's bucket does not affect
 *      tenant B (each tenant lives in its own DO).
 *   6. `appGetFilePath` / `appGetFile` recover scope from
 *      `vfs_meta.scope` so they too rate-limit (they take only
 *      `fileId` so cannot derive scope from arguments).
 *   7. Write methods refuse with EBUSY when the H6 partial-unique-
 *      index marker is present.
 *   8. Admin wipe RPCs apply rate-limit + write gate.
 */

import type { UserDO } from "@app/objects/user/user-do";

interface E {
  MOSSAIC_USER: DurableObjectNamespace<UserDO>;
}
const E = env as unknown as E;

function userStub(name: string): DurableObjectStub<UserDO> {
  return E.MOSSAIC_USER.get(E.MOSSAIC_USER.idFromName(name));
}

/**
 * Pre-seed a tight rate-limit + reset bucket-tokens for a tenant
 * DO. Mirror of `tests/integration/rate-limit.test.ts`'s helper.
 * Triggers `ensureInit` first via `appGetQuota` (which itself calls
 * the new gate — note: the FIRST call still succeeds because the
 * bucket is initialised to full burst on observation).
 */
async function setLimitForUser(
  userId: string,
  perSec: number,
  burst: number
): Promise<void> {
  const stub = userStub(vfsUserDOName("default", userId));
  await stub.appGetQuota(userId);
  await runInDurableObject(stub, async (_inst, state) => {
    state.storage.sql.exec(
      `INSERT OR REPLACE INTO quota
         (user_id, storage_used, storage_limit, file_count, pool_size,
          rate_limit_per_sec, rate_limit_burst, rl_tokens, rl_updated_at)
       VALUES (?, 0, 107374182400, 0, 32, ?, ?, ?, ?)`,
      userId,
      perSec,
      burst,
      burst,
      Date.now()
    );
  });
}

async function setLimitForAuthDo(
  email: string,
  perSec: number,
  burst: number
): Promise<void> {
  const stub = userStub(`auth:${email}`);
  // Trigger ensureInit so the schema exists.
  try {
    await stub.appHandleLogin(email, "wrong-password-just-init");
  } catch {
    // login fails (no row) but ensureInit ran.
  }
  const userId = `auth:${email}`;
  await runInDurableObject(stub, async (_inst, state) => {
    state.storage.sql.exec(
      `INSERT OR REPLACE INTO quota
         (user_id, storage_used, storage_limit, file_count, pool_size,
          rate_limit_per_sec, rate_limit_burst, rl_tokens, rl_updated_at)
       VALUES (?, 0, 107374182400, 0, 32, ?, ?, ?, ?)`,
      userId,
      perSec,
      burst,
      burst,
      Date.now()
    );
  });
}

describe("App route layer — unauthenticated requests are 401 (auth gate)", () => {
  it("GET /api/files (appListFiles) without bearer → 401", async () => {
    const r = await SELF.fetch("https://test/api/files", { method: "GET" });
    expect(r.status).toBe(401);
  });

  it("GET /api/analytics (appGetUserStats + appGetQuota) without bearer → 401", async () => {
    const r = await SELF.fetch("https://test/api/analytics", { method: "GET" });
    expect(r.status).toBe(401);
  });

  it("GET /api/gallery/photos (appGetGalleryPhotos) without bearer → 401", async () => {
    const r = await SELF.fetch("https://test/api/gallery/photos", {
      method: "GET",
    });
    expect(r.status).toBe(401);
  });

  it("DELETE /api/files/:id (appDeleteFile) without bearer → 401", async () => {
    const r = await SELF.fetch("https://test/api/files/some-id", {
      method: "DELETE",
    });
    expect(r.status).toBe(401);
  });

  it("DELETE /api/auth/account (appWipeAccountData/appWipeAuthRow) without bearer → 401", async () => {
    const r = await SELF.fetch("https://test/api/auth/account", {
      method: "DELETE",
    });
    expect(r.status).toBe(401);
  });
});

describe("Per-tenant rate-limit on read-class app* methods (audit N1)", () => {
  it("appGetQuota: drains bucket and trips EAGAIN", async () => {
    const userId = "gate-read-quota";
    await setLimitForUser(userId, 1, 3);
    const stub = userStub(vfsUserDOName("default", userId));

    // Burst of 3 → 3 calls succeed.
    await stub.appGetQuota(userId);
    await stub.appGetQuota(userId);
    await stub.appGetQuota(userId);

    // 4th immediately → EAGAIN.
    let caught: unknown = null;
    try {
      await stub.appGetQuota(userId);
    } catch (err) {
      caught = err;
    }
    // RPC errors are surfaced as Error subclasses; the message
    // carries the EAGAIN code (mirror of how rate-limit.test.ts
    // observes EAGAIN against canonical methods).
    expect(caught).toBeTruthy();
    const msg = caught instanceof Error ? caught.message : String(caught);
    expect(msg).toMatch(/EAGAIN|rate limit/i);
  });

  it("appListFiles: same bucket as appGetQuota — share the per-tenant limiter", async () => {
    const userId = "gate-read-shared";
    await setLimitForUser(userId, 1, 2);
    const stub = userStub(vfsUserDOName("default", userId));

    // Drain via appGetQuota.
    await stub.appGetQuota(userId);
    await stub.appGetQuota(userId);

    // appListFiles is a different method but hits the same per-
    // tenant bucket (same scope → same SQL row). Should EAGAIN.
    let caught: unknown = null;
    try {
      await stub.appListFiles(userId, null);
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeTruthy();
    const msg = caught instanceof Error ? caught.message : String(caught);
    expect(msg).toMatch(/EAGAIN|rate limit/i);
  });

  it("appGetFilePath / appGetFile recover scope from vfs_meta and rate-limit", async () => {
    const userId = "gate-read-no-userid";
    // Pre-seed scope via a userId-bearing gated call.
    const stub = userStub(vfsUserDOName("default", userId));
    await stub.appGetQuota(userId);

    // Now zero the bucket completely AND set the refill rate so
    // low that even a few hundred ms of test scheduling jitter
    // does NOT refill back to ≥1 token. perSec=0.1 ⇒ 1 token per
    // 10s; the scope-recovering RPC fires within milliseconds, so
    // refill is effectively 0 on the next call.
    await runInDurableObject(stub, async (_inst, state) => {
      state.storage.sql.exec(
        `UPDATE quota
            SET rate_limit_per_sec = 0.1,
                rate_limit_burst   = 1,
                rl_tokens          = 0,
                rl_updated_at      = ?
          WHERE user_id = ?`,
        Date.now(),
        userId
      );
    });

    // appGetFilePath has no userId arg — must read scope from
    // vfs_meta.scope (set by the prior gated call) and apply the
    // same limiter against the now-empty bucket.
    let caught: unknown = null;
    try {
      await stub.appGetFilePath("nonexistent-file-id");
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeTruthy();
    const msg = caught instanceof Error ? caught.message : String(caught);
    expect(msg).toMatch(/EAGAIN|rate limit/i);
  });
});

describe("Per-tenant rate-limit on write-class app* methods (audit N1)", () => {
  it("appCreateFolder + appDeleteFile: write methods rate-limit too", async () => {
    const userId = "gate-write";
    await setLimitForUser(userId, 1, 3);
    const stub = userStub(vfsUserDOName("default", userId));

    // Drain via 3 reads.
    await stub.appGetQuota(userId);
    await stub.appGetQuota(userId);
    await stub.appGetQuota(userId);

    // Write method hits same bucket → EAGAIN.
    let caught: unknown = null;
    try {
      await stub.appCreateFolder(userId, "should-fail", null);
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeTruthy();
    const msg = caught instanceof Error ? caught.message : String(caught);
    expect(msg).toMatch(/EAGAIN|rate limit/i);
  });
});

describe("Auth method brute-force defense (per-account bucket)", () => {
  it("appHandleLogin against the auth:<email> DO trips EAGAIN under burst", async () => {
    const email = "gate-brute@example.com";
    await setLimitForAuthDo(email, 1, 2);
    const stub = userStub(`auth:${email}`);

    // 2 calls drain the bucket (each call rate-limits even though
    // login fails — the gate runs before credential check).
    try {
      await stub.appHandleLogin(email, "x");
    } catch {
      // expected: invalid credentials
    }
    try {
      await stub.appHandleLogin(email, "x");
    } catch {
      // expected: invalid credentials
    }

    // 3rd attempt → EAGAIN (gate trips before login logic).
    let caught: unknown = null;
    try {
      await stub.appHandleLogin(email, "x");
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeTruthy();
    const msg = caught instanceof Error ? caught.message : String(caught);
    expect(msg).toMatch(/EAGAIN|rate limit/i);
  });
});

describe("Cross-tenant isolation (per-tenant DO ⇒ per-tenant bucket)", () => {
  it("tenant A's drained bucket does NOT affect tenant B", async () => {
    const userA = "gate-iso-a";
    const userB = "gate-iso-b";
    await setLimitForUser(userA, 1, 2);
    await setLimitForUser(userB, 1, 2);

    const stubA = userStub(vfsUserDOName("default", userA));
    const stubB = userStub(vfsUserDOName("default", userB));

    // Drain A's bucket completely.
    await stubA.appGetQuota(userA);
    await stubA.appGetQuota(userA);
    let aCaught: unknown = null;
    try {
      await stubA.appGetQuota(userA);
    } catch (err) {
      aCaught = err;
    }
    expect(aCaught).toBeTruthy();

    // B's bucket is untouched — call should succeed.
    const bQuota = await stubB.appGetQuota(userB);
    expect(bQuota).toBeTruthy();
  });
});

describe("Write methods refuse on H6 partial-unique-index degraded marker", () => {
  it("appCreateFolder throws EBUSY when files_unique_index marker is present", async () => {
    const userId = "gate-h6-busy";
    const stub = userStub(vfsUserDOName("default", userId));
    // Trigger ensureInit + scope-record by calling a read-class
    // gate first, then plant the H6 marker.
    await stub.appGetQuota(userId);
    await runInDurableObject(stub, async (_inst, state) => {
      state.storage.sql.exec(
        "INSERT OR REPLACE INTO vfs_meta (key, value) VALUES ('files_unique_index', 'degraded')"
      );
    });

    let caught: unknown = null;
    try {
      await stub.appCreateFolder(userId, "should-be-blocked", null);
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeTruthy();
    const msg = caught instanceof Error ? caught.message : String(caught);
    expect(msg).toMatch(/EBUSY|legacy duplicate/i);
  });

  it("read methods are NOT blocked by H6 marker (Core parity)", async () => {
    const userId = "gate-h6-read-ok";
    const stub = userStub(vfsUserDOName("default", userId));
    await stub.appGetQuota(userId);
    await runInDurableObject(stub, async (_inst, state) => {
      state.storage.sql.exec(
        "INSERT OR REPLACE INTO vfs_meta (key, value) VALUES ('files_unique_index', 'degraded')"
      );
    });
    // Read class call should still work — Core's gateVfs (reads)
    // tolerates dupes; we mirror that.
    const quota = await stub.appGetQuota(userId);
    expect(quota).toBeTruthy();
  });
});

describe("Admin wipe RPCs apply gate (defense against replayed wipe attempts)", () => {
  it("appWipeAccountData rate-limits", async () => {
    const userId = "gate-wipe-data";
    await setLimitForUser(userId, 1, 2);
    const stub = userStub(vfsUserDOName("default", userId));

    // Drain.
    await stub.appGetQuota(userId);
    await stub.appGetQuota(userId);

    // Wipe should EAGAIN at the gate.
    let caught: unknown = null;
    try {
      await stub.appWipeAccountData(userId);
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeTruthy();
    const msg = caught instanceof Error ? caught.message : String(caught);
    expect(msg).toMatch(/EAGAIN|rate limit/i);
  });

  it("appWipeAuthRow rate-limits per-email", async () => {
    const email = "gate-wipe-auth@example.com";
    await setLimitForAuthDo(email, 1, 2);
    const stub = userStub(`auth:${email}`);

    // Drain via 2 login attempts.
    try {
      await stub.appHandleLogin(email, "x");
    } catch {
      /* invalid creds */
    }
    try {
      await stub.appHandleLogin(email, "x");
    } catch {
      /* invalid creds */
    }

    let caught: unknown = null;
    try {
      await stub.appWipeAuthRow(email);
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeTruthy();
    const msg = caught instanceof Error ? caught.message : String(caught);
    expect(msg).toMatch(/EAGAIN|rate limit/i);
  });
});

describe("Scope persistence (alarm rehydration parity with Core)", () => {
  it("appGate persists scope to vfs_meta so Core's loadScope finds it", async () => {
    const userId = "gate-scope-persist";
    const stub = userStub(vfsUserDOName("default", userId));
    // First gated app* call.
    await stub.appGetQuota(userId);

    const scopeRow = await runInDurableObject(stub, async (_inst, state) => {
      const r = state.storage.sql
        .exec("SELECT value FROM vfs_meta WHERE key = 'scope'")
        .toArray()[0] as { value: string } | undefined;
      return r?.value ?? null;
    });
    expect(scopeRow).toBeTruthy();
    const parsed = JSON.parse(scopeRow as string) as {
      ns: string;
      tenant: string;
    };
    expect(parsed.ns).toBe("default");
    expect(parsed.tenant).toBe(userId);
  });
});
