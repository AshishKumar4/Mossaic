import { describe, it, expect } from "vitest";
import { env, runInDurableObject } from "cloudflare:test";
import type { UserDO } from "@app/objects/user/user-do";

/**
 * per-tenant rate limit enforcement.
 *
 * Token bucket per (tenant, sub?) stored in `quota.rate_limit_*`
 * columns. Refills at rate, allows up to burst, throws `EAGAIN`
 * (typed subclass on the SDK side) when exhausted. Tenants are
 * isolated by DO instance + SQL user_id; tenant A's burst doesn't
 * affect tenant B.
 */

import { createVFS, type MossaicEnv, EAGAIN, VFSFsError } from "../../sdk/src/index";
import { vfsUserDOName } from "@core/lib/utils";

interface E {
  MOSSAIC_USER: DurableObjectNamespace<UserDO>;
  MOSSAIC_SHARD: DurableObjectNamespace;
}
const E = env as unknown as E;

function envFor(): MossaicEnv {
  return {
    MOSSAIC_USER: E.MOSSAIC_USER as MossaicEnv["MOSSAIC_USER"],
    MOSSAIC_SHARD: E.MOSSAIC_SHARD as unknown as MossaicEnv["MOSSAIC_SHARD"],
  };
}

/**
 * Lower the rate limit for a tenant via direct DO state mutation
 * (the operator path uses `setRateLimit`, but we keep the test
 * surface tight by writing the SQL directly). Resets tokens to the
 * configured burst so subsequent ops re-bucket from a known state.
 */
async function setLimit(
  tenant: string,
  perSec: number,
  burst: number
): Promise<void> {
  const stub = E.MOSSAIC_USER.get(
    E.MOSSAIC_USER.idFromName(vfsUserDOName("default", tenant))
  );
  // Trigger init.
  await stub.vfsExists({ ns: "default", tenant }, "/");
  await runInDurableObject(stub, async (_inst, state) => {
    state.storage.sql.exec(
      `INSERT OR REPLACE INTO quota
         (user_id, storage_used, storage_limit, file_count, pool_size,
          rate_limit_per_sec, rate_limit_burst, rl_tokens, rl_updated_at)
       VALUES (?, 0, 107374182400, 0, 32, ?, ?, ?, ?)`,
      tenant,
      perSec,
      burst,
      burst,
      Date.now()
    );
  });
}

describe("Per-tenant rate limit", () => {
  it("under-limit ops succeed", async () => {
    const tenant = "rl-under";
    await setLimit(tenant, 100, 50);
    const vfs = createVFS(envFor(), { tenant });
    // 10 ops under the burst of 50 → all succeed.
    for (let i = 0; i < 10; i++) {
      expect(await vfs.exists("/")).toBe(true);
    }
  });

  it("burst exceeding limit returns EAGAIN typed subclass", async () => {
    const tenant = "rl-burst";
    // Tight limit: 1 op/sec refill, burst of 3.
    await setLimit(tenant, 1, 3);
    const vfs = createVFS(envFor(), { tenant });

    // Three exists() calls drain the bucket from full. The fourth
    // (immediately, no time elapsed for refill) hits EAGAIN.
    await vfs.exists("/");
    await vfs.exists("/");
    await vfs.exists("/");
    let caught: unknown = null;
    try {
      await vfs.exists("/");
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(EAGAIN);
    expect((caught as VFSFsError).code).toBe("EAGAIN");
  });

  it("tenant A's exhaustion does NOT affect tenant B", async () => {
    const tenantA = "rl-iso-A";
    const tenantB = "rl-iso-B";
    await setLimit(tenantA, 1, 2);
    await setLimit(tenantB, 1, 2);
    const vfsA = createVFS(envFor(), { tenant: tenantA });
    const vfsB = createVFS(envFor(), { tenant: tenantB });

    // Drain tenant A.
    await vfsA.exists("/");
    await vfsA.exists("/");
    await expect(vfsA.exists("/")).rejects.toBeInstanceOf(EAGAIN);

    // Tenant B still has full burst; should work fine.
    await vfsB.exists("/");
    await vfsB.exists("/");
    // Same pattern, B exhausts after its own burst.
    await expect(vfsB.exists("/")).rejects.toBeInstanceOf(EAGAIN);
  });

  it("tokens refill over time (sleep-based regression check)", async () => {
    const tenant = "rl-refill";
    // 50 ops/sec → 1 token per 20ms. Burst 1 so a single op drains.
    await setLimit(tenant, 50, 1);
    const vfs = createVFS(envFor(), { tenant });
    await vfs.exists("/"); // drains bucket
    // Wait 100ms — refill should give us at least 1 token back.
    await new Promise((r) => setTimeout(r, 100));
    expect(await vfs.exists("/")).toBe(true);
  });

  it("default rate (100/sec, 200 burst) is generous enough that the test suite never hits it", async () => {
    // Sanity: a fresh tenant with no explicit limit should let
    // 100 sequential ops through without EAGAIN. This guards
    // against accidentally tightening defaults below realistic
    // workload thresholds.
    const tenant = "rl-default";
    const vfs = createVFS(envFor(), { tenant });
    for (let i = 0; i < 100; i++) {
      // exists(/) runs synchronously through the bucket without
      // waiting for refill; 100 < 200 burst.
      expect(await vfs.exists("/")).toBe(true);
    }
  });

  it("server-side VFSError EAGAIN maps to typed EAGAIN subclass on SDK consumer", async () => {
    // Independent verification of the typed-error round-trip.
    const tenant = "rl-typed";
    await setLimit(tenant, 1, 1);
    const vfs = createVFS(envFor(), { tenant });
    await vfs.exists("/"); // drain
    let caught: unknown = null;
    try {
      await vfs.exists("/");
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(EAGAIN);
    expect(caught).toBeInstanceOf(VFSFsError);
    expect((caught as VFSFsError).errno).toBe(-11); // POSIX EAGAIN
  });
});
