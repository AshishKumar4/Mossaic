import { describe, it, expect } from "vitest";
import { env, runInDurableObject } from "cloudflare:test";

/**
 * P1-6 fix — admin RPCs apply per-tenant rate-limit gate.
 *
 * Pre-fix `adminSetVersioning`, `adminGetVersioning`,
 * `adminDedupePaths`, `adminReapTombstonedHeads`,
 * `adminPreGenerateStandardVariants` called only `ensureInit()` —
 * no `gateVfs` / `enforceRateLimit`. A binding holder could DoS
 * a tenant DO without throttling. Phase 31 closed the equivalent
 * gap on the App-side `app*` surface; this closes it on the
 * Core admin* surface.
 *
 * Tests pin:
 *   A1 — `adminGetVersioning` rate-limits via the persisted scope.
 *   A2 — `adminSetVersioning` rate-limits via the synthesized
 *        scope `{ ns: "default", tenant: userId }`.
 *   A3 — `adminDedupePaths` (write-class) refuses with EBUSY
 *        when the H6 partial-UNIQUE-INDEX marker is present.
 *   A4 — `adminPreGenerateStandardVariants` is read-class
 *        gated (does NOT trip on H6 marker).
 *   A5 — Cross-tenant isolation: admin RPC against tenant A's
 *        scope does not affect tenant B's bucket.
 */

import { vfsUserDOName } from "@core/lib/utils";
import type { UserDO } from "@app/objects/user/user-do";

interface E {
  MOSSAIC_USER: DurableObjectNamespace<UserDO>;
}
const E = env as unknown as E;

function userStub(name: string): DurableObjectStub<UserDO> {
  return E.MOSSAIC_USER.get(E.MOSSAIC_USER.idFromName(name));
}

async function setLimitForUser(
  userId: string,
  perSec: number,
  burst: number
): Promise<void> {
  const stub = userStub(vfsUserDOName("default", userId));
  // First call triggers ensureInit + scope persistence.
  await stub.adminGetVersioning(userId);
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

describe("admin RPC gating (P1-6)", () => {
  it("A1 — adminGetVersioning rate-limits per tenant", async () => {
    const userId = "admin-gate-a1";
    await setLimitForUser(userId, 1, 3);
    const stub = userStub(vfsUserDOName("default", userId));

    // Burst of 3 → 3 calls succeed.
    await stub.adminGetVersioning(userId);
    await stub.adminGetVersioning(userId);
    await stub.adminGetVersioning(userId);

    let caught: unknown = null;
    try {
      await stub.adminGetVersioning(userId);
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeTruthy();
    const msg = caught instanceof Error ? caught.message : String(caught);
    expect(msg).toMatch(/EAGAIN|rate limit/i);
  });

  it("A2 — adminSetVersioning rate-limits via synthesized scope", async () => {
    const userId = "admin-gate-a2";
    await setLimitForUser(userId, 1, 3);
    const stub = userStub(vfsUserDOName("default", userId));

    // Drain via 3 read-class admin calls.
    await stub.adminGetVersioning(userId);
    await stub.adminGetVersioning(userId);
    await stub.adminGetVersioning(userId);

    // Write-class admin call hits same per-tenant bucket → EAGAIN.
    let caught: unknown = null;
    try {
      await stub.adminSetVersioning(userId, true);
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeTruthy();
    const msg = caught instanceof Error ? caught.message : String(caught);
    expect(msg).toMatch(/EAGAIN|rate limit/i);
  });

  it("A3 — adminDedupePaths refuses with EBUSY on H6 marker", async () => {
    const userId = "admin-gate-a3";
    const stub = userStub(vfsUserDOName("default", userId));
    // Trigger ensureInit + scope persistence.
    await stub.adminGetVersioning(userId);
    // Plant the H6 partial-UNIQUE-INDEX marker.
    await runInDurableObject(stub, async (_inst, state) => {
      state.storage.sql.exec(
        "INSERT OR REPLACE INTO vfs_meta (key, value) VALUES ('files_unique_index', 'degraded')"
      );
    });

    let caught: unknown = null;
    try {
      await stub.adminDedupePaths(userId, {
        ns: "default",
        tenant: userId,
      });
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeTruthy();
    const msg = caught instanceof Error ? caught.message : String(caught);
    expect(msg).toMatch(/EBUSY|legacy duplicate/i);
  });

  it("A4 — adminPreGenerateStandardVariants is read-class (NOT blocked by H6)", async () => {
    const userId = "admin-gate-a4";
    const stub = userStub(vfsUserDOName("default", userId));
    await stub.adminGetVersioning(userId);
    await runInDurableObject(stub, async (_inst, state) => {
      state.storage.sql.exec(
        "INSERT OR REPLACE INTO vfs_meta (key, value) VALUES ('files_unique_index', 'degraded')"
      );
    });

    // Read-class gate; H6 marker does NOT block. The call may
    // throw for other reasons (file_id not found, etc.) — we only
    // assert it's NOT an EBUSY about the H6 marker.
    let caught: unknown = null;
    try {
      await stub.adminPreGenerateStandardVariants(
        { ns: "default", tenant: userId },
        {
          fileId: "nonexistent-file-id",
          path: "/x.png",
          mimeType: "image/png",
          fileName: "x.png",
          fileSize: 0,
          isEncrypted: false,
        }
      );
    } catch (err) {
      caught = err;
    }
    // If it didn't throw, that's fine. If it did throw, it must
    // NOT be an EBUSY-on-H6 — that would prove the read-class
    // gate ISN'T being used (the write gate would have refused
    // with EBUSY before any work).
    if (caught) {
      const msg = caught instanceof Error ? caught.message : String(caught);
      expect(msg).not.toMatch(/legacy duplicate rows block/i);
    }
  });

  it("A5 — cross-tenant isolation: tenant A's drained bucket does not affect B", async () => {
    const userA = "admin-gate-iso-a";
    const userB = "admin-gate-iso-b";
    await setLimitForUser(userA, 1, 2);
    await setLimitForUser(userB, 1, 2);

    const stubA = userStub(vfsUserDOName("default", userA));
    const stubB = userStub(vfsUserDOName("default", userB));

    // Drain A's bucket completely.
    await stubA.adminGetVersioning(userA);
    await stubA.adminGetVersioning(userA);
    let aCaught: unknown = null;
    try {
      await stubA.adminGetVersioning(userA);
    } catch (err) {
      aCaught = err;
    }
    expect(aCaught).toBeTruthy();

    // B's bucket is untouched — call succeeds.
    const bResult = await stubB.adminGetVersioning(userB);
    expect(bResult).toEqual({ enabled: false });
  });
});
