import { describe, it, expect } from "vitest";
import { SELF, env, runInDurableObject } from "cloudflare:test";
import type { UserDO } from "@app/objects/user/user-do";
import { signVFSToken } from "@core/lib/auth";
import { vfsUserDOName } from "@core/lib/utils";

/**
 * Phase 41 Fix 1 — multipart-routes.ts errToResponse must map EAGAIN
 * to HTTP 429 (audit 40A P1).
 *
 * Background: a per-tenant rate-limit hit throws `VFSError("EAGAIN",
 * ...)` from the gate inside UserDO. The HTTP route catches the
 * thrown error and runs it through `errToResponse`. Phase 39 left a
 * locally-redeclared `errToResponse` in multipart-routes.ts whose
 * KNOWN map LACKED `EAGAIN` — the rate-limit hit collapsed to 500
 * (and the SDK's mapServerError treated it as a generic
 * VFSFsError, not a typed `EAGAIN` retry signal).
 *
 * The fix imports the canonical `errToResponse` from `vfs.ts` so the
 * status-code map is single-source-of-truth. This test pins the
 * behaviour at the HTTP wire: a multipart route hitting a
 * rate-limited tenant returns 429 + body.code === "EAGAIN".
 *
 * Three pinning tests:
 *   ME1 — multipart/begin under rate limit → 429 + EAGAIN
 *   ME2 — multipart/finalize under rate limit → 429 + EAGAIN
 *   ME3 — multipart/abort under rate limit → 429 + EAGAIN
 *
 * Each test pinches the tenant's bucket to (perSec=1, burst=1, tokens=0)
 * via direct DO mutation — the next gated call hits EAGAIN
 * synchronously without waiting for a refill.
 */

interface E {
  MOSSAIC_USER: DurableObjectNamespace<UserDO>;
  JWT_SECRET?: string;
}
const TEST_ENV = env as unknown as E;

async function mint(tenant: string): Promise<string> {
  return signVFSToken(TEST_ENV as never, { ns: "default", tenant });
}

/**
 * Drain the tenant's token bucket so the very next gated VFS call
 * throws EAGAIN. We seed a fresh-ish row, then zero the tokens. The
 * DO's gateVfs() reads tokens, computes the refill against
 * rl_updated_at, and throws if the result is still < 1.
 *
 * To ensure no refill happens between this seed and the test's
 * subsequent route call, we also set rl_updated_at = now and
 * rate_limit_per_sec = a tiny value so the refill window contributes
 * negligible tokens within the test's millisecond budget.
 */
async function pinchBucket(tenant: string): Promise<void> {
  const stub = TEST_ENV.MOSSAIC_USER.get(
    TEST_ENV.MOSSAIC_USER.idFromName(vfsUserDOName("default", tenant))
  );
  // Trigger init so the quota row exists and the rate-limit columns
  // are populated.
  await stub.vfsExists({ ns: "default", tenant }, "/");
  await runInDurableObject(stub, async (_inst, state) => {
    state.storage.sql.exec(
      `INSERT OR REPLACE INTO quota
         (user_id, storage_used, storage_limit, file_count, pool_size,
          rate_limit_per_sec, rate_limit_burst, rl_tokens, rl_updated_at)
       VALUES (?, 0, 107374182400, 0, 32, 1, 1, 0, ?)`,
      tenant,
      Date.now()
    );
  });
}

describe("Phase 41 Fix 1 — multipart errToResponse maps EAGAIN to 429", () => {
  it("ME1 — multipart/begin under rate-limit → 429 + body.code = EAGAIN", async () => {
    const tenant = "me1-tenant";
    await pinchBucket(tenant);

    const bearer = await mint(tenant);
    const res = await SELF.fetch("https://test/api/vfs/multipart/begin", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${bearer}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        path: "/file.bin",
        size: 1024,
      }),
    });

    expect(res.status).toBe(429);
    const body = (await res.json()) as { code: string; message: string };
    expect(body.code).toBe("EAGAIN");
    // The message must surface the rate-limit context for operator
    // dashboards / SDK pretty-print, not a generic EINTERNAL.
    expect(body.message).toMatch(/rate.limit|EAGAIN/i);
  });

  it("ME2 — multipart/finalize under rate-limit → 429 + body.code = EAGAIN", async () => {
    const tenant = "me2-tenant";
    // Fresh bucket so begin() succeeds; pinch AFTER begin so finalize
    // is the call that hits EAGAIN.
    const stub = TEST_ENV.MOSSAIC_USER.get(
      TEST_ENV.MOSSAIC_USER.idFromName(vfsUserDOName("default", tenant))
    );
    await stub.vfsExists({ ns: "default", tenant }, "/");
    await runInDurableObject(stub, async (_inst, state) => {
      state.storage.sql.exec(
        `INSERT OR REPLACE INTO quota
           (user_id, storage_used, storage_limit, file_count, pool_size,
            rate_limit_per_sec, rate_limit_burst, rl_tokens, rl_updated_at)
         VALUES (?, 0, 107374182400, 0, 32, 100, 100, 100, ?)`,
        tenant,
        Date.now()
      );
    });

    const bearer = await mint(tenant);
    const beginRes = await SELF.fetch("https://test/api/vfs/multipart/begin", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${bearer}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ path: "/me2.bin", size: 4 }),
    });
    expect(beginRes.status).toBe(200);
    const begin = (await beginRes.json()) as {
      uploadId: string;
      sessionToken: string;
    };

    // Pinch the bucket; the upcoming finalize call hits EAGAIN.
    await pinchBucket(tenant);

    // Routes mount flat: POST /api/vfs/multipart/finalize with the
    // uploadId in the body (NOT /<uploadId>/finalize).
    const finalizeRes = await SELF.fetch(
      "https://test/api/vfs/multipart/finalize",
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${bearer}`,
          "X-Session-Token": begin.sessionToken,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          uploadId: begin.uploadId,
          chunkHashList: ["0".repeat(64)],
        }),
      }
    );

    expect(finalizeRes.status).toBe(429);
    const body = (await finalizeRes.json()) as {
      code: string;
      message: string;
    };
    expect(body.code).toBe("EAGAIN");
  });

  it("ME3 — multipart/abort under rate-limit → 429 + body.code = EAGAIN", async () => {
    const tenant = "me3-tenant";
    // Same shape as ME2: open begin first, then pinch, then abort.
    const stub = TEST_ENV.MOSSAIC_USER.get(
      TEST_ENV.MOSSAIC_USER.idFromName(vfsUserDOName("default", tenant))
    );
    await stub.vfsExists({ ns: "default", tenant }, "/");
    await runInDurableObject(stub, async (_inst, state) => {
      state.storage.sql.exec(
        `INSERT OR REPLACE INTO quota
           (user_id, storage_used, storage_limit, file_count, pool_size,
            rate_limit_per_sec, rate_limit_burst, rl_tokens, rl_updated_at)
         VALUES (?, 0, 107374182400, 0, 32, 100, 100, 100, ?)`,
        tenant,
        Date.now()
      );
    });

    const bearer = await mint(tenant);
    const beginRes = await SELF.fetch("https://test/api/vfs/multipart/begin", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${bearer}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ path: "/me3.bin", size: 4 }),
    });
    expect(beginRes.status).toBe(200);
    const begin = (await beginRes.json()) as {
      uploadId: string;
      sessionToken: string;
    };

    await pinchBucket(tenant);

    // Routes mount flat: POST /api/vfs/multipart/abort with the
    // uploadId in the body.
    const abortRes = await SELF.fetch(
      "https://test/api/vfs/multipart/abort",
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${bearer}`,
          "X-Session-Token": begin.sessionToken,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ uploadId: begin.uploadId }),
      }
    );

    expect(abortRes.status).toBe(429);
    const body = (await abortRes.json()) as {
      code: string;
      message: string;
    };
    expect(body.code).toBe("EAGAIN");
  });
});
