import { describe, it, expect } from "vitest";
import { env, runInDurableObject } from "cloudflare:test";
import { vfsUserDOName } from "@core/lib/utils";
import type { ShardDO } from "@core/objects/shard/shard-do";

/**
 * Phase 32 Fix 3 — rmrf subrequest budget.
 *
 * BATCH_LIMIT lowered 200 \u2192 30; non-versioning rmrf batches the
 * shard fan-out via `ShardDO.deleteManyChunks(fileIds[])`. Worst
 * case becomes poolSize = 32 subrequests per call regardless of
 * BATCH_LIMIT.
 *
 * Cases:
 *   RB1. BATCH_LIMIT honored \u2014 single call processes \u226430 files;
 *        more remain \u2192 done=false.
 *   RB2. rmrf of 30 chunked files completes in bounded
 *        subrequests; all rows reaped after drain loop.
 *   RB3. ShardDO.deleteManyChunks idempotent on missing fileIds.
 *
 * Phase 32.5 Fix 3 (BATCH_LIMIT branch):
 *   RB4. Versioning-ON tenant rmrf processes >30 files per call.
 *        The versioning branch tombstones in-place (no shard
 *        fan-out) so BATCH_LIMIT is raised to 200; this test
 *        proves the higher limit is in effect.
 */

interface E {
  MOSSAIC_USER: DurableObjectNamespace;
  MOSSAIC_SHARD: DurableObjectNamespace<ShardDO>;
}
const E = env as unknown as E;
const NS = "default";

describe("Phase 32 Fix 3 — rmrf subrequest budget", () => {
  it("RB1 — BATCH_LIMIT (30) honored", async () => {
    const tenant = "rb1-limit";
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
    );
    const scope = { ns: NS, tenant };
    await stub.vfsMkdir(scope, "/d", { recursive: true });
    for (let i = 0; i < 35; i++) {
      await stub.vfsWriteFile(scope, `/d/f${i}.txt`, new Uint8Array([i]));
    }
    const first = await stub.vfsRemoveRecursive(scope, "/d");
    expect(first.done).toBe(false);

    let safety = 50;
    while (!(await stub.vfsRemoveRecursive(scope, "/d")).done) {
      safety--;
      if (safety <= 0) throw new Error("rmrf did not terminate");
    }

    const remaining = await runInDurableObject(stub, async (_inst, state) => {
      return (
        state.storage.sql
          .exec(
            "SELECT COUNT(*) AS n FROM files WHERE user_id = ? AND status='complete'",
            tenant
          )
          .toArray()[0] as { n: number }
      ).n;
    });
    expect(remaining).toBe(0);
  });

  it(
    "RB2 — rmrf of 30 chunked files completes; all rows reaped",
    async () => {
      const tenant = "rb2-completes";
      const stub = E.MOSSAIC_USER.get(
        E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
      );
      const scope = { ns: NS, tenant };
      await stub.vfsMkdir(scope, "/big", { recursive: true });
      const payload = new Uint8Array(20 * 1024).fill(7);
      for (let i = 0; i < 30; i++) {
        await stub.vfsWriteFile(scope, `/big/f${i}.bin`, payload);
      }
      // Generous safety \u2014 under full-suite load the workerd test
      // pool can evict the DO mid-call, restarting the per-call
      // BATCH_LIMIT budget. The contract being tested is that
      // termination IS reached, not the exact number of iterations.
      let safety = 100;
      while (!(await stub.vfsRemoveRecursive(scope, "/big")).done) {
        safety--;
        if (safety <= 0) throw new Error("rmrf did not terminate");
      }
      const after = await runInDurableObject(stub, async (_inst, state) => {
        const f = (
          state.storage.sql
            .exec("SELECT COUNT(*) AS n FROM files WHERE user_id = ?", tenant)
            .toArray()[0] as { n: number }
        ).n;
        const fc = (
          state.storage.sql
            .exec("SELECT COUNT(*) AS n FROM file_chunks")
            .toArray()[0] as { n: number }
        ).n;
        return { f, fc };
      });
      expect(after.f).toBe(0);
      expect(after.fc).toBe(0);
    },
    30000 // 30s test timeout to ride out workerd eviction storms
  );

  it("RB3 — deleteManyChunks idempotent on missing file_ids", async () => {
    const stub = E.MOSSAIC_SHARD.get(
      E.MOSSAIC_SHARD.idFromName("rb3-missing-test")
    );
    await stub.getStorageBytes(); // ensureInit
    const r = await stub.deleteManyChunks(["nonexistent-1", "nonexistent-2"]);
    expect(r.marked).toBe(0);
  });

  it("RB4 \u2014 versioning-ON tenant rmrf processes >30 files per call", async () => {
    const tenant = "rb4-versioning-batch";
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
    );
    const scope = { ns: NS, tenant };
    const userId = tenant;

    // Enable versioning so rmrf takes the tombstone-in-place
    // branch (no shard fan-out, BATCH_LIMIT raised to 200).
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    await (stub as any).adminSetVersioning(userId, true);

    await stub.vfsMkdir(scope, "/dv", { recursive: true });
    // 50 tiny files \u2014 enough to prove >30 are processed in one
    // call (would have required 2 calls under the old BATCH_LIMIT=30).
    for (let i = 0; i < 50; i++) {
      await stub.vfsWriteFile(scope, `/dv/f${i}.txt`, new Uint8Array([i]));
    }

    const first = await stub.vfsRemoveRecursive(scope, "/dv");
    // Either the call completed (`done=true` because 50 \u2264 200)
    // OR \u2014 if the limit wasn't raised \u2014 it would return
    // done=false and 20 files would remain. Assert the high-limit
    // path: completion in one call.
    expect(first.done).toBe(true);

    // Tombstones, not metadata deletion: every path now resolves
    // as ENOENT for non-versioned reads. Verify by counting
    // tombstoned-head rows (file_name suffix \`.tombstoned-\`).
    const tombs = await runInDurableObject(stub, async (_inst, state) => {
      return (
        state.storage.sql
          .exec(
            "SELECT COUNT(*) AS n FROM files WHERE user_id = ? AND file_name LIKE '%.tombstoned-%'",
            userId
          )
          .toArray()[0] as { n: number }
      ).n;
    });
    expect(tombs).toBe(50);
  });
});
