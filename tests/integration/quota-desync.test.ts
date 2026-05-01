import { describe, it, expect } from "vitest";
import { env, runInDurableObject } from "cloudflare:test";
import { vfsUserDOName } from "@core/lib/utils";

/**
 * Phase 32.5 \u2014 quota desync correction.
 *
 * Pre-Phase-32.5: every successful write incremented
 * `quota.storage_used` and `file_count` via `recordWriteUsage`,
 * but ZERO destructive paths decremented. Tenants who deleted
 * accumulated monotonic inflation in proportion to their
 * lifetime delete volume \u2014 gallery / analytics surfaces
 * reported wildly inaccurate numbers for any non-trivial
 * tenant.
 *
 * Phase 32.5 wires negative-delta `recordWriteUsage` calls
 * through the two non-versioning destructive paths:
 *   - `hardDeleteFileRow` (write-commit.ts) \u2014 inline + chunked
 *   - `vfsRemoveRecursive` non-versioning batch (mutations.ts)
 *
 * Versioning paths intentionally keep the asymmetry: the
 * versioning write path doesn't `recordWriteUsage`-positive-count
 * either, so dropping versions doesn't `recordWriteUsage`-
 * negative-count. Full versioned-bytes accounting is Phase 32.6.
 *
 * Lean invariant `Mossaic.Vfs.Quota.pool_size_monotonic` is
 * preserved \u2014 negative storage_used deltas are clamped at 0
 * by the `MAX(0, col + ?)` SQL expression in
 * `recordWriteUsage` (helpers.ts:421-425), and the
 * pool-recompute is gated on `newPool > row.pool_size` so
 * shrink is impossible regardless of delta sign.
 *
 * Cases:
 *   QD1. Inline write+unlink balances storage_used + file_count.
 *   QD2. Chunked write+unlink balances storage_used + file_count.
 *   QD3. Rmrf of N files balances both counters in batch.
 *   QD4. Pool size monotonic across delete cycles \u2014 grows on
 *        write, never shrinks on delete.
 *   QD5. Concurrent overwrites balance: write A + overwrite B at
 *        same path \u2192 storage_used = sizeof(B) and file_count = 1.
 */

interface E {
  MOSSAIC_USER: DurableObjectNamespace;
}
const E = env as unknown as E;
const NS = "default";

interface QuotaRow {
  storage_used: number;
  file_count: number;
  pool_size: number;
  inline_bytes_used: number;
}

async function readQuota(
  stub: DurableObjectStub,
  userId: string
): Promise<QuotaRow> {
  return runInDurableObject(stub, async (_inst, state) => {
    const row = state.storage.sql
      .exec(
        `SELECT storage_used, file_count, pool_size,
                COALESCE(inline_bytes_used, 0) AS inline_bytes_used
           FROM quota WHERE user_id = ?`,
        userId
      )
      .toArray()[0] as QuotaRow | undefined;
    return (
      row ?? {
        storage_used: 0,
        file_count: 0,
        pool_size: 0,
        inline_bytes_used: 0,
      }
    );
  });
}

describe("Phase 32.5 \u2014 quota desync correction", () => {
  it("QD1 \u2014 inline write+unlink balances storage_used + file_count", async () => {
    const tenant = "qd1-inline-balance";
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
    );
    const scope = { ns: NS, tenant };

    const payload = new Uint8Array(2048).fill(1); // < INLINE_LIMIT
    await stub.vfsWriteFile(scope, "/inline.bin", payload);

    const after = await readQuota(stub, tenant);
    expect(after.storage_used).toBe(2048);
    expect(after.file_count).toBe(1);
    expect(after.inline_bytes_used).toBe(2048);

    await stub.vfsUnlink(scope, "/inline.bin");

    const final = await readQuota(stub, tenant);
    expect(final.storage_used).toBe(0);
    expect(final.file_count).toBe(0);
    expect(final.inline_bytes_used).toBe(0);
  });

  it("QD2 \u2014 chunked write+unlink balances storage_used + file_count", async () => {
    const tenant = "qd2-chunked-balance";
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
    );
    const scope = { ns: NS, tenant };

    // > INLINE_LIMIT (16 KiB) \u2192 chunked tier.
    const payload = new Uint8Array(64 * 1024).fill(2);
    await stub.vfsWriteFile(scope, "/chunked.bin", payload);

    const after = await readQuota(stub, tenant);
    expect(after.storage_used).toBe(64 * 1024);
    expect(after.file_count).toBe(1);
    // Chunked tier doesn't bump inline counter.
    expect(after.inline_bytes_used).toBe(0);

    await stub.vfsUnlink(scope, "/chunked.bin");

    const final = await readQuota(stub, tenant);
    expect(final.storage_used).toBe(0);
    expect(final.file_count).toBe(0);
    expect(final.inline_bytes_used).toBe(0);
  });

  it("QD3 \u2014 rmrf of N files decrements storage_used + file_count in batch", async () => {
    const tenant = "qd3-rmrf-batch";
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
    );
    const scope = { ns: NS, tenant };

    await stub.vfsMkdir(scope, "/dir");
    const N = 7;
    const fileSize = 4096; // inline tier
    for (let i = 0; i < N; i++) {
      const data = new Uint8Array(fileSize).fill(i + 1);
      await stub.vfsWriteFile(scope, `/dir/f${i}.bin`, data);
    }

    const before = await readQuota(stub, tenant);
    expect(before.file_count).toBe(N);
    expect(before.storage_used).toBe(N * fileSize);
    expect(before.inline_bytes_used).toBe(N * fileSize);

    // rmrf the whole subtree.
    let done = false;
    let safety = 50;
    while (!done && safety-- > 0) {
      const r = await stub.vfsRemoveRecursive(scope, "/dir");
      done = r.done;
    }
    expect(done).toBe(true);

    const after = await readQuota(stub, tenant);
    expect(after.storage_used).toBe(0);
    expect(after.file_count).toBe(0);
    expect(after.inline_bytes_used).toBe(0);
  });

  it("QD4 \u2014 pool size grows on write, never shrinks on delete (Lean monotonicity)", async () => {
    const tenant = "qd4-pool-monotonic";
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
    );
    const scope = { ns: NS, tenant };

    // Force the pool to grow without writing 5 GB of real data:
    // touch the quota row directly to simulate a tenant who has
    // already crossed enough write boundaries.
    await stub.vfsExists(scope, "/"); // ensureInit
    await runInDurableObject(stub, async (_inst, state) => {
      state.storage.sql.exec(
        `INSERT OR IGNORE INTO quota
           (user_id, storage_used, storage_limit, file_count, pool_size)
         VALUES (?, 0, 107374182400, 0, 33)`,
        tenant
      );
      state.storage.sql.exec(
        "UPDATE quota SET pool_size = 33 WHERE user_id = ?",
        tenant
      );
    });

    const baseline = await readQuota(stub, tenant);
    expect(baseline.pool_size).toBe(33);

    // Write something, then delete it. Pool MUST stay at 33 even
    // though storage_used returns to 0.
    const payload = new Uint8Array(1024).fill(5);
    await stub.vfsWriteFile(scope, "/transient.bin", payload);
    await stub.vfsUnlink(scope, "/transient.bin");

    const final = await readQuota(stub, tenant);
    expect(final.pool_size).toBe(33);
    expect(final.storage_used).toBe(0);
    expect(final.file_count).toBe(0);
  });

  it("QD5 \u2014 overwrite at same path balances to single file_count + final size", async () => {
    const tenant = "qd5-overwrite-balance";
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
    );
    const scope = { ns: NS, tenant };

    const a = new Uint8Array(1024).fill(0xaa); // 1 KiB
    const b = new Uint8Array(4096).fill(0xbb); // 4 KiB
    await stub.vfsWriteFile(scope, "/x.bin", a);
    const afterA = await readQuota(stub, tenant);
    expect(afterA.storage_used).toBe(1024);
    expect(afterA.file_count).toBe(1);
    expect(afterA.inline_bytes_used).toBe(1024);

    // Overwrite \u2014 commitRename flips status='deleted' on prior
    // row then hardDeleteFileRow runs. Pre-BUG-#1-fix: counter
    // skipped decrement because gate was `status === 'complete'`.
    // Post-fix: gate is `status !== 'uploading'`, so the prior
    // row's bytes are properly subtracted.
    await stub.vfsWriteFile(scope, "/x.bin", b);

    const afterB = await readQuota(stub, tenant);
    // file_count must be 1 (one path; overwrite doesn't double-count)
    expect(afterB.file_count).toBe(1);
    // storage_used must reflect ONLY the new 4 KiB, not 5 KiB sum
    expect(afterB.storage_used).toBe(4096);
    expect(afterB.inline_bytes_used).toBe(4096);
  });
});
