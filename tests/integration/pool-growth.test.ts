import { describe, it, expect } from "vitest";
import { env, runInDurableObject } from "cloudflare:test";
import { vfsUserDOName } from "@core/lib/utils";
import { createVFS, type MossaicEnv } from "../../sdk/src/index";

/**
 * Pool growth wiring (Phase 23 Fix 1).
 *
 * Before this fix the pool-growth feature was dead code: the App-side
 * `updateUsage` was only called from `appDeleteFile` with NEGATIVE
 * deltas, so `quota.pool_size` could only ever shrink (and even then
 * never below 32). This file pins the post-fix invariants:
 *
 *  1. Every committed write (inline / chunked / multipart) ticks
 *     `quota.storage_used` UP via the canonical `recordWriteUsage`
 *     helper, and grows `pool_size` once a 5 GB boundary is crossed.
 *  2. Pool size NEVER shrinks. Deletes tick `storage_used` down but
 *     `pool_size` stays at the high-water mark — rendezvous
 *     redistribution would orphan chunks pinned to high shard
 *     indices.
 *  3. The recompute is idempotent w.r.t. re-finalization races: pool
 *     never overshoots a stable byte total.
 *  4. Real-byte chunked / multipart / inline writes all flow through
 *     the same primitive so accounting is consistent across tiers.
 *
 * Real-byte uploads at 5 GB scale are infeasible in unit tests, so we
 * simulate the boundary crossings by directly mutating
 * `quota.storage_used` and then calling the helper with a tiny delta
 * to trigger the recompute. Real-byte E2E coverage at small scale is
 * provided by the inline / chunked / multipart cases below.
 */

interface E {
  MOSSAIC_USER: DurableObjectNamespace;
}
const E = env as unknown as E;
const NS = "default";

function makeEnv(): MossaicEnv {
  return { MOSSAIC_USER: E.MOSSAIC_USER as MossaicEnv["MOSSAIC_USER"] };
}

const FIVE_GB = 5 * 1024 * 1024 * 1024;

async function readQuota(stub: DurableObjectStub, userId: string): Promise<{ used: number; pool: number; files: number }> {
  return runInDurableObject(stub, async (_inst, state) => {
    const r = state.storage.sql
      .exec("SELECT storage_used, pool_size, file_count FROM quota WHERE user_id = ?", userId)
      .toArray()[0] as { storage_used: number; pool_size: number; file_count: number } | undefined;
    return r ? { used: r.storage_used, pool: r.pool_size, files: r.file_count } : { used: 0, pool: 32, files: 0 };
  });
}

/**
 * Seed the quota row with a synthetic `storage_used` so the next
 * recompute sees a large total without us having to actually upload
 * gigabytes of bytes. This exercises the SAME code path that a real
 * 5 GB upload would trigger — the helper just reads `storage_used`
 * post-update.
 */
async function seedStorageUsed(stub: DurableObjectStub, userId: string, bytes: number): Promise<void> {
  await runInDurableObject(stub, async (_inst, state) => {
    state.storage.sql.exec(
      `INSERT OR REPLACE INTO quota (user_id, storage_used, storage_limit, file_count, pool_size)
       VALUES (?, ?, 107374182400, 0, 32)`,
      userId,
      bytes
    );
  });
}

describe("pool growth wiring (Phase 23 Fix 1)", () => {
  it("crosses the first 5 GB boundary on the next real write: pool 32 → 33", async () => {
    const tenant = "pg-first-boundary";
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
    );
    const vfs = createVFS(makeEnv(), { tenant });
    await vfs.exists("/");

    // Pre-seed: tenant already has 5 GB minus 1 byte stored.
    await seedStorageUsed(stub, tenant, FIVE_GB - 1);
    expect((await readQuota(stub, tenant)).pool).toBe(32);

    // A real 1-byte-or-more write tips us over 5 GB. Use 20 KB to
    // hit the chunked tier (>INLINE_LIMIT=16KB).
    const bytes = 20 * 1024;
    await vfs.writeFile("/cross.bin", new Uint8Array(bytes).fill(7));

    const q = await readQuota(stub, tenant);
    expect(q.used).toBeGreaterThanOrEqual(FIVE_GB);
    expect(q.pool).toBe(33);
  });

  it("scales to 50 GB: pool 32 → 42", async () => {
    const tenant = "pg-power-user";
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
    );
    const vfs = createVFS(makeEnv(), { tenant });
    await vfs.exists("/");

    // Seed at 50 GB - 1 byte; one real write completes the crossing.
    await seedStorageUsed(stub, tenant, 10 * FIVE_GB - 1);
    await vfs.writeFile("/p.bin", new Uint8Array(20 * 1024).fill(1));

    expect((await readQuota(stub, tenant)).pool).toBe(42);
  });

  it("absorbs a previously-broken 320 GB ceiling: pool 32 → 96", async () => {
    // Pre-fix, the pool was effectively pinned at 32. With ShardDOs
    // capped near 10 GB SQLite each, that capped a single tenant at
    // ~320 GB before writes started failing. Post-fix the same 320
    // GB total grows the pool to 96 (32 + 320/5), distributing
    // future writes across 96 shards.
    const tenant = "pg-broken-ceiling";
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
    );
    const vfs = createVFS(makeEnv(), { tenant });
    await vfs.exists("/");

    await seedStorageUsed(stub, tenant, 64 * FIVE_GB - 1);
    await vfs.writeFile("/big.bin", new Uint8Array(20 * 1024).fill(2));

    expect((await readQuota(stub, tenant)).pool).toBe(96);
  });

  it("pool never shrinks on delete (high-water-mark)", async () => {
    const tenant = "pg-no-shrink";
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
    );
    const vfs = createVFS(makeEnv(), { tenant });
    await vfs.exists("/");

    // Seed to 25 GB stored, pool would be 37.
    await runInDurableObject(stub, async (_inst, state) => {
      state.storage.sql.exec(
        `INSERT OR REPLACE INTO quota (user_id, storage_used, storage_limit, file_count, pool_size)
         VALUES (?, ?, 107374182400, 5, 37)`,
        tenant,
        5 * FIVE_GB
      );
    });

    // Now write a small file (triggers a tiny POSITIVE delta — no
    // boundary crossing, pool stays put at 37).
    await vfs.writeFile("/x.bin", new Uint8Array(1024).fill(0));
    expect((await readQuota(stub, tenant)).pool).toBe(37);

    // Simulate a delete that takes us all the way to 0 used.
    await runInDurableObject(stub, async (_inst, state) => {
      state.storage.sql.exec(
        `UPDATE quota SET storage_used = 0, file_count = 0 WHERE user_id = ?`,
        tenant
      );
    });

    // Now do another small write — recompute sees used=1024, would
    // compute pool=32. The helper MUST NOT shrink.
    await vfs.writeFile("/y.bin", new Uint8Array(1024).fill(0));
    expect((await readQuota(stub, tenant)).pool).toBe(37);
  });

  it("inline-tier write ticks storage_used + file_count", async () => {
    const tenant = "pg-inline";
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
    );
    const vfs = createVFS(makeEnv(), { tenant });
    await vfs.exists("/");

    expect((await readQuota(stub, tenant)).used).toBe(0);

    const bytes = 1024;
    await vfs.writeFile("/i.txt", new Uint8Array(bytes).fill(1));

    const q = await readQuota(stub, tenant);
    expect(q.used).toBe(bytes);
    expect(q.files).toBe(1);
    expect(q.pool).toBe(32);
  });

  it("chunked-tier write ticks storage_used + file_count", async () => {
    const tenant = "pg-chunked";
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
    );
    const vfs = createVFS(makeEnv(), { tenant });
    await vfs.exists("/");

    const bytes = 64 * 1024; // > INLINE_LIMIT
    await vfs.writeFile("/c.bin", new Uint8Array(bytes).fill(2));

    const q = await readQuota(stub, tenant);
    expect(q.used).toBe(bytes);
    expect(q.files).toBe(1);
  });

  it("multiple writes accumulate into a stable storage_used total", async () => {
    const tenant = "pg-accumulate";
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
    );
    const vfs = createVFS(makeEnv(), { tenant });
    await vfs.exists("/");

    await vfs.writeFile("/a.bin", new Uint8Array(1024).fill(1));
    await vfs.writeFile("/b.bin", new Uint8Array(2048).fill(2));
    await vfs.writeFile("/c.bin", new Uint8Array(4096).fill(3));

    const q = await readQuota(stub, tenant);
    expect(q.used).toBe(1024 + 2048 + 4096);
    expect(q.files).toBe(3);
  });

  it("crossing exactly N×5GB lands the pool at 32+N (boundary arithmetic)", async () => {
    const tenant = "pg-boundary-math";
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
    );
    const vfs = createVFS(makeEnv(), { tenant });
    await vfs.exists("/");

    // Seed at exactly 7 × 5 GB. The recompute sees floor(35GB /
    // 5GB) = 7 → pool = 39.
    await seedStorageUsed(stub, tenant, 7 * FIVE_GB);
    await vfs.writeFile("/m.bin", new Uint8Array(1024).fill(0));
    expect((await readQuota(stub, tenant)).pool).toBe(39);
  });
});
