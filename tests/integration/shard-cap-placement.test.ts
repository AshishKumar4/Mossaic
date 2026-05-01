import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { env, runInDurableObject } from "cloudflare:test";
import type { UserDO } from "@app/objects/user/user-do";
import { vfsUserDOName, vfsShardDOName } from "@core/lib/utils";
import {
  monitorShardCapacity,
  SHARD_SOFT_CAP_BYTES,
} from "@core/objects/user/shard-capacity";
import type { ShardDO } from "@core/objects/shard/shard-do";

/**
 * Phase 28 Fix 4 — shard capacity WARNING (deferred cap-aware
 * placement → Phase 28.1).
 *
 * The current implementation is signal-only: when any shard reports
 * `bytesStored >= SHARD_SOFT_CAP_BYTES` (9 GB), the alarm-driven
 * `monitorShardCapacity` poll logs a structured warning. The
 * eventual placement-skip is documented as a TODO at
 * `shared/placement.ts`'s `placeChunk`.
 *
 * Cases:
 *   SC1. SHARD_SOFT_CAP_BYTES is 9 GB.
 *   SC2. monitorShardCapacity returns a snapshot per shard in the
 *        pool (forced — bypasses throttle).
 *   SC3. Snapshot marks `exceedsCap = true` on shards >= softCap.
 *   SC4. Throttle prevents repeated polls within 1h.
 *   SC5. Warning is logged (console.warn) for each over-cap shard.
 */

interface E {
  MOSSAIC_USER: DurableObjectNamespace<UserDO>;
  MOSSAIC_SHARD: DurableObjectNamespace<ShardDO>;
}
const E = env as unknown as E;
const NS = "default";

function userStub(tenant: string) {
  return E.MOSSAIC_USER.get(
    E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
  );
}

describe("Phase 28 Fix 4 — shard capacity warning", () => {
  let consoleWarnSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
    consoleWarnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
  });

  afterEach(() => {
    consoleWarnSpy.mockRestore();
  });

  it("SC1 — SHARD_SOFT_CAP_BYTES is 9 GB", () => {
    expect(SHARD_SOFT_CAP_BYTES).toBe(9 * 1024 * 1024 * 1024);
  });

  it("SC2 — monitorShardCapacity returns one snapshot per shard in the pool", async () => {
    const tenant = "sc2-snapshot";
    const stub = userStub(tenant);
    const scope = { ns: NS, tenant };
    await stub.vfsExists(scope, "/seed");

    const snaps = await runInDurableObject(stub, async (inst) => {
      return monitorShardCapacity(inst as never, scope, 4, { force: true });
    });
    expect(snaps.length).toBe(4);
    for (let i = 0; i < 4; i++) {
      expect(snaps[i]!.shardIndex).toBe(i);
      expect(snaps[i]!.exceedsCap).toBe(false);
    }
  });

  it("SC3 — snapshot marks exceedsCap=true when shard reports >= softCap", async () => {
    const tenant = "sc3-exceeds";
    const stub = userStub(tenant);
    const scope = { ns: NS, tenant };
    await stub.vfsExists(scope, "/seed");

    // Force shard #2 to report >= softCap by directly seeding its
    // chunks table from a runInDurableObject context.
    const shardName = vfsShardDOName(NS, tenant, undefined, 2);
    const shardStub = E.MOSSAIC_SHARD.get(
      E.MOSSAIC_SHARD.idFromName(shardName)
    );
    // Trigger the shard's ensureInit via a no-op RPC.
    await shardStub.getStorageBytes();
    await runInDurableObject(shardStub, async (_inst, state) => {
      // Insert one fake chunk row whose `size` is at the soft cap.
      // The chunks data BLOB stays small (1 byte) so this doesn't
      // consume real storage; what matters is the SUM(size) the
      // monitor reads.
      state.storage.sql.exec(
        `INSERT INTO chunks (hash, data, size, ref_count, created_at)
         VALUES ('h-sc3-fake', X'00', ?, 1, ?)`,
        SHARD_SOFT_CAP_BYTES,
        Date.now()
      );
    });

    const snaps = await runInDurableObject(stub, async (inst) => {
      return monitorShardCapacity(inst as never, scope, 4, { force: true });
    });
    const offender = snaps.find((s) => s.shardIndex === 2);
    expect(offender).toBeTruthy();
    expect(offender!.bytesStored).toBeGreaterThanOrEqual(SHARD_SOFT_CAP_BYTES);
    expect(offender!.exceedsCap).toBe(true);
    // Other shards should NOT be flagged.
    for (const s of snaps) {
      if (s.shardIndex !== 2) {
        expect(s.exceedsCap).toBe(false);
      }
    }
  });

  it("SC4 — throttle prevents repeated polls within 1h", async () => {
    const tenant = "sc4-throttle";
    const stub = userStub(tenant);
    const scope = { ns: NS, tenant };
    await stub.vfsExists(scope, "/seed");

    // First call (forced) — does the work, stamps the timestamp.
    const first = await runInDurableObject(stub, async (inst) => {
      return monitorShardCapacity(inst as never, scope, 2, { force: true });
    });
    expect(first.length).toBe(2);

    // Second call (no force) — within the 1h window, returns empty.
    const second = await runInDurableObject(stub, async (inst) => {
      return monitorShardCapacity(inst as never, scope, 2);
    });
    expect(second.length).toBe(0);

    // Third call WITH force — does the work again.
    const third = await runInDurableObject(stub, async (inst) => {
      return monitorShardCapacity(inst as never, scope, 2, { force: true });
    });
    expect(third.length).toBe(2);
  });

  it("SC5 — over-cap shard triggers a structured warning log", async () => {
    const tenant = "sc5-warning";
    const stub = userStub(tenant);
    const scope = { ns: NS, tenant };
    await stub.vfsExists(scope, "/seed");

    // Seed shard #0 at the soft cap.
    const shardName = vfsShardDOName(NS, tenant, undefined, 0);
    const shardStub = E.MOSSAIC_SHARD.get(
      E.MOSSAIC_SHARD.idFromName(shardName)
    );
    await shardStub.getStorageBytes();
    await runInDurableObject(shardStub, async (_inst, state) => {
      state.storage.sql.exec(
        `INSERT INTO chunks (hash, data, size, ref_count, created_at)
         VALUES ('h-sc5-fake', X'00', ?, 1, ?)`,
        SHARD_SOFT_CAP_BYTES,
        Date.now()
      );
    });

    await runInDurableObject(stub, async (inst) => {
      await monitorShardCapacity(inst as never, scope, 1, { force: true });
    });

    // Find a structured warning emitted for this tenant + shard.
    const calls = consoleWarnSpy.mock.calls.map((c) => String(c[0] ?? ""));
    const matching = calls.find(
      (line) =>
        line.includes("shard_capacity_soft_cap_exceeded") &&
        line.includes(tenant) &&
        line.includes('"shardIndex":0')
    );
    expect(matching).toBeTruthy();
    // The structured payload includes the soft-cap value + phase tag
    // for downstream filtering.
    // Phase 32 Fix 4 \u2014 the phase tag flipped from `28-warning-only`
    // to `32-cap-aware` when placement actually started skipping
    // full shards. The structured-warning shape is otherwise
    // unchanged; Logpush queries that filtered on
    // `event:shard_capacity_soft_cap_exceeded` continue to fire.
    expect(matching).toContain('"phase":"32-cap-aware"');
    expect(matching).toContain('"softCapBytes"');
  });
});
