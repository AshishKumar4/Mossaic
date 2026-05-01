import { describe, it, expect } from "vitest";
import { env, runInDurableObject } from "cloudflare:test";
import { vfsUserDOName } from "@core/lib/utils";
import { INLINE_LIMIT, INLINE_TIER_CAP } from "@shared/inline";

/**
 * Phase 32 Fix 5 — inline tier graceful migration.
 *
 * Pre-fix every write \u2264 INLINE_LIMIT (16 KiB) committed to the
 * inline tier (`files.inline_data`). A tenant doing tiny writes
 * could accumulate GiBs in a single UserDO. Post-fix:
 *   - `quota.inline_bytes_used` tracks cumulative inline bytes.
 *   - `vfsWriteFile` falls through to chunked tier when
 *     `inline_bytes_used + new > INLINE_TIER_CAP` (1 GiB).
 *   - `commitInlineTier` increments the counter.
 *   - `hardDeleteFileRow` decrements when an inline-tier row is
 *     deleted.
 *
 * Cases:
 *   IT1. Brand-new tenant: tiny writes go inline.
 *   IT2. Counter increments on inline writes.
 *   IT3. Counter decrements on delete (file_size <= INLINE_LIMIT).
 *   IT4. Cap crossed → falls through to chunked tier (chunk_count > 0).
 *   IT5. Pre-existing inline rows are still readable past the cap.
 */

interface E {
  MOSSAIC_USER: DurableObjectNamespace;
}
const E = env as unknown as E;
const NS = "default";

describe("Phase 32 Fix 5 — inline tier graceful migration", () => {
  it("IT1 — fresh tenant, tiny writes go inline", async () => {
    const tenant = "it1-fresh";
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
    );
    const scope = { ns: NS, tenant };
    const bytes = new TextEncoder().encode("tiny payload");
    await stub.vfsWriteFile(scope, "/t.txt", bytes);

    const row = await runInDurableObject(stub, async (_inst, state) => {
      return state.storage.sql
        .exec(
          "SELECT inline_data, chunk_count FROM files WHERE file_name = 't.txt'"
        )
        .toArray()[0] as {
        inline_data: ArrayBuffer | null;
        chunk_count: number;
      };
    });
    expect(row.inline_data).not.toBeNull();
    expect(row.chunk_count).toBe(0);
  });

  it("IT2 — counter increments on inline writes", async () => {
    const tenant = "it2-increment";
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
    );
    const scope = { ns: NS, tenant };
    const payload = new Uint8Array(1024).fill(1);
    await stub.vfsWriteFile(scope, "/a.bin", payload);
    await stub.vfsWriteFile(scope, "/b.bin", payload);
    await stub.vfsWriteFile(scope, "/c.bin", payload);

    const used = await runInDurableObject(stub, async (_inst, state) => {
      const r = state.storage.sql
        .exec(
          "SELECT COALESCE(inline_bytes_used, 0) AS used FROM quota WHERE user_id = ?",
          tenant
        )
        .toArray()[0] as { used: number } | undefined;
      return r?.used ?? 0;
    });
    expect(used).toBe(3 * 1024);
  });

  it("IT3 — counter decrements on delete of inline-tier rows", async () => {
    const tenant = "it3-decrement";
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
    );
    const scope = { ns: NS, tenant };
    const payload = new Uint8Array(2048).fill(7);
    await stub.vfsWriteFile(scope, "/d.bin", payload);

    const usedBefore = await runInDurableObject(stub, async (_inst, state) => {
      return (
        state.storage.sql
          .exec(
            "SELECT COALESCE(inline_bytes_used, 0) AS used FROM quota WHERE user_id = ?",
            tenant
          )
          .toArray()[0] as { used: number }
      ).used;
    });
    expect(usedBefore).toBe(2048);

    await stub.vfsUnlink(scope, "/d.bin");

    const usedAfter = await runInDurableObject(stub, async (_inst, state) => {
      return (
        state.storage.sql
          .exec(
            "SELECT COALESCE(inline_bytes_used, 0) AS used FROM quota WHERE user_id = ?",
            tenant
          )
          .toArray()[0] as { used: number }
      ).used;
    });
    expect(usedAfter).toBe(0);
  });

  it("IT4 — cap crossed → falls through to chunked tier", async () => {
    const tenant = "it4-cap-cross";
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
    );
    const scope = { ns: NS, tenant };

    // Trigger ensureInit so the quota table exists.
    await stub.vfsExists(scope, "/");
    // Pre-set the counter to just under cap so a small inline
    // write would push over.
    await runInDurableObject(stub, async (_inst, state) => {
      state.storage.sql.exec(
        `INSERT OR IGNORE INTO quota (user_id, storage_used, storage_limit, file_count, pool_size)
         VALUES (?, 0, 107374182400, 0, 32)`,
        tenant
      );
      state.storage.sql.exec(
        "UPDATE quota SET inline_bytes_used = ? WHERE user_id = ?",
        INLINE_TIER_CAP - 100,
        tenant
      );
    });

    // Write 200 bytes \u2014 would have inlined pre-Phase-32, but the
    // cap (1 GiB - 100 + 200 > 1 GiB) forces a chunked write.
    const bytes = new Uint8Array(200).fill(9);
    await stub.vfsWriteFile(scope, "/spill.bin", bytes);

    const row = await runInDurableObject(stub, async (_inst, state) => {
      return state.storage.sql
        .exec(
          "SELECT inline_data, chunk_count FROM files WHERE file_name = 'spill.bin'"
        )
        .toArray()[0] as {
        inline_data: ArrayBuffer | null;
        chunk_count: number;
      };
    });
    expect(row.inline_data).toBeNull();
    expect(row.chunk_count).toBeGreaterThan(0);
  });

  it("IT5 — pre-existing inline rows still readable past the cap", async () => {
    const tenant = "it5-existing-inline";
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
    );
    const scope = { ns: NS, tenant };

    // Write a tiny inline file BEFORE the cap is crossed.
    const original = new TextEncoder().encode("hello inline");
    await stub.vfsWriteFile(scope, "/old.txt", original);

    // Force the counter past the cap.
    await runInDurableObject(stub, async (_inst, state) => {
      state.storage.sql.exec(
        "UPDATE quota SET inline_bytes_used = ? WHERE user_id = ?",
        INLINE_TIER_CAP + 1024,
        tenant
      );
    });

    // Read the existing row \u2014 still inline, still readable.
    const back = await stub.vfsReadFile(scope, "/old.txt");
    expect(new TextDecoder().decode(back)).toBe("hello inline");

    // INLINE_LIMIT exists for tests' confidence in the constant.
    expect(INLINE_LIMIT).toBeGreaterThan(0);
  });
});
