import { describe, it, expect } from "vitest";
import { env, runInDurableObject } from "cloudflare:test";
import { vfsUserDOName } from "@core/lib/utils";

/**
 * Phase 42 \u2014 alarm handler error visibility.
 *
 * Pre-Phase-42 the three bare `catch {}` sites in
 * `user-do-core.ts:alarm()` swallowed every error. Now:
 *   - `logError` emits a structured entry with
 *     `event: alarm_handler_failed` and the failing sweep kind.
 *   - `vfs_meta.alarm_failures` counter increments per failure.
 *   - The alarm continues processing remaining work (no throw).
 *
 * Cases:
 *   AF1 vfs_meta.alarm_failures starts at 0 (or absent) on a
 *       fresh tenant.
 *   AF2 incrementing alarm_failures via the SQL pattern reaches
 *       expected count (validates the ON CONFLICT INTEGER cast
 *       used by recordAlarmFailure).
 *   AF3 alarm() runs to completion with no errors on a clean
 *       tenant; counter stays 0.
 */

interface E {
  MOSSAIC_USER: DurableObjectNamespace;
}
const E = env as unknown as E;
const NS = "default";

async function readAlarmFailures(stub: DurableObjectStub): Promise<number> {
  return runInDurableObject(stub, async (_inst, state) => {
    const r = state.storage.sql
      .exec("SELECT value FROM vfs_meta WHERE key = 'alarm_failures'")
      .toArray()[0] as { value: string } | undefined;
    if (!r) return 0;
    const parsed = Number.parseInt(r.value, 10);
    return Number.isFinite(parsed) ? parsed : 0;
  });
}

describe("Phase 42 \u2014 alarm-failures counter", () => {
  it("AF1 \u2014 fresh tenant: counter is 0 (absent row)", async () => {
    const tenant = "af1-fresh";
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
    );
    const scope = { ns: NS, tenant };
    await stub.vfsExists(scope, "/"); // ensureInit
    const n = await readAlarmFailures(stub);
    expect(n).toBe(0);
  });

  it("AF2 \u2014 SQL UPSERT increments alarm_failures correctly", async () => {
    const tenant = "af2-increment";
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
    );
    const scope = { ns: NS, tenant };
    await stub.vfsExists(scope, "/"); // ensureInit
    // Apply the same UPSERT pattern that recordAlarmFailure uses.
    for (let i = 0; i < 5; i++) {
      await runInDurableObject(stub, async (_inst, state) => {
        state.storage.sql.exec(
          `INSERT INTO vfs_meta (key, value)
           VALUES ('alarm_failures', '1')
           ON CONFLICT(key) DO UPDATE SET value = CAST((CAST(value AS INTEGER) + 1) AS TEXT)`
        );
      });
    }
    const n = await readAlarmFailures(stub);
    expect(n).toBe(5);
  });

  it("AF3 \u2014 alarm() runs cleanly on a clean tenant; counter stays 0", async () => {
    const tenant = "af3-clean";
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
    );
    const scope = { ns: NS, tenant };
    // Persist scope so alarm() finds it.
    await stub.vfsWriteFile(scope, "/seed.bin", new Uint8Array(8).fill(1));
    // Trigger alarm directly (the public path is via the
    // ensureStaleSweepScheduled trigger; in tests we call the
    // method directly).
    await runInDurableObject(stub, async (inst) => {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const alarm = (inst as any).alarm;
      if (typeof alarm === "function") {
        await alarm.call(inst);
      }
    });
    const n = await readAlarmFailures(stub);
    expect(n).toBe(0);
  });
});
