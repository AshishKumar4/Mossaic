import { describe, it, expect } from "vitest";
import { env, runInDurableObject } from "cloudflare:test";

/**
 * P1-8 fix — index reconciler retry cap.
 *
 * Pre-fix `reconcileUnindexedFiles` re-fired `indexFile` on every
 * alarm tick for any row with `indexed_at IS NULL`. A
 * permanently-failing file (corrupted source, unsupported MIME)
 * retried forever, burning AI binding budget every alarm and
 * blocking a slot in the bounded `limit=25` reconciler.
 *
 * The fix adds an `index_attempts` column. The reconciler's
 * catch path calls `appBumpIndexAttempts(fileId)` after each
 * failure. `appListUnindexedFiles` filters
 * `index_attempts < 5`, so once the cap is hit the row is
 * dormant. A `console.error` fires exactly once on the
 * cap-cross transition.
 *
 * Tests pin:
 *   I1 — `index_attempts` column exists with DEFAULT 0.
 *   I2 — appBumpIndexAttempts increments and reports the new count;
 *        capJustHit fires exactly once on the 4→5 transition.
 *   I3 — appListUnindexedFiles excludes rows with attempts >= 5.
 *   I4 — appMarkFileIndexed clears the row from the unindexed
 *        list as before (no regression).
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

/**
 * Seed a `complete` file row directly via SQL — the reconciler
 * surface only needs the row's `indexed_at IS NULL` state, not
 * actual chunks. Fastest path to the test fixture.
 */
async function seedCompleteFile(
  tenant: string,
  fileId: string,
  initialAttempts: number = 0
): Promise<void> {
  const stub = userStub(vfsUserDOName("default", tenant));
  // Ensures schema migration has run.
  await stub.appGetQuota(tenant);
  await runInDurableObject(stub, async (_inst, state) => {
    state.storage.sql.exec(
      `INSERT OR REPLACE INTO files
         (file_id, user_id, parent_id, file_name, file_size, file_hash,
          mime_type, chunk_size, chunk_count, pool_size, status,
          created_at, updated_at, mode, node_kind, index_attempts)
       VALUES (?, ?, NULL, ?, 100, '', 'image/png', 0, 0, 32,
               'complete', ?, ?, 420, 'file', ?)`,
      fileId,
      tenant,
      `${fileId}.png`,
      Date.now(),
      Date.now(),
      initialAttempts
    );
  });
}

describe("index reconciler attempts cap (P1-8)", () => {
  it("I1 — `index_attempts` column exists with DEFAULT 0", async () => {
    const tenant = "idx-cap-i1";
    const stub = userStub(vfsUserDOName("default", tenant));
    await stub.appGetQuota(tenant);
    const cols = await runInDurableObject(stub, async (_inst, state) => {
      return state.storage.sql.exec("PRAGMA table_info(files)").toArray() as {
        name: string;
        dflt_value: string | null;
      }[];
    });
    const col = cols.find((c) => c.name === "index_attempts");
    expect(col).toBeDefined();
    expect(String(col!.dflt_value)).toBe("0");
  });

  it("I2 — appBumpIndexAttempts increments; capJustHit fires once on transition", async () => {
    const tenant = "idx-cap-i2";
    const fileId = "file-i2";
    await seedCompleteFile(tenant, fileId, 0);
    const stub = userStub(vfsUserDOName("default", tenant));

    // Bump 1..4 — none should signal cap-just-hit.
    const r1 = await stub.appBumpIndexAttempts(fileId);
    expect(r1.attempts).toBe(1);
    expect(r1.capJustHit).toBe(false);

    const r2 = await stub.appBumpIndexAttempts(fileId);
    expect(r2.attempts).toBe(2);
    expect(r2.capJustHit).toBe(false);

    await stub.appBumpIndexAttempts(fileId); // 3
    await stub.appBumpIndexAttempts(fileId); // 4

    // Bump #5 hits the cap → capJustHit: true.
    const r5 = await stub.appBumpIndexAttempts(fileId);
    expect(r5.attempts).toBe(5);
    expect(r5.capJustHit).toBe(true);

    // Bump #6 — already past cap; capJustHit stays false.
    const r6 = await stub.appBumpIndexAttempts(fileId);
    expect(r6.attempts).toBe(6);
    expect(r6.capJustHit).toBe(false);
  });

  it("I3 — appListUnindexedFiles excludes rows past the cap", async () => {
    const tenant = "idx-cap-i3";
    // 3 files: under cap, at cap, way past cap.
    await seedCompleteFile(tenant, "file-under", 2);
    await seedCompleteFile(tenant, "file-at-cap", 5);
    await seedCompleteFile(tenant, "file-past-cap", 10);

    const stub = userStub(vfsUserDOName("default", tenant));
    const rows = await stub.appListUnindexedFiles(tenant, 25);
    const ids = rows.map((r) => r.file_id);
    expect(ids).toContain("file-under");
    expect(ids).not.toContain("file-at-cap");
    expect(ids).not.toContain("file-past-cap");
  });

  it("I4 — appMarkFileIndexed still removes rows from unindexed listing", async () => {
    const tenant = "idx-cap-i4";
    await seedCompleteFile(tenant, "file-i4", 2);

    const stub = userStub(vfsUserDOName("default", tenant));
    let rows = await stub.appListUnindexedFiles(tenant, 25);
    expect(rows.map((r) => r.file_id)).toContain("file-i4");

    await stub.appMarkFileIndexed("file-i4");

    rows = await stub.appListUnindexedFiles(tenant, 25);
    expect(rows.map((r) => r.file_id)).not.toContain("file-i4");
  });
});
