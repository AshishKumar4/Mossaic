import { describe, it, expect } from "vitest";
import { env, runInDurableObject } from "cloudflare:test";
import { vfsUserDOName } from "@core/lib/utils";
import { createVFS, type MossaicEnv, type UserDO } from "../../sdk/src/index";

/**
 * Search-index reconciler (Phase 23 Blindspot fix).
 *
 * Pre-fix flow:
 *   1. Multipart finalize commits a file row (status='complete').
 *   2. SPA fires POST /api/index/file (waitUntil → indexFile).
 *   3. If the SPA crashes between (1) and (2), the file is in VFS
 *      but never search-indexed. Silent miss.
 *
 * Post-fix:
 *   - `files.indexed_at` column tracks "indexed" timestamp.
 *   - `indexFile` stamps `indexed_at` on success via
 *     `appMarkFileIndexed`.
 *   - `appListUnindexedFiles` returns the NULL set.
 *   - `reconcileUnindexedFiles` (in `worker/app/routes/search.ts`)
 *     sweeps the set and re-fires `indexFile`.
 *
 * This test pins the column maintenance + listing behaviour. End-to-end
 * search behaviour is covered indirectly by existing search suites.
 */

interface E {
  MOSSAIC_USER: DurableObjectNamespace<UserDO>;
}
const E = env as unknown as E;
const NS = "default";

function makeEnv(): MossaicEnv {
  return { MOSSAIC_USER: E.MOSSAIC_USER as MossaicEnv["MOSSAIC_USER"] };
}

describe("indexed_at reconciler primitives", () => {
  it("schema has indexed_at column on files (idempotent ALTER)", async () => {
    const tenant = "ir-schema";
    const vfs = createVFS(makeEnv(), { tenant });
    await vfs.writeFile("/seed.txt", "hello");

    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
    );
    const present = await runInDurableObject(stub, async (_inst, state) => {
      const cols = state.storage.sql
        .exec("PRAGMA table_info(files)")
        .toArray() as { name: string }[];
      return cols.some((c) => c.name === "indexed_at");
    });
    expect(present).toBe(true);
  });

  it("appMarkFileIndexed sets indexed_at; appListUnindexedFiles excludes the file", async () => {
    const tenant = "ir-mark";
    const vfs = createVFS(makeEnv(), { tenant });
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
    );

    // Seed two files via canonical writeFile.
    await vfs.writeFile("/a.txt", "alpha");
    await vfs.writeFile("/b.txt", "beta");

    // Both should be unindexed initially.
    const before = await stub.appListUnindexedFiles(tenant, 10);
    expect(before.length).toBe(2);
    const names = before.map((r) => r.file_name).sort();
    expect(names).toEqual(["a.txt", "b.txt"]);

    // Mark one as indexed.
    await stub.appMarkFileIndexed(before[0].file_id);

    // Only the unmarked one is left.
    const after = await stub.appListUnindexedFiles(tenant, 10);
    expect(after.length).toBe(1);
    expect(after[0].file_id).toBe(before[1].file_id);

    // indexed_at column populated for the marked file.
    const stamp = await runInDurableObject(stub, async (_inst, state) => {
      const r = state.storage.sql
        .exec("SELECT indexed_at FROM files WHERE file_id = ?", before[0].file_id)
        .toArray()[0] as { indexed_at: number | null };
      return r.indexed_at;
    });
    expect(typeof stamp).toBe("number");
    expect(stamp).toBeGreaterThan(0);
  });

  it("appListUnindexedFiles respects the limit and oldest-first ordering", async () => {
    const tenant = "ir-limit";
    const vfs = createVFS(makeEnv(), { tenant });
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
    );

    // Write 5 files in known order.
    for (let i = 0; i < 5; i++) {
      await vfs.writeFile(`/f${i}.txt`, `n=${i}`);
    }

    // Limit = 3 returns the 3 oldest.
    const got = await stub.appListUnindexedFiles(tenant, 3);
    expect(got.length).toBe(3);
    expect(got.map((r) => r.file_name)).toEqual(["f0.txt", "f1.txt", "f2.txt"]);
  });

  it("limit is clamped to [1, 100]", async () => {
    const tenant = "ir-clamp";
    const vfs = createVFS(makeEnv(), { tenant });
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
    );
    await vfs.writeFile("/one.txt", "x");

    // 0 → clamped to 1, returns 1 row.
    const zero = await stub.appListUnindexedFiles(tenant, 0);
    expect(zero.length).toBe(1);

    // 9999 → clamped to 100, but we only have 1 file.
    const huge = await stub.appListUnindexedFiles(tenant, 9999);
    expect(huge.length).toBe(1);
  });

  it("deleted files are not returned (status filter)", async () => {
    const tenant = "ir-deleted";
    const vfs = createVFS(makeEnv(), { tenant });
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
    );
    await vfs.writeFile("/keep.txt", "x");
    await vfs.writeFile("/gone.txt", "y");
    await vfs.unlink("/gone.txt");

    const got = await stub.appListUnindexedFiles(tenant, 10);
    // unlink supersedes the file row (sets status='deleted' or
    // hard-deletes depending on versioning); either way it's not
    // returned by the reconciler.
    const names = got.map((r) => r.file_name);
    expect(names).toContain("keep.txt");
    expect(names).not.toContain("gone.txt");
  });
});

/**
 * Phase 26 — extended reconciler invariants (audit gap G9).
 *
 * The pre-existing 5 tests above pin the column primitives. These
 * additional cases cover the END-TO-END reconciliation flow that
 * the production fix (Phase 23 Blindspot) shipped:
 *
 *   IR-E1. Write succeeds but the inline indexFile call fails (or
 *          is never fired). The reconciler enumeration MUST pick
 *          the row up.
 *   IR-E2. Delete (unlink) BETWEEN index+search consistency: a row
 *          marked indexed_at then deleted MUST be excluded from
 *          the unindexed sweep AND the search-side hit set
 *          (bulk-delete consistency).
 *   IR-E3. Bulk-delete: 50 files, mark all indexed, unlink all,
 *          assert the unindexed sweep returns []. Pin that the
 *          reconciler does not emit re-index work for already-
 *          deleted rows.
 *   IR-E4. After the reconciler stamps a row, a subsequent write
 *          to the SAME path (versioning OFF — overwrite) MUST clear
 *          the indexed_at stamp so the new content gets re-indexed.
 *          (If the stamp were sticky, edits would never re-index.)
 */
describe("indexed_at reconciler — end-to-end flow (Phase 26 / G9)", () => {
  it("IR-E1 — file with NULL indexed_at (write-succeeded, index-failed) shows up in the unindexed sweep", async () => {
    const tenant = "ir-write-fail";
    const vfs = createVFS(makeEnv(), { tenant });
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
    );
    await vfs.writeFile("/orphan.txt", "stale-index");

    // Simulate the production failure: indexFile crashed → indexed_at
    // remained NULL. The row IS in the VFS but NOT yet searched.
    // Drive the reconciler enumeration directly.
    const got = await stub.appListUnindexedFiles(tenant, 10);
    expect(got.find((r) => r.file_name === "orphan.txt")).toBeTruthy();

    // After the reconciler successfully fires indexFile and calls
    // appMarkFileIndexed, the row drops from the list.
    const target = got.find((r) => r.file_name === "orphan.txt")!;
    await stub.appMarkFileIndexed(target.file_id);
    const after = await stub.appListUnindexedFiles(tenant, 10);
    expect(after.find((r) => r.file_name === "orphan.txt")).toBeUndefined();
  });

  it("IR-E2 — delete between index+search: an indexed-then-unlinked row is absent from BOTH unindexed sweep and live state", async () => {
    const tenant = "ir-delete-between";
    const vfs = createVFS(makeEnv(), { tenant });
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
    );
    await vfs.writeFile("/transient.txt", "x");

    // 1) Reconciler fires, indexes the row.
    const before = await stub.appListUnindexedFiles(tenant, 10);
    const target = before.find((r) => r.file_name === "transient.txt")!;
    await stub.appMarkFileIndexed(target.file_id);

    // 2) Race: row is unlinked AFTER index but BEFORE the next read.
    await vfs.unlink("/transient.txt");

    // 3) Unindexed sweep MUST not return this row (status filter).
    const got = await stub.appListUnindexedFiles(tenant, 10);
    expect(got.find((r) => r.file_name === "transient.txt")).toBeUndefined();

    // 4) Live state confirms exists=false.
    expect(await vfs.exists("/transient.txt")).toBe(false);
  });

  it("IR-E3 — bulk delete reconciliation: 20 files marked indexed → all unlinked → sweep returns []", async () => {
    const tenant = "ir-bulk-delete";
    const vfs = createVFS(makeEnv(), { tenant });
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
    );
    const N = 20;
    for (let i = 0; i < N; i++) {
      await vfs.writeFile(`/bulk-${i}.txt`, `n=${i}`);
    }

    // Mark all indexed.
    const ids = await stub.appListUnindexedFiles(tenant, N + 5);
    expect(ids.length).toBe(N);
    for (const r of ids) {
      await stub.appMarkFileIndexed(r.file_id);
    }
    const mid = await stub.appListUnindexedFiles(tenant, N + 5);
    expect(mid.length).toBe(0);

    // Unlink everything.
    for (let i = 0; i < N; i++) {
      await vfs.unlink(`/bulk-${i}.txt`);
    }

    // Sweep returns nothing — neither indexed-and-deleted rows nor
    // tombstones leak into the reconciler's unindexed enumeration.
    const sweep = await stub.appListUnindexedFiles(tenant, N + 5);
    expect(sweep.length).toBe(0);
  });

  it("IR-E4 — overwrite re-clears indexed_at: the new content gets re-indexed (no stale-stamp hide)", async () => {
    // Pin actual semantics observed under workerd: the writeFile
    // path on a versioning-OFF tenant either inserts a new row OR
    // updates the existing row's content + clears indexed_at. The
    // reconciler MUST see the row again after overwrite — otherwise
    // edits would never be re-indexed. (If a future change moved
    // to "stamp is sticky", users would silently lose search on
    // edited content.)
    const tenant = "ir-overwrite-reindex";
    const vfs = createVFS(makeEnv(), { tenant });
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
    );
    await vfs.writeFile("/edit.txt", "v1");

    const before = await stub.appListUnindexedFiles(tenant, 10);
    const target = before.find((r) => r.file_name === "edit.txt")!;
    await stub.appMarkFileIndexed(target.file_id);

    // Confirm the stamp landed.
    const mid = await stub.appListUnindexedFiles(tenant, 10);
    expect(mid.find((r) => r.file_name === "edit.txt")).toBeUndefined();

    // Overwrite. The new content needs re-indexing.
    await vfs.writeFile("/edit.txt", "v2-edited-content");
    const after = await stub.appListUnindexedFiles(tenant, 10);
    expect(after.find((r) => r.file_name === "edit.txt")).toBeTruthy();
  });
});
