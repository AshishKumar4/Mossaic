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
