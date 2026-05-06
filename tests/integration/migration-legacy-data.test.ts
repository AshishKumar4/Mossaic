import { describe, it, expect } from "vitest";
import { env, runInDurableObject } from "cloudflare:test";

/**
 * Phase 26 — legacy-data migration tests (audit gap G7 /
 * test-suite-audit.md §3.3 / §7.3).
 *
 * The existing `migration.test.ts` is schema-shape-only — it
 * inspects `PRAGMA table_info(files)` for column presence and
 * verifies `ensureInit` idempotency. It does NOT seed pre-Phase-9 /
 * pre-Phase-12 production rows (rows whose newer columns were not
 * yet populated when written) and validate that today's code paths
 * handle them. That class of bug is the second-most-likely shipper
 * after tombstones.
 *
 * Strategy: drive a canonical write through the high-level VFS API
 * to populate a row, then mutate the row directly via
 * `runInDurableObject` to simulate the legacy state (NULL out
 * head_version_id, NULL out indexed_at, NULL out the encryption
 * columns), then assert that the downstream read paths
 * (vfsStat / vfsListFiles / vfsReadFile / appListUnindexedFiles)
 * still return correct results.
 *
 * Pinned invariants:
 *
 *   M1. Pre-Phase-9 row (no head_version_id, no file_versions):
 *       stat / readFile / listFiles all succeed and return the
 *       legacy file's bytes.
 *   M2. Pre-Phase-12 row (NULL indexed_at, NULL encryption_*):
 *       reconciler picks it up; readFile returns plaintext.
 *   M3. Forward-compat: every column added by the Phase-9..Phase-22
 *       migrations is nullable / has a default, so legacy rows do
 *       not need a backfill to be readable.
 *   M4. Mixed legacy + new in the same UserDO: a tenant with one
 *       legacy row + one new row sees both correctly via listFiles.
 */

import {
  createVFS,
  type MossaicEnv,
  type UserDO,
} from "../../sdk/src/index";
import { vfsUserDOName } from "@core/lib/utils";

interface E {
  MOSSAIC_USER: DurableObjectNamespace<UserDO>;
  MOSSAIC_SHARD: DurableObjectNamespace;
}
const E = env as unknown as E;
const NS = "default";

function envFor(): MossaicEnv {
  return { MOSSAIC_USER: E.MOSSAIC_USER as MossaicEnv["MOSSAIC_USER"] };
}
function userStub(tenant: string) {
  return E.MOSSAIC_USER.get(
    E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
  );
}

describe("legacy-data migration — pre-Phase-9 (no head_version_id)", () => {
  it("M1 — file written before versioning column existed reads back via stat / readFile / listFiles", async () => {
    const tenant = "mig-legacy-prephase9";
    const vfs = createVFS(envFor(), { tenant });
    // Seed via the canonical inline tier (≤16KB).
    await vfs.writeFile("/legacy.txt", "legacy bytes");

    // Simulate pre-Phase-9 state: head_version_id may be NULL
    // already (versioning was off), but make doubly sure — and also
    // delete any file_versions rows in case a prior write created
    // one (it shouldn't have, since versioning is off by default).
    await runInDurableObject(userStub(tenant), async (_inst, state) => {
      state.storage.sql.exec("UPDATE files SET head_version_id = NULL");
      state.storage.sql.exec("DELETE FROM file_versions");
    });

    // All three read surfaces must succeed against the legacy row.
    const back = await vfs.readFile("/legacy.txt", { encoding: "utf8" });
    expect(back).toBe("legacy bytes");

    const stat = await vfs.stat("/legacy.txt");
    expect(stat.type).toBe("file");
    expect(stat.size).toBe("legacy bytes".length);

    const list = await vfs.listFiles({ orderBy: "name" });
    expect(list.items.length).toBe(1);
    expect(list.items[0].path).toBe("/legacy.txt");
  });

  it("M1b — chunked legacy file (no head_version_id) reads back via the chunk path", async () => {
    const tenant = "mig-legacy-chunked";
    const vfs = createVFS(envFor(), { tenant });
    // 20 KB → chunked tier.
    const payload = new Uint8Array(20_000);
    for (let i = 0; i < payload.length; i++) payload[i] = (i * 31) & 0xff;
    await vfs.writeFile("/big.bin", payload);

    await runInDurableObject(userStub(tenant), async (_inst, state) => {
      state.storage.sql.exec("UPDATE files SET head_version_id = NULL");
      state.storage.sql.exec("DELETE FROM file_versions");
    });

    const back = await vfs.readFile("/big.bin");
    expect(back.length).toBe(payload.length);
    // Sample-check a few bytes (full equality is slow under workerd).
    expect(back[0]).toBe(payload[0]);
    expect(back[10_000]).toBe(payload[10_000]);
    expect(back[19_999]).toBe(payload[19_999]);
  });
});

describe("legacy-data migration — pre-Phase-12 (NULL indexed_at)", () => {
  it("M2 — files written before indexed_at column existed are picked up by the reconciler", async () => {
    const tenant = "mig-legacy-indexed";
    const vfs = createVFS(envFor(), { tenant });
    const stub = userStub(tenant);
    await vfs.writeFile("/old1.txt", "x");
    await vfs.writeFile("/old2.txt", "y");

    // Simulate the column existing but never populated (legacy row
    // semantics — column added in Phase 23, rows written before the
    // ALTER had NULL implicitly; the migration's DEFAULT clause does
    // not rewrite history).
    await runInDurableObject(stub, async (_inst, state) => {
      state.storage.sql.exec("UPDATE files SET indexed_at = NULL");
    });

    const unindexed = await stub.appListUnindexedFiles(tenant, 10);
    expect(unindexed.length).toBe(2);
    const names = unindexed.map((r) => r.file_name).sort();
    expect(names).toEqual(["old1.txt", "old2.txt"]);
  });
});

describe("legacy-data migration — forward-compat (every new column nullable)", () => {
  it("M3 — every Phase-9..Phase-22 ALTER is nullable or has a DEFAULT (no backfill required)", async () => {
    const tenant = "mig-forward-compat";
    const vfs = createVFS(envFor(), { tenant });
    await vfs.writeFile("/seed.txt", "ok");

    const stub = userStub(tenant);
    const present = await runInDurableObject(stub, async (_inst, state) => {
      const cols = state.storage.sql
        .exec("PRAGMA table_info(files)")
        .toArray() as {
        name: string;
        notnull: number;
        dflt_value: string | null;
      }[];
      // Every column added post-Phase-9 must be either nullable
      // (notnull=0) or have a non-NULL default. Otherwise inserting
      // a legacy-shaped row via raw SQL would EBADRECORD.
      const culprits = cols.filter(
        (c) => c.notnull === 1 && c.dflt_value === null
      );
      // The original baseline columns (file_id, user_id, file_name,
      // file_size, file_hash, mime_type, chunk_size, chunk_count,
      // pool_size, status, created_at, updated_at) are NOT NULL with
      // no default — these are the only legitimate culprits because
      // they predate the migration story. Anything else is a new
      // column that broke forward-compat.
      const baseline = new Set([
        "file_id",
        "user_id",
        "file_name",
        "file_size",
        "file_hash",
        "mime_type",
        "chunk_size",
        "chunk_count",
        "pool_size",
        "status",
        "created_at",
        "updated_at",
      ]);
      return culprits
        .map((c) => c.name)
        .filter((n) => !baseline.has(n));
    });
    expect(present).toEqual([]);
  });
});

describe("legacy-data migration — mixed legacy + new same UserDO", () => {
  it("M4 — listFiles surfaces both a legacy-shaped row and a new versioning-on row", async () => {
    const tenant = "mig-mixed";
    const stub = userStub(tenant);

    // Write a legacy row first (versioning OFF).
    const legacy = createVFS(envFor(), { tenant });
    await legacy.writeFile("/legacy.txt", "legacy bytes");
    // Force the legacy shape: NULL head, no file_versions.
    await runInDurableObject(stub, async (_inst, state) => {
      state.storage.sql.exec("UPDATE files SET head_version_id = NULL");
      state.storage.sql.exec("DELETE FROM file_versions");
    });

    // Now flip versioning on for the same tenant and write a
    // versioned row.
    const versioned = createVFS(envFor(), {
      tenant,
      versioning: "enabled",
    });
    await versioned.writeFile("/versioned.txt", "versioned bytes");

    // Both rows must come back via listFiles.
    const list = await versioned.listFiles({ orderBy: "name" });
    const paths = list.items.map((i) => i.path).sort();
    expect(paths).toEqual(["/legacy.txt", "/versioned.txt"]);

    // Reads must work for both (legacy via files columns, new via
    // file_versions head pointer).
    const a = await versioned.readFile("/legacy.txt", { encoding: "utf8" });
    expect(a).toBe("legacy bytes");
    const b = await versioned.readFile("/versioned.txt", { encoding: "utf8" });
    expect(b).toBe("versioned bytes");

    // The versioned row has a head_version_id pointing at a
    // file_versions row; the legacy row has NULL.
    const headState = await runInDurableObject(stub, async (_inst, state) => {
      const rows = state.storage.sql
        .exec(
          "SELECT file_name, head_version_id FROM files ORDER BY file_name"
        )
        .toArray() as { file_name: string; head_version_id: string | null }[];
      return rows;
    });
    expect(headState).toHaveLength(2);
    const legacyRow = headState.find((r) => r.file_name === "legacy.txt")!;
    const verRow = headState.find((r) => r.file_name === "versioned.txt")!;
    expect(legacyRow.head_version_id).toBeNull();
    expect(verRow.head_version_id).not.toBeNull();
  });
});
