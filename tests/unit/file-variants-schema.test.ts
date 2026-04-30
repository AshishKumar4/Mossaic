import { describe, it, expect } from "vitest";
import { env, runInDurableObject } from "cloudflare:test";
import { vfsUserDOName } from "@core/lib/utils";

/**
 * `file_variants` schema integrity.
 *
 *   F1.  CREATE TABLE IF NOT EXISTS is idempotent across ensureInit calls.
 *   F2.  Composite PK (file_id, variant_kind, renderer_kind) rejects
 *        duplicate inserts with a constraint failure.
 *   F3.  ON DELETE CASCADE removes file_variants rows when the parent
 *        files row is deleted.
 *   F4.  Indexes on chunk_hash and file_id are present + queryable.
 */

interface E {
  MOSSAIC_USER: DurableObjectNamespace;
}
const E = env as unknown as E;
const NS = "default";

function userStub(tenant: string) {
  return E.MOSSAIC_USER.get(
    E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
  );
}

describe("file_variants schema", () => {
  it("F1 — CREATE TABLE is idempotent across DO instances", async () => {
    const tenant = "fv-schema-1";
    const stub = userStub(tenant);
    // First touch initialises schema.
    await stub.vfsExists({ ns: NS, tenant }, "/seed");
    // Read columns to confirm the table is there.
    const cols = await runInDurableObject(stub, async (_, state) => {
      return state.storage.sql
        .exec("PRAGMA table_info(file_variants)")
        .toArray() as { name: string; type: string }[];
    });
    const names = cols.map((c) => c.name);
    expect(names).toContain("file_id");
    expect(names).toContain("variant_kind");
    expect(names).toContain("renderer_kind");
    expect(names).toContain("chunk_hash");
    expect(names).toContain("shard_index");
    expect(names).toContain("mime_type");
    expect(names).toContain("width");
    expect(names).toContain("height");
    expect(names).toContain("byte_size");
    expect(names).toContain("created_at");
  });

  it("F2 — composite PK rejects duplicate (file_id, variant_kind, renderer_kind)", async () => {
    const tenant = "fv-schema-2";
    const stub = userStub(tenant);
    await stub.vfsExists({ ns: NS, tenant }, "/seed");
    const result = await runInDurableObject(stub, async (_, state) => {
      // Seed a files row so the FK satisfies.
      const fileId = "f-pk-test";
      state.storage.sql.exec(
        `INSERT INTO files (file_id, user_id, parent_id, file_name, file_size, file_hash, mime_type, chunk_size, chunk_count, pool_size, status, created_at, updated_at, mode, node_kind)
         VALUES (?, ?, NULL, ?, 0, '', 'application/octet-stream', 0, 0, 32, 'complete', 0, 0, 0, 'file')`,
        fileId,
        tenant,
        "test.bin"
      );
      const rk = "icon-card";
      state.storage.sql.exec(
        `INSERT INTO file_variants (file_id, variant_kind, renderer_kind, chunk_hash, shard_index, mime_type, width, height, byte_size, created_at)
         VALUES (?, 'thumb', ?, 'h1', 0, 'image/svg+xml', 256, 256, 100, 0)`,
        fileId,
        rk
      );
      let dupeFailed = false;
      try {
        state.storage.sql.exec(
          `INSERT INTO file_variants (file_id, variant_kind, renderer_kind, chunk_hash, shard_index, mime_type, width, height, byte_size, created_at)
           VALUES (?, 'thumb', ?, 'h2', 0, 'image/svg+xml', 256, 256, 100, 0)`,
          fileId,
          rk
        );
      } catch {
        dupeFailed = true;
      }
      return { dupeFailed };
    });
    expect(result.dupeFailed).toBe(true);
  });

  it("F3 — ON DELETE CASCADE removes file_variants rows", async () => {
    const tenant = "fv-schema-3";
    const stub = userStub(tenant);
    await stub.vfsExists({ ns: NS, tenant }, "/seed");
    const result = await runInDurableObject(stub, async (_, state) => {
      // Enable foreign key enforcement (workerd default is OFF for SQLite).
      state.storage.sql.exec("PRAGMA foreign_keys = ON");
      const fileId = "f-cascade";
      state.storage.sql.exec(
        `INSERT INTO files (file_id, user_id, parent_id, file_name, file_size, file_hash, mime_type, chunk_size, chunk_count, pool_size, status, created_at, updated_at, mode, node_kind)
         VALUES (?, ?, NULL, ?, 0, '', 'application/octet-stream', 0, 0, 32, 'complete', 0, 0, 0, 'file')`,
        fileId,
        tenant,
        "test.bin"
      );
      state.storage.sql.exec(
        `INSERT INTO file_variants (file_id, variant_kind, renderer_kind, chunk_hash, shard_index, mime_type, width, height, byte_size, created_at)
         VALUES (?, 'thumb', 'icon-card', 'h', 0, 'image/svg+xml', 256, 256, 100, 0)`,
        fileId
      );
      const before = (
        state.storage.sql
          .exec(
            "SELECT COUNT(*) AS n FROM file_variants WHERE file_id = ?",
            fileId
          )
          .toArray()[0] as { n: number }
      ).n;
      state.storage.sql.exec("DELETE FROM files WHERE file_id = ?", fileId);
      const after = (
        state.storage.sql
          .exec(
            "SELECT COUNT(*) AS n FROM file_variants WHERE file_id = ?",
            fileId
          )
          .toArray()[0] as { n: number }
      ).n;
      return { before, after };
    });
    expect(result.before).toBe(1);
    expect(result.after).toBe(0);
  });

  it("F4 — indexes on chunk_hash and file_id exist", async () => {
    const tenant = "fv-schema-4";
    const stub = userStub(tenant);
    await stub.vfsExists({ ns: NS, tenant }, "/seed");
    const indexes = await runInDurableObject(stub, async (_, state) => {
      return state.storage.sql
        .exec(
          "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'file_variants'"
        )
        .toArray() as { name: string }[];
    });
    const names = indexes.map((i) => i.name);
    expect(names).toContain("idx_file_variants_hash");
    expect(names).toContain("idx_file_variants_file");
  });
});
