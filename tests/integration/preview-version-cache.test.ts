import { describe, it, expect } from "vitest";
import { env, runInDurableObject } from "cloudflare:test";
import { vfsUserDOName } from "@core/lib/utils";

/**
 * Phase 28 Fix 1 — version-aware variant cache.
 *
 * Pre-fix the cache key was `(file_id, variant_kind, renderer_kind)`.
 * After v2 supersedes v1 on a versioning-on tenant, the cache row
 * for v1 still matched a thumbnail lookup and the gallery served
 * STALE bytes for the file's HEAD. Bug class:
 *
 *   "I edited the photo but the thumbnail shows the old image."
 *
 * Post-fix the cache row stamps `version_id`; readers gate on a
 * match with the file's current `head_version_id`. Tests:
 *
 *   PV1. Schema — `file_variants.version_id` column exists.
 *   PV2. Cache hit on the SAME head — row matches; reader returns
 *        cached bytes.
 *   PV3. Cache miss after head changes — pre-existing row's
 *        version_id != current head; lookup returns null; the
 *        caller re-renders.
 *   PV4. Legacy NULL-version row — non-versioned tenant gets a
 *        passthrough hit (NULL == NULL semantics in the predicate).
 *   PV5. INSERT OR REPLACE supersedes the stale row — after a
 *        re-render the cache holds the new version's row, not the
 *        old one. (Composite PK collision returns the LATEST.)
 */

import { findVariantRow } from "@core/objects/user/preview-variants";

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

describe("Phase 28 Fix 1 — version-aware variant cache", () => {
  it("PV1 — file_variants.version_id column exists post-ALTER", async () => {
    const tenant = "pv1-schema";
    const stub = userStub(tenant);
    await stub.vfsExists({ ns: NS, tenant }, "/seed");
    const cols = await runInDurableObject(stub, async (_, state) => {
      return state.storage.sql
        .exec("PRAGMA table_info(file_variants)")
        .toArray() as { name: string }[];
    });
    expect(cols.map((c) => c.name)).toContain("version_id");
  });

  it("PV2 — cache hit on same head_version_id", async () => {
    const tenant = "pv2-hit";
    const stub = userStub(tenant);
    await stub.vfsExists({ ns: NS, tenant }, "/seed");
    await runInDurableObject(stub, async (inst, state) => {
      // Seed: a file row + a variant row stamped with v1.
      state.storage.sql.exec(
        `INSERT INTO files (file_id, user_id, parent_id, file_name, file_size, file_hash, mime_type, chunk_size, chunk_count, pool_size, status, created_at, updated_at, mode, node_kind)
         VALUES ('f-1', ?, NULL, 'a.png', 100, '', 'image/png', 0, 0, 32, 'complete', ?, ?, 420, 'file')`,
        tenant,
        Date.now(),
        Date.now()
      );
      state.storage.sql.exec(
        `INSERT INTO file_variants
           (file_id, variant_kind, renderer_kind, chunk_hash, shard_index,
            mime_type, width, height, byte_size, created_at, version_id)
         VALUES ('f-1', 'thumb', 'image', 'h1', 5, 'image/webp', 64, 64, 1234, ?, 'v1')`,
        Date.now()
      );

      // Lookup with matching head_version_id → hit.
      const row = findVariantRow(
        inst as never,
        "f-1",
        "thumb",
        "image",
        "v1"
      );
      expect(row).not.toBeNull();
      expect(row!.chunkHash).toBe("h1");
    });
  });

  it("PV3 — cache miss when head_version_id moves to v2", async () => {
    const tenant = "pv3-miss";
    const stub = userStub(tenant);
    await stub.vfsExists({ ns: NS, tenant }, "/seed");
    await runInDurableObject(stub, async (inst, state) => {
      state.storage.sql.exec(
        `INSERT INTO files (file_id, user_id, parent_id, file_name, file_size, file_hash, mime_type, chunk_size, chunk_count, pool_size, status, created_at, updated_at, mode, node_kind)
         VALUES ('f-2', ?, NULL, 'b.png', 100, '', 'image/png', 0, 0, 32, 'complete', ?, ?, 420, 'file')`,
        tenant,
        Date.now(),
        Date.now()
      );
      // Cache row stamped v1.
      state.storage.sql.exec(
        `INSERT INTO file_variants
           (file_id, variant_kind, renderer_kind, chunk_hash, shard_index,
            mime_type, width, height, byte_size, created_at, version_id)
         VALUES ('f-2', 'thumb', 'image', 'h1', 5, 'image/webp', 64, 64, 1234, ?, 'v1')`,
        Date.now()
      );

      // Lookup with the file's NEW head v2 → miss (caller re-renders).
      const row = findVariantRow(
        inst as never,
        "f-2",
        "thumb",
        "image",
        "v2"
      );
      expect(row).toBeNull();
    });
  });

  it("PV4 — legacy NULL-version row matches a NULL lookup (non-versioning passthrough)", async () => {
    const tenant = "pv4-legacy";
    const stub = userStub(tenant);
    await stub.vfsExists({ ns: NS, tenant }, "/seed");
    await runInDurableObject(stub, async (inst, state) => {
      state.storage.sql.exec(
        `INSERT INTO files (file_id, user_id, parent_id, file_name, file_size, file_hash, mime_type, chunk_size, chunk_count, pool_size, status, created_at, updated_at, mode, node_kind)
         VALUES ('f-3', ?, NULL, 'c.png', 100, '', 'image/png', 0, 0, 32, 'complete', ?, ?, 420, 'file')`,
        tenant,
        Date.now(),
        Date.now()
      );
      // Legacy variant row — version_id NULL (pre-Phase-28 schema).
      state.storage.sql.exec(
        `INSERT INTO file_variants
           (file_id, variant_kind, renderer_kind, chunk_hash, shard_index,
            mime_type, width, height, byte_size, created_at, version_id)
         VALUES ('f-3', 'thumb', 'image', 'h1', 5, 'image/webp', 64, 64, 1234, ?, NULL)`,
        Date.now()
      );

      // Lookup with NULL head — hits the legacy row.
      const row = findVariantRow(
        inst as never,
        "f-3",
        "thumb",
        "image",
        null
      );
      expect(row).not.toBeNull();
      expect(row!.chunkHash).toBe("h1");

      // Lookup with a version id v1 — does NOT match the NULL row
      // (legacy rows are reachable only via NULL-on-NULL).
      const miss = findVariantRow(
        inst as never,
        "f-3",
        "thumb",
        "image",
        "v1"
      );
      expect(miss).toBeNull();
    });
  });

  it("PV5 — INSERT OR REPLACE supersedes a stale row by composite PK", async () => {
    const tenant = "pv5-replace";
    const stub = userStub(tenant);
    await stub.vfsExists({ ns: NS, tenant }, "/seed");
    await runInDurableObject(stub, async (inst, state) => {
      state.storage.sql.exec(
        `INSERT INTO files (file_id, user_id, parent_id, file_name, file_size, file_hash, mime_type, chunk_size, chunk_count, pool_size, status, created_at, updated_at, mode, node_kind)
         VALUES ('f-4', ?, NULL, 'd.png', 100, '', 'image/png', 0, 0, 32, 'complete', ?, ?, 420, 'file')`,
        tenant,
        Date.now(),
        Date.now()
      );
      // Stale row for v1.
      state.storage.sql.exec(
        `INSERT INTO file_variants
           (file_id, variant_kind, renderer_kind, chunk_hash, shard_index,
            mime_type, width, height, byte_size, created_at, version_id)
         VALUES ('f-4', 'thumb', 'image', 'h_old', 5, 'image/webp', 64, 64, 1234, ?, 'v1')`,
        Date.now()
      );
      // Re-render writes a v2 row for the same composite PK via
      // INSERT OR REPLACE — the row supersedes the v1 stale entry.
      state.storage.sql.exec(
        `INSERT OR REPLACE INTO file_variants
           (file_id, variant_kind, renderer_kind, chunk_hash, shard_index,
            mime_type, width, height, byte_size, created_at, version_id)
         VALUES ('f-4', 'thumb', 'image', 'h_new', 9, 'image/webp', 64, 64, 5678, ?, 'v2')`,
        Date.now()
      );

      // The v2 row is the only one (PK collision dropped v1).
      const all = state.storage.sql
        .exec(
          "SELECT chunk_hash, version_id FROM file_variants WHERE file_id = 'f-4'"
        )
        .toArray() as { chunk_hash: string; version_id: string | null }[];
      expect(all.length).toBe(1);
      expect(all[0].chunk_hash).toBe("h_new");
      expect(all[0].version_id).toBe("v2");

      // Reader against v2 hits the new row.
      const hit = findVariantRow(
        inst as never,
        "f-4",
        "thumb",
        "image",
        "v2"
      );
      expect(hit).not.toBeNull();
      expect(hit!.chunkHash).toBe("h_new");

      // Reader against v1 (the now-stale id) misses.
      const miss = findVariantRow(
        inst as never,
        "f-4",
        "thumb",
        "image",
        "v1"
      );
      expect(miss).toBeNull();
    });
  });
});
