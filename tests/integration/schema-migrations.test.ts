import { describe, it, expect } from "vitest";
import { env, runInDurableObject } from "cloudflare:test";
import {
  applyMigrationOnce,
  ensureMigrationsTable,
} from "@core/lib/migrations";
import type { UserDO } from "@app/objects/user/user-do";
import type { SearchDO } from "@app/objects/search/search-do";
import type { ShardDO } from "@core/objects/shard/shard-do";
import { vfsUserDOName } from "@core/lib/utils";

/**
 * `applyMigrationOnce` — schema-version registry.
 *
 * Replaces 33 sites of `try { ALTER } catch {}` (idempotent on
 * "duplicate column name" SQLite errors) with a `meta_schema`
 * tracked, named-migration model. Tests pin the bridge contract
 * (existing instances whose columns already exist must NOT throw)
 * and the visibility contract (genuinely failing migrations must
 * propagate, not silently be recorded).
 */

interface E {
  MOSSAIC_USER: DurableObjectNamespace<UserDO>;
  MOSSAIC_SHARD: DurableObjectNamespace<ShardDO>;
  SEARCH_DO: DurableObjectNamespace<SearchDO>;
}
const E = env as unknown as E;

function userStub(name = "schema-mig") {
  return E.MOSSAIC_USER.get(
    E.MOSSAIC_USER.idFromName(vfsUserDOName("default", name))
  );
}

function shardStub(name: string) {
  return E.MOSSAIC_SHARD.get(E.MOSSAIC_SHARD.idFromName(name));
}

function searchStub(name: string) {
  return E.SEARCH_DO.get(E.SEARCH_DO.idFromName(name));
}

interface InitializableDO {
  initialized: boolean;
  ensureInit(): void;
}

describe("applyMigrationOnce", () => {
  it("M1 — applies the body the first time only", async () => {
    const stub = userStub();
    await runInDurableObject(stub, (instance: UserDO) => {
      const sql = (instance as unknown as { sql: SqlStorage }).sql;

      ensureMigrationsTable(sql);
      sql.exec("CREATE TABLE IF NOT EXISTS m1 (k TEXT)");
      let calls = 0;
      const fn = () => {
        calls++;
        sql.exec("ALTER TABLE m1 ADD COLUMN v TEXT");
      };
      applyMigrationOnce(sql, "m1_add_v", fn);
      applyMigrationOnce(sql, "m1_add_v", fn);
      applyMigrationOnce(sql, "m1_add_v", fn);
      expect(calls).toBe(1);
    });
  });

  it("M2 — bridge: pre-applied column on a fresh meta_schema doesn't throw", async () => {
    const stub = userStub();
    await runInDurableObject(stub, (instance: UserDO) => {
      const sql = (instance as unknown as { sql: SqlStorage }).sql;

      ensureMigrationsTable(sql);
      sql.exec("CREATE TABLE IF NOT EXISTS m2 (k TEXT, already_present TEXT)");
      // First time: column already exists — must not throw, must
      // record the migration anyway so subsequent runs no-op.
      applyMigrationOnce(sql, "m2_add_already_present", () => {
        sql.exec("ALTER TABLE m2 ADD COLUMN already_present TEXT");
      });
      const recorded = sql
        .exec(
          "SELECT 1 AS one FROM meta_schema WHERE name = ?",
          "m2_add_already_present"
        )
        .toArray();
      expect(recorded.length).toBe(1);
    });
  });

  it("M3 — propagates non-duplicate errors (visibility)", async () => {
    const stub = userStub();
    await runInDurableObject(stub, (instance: UserDO) => {
      const sql = (instance as unknown as { sql: SqlStorage }).sql;

      ensureMigrationsTable(sql);
      // Reference a non-existent table — SQLite raises "no such table",
      // NOT a duplicate-column error; helper must re-throw.
      expect(() =>
        applyMigrationOnce(sql, "m3_bad", () => {
          sql.exec(
            "ALTER TABLE table_that_does_not_exist ADD COLUMN x TEXT"
          );
        })
      ).toThrow();

      // Failed migration must NOT be recorded (so a subsequent fix +
      // re-run can apply it).
      const recorded = sql
        .exec("SELECT 1 AS one FROM meta_schema WHERE name = ?", "m3_bad")
        .toArray();
      expect(recorded.length).toBe(0);
    });
  });

  it("M4 — ensureMigrationsTable is idempotent", async () => {
    const stub = userStub();
    await runInDurableObject(stub, (instance: UserDO) => {
      const sql = (instance as unknown as { sql: SqlStorage }).sql;

      ensureMigrationsTable(sql);
      // Row count stays whatever it is.
      const before = sql.exec("SELECT COUNT(*) AS n FROM meta_schema").toArray()[0] as {
        n: number;
      };
      ensureMigrationsTable(sql);
      ensureMigrationsTable(sql);
      const after = sql.exec("SELECT COUNT(*) AS n FROM meta_schema").toArray()[0] as {
        n: number;
      };
      expect(after.n).toBe(before.n);
    });
  });

  it("M5 — distinct names track distinct migrations", async () => {
    const stub = userStub();
    await runInDurableObject(stub, (instance: UserDO) => {
      const sql = (instance as unknown as { sql: SqlStorage }).sql;

      ensureMigrationsTable(sql);
      sql.exec("CREATE TABLE IF NOT EXISTS m5 (k TEXT)");
      let aCalls = 0;
      let bCalls = 0;
      applyMigrationOnce(sql, "m5_a", () => {
        aCalls++;
        sql.exec("ALTER TABLE m5 ADD COLUMN a TEXT");
      });
      applyMigrationOnce(sql, "m5_b", () => {
        bCalls++;
        sql.exec("ALTER TABLE m5 ADD COLUMN b TEXT");
      });
      applyMigrationOnce(sql, "m5_a", () => {
        aCalls++;
      });
      applyMigrationOnce(sql, "m5_b", () => {
        bCalls++;
      });
      expect(aCalls).toBe(1);
      expect(bCalls).toBe(1);
    });
  });

  it("M6 — ensureInit on a fresh DO records every migration name", async () => {
    // The UserDO ensureInit() runs the full migration suite. After
    // it lands, meta_schema has rows for every named migration
    // recorded in user-do-core.ts.
    const stub = userStub();
    await runInDurableObject(stub, (instance: UserDO) => {
      // Force the schema to materialize.
      (instance as unknown as { ensureInit?: () => void }).ensureInit?.();
      const sql = (instance as unknown as { sql: SqlStorage }).sql;

      const rows = sql
        .exec("SELECT name FROM meta_schema ORDER BY name")
        .toArray() as { name: string }[];
      // We don't assert an exact list (the migration set is allowed
      // to grow); just confirm the registry isn't empty AND that
      // every recorded name is the expected `<table>_<purpose>`
      // shape (lowercase + underscores + alphanumerics, no spaces).
      expect(rows.length).toBeGreaterThan(0);
      for (const r of rows) {
        expect(r.name).toMatch(/^[a-z][a-z0-9_]*$/);
      }
    });
  });
});

describe("transactional DO schema initialization", () => {
  it("rolls back UserDO initialization and retries on the same instance", async () => {
    const stub = userStub("schema-init-rollback");

    await runInDurableObject(stub, (instance: UserDO, state) => {
      const sql = state.storage.sql;
      const internals = instance as unknown as InitializableDO;

      sql.exec("CREATE VIEW quota AS SELECT 'legacy' AS user_id");

      expect(() => internals.ensureInit()).toThrow(/view/i);
      expect(internals.initialized).toBe(false);
      expect(
        sql
          .exec(
            "SELECT name FROM sqlite_master WHERE name IN ('meta_schema', 'files', 'file_chunks') ORDER BY name"
          )
          .toArray()
      ).toEqual([]);

      sql.exec("DROP VIEW quota");
      internals.ensureInit();

      expect(internals.initialized).toBe(true);
      expect(
        sql
          .exec(
            "SELECT name FROM meta_schema WHERE name = 'quota_add_rate_limit_per_sec'"
          )
          .toArray()
      ).toEqual([{ name: "quota_add_rate_limit_per_sec" }]);
      expect(
        sql.exec("PRAGMA table_info(chunk_cleanup_intents)").toArray().length
      ).toBeGreaterThan(0);
    });
  });

  it("rolls back ShardDO initialization and retries on the same instance", async () => {
    const stub = shardStub("schema-init-rollback:shard");

    await runInDurableObject(stub, (instance: ShardDO, state) => {
      const sql = state.storage.sql;
      const internals = instance as unknown as InitializableDO;

      sql.exec("CREATE VIEW chunks AS SELECT 'legacy' AS hash");

      expect(() => internals.ensureInit()).toThrow(/view/i);
      expect(internals.initialized).toBe(false);
      expect(
        sql
          .exec(
            "SELECT name FROM sqlite_master WHERE name IN ('meta_schema', 'chunk_refs', 'shard_meta') ORDER BY name"
          )
          .toArray()
      ).toEqual([]);

      sql.exec("DROP VIEW chunks");
      internals.ensureInit();

      expect(internals.initialized).toBe(true);
      expect(
        sql
          .exec(
            "SELECT name FROM meta_schema WHERE name = 'chunks_add_deleted_at'"
          )
          .toArray()
      ).toEqual([{ name: "chunks_add_deleted_at" }]);

      internals.initialized = false;
      expect(() => internals.ensureInit()).not.toThrow();
      expect(
        sql
          .exec(
            "SELECT name FROM meta_schema WHERE name = 'chunks_add_deleted_at'"
          )
          .toArray()
      ).toEqual([{ name: "chunks_add_deleted_at" }]);
    });
  });

  it("rolls back SearchDO initialization and retries on the same instance", async () => {
    const stub = searchStub("schema-init-rollback:search");

    await runInDurableObject(stub, (instance: SearchDO, state) => {
      const sql = state.storage.sql;
      const internals = instance as unknown as InitializableDO;

      sql.exec("CREATE VIEW vectors AS SELECT 'legacy' AS id");

      expect(() => internals.ensureInit()).toThrow(/view/i);
      expect(internals.initialized).toBe(false);
      expect(
        sql
          .exec(
            "SELECT name FROM sqlite_master WHERE name IN ('meta_schema', 'vector_metadata', 'search_config') ORDER BY name"
          )
          .toArray()
      ).toEqual([]);

      sql.exec("DROP VIEW vectors");
      internals.ensureInit();

      expect(internals.initialized).toBe(true);
      expect(
        sql.exec("SELECT name FROM meta_schema ORDER BY name").toArray()
      ).toEqual([
        { name: "vector_metadata_add_space" },
        { name: "vectors_add_space" },
      ]);
      expect(
        sql.exec("PRAGMA table_info(vectors)").toArray().map((column) => column.name)
      ).toContain("space");

      internals.initialized = false;
      expect(() => internals.ensureInit()).not.toThrow();
      expect(
        sql.exec("SELECT name FROM meta_schema ORDER BY name").toArray()
      ).toEqual([
        { name: "vector_metadata_add_space" },
        { name: "vectors_add_space" },
      ]);
    });
  });
});
