import { describe, it, expect } from "vitest";
import { env, runInDurableObject } from "cloudflare:test";
import {
  applyMigrationOnce,
  ensureMigrationsTable,
} from "@core/lib/migrations";
import type { UserDO } from "@app/objects/user/user-do";
import type { SearchDO } from "@app/objects/search/search-do";
import {
  SHARD_CLEANUP_JOURNAL_TTL_MS,
  type ShardDO,
} from "@core/objects/shard/shard-do";
import { vfsUserDOName } from "@core/lib/utils";
import {
  MULTIPART_FENCE_GC_GRACE_MS,
  MULTIPART_MAX_TTL_MS,
} from "@shared/multipart";

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
  it("migrates existing cleanup intents with a resumable cursor and generation", async () => {
    const stub = userStub("cleanup-outbox-cursor-migration");
    await runInDurableObject(stub, (_instance, state) => {
      state.storage.sql.exec(`
        CREATE TABLE chunk_cleanup_intents (
          ref_id TEXT NOT NULL,
          shard_index INTEGER NOT NULL,
          cleanup_kind TEXT NOT NULL DEFAULT 'chunks',
          state TEXT NOT NULL DEFAULT 'pending',
          generation INTEGER NOT NULL DEFAULT 0,
          provisional INTEGER NOT NULL DEFAULT 0,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL,
          next_attempt_at INTEGER NOT NULL DEFAULT 0,
          attempts INTEGER NOT NULL DEFAULT 0,
          last_error TEXT,
          PRIMARY KEY (ref_id, shard_index)
        )
      `);
      state.storage.sql.exec(
        `INSERT INTO chunk_cleanup_intents
           (ref_id, shard_index, cleanup_kind, state, generation, provisional,
            created_at, updated_at, next_attempt_at, attempts, last_error)
         VALUES ('legacy-ref', 7, 'multipart_staging', 'pending', 3, 0,
                 100, 200, 300, 4, 'retry')`
      );
    });

    await stub.appGetQuota("cleanup-outbox-cursor-migration");
    const migrated = await runInDurableObject(stub, (_instance, state) =>
      state.storage.sql
        .exec(
          `SELECT ref_id, shard_index, cleanup_kind, state, generation,
                  cleanup_generation, cleanup_cursor, cleanup_phase,
                  created_at, updated_at, next_attempt_at, attempts, last_error
             FROM chunk_cleanup_intents WHERE ref_id = 'legacy-ref'`
        )
        .toArray()[0]
    );
    expect(migrated).toMatchObject({
      ref_id: "legacy-ref",
      shard_index: 7,
      cleanup_kind: "multipart_staging",
      state: "pending",
      generation: 3,
      cleanup_cursor: 0,
      cleanup_phase: "staging",
      created_at: 100,
      updated_at: 200,
      next_attempt_at: 300,
      attempts: 4,
      last_error: "retry",
    });
    expect(migrated).toHaveProperty(
      "cleanup_generation",
      expect.stringMatching(/^[0-9a-f]{32}$/)
    );
  });

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

describe("large schema maintenance migration", () => {
  it("pages historical retention, cleanup, and upload state across cold starts", async () => {
    const stub = userStub("schema-large-paged-maintenance");

    await runInDurableObject(stub, (instance: UserDO, state) => {
      const sql = state.storage.sql;
      const internals = instance as unknown as InitializableDO;
      internals.ensureInit();
      sql.exec("DROP TRIGGER version_retention_order_insert");
      sql.exec("DROP TRIGGER version_retention_order_update");
      sql.exec("DROP TRIGGER version_retention_order_delete");
      sql.exec("DELETE FROM version_retention_order");
      sql.exec(
        `DELETE FROM schema_maintenance WHERE name IN (
          'version_retention_order_v1', 'chunk_cleanup_intents_v2',
          'upload_staged_hash_cursor_v1'
        )`
      );

      for (let index = 0; index < 600; index++) {
        const id = index.toString().padStart(4, "0");
        sql.exec(
          `INSERT INTO file_versions
             (path_id, version_id, user_id, size, mode, mtime_ms, deleted,
              inline_data, chunk_size, chunk_count, file_hash, mime_type)
           VALUES ('legacy-path', ?, 'legacy-user', 0, 420, ?, 0,
                   NULL, 0, 0, '', 'text/plain')`,
          `legacy-version-${id}`,
          index
        );
        sql.exec(
          `INSERT INTO chunk_cleanup_intents
             (ref_id, shard_index, cleanup_kind, state, generation, provisional,
              created_at, updated_at, next_attempt_at, attempts, last_error,
              cleanup_generation, cleanup_cursor, cleanup_phase)
           VALUES (?, 0, 'multipart_staging', 'pending', 0, 0,
                   0, 0, 0, 0, NULL, '', 0, 'chunks')`,
          `legacy-cleanup-${id}`
        );
        sql.exec(
          `INSERT INTO upload_sessions
             (upload_id, user_id, parent_id, leaf, total_size, total_chunks,
              chunk_size, pool_size, expires_at, status, mode, mime_type, created_at)
           VALUES (?, 'legacy-user', NULL, ?, 1, 1, 1, 32, 1,
                   'open', 420, 'application/octet-stream', 1)`,
          `legacy-upload-${id}`,
          `legacy-${id}.bin`
        );
        sql.exec(
          `INSERT INTO upload_expected_chunks (upload_id, chunk_index, chunk_hash)
           VALUES (?, 0, ?)`,
          `legacy-upload-${id}`,
          index.toString(16).padStart(64, "0")
        );
      }

      for (const expected of [256, 512, 600]) {
        internals.initialized = false;
        internals.ensureInit();
        expect(
          (
            sql.exec("SELECT COUNT(*) AS n FROM version_retention_order").toArray()[0] as {
              n: number;
            }
          ).n
        ).toBe(expected);
        expect(
          (
            sql
              .exec(
                `SELECT COUNT(*) AS n FROM chunk_cleanup_intents
                  WHERE cleanup_generation != '' AND cleanup_phase = 'staging'`
              )
              .toArray()[0] as { n: number }
          ).n
        ).toBe(expected);
        expect(
          (
            sql
              .exec("SELECT COUNT(*) AS n FROM upload_sessions WHERE staged_hash_cursor = 1")
              .toArray()[0] as { n: number }
          ).n
        ).toBe(expected);
      }

      expect(
        sql
          .exec(
            `SELECT name, state FROM schema_maintenance
              WHERE name IN (
                'version_retention_order_v1', 'chunk_cleanup_intents_v2',
                'upload_staged_hash_cursor_v1'
              ) ORDER BY name`
          )
          .toArray()
      ).toEqual([
        { name: "chunk_cleanup_intents_v2", state: "ready" },
        { name: "upload_staged_hash_cursor_v1", state: "ready" },
        { name: "version_retention_order_v1", state: "ready" },
      ]);
    });
  });
});

describe("multipart placement version migration", () => {
  it("records the migration and defaults existing sessions to legacy placement", async () => {
    const stub = userStub("schema-multipart-placement-version");

    await runInDurableObject(stub, (instance: UserDO, state) => {
      const internals = instance as unknown as InitializableDO;
      state.storage.sql.exec(`
        CREATE TABLE upload_sessions (
          upload_id TEXT PRIMARY KEY,
          user_id TEXT NOT NULL,
          parent_id TEXT,
          leaf TEXT NOT NULL,
          total_size INTEGER NOT NULL,
          total_chunks INTEGER NOT NULL,
          chunk_size INTEGER NOT NULL,
          pool_size INTEGER NOT NULL,
          expires_at INTEGER NOT NULL,
          status TEXT NOT NULL,
          encryption_mode TEXT,
          encryption_key_id TEXT,
          metadata_blob BLOB,
          tags_json TEXT,
          version_label TEXT,
          version_user_visible INTEGER,
          mode INTEGER NOT NULL,
          mime_type TEXT NOT NULL,
          created_at INTEGER NOT NULL
        )
      `);
      state.storage.sql.exec(
        `INSERT INTO upload_sessions
           (upload_id, user_id, parent_id, leaf, total_size, total_chunks,
            chunk_size, pool_size, expires_at, status, mode, mime_type, created_at)
         VALUES ('legacy-upload', 'legacy-user', NULL, 'legacy.bin', 1, 1,
                 1, 32, 1, 'open', 420, 'application/octet-stream', 1)`
      );
      internals.ensureInit();

      expect(
        state.storage.sql
          .exec(
            "SELECT name FROM meta_schema WHERE name = 'upload_sessions_add_placement_version'"
          )
          .toArray()
      ).toEqual([{ name: "upload_sessions_add_placement_version" }]);
      expect(
        state.storage.sql
          .exec("PRAGMA table_info(upload_sessions)")
          .toArray()
          .find((column) => column.name === "placement_version")
      ).toMatchObject({
        name: "placement_version",
        notnull: 1,
        dflt_value: "1",
      });
      expect(
        state.storage.sql
          .exec(
            "SELECT placement_version FROM upload_sessions WHERE upload_id = 'legacy-upload'"
          )
          .toArray()
      ).toEqual([{ placement_version: 1 }]);
    });
  });
});

describe("cleanup journal lifecycle migration", () => {
  it("reconstructs legacy progress and only reclaims completed generations", async () => {
    const stub = shardStub("schema-cleanup-journal-lifecycle");

    await runInDurableObject(stub, async (instance: ShardDO, state) => {
      const sql = state.storage.sql;
      const internals = instance as unknown as InitializableDO;
      internals.ensureInit();
      const createdAt = Date.now() - SHARD_CLEANUP_JOURNAL_TTL_MS - 1;
      sql.exec(
        `DELETE FROM schema_maintenance
          WHERE name = 'shard_cleanup_journal_lifecycle_v2'`
      );
      sql.exec(
        `INSERT OR REPLACE INTO schema_maintenance
           (name, state, cursor, updated_at)
         VALUES ('shard_cleanup_journal_expiry_v1', 'ready', '2', ?)`,
        createdAt
      );
      for (const row of [
        { refId: "legacy-active", nextCursor: 256, processed: 256, done: 0 },
        { refId: "legacy-complete", nextCursor: 3, processed: 3, done: 1 },
      ]) {
        sql.exec(
          `INSERT INTO shard_cleanup_pages
             (cleanup_kind, ref_id, cleanup_generation, request_cursor,
              next_cursor, processed, marked, done, created_at)
           VALUES ('refs', ?, 'legacy-generation', 0, ?, ?, 0, ?, ?)`,
          row.refId,
          row.nextCursor,
          row.processed,
          row.done,
          createdAt
        );
        sql.exec(
          `INSERT INTO shard_cleanup_page_expirations
             (expires_at, cleanup_kind, ref_id, cleanup_generation, request_cursor)
           VALUES (?, 'refs', ?, 'legacy-generation', 0)`,
          createdAt + SHARD_CLEANUP_JOURNAL_TTL_MS,
          row.refId
        );
      }

      internals.initialized = false;
      internals.ensureInit();
      expect(
        sql
          .exec(
            `SELECT ref_id, next_cursor, done FROM shard_cleanup_progress
              ORDER BY ref_id`
          )
          .toArray()
      ).toEqual([
        { ref_id: "legacy-active", next_cursor: 256, done: 0 },
        { ref_id: "legacy-complete", next_cursor: 3, done: 1 },
      ]);

      await instance.alarm();

      expect(
        sql.exec("SELECT ref_id FROM shard_cleanup_pages ORDER BY ref_id").toArray()
      ).toEqual([{ ref_id: "legacy-active" }]);
      expect(
        sql.exec("SELECT ref_id, done FROM shard_cleanup_progress").toArray()
      ).toEqual([{ ref_id: "legacy-active", done: 0 }]);
      expect(
        sql
          .exec("SELECT ref_id FROM shard_cleanup_page_expirations")
          .toArray()
      ).toEqual([{ ref_id: "legacy-active" }]);
      expect(
        sql
          .exec(
            `SELECT state FROM schema_maintenance
              WHERE name = 'shard_cleanup_journal_lifecycle_v2'`
          )
          .toArray()
      ).toEqual([{ state: "ready" }]);
    });
  });
});

describe("multipart fence expiry migration", () => {
  it("backfills legacy fences from updated_at and arms their GC deadline", async () => {
    const stub = shardStub("schema-fence-expiry-backfill");
    const updatedAt = Date.now() - 24 * 60 * 60 * 1000;
    const expectedExpiry = updatedAt + MULTIPART_MAX_TTL_MS;

    const migrated = await runInDurableObject(
      stub,
      async (instance: ShardDO, state) => {
        const sql = state.storage.sql;
        const internals = instance as unknown as InitializableDO;
        sql.exec(`
          CREATE TABLE multipart_fences (
            upload_id TEXT PRIMARY KEY,
            fence_id TEXT NOT NULL,
            state TEXT NOT NULL,
            updated_at INTEGER NOT NULL
          )
        `);
        sql.exec(
          "INSERT INTO multipart_fences VALUES ('legacy-upload', 'legacy-fence', 'finalizing', ?)",
          updatedAt
        );

        internals.ensureInit();
        await Promise.resolve();
        return {
          row: sql
            .exec(
              "SELECT updated_at, expires_at FROM multipart_fences WHERE upload_id = 'legacy-upload'"
            )
            .toArray()[0],
          migrations: sql
            .exec(
              `SELECT name FROM meta_schema
                WHERE name LIKE 'multipart_fences_%' ORDER BY name`
            )
            .toArray(),
        };
      }
    );
    const alarm = await runInDurableObject(stub, (_instance, state) =>
      state.storage.getAlarm()
    );

    expect(migrated).toEqual({
      row: { updated_at: updatedAt, expires_at: expectedExpiry },
      migrations: [
        { name: "multipart_fences_add_expires_at" },
      ],
    });
    expect(alarm).toBe(expectedExpiry + MULTIPART_FENCE_GC_GRACE_MS);
  });

  it("reclaims a backfilled fence only after max TTL and grace have elapsed", async () => {
    const stub = shardStub("schema-fence-expiry-reclaim");
    const updatedAt =
      Date.now() - MULTIPART_MAX_TTL_MS - MULTIPART_FENCE_GC_GRACE_MS - 2_000;

    await runInDurableObject(stub, (instance: ShardDO, state) => {
      const sql = state.storage.sql;
      const internals = instance as unknown as InitializableDO;
      sql.exec(`
        CREATE TABLE multipart_fences (
          upload_id TEXT PRIMARY KEY,
          fence_id TEXT NOT NULL,
          state TEXT NOT NULL,
          updated_at INTEGER NOT NULL
        )
      `);
      sql.exec(
        "INSERT INTO multipart_fences VALUES ('expired-upload', 'expired-fence', 'aborting', ?)",
        updatedAt
      );
      internals.ensureInit();
    });

    await runInDurableObject(stub, (instance) => instance.alarm());
    await expect(
      runInDurableObject(stub, (_instance, state) =>
        state.storage.sql
          .exec("SELECT COUNT(*) AS n FROM multipart_fences")
          .toArray()[0]
      )
    ).resolves.toEqual({ n: 0 });
  });
});
