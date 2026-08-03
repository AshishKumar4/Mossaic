import { describe, it, expect } from "vitest";
import { env, runInDurableObject } from "cloudflare:test";
import {
  applyMigrationOnce,
  ensureMigrationsTable,
} from "@core/lib/migrations";
import type { UserDO } from "@app/objects/user/user-do";
import type { SearchDO } from "@app/objects/search/search-do";
import {
  MULTIPART_FENCE_PAGE_LIMIT,
  type ShardDO,
} from "@core/objects/shard/shard-do";
import { vfsUserDOName } from "@core/lib/utils";
import {
  MULTIPART_FENCE_GC_GRACE_MS,
  MULTIPART_LEGACY_PLACEMENT_VERSION,
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

/**
 * Fence rows predate `expires_at`, so an upgraded shard holds rows with
 * no deadline at all. The backfill derives one from the only timestamp
 * they carry, which is what lets them be reclaimed later instead of
 * accumulating forever — and what keeps them from being reclaimed
 * early, while a straggler PUT could still arrive.
 */
describe("multipart fence expiry migration", () => {
  const LEGACY_FENCES_DDL = `
    CREATE TABLE multipart_fences (
      upload_id TEXT PRIMARY KEY,
      fence_id TEXT NOT NULL,
      state TEXT NOT NULL,
      updated_at INTEGER NOT NULL
    )
  `;

  function fenceRows(sql: SqlStorage): Array<Record<string, unknown>> {
    return sql
      .exec("SELECT upload_id, updated_at, expires_at FROM multipart_fences")
      .toArray();
  }

  it("backfills legacy fences from updated_at and arms their GC deadline", async () => {
    const stub = shardStub("schema-fence-expiry-backfill");
    const updatedAt = Date.now() - 24 * 60 * 60 * 1000;
    const expectedExpiry = updatedAt + MULTIPART_MAX_TTL_MS;

    const migrated = await runInDurableObject(
      stub,
      async (instance: ShardDO, state) => {
        const sql = state.storage.sql;
        const internals = instance as unknown as InitializableDO;
        sql.exec(LEGACY_FENCES_DDL);
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
      migrations: [{ name: "multipart_fences_add_expires_at" }],
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
      sql.exec(LEGACY_FENCES_DDL);
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

  it("keeps a legacy fence whose derived deadline has not passed", async () => {
    const stub = shardStub("schema-fence-expiry-still-owed");
    // Older than any wall-clock margin the GC applies on its own, yet a
    // token minted at the max TTL against it is still live for another
    // minute — the row is the only thing that would reject that PUT.
    const updatedAt = Date.now() - MULTIPART_MAX_TTL_MS + 60_000;

    await runInDurableObject(stub, (instance: ShardDO, state) => {
      const sql = state.storage.sql;
      const internals = instance as unknown as InitializableDO;
      sql.exec(LEGACY_FENCES_DDL);
      sql.exec(
        "INSERT INTO multipart_fences VALUES ('owed-upload', 'owed-fence', 'aborting', ?)",
        updatedAt
      );
      internals.ensureInit();
    });

    await runInDurableObject(stub, (instance) => instance.alarm());
    await expect(
      runInDurableObject(stub, (_instance, state) =>
        fenceRows(state.storage.sql)
      )
    ).resolves.toEqual([
      {
        upload_id: "owed-upload",
        updated_at: updatedAt,
        expires_at: updatedAt + MULTIPART_MAX_TTL_MS,
      },
    ]);
  });

  it("pages the backfill and the reclaim across invocations", async () => {
    const stub = shardStub("schema-fence-expiry-paged");
    const total = 2 * MULTIPART_FENCE_PAGE_LIMIT + 88;
    const updatedAt =
      Date.now() - MULTIPART_MAX_TTL_MS - MULTIPART_FENCE_GC_GRACE_MS - 2_000;

    await runInDurableObject(stub, async (instance: ShardDO, state) => {
      const sql = state.storage.sql;
      const internals = instance as unknown as InitializableDO;
      sql.exec(LEGACY_FENCES_DDL);
      for (let index = 0; index < total; index++) {
        sql.exec(
          "INSERT INTO multipart_fences VALUES (?, 'legacy-fence', 'aborting', ?)",
          `legacy-upload-${index.toString().padStart(4, "0")}`,
          updatedAt
        );
      }

      const withDeadline = (): number =>
        (
          sql
            .exec(
              "SELECT COUNT(*) AS n FROM multipart_fences WHERE expires_at IS NOT NULL"
            )
            .toArray()[0] as { n: number }
        ).n;
      const remaining = (): number =>
        (
          sql
            .exec("SELECT COUNT(*) AS n FROM multipart_fences")
            .toArray()[0] as { n: number }
        ).n;

      // A cold start backfills one page and leaves the rest for the next
      // one, so no single invocation is unbounded.
      for (const expected of [
        MULTIPART_FENCE_PAGE_LIMIT,
        2 * MULTIPART_FENCE_PAGE_LIMIT,
        total,
      ]) {
        internals.initialized = false;
        internals.ensureInit();
        expect(withDeadline()).toBe(expected);
      }

      // Reclaiming is paged the same way, oldest deadline first.
      for (const expected of [total - MULTIPART_FENCE_PAGE_LIMIT, 88, 0]) {
        await instance.alarm();
        expect(remaining()).toBe(expected);
      }
    });
  });
});

/**
 * Placement versioning arrived after multipart sessions were already
 * open on deployed instances. Those sessions staged chunks wherever
 * rendezvous hashing put them, so the column has to read back as v1 for
 * every row that predates it — otherwise finalize would look for their
 * chunks on shards that never received any.
 */
describe("upload session placement-version migration", () => {
  const LEGACY_SESSIONS_DDL = `
    CREATE TABLE upload_sessions (
      upload_id            TEXT PRIMARY KEY,
      user_id              TEXT NOT NULL,
      parent_id            TEXT,
      leaf                 TEXT NOT NULL,
      total_size           INTEGER NOT NULL,
      total_chunks         INTEGER NOT NULL,
      chunk_size           INTEGER NOT NULL,
      pool_size            INTEGER NOT NULL,
      expires_at           INTEGER NOT NULL,
      status               TEXT NOT NULL,
      encryption_mode      TEXT,
      encryption_key_id    TEXT,
      metadata_blob        BLOB,
      tags_json            TEXT,
      version_label        TEXT,
      version_user_visible INTEGER,
      mode                 INTEGER NOT NULL,
      mime_type            TEXT NOT NULL,
      created_at           INTEGER NOT NULL
    )
  `;

  it("defaults pre-upgrade sessions to legacy rendezvous placement", async () => {
    const stub = userStub("schema-placement-version");

    const migrated = await runInDurableObject(stub, (instance: UserDO, state) => {
      const sql = state.storage.sql;
      const internals = instance as unknown as InitializableDO;
      sql.exec(LEGACY_SESSIONS_DDL);
      sql.exec(
        `INSERT INTO upload_sessions
           (upload_id, user_id, leaf, total_size, total_chunks, chunk_size,
            pool_size, expires_at, status, mode, mime_type, created_at)
         VALUES ('legacy-session', 'tenant-a', 'x.bin', 1, 1, 1, 32, ?, 'open',
                 420, 'application/octet-stream', ?)`,
        Date.now() + 60_000,
        Date.now()
      );

      internals.ensureInit();
      return {
        row: sql
          .exec(
            "SELECT placement_version FROM upload_sessions WHERE upload_id = 'legacy-session'"
          )
          .toArray()[0],
        migration: sql
          .exec(
            `SELECT name FROM meta_schema
              WHERE name = 'upload_sessions_add_placement_version'`
          )
          .toArray(),
      };
    });

    expect(migrated).toEqual({
      row: { placement_version: MULTIPART_LEGACY_PLACEMENT_VERSION },
      migration: [{ name: "upload_sessions_add_placement_version" }],
    });
  });
});

/**
 * Paged abort arrived after multipart sessions were already being aborted on
 * deployed instances. A session the previous server left mid-abort has already
 * fenced some of its shards, so the machine has to adopt it at the first phase
 * rather than treat an absent phase as a corrupt row — and a session that
 * already reached its terminal status has to read back as terminal, so no
 * later reader has to special-case the rows this migration found.
 */
describe("upload session abort-phase migration", () => {
  const LEGACY_SESSIONS_DDL = `
    CREATE TABLE upload_sessions (
      upload_id            TEXT PRIMARY KEY,
      user_id              TEXT NOT NULL,
      parent_id            TEXT,
      leaf                 TEXT NOT NULL,
      total_size           INTEGER NOT NULL,
      total_chunks         INTEGER NOT NULL,
      chunk_size           INTEGER NOT NULL,
      pool_size            INTEGER NOT NULL,
      expires_at           INTEGER NOT NULL,
      status               TEXT NOT NULL,
      encryption_mode      TEXT,
      encryption_key_id    TEXT,
      metadata_blob        BLOB,
      tags_json            TEXT,
      version_label        TEXT,
      version_user_visible INTEGER,
      mode                 INTEGER NOT NULL,
      mime_type            TEXT NOT NULL,
      created_at           INTEGER NOT NULL
    )
  `;

  it("arms a session left mid-abort and labels the terminal ones", async () => {
    const stub = userStub("schema-abort-phase");

    const migrated = await runInDurableObject(stub, (instance: UserDO, state) => {
      const sql = state.storage.sql;
      const internals = instance as unknown as InitializableDO;
      sql.exec(LEGACY_SESSIONS_DDL);
      for (const [uploadId, status] of [
        ["mid-abort", "aborting"],
        ["already-aborted", "aborted"],
        ["already-poisoned", "poisoned"],
        ["still-open", "open"],
      ]) {
        sql.exec(
          `INSERT INTO upload_sessions
             (upload_id, user_id, leaf, total_size, total_chunks, chunk_size,
              pool_size, expires_at, status, mode, mime_type, created_at)
           VALUES (?, 'tenant-a', 'x.bin', 1, 1, 1, 32, ?, ?, 420,
                   'application/octet-stream', ?)`,
          uploadId,
          Date.now() + 60_000,
          status,
          Date.now()
        );
      }

      internals.ensureInit();
      return {
        rows: sql
          .exec(
            `SELECT upload_id, abort_phase, abort_fence_cursor,
                    abort_old_intent_cursor, abort_retry_at
               FROM upload_sessions ORDER BY upload_id`
          )
          .toArray(),
        migration: sql
          .exec(
            `SELECT name FROM meta_schema
              WHERE name = 'upload_sessions_adopt_abort_phase'`
          )
          .toArray(),
      };
    });

    expect(migrated).toEqual({
      rows: [
        {
          upload_id: "already-aborted",
          abort_phase: "done",
          abort_fence_cursor: 0,
          abort_old_intent_cursor: -1,
          abort_retry_at: 0,
        },
        {
          upload_id: "already-poisoned",
          abort_phase: "done",
          abort_fence_cursor: 0,
          abort_old_intent_cursor: -1,
          abort_retry_at: 0,
        },
        {
          upload_id: "mid-abort",
          abort_phase: "fencing",
          abort_fence_cursor: 0,
          abort_old_intent_cursor: -1,
          abort_retry_at: 0,
        },
        {
          upload_id: "still-open",
          abort_phase: null,
          abort_fence_cursor: 0,
          abort_old_intent_cursor: -1,
          abort_retry_at: 0,
        },
      ],
      migration: [{ name: "upload_sessions_adopt_abort_phase" }],
    });
  });
});
