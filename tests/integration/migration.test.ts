import { describe, it, expect } from "vitest";
import { env, runInDurableObject } from "cloudflare:test";

/**
 * Phase 1 — Schema migration tests.
 *
 * Verifies sdk-impl-plan §3.1 / §3.2:
 * - new columns (mode, inline_data, symlink_target, node_kind on files;
 *   mode on folders; deleted_at on chunks) appear with correct defaults
 * - partial unique indexes on (parent_id, name) exist
 * - lookup indexes (idx_files_parent, idx_folders_parent, idx_chunks_deleted,
 *   idx_chunk_refs_file) exist
 * - ensureInit is idempotent — running twice does not throw
 * - legacy rows (without the new columns) read back with safe defaults
 */

interface Env {
  USER_DO: DurableObjectNamespace;
  SHARD_DO: DurableObjectNamespace;
}

const E = env as unknown as Env;

function userStub(name: string) {
  return E.USER_DO.get(E.USER_DO.idFromName(name));
}
function shardStub(name: string) {
  return E.SHARD_DO.get(E.SHARD_DO.idFromName(name));
}

describe("UserDO schema migrations", () => {
  it("adds new columns with correct defaults to a fresh DO", async () => {
    const stub = userStub("migration:fresh");

    // First request triggers ensureInit.
    const res = await stub.fetch(
      new Request("http://internal/quota", {
        method: "POST",
        body: JSON.stringify({ userId: "u1" }),
      })
    );
    expect(res.ok).toBe(true);

    // Inspect schema directly.
    await runInDurableObject(stub, async (_instance, state) => {
      const sql = state.storage.sql;

      // files columns
      const fileCols = sql
        .exec("PRAGMA table_info(files)")
        .toArray() as { name: string; dflt_value: string | null }[];
      const colNames = fileCols.map((c) => c.name);
      expect(colNames).toContain("mode");
      expect(colNames).toContain("inline_data");
      expect(colNames).toContain("symlink_target");
      expect(colNames).toContain("node_kind");

      // mode default = 420 (0o644), node_kind default = 'file'
      const modeDef = fileCols.find((c) => c.name === "mode")?.dflt_value;
      expect(String(modeDef)).toBe("420");
      const kindDef = fileCols.find((c) => c.name === "node_kind")?.dflt_value;
      // SQLite stores TEXT defaults quoted
      expect(String(kindDef).replace(/'/g, "")).toBe("file");

      // folders.mode default = 493 (0o755)
      const folderCols = sql
        .exec("PRAGMA table_info(folders)")
        .toArray() as { name: string; dflt_value: string | null }[];
      expect(folderCols.map((c) => c.name)).toContain("mode");
      const folderModeDef = folderCols.find((c) => c.name === "mode")
        ?.dflt_value;
      expect(String(folderModeDef)).toBe("493");

      // unique partial indexes exist
      const indexes = sql
        .exec("SELECT name FROM sqlite_master WHERE type='index'")
        .toArray() as { name: string }[];
      const idxNames = indexes.map((i) => i.name);
      expect(idxNames).toContain("uniq_files_parent_name");
      expect(idxNames).toContain("uniq_folders_parent_name");
      expect(idxNames).toContain("idx_files_parent");
      expect(idxNames).toContain("idx_folders_parent");
    });
  });

  it("is idempotent — running ensureInit twice does not throw", async () => {
    const stub = userStub("migration:idempotent");

    // Trigger init twice through two requests. The second one runs the
    // in-memory `initialized` flag short-circuit.
    const r1 = await stub.fetch(
      new Request("http://internal/quota", {
        method: "POST",
        body: JSON.stringify({ userId: "u1" }),
      })
    );
    expect(r1.ok).toBe(true);
    const r2 = await stub.fetch(
      new Request("http://internal/quota", {
        method: "POST",
        body: JSON.stringify({ userId: "u1" }),
      })
    );
    expect(r2.ok).toBe(true);

    // Force a re-run of the migrations in a *fresh* call by directly invoking
    // the same SQL the migrations issue. This proves try/catch absorbs the
    // duplicate-column errors.
    await runInDurableObject(stub, async (_instance, state) => {
      const sql = state.storage.sql;

      // ALTER should throw "duplicate column name" because the column
      // already exists from the first ensureInit. We assert the throw
      // happens AND can be swallowed.
      let threw = false;
      try {
        sql.exec("ALTER TABLE files ADD COLUMN mode INTEGER NOT NULL DEFAULT 420");
      } catch {
        threw = true;
      }
      expect(threw).toBe(true);

      // CREATE INDEX IF NOT EXISTS is naturally idempotent.
      sql.exec(`
        CREATE UNIQUE INDEX IF NOT EXISTS uniq_files_parent_name
          ON files(user_id, IFNULL(parent_id, ''), file_name)
          WHERE status != 'deleted'
      `);
      sql.exec(`
        CREATE INDEX IF NOT EXISTS idx_files_parent
          ON files(user_id, parent_id, status)
      `);
    });
  });

  it("legacy rows (no inline_data) read back with default node_kind='file' and inlineData=null", async () => {
    const stub = userStub("migration:legacy");

    // Signup to get a user, then create a file via the existing legacy
    // /files/create + /files/chunk + /files/complete path. The new columns
    // should be auto-populated with defaults — `node_kind='file'`,
    // `inline_data=NULL`, `mode=420`.
    const signup = await stub.fetch(
      new Request("http://internal/signup", {
        method: "POST",
        body: JSON.stringify({
          email: "legacy@example.com",
          password: "password123",
        }),
      })
    );
    expect(signup.ok).toBe(true);
    const { userId } = (await signup.json()) as { userId: string };

    const create = await stub.fetch(
      new Request("http://internal/files/create", {
        method: "POST",
        body: JSON.stringify({
          userId,
          fileName: "legacy.txt",
          fileSize: 11,
          mimeType: "text/plain",
          parentId: null,
        }),
      })
    );
    expect(create.ok).toBe(true);
    const created = (await create.json()) as { fileId: string };

    // Record one chunk + complete
    await stub.fetch(
      new Request("http://internal/files/chunk", {
        method: "POST",
        body: JSON.stringify({
          fileId: created.fileId,
          chunkIndex: 0,
          chunkHash: "deadbeef".repeat(8),
          chunkSize: 11,
          shardIndex: 0,
        }),
      })
    );
    await stub.fetch(
      new Request("http://internal/files/complete", {
        method: "POST",
        body: JSON.stringify({
          fileId: created.fileId,
          fileHash: "feedface".repeat(8),
          userId,
          fileSize: 11,
        }),
      })
    );

    // Read back the manifest — should include the new fields with defaults.
    const manRes = await stub.fetch(
      new Request(`http://internal/files/manifest/${created.fileId}`)
    );
    expect(manRes.ok).toBe(true);
    const manifest = (await manRes.json()) as {
      mode?: number;
      nodeKind?: string;
      symlinkTarget?: string | null;
      inlineData?: ArrayBuffer | null;
      chunks: { index: number }[];
    };
    expect(manifest.mode).toBe(420);
    expect(manifest.nodeKind).toBe("file");
    expect(manifest.symlinkTarget).toBeNull();
    expect(manifest.inlineData).toBeNull();
    expect(manifest.chunks).toHaveLength(1);
  });
});

describe("ShardDO schema migrations", () => {
  it("adds deleted_at column and GC indexes", async () => {
    const stub = shardStub("migration:shard:fresh");

    // Trigger init.
    const res = await stub.fetch(new Request("http://internal/stats"));
    expect(res.ok).toBe(true);

    await runInDurableObject(stub, async (_instance, state) => {
      const sql = state.storage.sql;

      const cols = sql
        .exec("PRAGMA table_info(chunks)")
        .toArray() as { name: string }[];
      expect(cols.map((c) => c.name)).toContain("deleted_at");

      const indexes = sql
        .exec("SELECT name FROM sqlite_master WHERE type='index'")
        .toArray() as { name: string }[];
      const idxNames = indexes.map((i) => i.name);
      expect(idxNames).toContain("idx_chunks_deleted");
      expect(idxNames).toContain("idx_chunk_refs_file");
    });
  });

  it("ensureInit is idempotent on ShardDO", async () => {
    const stub = shardStub("migration:shard:idempotent");

    const r1 = await stub.fetch(new Request("http://internal/stats"));
    expect(r1.ok).toBe(true);
    const r2 = await stub.fetch(new Request("http://internal/stats"));
    expect(r2.ok).toBe(true);

    await runInDurableObject(stub, async (_instance, state) => {
      const sql = state.storage.sql;
      let threw = false;
      try {
        sql.exec("ALTER TABLE chunks ADD COLUMN deleted_at INTEGER");
      } catch {
        threw = true;
      }
      expect(threw).toBe(true);

      sql.exec(`
        CREATE INDEX IF NOT EXISTS idx_chunks_deleted
          ON chunks(deleted_at) WHERE deleted_at IS NOT NULL
      `);
    });
  });
});
