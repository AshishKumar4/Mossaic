import { describe, it, expect } from "vitest";
import { env, runInDurableObject } from "cloudflare:test";

/**
 * Phase 6 — admin.dedupePaths regression test.
 *
 * Phase 1's UNIQUE partial index on (user_id, parent_id, file_name)
 * WHERE status != 'deleted' is created lazily in ensureInit. If a
 * legacy tenant has live duplicates pre-migration, the index
 * creation throws and is swallowed. dedupePaths(userId, scope?) is
 * the manual cleanup pass:
 *
 *   - Finds groups of (parent_id, name) with >1 live rows
 *   - Keeps newest row (max updated_at, ties broken by file_id desc)
 *   - Hard-deletes the rest + dispatches deleteChunks RPC per shard
 *   - Re-creates the UNIQUE indexes
 *
 * Tests cover:
 *   1. No-op on a clean DO (zero dupes; index already exists)
 *   2. File dupes resolved correctly (winner = newest; losers gone)
 *   3. Folder dupes resolved when no children
 *   4. Folder dupes skipped when loser has children
 *   5. ShardDO.deleteChunks dispatched per touched shard
 *   6. Race-safe: dedupePaths during a concurrent VFS write doesn't
 *      delete the in-flight tmp row (status='uploading' is filtered)
 */

import { vfsShardDOName, vfsUserDOName } from "@core/lib/utils";

interface E {
  USER_DO: DurableObjectNamespace;
  SHARD_DO: DurableObjectNamespace;
}
const E = env as unknown as E;

const NS = "default";

function userStub(tenant: string) {
  return E.USER_DO.get(E.USER_DO.idFromName(vfsUserDOName(NS, tenant)));
}

function shardStub(tenant: string, idx: number) {
  return E.SHARD_DO.get(
    E.SHARD_DO.idFromName(vfsShardDOName(NS, tenant, undefined, idx))
  );
}

describe("admin.dedupePaths", () => {
  it("no-op on a clean DO with no duplicates", async () => {
    const tenant = "dedupe-clean";
    const stub = userStub(tenant);
    // Trigger init by issuing one VFS call.
    const scope = { ns: NS, tenant };
    await stub.vfsMkdir(scope, "/work");
    await stub.vfsWriteFile(scope, "/a.txt", new TextEncoder().encode("a"));

    const result = await stub.adminDedupePaths(tenant, scope);
    expect(result.fileDupesResolved).toBe(0);
    expect(result.folderDupesResolved).toBe(0);
    expect(result.folderDupesSkipped).toBe(0);
    expect(result.uniqFilesIndex).toBe(true);
    expect(result.uniqFoldersIndex).toBe(true);

    // Sanity: the file is still readable.
    const back = await stub.vfsReadFile(scope, "/a.txt");
    expect(new TextDecoder().decode(back)).toBe("a");
  });

  it("resolves file duplicates, keeps newest, hard-deletes losers", async () => {
    const tenant = "dedupe-files";
    const stub = userStub(tenant);
    const scope = { ns: NS, tenant };
    // Trigger init.
    await stub.vfsExists(scope, "/");

    // Drop the unique index so we can SQL-seed duplicate rows. The
    // index would otherwise reject the second INSERT.
    await runInDurableObject(stub, async (_inst, state) => {
      const sql = state.storage.sql;
      sql.exec("DROP INDEX IF EXISTS uniq_files_parent_name");
      // Insert 3 duplicate rows for /collide.txt, all status='complete'.
      // Different file_ids, different updated_at — newest is "ccc-id".
      const base = Date.now();
      for (const [id, ts] of [
        ["aaa-id", base - 3000],
        ["bbb-id", base - 2000],
        ["ccc-id", base - 1000],
      ] as [string, number][]) {
        sql.exec(
          `INSERT INTO files (file_id, user_id, parent_id, file_name, file_size, file_hash, mime_type, chunk_size, chunk_count, pool_size, status, created_at, updated_at, mode, node_kind, inline_data)
           VALUES (?, ?, NULL, 'collide.txt', 1, '', 'text/plain', 0, 0, 32, 'complete', ?, ?, 420, 'file', ?)`,
          id,
          tenant,
          ts,
          ts,
          new TextEncoder().encode(id.charAt(0))
        );
      }
    });

    const result = await stub.adminDedupePaths(tenant, scope);
    expect(result.fileDupesResolved).toBe(2);
    expect(result.uniqFilesIndex).toBe(true);

    // The winner ("ccc-id" with newest updated_at) survives. Read
    // /collide.txt and confirm we got "c" (its inline payload).
    const back = await stub.vfsReadFile(scope, "/collide.txt");
    expect(new TextDecoder().decode(back)).toBe("c");

    // The losers are gone from `files`.
    const remaining = await runInDurableObject(stub, async (_inst, state) => {
      return state.storage.sql
        .exec(
          "SELECT file_id FROM files WHERE file_name = 'collide.txt' ORDER BY file_id"
        )
        .toArray() as { file_id: string }[];
    });
    expect(remaining.map((r) => r.file_id)).toEqual(["ccc-id"]);
  });

  it("resolves folder duplicates when loser has no children", async () => {
    const tenant = "dedupe-folders-empty";
    const stub = userStub(tenant);
    const scope = { ns: NS, tenant };
    await stub.vfsExists(scope, "/");

    await runInDurableObject(stub, async (_inst, state) => {
      const sql = state.storage.sql;
      sql.exec("DROP INDEX IF EXISTS uniq_folders_parent_name");
      const base = Date.now();
      sql.exec(
        `INSERT INTO folders (folder_id, user_id, parent_id, name, created_at, updated_at, mode)
         VALUES ('f-old', ?, NULL, 'work', ?, ?, 493)`,
        tenant,
        base - 5000,
        base - 5000
      );
      sql.exec(
        `INSERT INTO folders (folder_id, user_id, parent_id, name, created_at, updated_at, mode)
         VALUES ('f-new', ?, NULL, 'work', ?, ?, 493)`,
        tenant,
        base - 1000,
        base - 1000
      );
    });

    const result = await stub.adminDedupePaths(tenant, scope);
    expect(result.folderDupesResolved).toBe(1);
    expect(result.folderDupesSkipped).toBe(0);
    expect(result.uniqFoldersIndex).toBe(true);

    // Only the newest folder survives.
    const remaining = await runInDurableObject(stub, async (_inst, state) => {
      return state.storage.sql
        .exec("SELECT folder_id FROM folders WHERE name = 'work'")
        .toArray() as { folder_id: string }[];
    });
    expect(remaining.map((r) => r.folder_id)).toEqual(["f-new"]);
  });

  it("skips folder duplicates when loser has children (safety)", async () => {
    const tenant = "dedupe-folders-with-children";
    const stub = userStub(tenant);
    const scope = { ns: NS, tenant };
    await stub.vfsExists(scope, "/");

    await runInDurableObject(stub, async (_inst, state) => {
      const sql = state.storage.sql;
      sql.exec("DROP INDEX IF EXISTS uniq_folders_parent_name");
      const base = Date.now();
      // Old folder with a child; new folder without.
      sql.exec(
        `INSERT INTO folders (folder_id, user_id, parent_id, name, created_at, updated_at, mode)
         VALUES ('f-old-with-kid', ?, NULL, 'work', ?, ?, 493)`,
        tenant,
        base - 5000,
        base - 5000
      );
      sql.exec(
        `INSERT INTO folders (folder_id, user_id, parent_id, name, created_at, updated_at, mode)
         VALUES ('f-new', ?, NULL, 'work', ?, ?, 493)`,
        tenant,
        base - 1000,
        base - 1000
      );
      // Child of the OLD folder — newest wins by updated_at, so the
      // OLD one is the loser. Loser has a child → skip.
      sql.exec(
        `INSERT INTO files (file_id, user_id, parent_id, file_name, file_size, file_hash, mime_type, chunk_size, chunk_count, pool_size, status, created_at, updated_at, mode, node_kind, inline_data)
         VALUES ('child-id', ?, 'f-old-with-kid', 'inside.txt', 1, '', 'text/plain', 0, 0, 32, 'complete', ?, ?, 420, 'file', ?)`,
        tenant,
        base - 4000,
        base - 4000,
        new TextEncoder().encode("k")
      );
    });

    const result = await stub.adminDedupePaths(tenant, scope);
    expect(result.folderDupesSkipped).toBe(1);
    expect(result.folderDupesResolved).toBe(0);
    // Both folder rows still exist; the unique index can't be
    // created while dupes remain.
    expect(result.uniqFoldersIndex).toBe(false);
    const remaining = await runInDurableObject(stub, async (_inst, state) => {
      return state.storage.sql
        .exec("SELECT folder_id FROM folders WHERE name = 'work' ORDER BY folder_id")
        .toArray() as { folder_id: string }[];
    });
    expect(remaining.map((r) => r.folder_id).sort()).toEqual([
      "f-new",
      "f-old-with-kid",
    ]);
  });

  it("dispatches deleteChunks RPC per touched shard for chunked dupes", async () => {
    const tenant = "dedupe-chunks";
    const stub = userStub(tenant);
    const scope = { ns: NS, tenant };
    await stub.vfsExists(scope, "/");

    // Seed two duplicate chunked-tier rows. Each loser has chunks on
    // shard 0. After dedupePaths, the loser's chunk_refs should be
    // gone from the shard.
    await runInDurableObject(stub, async (_inst, state) => {
      const sql = state.storage.sql;
      sql.exec("DROP INDEX IF EXISTS uniq_files_parent_name");
      const base = Date.now();
      // Loser
      sql.exec(
        `INSERT INTO files (file_id, user_id, parent_id, file_name, file_size, file_hash, mime_type, chunk_size, chunk_count, pool_size, status, created_at, updated_at, mode, node_kind)
         VALUES ('loser-id', ?, NULL, 'big.bin', 5, '', 'application/octet-stream', 5, 1, 32, 'complete', ?, ?, 420, 'file')`,
        tenant,
        base - 5000,
        base - 5000
      );
      sql.exec(
        `INSERT INTO file_chunks (file_id, chunk_index, chunk_hash, chunk_size, shard_index)
         VALUES ('loser-id', 0, 'loser-hash', 5, 0)`
      );
      // Winner
      sql.exec(
        `INSERT INTO files (file_id, user_id, parent_id, file_name, file_size, file_hash, mime_type, chunk_size, chunk_count, pool_size, status, created_at, updated_at, mode, node_kind)
         VALUES ('winner-id', ?, NULL, 'big.bin', 5, '', 'application/octet-stream', 5, 1, 32, 'complete', ?, ?, 420, 'file')`,
        tenant,
        base - 1000,
        base - 1000
      );
      sql.exec(
        `INSERT INTO file_chunks (file_id, chunk_index, chunk_hash, chunk_size, shard_index)
         VALUES ('winner-id', 0, 'winner-hash', 5, 0)`
      );
    });

    // Seed actual chunk_refs rows on the shard for both files so
    // deleteChunks has work to do.
    const shard = shardStub(tenant, 0);
    await shard.putChunk(
      "loser-hash",
      new TextEncoder().encode("loser"),
      "loser-id",
      0,
      tenant
    );
    await shard.putChunk(
      "winner-hash",
      new TextEncoder().encode("winnr"),
      "winner-id",
      0,
      tenant
    );

    const result = await stub.adminDedupePaths(tenant, scope);
    expect(result.fileDupesResolved).toBe(1);

    // The loser's chunk_refs row is gone from the shard.
    const refsLeft = await runInDurableObject(shard, async (_inst, state) => {
      return state.storage.sql
        .exec("SELECT file_id FROM chunk_refs ORDER BY file_id")
        .toArray() as { file_id: string }[];
    });
    expect(refsLeft.map((r) => r.file_id)).toEqual(["winner-id"]);
  });

  it("does NOT touch in-flight uploading rows (race-safe)", async () => {
    const tenant = "dedupe-race";
    const stub = userStub(tenant);
    const scope = { ns: NS, tenant };

    // Open a write stream — handle row has status='uploading' and a
    // tmp file_name. dedupePaths only considers status != 'deleted'
    // *and* groups by name; the tmp name is unique so it can't
    // appear in any dupe group anyway. We still assert it survives.
    const handle = await stub.vfsBeginWriteStream(scope, "/in-flight.bin");

    // Create a separate dupe situation on a DIFFERENT path while
    // the handle is open.
    await runInDurableObject(stub, async (_inst, state) => {
      const sql = state.storage.sql;
      sql.exec("DROP INDEX IF EXISTS uniq_files_parent_name");
      const base = Date.now();
      for (const id of ["dupe-a", "dupe-b"]) {
        sql.exec(
          `INSERT INTO files (file_id, user_id, parent_id, file_name, file_size, file_hash, mime_type, chunk_size, chunk_count, pool_size, status, created_at, updated_at, mode, node_kind, inline_data)
           VALUES (?, ?, NULL, 'other.txt', 1, '', 'text/plain', 0, 0, 32, 'complete', ?, ?, 420, 'file', ?)`,
          id,
          tenant,
          base - (id === "dupe-a" ? 5000 : 1000),
          base - (id === "dupe-a" ? 5000 : 1000),
          new TextEncoder().encode("x")
        );
      }
    });

    const result = await stub.adminDedupePaths(tenant, scope);
    expect(result.fileDupesResolved).toBe(1); // only "other.txt" had a dupe

    // The in-flight tmp row survives unscathed.
    const tmpRow = await runInDurableObject(stub, async (_inst, state) => {
      return state.storage.sql
        .exec("SELECT file_id, status FROM files WHERE file_id = ?", handle.tmpId)
        .toArray()[0] as { file_id: string; status: string } | undefined;
    });
    expect(tmpRow).toBeTruthy();
    expect(tmpRow!.status).toBe("uploading");

    // Cleanup the handle so we don't leak.
    await stub.vfsAbortWriteStream(scope, handle);
  });
});
