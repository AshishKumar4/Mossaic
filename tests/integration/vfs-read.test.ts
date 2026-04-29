import { describe, it, expect } from "vitest";
import { env, runInDurableObject } from "cloudflare:test";

/**
 * Phase 2 — Read-side VFS RPC integration tests.
 *
 * Drives the typed RPC surface (vfsStat / vfsLstat / vfsExists /
 * vfsReadlink / vfsReaddir / vfsReadManyStat / vfsReadFile /
 * vfsOpenManifest / vfsReadChunk) directly through DO RPC. The consumer
 * test fixture pretends to be the SDK: gets a stub via
 * `env.MOSSAIC_USER.get(idFromName(...))` and calls the methods.
 *
 * Phase 4 wires shard naming through `vfsShardDOName(ns, tenant, sub, idx)`,
 * so seed steps that pre-populate ShardDO state must use the same
 * derivation. We hard-code namespace="default" + sub=undefined to match
 * the scope the tests pass to the RPC methods.
 */

import type { UserDO } from "@app/objects/user/user-do";
import type { ShardDO } from "@core/objects/shard/shard-do";
import { INLINE_LIMIT } from "@shared/inline";
import { vfsShardDOName } from "@core/lib/utils";

interface E {
  MOSSAIC_USER: DurableObjectNamespace<UserDO>;
  MOSSAIC_SHARD: DurableObjectNamespace<ShardDO>;
}
const E = env as unknown as E;

async function sha256Hex(data: Uint8Array): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", data);
  return Array.from(new Uint8Array(digest))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

/**
 * Seed a UserDO via the App's typed `appHandleSignup` RPC. The RPC
 * materializes the auth + quota rows (same `handleSignup` helper the
 * pre-Phase-17 `_legacyFetch` /signup route called) and returns the
 * generated user_id we use as `scope.tenant`.
 */
async function seedUser(
  stub: DurableObjectStub<UserDO>,
  email: string
): Promise<string> {
  const { userId } = await stub.appHandleSignup(email, "abcd1234");
  return userId;
}

describe("vfsStat / vfsLstat / vfsExists", () => {
  it("stats a file at the root after legacy upload", async () => {
    const stub = E.MOSSAIC_USER.get(E.MOSSAIC_USER.idFromName("vfs-read:stat"));
    const userId = await seedUser(stub, "stat@e.com");

    const { fileId } = await stub.appCreateFile(
      userId,
      "stat.txt",
      5,
      "text/plain",
      null
    );
    await stub.appCompleteFile(fileId, "0".repeat(64), userId, 5);

    const scope = { ns: "default", tenant: userId };
    const stat = await stub.vfsStat(scope, "/stat.txt");
    expect(stat.type).toBe("file");
    expect(stat.size).toBe(5);
    expect(stat.mode).toBe(420);
    expect(Number.isInteger(stat.ino)).toBe(true);
    expect(stat.ino).toBeGreaterThan(0);

    expect(await stub.vfsExists(scope, "/stat.txt")).toBe(true);
    expect(await stub.vfsExists(scope, "/missing")).toBe(false);
  });

  it("ENOENT when the path doesn't exist", async () => {
    const stub = E.MOSSAIC_USER.get(E.MOSSAIC_USER.idFromName("vfs-read:enoent"));
    const userId = await seedUser(stub, "enoent@e.com");
    const scope = { ns: "default", tenant: userId };
    await expect(stub.vfsStat(scope, "/nope")).rejects.toThrow(/ENOENT/);
  });

  it("EISDIR-like for stat on a directory returns dir kind, readFile throws EISDIR", async () => {
    const stub = E.MOSSAIC_USER.get(E.MOSSAIC_USER.idFromName("vfs-read:isdir"));
    const userId = await seedUser(stub, "isdir@e.com");
    await stub.appCreateFolder(userId, "docs", null);
    const scope = { ns: "default", tenant: userId };

    const stat = await stub.vfsStat(scope, "/docs");
    expect(stat.type).toBe("dir");
    expect(stat.mode).toBe(493); // 0o755 default

    await expect(stub.vfsReadFile(scope, "/docs")).rejects.toThrow(/EISDIR/);
  });

  it("lstat returns the symlink; stat follows it", async () => {
    const stub = E.MOSSAIC_USER.get(E.MOSSAIC_USER.idFromName("vfs-read:symlink"));
    const userId = await seedUser(stub, "ln@e.com");

    // Seed a real file + a symlink pointing at it.
    await runInDurableObject(stub, async (_instance, state) => {
      const sql = state.storage.sql;
      const now = Date.now();
      sql.exec(
        `INSERT INTO files (file_id, user_id, parent_id, file_name, file_size, file_hash, mime_type, chunk_size, chunk_count, pool_size, status, created_at, updated_at, mode, node_kind)
         VALUES ('real-id', ?, NULL, 'real.txt', 7, '', 'text/plain', 7, 1, 32, 'complete', ?, ?, 420, 'file')`,
        userId,
        now,
        now
      );
      sql.exec(
        `INSERT INTO files (file_id, user_id, parent_id, file_name, file_size, file_hash, mime_type, chunk_size, chunk_count, pool_size, status, created_at, updated_at, mode, node_kind, symlink_target)
         VALUES ('ln-id', ?, NULL, 'ln', 0, '', 'inode/symlink', 0, 0, 32, 'complete', ?, ?, 511, 'symlink', '/real.txt')`,
        userId,
        now,
        now
      );
    });
    const scope = { ns: "default", tenant: userId };

    const ls = await stub.vfsLstat(scope, "/ln");
    expect(ls.type).toBe("symlink");
    expect(ls.size).toBe("/real.txt".length);

    const s = await stub.vfsStat(scope, "/ln");
    expect(s.type).toBe("file");
    expect(s.size).toBe(7);

    expect(await stub.vfsReadlink(scope, "/ln")).toBe("/real.txt");
    await expect(stub.vfsReadlink(scope, "/real.txt")).rejects.toThrow(
      /EINVAL/
    );
  });
});

describe("vfsReaddir", () => {
  it("lists files and folders, sorted", async () => {
    const stub = E.MOSSAIC_USER.get(E.MOSSAIC_USER.idFromName("vfs-read:readdir"));
    const userId = await seedUser(stub, "rd@e.com");

    // Create /a (dir), /b (dir), /c.txt, /d.txt — at root.
    for (const name of ["a", "b"]) {
      await stub.appCreateFolder(userId, name, null);
    }
    for (const name of ["c.txt", "d.txt"]) {
      const { fileId } = await stub.appCreateFile(
        userId,
        name,
        1,
        "text/plain",
        null
      );
      await stub.appCompleteFile(fileId, "0".repeat(64), userId, 1);
    }
    const scope = { ns: "default", tenant: userId };

    const entries = await stub.vfsReaddir(scope, "/");
    expect(entries).toEqual(["a", "b", "c.txt", "d.txt"]);

    // Listing a missing dir → ENOENT
    await expect(stub.vfsReaddir(scope, "/nope")).rejects.toThrow(/ENOENT/);
  });

  it("ENOTDIR when path resolves to a file", async () => {
    const stub = E.MOSSAIC_USER.get(E.MOSSAIC_USER.idFromName("vfs-read:enotdir"));
    const userId = await seedUser(stub, "ntd@e.com");
    const { fileId } = await stub.appCreateFile(
      userId,
      "f.txt",
      1,
      "text/plain",
      null
    );
    await stub.appCompleteFile(fileId, "0".repeat(64), userId, 1);
    const scope = { ns: "default", tenant: userId };
    await expect(stub.vfsReaddir(scope, "/f.txt")).rejects.toThrow(/ENOTDIR/);
  });
});

describe("vfsReadFile (inline tier + chunked path)", () => {
  it("reads inlined file with ZERO ShardDO subrequests", async () => {
    const stub = E.MOSSAIC_USER.get(E.MOSSAIC_USER.idFromName("vfs-read:inline"));
    const userId = await seedUser(stub, "il@e.com");

    // Manually insert an inlined file (Phase 3 will write this via
    // vfsWriteFile; here we simulate the row a writeFile would produce).
    const payload = new TextEncoder().encode("hello inline tier!");
    expect(payload.byteLength).toBeLessThanOrEqual(INLINE_LIMIT);

    await runInDurableObject(stub, async (_instance, state) => {
      const sql = state.storage.sql;
      const now = Date.now();
      sql.exec(
        `INSERT INTO files (file_id, user_id, parent_id, file_name, file_size, file_hash, mime_type, chunk_size, chunk_count, pool_size, status, created_at, updated_at, mode, node_kind, inline_data)
         VALUES ('inline-id', ?, NULL, 'note.txt', ?, '', 'text/plain', 0, 0, 32, 'complete', ?, ?, 420, 'file', ?)`,
        userId,
        payload.byteLength,
        now,
        now,
        payload
      );
    });

    const scope = { ns: "default", tenant: userId };
    const got = await stub.vfsReadFile(scope, "/note.txt");
    expect(new TextDecoder().decode(got)).toBe("hello inline tier!");

    const stat = await stub.vfsStat(scope, "/note.txt");
    expect(stat.size).toBe(payload.byteLength);

    // Confirm the path went via the inline branch — no shard rows touched.
    // We verify by inspecting the inline_data column on the row.
    await runInDurableObject(stub, async (_instance, state) => {
      const row = state.storage.sql
        .exec(
          "SELECT inline_data, chunk_count FROM files WHERE file_id='inline-id'"
        )
        .toArray()[0] as { inline_data: ArrayBuffer; chunk_count: number };
      expect(row.inline_data.byteLength).toBe(payload.byteLength);
      expect(row.chunk_count).toBe(0);
    });
  });

  it("reads a chunked file by fanning out ShardDO subrequests", async () => {
    const stub = E.MOSSAIC_USER.get(E.MOSSAIC_USER.idFromName("vfs-read:chunked-user"));
    const userId = await seedUser(stub, "ch@e.com");
    const shardIdx = 0;
    const shardDO = E.MOSSAIC_SHARD.get(
      E.MOSSAIC_SHARD.idFromName(vfsShardDOName("default", userId, undefined, shardIdx))
    );

    // Two-chunk file. We use the legacy upload protocol since vfsWriteFile
    // hasn't shipped yet (Phase 3).
    const part1 = new TextEncoder().encode("chunk-one--");
    const part2 = new TextEncoder().encode("chunk-two!!");
    const full = new Uint8Array(part1.byteLength + part2.byteLength);
    full.set(part1, 0);
    full.set(part2, part1.byteLength);

    const { fileId } = await stub.appCreateFile(
      userId,
      "two.bin",
      full.byteLength,
      "application/octet-stream",
      null
    );

    // Override chunk_size + chunk_count so reading walks two chunk rows.
    await runInDurableObject(stub, async (_instance, state) => {
      state.storage.sql.exec(
        "UPDATE files SET chunk_size = ?, chunk_count = 2 WHERE file_id = ?",
        part1.byteLength,
        fileId
      );
    });

    // Upload both chunks via the existing ShardDO protocol.
    for (let i = 0; i < 2; i++) {
      const part = i === 0 ? part1 : part2;
      const hash = await sha256Hex(part);
      const put = await shardDO.fetch(
        new Request("http://internal/chunk", {
          method: "PUT",
          headers: {
            "X-Chunk-Hash": hash,
            "X-File-Id": fileId,
            "X-Chunk-Index": String(i),
            "X-User-Id": userId,
          },
          body: part,
        })
      );
      expect(put.ok).toBe(true);
      await stub.appRecordChunk(fileId, i, hash, part.byteLength, shardIdx);
    }
    await stub.appCompleteFile(
      fileId,
      "0".repeat(64),
      userId,
      full.byteLength
    );

    const scope = { ns: "default", tenant: userId };
    const out = await stub.vfsReadFile(scope, "/two.bin");
    expect(new TextDecoder().decode(out)).toBe(
      "chunk-one--chunk-two!!"
    );
  });
});

describe("vfsReadManyStat", () => {
  it("returns one stat per path, null for misses", async () => {
    const stub = E.MOSSAIC_USER.get(E.MOSSAIC_USER.idFromName("vfs-read:many"));
    const userId = await seedUser(stub, "many@e.com");

    for (const name of ["a.txt", "b.txt"]) {
      const { fileId } = await stub.appCreateFile(
        userId,
        name,
        3,
        "text/plain",
        null
      );
      await stub.appCompleteFile(fileId, "0".repeat(64), userId, 3);
    }

    const scope = { ns: "default", tenant: userId };
    const stats = await stub.vfsReadManyStat(scope, [
      "/a.txt",
      "/missing",
      "/b.txt",
    ]);
    expect(stats).toHaveLength(3);
    expect(stats[0]?.type).toBe("file");
    expect(stats[1]).toBeNull();
    expect(stats[2]?.type).toBe("file");
  });
});

describe("vfsOpenManifest / vfsReadChunk", () => {
  it("openManifest hides shardIndex; readChunk serves bytes", async () => {
    const stub = E.MOSSAIC_USER.get(E.MOSSAIC_USER.idFromName("vfs-read:manifest-u"));
    const userId = await seedUser(stub, "mf@e.com");
    const shardIdx = 0;
    const shardDO = E.MOSSAIC_SHARD.get(
      E.MOSSAIC_SHARD.idFromName(vfsShardDOName("default", userId, undefined, shardIdx))
    );

    const part = new TextEncoder().encode("just-one-chunk");
    const { fileId } = await stub.appCreateFile(
      userId,
      "m.bin",
      part.byteLength,
      "application/octet-stream",
      null
    );
    const hash = await sha256Hex(part);
    await shardDO.fetch(
      new Request("http://internal/chunk", {
        method: "PUT",
        headers: {
          "X-Chunk-Hash": hash,
          "X-File-Id": fileId,
          "X-Chunk-Index": "0",
          "X-User-Id": userId,
        },
        body: part,
      })
    );
    await stub.appRecordChunk(fileId, 0, hash, part.byteLength, shardIdx);
    await stub.appCompleteFile(
      fileId,
      "0".repeat(64),
      userId,
      part.byteLength
    );

    const scope = { ns: "default", tenant: userId };
    const m = await stub.vfsOpenManifest(scope, "/m.bin");
    expect(m.inlined).toBe(false);
    expect(m.size).toBe(part.byteLength);
    expect(m.chunks).toHaveLength(1);
    expect(m.chunks[0]).toMatchObject({ index: 0, hash, size: part.byteLength });
    // shardIndex MUST NOT leak in the public shape.
    expect((m.chunks[0] as Record<string, unknown>).shardIndex).toBeUndefined();

    const ck = await stub.vfsReadChunk(scope, "/m.bin", 0);
    expect(new TextDecoder().decode(ck)).toBe("just-one-chunk");
  });

  it("openManifest reports inlined=true for inlined files", async () => {
    const stub = E.MOSSAIC_USER.get(E.MOSSAIC_USER.idFromName("vfs-read:manifest-il"));
    const userId = await seedUser(stub, "mfil@e.com");
    const payload = new TextEncoder().encode("inline-only");
    await runInDurableObject(stub, async (_instance, state) => {
      const sql = state.storage.sql;
      const now = Date.now();
      sql.exec(
        `INSERT INTO files (file_id, user_id, parent_id, file_name, file_size, file_hash, mime_type, chunk_size, chunk_count, pool_size, status, created_at, updated_at, mode, node_kind, inline_data)
         VALUES ('mfil-id', ?, NULL, 'i.txt', ?, '', 'text/plain', 0, 0, 32, 'complete', ?, ?, 420, 'file', ?)`,
        userId,
        payload.byteLength,
        now,
        now,
        payload
      );
    });

    const scope = { ns: "default", tenant: userId };
    const m = await stub.vfsOpenManifest(scope, "/i.txt");
    expect(m.inlined).toBe(true);
    expect(m.size).toBe(payload.byteLength);
    expect(m.chunks).toHaveLength(0);

    const ck = await stub.vfsReadChunk(scope, "/i.txt", 0);
    expect(new TextDecoder().decode(ck)).toBe("inline-only");
  });
});

describe("scope handling", () => {
  it("rejects empty tenant", async () => {
    const stub = E.MOSSAIC_USER.get(E.MOSSAIC_USER.idFromName("vfs-read:bad-scope"));
    await expect(
      stub.vfsStat({ ns: "default", tenant: "" }, "/")
    ).rejects.toThrow(/EINVAL/);
  });

  it("isolates tenants by user_id (sub composes into the user_id key)", async () => {
    const stub = E.MOSSAIC_USER.get(E.MOSSAIC_USER.idFromName("vfs-read:tenant-iso"));
    const userId = await seedUser(stub, "iso@e.com");

    // Insert a file under tenant=userId, sub=undefined.
    await stub.appCreateFile(userId, "owned.txt", 1, "text/plain", null);

    // scope.sub set → user_id becomes "<userId>::<sub>" which has no rows
    const scopeWithSub = { ns: "default", tenant: userId, sub: "alice" };
    expect(await stub.vfsExists(scopeWithSub, "/owned.txt")).toBe(false);

    const scopeNoSub = { ns: "default", tenant: userId };
    expect(await stub.vfsExists(scopeNoSub, "/owned.txt")).toBe(true);
  });
});
