import { describe, it, expect } from "vitest";
import { env, runInDurableObject } from "cloudflare:test";

/**
 * Read-side VFS RPC integration tests.
 *
 * Drives the typed RPC surface (vfsStat / vfsLstat / vfsExists /
 * vfsReadlink / vfsReaddir / vfsReadManyStat / vfsReadFile /
 * vfsOpenManifest / vfsReadChunk) directly through DO RPC. The consumer
 * test fixture pretends to be the SDK: gets a stub via
 * `env.MOSSAIC_USER.get(idFromName(...))` and calls the methods.
 */

import type { UserDO } from "@app/objects/user/user-do";
import { INLINE_LIMIT } from "@shared/inline";

interface E {
  MOSSAIC_USER: DurableObjectNamespace<UserDO>;
  MOSSAIC_SHARD: DurableObjectNamespace;
}
const E = env as unknown as E;

/** Seed a UserDO via `appHandleSignup`; returns the generated user_id. */
async function seedUser(
  stub: DurableObjectStub<UserDO>,
  email: string
): Promise<string> {
  const { userId } = await stub.appHandleSignup(email, "abcd1234");
  return userId;
}

describe("vfsStat / vfsLstat / vfsExists", () => {
  it("stats a file at the root after a canonical write", async () => {
    const stub = E.MOSSAIC_USER.get(E.MOSSAIC_USER.idFromName("vfs-read:stat"));
    const userId = await seedUser(stub, "stat@e.com");

    const scope = { ns: "default", tenant: userId };
    await stub.vfsWriteFile(
      scope,
      "/stat.txt",
      new TextEncoder().encode("hello"),
      { mimeType: "text/plain" }
    );
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
    const scope = { ns: "default", tenant: userId };
    for (const name of ["c.txt", "d.txt"]) {
      await stub.vfsWriteFile(
        scope,
        `/${name}`,
        new TextEncoder().encode("x"),
        { mimeType: "text/plain" }
      );
    }

    const entries = await stub.vfsReaddir(scope, "/");
    expect(entries).toEqual(["a", "b", "c.txt", "d.txt"]);

    // Listing a missing dir → ENOENT
    await expect(stub.vfsReaddir(scope, "/nope")).rejects.toThrow(/ENOENT/);
  });

  it("ENOTDIR when path resolves to a file", async () => {
    const stub = E.MOSSAIC_USER.get(E.MOSSAIC_USER.idFromName("vfs-read:enotdir"));
    const userId = await seedUser(stub, "ntd@e.com");
    const scope = { ns: "default", tenant: userId };
    await stub.vfsWriteFile(
      scope,
      "/f.txt",
      new TextEncoder().encode("x"),
      { mimeType: "text/plain" }
    );
    await expect(stub.vfsReaddir(scope, "/f.txt")).rejects.toThrow(/ENOTDIR/);
  });
});

describe("vfsReadFile (inline tier + chunked path)", () => {
  it("reads inlined file with ZERO ShardDO subrequests", async () => {
    const stub = E.MOSSAIC_USER.get(E.MOSSAIC_USER.idFromName("vfs-read:inline"));
    const userId = await seedUser(stub, "il@e.com");

    // Manually insert an inlined file.
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

    // 20KB > INLINE_LIMIT (16KB) → forces the chunked tier so the read
    // exercises the ShardDO fan-out path.
    const full = new Uint8Array(20 * 1024);
    for (let i = 0; i < full.length; i++) full[i] = (i * 31 + 7) & 0xff;

    const scope = { ns: "default", tenant: userId };
    await stub.vfsWriteFile(scope, "/two.bin", full, {
      mimeType: "application/octet-stream",
    });

    const out = await stub.vfsReadFile(scope, "/two.bin");
    expect(new Uint8Array(out)).toEqual(full);
  });
});

describe("vfsReadManyStat", () => {
  it("returns one stat per path, null for misses", async () => {
    const stub = E.MOSSAIC_USER.get(E.MOSSAIC_USER.idFromName("vfs-read:many"));
    const userId = await seedUser(stub, "many@e.com");

    const scope = { ns: "default", tenant: userId };
    for (const name of ["a.txt", "b.txt"]) {
      await stub.vfsWriteFile(
        scope,
        `/${name}`,
        new TextEncoder().encode("xxx"),
        { mimeType: "text/plain" }
      );
    }

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

    // 20KB > INLINE_LIMIT (16KB) → chunked tier with at least one chunk.
    const full = new Uint8Array(20 * 1024);
    for (let i = 0; i < full.length; i++) full[i] = (i * 17 + 3) & 0xff;

    const scope = { ns: "default", tenant: userId };
    await stub.vfsWriteFile(scope, "/m.bin", full, {
      mimeType: "application/octet-stream",
    });

    const m = await stub.vfsOpenManifest(scope, "/m.bin");
    expect(m.inlined).toBe(false);
    expect(m.size).toBe(full.byteLength);
    expect(m.chunks.length).toBeGreaterThan(0);
    // shardIndex MUST NOT leak in the public shape.
    expect((m.chunks[0] as Record<string, unknown>).shardIndex).toBeUndefined();
    expect(typeof m.chunks[0].hash).toBe("string");
    expect(m.chunks[0].hash.length).toBe(64);

    const ck = await stub.vfsReadChunk(scope, "/m.bin", 0);
    // First chunk = first chunkSize bytes of the original payload.
    expect(new Uint8Array(ck).slice(0, 16))
      .toEqual(full.slice(0, 16));
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

    // Write under tenant=userId, sub=undefined.
    const scopeNoSub = { ns: "default", tenant: userId } as const;
    await stub.vfsWriteFile(
      scopeNoSub,
      "/owned.txt",
      new TextEncoder().encode("x"),
      { mimeType: "text/plain" }
    );

    // scope.sub set → user_id becomes "<userId>::<sub>" which has no rows
    const scopeWithSub = { ns: "default", tenant: userId, sub: "alice" };
    expect(await stub.vfsExists(scopeWithSub, "/owned.txt")).toBe(false);
    expect(await stub.vfsExists(scopeNoSub, "/owned.txt")).toBe(true);
  });
});
