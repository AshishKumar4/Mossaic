import { describe, it, expect } from "vitest";
import { env, runInDurableObject } from "cloudflare:test";

/**
 * Phase 2 — Path resolution tests (sdk-impl-plan §10).
 *
 * Verifies:
 *   - normalizePath/dirname/basename/resolveSymlinkTarget pure utils
 *   - resolvePath: file/dir/symlink/ENOENT/ENOTDIR distinctions
 *   - resolvePathFollow: 40-hop ELOOP, relative-target chasing
 *   - All resolution happens in ONE DO method call (no env access required
 *     because resolvePath is pure SQL)
 */

import {
  basename,
  dirname,
  normalizePath,
  resolveSymlinkTarget,
  VFSPathError,
} from "@shared/vfs-paths";

import type { UserDO } from "@app/objects/user/user-do";

interface E {
  MOSSAIC_USER: DurableObjectNamespace<UserDO>;
}
const E = env as unknown as E;
const userStub = (n: string): DurableObjectStub<UserDO> =>
  E.MOSSAIC_USER.get(E.MOSSAIC_USER.idFromName(n));

describe("normalizePath", () => {
  it("normalizes various forms", () => {
    expect(normalizePath("/")).toEqual([]);
    expect(normalizePath("/foo")).toEqual(["foo"]);
    expect(normalizePath("/foo/bar")).toEqual(["foo", "bar"]);
    expect(normalizePath("/foo/./bar/")).toEqual(["foo", "bar"]);
    expect(normalizePath("/foo/../bar")).toEqual(["bar"]);
    expect(normalizePath("//foo///bar")).toEqual(["foo", "bar"]);
    expect(normalizePath("/a/b/c/../../d")).toEqual(["a", "d"]);
  });

  it("rejects non-absolute, NUL bytes, walks above root", () => {
    expect(() => normalizePath("foo")).toThrow(VFSPathError);
    expect(() => normalizePath("")).toThrow(VFSPathError);
    expect(() => normalizePath("/foo\0bar")).toThrow(VFSPathError);
    expect(() => normalizePath("/..")).toThrow(VFSPathError);
    expect(() => normalizePath("/foo/../..")).toThrow(VFSPathError);
  });
});

describe("dirname / basename", () => {
  it("dirname collapses to /", () => {
    expect(dirname("/")).toBe("/");
    expect(dirname("/foo")).toBe("/");
    expect(dirname("/foo/bar")).toBe("/foo");
    expect(dirname("/foo/bar/baz.txt")).toBe("/foo/bar");
  });

  it("basename returns last segment", () => {
    expect(basename("/")).toBe("");
    expect(basename("/foo")).toBe("foo");
    expect(basename("/foo/bar.txt")).toBe("bar.txt");
  });
});

describe("resolveSymlinkTarget", () => {
  it("absolute targets win", () => {
    expect(resolveSymlinkTarget("/a/b/link", "/elsewhere/x")).toBe(
      "/elsewhere/x"
    );
  });

  it("relative targets resolve against link's parent", () => {
    expect(resolveSymlinkTarget("/a/b/link", "target.txt")).toBe(
      "/a/b/target.txt"
    );
    expect(resolveSymlinkTarget("/a/b/link", "../sibling/x")).toBe(
      "/a/sibling/x"
    );
    expect(resolveSymlinkTarget("/a/b/link", "./x")).toBe("/a/b/x");
  });

  it("rejects empty target and walks-above-root", () => {
    expect(() => resolveSymlinkTarget("/a", "")).toThrow();
    expect(() => resolveSymlinkTarget("/a", "../..")).toThrow();
  });
});

describe("resolvePath inside the DO", () => {
  it("resolves files, dirs, missing leaves and ENOTDIR", async () => {
    const stub = userStub("path-walk:basic");

    // Seed: signup → folder /home → folder /home/work → write
    // /home/work/notes.txt via canonical vfsWriteFile.
    const { userId } = await stub.appHandleSignup("pw@e.com", "abcd1234");
    const home = await stub.appCreateFolder(userId, "home", null);
    const work = await stub.appCreateFolder(userId, "work", home.folderId);

    const scope = { ns: "default", tenant: userId } as const;
    await stub.vfsWriteFile(
      scope,
      "/home/work/notes.txt",
      new TextEncoder().encode("hello"),
      { mimeType: "text/plain" }
    );

    // Drive resolvePath via runInDurableObject + capture the fileId
    // we'll assert against from the row that vfsWriteFile inserted.
    await runInDurableObject(stub, async (instance, state) => {
      const fileIdRow = state.storage.sql
        .exec(
          "SELECT file_id FROM files WHERE user_id = ? AND file_name = 'notes.txt'",
          userId
        )
        .toArray()[0] as { file_id: string } | undefined;
      expect(fileIdRow).toBeDefined();
      const fileId = fileIdRow!.file_id;

      const { resolvePath } = await import(
        "@core/objects/user/path-walk"
      );

      // root
      const root = resolvePath(instance as never, userId, "/");
      expect(root.kind).toBe("dir");

      // file
      const f = resolvePath(instance as never, userId, "/home/work/notes.txt");
      expect(f.kind).toBe("file");
      if (f.kind === "file") expect(f.leafId).toBe(fileId);

      // intermediate folder
      const d = resolvePath(instance as never, userId, "/home/work");
      expect(d.kind).toBe("dir");
      if (d.kind === "dir") expect(d.leafId).toBe(work.folderId);

      // missing leaf → ENOENT
      const miss = resolvePath(instance as never, userId, "/home/work/nope");
      expect(miss.kind).toBe("ENOENT");

      // ENOTDIR: try to descend through the file
      const notDir = resolvePath(
        instance as never,
        userId,
        "/home/work/notes.txt/something"
      );
      expect(notDir.kind).toBe("ENOTDIR");

      // missing intermediate → ENOENT
      const missMid = resolvePath(
        instance as never,
        userId,
        "/home/missing/foo"
      );
      expect(missMid.kind).toBe("ENOENT");
    });
  });

  it("symlink resolution: lstat-style returns symlink, follow chases up to ELOOP", async () => {
    const stub = userStub("path-walk:symlink");

    const { userId } = await stub.appHandleSignup("sl@e.com", "abcd1234");

    // Create /target (a real file) and /link → /target plus a cycle for ELOOP test.
    await runInDurableObject(stub, async (_instance, state) => {
      const sql = state.storage.sql;
      const now = Date.now();
      // /target (regular file)
      sql.exec(
        `INSERT INTO files (file_id, user_id, parent_id, file_name, file_size, file_hash, mime_type, chunk_size, chunk_count, pool_size, status, created_at, updated_at, mode, node_kind)
         VALUES ('target-id', ?, NULL, 'target', 5, '', 'text/plain', 5, 1, 32, 'complete', ?, ?, 420, 'file')`,
        userId,
        now,
        now
      );
      // /link → /target (symlink, absolute target)
      sql.exec(
        `INSERT INTO files (file_id, user_id, parent_id, file_name, file_size, file_hash, mime_type, chunk_size, chunk_count, pool_size, status, created_at, updated_at, mode, node_kind, symlink_target)
         VALUES ('link-id', ?, NULL, 'link', 0, '', 'inode/symlink', 0, 0, 32, 'complete', ?, ?, 511, 'symlink', '/target')`,
        userId,
        now,
        now
      );
      // /loop1 → /loop2, /loop2 → /loop1 (cycle for ELOOP)
      sql.exec(
        `INSERT INTO files (file_id, user_id, parent_id, file_name, file_size, file_hash, mime_type, chunk_size, chunk_count, pool_size, status, created_at, updated_at, mode, node_kind, symlink_target)
         VALUES ('loop1-id', ?, NULL, 'loop1', 0, '', 'inode/symlink', 0, 0, 32, 'complete', ?, ?, 511, 'symlink', '/loop2')`,
        userId,
        now,
        now
      );
      sql.exec(
        `INSERT INTO files (file_id, user_id, parent_id, file_name, file_size, file_hash, mime_type, chunk_size, chunk_count, pool_size, status, created_at, updated_at, mode, node_kind, symlink_target)
         VALUES ('loop2-id', ?, NULL, 'loop2', 0, '', 'inode/symlink', 0, 0, 32, 'complete', ?, ?, 511, 'symlink', '/loop1')`,
        userId,
        now,
        now
      );
    });

    await runInDurableObject(stub, async (instance) => {
      const { resolvePath, resolvePathFollow } = await import(
        "@core/objects/user/path-walk"
      );

      // lstat-style: returns the symlink
      const ls = resolvePath(instance as never, userId, "/link");
      expect(ls.kind).toBe("symlink");
      if (ls.kind === "symlink") expect(ls.target).toBe("/target");

      // stat-style: chases to the file
      const fol = resolvePathFollow(instance as never, userId, "/link");
      expect(fol.kind).toBe("file");
      if (fol.kind === "file") expect(fol.leafId).toBe("target-id");

      // ELOOP via cycle
      const loop = resolvePathFollow(instance as never, userId, "/loop1");
      expect(loop.kind).toBe("ELOOP");
    });
  });
});
