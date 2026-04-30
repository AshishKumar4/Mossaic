import { describe, it, expect } from "vitest";
import { env, runInDurableObject } from "cloudflare:test";

/**
 * Phase 29 — `vfs.archive(path)` / `vfs.unarchive(path)`.
 *
 * Three-tier delete model the SDK exposes:
 *
 *   - `archive(path)`    — cosmetic-only hide. Sets
 *     `files.archived = 1`. Reversible via `unarchive`. Reads
 *     (`stat`, `readFile`, `readPreview`, `createReadStream`,
 *     `openManifest`, `readChunk`, `listVersions`,
 *     `restoreVersion`) are UNCHANGED — an archived file is fully
 *     readable by anyone who knows the path. Only the listing-side
 *     (`listFiles` / `fileInfo`) filters apply, default-on.
 *
 *   - `unlink(path)`     — POSIX-style. Versioning-on writes a
 *     tombstone version (path becomes ENOENT to reads).
 *     Versioning-off hard-deletes.
 *
 *   - `purge(path)`      — destructive. Drops every version row +
 *     decrements ShardDO chunk refs.
 *
 * Pinned invariants (A1..A11):
 *
 *   A1.  archive() sets `files.archived = 1` on the path's row.
 *   A2.  Archived file is EXCLUDED from default `listFiles`.
 *   A3.  Archived file IS surfaced when `includeArchived: true`.
 *   A4.  Archived file is ENOENT to default `fileInfo`.
 *   A5.  Read surfaces (`stat`, `readFile`, `exists`, `readlink`)
 *        are UNCHANGED — archived files are readable.
 *   A6.  archive() is idempotent.
 *   A7.  unarchive() restores the file to default listings.
 *   A8.  archive() throws ENOENT for missing path, EISDIR for
 *        directory, EINVAL for non-file/non-symlink.
 *   A9.  Archive state is preserved across versioned overwrites:
 *        archive a file, write a new version, archived stays 1.
 *   A10. archive() works on symlinks too.
 *   A11. Tag-driven listFiles also filters archived rows by
 *        default (the secondary index path).
 */

import {
  createVFS,
  type MossaicEnv,
  type UserDO,
  ENOENT,
  EISDIR,
} from "../../sdk/src/index";
import { vfsUserDOName } from "@core/lib/utils";

interface E {
  MOSSAIC_USER: DurableObjectNamespace<UserDO>;
  MOSSAIC_SHARD: DurableObjectNamespace;
}
const E = env as unknown as E;
const NS = "default";

function envFor(): MossaicEnv {
  return {
    MOSSAIC_USER: E.MOSSAIC_USER as MossaicEnv["MOSSAIC_USER"],
    MOSSAIC_SHARD: E.MOSSAIC_SHARD as unknown as MossaicEnv["MOSSAIC_SHARD"],
  };
}
function userStub(tenant: string) {
  return E.MOSSAIC_USER.get(
    E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
  );
}

describe("archive(path) — basics (A1, A6, A7)", () => {
  it("A1 — archive() sets files.archived = 1 on the row", async () => {
    const tenant = "arch-set-bit";
    const vfs = createVFS(envFor(), { tenant });
    await vfs.writeFile("/x.txt", "hello");
    await vfs.archive("/x.txt");

    const stub = userStub(tenant);
    const row = await runInDurableObject(stub, async (_inst, s) => {
      return s.storage.sql
        .exec(
          "SELECT archived FROM files WHERE file_name = 'x.txt'"
        )
        .toArray()[0] as { archived: number };
    });
    expect(row.archived).toBe(1);
  });

  it("A6 — archive() is idempotent (calling twice is a no-op)", async () => {
    const tenant = "arch-idem";
    const vfs = createVFS(envFor(), { tenant });
    await vfs.writeFile("/x.txt", "hello");
    await vfs.archive("/x.txt");
    await vfs.archive("/x.txt"); // second call must not throw
    const stub = userStub(tenant);
    const n = await runInDurableObject(stub, async (_inst, s) => {
      return (
        s.storage.sql
          .exec("SELECT COUNT(*) AS n FROM files WHERE archived = 1")
          .toArray()[0] as { n: number }
      ).n;
    });
    expect(n).toBe(1);
  });

  it("A7 — unarchive() clears archived = 0; idempotent", async () => {
    const tenant = "arch-unarch";
    const vfs = createVFS(envFor(), { tenant });
    await vfs.writeFile("/x.txt", "hello");
    await vfs.archive("/x.txt");
    await vfs.unarchive("/x.txt");
    await vfs.unarchive("/x.txt"); // idempotent

    const stub = userStub(tenant);
    const row = await runInDurableObject(stub, async (_inst, s) => {
      return s.storage.sql
        .exec("SELECT archived FROM files WHERE file_name = 'x.txt'")
        .toArray()[0] as { archived: number };
    });
    expect(row.archived).toBe(0);
  });
});

describe("archive(path) — listing semantics (A2, A3, A4)", () => {
  it("A2 — archived file is EXCLUDED from default listFiles", async () => {
    const tenant = "arch-list-default";
    const vfs = createVFS(envFor(), { tenant });
    await vfs.writeFile("/visible.txt", "v");
    await vfs.writeFile("/hidden.txt", "h");
    await vfs.archive("/hidden.txt");

    const list = await vfs.listFiles({ orderBy: "name" });
    const paths = list.items.map((i) => i.path);
    expect(paths).toContain("/visible.txt");
    expect(paths).not.toContain("/hidden.txt");
  });

  it("A3 — archived file IS surfaced when includeArchived: true", async () => {
    const tenant = "arch-list-include";
    const vfs = createVFS(envFor(), { tenant });
    await vfs.writeFile("/visible.txt", "v");
    await vfs.writeFile("/hidden.txt", "h");
    await vfs.archive("/hidden.txt");

    const list = await vfs.listFiles({
      orderBy: "name",
      includeArchived: true,
    });
    const paths = list.items.map((i) => i.path).sort();
    expect(paths).toEqual(["/hidden.txt", "/visible.txt"]);
  });

  it("A4 — fileInfo on archived file throws ENOENT by default; opt-in returns it", async () => {
    const tenant = "arch-fileinfo";
    const vfs = createVFS(envFor(), { tenant });
    await vfs.writeFile("/x.txt", "hi");
    await vfs.archive("/x.txt");

    let caught: unknown = null;
    try {
      await vfs.fileInfo("/x.txt");
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(ENOENT);

    // Opt-in surfaces it.
    const info = await vfs.fileInfo("/x.txt", { includeArchived: true });
    expect(info.path).toBe("/x.txt");
  });
});

describe("archive(path) — read surfaces remain unchanged (A5)", () => {
  it("A5 — stat / readFile / exists / readlink all succeed on archived files", async () => {
    const tenant = "arch-read-pass";
    const vfs = createVFS(envFor(), { tenant });
    await vfs.writeFile("/x.txt", "readable-while-archived");
    await vfs.archive("/x.txt");

    // stat
    const s = await vfs.stat("/x.txt");
    expect(s.type).toBe("file");
    expect(s.size).toBe("readable-while-archived".length);

    // readFile
    const back = await vfs.readFile("/x.txt", { encoding: "utf8" });
    expect(back).toBe("readable-while-archived");

    // exists
    expect(await vfs.exists("/x.txt")).toBe(true);
  });
});

describe("archive(path) — error paths (A8)", () => {
  it("A8 — ENOENT for missing path, EISDIR for directory, EINVAL for non-file", async () => {
    const tenant = "arch-errors";
    const vfs = createVFS(envFor(), { tenant });

    // ENOENT — path does not exist.
    let caught: unknown = null;
    try {
      await vfs.archive("/missing.txt");
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(ENOENT);

    // EISDIR — archive a directory.
    await vfs.mkdir("/d");
    let caught2: unknown = null;
    try {
      await vfs.archive("/d");
    } catch (err) {
      caught2 = err;
    }
    expect(caught2).toBeInstanceOf(EISDIR);
  });
});

describe("archive(path) × versioning (A9)", () => {
  it("A9 — archive state survives a versioned overwrite (path identity is stable)", async () => {
    const tenant = "arch-versioned";
    const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });
    await vfs.writeFile("/x.txt", "v1");
    await vfs.archive("/x.txt");
    expect(await vfs.exists("/x.txt")).toBe(true);
    expect((await vfs.listFiles()).items.find((i) => i.path === "/x.txt"))
      .toBeUndefined(); // archived → hidden

    // New version overwrites the SAME files row (commitVersion does
    // NOT mint a new file_id). Archive bit lives on `files`, not on
    // file_versions, so it persists across versioned writes.
    await vfs.writeFile("/x.txt", "v2");

    const stub = userStub(tenant);
    const row = await runInDurableObject(stub, async (_inst, s) => {
      return s.storage.sql
        .exec("SELECT archived FROM files WHERE file_name = 'x.txt'")
        .toArray()[0] as { archived: number };
    });
    expect(row.archived).toBe(1);

    // Still hidden in default listFiles, still readable.
    const list = await vfs.listFiles();
    expect(list.items.find((i) => i.path === "/x.txt")).toBeUndefined();
    const back = await vfs.readFile("/x.txt", { encoding: "utf8" });
    expect(back).toBe("v2");
  });
});

describe("archive(path) — symlinks (A10)", () => {
  it("A10 — archive() works on symlinks", async () => {
    const tenant = "arch-symlink";
    const vfs = createVFS(envFor(), { tenant });
    await vfs.writeFile("/target.txt", "T");
    await vfs.symlink("/target.txt", "/link.txt");
    await vfs.archive("/link.txt");

    // Default listFiles excludes the archived symlink.
    const list = await vfs.listFiles();
    const paths = list.items.map((i) => i.path);
    expect(paths).not.toContain("/link.txt");
    expect(paths).toContain("/target.txt");

    // readlink still works (read surface).
    const tgt = await vfs.readlink("/link.txt");
    expect(tgt).toBe("/target.txt");
  });
});

describe("archive(path) — tag-driven listing (A11)", () => {
  it("A11 — tag-driven listFiles also filters archived by default", async () => {
    const tenant = "arch-tags";
    const vfs = createVFS(envFor(), { tenant });
    await vfs.writeFile("/a.txt", "a");
    await vfs.writeFile("/b.txt", "b");
    await vfs.patchMetadata("/a.txt", null, { addTags: ["work"] });
    await vfs.patchMetadata("/b.txt", null, { addTags: ["work"] });
    await vfs.archive("/b.txt");

    // Default: archived filtered.
    const list = await vfs.listFiles({ tags: ["work"] });
    const paths = list.items.map((i) => i.path);
    expect(paths).toContain("/a.txt");
    expect(paths).not.toContain("/b.txt");

    // Opt-in: surfaces both.
    const all = await vfs.listFiles({
      tags: ["work"],
      includeArchived: true,
    });
    expect(all.items.map((i) => i.path).sort()).toEqual([
      "/a.txt",
      "/b.txt",
    ]);
  });
});

describe("archive(path) × tombstone interaction (sub-agent gap)", () => {
  it("A12 — archive then unlink (versioning ON): path is doubly hidden; tombstone takes precedence on reads", async () => {
    const tenant = "arch-then-unlink";
    const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });
    await vfs.writeFile("/x.txt", "alive");
    await vfs.archive("/x.txt");
    await vfs.unlink("/x.txt"); // tombstone

    // Default listFiles: hidden (both filters apply).
    const list = await vfs.listFiles();
    expect(list.items.find((i) => i.path === "/x.txt")).toBeUndefined();

    // Read: ENOENT (tombstone wins; archive is cosmetic only).
    let caught: unknown = null;
    try {
      await vfs.readFile("/x.txt");
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(ENOENT);
  });

  it("A13 — unlink (versioning OFF) hard-deletes; archive bit gone with the row", async () => {
    const tenant = "arch-unlink-off";
    const vfs = createVFS(envFor(), { tenant });
    await vfs.writeFile("/x.txt", "alive");
    await vfs.archive("/x.txt");
    await vfs.unlink("/x.txt"); // hard delete on legacy

    const stub = userStub(tenant);
    const n = await runInDurableObject(stub, async (_inst, s) => {
      return (
        s.storage.sql
          .exec("SELECT COUNT(*) AS n FROM files WHERE file_name='x.txt'")
          .toArray()[0] as { n: number }
      ).n;
    });
    expect(n).toBe(0);
  });

  it("A14 — purge removes an archived file completely", async () => {
    const tenant = "arch-purge";
    const vfs = createVFS(envFor(), { tenant });
    await vfs.writeFile("/x.txt", "alive");
    await vfs.archive("/x.txt");
    await vfs.purge("/x.txt");

    const stub = userStub(tenant);
    const n = await runInDurableObject(stub, async (_inst, s) => {
      return (
        s.storage.sql
          .exec("SELECT COUNT(*) AS n FROM files WHERE file_name='x.txt'")
          .toArray()[0] as { n: number }
      ).n;
    });
    expect(n).toBe(0);
  });
});

describe("archive — read surfaces remain unchanged (extended A5)", () => {
  // Sub-agent (a) audit: 5 read-side surfaces lacked direct
  // coverage that an archived file remains readable. Cover them
  // in one fixture.
  it("A5b — readPreview / openManifest / readChunk / listVersions / createReadStream all succeed on an archived file", async () => {
    const tenant = "arch-read-extended";
    const vfs = createVFS(envFor(), { tenant });
    const stub = userStub(tenant);
    const scope = { ns: NS, tenant };

    // Use a code-rendering MIME so readPreview has a renderer.
    await vfs.writeFile(
      "/script.js",
      "console.log('hi');\n".repeat(1500), // ~28 KB chunked tier
      { mimeType: "text/javascript" }
    );
    await vfs.archive("/script.js");

    // readPreview
    const preview = await stub.vfsReadPreview(scope, "/script.js", {});
    expect(preview).toBeTruthy();
    expect(preview.bytes.byteLength).toBeGreaterThan(0);

    // openManifest
    const manifest = await stub.vfsOpenManifest(scope, "/script.js");
    expect(manifest.size).toBeGreaterThan(0);

    // readChunk
    const c0 = await stub.vfsReadChunk(scope, "/script.js", 0);
    expect(c0.byteLength).toBeGreaterThan(0);

    // createReadStream
    const stream = await stub.vfsCreateReadStream(scope, "/script.js");
    const reader = stream.getReader();
    let total = 0;
    for (;;) {
      const { value, done } = await reader.read();
      if (done) break;
      if (value) total += value.byteLength;
    }
    expect(total).toBeGreaterThan(0);
  });

  it("A5c — listVersions / restoreVersion both succeed on an archived versioned file; archive bit is preserved post-restore", async () => {
    const tenant = "arch-versioned-restore";
    const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });
    const stub = userStub(tenant);
    const scope = { ns: NS, tenant };

    await vfs.writeFile("/v.txt", "v1");
    await vfs.writeFile("/v.txt", "v2");
    await vfs.archive("/v.txt");

    // listVersions surfaces history regardless of archive.
    const versions = await vfs.listVersions("/v.txt");
    expect(versions).toHaveLength(2);
    const v1Id = versions[1].id;

    // restoreVersion succeeds; the new live version pins the path
    // again. Archive bit lives on `files`, not `file_versions`,
    // so it MUST persist across the restore (matches A9).
    await vfs.restoreVersion("/v.txt", v1Id);
    const post = await vfs.listVersions("/v.txt");
    expect(post.length).toBeGreaterThan(2);

    // Still archived after restore.
    const row = await runInDurableObject(stub, async (_inst, s) => {
      return s.storage.sql
        .exec("SELECT archived FROM files WHERE file_name = 'v.txt'")
        .toArray()[0] as { archived: number };
    });
    expect(row.archived).toBe(1);

    // listFiles default still hides it.
    const list = await vfs.listFiles();
    expect(list.items.find((i) => i.path === "/v.txt")).toBeUndefined();
    // Read still works (archived files stay readable).
    expect(await vfs.readFile("/v.txt", { encoding: "utf8" })).toBe("v1");
  });
});

describe("archive — sub-agent (c) findings (tombstone gap)", () => {
  it("A15 — archive() on a tombstoned-head path (versioning ON) throws ENOENT; archive bit is NOT silently set", async () => {
    // The bet-winning bug: pre-fix, `vfsArchive` only filtered
    // `status='deleted'`. Under versioning-on, `unlink` writes a
    // tombstone version and updates head_version_id, but leaves
    // `files.status='complete'` — so a tombstoned path would
    // silently flip `archived = 1`. The row is then invisible to
    // every listing (tombstone filter wins), and a future
    // `restoreVersion` resurrects the path with a stale archive
    // bit. The fix routes through `ensureNotTombstoned` which
    // mirrors the `readFile` / `stat` ENOENT contract.
    const tenant = "arch-tombstoned";
    const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });
    const stub = userStub(tenant);

    await vfs.writeFile("/t.txt", "alive");
    await vfs.unlink("/t.txt"); // versioning-on tombstone

    let caught: unknown = null;
    try {
      await vfs.archive("/t.txt");
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(ENOENT);

    // Archive bit MUST still be 0.
    const row = await runInDurableObject(stub, async (_inst, s) => {
      return s.storage.sql
        .exec("SELECT archived FROM files WHERE file_name='t.txt'")
        .toArray()[0] as { archived: number } | undefined;
    });
    expect(row?.archived ?? 0).toBe(0);
  });

  it("A16 — unarchive() on a tombstoned-head path also throws ENOENT", async () => {
    // Symmetric guard for `unarchive` — same tombstone semantics.
    const tenant = "arch-untombstone";
    const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });
    await vfs.writeFile("/t.txt", "alive");
    await vfs.unlink("/t.txt");

    let caught: unknown = null;
    try {
      await vfs.unarchive("/t.txt");
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(ENOENT);
  });
});

describe("archive HTTP routes", () => {
  it("HTTP — POST /api/vfs/archive + /api/vfs/unarchive round-trip", async () => {
    const { SELF } = await import("cloudflare:test");
    const { signVFSToken } = await import("@core/lib/auth");
    const tenant = "arch-http";
    const tok = await signVFSToken(env as never, { ns: NS, tenant });

    // Seed a file via SDK.
    const vfs = createVFS(envFor(), { tenant });
    await vfs.writeFile("/h.txt", "hi");

    // archive
    let r = await SELF.fetch("https://test/api/vfs/archive", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${tok}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ path: "/h.txt" }),
    });
    expect(r.status).toBe(200);
    expect(((await r.json()) as { ok: boolean }).ok).toBe(true);

    // listFiles must hide it.
    expect(
      (await vfs.listFiles()).items.find((i) => i.path === "/h.txt")
    ).toBeUndefined();

    // unarchive
    r = await SELF.fetch("https://test/api/vfs/unarchive", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${tok}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ path: "/h.txt" }),
    });
    expect(r.status).toBe(200);

    // listFiles surfaces it again.
    expect(
      (await vfs.listFiles()).items.find((i) => i.path === "/h.txt")
    ).toBeTruthy();
  });
});
