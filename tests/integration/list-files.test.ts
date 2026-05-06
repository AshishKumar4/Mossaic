import { describe, it, expect } from "vitest";
import { env, runInDurableObject } from "cloudflare:test";

/**
 * indexed listFiles + cursor pagination.
 *
 * Pinned invariants:
 *   L1.  Empty tenant: empty items, no cursor.
 *   L2.  Pagination: 100 files, page 1+2+3 cursors stable.
 *   L3.  Cursor tampering: flipped byte → EINVAL.
 *   L4.  Cursor orderBy mismatch → EINVAL.
 *   L5.  Tag intersection: AND semantics.
 *   L6.  Concurrent insert during pagination: page 2 unaffected.
 *   L7.  Prefix filter: only files in the prefix's parent directory.
 *   L8.  Metadata post-filter narrows to matching rows.
 *   L9.  orderBy:'name' direction:'asc' returns alphabetical.
 *   L10. Performance: 1k files default-list completes <500ms in
 *        Miniflare (production gate is <50ms; Miniflare is slower
 *        than colo SQLite, this is a soft gate).
 */

import { createVFS, type MossaicEnv, type UserDO, EINVAL } from "../../sdk/src/index";
import { vfsUserDOName } from "@core/lib/utils";

interface E {
  MOSSAIC_USER: DurableObjectNamespace<UserDO>;
}
const E = env as unknown as E;
const NS = "default";

function envFor(): MossaicEnv {
  return { MOSSAIC_USER: E.MOSSAIC_USER as MossaicEnv["MOSSAIC_USER"] };
}
function userStub(tenant: string) {
  return E.MOSSAIC_USER.get(E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant)));
}

describe("listFiles basic (L1, L2, L7, L9)", () => {
  it("empty tenant returns empty items, no cursor", async () => {
    const vfs = createVFS(envFor(), { tenant: "p12-list-empty" });
    const r = await vfs.listFiles();
    expect(r.items).toEqual([]);
    expect(r.cursor).toBeUndefined();
  });

  it("paginates 100 files via cursor", async () => {
    const tenant = "p12-list-pagination";
    const vfs = createVFS(envFor(), { tenant });
    for (let i = 0; i < 100; i++) {
      await vfs.writeFile(`/f${String(i).padStart(3, "0")}.txt`, `data${i}`);
    }
    const p1 = await vfs.listFiles({ limit: 30 });
    expect(p1.items.length).toBe(30);
    expect(p1.cursor).toBeDefined();

    const p2 = await vfs.listFiles({ limit: 30, cursor: p1.cursor });
    expect(p2.items.length).toBe(30);
    expect(p2.cursor).toBeDefined();

    // Pages must be disjoint.
    const ids1 = new Set(p1.items.map((i) => i.pathId));
    const ids2 = new Set(p2.items.map((i) => i.pathId));
    for (const id of ids2) expect(ids1.has(id)).toBe(false);

    const p3 = await vfs.listFiles({ limit: 30, cursor: p2.cursor });
    expect(p3.items.length).toBe(30);
    const p4 = await vfs.listFiles({ limit: 30, cursor: p3.cursor });
    expect(p4.items.length).toBe(10);
    expect(p4.cursor).toBeUndefined();
  });

  it("orderBy:'name' direction:'asc' returns alphabetical", async () => {
    const tenant = "p12-list-name";
    const vfs = createVFS(envFor(), { tenant });
    await vfs.writeFile("/c.txt", "1");
    await vfs.writeFile("/a.txt", "1");
    await vfs.writeFile("/b.txt", "1");
    const r = await vfs.listFiles({ orderBy: "name", direction: "asc" });
    expect(r.items.map((i) => i.path)).toEqual(["/a.txt", "/b.txt", "/c.txt"]);
  });

  it("prefix filter narrows to a directory", async () => {
    const tenant = "p12-list-prefix";
    const vfs = createVFS(envFor(), { tenant });
    await vfs.mkdir("/photos");
    await vfs.writeFile("/photos/a.jpg", "x");
    await vfs.writeFile("/photos/b.jpg", "x");
    await vfs.writeFile("/notes.txt", "x");

    const photos = await vfs.listFiles({ prefix: "/photos" });
    expect(photos.items.map((i) => i.path).sort()).toEqual([
      "/photos/a.jpg",
      "/photos/b.jpg",
    ]);
  });

  it("fileInfo returns one file with stat, metadata, and tags", async () => {
    const tenant = "p12-file-info";
    const vfs = createVFS(envFor(), { tenant });
    await vfs.mkdir("/docs");
    await vfs.writeFile("/docs/report.md", "hello", {
      metadata: { title: "Report" },
      tags: ["seal:output"],
    });

    const info = await vfs.fileInfo("/docs/report.md", { includeMetadata: true });

    expect(info.path).toBe("/docs/report.md");
    expect(info.stat?.isFile()).toBe(true);
    expect(info.metadata).toEqual({ title: "Report" });
    expect(info.tags).toEqual(["seal:output"]);
    await expect(vfs.fileInfo("/docs")).rejects.toMatchObject({ code: "EISDIR" });
    await expect(vfs.fileInfo("/docs/missing.md")).rejects.toMatchObject({ code: "ENOENT" });
  });
});

describe("cursor security (L3, L4)", () => {
  it("tampered cursor → EINVAL", async () => {
    const tenant = "p12-list-cursor-tamper";
    const vfs = createVFS(envFor(), { tenant });
    for (let i = 0; i < 10; i++) await vfs.writeFile(`/f${i}.txt`, "x");
    const r = await vfs.listFiles({ limit: 5 });
    expect(r.cursor).toBeDefined();

    // Flip the last char of the base64 payload to break HMAC.
    const tampered = r.cursor!.slice(0, -1) + (r.cursor![r.cursor!.length - 1] === "A" ? "B" : "A");
    await expect(
      vfs.listFiles({ limit: 5, cursor: tampered })
    ).rejects.toBeInstanceOf(EINVAL);
  });

  it("orderBy mismatch → EINVAL", async () => {
    const tenant = "p12-list-cursor-ob";
    const vfs = createVFS(envFor(), { tenant });
    for (let i = 0; i < 10; i++) await vfs.writeFile(`/f${i}.txt`, "x");
    const r = await vfs.listFiles({ orderBy: "mtime", limit: 5 });
    await expect(
      vfs.listFiles({ orderBy: "name", direction: "asc", cursor: r.cursor! })
    ).rejects.toBeInstanceOf(EINVAL);
  });
});

describe("listFiles tags (L5)", () => {
  it("AND semantics: only files with ALL tags", async () => {
    const tenant = "p12-list-tags-and";
    const vfs = createVFS(envFor(), { tenant });
    await vfs.writeFile("/a.txt", "x", { tags: ["alpha", "beta"] });
    await vfs.writeFile("/b.txt", "x", { tags: ["alpha"] });
    await vfs.writeFile("/c.txt", "x", { tags: ["beta"] });
    await vfs.writeFile("/d.txt", "x", { tags: ["alpha", "beta", "gamma"] });

    const r = await vfs.listFiles({ tags: ["alpha", "beta"] });
    expect(r.items.map((i) => i.path).sort()).toEqual(["/a.txt", "/d.txt"]);
  });

  it("single tag returns matching files", async () => {
    const tenant = "p12-list-tags-single";
    const vfs = createVFS(envFor(), { tenant });
    await vfs.writeFile("/a.txt", "x", { tags: ["t"] });
    await vfs.writeFile("/b.txt", "x");
    await vfs.writeFile("/c.txt", "x", { tags: ["t"] });

    const r = await vfs.listFiles({ tags: ["t"] });
    expect(r.items.map((i) => i.path).sort()).toEqual(["/a.txt", "/c.txt"]);
  });

  it(">8 tags → EINVAL", async () => {
    const vfs = createVFS(envFor(), { tenant: "p12-list-tags-cap" });
    const tags = ["a", "b", "c", "d", "e", "f", "g", "h", "i"];
    await expect(vfs.listFiles({ tags })).rejects.toBeInstanceOf(EINVAL);
  });
});

describe("metadata filter (L8)", () => {
  it("filters to rows whose metadata matches", async () => {
    const tenant = "p12-list-meta";
    const vfs = createVFS(envFor(), { tenant });
    await vfs.writeFile("/a.txt", "x", { metadata: { color: "red" } });
    await vfs.writeFile("/b.txt", "x", { metadata: { color: "blue" } });
    await vfs.writeFile("/c.txt", "x", { metadata: { color: "red", size: 10 } });

    const r = await vfs.listFiles({
      metadata: { color: "red" },
      includeMetadata: true,
    });
    expect(r.items.map((i) => i.path).sort()).toEqual(["/a.txt", "/c.txt"]);
    for (const item of r.items) {
      expect((item.metadata as { color: string }).color).toBe("red");
    }
  });
});

describe("concurrent insert during pagination (L6)", () => {
  it("inserting newer files between pages does not corrupt page 2", async () => {
    const tenant = "p12-list-concurrent";
    const vfs = createVFS(envFor(), { tenant });
    // Write 10 files with strictly increasing mtime.
    const ids: string[] = [];
    for (let i = 0; i < 10; i++) {
      await vfs.writeFile(`/f${String(i).padStart(2, "0")}.txt`, `${i}`);
      await new Promise((r) => setTimeout(r, 1));
    }
    // Page 1 (newest 5).
    const p1 = await vfs.listFiles({ limit: 5 });
    expect(p1.items.length).toBe(5);
    for (const it of p1.items) ids.push(it.pathId);

    // Insert 3 newer files BEFORE fetching page 2.
    for (let i = 10; i < 13; i++) {
      await vfs.writeFile(`/f${i}.txt`, `${i}`);
      await new Promise((r) => setTimeout(r, 1));
    }

    // Page 2 (next 5). Cursor's seek boundary is the prior page's
    // mtime; the new files have HIGHER mtime so they don't appear
    // here. Result: page 2 returns the older 5 originals.
    const p2 = await vfs.listFiles({ limit: 5, cursor: p1.cursor });
    expect(p2.items.length).toBe(5);
    const seen = new Set<string>();
    for (const it of p1.items) seen.add(it.pathId);
    for (const it of p2.items) {
      expect(seen.has(it.pathId)).toBe(false); // disjoint
      seen.add(it.pathId);
    }
    expect(seen.size).toBe(10);
  });
});

describe("performance smoke (L10)", () => {
  it("1k-file default listFiles under 500ms (Miniflare soft gate)", async () => {
    const tenant = "p12-list-perf";
    const stub = userStub(tenant);
    // Bulk seed via direct SQL to skip rate limits + write overhead.
    await runInDurableObject(stub, async (inst, _state) => {
      // Trigger ensureInit + record scope.
      await inst.vfsExists(
        { ns: NS, tenant },
        "/"
      );
    });
    await runInDurableObject(stub, async (_, state) => {
      const now = Date.now();
      for (let i = 0; i < 1000; i++) {
        const id = `bulk-${i.toString().padStart(4, "0")}`;
        state.storage.sql.exec(
          `INSERT INTO files (file_id, user_id, parent_id, file_name,
              file_size, file_hash, mime_type, chunk_size, chunk_count,
              pool_size, status, created_at, updated_at, mode, node_kind)
           VALUES (?, ?, NULL, ?, 1, '', 'text/plain', 0, 0, 32,
                   'complete', ?, ?, 420, 'file')`,
          id,
          tenant,
          `f${i}.txt`,
          now - i,
          now - i
        );
      }
    });
    const vfs = createVFS(envFor(), { tenant });
    const start = Date.now();
    const r = await vfs.listFiles({ limit: 50 });
    const elapsed = Date.now() - start;
    expect(r.items.length).toBe(50);
    expect(elapsed).toBeLessThan(500);
    // Stash the elapsed for visibility in CI logs.
    // eslint-disable-next-line no-console
    console.log(`[perf] listFiles(limit=50) over 1000 rows: ${elapsed}ms`);
  });
});
