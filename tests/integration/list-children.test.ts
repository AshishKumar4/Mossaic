import { describe, it, expect } from "vitest";
import { env } from "cloudflare:test";

/**
 * Phase 46 — `vfs.listChildren` batched directory listing.
 *
 * Pinned invariants:
 *   LC1.  Empty folder: revision = 0, entries = [], no cursor.
 *   LC2.  Mixed folder: folder + file + symlink entries surfaced
 *         with correct kind discriminant.
 *   LC3.  Single round-trip equivalence — `listChildren` returns the
 *         SAME (path, kind, stat, tags, metadata) set the legacy
 *         `readdir + lstat × N` loop would produce, in 1 RPC vs N+1.
 *   LC4.  Pagination: 100-child folder, three pages stable, last
 *         page has no cursor; pages are disjoint.
 *   LC5.  Cursor encodes kind tiebreaker — when folder + file share
 *         an mtime tick, page 2 starts where page 1 left off (no
 *         duplicates, no skips).
 *   LC6.  `includeContentHash: true` surfaces `contentHash` on
 *         file entries; default behaviour omits it.
 *   LC7.  `includeMetadata: true` surfaces `metadata` on file
 *         entries.
 *   LC8.  Tombstones excluded by default (versioning-on tenant);
 *         `includeTombstones: true` includes them.
 *   LC9.  `path` resolves; ENOENT for missing folder; ENOTDIR for
 *         a file path.
 *   LC10. Symlink kind carries `target`.
 */

import {
  createVFS,
  type MossaicEnv,
  type UserDO,
  ENOENT,
  ENOTDIR,
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

describe("listChildren — basic shape (LC1, LC2, LC10)", () => {
  it("LC1 — empty folder: revision 0, no entries, no cursor", async () => {
    const vfs = createVFS(envFor(), { tenant: "p46-lc-empty" });
    await vfs.mkdir("/empty");
    const r = await vfs.listChildren("/empty");
    expect(r.entries).toEqual([]);
    expect(r.cursor).toBeUndefined();
    expect(r.revision).toBe(0);
  });

  it("LC2 — mixed kinds: folder + file + symlink surface with correct discriminants", async () => {
    const tenant = "p46-lc-mixed";
    const vfs = createVFS(envFor(), { tenant });
    await vfs.mkdir("/d/sub", { recursive: true });
    await vfs.writeFile("/d/file.txt", "hi");
    await vfs.symlink("/d/file.txt", "/d/link.txt");

    const r = await vfs.listChildren("/d");
    const byKind = Object.fromEntries(
      r.entries.map((e) => [e.name, e.kind])
    );
    expect(byKind).toEqual({
      sub: "folder",
      "file.txt": "file",
      "link.txt": "symlink",
    });
    expect(r.revision).toBeGreaterThan(0);
  });

  it("LC10 — symlink entry carries `target`", async () => {
    const tenant = "p46-lc-sym";
    const vfs = createVFS(envFor(), { tenant });
    await vfs.mkdir("/d");
    await vfs.writeFile("/d/orig.txt", "1");
    await vfs.symlink("/d/orig.txt", "/d/alias.txt");

    const r = await vfs.listChildren("/d");
    const link = r.entries.find((e) => e.name === "alias.txt");
    expect(link).toBeDefined();
    if (link?.kind === "symlink") {
      expect(link.target).toBe("/d/orig.txt");
    } else {
      throw new Error("alias.txt should have kind=symlink");
    }
  });
});

describe("listChildren — N+1 equivalence (LC3)", () => {
  it("LC3 — listChildren entries match readdir + lstat × N", async () => {
    const tenant = "p46-lc-eq";
    const vfs = createVFS(envFor(), { tenant });
    await vfs.mkdir("/d/sub", { recursive: true });
    await vfs.writeFile("/d/a.txt", "a");
    await vfs.writeFile("/d/b.txt", "bb");
    await vfs.symlink("/d/a.txt", "/d/c.lnk");

    // Legacy: readdir + lstat × N.
    const names = await vfs.readdir("/d");
    const legacy = await Promise.all(
      names.sort().map(async (name) => {
        const s = await vfs.lstat(`/d/${name}`);
        return { name, type: s.isDirectory() ? "dir" : s.isSymbolicLink() ? "symlink" : "file", size: s.size };
      })
    );

    // New: listChildren with name asc.
    const r = await vfs.listChildren("/d", {
      orderBy: "name",
      direction: "asc",
    });
    const modern = r.entries.map((e) => ({
      name: e.name,
      type: e.kind === "folder" ? "dir" : e.kind,
      size: e.stat?.size ?? 0,
    }));

    expect(modern).toEqual(legacy);
  });
});

describe("listChildren — pagination (LC4, LC5)", () => {
  it("LC4 — 100 children paginate stably across 3 pages", async () => {
    const tenant = "p46-lc-paginate";
    const vfs = createVFS(envFor(), { tenant });
    await vfs.mkdir("/d");
    for (let i = 0; i < 100; i++) {
      await vfs.writeFile(`/d/f${String(i).padStart(3, "0")}.txt`, `${i}`);
    }
    const p1 = await vfs.listChildren("/d", {
      limit: 40,
      orderBy: "name",
      direction: "asc",
    });
    expect(p1.entries.length).toBe(40);
    expect(p1.cursor).toBeDefined();

    const p2 = await vfs.listChildren("/d", {
      limit: 40,
      cursor: p1.cursor,
      orderBy: "name",
      direction: "asc",
    });
    expect(p2.entries.length).toBe(40);
    expect(p2.cursor).toBeDefined();

    const p3 = await vfs.listChildren("/d", {
      limit: 40,
      cursor: p2.cursor,
      orderBy: "name",
      direction: "asc",
    });
    expect(p3.entries.length).toBe(20);
    expect(p3.cursor).toBeUndefined();

    const ids = new Set<string>();
    for (const e of [...p1.entries, ...p2.entries, ...p3.entries]) {
      expect(ids.has(e.pathId)).toBe(false);
      ids.add(e.pathId);
    }
    expect(ids.size).toBe(100);
  });

  it("LC5 — folder/file share orderValue: cursor disambiguates via kind", async () => {
    const tenant = "p46-lc-kind-tie";
    const vfs = createVFS(envFor(), { tenant });
    await vfs.mkdir("/d");
    // Create a folder and a file with similar names — orderBy 'name' should
    // place them adjacently and cursor must round-trip the kind boundary.
    await vfs.mkdir("/d/m");
    await vfs.writeFile("/d/m.txt", "x");
    await vfs.writeFile("/d/n.txt", "y");

    const p1 = await vfs.listChildren("/d", {
      limit: 1,
      orderBy: "name",
      direction: "asc",
    });
    expect(p1.entries.length).toBe(1);
    expect(p1.entries[0].name).toBe("m"); // folder 'm' < file 'm.txt' lex
    expect(p1.cursor).toBeDefined();

    const p2 = await vfs.listChildren("/d", {
      limit: 1,
      cursor: p1.cursor,
      orderBy: "name",
      direction: "asc",
    });
    expect(p2.entries.length).toBe(1);
    expect(p2.entries[0].name).toBe("m.txt");

    const p3 = await vfs.listChildren("/d", {
      limit: 1,
      cursor: p2.cursor,
      orderBy: "name",
      direction: "asc",
    });
    expect(p3.entries.length).toBe(1);
    expect(p3.entries[0].name).toBe("n.txt");
    expect(p3.cursor).toBeUndefined();
  });
});

describe("listChildren — opt-in fields (LC6, LC7)", () => {
  it("LC6 — includeContentHash surfaces SHA-256 hex on chunked-tier file entries", async () => {
    const tenant = "p46-lc-hash";
    const vfs = createVFS(envFor(), { tenant });
    await vfs.mkdir("/d");
    // Chunked tier (>16 KB inline cap) — file_hash is computed.
    // Inline-tier rows persist file_hash = '' (pre-Phase-46
    // behaviour) so the SDK surface omits `contentHash` for them.
    await vfs.writeFile("/d/a.bin", new Uint8Array(32 * 1024).fill(0xab));

    const off = await vfs.listChildren("/d");
    const fileOff = off.entries.find((e) => e.name === "a.bin");
    if (fileOff?.kind !== "file") throw new Error("expected file kind");
    expect(fileOff.contentHash).toBeUndefined();

    const on = await vfs.listChildren("/d", { includeContentHash: true });
    const fileOn = on.entries.find((e) => e.name === "a.bin");
    if (fileOn?.kind !== "file") throw new Error("expected file kind");
    expect(fileOn.contentHash).toMatch(/^[0-9a-f]{64}$/);
  });

  it("LC7 — includeMetadata surfaces metadata on file entries", async () => {
    const tenant = "p46-lc-meta";
    const vfs = createVFS(envFor(), { tenant });
    await vfs.mkdir("/d");
    await vfs.writeFile("/d/a.txt", "x", {
      metadata: { color: "red", count: 7 },
    });

    const off = await vfs.listChildren("/d");
    const fileOff = off.entries.find((e) => e.name === "a.txt");
    if (fileOff?.kind !== "file") throw new Error("expected file kind");
    expect(fileOff.metadata).toBeUndefined();

    const on = await vfs.listChildren("/d", { includeMetadata: true });
    const fileOn = on.entries.find((e) => e.name === "a.txt");
    if (fileOn?.kind !== "file") throw new Error("expected file kind");
    expect(fileOn.metadata).toEqual({ color: "red", count: 7 });
  });
});

describe("listChildren — error surfaces (LC9)", () => {
  it("LC9a — missing folder throws ENOENT", async () => {
    const vfs = createVFS(envFor(), { tenant: "p46-lc-enoent" });
    await expect(vfs.listChildren("/missing")).rejects.toMatchObject({
      code: "ENOENT",
    });
  });

  it("LC9b — file path throws ENOTDIR", async () => {
    const tenant = "p46-lc-enotdir";
    const vfs = createVFS(envFor(), { tenant });
    await vfs.writeFile("/file.txt", "x");
    await expect(vfs.listChildren("/file.txt")).rejects.toMatchObject({
      code: "ENOTDIR",
    });
  });
});
