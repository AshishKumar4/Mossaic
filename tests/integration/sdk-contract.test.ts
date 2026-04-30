import { describe, it, expect } from "vitest";
import { env } from "cloudflare:test";

/**
 * Phase 26 — SDK ↔ Worker contract test (audit gap, prompt Fix 5).
 *
 * `tests/unit/sdk-surface-parity.test.ts` is shape-only: it asserts
 * the same exports exist on `@mossaic/sdk` and `@mossaic/sdk/http`,
 * but never actually CALLS any of them through the worker. That
 * leaves a drift surface — the worker's RPC return shape can change
 * without breaking either SDK build, then ship a runtime decoder
 * mismatch for users.
 *
 * This file drives every public VFSClient method against a real
 * worker (binding-mode, via createVFS), and validates the returned
 * value against the SDK's declared types using runtime structural
 * assertions. If the worker stops returning a field (or returns it
 * under a renamed key, or with a wrong primitive type), this test
 * fails BEFORE the SDK consumer sees it as an undefined property.
 *
 * Strategy: one tenant, fixture data created up-front, then a
 * battery of RPC drives. Each `it()` covers one SDK method and
 * pins the contract.
 *
 * Pinned invariants:
 *
 *   C1.  stat / lstat returns a VFSStat instance with all required
 *        fields and methods (isFile, isDirectory, etc).
 *   C2.  exists returns boolean (not truthy/falsy non-boolean).
 *   C3.  readFile (no opts) returns Uint8Array; with utf8 returns
 *        string. Type discrimination matches the overload set.
 *   C4.  readManyStat returns one entry per input path; missing
 *        paths surface as `null`, not throw.
 *   C5.  readdir returns string[].
 *   C6.  listFiles returns { items: ListFilesItem[], cursor?: string }
 *        where each item has the documented shape.
 *   C7.  writeFile / unlink / mkdir / rmdir return undefined (void).
 *   C8.  copyFile returns undefined; the dest reads back correctly.
 *   C9.  symlink / readlink round-trip.
 *   C10. listVersions returns VersionInfo[] (or [] on
 *        versioning-OFF tenants).
 *   C11. fileInfo returns a ListFilesItem.
 *   C12. openManifest returns either inline data or a chunk array
 *        (discriminated union).
 */

import {
  createVFS,
  VFSStat,
  type MossaicEnv,
  type UserDO,
} from "../../sdk/src/index";

interface E {
  MOSSAIC_USER: DurableObjectNamespace<UserDO>;
}
const E = env as unknown as E;

function envFor(): MossaicEnv {
  return { MOSSAIC_USER: E.MOSSAIC_USER as MossaicEnv["MOSSAIC_USER"] };
}

const TXT = "sdk-contract bytes\n";

describe("SDK ↔ Worker contract — read surface (C1, C2, C3, C5)", () => {
  it("C1 — stat / lstat returns a VFSStat instance with full fs.Stats shape", async () => {
    const vfs = createVFS(envFor(), { tenant: "sdk-contract-stat" });
    await vfs.writeFile("/a.txt", TXT);

    const s = await vfs.stat("/a.txt");
    expect(s).toBeInstanceOf(VFSStat);
    expect(s.type).toBe("file");
    expect(typeof s.mode).toBe("number");
    expect(typeof s.size).toBe("number");
    expect(typeof s.mtimeMs).toBe("number");
    expect(typeof s.atimeMs).toBe("number");
    expect(typeof s.ctimeMs).toBe("number");
    expect(typeof s.birthtimeMs).toBe("number");
    expect(s.mtime).toBeInstanceOf(Date);
    expect(s.atime).toBeInstanceOf(Date);
    expect(typeof s.uid).toBe("number");
    expect(typeof s.gid).toBe("number");
    expect(typeof s.ino).toBe("number");
    expect(typeof s.dev).toBe("number");
    expect(typeof s.nlink).toBe("number");
    expect(typeof s.blksize).toBe("number");
    expect(typeof s.blocks).toBe("number");
    expect(typeof s.isFile).toBe("function");
    expect(typeof s.isDirectory).toBe("function");
    expect(typeof s.isSymbolicLink).toBe("function");
    expect(s.isFile()).toBe(true);
    expect(s.isDirectory()).toBe(false);
    expect(s.size).toBe(TXT.length);

    // lstat on the same path returns the same shape.
    const ls = await vfs.lstat("/a.txt");
    expect(ls).toBeInstanceOf(VFSStat);
    expect(ls.type).toBe("file");
  });

  it("C2 — exists returns strict boolean", async () => {
    const vfs = createVFS(envFor(), { tenant: "sdk-contract-exists" });
    await vfs.writeFile("/y.txt", "y");
    const yes = await vfs.exists("/y.txt");
    const no = await vfs.exists("/missing.txt");
    expect(yes).toBe(true);
    expect(no).toBe(false);
    // Strict boolean — not "truthy"/"falsy" of a non-boolean.
    expect(typeof yes).toBe("boolean");
    expect(typeof no).toBe("boolean");
  });

  it("C3 — readFile overloads: bytes by default, string with utf8", async () => {
    const vfs = createVFS(envFor(), { tenant: "sdk-contract-read" });
    await vfs.writeFile("/r.txt", TXT);
    const bytes = await vfs.readFile("/r.txt");
    expect(bytes).toBeInstanceOf(Uint8Array);
    expect(bytes.length).toBe(TXT.length);

    const str = await vfs.readFile("/r.txt", { encoding: "utf8" });
    expect(typeof str).toBe("string");
    expect(str).toBe(TXT);
  });

  it("C5 — readdir returns string[] (one entry per child name, no leading slash)", async () => {
    const vfs = createVFS(envFor(), { tenant: "sdk-contract-readdir" });
    await vfs.mkdir("/d");
    await vfs.writeFile("/d/a.txt", "a");
    await vfs.writeFile("/d/b.txt", "b");
    const names = await vfs.readdir("/d");
    expect(Array.isArray(names)).toBe(true);
    for (const n of names) {
      expect(typeof n).toBe("string");
      expect(n.startsWith("/")).toBe(false);
    }
    expect([...names].sort()).toEqual(["a.txt", "b.txt"]);
  });
});

describe("SDK ↔ Worker contract — batch read surface (C4)", () => {
  it("C4 — readManyStat: one entry per input, null for missing", async () => {
    const vfs = createVFS(envFor(), { tenant: "sdk-contract-many" });
    await vfs.writeFile("/exists.txt", "x");
    const got = await vfs.readManyStat([
      "/exists.txt",
      "/missing-1.txt",
      "/missing-2.txt",
    ]);
    expect(Array.isArray(got)).toBe(true);
    expect(got.length).toBe(3);
    expect(got[0]).toBeInstanceOf(VFSStat);
    expect(got[1]).toBeNull();
    expect(got[2]).toBeNull();
  });
});

describe("SDK ↔ Worker contract — listFiles + fileInfo (C6, C11)", () => {
  it("C6 — listFiles returns { items, cursor? } with documented item shape", async () => {
    const vfs = createVFS(envFor(), { tenant: "sdk-contract-list" });
    await vfs.writeFile("/a.txt", "1");
    await vfs.writeFile("/b.txt", "22");
    const page = await vfs.listFiles({ orderBy: "name" });
    expect(page).toBeTruthy();
    expect(Array.isArray(page.items)).toBe(true);
    expect(page.items.length).toBe(2);
    for (const it of page.items) {
      expect(typeof it.path).toBe("string");
      // pathId is the stable identity (file_id).
      expect(typeof it.pathId).toBe("string");
      // tags is always present, sorted.
      expect(Array.isArray(it.tags)).toBe(true);
      // stat is present by default (includeStat !== false).
      expect(it.stat).toBeInstanceOf(VFSStat);
      expect(typeof it.stat!.size).toBe("number");
      expect(typeof it.stat!.mtimeMs).toBe("number");
    }
    // cursor is optional. If present, it's a string.
    if (page.cursor !== undefined) {
      expect(typeof page.cursor).toBe("string");
    }
  });

  it("C11 — fileInfo returns a single ListFilesItem matching listFiles shape", async () => {
    const vfs = createVFS(envFor(), { tenant: "sdk-contract-info" });
    await vfs.writeFile("/x.txt", "xyz");
    const info = await vfs.fileInfo("/x.txt");
    expect(info.path).toBe("/x.txt");
    expect(typeof info.pathId).toBe("string");
    expect(Array.isArray(info.tags)).toBe(true);
    expect(info.stat).toBeInstanceOf(VFSStat);
    expect(info.stat!.size).toBe(3);
  });
});

describe("SDK ↔ Worker contract — write surface (C7, C8)", () => {
  it("C7 — writeFile / unlink / mkdir / rmdir all return undefined", async () => {
    const vfs = createVFS(envFor(), { tenant: "sdk-contract-write" });
    const w = await vfs.writeFile("/a.txt", "x");
    expect(w).toBeUndefined();

    const m = await vfs.mkdir("/d");
    expect(m).toBeUndefined();

    const u = await vfs.unlink("/a.txt");
    expect(u).toBeUndefined();

    const rd = await vfs.rmdir("/d");
    expect(rd).toBeUndefined();
  });

  it("C8 — copyFile: dest reads back the same bytes; method returns undefined", async () => {
    const vfs = createVFS(envFor(), { tenant: "sdk-contract-copy" });
    await vfs.writeFile("/src.txt", "hello-copy");
    const r = await vfs.copyFile("/src.txt", "/dst.txt");
    expect(r).toBeUndefined();
    const back = await vfs.readFile("/dst.txt", { encoding: "utf8" });
    expect(back).toBe("hello-copy");
  });
});

describe("SDK ↔ Worker contract — symlink + readlink (C9)", () => {
  it("C9 — symlink → readlink round-trip returns the exact target string", async () => {
    const vfs = createVFS(envFor(), { tenant: "sdk-contract-symlink" });
    await vfs.writeFile("/target.txt", "T");
    await vfs.symlink("/target.txt", "/link.txt");
    const tgt = await vfs.readlink("/link.txt");
    expect(typeof tgt).toBe("string");
    expect(tgt).toBe("/target.txt");
  });
});

describe("SDK ↔ Worker contract — versioning surface (C10)", () => {
  it("C10 — listVersions returns [] on a versioning-OFF tenant (no throw)", async () => {
    const vfs = createVFS(envFor(), { tenant: "sdk-contract-versions-off" });
    await vfs.writeFile("/x.txt", "x");
    const got = await vfs.listVersions("/x.txt");
    expect(Array.isArray(got)).toBe(true);
    expect(got.length).toBe(0);
  });

  it("C10b — listVersions on a versioning-ON tenant returns VersionInfo[] with documented fields", async () => {
    const vfs = createVFS(envFor(), {
      tenant: "sdk-contract-versions-on",
      versioning: "enabled",
    });
    await vfs.writeFile("/x.txt", "v1");
    await vfs.writeFile("/x.txt", "v2");
    const got = await vfs.listVersions("/x.txt");
    expect(got.length).toBe(2);
    for (const v of got) {
      expect(typeof v.id).toBe("string");
      expect(typeof v.size).toBe("number");
      expect(typeof v.mtimeMs).toBe("number");
      expect(typeof v.deleted).toBe("boolean");
    }
  });
});

describe("SDK ↔ Worker contract — manifest escape hatch (C12)", () => {
  it("C12 — openManifest discriminator: inline tier carries inline bytes; chunked tier carries chunk array", async () => {
    const vfs = createVFS(envFor(), { tenant: "sdk-contract-manifest" });
    // Inline tier (≤16KB).
    await vfs.writeFile("/small.txt", "small");
    const inline = await vfs.openManifest("/small.txt");
    expect(typeof inline.inlined).toBe("boolean");
    expect(inline.inlined).toBe(true);
    expect(inline.chunks).toBeDefined();
    // Chunked tier (>16KB).
    const big = new Uint8Array(20_000);
    for (let i = 0; i < big.length; i++) big[i] = i & 0xff;
    await vfs.writeFile("/big.bin", big);
    const chunked = await vfs.openManifest("/big.bin");
    expect(chunked.inlined).toBe(false);
    expect(Array.isArray(chunked.chunks)).toBe(true);
    expect(chunked.chunks.length).toBeGreaterThan(0);
    expect(typeof chunked.fileId).toBe("string");
    expect(typeof chunked.size).toBe("number");
    expect(typeof chunked.chunkSize).toBe("number");
    expect(typeof chunked.chunkCount).toBe("number");
    for (const c of chunked.chunks) {
      expect(typeof c.index).toBe("number");
      expect(typeof c.hash).toBe("string");
      expect(typeof c.size).toBe("number");
    }
  });
});
