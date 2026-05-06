/**
 * E2E B — Basic file ops (10 cases).
 */

import { describe, it, expect, beforeEach, afterEach, beforeAll } from "vitest";
import { freshTenant, type TenantCtx } from "./helpers/tenant.js";
import { hasSecret, requireSecret } from "./helpers/env.js";

describe.skipIf(!hasSecret())("B — Basic file ops", () => {
  beforeAll(() => requireSecret());

  let ctx: TenantCtx;
  beforeEach(async () => { ctx = await freshTenant(); });
  afterEach(async () => { await ctx.teardown(); });

  it("B.1 — writeFile/readFile UTF-8 round-trip + bytes round-trip", async () => {
    await ctx.vfs.writeFile("/utf8.txt", "héllo wörld");
    const back = await ctx.vfs.readFile("/utf8.txt", { encoding: "utf8" });
    expect(back).toBe("héllo wörld");

    const payload = new Uint8Array([0xde, 0xad, 0xbe, 0xef, 0xca, 0xfe]);
    await ctx.vfs.writeFile("/raw.bin", payload);
    const bytes = await ctx.vfs.readFile("/raw.bin");
    expect(bytes.byteLength).toBe(6);
    expect([...bytes]).toEqual([...payload]);
  });

  it("B.2 — mkdir({recursive:true}) + readdir lists nested entries", async () => {
    await ctx.vfs.mkdir("/a/b/c", { recursive: true });
    await ctx.vfs.writeFile("/a/b/c/x.txt", "x");
    const entries = await ctx.vfs.readdir("/a/b/c");
    expect(entries).toContain("x.txt");
    const root = await ctx.vfs.readdir("/");
    expect(root).toContain("a");
  });

  it("B.3 — rmdir on non-empty fails ENOTEMPTY; removeRecursive succeeds", async () => {
    await ctx.vfs.mkdir("/d", { recursive: true });
    await ctx.vfs.writeFile("/d/a.txt", "x");
    await expect(ctx.vfs.rmdir("/d")).rejects.toMatchObject({ code: "ENOTEMPTY" });
    await ctx.vfs.removeRecursive("/d");
    expect(await ctx.vfs.exists("/d")).toBe(false);
  });

  it("B.4 — stat reports isFile, mtimeMs, size", async () => {
    const before = Date.now();
    await ctx.vfs.writeFile("/s.txt", "hi");
    const s = await ctx.vfs.stat("/s.txt");
    expect(s.isFile()).toBe(true);
    expect(s.size).toBe(2);
    expect(s.mtimeMs).toBeGreaterThanOrEqual(before - 1000);
    expect(s.mtimeMs).toBeLessThan(Date.now() + 1000);
  });

  it("B.5 — symlink + lstat + readlink", async () => {
    await ctx.vfs.writeFile("/target.txt", "t");
    await ctx.vfs.symlink("/target.txt", "/link");
    const ls = await ctx.vfs.lstat("/link");
    expect(ls.isSymbolicLink()).toBe(true);
    const r = await ctx.vfs.readlink("/link");
    expect(r).toBe("/target.txt");
  });

  it("B.6 — rename moves a file", async () => {
    await ctx.vfs.writeFile("/old.txt", "v");
    await ctx.vfs.rename("/old.txt", "/new.txt");
    expect(await ctx.vfs.exists("/old.txt")).toBe(false);
    expect(await ctx.vfs.exists("/new.txt")).toBe(true);
  });

  it("B.7 — chmod is reflected in stat.mode", async () => {
    await ctx.vfs.writeFile("/m.txt", "m");
    await ctx.vfs.chmod("/m.txt", 0o600);
    const s = await ctx.vfs.stat("/m.txt");
    expect(s.mode & 0o777).toBe(0o600);
  });

  it("B.8 — readManyStat returns null for missing without throwing", async () => {
    await ctx.vfs.writeFile("/a.txt", "a");
    await ctx.vfs.writeFile("/b.txt", "b");
    const stats = await ctx.vfs.readManyStat(["/a.txt", "/b.txt", "/nope.txt"]);
    expect(stats[0]?.size).toBe(1);
    expect(stats[1]?.size).toBe(1);
    expect(stats[2]).toBe(null);
  });

  it("B.9 — unlink on missing path → ENOENT", async () => {
    await expect(ctx.vfs.unlink("/no-such-file")).rejects.toMatchObject({
      code: "ENOENT",
    });
  });

  it("B.10 — mkdir on existing without recursive → EEXIST", async () => {
    await ctx.vfs.mkdir("/dx");
    await expect(ctx.vfs.mkdir("/dx")).rejects.toMatchObject({ code: "EEXIST" });
    // recursive: true is idempotent.
    await ctx.vfs.mkdir("/dx", { recursive: true });
  });
});
