/**
 * E2E D — metadata/tags/copyFile/listFiles + cursor pagination
 * + tampering + caps (12 cases).
 */

import { describe, it, expect, beforeEach, afterEach, beforeAll } from "vitest";
import { freshTenant, type TenantCtx } from "./helpers/tenant.js";
import { hasSecret, requireSecret } from "./helpers/env.js";
import { METADATA_MAX_BYTES, TAGS_MAX_PER_FILE } from "@mossaic/sdk/http";

describe.skipIf(!hasSecret())("D — metadata/tags/copyFile/listFiles", () => {
  beforeAll(() => requireSecret());

  let ctx: TenantCtx;
  beforeEach(async () => { ctx = await freshTenant(); });
  afterEach(async () => { await ctx.teardown(); });

  it("D.1 — writeFile w/ metadata + tags; find({tags}) returns it", async () => {
    await ctx.vfs.writeFile("/p1.bin", new Uint8Array([1, 2, 3]), {
      metadata: { camera: "Pixel 8" },
      tags: ["nature", "2026"],
    });
    const page = await ctx.vfs.listFiles({ tags: ["nature"], includeMetadata: true });
    const item = page.items.find((i) => i.path === "/p1.bin");
    expect(item).toBeDefined();
    expect(item!.tags).toContain("nature");
    expect(item!.metadata).toMatchObject({ camera: "Pixel 8" });
  });

  it("D.2 — patchMetadata deep-merge", async () => {
    await ctx.vfs.writeFile("/p2.bin", "x", { metadata: { a: { b: 1 } } });
    await ctx.vfs.patchMetadata("/p2.bin", { a: { c: 2 } });
    const page = await ctx.vfs.listFiles({ prefix: "/", includeMetadata: true });
    const item = page.items.find((i) => i.path === "/p2.bin")!;
    expect(item.metadata).toMatchObject({ a: { b: 1, c: 2 } });
  });

  it("D.3 — patchMetadata null-leaf removes a key", async () => {
    await ctx.vfs.writeFile("/p3.bin", "x", { metadata: { keep: 1, drop: 2 } });
    await ctx.vfs.patchMetadata("/p3.bin", { drop: null });
    const page = await ctx.vfs.listFiles({ prefix: "/", includeMetadata: true });
    const item = page.items.find((i) => i.path === "/p3.bin")!;
    expect(item.metadata).toEqual({ keep: 1 });
  });

  it("D.4 — patchMetadata(p, null) clears the metadata blob", async () => {
    await ctx.vfs.writeFile("/p4.bin", "x", { metadata: { a: 1 } });
    await ctx.vfs.patchMetadata("/p4.bin", null);
    const page = await ctx.vfs.listFiles({ prefix: "/", includeMetadata: true });
    const item = page.items.find((i) => i.path === "/p4.bin")!;
    expect(item.metadata == null).toBe(true);
  });

  it("D.5 — patchMetadata addTags + removeTags atomic with metadata patch", async () => {
    await ctx.vfs.writeFile("/p5.bin", "x", {
      metadata: { state: "pending" },
      tags: ["pending"],
    });
    await ctx.vfs.patchMetadata("/p5.bin", { state: "approved" }, {
      addTags: ["approved"],
      removeTags: ["pending"],
    });
    const page = await ctx.vfs.listFiles({ tags: ["approved"], includeMetadata: true });
    const item = page.items.find((i) => i.path === "/p5.bin")!;
    expect(item.metadata).toEqual({ state: "approved" });
    expect(item.tags.sort()).toEqual(["approved"]);
  });

  it("D.6 — copyFile inherits source metadata + tags by default", async () => {
    await ctx.vfs.writeFile("/src.bin", "src", {
      metadata: { src: true },
      tags: ["origin"],
    });
    await ctx.vfs.copyFile("/src.bin", "/dest.bin");
    const page = await ctx.vfs.listFiles({ tags: ["origin"], includeMetadata: true });
    const dest = page.items.find((i) => i.path === "/dest.bin")!;
    expect(dest.metadata).toEqual({ src: true });
    expect(dest.tags).toContain("origin");
  });

  it("D.7 — copyFile with explicit metadata/tags REPLACES (not merges)", async () => {
    await ctx.vfs.writeFile("/srcA.bin", "x", {
      metadata: { a: 1 },
      tags: ["one"],
    });
    await ctx.vfs.copyFile("/srcA.bin", "/destA.bin", {
      metadata: { b: 2 },
      tags: ["two"],
    });
    const page = await ctx.vfs.listFiles({ prefix: "/", includeMetadata: true });
    const dest = page.items.find((i) => i.path === "/destA.bin")!;
    expect(dest.metadata).toEqual({ b: 2 });
    expect(dest.tags).toEqual(["two"]);
  });

  it("D.8 — copyFile overwrite:false on existing dest → EEXIST", async () => {
    await ctx.vfs.writeFile("/d8src.bin", "s");
    await ctx.vfs.writeFile("/d8dest.bin", "d");
    await expect(
      ctx.vfs.copyFile("/d8src.bin", "/d8dest.bin", { overwrite: false }),
    ).rejects.toMatchObject({ code: "EEXIST" });
  });

  it("D.9 — listFiles cursor pagination across many files", async () => {
    // Write 25 small files. Per-tenant rate limit is 100/s default
    // so this is well under; sequential to avoid burst pressure.
    const total = 25;
    for (let i = 0; i < total; i++) {
      await ctx.vfs.writeFile(
        `/file-${String(i).padStart(3, "0")}.txt`,
        `n=${i}`,
        { tags: ["seed"] },
      );
    }
    const seenPaths = new Set<string>();
    let cursor: string | undefined;
    let pages = 0;
    do {
      const page = await ctx.vfs.listFiles({ tags: ["seed"], limit: 10, cursor });
      pages++;
      for (const item of page.items) {
        expect(seenPaths.has(item.path)).toBe(false);
        seenPaths.add(item.path);
      }
      cursor = page.cursor;
    } while (cursor && pages < 10);
    expect(seenPaths.size).toBe(total);
    expect(pages).toBeGreaterThanOrEqual(3);
  }, 180_000);

  it("D.10 — tampered cursor → EINVAL", async () => {
    for (let i = 0; i < 12; i++) {
      await ctx.vfs.writeFile(`/d10-${i}.txt`, `n=${i}`, { tags: ["d10"] });
    }
    const page1 = await ctx.vfs.listFiles({ tags: ["d10"], limit: 5 });
    expect(page1.cursor).toBeDefined();
    const c = page1.cursor!;
    // Flip the middle character (base64-url alphabet 'A' or '0' → swap to a
    // different valid char).
    const mid = Math.floor(c.length / 2);
    const original = c[mid];
    const swap = original === "A" ? "B" : "A";
    const flipped = c.slice(0, mid) + swap + c.slice(mid + 1);
    await expect(
      ctx.vfs.listFiles({ tags: ["d10"], limit: 5, cursor: flipped }),
    ).rejects.toMatchObject({ code: "EINVAL" });
  }, 60_000);

  it("D.11 — cursor reuse with mismatched orderBy → EINVAL", async () => {
    for (let i = 0; i < 6; i++) {
      await ctx.vfs.writeFile(`/d11-${i}.txt`, "x", { tags: ["d11"] });
    }
    const page1 = await ctx.vfs.listFiles({
      tags: ["d11"],
      limit: 3,
      orderBy: "mtime",
    });
    expect(page1.cursor).toBeDefined();
    await expect(
      ctx.vfs.listFiles({
        tags: ["d11"],
        limit: 3,
        cursor: page1.cursor,
        orderBy: "name",
      }),
    ).rejects.toMatchObject({ code: "EINVAL" });
  });

  it("D.12 — cap enforcement: oversized metadata, too many tags, bad charset", async () => {
    // a) Oversized metadata
    const oversize = "x".repeat(METADATA_MAX_BYTES + 100);
    await expect(
      ctx.vfs.writeFile("/d12a.bin", "x", { metadata: { huge: oversize } }),
    ).rejects.toMatchObject({ code: "EINVAL" });

    // b) Too many tags
    const tooMany = Array.from({ length: TAGS_MAX_PER_FILE + 1 }, (_, i) => `t${i}`);
    await expect(
      ctx.vfs.writeFile("/d12b.bin", "x", { tags: tooMany }),
    ).rejects.toMatchObject({ code: "EINVAL" });

    // c) Bad charset
    await expect(
      ctx.vfs.writeFile("/d12c.bin", "x", { tags: ["bad!char"] }),
    ).rejects.toMatchObject({ code: "EINVAL" });
  });
});
