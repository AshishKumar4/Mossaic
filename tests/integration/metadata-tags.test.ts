import { describe, it, expect } from "vitest";
import { env, runInDurableObject } from "cloudflare:test";

/**
 * Phase 12 — metadata + tags as first-class file properties.
 *
 * Pinned invariants:
 *   M1. writeFile metadata round-trip: write `{a:1,b:{c:2}}`, then
 *       a later listFiles({includeMetadata:true}) returns the same
 *       structure verbatim.
 *   M2. patchMetadata deep-merge: existing `{a:1,b:{c:2}}` + patch
 *       `{b:{d:3}}` → `{a:1,b:{c:2,d:3}}` (recurse on objects, NOT
 *       arrays).
 *   M3. patchMetadata null-leaf tombstone: `{a:1,b:2}` + `{a:null}`
 *       → `{b:2}`.
 *   M4. patchMetadata null root: clears metadata column to NULL.
 *   M5. Tags round-trip via writeFile + listFiles-by-tag.
 *   M6. Tag mtime bump: writing a file updates `file_tags.mtime_ms`
 *       so list-by-tag orderBy:mtime DESC reflects recency.
 *   M7. All cap violations throw EINVAL pre-flight (no SQL touches
 *       the row).
 */

import { createVFS, type MossaicEnv, type UserDO, EINVAL } from "../../sdk/src/index";
import { vfsUserDOName } from "@core/lib/utils";

interface E {
  USER_DO: DurableObjectNamespace<UserDO>;
  SHARD_DO: DurableObjectNamespace;
}
const E = env as unknown as E;
const NS = "default";

function envFor(): MossaicEnv {
  return { MOSSAIC_USER: E.USER_DO as MossaicEnv["MOSSAIC_USER"] };
}
function userStub(tenant: string) {
  return E.USER_DO.get(E.USER_DO.idFromName(vfsUserDOName(NS, tenant)));
}

describe("Phase 12 — writeFile metadata + tags (M1, M5, M6)", () => {
  it("metadata round-trip via raw SQL inspection", async () => {
    const tenant = "p12-meta-rt";
    const vfs = createVFS(envFor(), { tenant });
    await vfs.writeFile("/note.txt", "hi", {
      metadata: { project: "alpha", priority: 5, tags: ["a", "b"] },
    });

    const stub = userStub(tenant);
    const blob = await runInDurableObject(stub, async (_inst, state) => {
      const r = state.storage.sql
        .exec("SELECT metadata FROM files WHERE file_name = 'note.txt'")
        .toArray()[0] as { metadata: ArrayBuffer };
      return new TextDecoder().decode(new Uint8Array(r.metadata));
    });
    const parsed = JSON.parse(blob);
    expect(parsed).toEqual({ project: "alpha", priority: 5, tags: ["a", "b"] });
  });

  it("tags written via writeFile land in file_tags with mtime", async () => {
    const tenant = "p12-tags-rt";
    const vfs = createVFS(envFor(), { tenant });
    const before = Date.now() - 1;
    await vfs.writeFile("/photo.jpg", "x", {
      tags: ["urgent", "client/acme"],
    });
    const stub = userStub(tenant);
    const rows = await runInDurableObject(stub, async (_inst, state) => {
      return state.storage.sql
        .exec("SELECT tag, mtime_ms FROM file_tags ORDER BY tag")
        .toArray() as { tag: string; mtime_ms: number }[];
    });
    expect(rows.map((r) => r.tag)).toEqual(["client/acme", "urgent"]);
    expect(rows[0].mtime_ms).toBeGreaterThan(before);
  });

  it("tag mtime bump on overwrite", async () => {
    const tenant = "p12-tag-bump";
    const vfs = createVFS(envFor(), { tenant });
    await vfs.writeFile("/a.txt", "v1", { tags: ["t"] });
    const stub = userStub(tenant);
    const m1 = await runInDurableObject(stub, async (_, state) => {
      return (
        state.storage.sql
          .exec("SELECT mtime_ms FROM file_tags WHERE tag = 't'")
          .toArray()[0] as { mtime_ms: number }
      ).mtime_ms;
    });
    await new Promise((r) => setTimeout(r, 5));
    await vfs.writeFile("/a.txt", "v2"); // no tags opt → bump only
    const m2 = await runInDurableObject(stub, async (_, state) => {
      return (
        state.storage.sql
          .exec("SELECT mtime_ms FROM file_tags WHERE tag = 't'")
          .toArray()[0] as { mtime_ms: number }
      ).mtime_ms;
    });
    expect(m2).toBeGreaterThanOrEqual(m1);
  });

  it("explicit tags=[] drops all tags", async () => {
    const tenant = "p12-tags-clear";
    const vfs = createVFS(envFor(), { tenant });
    await vfs.writeFile("/a.txt", "x", { tags: ["one", "two"] });
    await vfs.writeFile("/a.txt", "y", { tags: [] });
    const stub = userStub(tenant);
    const count = await runInDurableObject(stub, async (_, state) => {
      return (
        state.storage.sql
          .exec("SELECT COUNT(*) AS n FROM file_tags")
          .toArray()[0] as { n: number }
      ).n;
    });
    expect(count).toBe(0);
  });

  it("metadata=null on writeFile clears the column", async () => {
    const tenant = "p12-meta-clear";
    const vfs = createVFS(envFor(), { tenant });
    await vfs.writeFile("/a.txt", "x", { metadata: { keep: 1 } });
    await vfs.writeFile("/a.txt", "y", { metadata: null });
    const stub = userStub(tenant);
    const blob = await runInDurableObject(stub, async (_, state) => {
      return (
        state.storage.sql
          .exec("SELECT metadata FROM files WHERE file_name='a.txt'")
          .toArray()[0] as { metadata: ArrayBuffer | null }
      ).metadata;
    });
    expect(blob).toBeNull();
  });
});

describe("Phase 12 — patchMetadata (M2, M3, M4)", () => {
  it("deep-merge: nested object recursion", async () => {
    const tenant = "p12-merge-deep";
    const vfs = createVFS(envFor(), { tenant });
    await vfs.writeFile("/a.txt", "x", {
      metadata: { a: 1, b: { c: 2 } },
    });
    await vfs.patchMetadata("/a.txt", { b: { d: 3 } });
    const stub = userStub(tenant);
    const blob = await runInDurableObject(stub, async (_, state) => {
      return new TextDecoder().decode(
        new Uint8Array(
          (
            state.storage.sql
              .exec("SELECT metadata FROM files WHERE file_name='a.txt'")
              .toArray()[0] as { metadata: ArrayBuffer }
          ).metadata
        )
      );
    });
    expect(JSON.parse(blob)).toEqual({ a: 1, b: { c: 2, d: 3 } });
  });

  it("null leaf is a tombstone (deletes key)", async () => {
    const tenant = "p12-merge-null-leaf";
    const vfs = createVFS(envFor(), { tenant });
    await vfs.writeFile("/a.txt", "x", { metadata: { a: 1, b: 2 } });
    await vfs.patchMetadata("/a.txt", { a: null });
    const stub = userStub(tenant);
    const blob = await runInDurableObject(stub, async (_, state) => {
      return new TextDecoder().decode(
        new Uint8Array(
          (
            state.storage.sql
              .exec("SELECT metadata FROM files WHERE file_name='a.txt'")
              .toArray()[0] as { metadata: ArrayBuffer }
          ).metadata
        )
      );
    });
    expect(JSON.parse(blob)).toEqual({ b: 2 });
  });

  it("null root clears the metadata column", async () => {
    const tenant = "p12-merge-null-root";
    const vfs = createVFS(envFor(), { tenant });
    await vfs.writeFile("/a.txt", "x", { metadata: { a: 1 } });
    await vfs.patchMetadata("/a.txt", null);
    const stub = userStub(tenant);
    const meta = await runInDurableObject(stub, async (_, state) => {
      return (
        state.storage.sql
          .exec("SELECT metadata FROM files WHERE file_name='a.txt'")
          .toArray()[0] as { metadata: ArrayBuffer | null }
      ).metadata;
    });
    expect(meta).toBeNull();
  });

  it("array values are REPLACED, not merged", async () => {
    const tenant = "p12-merge-array";
    const vfs = createVFS(envFor(), { tenant });
    await vfs.writeFile("/a.txt", "x", { metadata: { xs: [1, 2, 3] } });
    await vfs.patchMetadata("/a.txt", { xs: [99] });
    const stub = userStub(tenant);
    const blob = await runInDurableObject(stub, async (_, state) => {
      return new TextDecoder().decode(
        new Uint8Array(
          (
            state.storage.sql
              .exec("SELECT metadata FROM files WHERE file_name='a.txt'")
              .toArray()[0] as { metadata: ArrayBuffer }
          ).metadata
        )
      );
    });
    expect(JSON.parse(blob)).toEqual({ xs: [99] });
  });

  it("addTags + removeTags atomic", async () => {
    const tenant = "p12-tags-patch";
    const vfs = createVFS(envFor(), { tenant });
    await vfs.writeFile("/a.txt", "x", { tags: ["a", "b", "c"] });
    await vfs.patchMetadata("/a.txt", null, {
      addTags: ["d"],
      removeTags: ["a"],
    });
    const stub = userStub(tenant);
    const tags = await runInDurableObject(stub, async (_, state) => {
      return (
        state.storage.sql
          .exec("SELECT tag FROM file_tags ORDER BY tag")
          .toArray() as { tag: string }[]
      ).map((r) => r.tag);
    });
    expect(tags).toEqual(["b", "c", "d"]);
  });

  it("ENOENT on missing path", async () => {
    const tenant = "p12-patch-missing";
    const vfs = createVFS(envFor(), { tenant });
    await expect(vfs.patchMetadata("/missing", { a: 1 })).rejects.toThrow();
  });
});

describe("Phase 12 — cap validation (M7)", () => {
  it("metadata over 64KB → EINVAL", async () => {
    const tenant = "p12-meta-cap";
    const vfs = createVFS(envFor(), { tenant });
    const huge = { blob: "x".repeat(70_000) };
    await expect(
      vfs.writeFile("/a.txt", "x", { metadata: huge })
    ).rejects.toBeInstanceOf(EINVAL);
  });

  it("tags > 32 → EINVAL", async () => {
    const tenant = "p12-tags-count";
    const vfs = createVFS(envFor(), { tenant });
    const tags = Array.from({ length: 33 }, (_, i) => `tag${i}`);
    await expect(
      vfs.writeFile("/a.txt", "x", { tags })
    ).rejects.toBeInstanceOf(EINVAL);
  });

  it("tag length > 128 → EINVAL", async () => {
    const tenant = "p12-tag-len";
    const vfs = createVFS(envFor(), { tenant });
    const tag = "a".repeat(129);
    await expect(
      vfs.writeFile("/a.txt", "x", { tags: [tag] })
    ).rejects.toBeInstanceOf(EINVAL);
  });

  it("tag with disallowed char → EINVAL", async () => {
    const tenant = "p12-tag-charset";
    const vfs = createVFS(envFor(), { tenant });
    await expect(
      vfs.writeFile("/a.txt", "x", { tags: ["bad space"] })
    ).rejects.toBeInstanceOf(EINVAL);
  });

  it("metadata depth > 10 → EINVAL", async () => {
    const tenant = "p12-meta-depth";
    const vfs = createVFS(envFor(), { tenant });
    let nested: Record<string, unknown> = {};
    let cur = nested;
    for (let i = 0; i < 15; i++) {
      cur.x = {};
      cur = cur.x as Record<string, unknown>;
    }
    await expect(
      vfs.writeFile("/a.txt", "x", { metadata: nested })
    ).rejects.toBeInstanceOf(EINVAL);
  });

  it("metadata root must be plain object (array → EINVAL)", async () => {
    const tenant = "p12-meta-root";
    const vfs = createVFS(envFor(), { tenant });
    await expect(
      vfs.writeFile("/a.txt", "x", {
        metadata: ["not", "an", "object"] as unknown as Record<string, unknown>,
      })
    ).rejects.toBeInstanceOf(EINVAL);
  });

  it("duplicate tag → EINVAL", async () => {
    const tenant = "p12-tag-dup";
    const vfs = createVFS(envFor(), { tenant });
    await expect(
      vfs.writeFile("/a.txt", "x", { tags: ["a", "a"] })
    ).rejects.toBeInstanceOf(EINVAL);
  });
});
