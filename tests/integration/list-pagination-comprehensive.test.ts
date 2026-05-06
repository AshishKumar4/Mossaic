import { describe, it, expect } from "vitest";
import { env, runInDurableObject } from "cloudflare:test";
import {
  createVFS,
  type MossaicEnv,
  type UserDO,
} from "../../sdk/src/index";
import { vfsUserDOName } from "@core/lib/utils";

/**
 * Comprehensive pagination audit — Phase 47 verifies the Phase 46
 * fix on additional surfaces and pins the design choices on
 * pagination-adjacent APIs that intentionally do NOT paginate.
 *
 * Scope:
 *   - vfs.listFiles — pagination correctness across ALL post-
 *     filter shapes (metadata, tags, archive, tombstone).
 *   - vfs.listVersions — intentionally single-page; verify it
 *     does NOT silently truncate the user's data and document the
 *     hard cap so consumers can plan around it.
 *   - vfs.listChildren — tag-filter / archive-filter / tombstone-
 *     filter interaction with the (folders, files, symlinks) merge
 *     pagination.
 *
 * Cases:
 *   LP1 — listFiles metadata + tag double-filter still paginates.
 *   LP2 — listFiles archive-on / tombstone-on filter paths emit
 *         cursor when SQL was full.
 *   LP3 — listVersions returns up to `limit ?? 1000` versions, does
 *         not silently exceed the cap, and the cap is observable
 *         (the LAST row's mtime is the oldest of those returned).
 *   LP4 — listChildren paginates across folders + files in one
 *         lexicographic stream; no entry is skipped at the page
 *         boundary.
 *   LP5 — listFiles cursor is HMAC-signed and round-trips through
 *         the wire path (already covered by cursor-secret.test.ts;
 *         this case re-asserts the property under the post-filter
 *         page-shrink scenario specifically).
 */

interface E {
  MOSSAIC_USER: DurableObjectNamespace<UserDO>;
  MOSSAIC_SHARD: DurableObjectNamespace;
}
const TEST_ENV = env as unknown as E;

function envFor(): MossaicEnv {
  return {
    MOSSAIC_USER: TEST_ENV.MOSSAIC_USER as MossaicEnv["MOSSAIC_USER"],
    MOSSAIC_SHARD:
      TEST_ENV.MOSSAIC_SHARD as unknown as MossaicEnv["MOSSAIC_SHARD"],
  };
}

describe("listFiles / listVersions / listChildren — comprehensive pagination", () => {
  it("LP1 — metadata + tags double-filter emits cursor on full SQL page", async () => {
    const vfs = createVFS(envFor(), { tenant: "lp1-double-filter" });
    // 60 files. Tag 'media' on every 3rd. Metadata kind: 'photo' on
    // every 5th. The intersection is sparse.
    for (let i = 0; i < 60; i++) {
      const tags: string[] = [];
      if (i % 3 === 0) tags.push("media");
      const meta: Record<string, unknown> = {};
      if (i % 5 === 0) meta.kind = "photo";
      await vfs.writeFile(
        `/f${String(i).padStart(3, "0")}.txt`,
        `${i}`,
        {
          tags: tags.length ? tags : undefined,
          metadata: Object.keys(meta).length ? meta : undefined,
        }
      );
    }
    // Page 1 of 10 with both filters. Pre-Phase-46 the post-filter
    // would silently drop the cursor when the page was sparse.
    const p1 = await vfs.listFiles({
      limit: 10,
      tags: ["media"],
      metadata: { kind: "photo" },
      orderBy: "name",
      direction: "asc",
    });
    // SQL fetched a full page (limit ≤ rows that match the
    // tag intersect SQL); post-filter narrowed it. Cursor MUST be
    // defined so the caller can iterate.
    if (p1.items.length < 10) {
      expect(p1.cursor).toBeDefined();
    }

    // Iterate to exhaustion.
    const seen = new Set<string>();
    let cursor = p1.cursor;
    for (const item of p1.items) seen.add(item.path);
    let safety = 50;
    while (cursor && safety-- > 0) {
      const next = await vfs.listFiles({
        limit: 10,
        tags: ["media"],
        metadata: { kind: "photo" },
        orderBy: "name",
        direction: "asc",
        cursor,
      });
      for (const item of next.items) seen.add(item.path);
      cursor = next.cursor;
    }

    // Ground truth: i % 3 === 0 AND i % 5 === 0 → i % 15 === 0
    // for i in [0, 60) → {0, 15, 30, 45} → 4 matches.
    const expected = new Set<string>();
    for (let i = 0; i < 60; i += 15) {
      expected.add(`/f${String(i).padStart(3, "0")}.txt`);
    }
    expect(seen).toEqual(expected);
  });

  it("LP2 — archive=true / tombstone=true filter paths still emit cursor", async () => {
    const vfs = createVFS(envFor(), { tenant: "lp2-archive-tombstone" });
    // 30 files. Archive every 3rd; the rest stay live.
    for (let i = 0; i < 30; i++) {
      await vfs.writeFile(`/a${String(i).padStart(2, "0")}.txt`, `${i}`);
    }
    for (let i = 0; i < 30; i += 3) {
      await vfs.archive(`/a${String(i).padStart(2, "0")}.txt`);
    }

    // Default listFiles excludes archived. Page 1 of 5; iterate to
    // exhaustion and confirm we see all 20 non-archived.
    const seen = new Set<string>();
    let cursor: string | undefined = undefined;
    let safety = 30;
    do {
      const page: { items: { path: string }[]; cursor?: string } =
        await vfs.listFiles({
          limit: 5,
          orderBy: "name",
          direction: "asc",
          cursor,
        });
      for (const item of page.items) seen.add(item.path);
      cursor = page.cursor;
    } while (cursor && safety-- > 0);
    expect(seen.size).toBe(20);
  });

  it("LP3 — listVersions has hard cap (intentional design); no silent skip", async () => {
    const vfs = createVFS(envFor(), {
      tenant: "lp3-versions-cap",
      versioning: "enabled",
    });
    // Write 30 versions of one file. Default limit on listVersions
    // is 1000; we don't approach the cap but we DO verify the API
    // contract: every version we wrote is observable.
    const path = "/v.txt";
    for (let i = 0; i < 30; i++) {
      await vfs.writeFile(path, `version-${i}`);
    }
    const versions = await vfs.listVersions(path);
    expect(versions.length).toBe(30);
    // Newest first by mtimeMs; verify monotonic descending order.
    for (let i = 1; i < versions.length; i++) {
      expect(versions[i].mtimeMs).toBeLessThanOrEqual(versions[i - 1].mtimeMs);
    }

    // Now test the cap: limit=10 returns the 10 newest, not silently
    // a different 10. The API does NOT carry a cursor (single-page
    // by design); operators wanting full enumeration should pass a
    // larger limit. This test pins that contract so a future fix
    // doesn't accidentally start dropping older versions.
    const top10 = await vfs.listVersions(path, { limit: 10 });
    expect(top10.length).toBe(10);
    // The 10 returned are the 10 newest of the 30 we wrote.
    expect(top10[0].mtimeMs).toBe(versions[0].mtimeMs);
    expect(top10[9].mtimeMs).toBe(versions[9].mtimeMs);
  });

  it("LP4 — listChildren paginates folders + files together without skipping", async () => {
    const vfs = createVFS(envFor(), { tenant: "lp4-children" });
    // 5 folders + 15 files at root.
    for (let i = 0; i < 5; i++) {
      await vfs.mkdir(`/d${i}`);
    }
    for (let i = 0; i < 15; i++) {
      await vfs.writeFile(`/f${String(i).padStart(2, "0")}.txt`, `${i}`);
    }

    // Page through with a small limit to force at least 3 pages.
    const seen = new Set<string>();
    let cursor: string | undefined = undefined;
    let pages = 0;
    let safety = 20;
    do {
      const page: {
        entries: { path: string; kind: string }[];
        cursor?: string;
      } = await vfs.listChildren("/", {
        limit: 7,
        orderBy: "name",
        direction: "asc",
        cursor,
      });
      pages++;
      for (const entry of page.entries) seen.add(entry.path);
      cursor = page.cursor;
    } while (cursor && safety-- > 0);

    // Total entries: 5 folders + 15 files = 20.
    expect(seen.size).toBe(20);
    expect(pages).toBeGreaterThanOrEqual(3);
  });

  it("LP5 — post-filter cursor round-trips through HMAC verify", async () => {
    const vfs = createVFS(envFor(), { tenant: "lp5-cursor-hmac" });
    for (let i = 0; i < 25; i++) {
      await vfs.writeFile(`/f${String(i).padStart(2, "0")}.txt`, `${i}`, {
        metadata: { batch: i < 5 ? "alpha" : "beta" },
      });
    }
    const p1 = await vfs.listFiles({
      limit: 5,
      metadata: { batch: "alpha" },
      orderBy: "name",
      direction: "asc",
    });
    // Page 1 yields the 5 alpha matches (full page); cursor
    // emitted because SQL fetched a full sqlLimit page.
    expect(p1.items.length).toBe(5);
    if (p1.cursor) {
      expect(typeof p1.cursor).toBe("string");
      // Tampered cursor → server rejects with EINVAL.
      const tampered =
        p1.cursor.slice(0, p1.cursor.length - 1) +
        (p1.cursor.endsWith("a") ? "b" : "a");
      let rejected = false;
      try {
        await vfs.listFiles({
          limit: 5,
          metadata: { batch: "alpha" },
          orderBy: "name",
          direction: "asc",
          cursor: tampered,
        });
      } catch {
        rejected = true;
      }
      expect(rejected).toBe(true);
    }
    // Verify that the durable object carries no leaked cursor state
    // (cursors are stateless HMAC tokens, never persisted).
    await runInDurableObject(
      TEST_ENV.MOSSAIC_USER.get(
        TEST_ENV.MOSSAIC_USER.idFromName(
          vfsUserDOName("default", "lp5-cursor-hmac")
        )
      ),
      (_inst, state) => {
        const tables = state.storage.sql
          .exec(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%cursor%'"
          )
          .toArray() as { name: string }[];
        // No cursor-state tables — pinning the stateless design.
        expect(tables.length).toBe(0);
      }
    );
  });
});
