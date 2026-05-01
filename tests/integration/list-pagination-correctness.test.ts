import { describe, it, expect } from "vitest";
import { env } from "cloudflare:test";

/**
 * Phase 46 — `vfs.listFiles` pagination-correctness regression
 * coverage.
 *
 * Pre-Phase-46 bug: when a metadata or tag post-filter shrunk the
 * page below `limit`, `listFiles` emitted no cursor — even though
 * the underlying SQL had returned a full page and more matches
 * lived past the boundary. Callers iterating to enumerate the
 * dataset stopped early.
 *
 * Phase 46 fix: emit cursor whenever SQL fetched a full page,
 * anchored at the LAST row that came back from SQL (not the last
 * surviving item). Strict-monotonic boundary on (orderValue,
 * file_id) means the next page resumes past every row SQL has
 * already considered, so no row is unreachable.
 *
 * Pinned invariants:
 *   PC1.  Metadata-filter short page: cursor present when SQL was
 *         full (matches < limit but candidates filled sqlLimit).
 *   PC2.  Metadata-filter complete enumeration: paginating to
 *         exhaustion finds every match — the bug regression test.
 *   PC3.  Tag-filter short page: same fix applies (tag intersect
 *         is also a post-filter step).
 *   PC4.  No metadata, full SQL page: still emits cursor (existing
 *         behaviour preserved).
 *   PC5.  Empty result with no metadata filter: no cursor.
 */

import {
  createVFS,
  type MossaicEnv,
  type UserDO,
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

describe("listFiles pagination correctness — Phase 46 fix (PC1..PC5)", () => {
  it("PC1 — metadata filter short-page emits cursor when SQL was full", async () => {
    const vfs = createVFS(envFor(), { tenant: "p46-pc-meta-short" });
    // 50 files; only every 10th carries `kind: 'special'`. With
    // limit=10 the SQL returns 10 rows, post-filter narrows to ~1,
    // and the rest of the matches live past the boundary.
    for (let i = 0; i < 50; i++) {
      await vfs.writeFile(`/f${String(i).padStart(3, "0")}.txt`, `${i}`, {
        metadata: i % 10 === 0 ? { kind: "special" } : { kind: "normal" },
      });
    }
    const p1 = await vfs.listFiles({
      limit: 10,
      metadata: { kind: "special" },
      orderBy: "name",
      direction: "asc",
    });
    // Pre-fix: cursor would be undefined and the user thinks
    // there are only ~1 matches.
    expect(p1.cursor).toBeDefined();
  });

  it("PC2 — metadata-filter complete enumeration finds every match", async () => {
    const vfs = createVFS(envFor(), { tenant: "p46-pc-meta-full" });
    const SPECIAL_COUNT = 5;
    const TOTAL = 50;
    for (let i = 0; i < TOTAL; i++) {
      await vfs.writeFile(`/f${String(i).padStart(3, "0")}.txt`, `${i}`, {
        metadata: i % 10 === 0 ? { kind: "special" } : { kind: "normal" },
      });
    }
    // Iterate with a small limit so the bug would manifest.
    const collected: string[] = [];
    let cursor: string | undefined;
    let safety = 20; // bound iterations to avoid runaway in regression
    do {
      const r = await vfs.listFiles({
        limit: 7,
        metadata: { kind: "special" },
        orderBy: "name",
        direction: "asc",
        cursor,
      });
      for (const it of r.items) collected.push(it.path);
      cursor = r.cursor;
    } while (cursor && --safety > 0);
    expect(collected.length).toBe(SPECIAL_COUNT);
    expect(safety).toBeGreaterThan(0); // termination not by safety bound
  });

  it("PC3 — tag-filter short page emits cursor when SQL was full", async () => {
    const vfs = createVFS(envFor(), { tenant: "p46-pc-tag" });
    for (let i = 0; i < 30; i++) {
      await vfs.writeFile(`/f${String(i).padStart(3, "0")}.txt`, `${i}`, {
        tags: i % 8 === 0 ? ["red", "rare"] : ["blue"],
      });
    }
    // Tag intersection AND of "red" + "rare" matches ~4 rows.
    // With limit=5 the SQL over-fetches; the post-intersect page
    // can be short. Emit cursor anyway when SQL was full.
    const p1 = await vfs.listFiles({
      limit: 5,
      tags: ["red", "rare"],
    });
    // 4 matches total — page may either be complete (length<limit)
    // or boundary case. Verify no rows are unreachable: paginate
    // to exhaustion, count matches.
    const collected: string[] = [];
    for (const it of p1.items) collected.push(it.path);
    let cursor = p1.cursor;
    let safety = 20;
    while (cursor && --safety > 0) {
      const r = await vfs.listFiles({
        limit: 5,
        tags: ["red", "rare"],
        cursor,
      });
      for (const it of r.items) collected.push(it.path);
      cursor = r.cursor;
    }
    expect(collected.length).toBe(4);
  });

  it("PC4 — no metadata, full SQL page still emits cursor", async () => {
    const vfs = createVFS(envFor(), { tenant: "p46-pc-plain" });
    for (let i = 0; i < 20; i++) {
      await vfs.writeFile(`/f${String(i).padStart(3, "0")}.txt`, `${i}`);
    }
    const r = await vfs.listFiles({ limit: 10, orderBy: "name", direction: "asc" });
    expect(r.items.length).toBe(10);
    expect(r.cursor).toBeDefined();
  });

  it("PC5 — empty result with no filter: no cursor", async () => {
    const vfs = createVFS(envFor(), { tenant: "p46-pc-empty" });
    const r = await vfs.listFiles({ limit: 10 });
    expect(r.items).toEqual([]);
    expect(r.cursor).toBeUndefined();
  });
});
