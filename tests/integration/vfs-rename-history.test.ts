import { describe, it, expect } from "vitest";
import { env, runInDurableObject } from "cloudflare:test";
import type { UserDO } from "@app/objects/user/user-do";
import { vfsUserDOName, vfsShardDOName } from "@core/lib/utils";
import { createVFS, type MossaicEnv } from "../../sdk/src/index";

/**
 * Phase 52 P1-2 regression — vfsRename overwrite under versioning ON
 * preserves the destination path's history.
 *
 * Pre-Phase-52 behaviour (Phase 35 audit P1-2): the rename-overwrite
 * path tombstoned the displaced file_id and renamed it to a synthetic
 * `<file_id>.tombstoned-<ts>` to free the unique-index slot, then
 * moved the source row into that slot. The displaced path's history
 * (rows under file_id `fA`) became orphaned:
 *   - listVersions(A) resolved to the new occupant, returning only
 *     its history.
 *   - The synthetic name was filtered from listFiles by the
 *     tombstone-head guard.
 *   - No public API took a file_id, so fA's a1/a2/etc. versions
 *     were unreachable except via the
 *     adminReapTombstonedHeads({mode:"walkBack"}) recovery
 *     primitive — operator-only.
 *
 * Phase 52 fix (mutations.ts:renameOverwriteVersioned): instead of
 * moving the source row INTO A's slot, we IMPORT source's content as
 * a NEW VERSION on A's history. After this:
 *   - A's path_id (= fA) stays put. Path A still resolves to fA.
 *   - fA's history grows: [a1, a2, …, b_imported_as_head].
 *     listVersions(A) returns the full chain.
 *   - readFile(A) returns the imported content.
 *   - Source row fB is reaped (ENOENT at B post-rename).
 *
 * Tests:
 *   RH1 — inline tier: 2 versions at A + 1 at B, rename(B→A) →
 *         listVersions(A) returns 3 entries, readFile(A) returns B's
 *         bytes, B is ENOENT, source files-row reaped.
 *   RH2 — chunked tier: same shape but with payloads >INLINE_LIMIT
 *         so chunks land on shards. Asserts ShardDO chunk_refs are
 *         correctly migrated (no orphan refs leaked).
 *   RH3 — restoreVersion of a pre-rename A version succeeds:
 *         vfs.readFile(A) after restore returns the historical bytes,
 *         confirming the prior versions' chunks survived the rename.
 *   RH4 — rename(B→A) where source has multi-version history: only
 *         B's HEAD content is migrated to A. B's older versions are
 *         dropped along with B's `files` row.
 */

interface E {
  MOSSAIC_USER: DurableObjectNamespace<UserDO>;
  MOSSAIC_SHARD: DurableObjectNamespace;
}
const E = env as unknown as E;

function envFor(): MossaicEnv {
  return {
    MOSSAIC_USER: E.MOSSAIC_USER as MossaicEnv["MOSSAIC_USER"],
    MOSSAIC_SHARD: E.MOSSAIC_SHARD as unknown as MossaicEnv["MOSSAIC_SHARD"],
  };
}

describe("Phase 52 P1-2 — vfsRename overwrite preserves dst history under versioning ON", () => {
  it("RH1 — inline tier: dst history preserved + src reaped + readFile sees src content", async () => {
    const tenant = "rh1-tenant";
    const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });

    await vfs.writeFile("/A.txt", new TextEncoder().encode("a1"));
    await vfs.writeFile("/A.txt", new TextEncoder().encode("a2"));
    await vfs.writeFile("/B.txt", new TextEncoder().encode("b1"));

    expect((await vfs.listVersions("/A.txt")).length).toBe(2);
    expect((await vfs.listVersions("/B.txt")).length).toBe(1);

    await vfs.rename("/B.txt", "/A.txt");

    // A's history grew from 2 to 3.
    const aHistory = await vfs.listVersions("/A.txt");
    expect(aHistory.length).toBe(3);
    // The most recent entry IS the imported B content (size 2,
    // not deleted).
    const head = aHistory[0]; // newest-first
    expect(head.deleted).toBe(false);
    expect(head.size).toBe(2);

    // readFile(A) returns the imported content.
    const aBytes = await vfs.readFile("/A.txt", { encoding: "utf8" });
    expect(aBytes).toBe("b1");

    // B is ENOENT.
    expect(await vfs.exists("/B.txt")).toBe(false);

    // SQL-level: only ONE `files` row remains for the tenant; the
    // source `files` row + its version_chunks were all dropped.
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName("default", tenant))
    );
    const sqlState = await runInDurableObject(stub, (_inst, state) => {
      const filesCount = (
        state.storage.sql
          .exec(
            "SELECT COUNT(*) AS n FROM files WHERE user_id = ? AND status = 'complete'",
            tenant
          )
          .toArray()[0] as { n: number }
      ).n;
      const versionCount = (
        state.storage.sql
          .exec(
            "SELECT COUNT(*) AS n FROM file_versions WHERE user_id = ?",
            tenant
          )
          .toArray()[0] as { n: number }
      ).n;
      return { filesCount, versionCount };
    });
    expect(sqlState.filesCount).toBe(1);
    expect(sqlState.versionCount).toBe(3);
  });

  it("RH2 — chunked tier: dst history preserved + ShardDO chunk_refs migrated cleanly", async () => {
    const tenant = "rh2-tenant";
    const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });

    // Use payloads >INLINE_LIMIT (16 KB) so they go chunked.
    const a1 = new Uint8Array(20 * 1024).fill(0xa1);
    const a2 = new Uint8Array(20 * 1024).fill(0xa2);
    const b1 = new Uint8Array(20 * 1024).fill(0xb1);

    await vfs.writeFile("/A.bin", a1);
    await vfs.writeFile("/A.bin", a2);
    await vfs.writeFile("/B.bin", b1);

    expect((await vfs.listVersions("/A.bin")).length).toBe(2);

    await vfs.rename("/B.bin", "/A.bin");

    // History: a1, a2, b_imported.
    const aHistory = await vfs.listVersions("/A.bin");
    expect(aHistory.length).toBe(3);

    // readFile(A) returns b1 bytes.
    const aBytes = await vfs.readFile("/A.bin");
    expect(aBytes.byteLength).toBe(b1.byteLength);
    expect(aBytes[0]).toBe(0xb1);
    expect(aBytes[aBytes.byteLength - 1]).toBe(0xb1);

    // B is gone.
    expect(await vfs.exists("/B.bin")).toBe(false);

    // ShardDO state: each unique chunk_hash should still have
    // chunk_refs that resolve to either a live version on the
    // dst path OR have been reaped (ref_count=0). Critically,
    // there must be NO chunk_refs whose file_id starts with the
    // (now-reaped) source file_id prefix.
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName("default", tenant))
    );
    const dstFileId = await runInDurableObject(stub, (_inst, state) => {
      const r = state.storage.sql
        .exec(
          "SELECT file_id FROM files WHERE user_id = ? AND file_name = 'A.bin'",
          tenant
        )
        .toArray()[0] as { file_id: string } | undefined;
      return r?.file_id ?? "";
    });
    expect(dstFileId).not.toBe("");

    // Probe each shard for orphan refs whose file_id matches the
    // OLD source prefix. We can't easily enumerate the source
    // file_id post-reap, so instead check that every
    // `chunk_refs.file_id` either:
    //   - starts with `${dstFileId}#` (the migrated refs), OR
    //   - matches an existing live version's shard_ref_id
    //     (multipart-finalized rows etc.).
    // Walk all 32 shards. Skip shards that never received any
    // writes (chunk_refs table absent).
    const userIdPrefix = tenant; // userIdFor for sub-less scope
    const dstPrefix = `${dstFileId}#`;
    let totalRefs = 0;
    let danglingRefs = 0;
    for (let s = 0; s < 32; s++) {
      const shardName = vfsShardDOName("default", tenant, undefined, s);
      const shardStub = E.MOSSAIC_SHARD.get(
        E.MOSSAIC_SHARD.idFromName(shardName)
      );
      try {
        const refs = await runInDurableObject(
          shardStub,
          (_inst, state) => {
            return state.storage.sql
              .exec(
                "SELECT chunk_hash, file_id FROM chunk_refs WHERE user_id = ?",
                userIdPrefix
              )
              .toArray() as { chunk_hash: string; file_id: string }[];
          }
        );
        for (const r of refs) {
          totalRefs++;
          if (!r.file_id.startsWith(dstPrefix)) {
            danglingRefs++;
          }
        }
      } catch {
        // Shard never initialised; no rows. Skip.
      }
    }
    expect(totalRefs).toBeGreaterThan(0);
    // Every surviving chunk_ref must be filed under the dst path's
    // versions. A non-zero count of refs pointing at neither dst
    // nor any current live shard_ref_id would mean the source's
    // chunk_refs leaked.
    expect(danglingRefs).toBe(0);
  });

  it("RH3 — restoreVersion of a pre-rename A-version succeeds", async () => {
    const tenant = "rh3-tenant";
    const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });

    await vfs.writeFile("/A.txt", new TextEncoder().encode("a1"));
    await vfs.writeFile("/A.txt", new TextEncoder().encode("a2"));
    await vfs.writeFile("/B.txt", new TextEncoder().encode("b_new_head"));

    const aHistoryBefore = await vfs.listVersions("/A.txt");
    expect(aHistoryBefore.length).toBe(2);
    // versions returned newest-first → [a2, a1]; pick a1 to restore.
    const a1Id = aHistoryBefore[1].id;

    await vfs.rename("/B.txt", "/A.txt");

    // Confirm a1 still appears in history post-rename.
    const aHistoryAfter = await vfs.listVersions("/A.txt");
    expect(aHistoryAfter.map((v) => v.id)).toContain(a1Id);

    // Restore a1: A's content reverts to "a1".
    await vfs.restoreVersion("/A.txt", a1Id);
    const restored = await vfs.readFile("/A.txt", { encoding: "utf8" });
    expect(restored).toBe("a1");
  });

  it("RH4 — multi-version src: only HEAD migrates; older src versions reaped", async () => {
    const tenant = "rh4-tenant";
    const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });

    await vfs.writeFile("/A.txt", new TextEncoder().encode("a1"));
    // B has 3 versions; only b3 (head) should migrate to A.
    await vfs.writeFile("/B.txt", new TextEncoder().encode("b1"));
    await vfs.writeFile("/B.txt", new TextEncoder().encode("b2"));
    await vfs.writeFile("/B.txt", new TextEncoder().encode("b3"));

    expect((await vfs.listVersions("/A.txt")).length).toBe(1);
    expect((await vfs.listVersions("/B.txt")).length).toBe(3);

    await vfs.rename("/B.txt", "/A.txt");

    // A has 1 + 1 = 2 versions (a1 + b3-imported). B's b1, b2 are
    // not migrated — they were the source's history, not part of
    // its head. They get reaped along with the source row.
    const aHistory = await vfs.listVersions("/A.txt");
    expect(aHistory.length).toBe(2);

    // readFile(A) returns b3 (the migrated head).
    const aBytes = await vfs.readFile("/A.txt", { encoding: "utf8" });
    expect(aBytes).toBe("b3");
  });
});
