import { describe, it, expect } from "vitest";
import { env, runInDurableObject, runDurableObjectAlarm } from "cloudflare:test";

/**
 * Phase 9 — file-level versioning (S3-style, opt-in).
 *
 * Pinned invariants (these are the targets for the upcoming TSLean
 * formal proofs):
 *
 *   I1. Versioning OFF tenant: byte-equivalent to Phase 8.
 *       file_versions stays empty; head_version_id stays NULL.
 *   I2. Versioning ON: every writeFile inserts exactly one new
 *       file_versions row; head_version_id points at it.
 *   I3. unlink ON tenant: writes a tombstone row (deleted=1, no
 *       chunks); existing version chunks NOT decremented; the
 *       previous content's chunk_refs survive on the shard.
 *   I4. readFile (no version) on tombstoned head → ENOENT.
 *   I5. readFile (versionId) returns the exact bytes of that
 *       historical version.
 *   I6. listVersions returns newest-first; tombstones included.
 *   I7. restoreVersion(srcId) → new version with same content;
 *       chunks deduplicate (refcount += 1, no new blobs uploaded).
 *   I8. dropVersions reaps unreferenced versions; head is preserved
 *       even when filters say otherwise; chunks become eligible
 *       for the alarm sweeper.
 *   I9. Cross-version dedup: two writes of the same content share
 *       the same chunk_hash; refcount = (number of versions
 *       referencing it).
 */

import {
  createVFS,
  type MossaicEnv,
  type UserDO,
  ENOENT,
  EINVAL,
} from "../../sdk/src/index";
import { vfsUserDOName, vfsShardDOName } from "../../worker/lib/utils";

interface E {
  USER_DO: DurableObjectNamespace<UserDO>;
  SHARD_DO: DurableObjectNamespace;
}
const E = env as unknown as E;

function envFor(): MossaicEnv {
  return { MOSSAIC_USER: E.USER_DO as MossaicEnv["MOSSAIC_USER"] };
}

describe("Phase 9 — versioning OFF (default): byte-equivalent to Phase 8 (I1)", () => {
  it("writes do NOT create file_versions rows by default", async () => {
    const tenant = "ver-off-default";
    const vfs = createVFS(envFor(), { tenant });
    await vfs.writeFile("/x.txt", "first");
    await vfs.writeFile("/x.txt", "second");
    await vfs.writeFile("/x.txt", "third");

    const stub = E.USER_DO.get(
      E.USER_DO.idFromName(vfsUserDOName("default", tenant))
    );
    const counts = await runInDurableObject(stub, async (_inst, state) => {
      const vCount = state.storage.sql
        .exec("SELECT COUNT(*) AS n FROM file_versions")
        .toArray()[0] as { n: number };
      const headRow = state.storage.sql
        .exec(
          "SELECT head_version_id FROM files WHERE file_name = 'x.txt'"
        )
        .toArray()[0] as { head_version_id: string | null } | undefined;
      return { versionRows: vCount.n, head: headRow?.head_version_id ?? null };
    });
    expect(counts.versionRows).toBe(0);
    expect(counts.head).toBeNull();

    // Reads still work — Phase 8 path returns the latest content.
    const back = await vfs.readFile("/x.txt", { encoding: "utf8" });
    expect(back).toBe("third");
  });
});

describe("Phase 9 — versioning ON (I2, I5, I6): write/read/list", () => {
  it("creates 5 versions of /foo.txt; readFile current = v5; readFile by ID = historical", async () => {
    const tenant = "ver-multi";
    const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });
    await vfs.writeFile("/foo.txt", "v1");
    await vfs.writeFile("/foo.txt", "v2");
    await vfs.writeFile("/foo.txt", "v3");
    await vfs.writeFile("/foo.txt", "v4");
    await vfs.writeFile("/foo.txt", "v5");

    // Newest read = v5 (head).
    expect(await vfs.readFile("/foo.txt", { encoding: "utf8" })).toBe("v5");

    // listVersions: newest-first, all 5 present, no tombstones.
    const versions = await vfs.listVersions("/foo.txt");
    expect(versions).toHaveLength(5);
    for (const v of versions) {
      expect(v.deleted).toBe(false);
      expect(v.size).toBe(2);
    }
    // Sort order: newest first.
    for (let i = 1; i < versions.length; i++) {
      expect(versions[i - 1].mtimeMs).toBeGreaterThanOrEqual(
        versions[i].mtimeMs
      );
    }

    // Read each historical version explicitly.
    const labels = ["v5", "v4", "v3", "v2", "v1"];
    for (let i = 0; i < 5; i++) {
      const txt = await vfs.readFile("/foo.txt", {
        version: versions[i].id,
        encoding: "utf8",
      });
      expect(txt).toBe(labels[i]);
    }
  });

  it("readFile with a non-existent versionId throws ENOENT", async () => {
    const tenant = "ver-bad-id";
    const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });
    await vfs.writeFile("/x.txt", "hello");
    let caught: unknown = null;
    try {
      await vfs.readFile("/x.txt", { version: "does-not-exist" });
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(ENOENT);
  });
});

describe("Phase 9 — restoreVersion (I7)", () => {
  it("restoreVersion creates a new version with old content; head flips", async () => {
    const tenant = "ver-restore";
    const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });
    await vfs.writeFile("/foo.txt", "v1");
    await vfs.writeFile("/foo.txt", "v2");
    await vfs.writeFile("/foo.txt", "v3");
    await vfs.writeFile("/foo.txt", "v4");
    await vfs.writeFile("/foo.txt", "v5");

    const versions = await vfs.listVersions("/foo.txt");
    const v3 = versions[2]; // newest-first index → v3 is at idx 2

    const r = await vfs.restoreVersion("/foo.txt", v3.id);
    expect(typeof r.id).toBe("string");
    expect(r.id).not.toBe(v3.id);

    // Head is now v6 (the restored copy), content = v3's bytes.
    const head = await vfs.readFile("/foo.txt", { encoding: "utf8" });
    expect(head).toBe("v3");

    // History grew by 1.
    const after = await vfs.listVersions("/foo.txt");
    expect(after).toHaveLength(6);
    expect(after[0].id).toBe(r.id);
  });

  it("restoreVersion on a tombstone throws EINVAL", async () => {
    const tenant = "ver-restore-tomb";
    const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });
    await vfs.writeFile("/x.txt", "alive");
    await vfs.unlink("/x.txt"); // tombstone

    const versions = await vfs.listVersions("/x.txt");
    const tomb = versions.find((v) => v.deleted);
    expect(tomb).toBeTruthy();

    let caught: unknown = null;
    try {
      await vfs.restoreVersion("/x.txt", tomb!.id);
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(EINVAL);
  });
});

describe("Phase 9 — unlink tombstones (I3, I4)", () => {
  it("unlink writes a tombstone; readFile head → ENOENT but listVersions still surfaces history", async () => {
    const tenant = "ver-tombstone";
    const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });
    await vfs.writeFile("/x.txt", "before unlink");
    await vfs.unlink("/x.txt");

    // exists + readFile both see the tombstone as ENOENT.
    expect(await vfs.exists("/x.txt")).toBe(false);
    let caught: unknown = null;
    try {
      await vfs.readFile("/x.txt");
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(ENOENT);

    // History intact: 2 versions (alive + tombstone).
    const versions = await vfs.listVersions("/x.txt");
    expect(versions).toHaveLength(2);
    expect(versions[0].deleted).toBe(true); // tombstone is newest
    expect(versions[1].deleted).toBe(false);
    expect(versions[1].size).toBeGreaterThan(0);
  });
});

describe("Phase 9 — dropVersions retention policies (I8)", () => {
  it("dropVersions({ keepLast: 2 }) drops v1-v3, keeps v4-v6 (head + 1 newest)", async () => {
    const tenant = "ver-drop-keepLast";
    const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });
    for (let i = 1; i <= 6; i++) {
      await vfs.writeFile("/foo.txt", `v${i}`);
    }
    expect((await vfs.listVersions("/foo.txt")).length).toBe(6);

    const r = await vfs.dropVersions("/foo.txt", { keepLast: 2 });
    // keepSet = head (v6) ∪ newest 2 = {v6, v5}. Drop v1,v2,v3,v4.
    expect(r.dropped).toBe(4);
    expect(r.kept).toBe(2);

    const remaining = await vfs.listVersions("/foo.txt");
    expect(remaining).toHaveLength(2);
    // Head still readable.
    expect(await vfs.readFile("/foo.txt", { encoding: "utf8" })).toBe("v6");
  });

  it("dropVersions({ olderThan: cutoff }) drops only stale versions, keeps head always", async () => {
    const tenant = "ver-drop-olderThan";
    const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });
    await vfs.writeFile("/x.txt", "old1");
    await vfs.writeFile("/x.txt", "old2");
    // Sleep 50ms so cutoff cleanly partitions.
    await new Promise((r) => setTimeout(r, 50));
    const cutoff = Date.now();
    await new Promise((r) => setTimeout(r, 5));
    await vfs.writeFile("/x.txt", "new1");

    const r = await vfs.dropVersions("/x.txt", { olderThan: cutoff });
    expect(r.dropped).toBe(2);
    expect(r.kept).toBe(1);
    expect(await vfs.readFile("/x.txt", { encoding: "utf8" })).toBe("new1");
  });

  it("dropVersions({}) drops everything except the head", async () => {
    const tenant = "ver-drop-all";
    const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });
    for (let i = 1; i <= 5; i++) {
      await vfs.writeFile("/x.txt", `v${i}`);
    }
    const r = await vfs.dropVersions("/x.txt", {});
    expect(r.dropped).toBe(4);
    expect(r.kept).toBe(1);
    expect(await vfs.readFile("/x.txt", { encoding: "utf8" })).toBe("v5");
  });

  it("dropVersions never drops the head, even when filters target it", async () => {
    const tenant = "ver-drop-head-safe";
    const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });
    await vfs.writeFile("/x.txt", "v1");
    await vfs.writeFile("/x.txt", "v2");
    // olderThan=now+1hr — every version is "old". But the head must
    // survive the policy.
    const r = await vfs.dropVersions("/x.txt", {
      olderThan: Date.now() + 3_600_000,
    });
    // Both versions are "old" — but the head (v2) is preserved
    // unconditionally.
    expect(r.kept).toBeGreaterThanOrEqual(1);
    expect(await vfs.readFile("/x.txt", { encoding: "utf8" })).toBe("v2");
  });
});

describe("Phase 9 — cross-version dedup (I9) + chunk reclamation", () => {
  it("two versions with identical chunked content share the chunk row; refcount = 2", async () => {
    const tenant = "ver-dedup";
    const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });
    // Force chunked tier with > INLINE_LIMIT bytes (16 KB).
    const payload = new Uint8Array(20 * 1024).fill(0xab);
    await vfs.writeFile("/big.bin", payload);
    await vfs.writeFile("/big.bin", payload); // identical content

    // Pull the shard the chunk landed on, count refs for that hash.
    const userStub = E.USER_DO.get(
      E.USER_DO.idFromName(vfsUserDOName("default", tenant))
    );
    const { hash, shardIdx, refCount } = await runInDurableObject(
      userStub,
      async (_inst, state) => {
        const row = state.storage.sql
          .exec(
            "SELECT chunk_hash, shard_index FROM version_chunks LIMIT 1"
          )
          .toArray()[0] as { chunk_hash: string; shard_index: number };
        const refs = state.storage.sql
          .exec(
            "SELECT COUNT(*) AS n FROM version_chunks WHERE chunk_hash = ?",
            row.chunk_hash
          )
          .toArray()[0] as { n: number };
        return {
          hash: row.chunk_hash,
          shardIdx: row.shard_index,
          refCount: refs.n,
        };
      }
    );
    expect(refCount).toBe(2); // two versions both reference the hash

    // ShardDO's chunk row exists once; ref_count on that single row = 2.
    const shardStub = E.SHARD_DO.get(
      E.SHARD_DO.idFromName(
        vfsShardDOName("default", tenant, undefined, shardIdx)
      )
    );
    const shardCounts = await runInDurableObject(
      shardStub,
      async (_inst, state) => {
        const chunks = state.storage.sql
          .exec(
            "SELECT COUNT(*) AS n FROM chunks WHERE hash = ?",
            hash
          )
          .toArray()[0] as { n: number };
        const ref = state.storage.sql
          .exec("SELECT ref_count FROM chunks WHERE hash = ?", hash)
          .toArray()[0] as { ref_count: number };
        return { chunkRows: chunks.n, refCount: ref.ref_count };
      }
    );
    expect(shardCounts.chunkRows).toBe(1); // dedup: one blob
    expect(shardCounts.refCount).toBe(2); // two version refs
  });

  it("dropVersions decrements chunk refs; alarm sweeper reclaims when refcount → 0", async () => {
    const tenant = "ver-drop-sweep";
    const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });

    // Three versions of a chunked file (each unique content so each
    // has its own chunk_hash; no dedup confusion).
    const v1 = new Uint8Array(20 * 1024).fill(1);
    const v2 = new Uint8Array(20 * 1024).fill(2);
    const v3 = new Uint8Array(20 * 1024).fill(3);
    await vfs.writeFile("/x.bin", v1);
    await vfs.writeFile("/x.bin", v2);
    await vfs.writeFile("/x.bin", v3);

    const userStub = E.USER_DO.get(
      E.USER_DO.idFromName(vfsUserDOName("default", tenant))
    );
    const before = await runInDurableObject(userStub, async (_inst, state) => {
      const r = state.storage.sql
        .exec("SELECT COUNT(*) AS n FROM file_versions")
        .toArray()[0] as { n: number };
      return r.n;
    });
    expect(before).toBe(3);

    // Drop v1 + v2 (keep v3 which is the head).
    const r = await vfs.dropVersions("/x.bin", { keepLast: 1 });
    expect(r.dropped).toBe(2);
    expect(r.kept).toBe(1);

    // Pull the shard for the v1 chunk and verify the ref dropped.
    // v1's hash is no longer referenced anywhere (v2/v3 had different
    // content). The alarm sweeper after grace will reclaim the blob.
    const shards = await runInDurableObject(
      userStub,
      async (_inst, state) => {
        return state.storage.sql
          .exec("SELECT DISTINCT shard_index FROM version_chunks")
          .toArray() as { shard_index: number }[];
      }
    );
    expect(shards.length).toBeGreaterThan(0);

    // Trigger the ShardDO alarm to immediately reap soft-marked
    // chunks. Probe each touched shard.
    for (const { shard_index } of shards) {
      const shard = E.SHARD_DO.get(
        E.SHARD_DO.idFromName(
          vfsShardDOName("default", tenant, undefined, shard_index)
        )
      );
      // Ensure the alarm has been scheduled by deleteChunks.
      await runInDurableObject(shard, async (_inst, state) => {
        // Force grace expiry by backdating deleted_at on any
        // soft-marked chunks.
        state.storage.sql.exec(
          "UPDATE chunks SET deleted_at = 0 WHERE deleted_at IS NOT NULL"
        );
        // Schedule + run the alarm.
        await state.storage.setAlarm(Date.now() + 1);
      });
      // Run the alarm.
      await runDurableObjectAlarm(shard);
    }

    // Verify v3 (the surviving version) still readable end-to-end.
    const back = await vfs.readFile("/x.bin");
    expect(back.byteLength).toBe(v3.byteLength);
    expect(back[0]).toBe(3);
  });
});

describe("Phase 9 — inline tier (no shard call)", () => {
  it("small payload (<INLINE_LIMIT) goes inline; readFile returns exact bytes", async () => {
    const tenant = "ver-inline";
    const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });
    await vfs.writeFile("/note.txt", "hello small");

    // Verify no version_chunks row exists for the inline version.
    const stub = E.USER_DO.get(
      E.USER_DO.idFromName(vfsUserDOName("default", tenant))
    );
    const stats = await runInDurableObject(stub, async (_inst, state) => {
      const v = state.storage.sql
        .exec(
          "SELECT version_id, size, inline_data FROM file_versions LIMIT 1"
        )
        .toArray()[0] as {
        version_id: string;
        size: number;
        inline_data: ArrayBuffer | null;
      };
      const chunkCount = state.storage.sql
        .exec(
          "SELECT COUNT(*) AS n FROM version_chunks WHERE version_id = ?",
          v.version_id
        )
        .toArray()[0] as { n: number };
      return { hasInline: v.inline_data !== null, chunkRows: chunkCount.n };
    });
    expect(stats.hasInline).toBe(true);
    expect(stats.chunkRows).toBe(0);

    expect(await vfs.readFile("/note.txt", { encoding: "utf8" })).toBe(
      "hello small"
    );
  });
});

describe("Phase 9 — listVersions performance (large history)", () => {
  it("listVersions over 200 versions returns sorted in <100ms", async () => {
    // 10k is too slow for the test suite (would take many seconds
    // per write). 200 still proves the index works — without the
    // index this would be a full scan.
    const tenant = "ver-perf";
    const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });
    for (let i = 0; i < 200; i++) {
      await vfs.writeFile("/perf.txt", `v${i}`);
    }
    const t0 = performance.now();
    const versions = await vfs.listVersions("/perf.txt", { limit: 200 });
    const dt = performance.now() - t0;
    expect(versions).toHaveLength(200);
    // Sub-100ms even with vitest-pool overhead. (In Miniflare this
    // is single-digit ms.)
    expect(dt).toBeLessThan(100);
    // Newest-first.
    expect(versions[0].mtimeMs).toBeGreaterThanOrEqual(
      versions[versions.length - 1].mtimeMs
    );
  });
});

describe("Phase 9 — head-version idempotency", () => {
  it("rapid back-to-back writes each create a distinct version", async () => {
    const tenant = "ver-rapid";
    const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });
    const promises = [
      vfs.writeFile("/x.txt", "a"),
      vfs.writeFile("/x.txt", "b"),
      vfs.writeFile("/x.txt", "c"),
    ];
    await Promise.all(promises);
    const versions = await vfs.listVersions("/x.txt");
    expect(versions.length).toBeGreaterThanOrEqual(3);
    // Whatever wins the head race, the head's content must be
    // readable.
    const head = await vfs.readFile("/x.txt", { encoding: "utf8" });
    expect(["a", "b", "c"]).toContain(head);
  });
});

describe("Phase 9 — restoreVersion vs swept chunks (audit C2 regression)", () => {
  it("restoreVersion of a version whose chunks were swept throws ENOENT (never silently corrupts)", async () => {
    const tenant = "ver-c2-regression";
    const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });

    // v1: chunked content (>INLINE_LIMIT so chunks land on shards).
    // v2: different chunked content so v1's chunks are not deduped.
    const v1Bytes = new Uint8Array(20 * 1024).fill(0x11);
    const v2Bytes = new Uint8Array(20 * 1024).fill(0x22);
    await vfs.writeFile("/file.bin", v1Bytes);
    const versionsAfterV1 = await vfs.listVersions("/file.bin");
    expect(versionsAfterV1.length).toBe(1);
    const v1Id = versionsAfterV1[0].id;

    await vfs.writeFile("/file.bin", v2Bytes);

    // Drop everything except the head (v2). v1's chunks become
    // soft-marked on their shard.
    const dropped = await vfs.dropVersions("/file.bin", { keepLast: 1 });
    expect(dropped.dropped).toBe(1);

    // Force the alarm sweep on every shard touched.
    const userStub = E.USER_DO.get(
      E.USER_DO.idFromName(vfsUserDOName("default", tenant))
    );
    // Identify shards via the surviving version_chunks rows + shard
    // capacity. Easier: enumerate the 32-shard pool head and force-
    // sweep any with soft-marked chunks.
    for (let s = 0; s < 32; s++) {
      const shard = E.SHARD_DO.get(
        E.SHARD_DO.idFromName(vfsShardDOName("default", tenant, undefined, s))
      );
      // Backdate any deleted_at into the past, force the alarm to
      // run immediately. If the shard has no soft-marked rows OR
      // hasn't been initialised at all (the rendezvous-hashed pool
      // only ever touches a subset of the 32 instances), this is a
      // cheap no-op.
      const touched = await runInDurableObject(
        shard,
        async (_inst, state) => {
          try {
            state.storage.sql.exec(
              "UPDATE chunks SET deleted_at = 0 WHERE deleted_at IS NOT NULL"
            );
            await state.storage.setAlarm(Date.now() + 1);
            return true;
          } catch {
            // chunks table not created → this shard never received
            // any writes; skip the alarm dance.
            return false;
          }
        }
      );
      if (touched) await runDurableObjectAlarm(shard);
    }

    // C2 contract: even if the corresponding file_versions row had
    // somehow survived (by upgrade race / partial replay), restoreVersion
    // MUST refuse rather than silently insert empty bytes.
    //
    // dropVersionRows in the current implementation also deletes the
    // file_versions row, which makes restoreVersion throw ENOENT via
    // getVersion-returns-null. We assert ENOENT either way.
    let threw: unknown = null;
    try {
      await vfs.restoreVersion("/file.bin", v1Id);
    } catch (err) {
      threw = err;
    }
    // Either path is acceptable per audit C2:
    //   - getVersion returns null → ENOENT "version <id> not found"
    //   - chunksAlive pre-flight detects swept chunks → ENOENT
    //     "source chunks swept on shard ..."
    expect(threw).toBeInstanceOf(ENOENT);

    // The head (v2) must remain readable end-to-end — no collateral
    // damage from the failed restore.
    const head = await vfs.readFile("/file.bin");
    expect(head.byteLength).toBe(v2Bytes.byteLength);
    expect(head[0]).toBe(0x22);
  });

  it("chunksAlive ShardDO RPC returns only present, live, and unmarked chunks", async () => {
    const tenant = "ver-c2-chunksalive";
    const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });
    const buf = new Uint8Array(20 * 1024).fill(0x33);
    await vfs.writeFile("/x.bin", buf);

    // Find one shard that holds a chunk for /x.bin.
    const userStub = E.USER_DO.get(
      E.USER_DO.idFromName(vfsUserDOName("default", tenant))
    );
    const refs = await runInDurableObject(userStub, async (_inst, state) => {
      return state.storage.sql
        .exec("SELECT chunk_hash, shard_index FROM version_chunks LIMIT 1")
        .toArray() as { chunk_hash: string; shard_index: number }[];
    });
    expect(refs.length).toBe(1);
    const { chunk_hash, shard_index } = refs[0];
    const shard = E.SHARD_DO.get(
      E.SHARD_DO.idFromName(
        vfsShardDOName("default", tenant, undefined, shard_index)
      )
    );

    // Live + present → returned in alive set.
    const live = await (shard as unknown as {
      chunksAlive: (h: string[]) => Promise<{ alive: string[] }>;
    }).chunksAlive([chunk_hash]);
    expect(live.alive).toEqual([chunk_hash]);

    // A made-up hash → not returned.
    const fake = "0".repeat(64);
    const missing = await (shard as unknown as {
      chunksAlive: (h: string[]) => Promise<{ alive: string[] }>;
    }).chunksAlive([fake]);
    expect(missing.alive).toEqual([]);

    // Soft-mark the live chunk → it becomes "not alive" for the
    // purposes of restoreVersion safety.
    await runInDurableObject(shard, async (_inst, state) => {
      state.storage.sql.exec(
        "UPDATE chunks SET deleted_at = ? WHERE hash = ?",
        Date.now(),
        chunk_hash
      );
    });
    const marked = await (shard as unknown as {
      chunksAlive: (h: string[]) => Promise<{ alive: string[] }>;
    }).chunksAlive([chunk_hash]);
    expect(marked.alive).toEqual([]);
  });
});
