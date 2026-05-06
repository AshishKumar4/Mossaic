import { describe, it, expect } from "vitest";
import {
  env,
  runInDurableObject,
  runDurableObjectAlarm,
} from "cloudflare:test";

/**
 * Phase 3 — Write-side VFS RPC integration tests.
 *
 * Drives the write-side typed RPC surface (vfsWriteFile, vfsUnlink,
 * vfsMkdir, vfsRmdir, vfsRename, vfsChmod, vfsSymlink,
 * vfsRemoveRecursive) and the chunk-GC machinery (ShardDO.deleteChunks +
 * alarm-driven hard-delete) directly through DO RPC. Mirrors the test
 * pattern from vfs-read.test.ts.
 *
 * Coverage (sdk-impl-plan §7, §8):
 *   - writeFile atomicity: temp-id-then-rename, no torn manifest visible
 *   - inline tier vs chunked tier branching
 *   - refcount decrement on unlink / overwrite / rename-replace
 *   - ShardDO alarm sweeper: refcount→0 marks deleted_at, alarm fires,
 *     blob hard-deleted, capacity reconciled
 *   - integration: writeFile → readFile → unlink → readFile(ENOENT) → GC
 *   - concurrent writeFile to same path resolves via UNIQUE
 *   - rename-replace decrements old, new path inherits new bytes
 *   - removeRecursive only orphans GC'd; shared chunks survive
 */

import type { UserDO } from "../../worker/objects/user/user-do";
import type { ShardDO } from "../../worker/objects/shard/shard-do";
import { INLINE_LIMIT } from "@shared/inline";

interface E {
  USER_DO: DurableObjectNamespace<UserDO>;
  SHARD_DO: DurableObjectNamespace<ShardDO>;
}
const E = env as unknown as E;

// ── Helpers ────────────────────────────────────────────────────────────

/**
 * Seed a UserDO via the legacy /signup route to materialize the quota
 * row + the user_id we'll use as scope.tenant. Identical pattern to
 * vfs-read.test.ts.
 */
async function seedUser(
  stub: DurableObjectStub<UserDO>,
  email: string
): Promise<string> {
  const sup = await stub.fetch(
    new Request("http://internal/signup", {
      method: "POST",
      body: JSON.stringify({ email, password: "abcd1234" }),
    })
  );
  expect(sup.ok).toBe(true);
  const { userId } = (await sup.json()) as { userId: string };
  return userId;
}

/** Build a deterministic Uint8Array of `n` bytes for content tests. */
function bytes(n: number, fill = 0xab): Uint8Array {
  const u = new Uint8Array(n);
  u.fill(fill);
  return u;
}

/** Read all chunk rows (refcounts) from a ShardDO. */
async function readShardSnapshot(
  stub: DurableObjectStub<ShardDO>
): Promise<{ hashes: { hash: string; ref_count: number; deleted_at: number | null }[]; refs: number }> {
  return runInDurableObject(stub, async (_instance, state) => {
    const sql = state.storage.sql;
    const hashes = sql
      .exec(
        "SELECT hash, ref_count, deleted_at FROM chunks ORDER BY hash"
      )
      .toArray() as {
      hash: string;
      ref_count: number;
      deleted_at: number | null;
    }[];
    const refs = (
      sql.exec("SELECT COUNT(*) as n FROM chunk_refs").toArray()[0] as {
        n: number;
      }
    ).n;
    return { hashes, refs };
  });
}

/**
 * Backdate every soft-marked chunk's deleted_at into the past, so the
 * 30s grace window in alarm() lets them be hard-deleted on the next
 * `runDurableObjectAlarm` tick. Without this the test would have to
 * sleep 30s.
 */
async function backdateDeletedAt(
  stub: DurableObjectStub<ShardDO>,
  agoMs = 60_000
): Promise<void> {
  await runInDurableObject(stub, async (_instance, state) => {
    state.storage.sql.exec(
      "UPDATE chunks SET deleted_at = ? WHERE deleted_at IS NOT NULL",
      Date.now() - agoMs
    );
  });
}

/**
 * Force-fire the alarm right now. In the vitest-pool-workers harness,
 * `runDurableObjectAlarm` runs whatever alarm is scheduled regardless
 * of its timestamp, so we don't need to time-travel — but if no alarm
 * is scheduled we synthesize one so the harness has something to run.
 */
async function fireAlarmNow(stub: DurableObjectStub<ShardDO>): Promise<boolean> {
  await runInDurableObject(stub, async (_instance, state) => {
    const cur = await state.storage.getAlarm();
    if (cur === null) {
      // No alarm pending — synthesize one in the past so the harness
      // sees something to run. (Setting in the past is fine; alarm
      // handlers must be idempotent.)
      await state.storage.setAlarm(Date.now() + 1);
    }
  });
  return runDurableObjectAlarm(stub);
}

// ── writeFile: inline tier ─────────────────────────────────────────────

describe("vfsWriteFile (inline tier)", () => {
  it("inlines a small payload, never touches ShardDO", async () => {
    const stub = E.USER_DO.get(E.USER_DO.idFromName("vfs-write:inline-small"));
    const userId = await seedUser(stub, "il-w@e.com");
    const scope = { ns: "default", tenant: userId };

    const payload = new TextEncoder().encode("hello phase 3");
    expect(payload.byteLength).toBeLessThanOrEqual(INLINE_LIMIT);
    await stub.vfsWriteFile(scope, "/note.txt", payload);

    // Round-trip via read.
    const got = await stub.vfsReadFile(scope, "/note.txt");
    expect(new TextDecoder().decode(got)).toBe("hello phase 3");

    // Verify the row went via inline branch (no chunk rows).
    await runInDurableObject(stub, async (_instance, state) => {
      const row = state.storage.sql
        .exec(
          "SELECT inline_data, chunk_count, status FROM files WHERE user_id=? AND file_name='note.txt'",
          userId
        )
        .toArray()[0] as
        | {
            inline_data: ArrayBuffer | null;
            chunk_count: number;
            status: string;
          }
        | undefined;
      expect(row).toBeDefined();
      expect(row!.status).toBe("complete");
      expect(row!.chunk_count).toBe(0);
      expect(row!.inline_data).not.toBeNull();
      expect(row!.inline_data!.byteLength).toBe(payload.byteLength);

      // No file_chunks rows for an inlined write.
      const chunks = state.storage.sql
        .exec(
          "SELECT COUNT(*) as n FROM file_chunks WHERE file_id IN (SELECT file_id FROM files WHERE user_id=?)",
          userId
        )
        .toArray()[0] as { n: number };
      expect(chunks.n).toBe(0);
    });
  });

  it("EISDIR when target path is a directory", async () => {
    const stub = E.USER_DO.get(E.USER_DO.idFromName("vfs-write:isdir"));
    const userId = await seedUser(stub, "isd@e.com");
    const scope = { ns: "default", tenant: userId };

    await stub.vfsMkdir(scope, "/d");
    await expect(
      stub.vfsWriteFile(scope, "/d", new Uint8Array([1, 2, 3]))
    ).rejects.toThrow(/EISDIR/);
  });

  it("ENOENT when parent directory is missing", async () => {
    const stub = E.USER_DO.get(E.USER_DO.idFromName("vfs-write:noparent"));
    const userId = await seedUser(stub, "np@e.com");
    const scope = { ns: "default", tenant: userId };

    await expect(
      stub.vfsWriteFile(scope, "/missing/x.txt", new Uint8Array([1]))
    ).rejects.toThrow(/ENOENT/);
  });
});

// ── writeFile: atomicity ───────────────────────────────────────────────

describe("vfsWriteFile atomicity (temp-id-then-rename)", () => {
  it("never exposes the tmp row at the real leaf name during a write", async () => {
    // Inlined write is essentially synchronous from the test's POV — the
    // tmp-row insert and the rename happen in one DO method body. We
    // can verify correctness by post-conditions: there must be exactly
    // ONE live row at the leaf name (the new one), no _vfs_tmp_*
    // rows, and chunk_count consistent with inline branch.
    const stub = E.USER_DO.get(E.USER_DO.idFromName("vfs-write:atomic-inline"));
    const userId = await seedUser(stub, "at@e.com");
    const scope = { ns: "default", tenant: userId };

    await stub.vfsWriteFile(scope, "/x", new TextEncoder().encode("v1"));
    await stub.vfsWriteFile(scope, "/x", new TextEncoder().encode("v2"));

    const live = await runInDurableObject(stub, async (_instance, state) => {
      const rows = state.storage.sql
        .exec(
          "SELECT file_name, status FROM files WHERE user_id=? ORDER BY file_name",
          userId
        )
        .toArray() as { file_name: string; status: string }[];
      return rows;
    });

    // After two overwrites: exactly one live "x" with status='complete'
    // and one tombstoned row (the v1 supersede). No _vfs_tmp_* leaks.
    const liveOnes = live.filter((r) => r.status === "complete");
    expect(liveOnes.length).toBe(1);
    expect(liveOnes[0].file_name).toBe("x");
    const tmpLeaks = live.filter((r) => r.file_name.startsWith("_vfs_tmp_"));
    expect(tmpLeaks).toEqual([]);

    // Read sees v2.
    const v = await stub.vfsReadFile(scope, "/x");
    expect(new TextDecoder().decode(v)).toBe("v2");
  });

  it("overwrite leaves the old file's rows hard-deleted, not lingering", async () => {
    const stub = E.USER_DO.get(E.USER_DO.idFromName("vfs-write:overwrite-clean"));
    const userId = await seedUser(stub, "ow@e.com");
    const scope = { ns: "default", tenant: userId };

    await stub.vfsWriteFile(
      scope,
      "/x",
      new TextEncoder().encode("first version")
    );

    // Capture the file_id of v1 before overwrite.
    const v1Id = await runInDurableObject(stub, async (_instance, state) => {
      const r = state.storage.sql
        .exec(
          "SELECT file_id FROM files WHERE user_id=? AND file_name='x' AND status='complete'",
          userId
        )
        .toArray()[0] as { file_id: string };
      return r.file_id;
    });

    await stub.vfsWriteFile(
      scope,
      "/x",
      new TextEncoder().encode("second version")
    );

    // The v1 row must be HARD-DELETED, not soft-tombstoned. (commitRename
    // calls hardDeleteFileRow on the superseded id.)
    const v1Still = await runInDurableObject(stub, async (_instance, state) => {
      return state.storage.sql
        .exec("SELECT 1 FROM files WHERE file_id=?", v1Id)
        .toArray().length;
    });
    expect(v1Still).toBe(0);
  });
});

// ── writeFile: chunked tier ────────────────────────────────────────────

describe("vfsWriteFile (chunked tier) + ShardDO refcount", () => {
  it("writes a >INLINE_LIMIT file as chunks and dedups on rewrite of identical content", async () => {
    const stub = E.USER_DO.get(E.USER_DO.idFromName("vfs-write:chunked"));
    const userId = await seedUser(stub, "ch-w@e.com");
    const scope = { ns: "default", tenant: userId };

    const payload = bytes(32 * 1024, 0x42); // 32KB > INLINE_LIMIT(16K)
    await stub.vfsWriteFile(scope, "/blob.bin", payload);

    // Verify file row + at least one file_chunks row.
    const meta = await runInDurableObject(stub, async (_instance, state) => {
      const row = state.storage.sql
        .exec(
          "SELECT file_id, chunk_count, status, inline_data FROM files WHERE user_id=? AND file_name='blob.bin'",
          userId
        )
        .toArray()[0] as {
        file_id: string;
        chunk_count: number;
        status: string;
        inline_data: ArrayBuffer | null;
      };
      const chunks = state.storage.sql
        .exec(
          "SELECT chunk_index, chunk_hash, shard_index FROM file_chunks WHERE file_id=? ORDER BY chunk_index",
          row.file_id
        )
        .toArray() as {
        chunk_index: number;
        chunk_hash: string;
        shard_index: number;
      }[];
      return { row, chunks };
    });

    expect(meta.row.status).toBe("complete");
    expect(meta.row.inline_data).toBeNull();
    expect(meta.row.chunk_count).toBeGreaterThan(0);
    expect(meta.chunks.length).toBe(meta.row.chunk_count);

    // Read it back — round-trip equality.
    const got = await stub.vfsReadFile(scope, "/blob.bin");
    expect(got.byteLength).toBe(payload.byteLength);
    expect(got).toEqual(payload);

    // Now overwrite with IDENTICAL content. Each writeFile uses a
    // fresh tmp file_id, and placeChunk hashes (file_id, idx) into a
    // shard slot — so the new chunks may land on a DIFFERENT shard
    // than the old ones. Both shards may show refcount activity.
    //
    // After the supersede GC settles, the union over all touched
    // shards must show exactly the SHARED hash live somewhere. The
    // old shard's chunks (under the old file_id) end up at
    // ref_count=0 + deleted_at set; the new shard's chunks live at
    // ref_count=1.
    const oldShardIdx = meta.chunks[0].shard_index;
    const oldShard = E.SHARD_DO.get(
      E.SHARD_DO.idFromName(`shard:${userId}:${oldShardIdx}`)
    );
    const oldHashes = (await readShardSnapshot(oldShard)).hashes.map(
      (h) => h.hash
    );

    await stub.vfsWriteFile(scope, "/blob.bin", payload);

    // Collect all shards touched after the rewrite.
    const allShardIdxs = await runInDurableObject(
      stub,
      async (_instance, state) => {
        const rows = state.storage.sql
          .exec(
            "SELECT DISTINCT shard_index FROM file_chunks WHERE file_id IN (SELECT file_id FROM files WHERE user_id=? AND status='complete')",
            userId
          )
          .toArray() as { shard_index: number }[];
        return rows.map((r) => r.shard_index);
      }
    );

    // Aggregate live and marked counts across every touched shard
    // (including the old one if it differs).
    const shardIdxsToCheck = new Set([oldShardIdx, ...allShardIdxs]);
    let liveCount = 0;
    let markedCount = 0;
    for (const idx of shardIdxsToCheck) {
      const ss = E.SHARD_DO.get(
        E.SHARD_DO.idFromName(`shard:${userId}:${idx}`)
      );
      const snap = await readShardSnapshot(ss);
      for (const h of snap.hashes) {
        if (h.deleted_at === null) {
          expect(h.ref_count).toBeGreaterThan(0);
          liveCount++;
        } else {
          expect(h.ref_count).toBe(0);
          markedCount++;
        }
      }
    }
    // Exactly one chunk hash is live (the rewritten file's).
    expect(liveCount).toBe(1);
    // Either zero (same shard, dedup'd) or one (different shard, old
    // copy soft-marked) marked-for-GC chunks.
    expect([0, 1]).toContain(markedCount);
    expect(oldHashes.length).toBe(1);
  });
});

// ── unlink + chunk GC ──────────────────────────────────────────────────

describe("vfsUnlink + ShardDO chunk GC alarm sweeper", () => {
  it("unlink decrements ref_count to 0, marks deleted_at, alarm hard-deletes", async () => {
    const stub = E.USER_DO.get(E.USER_DO.idFromName("vfs-write:unlink-gc"));
    const userId = await seedUser(stub, "un@e.com");
    const scope = { ns: "default", tenant: userId };

    const payload = bytes(32 * 1024, 0x77);
    await stub.vfsWriteFile(scope, "/gone.bin", payload);

    const shardIdx = await runInDurableObject(stub, async (_instance, state) => {
      const r = state.storage.sql
        .exec(
          "SELECT shard_index FROM file_chunks WHERE file_id IN (SELECT file_id FROM files WHERE user_id=? AND file_name='gone.bin') LIMIT 1",
          userId
        )
        .toArray()[0] as { shard_index: number };
      return r.shard_index;
    });
    const shardStub = E.SHARD_DO.get(
      E.SHARD_DO.idFromName(`shard:${userId}:${shardIdx}`)
    );

    // Pre-unlink: ref_count=1 on each hash, no deleted_at.
    let snap = await readShardSnapshot(shardStub);
    expect(snap.hashes.length).toBeGreaterThan(0);
    for (const h of snap.hashes) {
      expect(h.ref_count).toBe(1);
      expect(h.deleted_at).toBeNull();
    }

    await stub.vfsUnlink(scope, "/gone.bin");

    // Post-unlink, pre-alarm: ref_count=0, deleted_at set.
    snap = await readShardSnapshot(shardStub);
    for (const h of snap.hashes) {
      expect(h.ref_count).toBe(0);
      expect(h.deleted_at).not.toBeNull();
    }
    expect(snap.refs).toBe(0); // chunk_refs row dropped

    // ENOENT on read after unlink.
    await expect(stub.vfsReadFile(scope, "/gone.bin")).rejects.toThrow(
      /ENOENT/
    );

    // Backdate deleted_at past the 30s grace, force-fire the alarm.
    await backdateDeletedAt(shardStub);
    await fireAlarmNow(shardStub);

    // Post-alarm: chunk rows hard-deleted.
    snap = await readShardSnapshot(shardStub);
    expect(snap.hashes.length).toBe(0);
  });

  it("refcount cannot drift negative on repeated deleteChunks", async () => {
    // Direct ShardDO test: deleteChunks twice on the same fileId. The
    // MAX(0, ref_count - 1) clause should keep things sane; second
    // call must be a no-op.
    const shardStub = E.SHARD_DO.get(
      E.SHARD_DO.idFromName("vfs-write:negdrift-shard")
    );

    const hash = "f".repeat(64);
    const fileId = "fA";
    // PUT a chunk via the legacy HTTP route.
    await shardStub.fetch(
      new Request("http://internal/chunk", {
        method: "PUT",
        headers: {
          "X-Chunk-Hash": hash,
          "X-File-Id": fileId,
          "X-Chunk-Index": "0",
          "X-User-Id": "u",
        },
        body: "data",
      })
    );

    // First deleteChunks: ref_count 1 → 0, mark deleted_at.
    const r1 = await shardStub.deleteChunks(fileId);
    expect(r1.marked).toBe(1);

    // Second deleteChunks (no chunk_refs left for this fileId): no-op.
    const r2 = await shardStub.deleteChunks(fileId);
    expect(r2.marked).toBe(0);

    // ref_count must still be 0, never negative.
    const snap = await readShardSnapshot(shardStub);
    const h = snap.hashes.find((x) => x.hash === hash);
    expect(h).toBeDefined();
    expect(h!.ref_count).toBe(0);
  });

  it("resurrection: re-uploading the same hash before the alarm clears deleted_at", async () => {
    const shardStub = E.SHARD_DO.get(
      E.SHARD_DO.idFromName("vfs-write:resurrect-shard")
    );

    const hash = "1".repeat(64);
    // Initial PUT.
    await shardStub.fetch(
      new Request("http://internal/chunk", {
        method: "PUT",
        headers: {
          "X-Chunk-Hash": hash,
          "X-File-Id": "fA",
          "X-Chunk-Index": "0",
          "X-User-Id": "u",
        },
        body: "data",
      })
    );

    // deleteChunks → marks deleted_at.
    await shardStub.deleteChunks("fA");
    let snap = await readShardSnapshot(shardStub);
    expect(snap.hashes[0].deleted_at).not.toBeNull();
    expect(snap.hashes[0].ref_count).toBe(0);

    // Now another file references the same chunk hash before the alarm.
    await shardStub.putChunk(hash, new TextEncoder().encode("data"), "fB", 0, "u");

    // deleted_at should be cleared, ref_count back at 1.
    snap = await readShardSnapshot(shardStub);
    expect(snap.hashes[0].deleted_at).toBeNull();
    expect(snap.hashes[0].ref_count).toBe(1);

    // Alarm-driven sweep should NOT delete the resurrected chunk even
    // if we backdate the (now-NULL) deleted_at — because alarm() only
    // touches rows whose deleted_at IS NOT NULL. Sanity-check: alarm
    // returns true if there was any alarm scheduled, false otherwise.
    // Here we may not have an alarm scheduled (we cleared it via
    // resurrection). Either way, the chunk must remain.
    await runDurableObjectAlarm(shardStub);
    snap = await readShardSnapshot(shardStub);
    expect(snap.hashes.length).toBe(1);
    expect(snap.hashes[0].ref_count).toBe(1);
  });
});

// ── rename & rename-replace ────────────────────────────────────────────

describe("vfsRename", () => {
  it("simple rename: src no longer resolves, dst does, chunks intact", async () => {
    const stub = E.USER_DO.get(E.USER_DO.idFromName("vfs-write:rename-simple"));
    const userId = await seedUser(stub, "rn@e.com");
    const scope = { ns: "default", tenant: userId };

    await stub.vfsWriteFile(scope, "/a.bin", bytes(32 * 1024, 0x11));
    await stub.vfsRename(scope, "/a.bin", "/b.bin");

    expect(await stub.vfsExists(scope, "/a.bin")).toBe(false);
    expect(await stub.vfsExists(scope, "/b.bin")).toBe(true);
    const got = await stub.vfsReadFile(scope, "/b.bin");
    expect(got.byteLength).toBe(32 * 1024);
  });

  it("rename-replace: dst's old chunks are GC'd, new path inherits src chunks", async () => {
    const stub = E.USER_DO.get(E.USER_DO.idFromName("vfs-write:rename-replace"));
    const userId = await seedUser(stub, "rr@e.com");
    const scope = { ns: "default", tenant: userId };

    // Different content so the chunks are different hashes.
    const payloadA = bytes(32 * 1024, 0x11);
    const payloadB = bytes(32 * 1024, 0x22);
    await stub.vfsWriteFile(scope, "/a", payloadA);
    await stub.vfsWriteFile(scope, "/b", payloadB);

    // Snapshot the shard state. (32 KB → 1 chunk; placement may pick
    // different shards for /a and /b. Inspect both.)
    const allShardIdxs = await runInDurableObject(
      stub,
      async (_instance, state) => {
        const rows = state.storage.sql
          .exec(
            "SELECT DISTINCT shard_index FROM file_chunks WHERE file_id IN (SELECT file_id FROM files WHERE user_id=?)",
            userId
          )
          .toArray() as { shard_index: number }[];
        return rows.map((r) => r.shard_index);
      }
    );

    const shardStubs = allShardIdxs.map((idx) =>
      E.SHARD_DO.get(E.SHARD_DO.idFromName(`shard:${userId}:${idx}`))
    );

    // Pre-rename: every chunk has ref_count=1.
    for (const ss of shardStubs) {
      const snap = await readShardSnapshot(ss);
      for (const h of snap.hashes) expect(h.ref_count).toBe(1);
    }

    // Rename /a over /b — replaces b's content with a's.
    await stub.vfsRename(scope, "/a", "/b");

    expect(await stub.vfsExists(scope, "/a")).toBe(false);
    const got = await stub.vfsReadFile(scope, "/b");
    expect(got).toEqual(payloadA);

    // Post-rename: payloadB's hash should be marked for GC (ref_count=0).
    // payloadA's hash should still have ref_count=1.
    let totalLive = 0;
    let totalMarked = 0;
    for (const ss of shardStubs) {
      const snap = await readShardSnapshot(ss);
      for (const h of snap.hashes) {
        if (h.deleted_at !== null) totalMarked++;
        else totalLive++;
      }
    }
    expect(totalLive).toBe(1); // payloadA's hash, now under /b
    expect(totalMarked).toBe(1); // payloadB's hash, displaced
  });

  it("EISDIR when dst is a directory", async () => {
    const stub = E.USER_DO.get(E.USER_DO.idFromName("vfs-write:rename-isdir"));
    const userId = await seedUser(stub, "rnd@e.com");
    const scope = { ns: "default", tenant: userId };

    await stub.vfsWriteFile(scope, "/file.txt", new Uint8Array([1, 2, 3]));
    await stub.vfsMkdir(scope, "/d");
    await expect(stub.vfsRename(scope, "/file.txt", "/d")).rejects.toThrow(
      /EISDIR/
    );
  });

  it("ENOENT when src is missing", async () => {
    const stub = E.USER_DO.get(E.USER_DO.idFromName("vfs-write:rename-noent"));
    const userId = await seedUser(stub, "rne@e.com");
    const scope = { ns: "default", tenant: userId };
    await expect(stub.vfsRename(scope, "/nope", "/other")).rejects.toThrow(
      /ENOENT/
    );
  });
});

// ── mkdir / rmdir / chmod / symlink ────────────────────────────────────

describe("vfsMkdir / vfsRmdir / vfsChmod / vfsSymlink", () => {
  it("mkdir EEXIST on collision; recursive mkdir is idempotent", async () => {
    const stub = E.USER_DO.get(E.USER_DO.idFromName("vfs-write:mkdir"));
    const userId = await seedUser(stub, "mk@e.com");
    const scope = { ns: "default", tenant: userId };

    await stub.vfsMkdir(scope, "/d");
    await expect(stub.vfsMkdir(scope, "/d")).rejects.toThrow(/EEXIST/);
    await stub.vfsMkdir(scope, "/d", { recursive: true }); // ok, idempotent
    await stub.vfsMkdir(scope, "/a/b/c", { recursive: true });
    expect(await stub.vfsExists(scope, "/a")).toBe(true);
    expect(await stub.vfsExists(scope, "/a/b")).toBe(true);
    expect(await stub.vfsExists(scope, "/a/b/c")).toBe(true);
  });

  it("rmdir refuses non-empty; ok on empty", async () => {
    const stub = E.USER_DO.get(E.USER_DO.idFromName("vfs-write:rmdir"));
    const userId = await seedUser(stub, "rm@e.com");
    const scope = { ns: "default", tenant: userId };

    await stub.vfsMkdir(scope, "/d");
    await stub.vfsWriteFile(scope, "/d/x", new Uint8Array([1]));
    await expect(stub.vfsRmdir(scope, "/d")).rejects.toThrow(/ENOTDIR|ENOTEMPTY|not empty/i);
    await stub.vfsUnlink(scope, "/d/x");
    await stub.vfsRmdir(scope, "/d");
    expect(await stub.vfsExists(scope, "/d")).toBe(false);
  });

  it("chmod updates mode on a file and a dir", async () => {
    const stub = E.USER_DO.get(E.USER_DO.idFromName("vfs-write:chmod"));
    const userId = await seedUser(stub, "cm@e.com");
    const scope = { ns: "default", tenant: userId };

    await stub.vfsWriteFile(scope, "/f", new Uint8Array([1, 2]));
    await stub.vfsChmod(scope, "/f", 0o600);
    const s = await stub.vfsStat(scope, "/f");
    expect(s.mode).toBe(0o600);

    await stub.vfsMkdir(scope, "/d");
    await stub.vfsChmod(scope, "/d", 0o700);
    const sd = await stub.vfsStat(scope, "/d");
    expect(sd.mode).toBe(0o700);
  });

  it("symlink: create + lstat + stat-follow", async () => {
    const stub = E.USER_DO.get(E.USER_DO.idFromName("vfs-write:symlink"));
    const userId = await seedUser(stub, "sm@e.com");
    const scope = { ns: "default", tenant: userId };

    await stub.vfsWriteFile(scope, "/real.txt", new TextEncoder().encode("X"));
    await stub.vfsSymlink(scope, "/real.txt", "/ln");

    const ls = await stub.vfsLstat(scope, "/ln");
    expect(ls.type).toBe("symlink");
    const s = await stub.vfsStat(scope, "/ln");
    expect(s.type).toBe("file");

    // Reading via symlink follows.
    const got = await stub.vfsReadFile(scope, "/ln");
    expect(new TextDecoder().decode(got)).toBe("X");

    // EEXIST on duplicate symlink.
    await expect(stub.vfsSymlink(scope, "/real.txt", "/ln")).rejects.toThrow(
      /EEXIST/
    );

    // Unlinking the symlink doesn't touch the real file.
    await stub.vfsUnlink(scope, "/ln");
    expect(await stub.vfsExists(scope, "/ln")).toBe(false);
    expect(await stub.vfsExists(scope, "/real.txt")).toBe(true);
  });
});

// ── concurrent writes ──────────────────────────────────────────────────

describe("concurrent vfsWriteFile (UNIQUE-index serialization)", () => {
  it("two parallel writeFile to same path: exactly one survives, content intact", async () => {
    const stub = E.USER_DO.get(E.USER_DO.idFromName("vfs-write:concurrent"));
    const userId = await seedUser(stub, "co@e.com");
    const scope = { ns: "default", tenant: userId };

    const A = new TextEncoder().encode("AAAA");
    const B = new TextEncoder().encode("BBBB");

    // The DO is single-threaded, so the two RPC calls serialize at the
    // DO. Whichever the runtime schedules second wins. The post-condition
    // is what matters: the read returns one of them in full, and only
    // ONE row is live at the leaf.
    const [ra, rb] = await Promise.allSettled([
      stub.vfsWriteFile(scope, "/race", A),
      stub.vfsWriteFile(scope, "/race", B),
    ]);
    // Both succeed (single-threaded => no actual race conflict).
    expect(ra.status).toBe("fulfilled");
    expect(rb.status).toBe("fulfilled");

    const got = await stub.vfsReadFile(scope, "/race");
    const decoded = new TextDecoder().decode(got);
    expect(["AAAA", "BBBB"]).toContain(decoded);

    // Exactly one live row at the leaf.
    const live = await runInDurableObject(stub, async (_instance, state) => {
      const rows = state.storage.sql
        .exec(
          "SELECT file_name, status FROM files WHERE user_id=? AND status='complete'",
          userId
        )
        .toArray() as { file_name: string; status: string }[];
      return rows.filter((r) => r.file_name === "race").length;
    });
    expect(live).toBe(1);
  });

  it("ten back-to-back overwrites of same path: still one live row, no tmp leaks", async () => {
    const stub = E.USER_DO.get(E.USER_DO.idFromName("vfs-write:overwrite-x10"));
    const userId = await seedUser(stub, "ox@e.com");
    const scope = { ns: "default", tenant: userId };

    for (let i = 0; i < 10; i++) {
      await stub.vfsWriteFile(
        scope,
        "/x",
        new TextEncoder().encode(`v${i}`)
      );
    }
    const got = await stub.vfsReadFile(scope, "/x");
    expect(new TextDecoder().decode(got)).toBe("v9");

    const counts = await runInDurableObject(
      stub,
      async (_instance, state) => {
        const all = state.storage.sql
          .exec(
            "SELECT file_name, status FROM files WHERE user_id=?",
            userId
          )
          .toArray() as { file_name: string; status: string }[];
        return {
          live: all.filter(
            (r) => r.file_name === "x" && r.status === "complete"
          ).length,
          tmpLeaks: all.filter((r) => r.file_name.startsWith("_vfs_tmp_"))
            .length,
        };
      }
    );
    expect(counts.live).toBe(1);
    expect(counts.tmpLeaks).toBe(0);
  });
});

// ── removeRecursive ────────────────────────────────────────────────────

describe("vfsRemoveRecursive", () => {
  it("rm -rf: empties subtree, GCs orphan chunks, leaves shared chunks alive", async () => {
    const stub = E.USER_DO.get(E.USER_DO.idFromName("vfs-write:rmrf"));
    const userId = await seedUser(stub, "rf@e.com");
    const scope = { ns: "default", tenant: userId };

    // Build a tree: /tree/a, /tree/b, /tree/sub/c, /outside
    // /tree/a + /outside share identical content (same chunk hashes,
    // dedup'd). /tree/b and /tree/sub/c each have unique content.
    const SHARED = bytes(32 * 1024, 0x55);
    const UNIQUE_B = bytes(32 * 1024, 0x66);
    const UNIQUE_C = bytes(32 * 1024, 0x77);

    await stub.vfsMkdir(scope, "/tree");
    await stub.vfsMkdir(scope, "/tree/sub");
    await stub.vfsWriteFile(scope, "/tree/a", SHARED);
    await stub.vfsWriteFile(scope, "/tree/b", UNIQUE_B);
    await stub.vfsWriteFile(scope, "/tree/sub/c", UNIQUE_C);
    await stub.vfsWriteFile(scope, "/outside", SHARED);

    // Snapshot all touched shards.
    const allShardIdxs = await runInDurableObject(
      stub,
      async (_instance, state) => {
        const rows = state.storage.sql
          .exec(
            "SELECT DISTINCT shard_index FROM file_chunks WHERE file_id IN (SELECT file_id FROM files WHERE user_id=?)",
            userId
          )
          .toArray() as { shard_index: number }[];
        return rows.map((r) => r.shard_index);
      }
    );
    const shardStubs = allShardIdxs.map((idx) =>
      E.SHARD_DO.get(E.SHARD_DO.idFromName(`shard:${userId}:${idx}`))
    );

    // rm -rf the tree.
    let r = await stub.vfsRemoveRecursive(scope, "/tree");
    while (!r.done) r = await stub.vfsRemoveRecursive(scope, "/tree", r.cursor);

    // /tree gone, /outside survives.
    expect(await stub.vfsExists(scope, "/tree")).toBe(false);
    expect(await stub.vfsExists(scope, "/tree/a")).toBe(false);
    expect(await stub.vfsExists(scope, "/tree/b")).toBe(false);
    expect(await stub.vfsExists(scope, "/tree/sub/c")).toBe(false);
    expect(await stub.vfsExists(scope, "/outside")).toBe(true);

    // Aggregate post-rmrf state across all touched shards.
    //
    // Note on placement: placeChunk hashes (file_id, chunk_index) → shard
    // slot. Identical content under DIFFERENT file_ids lands on potentially
    // DIFFERENT shards — i.e. cross-file dedup is shard-local, not
    // tenant-global. Concretely, /tree/a and /outside share bytes but
    // their chunks live on whichever shards their (file_id, 0) hash
    // picks. So the post-condition is:
    //
    //   - Exactly ONE live chunk remains anywhere in the tenant — the
    //     one under /outside (whatever shard it landed on).
    //   - Every other chunk (the three under /tree, regardless of
    //     whether dedup collapsed any of them on the same shard) is
    //     either gone or marked deleted_at.
    let liveCount = 0;
    let markedCount = 0;
    for (const ss of shardStubs) {
      const snap = await readShardSnapshot(ss);
      for (const h of snap.hashes) {
        if (h.deleted_at === null) {
          expect(h.ref_count).toBeGreaterThan(0);
          liveCount++;
        } else {
          expect(h.ref_count).toBe(0);
          markedCount++;
        }
      }
    }
    // One live chunk: /outside survives. Marked-for-GC count is the
    // number of chunks under the /tree subtree that didn't dedup
    // shard-locally with /outside. Worst case: 3 (no dedup); best
    // case: 2 (the SHARED chunk co-located with /outside's gets
    // released by /tree/a but stays live for /outside). Either is
    // valid — we just assert /outside survives + some chunks are
    // pending GC.
    expect(liveCount).toBe(1);
    expect(markedCount).toBeGreaterThanOrEqual(2);
    expect(markedCount).toBeLessThanOrEqual(3);

    // /outside still readable end-to-end.
    const got = await stub.vfsReadFile(scope, "/outside");
    expect(got).toEqual(SHARED);
  });
});

// ── End-to-end integration ────────────────────────────────────────────

describe("end-to-end VFS lifecycle", () => {
  it("writeFile → readFile → unlink → readFile(ENOENT) → chunks GC'd", async () => {
    const stub = E.USER_DO.get(E.USER_DO.idFromName("vfs-write:lifecycle"));
    const userId = await seedUser(stub, "lc@e.com");
    const scope = { ns: "default", tenant: userId };

    // 1) writeFile (chunked tier so we can observe chunk GC)
    const payload = bytes(48 * 1024, 0xcc);
    await stub.vfsWriteFile(scope, "/lf.bin", payload);

    // 2) readFile round-trip
    const r1 = await stub.vfsReadFile(scope, "/lf.bin");
    expect(r1).toEqual(payload);

    // Capture shard state for later verification.
    const shardIdx = await runInDurableObject(stub, async (_instance, state) => {
      const r = state.storage.sql
        .exec(
          "SELECT shard_index FROM file_chunks WHERE file_id IN (SELECT file_id FROM files WHERE user_id=? AND file_name='lf.bin') LIMIT 1",
          userId
        )
        .toArray()[0] as { shard_index: number };
      return r.shard_index;
    });
    const shardStub = E.SHARD_DO.get(
      E.SHARD_DO.idFromName(`shard:${userId}:${shardIdx}`)
    );
    let snap = await readShardSnapshot(shardStub);
    expect(snap.hashes.length).toBeGreaterThan(0);
    for (const h of snap.hashes) {
      expect(h.ref_count).toBe(1);
      expect(h.deleted_at).toBeNull();
    }

    // 3) unlink → ENOENT on read
    await stub.vfsUnlink(scope, "/lf.bin");
    await expect(stub.vfsReadFile(scope, "/lf.bin")).rejects.toThrow(/ENOENT/);

    // 4) chunks soft-marked
    snap = await readShardSnapshot(shardStub);
    for (const h of snap.hashes) {
      expect(h.ref_count).toBe(0);
      expect(h.deleted_at).not.toBeNull();
    }

    // 5) alarm-driven hard delete
    await backdateDeletedAt(shardStub);
    await fireAlarmNow(shardStub);

    snap = await readShardSnapshot(shardStub);
    expect(snap.hashes.length).toBe(0); // all hard-deleted
    expect(snap.refs).toBe(0);
  });
});
