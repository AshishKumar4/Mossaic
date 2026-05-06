import { describe, it, expect } from "vitest";
import { env, runInDurableObject } from "cloudflare:test";

/**
 * P1-1 fix — atomic `restoreVersion` via `ShardDO.restoreChunkRef`.
 *
 * Pre-fix `restoreVersion` did `chunksAlive` pre-flight + a per-
 * chunk `putChunk(empty)` loop. Between the two RPCs an unrelated
 * `dropVersions` of a sibling version that held the last reference
 * to a shared chunk could decrement-and-sweep the chunk; the loop
 * either threw partway (leaking chunk_refs under the new ref id)
 * or hit the 0-byte cold-path defense and corrupted the restored
 * version.
 *
 * The fix collapses (chunksAlive + bump-ref) into ONE atomic RPC
 * that holds the DO single-thread across both checks. These tests
 * pin:
 *   R1 — happy path: restoreVersion produces a new version whose
 *        bytes equal the source's.
 *   R2 — restoreChunkRef refuses when the chunk is swept (returns
 *        ENOENT cleanly via VFSError).
 *   R3 — restoreChunkRef is idempotent: calling restoreVersion on
 *        the same source twice produces two distinct version_ids
 *        without inflating ref_count beyond the expected delta.
 */

import {
  createVFS,
  type MossaicEnv,
  type UserDO,
  ENOENT,
} from "../../sdk/src/index";
import { vfsUserDOName, vfsShardDOName } from "@core/lib/utils";
import { placeChunkForVersion } from "@core/objects/user/vfs-versions";

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

describe("restoreVersion atomic (P1-1)", () => {
  it("R1 — happy path: restored version has source's bytes", async () => {
    const tenant = "restore-atomic-r1";
    const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });
    // Use a chunked-tier write so restoreVersion takes the
    // chunk-restore path (not the inline shortcut).
    const big = new Uint8Array(20 * 1024).fill(0x42);
    await vfs.writeFile("/r1.bin", big);
    const verA = await vfs.listVersions("/r1.bin");
    expect(verA).toHaveLength(1);
    const v1Id = verA[0].id;

    // Overwrite to push a new head.
    await vfs.writeFile("/r1.bin", new Uint8Array(20 * 1024).fill(0x99));

    const r = await vfs.restoreVersion("/r1.bin", v1Id);
    expect(typeof r.id).toBe("string");
    expect(r.id).not.toBe(v1Id);

    const back = await vfs.readFile("/r1.bin");
    expect(back.byteLength).toBe(big.byteLength);
    expect(back[0]).toBe(0x42);
    expect(back[back.byteLength - 1]).toBe(0x42);
  });

  it("R2 — restoreVersion refuses cleanly when source chunk was swept", async () => {
    // Construct a synthetic state where version_chunks references
    // a chunk_hash that does NOT exist on the shard. This is the
    // post-sweep state the atomic RPC must detect and refuse.
    const tenant = "restore-atomic-r2";
    const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });
    const big = new Uint8Array(20 * 1024).fill(0x33);
    await vfs.writeFile("/r2.bin", big);

    const userStub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName("default", tenant))
    );

    // Find the source version + its chunks.
    const { sourceVersionId, sourceChunk } = await runInDurableObject(
      userStub,
      async (_inst, state) => {
        const v = state.storage.sql
          .exec(
            "SELECT version_id FROM file_versions ORDER BY mtime_ms DESC LIMIT 1"
          )
          .toArray()[0] as { version_id: string };
        const c = state.storage.sql
          .exec(
            "SELECT chunk_hash, shard_index, chunk_index FROM version_chunks WHERE version_id = ?",
            v.version_id
          )
          .toArray()[0] as {
          chunk_hash: string;
          shard_index: number;
          chunk_index: number;
        };
        return { sourceVersionId: v.version_id, sourceChunk: c };
      }
    );

    // Manually delete the chunks row on the shard — simulates the
    // alarm-sweep terminal state.
    const shardStub = E.MOSSAIC_SHARD.get(
      E.MOSSAIC_SHARD.idFromName(
        vfsShardDOName("default", tenant, undefined, sourceChunk.shard_index)
      )
    );
    await runInDurableObject(shardStub, async (_inst, state) => {
      state.storage.sql.exec(
        "DELETE FROM chunks WHERE hash = ?",
        sourceChunk.chunk_hash
      );
      state.storage.sql.exec(
        "DELETE FROM chunk_refs WHERE chunk_hash = ?",
        sourceChunk.chunk_hash
      );
    });

    // restoreVersion now must surface ENOENT; the atomic
    // restoreChunkRef RPC catches the swept-chunk state and throws.
    let caught: unknown = null;
    try {
      await vfs.restoreVersion("/r2.bin", sourceVersionId);
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(ENOENT);

    // Suppress unused import warning. `placeChunkForVersion` is
    // reserved for future tests that exercise the shard-side
    // resolver; including it here keeps the import surface stable.
    void placeChunkForVersion;
  });

  it("R3 — restoreChunkRef is idempotent across repeated restores", async () => {
    const tenant = "restore-atomic-r3";
    const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });
    const data = new Uint8Array(20 * 1024).fill(0x77);
    await vfs.writeFile("/r3.bin", data);
    const versions = await vfs.listVersions("/r3.bin");
    const v1Id = versions[0].id;

    // Restore twice. Each call should produce a distinct version
    // and bump ref_count by exactly 1 (one new chunk_refs row per
    // call). No exceptions.
    const r1 = await vfs.restoreVersion("/r3.bin", v1Id);
    const r2 = await vfs.restoreVersion("/r3.bin", v1Id);
    expect(r1.id).not.toBe(r2.id);
    expect(r1.id).not.toBe(v1Id);
    expect(r2.id).not.toBe(v1Id);

    // The head is now r2's content; readFile returns the original
    // bytes (content-addressed → all three versions resolve to
    // the same chunk row on the shard).
    const back = await vfs.readFile("/r3.bin");
    expect(back.byteLength).toBe(data.byteLength);
    expect(back[0]).toBe(0x77);

    // ref_count on the chunk should equal the number of distinct
    // refs: original v1 + r1 + r2 = 3.
    const userStub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName("default", tenant))
    );
    const sourceChunk = await runInDurableObject(
      userStub,
      async (_inst, state) => {
        return state.storage.sql
          .exec(
            "SELECT chunk_hash, shard_index FROM version_chunks WHERE version_id = ? LIMIT 1",
            v1Id
          )
          .toArray()[0] as { chunk_hash: string; shard_index: number };
      }
    );
    const shardStub = E.MOSSAIC_SHARD.get(
      E.MOSSAIC_SHARD.idFromName(
        vfsShardDOName("default", tenant, undefined, sourceChunk.shard_index)
      )
    );
    const refCount = await runInDurableObject(
      shardStub,
      async (_inst, state) => {
        const r = state.storage.sql
          .exec("SELECT ref_count FROM chunks WHERE hash = ?", sourceChunk.chunk_hash)
          .toArray()[0] as { ref_count: number };
        return r.ref_count;
      }
    );
    expect(refCount).toBe(3);
  });
});
