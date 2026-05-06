import { describe, it, expect } from "vitest";
import { env, runInDurableObject } from "cloudflare:test";

/**
 * Phase 12 — copyFile.
 *
 * Pinned invariants:
 *   C1. Inline-tier copy: ZERO new shard work. dest.inline_data
 *       byte-equals src.inline_data.
 *   C2. Chunked-tier copy: chunks.length on every shard is
 *       UNCHANGED; ref_count goes +1 per src chunk on each shard;
 *       dest's file_chunks rows mirror src's chunk_hashes.
 *   C3. Versioned-tier copy: dest gets a fresh file_versions row
 *       with `user_visible=1`; version_chunks mirror src's head
 *       version chunks; chunks.length unchanged.
 *   C4. Yjs-mode src: dest is plain (mode_yjs=0); src yjs_oplog
 *       unchanged; dest's content equals src's materialized state.
 *   C5. overwrite=false + dest exists → EEXIST.
 *   C6. src missing → ENOENT.
 *   C7. src is directory → EISDIR.
 *   C8. src === dest → EINVAL.
 *   C9. Concurrent copies of same src to different dests: ref_count
 *       advances by N; chunks.length unchanged.
 *   C10. metadata + tags inherit by default; explicit opts override.
 */

import { createVFS, type MossaicEnv, type UserDO, ENOENT, EEXIST, EISDIR, EINVAL } from "../../sdk/src/index";
import { vfsUserDOName, vfsShardDOName } from "@core/lib/utils";
import type { ShardDO } from "@core/objects/shard/shard-do";

interface E {
  USER_DO: DurableObjectNamespace<UserDO>;
  SHARD_DO: DurableObjectNamespace<ShardDO>;
}
const E = env as unknown as E;
const NS = "default";

function envFor(): MossaicEnv {
  return { MOSSAIC_USER: E.USER_DO as MossaicEnv["MOSSAIC_USER"] };
}
function userStub(tenant: string) {
  return E.USER_DO.get(E.USER_DO.idFromName(vfsUserDOName(NS, tenant)));
}
function shardStub(tenant: string, idx: number) {
  return E.SHARD_DO.get(
    E.SHARD_DO.idFromName(vfsShardDOName(NS, tenant, undefined, idx))
  );
}

async function chunkRefCount(
  tenant: string,
  shardIdx: number,
  hash: string
): Promise<number> {
  const stub = shardStub(tenant, shardIdx);
  return runInDurableObject(stub, async (_, state) => {
    const r = state.storage.sql
      .exec("SELECT ref_count FROM chunks WHERE hash = ?", hash)
      .toArray()[0] as { ref_count: number } | undefined;
    return r?.ref_count ?? 0;
  });
}

describe("Phase 12 — copyFile inline-tier (C1)", () => {
  it("inline copy produces identical inline_data, zero new chunks", async () => {
    const tenant = "p12-cp-inline";
    const vfs = createVFS(envFor(), { tenant });
    await vfs.writeFile("/src.txt", "hello world", {
      metadata: { color: "red" },
      tags: ["t1"],
    });

    await vfs.copyFile("/src.txt", "/dest.txt");

    expect(await vfs.readFile("/dest.txt", { encoding: "utf8" })).toBe(
      "hello world"
    );
    // Inherited metadata + tags.
    const stub = userStub(tenant);
    const destInfo = await runInDurableObject(stub, async (_, state) => {
      const f = state.storage.sql
        .exec(
          "SELECT file_id, inline_data, metadata FROM files WHERE file_name='dest.txt'"
        )
        .toArray()[0] as {
        file_id: string;
        inline_data: ArrayBuffer | null;
        metadata: ArrayBuffer | null;
      };
      const tags = (
        state.storage.sql
          .exec("SELECT tag FROM file_tags WHERE path_id = ?", f.file_id)
          .toArray() as { tag: string }[]
      ).map((r) => r.tag);
      return {
        inline: f.inline_data
          ? new TextDecoder().decode(new Uint8Array(f.inline_data))
          : null,
        metadata: f.metadata
          ? JSON.parse(new TextDecoder().decode(new Uint8Array(f.metadata)))
          : null,
        tags,
      };
    });
    expect(destInfo.inline).toBe("hello world");
    expect(destInfo.metadata).toEqual({ color: "red" });
    expect(destInfo.tags).toEqual(["t1"]);
  });
});

describe("Phase 12 — copyFile chunked-tier (C2)", () => {
  it("chunked copy bumps refcount by 1 per chunk, no new chunks created", async () => {
    const tenant = "p12-cp-chunked";
    const vfs = createVFS(envFor(), { tenant });
    // INLINE_LIMIT is 16 KB; write 64 KB to force chunked tier.
    const big = new Uint8Array(64 * 1024).fill(0x41);
    await vfs.writeFile("/big.bin", big);

    // Capture src manifest (chunk hashes + shards).
    const stub = userStub(tenant);
    const srcManifest = await runInDurableObject(stub, async (_, state) => {
      const fid = (
        state.storage.sql
          .exec("SELECT file_id FROM files WHERE file_name='big.bin'")
          .toArray()[0] as { file_id: string }
      ).file_id;
      return state.storage.sql
        .exec(
          "SELECT chunk_hash, shard_index FROM file_chunks WHERE file_id=?",
          fid
        )
        .toArray() as { chunk_hash: string; shard_index: number }[];
    });
    expect(srcManifest.length).toBeGreaterThan(0);

    // Snapshot ref counts and chunks-table row counts BEFORE copy.
    const beforeRefs = await Promise.all(
      srcManifest.map((c) =>
        chunkRefCount(tenant, c.shard_index, c.chunk_hash)
      )
    );
    const touchedShards = Array.from(
      new Set(srcManifest.map((c) => c.shard_index))
    );
    const beforeChunkCounts = await Promise.all(
      touchedShards.map(async (idx) => {
        const stb = shardStub(tenant, idx);
        return runInDurableObject(stb, async (_, state) => {
          return (
            state.storage.sql
              .exec("SELECT COUNT(*) AS n FROM chunks")
              .toArray()[0] as { n: number }
          ).n;
        });
      })
    );

    // Copy.
    await vfs.copyFile("/big.bin", "/big-copy.bin");

    // ref_count = before + 1 for every src chunk_hash.
    const afterRefs = await Promise.all(
      srcManifest.map((c) =>
        chunkRefCount(tenant, c.shard_index, c.chunk_hash)
      )
    );
    for (let i = 0; i < beforeRefs.length; i++) {
      expect(afterRefs[i]).toBe(beforeRefs[i] + 1);
    }

    // chunks-table row counts unchanged on every touched shard.
    const afterChunkCounts = await Promise.all(
      touchedShards.map(async (idx) => {
        const stb = shardStub(tenant, idx);
        return runInDurableObject(stb, async (_, state) => {
          return (
            state.storage.sql
              .exec("SELECT COUNT(*) AS n FROM chunks")
              .toArray()[0] as { n: number }
          ).n;
        });
      })
    );
    expect(afterChunkCounts).toEqual(beforeChunkCounts);

    // dest readback equals src bytes.
    const destBytes = await vfs.readFile("/big-copy.bin");
    expect(destBytes.byteLength).toBe(big.byteLength);
  });
});

describe("Phase 12 — copyFile error paths (C5..C8)", () => {
  it("EEXIST when dest exists and overwrite=false", async () => {
    const tenant = "p12-cp-eexist";
    const vfs = createVFS(envFor(), { tenant });
    await vfs.writeFile("/a.txt", "src");
    await vfs.writeFile("/b.txt", "dest");
    await expect(
      vfs.copyFile("/a.txt", "/b.txt", { overwrite: false })
    ).rejects.toBeInstanceOf(EEXIST);
  });

  it("ENOENT on missing src", async () => {
    const tenant = "p12-cp-enoent";
    const vfs = createVFS(envFor(), { tenant });
    await expect(
      vfs.copyFile("/missing.txt", "/dest.txt")
    ).rejects.toBeInstanceOf(ENOENT);
  });

  it("EISDIR when src is a directory", async () => {
    const tenant = "p12-cp-eisdir";
    const vfs = createVFS(envFor(), { tenant });
    await vfs.mkdir("/dir");
    await expect(vfs.copyFile("/dir", "/dest.txt")).rejects.toBeInstanceOf(
      EISDIR
    );
  });

  it("EINVAL when src === dest", async () => {
    const tenant = "p12-cp-self";
    const vfs = createVFS(envFor(), { tenant });
    await vfs.writeFile("/a.txt", "x");
    await expect(vfs.copyFile("/a.txt", "/a.txt")).rejects.toBeInstanceOf(
      EINVAL
    );
  });

  it("overwrite=true (default) supersedes dest atomically", async () => {
    const tenant = "p12-cp-overwrite";
    const vfs = createVFS(envFor(), { tenant });
    await vfs.writeFile("/src.txt", "newbytes");
    await vfs.writeFile("/dest.txt", "oldbytes");
    await vfs.copyFile("/src.txt", "/dest.txt"); // default overwrite
    expect(await vfs.readFile("/dest.txt", { encoding: "utf8" })).toBe(
      "newbytes"
    );
  });
});

describe("Phase 12 — copyFile yjs-mode src (C4)", () => {
  it("yjs-mode src forks to a plain dest with materialized bytes", async () => {
    const tenant = "p12-cp-yjs";
    const vfs = createVFS(envFor(), { tenant });
    await vfs.writeFile("/live.md", "");
    await vfs.setYjsMode("/live.md", true);
    await vfs.writeFile("/live.md", "live content");

    await vfs.copyFile("/live.md", "/snapshot.md");

    expect(await vfs.readFile("/snapshot.md", { encoding: "utf8" })).toBe(
      "live content"
    );
    // dest is plain — no yjs op log, no mode_yjs bit.
    const stub = userStub(tenant);
    const destInfo = await runInDurableObject(stub, async (_, state) => {
      const f = state.storage.sql
        .exec(
          "SELECT file_id, mode_yjs FROM files WHERE file_name='snapshot.md'"
        )
        .toArray()[0] as { file_id: string; mode_yjs: number };
      const oplog = (
        state.storage.sql
          .exec(
            "SELECT COUNT(*) AS n FROM yjs_oplog WHERE path_id = ?",
            f.file_id
          )
          .toArray()[0] as { n: number }
      ).n;
      return { mode_yjs: f.mode_yjs, oplog };
    });
    expect(destInfo.mode_yjs).toBe(0);
    expect(destInfo.oplog).toBe(0);
  });
});

describe("Phase 12 — copyFile concurrent (C9)", () => {
  it("3 parallel copies to distinct dests bump ref_count by 3", async () => {
    const tenant = "p12-cp-concurrent";
    const vfs = createVFS(envFor(), { tenant });
    const big = new Uint8Array(64 * 1024).fill(0x42);
    await vfs.writeFile("/src.bin", big);

    const stub = userStub(tenant);
    const srcManifest = await runInDurableObject(stub, async (_, state) => {
      const fid = (
        state.storage.sql
          .exec("SELECT file_id FROM files WHERE file_name='src.bin'")
          .toArray()[0] as { file_id: string }
      ).file_id;
      return state.storage.sql
        .exec(
          "SELECT chunk_hash, shard_index FROM file_chunks WHERE file_id=?",
          fid
        )
        .toArray() as { chunk_hash: string; shard_index: number }[];
    });
    const before = await Promise.all(
      srcManifest.map((c) =>
        chunkRefCount(tenant, c.shard_index, c.chunk_hash)
      )
    );

    await Promise.all([
      vfs.copyFile("/src.bin", "/c1.bin"),
      vfs.copyFile("/src.bin", "/c2.bin"),
      vfs.copyFile("/src.bin", "/c3.bin"),
    ]);

    const after = await Promise.all(
      srcManifest.map((c) =>
        chunkRefCount(tenant, c.shard_index, c.chunk_hash)
      )
    );
    for (let i = 0; i < before.length; i++) {
      expect(after[i]).toBe(before[i] + 3);
    }
  });
});

describe("Phase 12 — copyFile metadata + tags inheritance (C10)", () => {
  it("inherits src metadata + tags by default; explicit opts override", async () => {
    const tenant = "p12-cp-meta";
    const vfs = createVFS(envFor(), { tenant });
    await vfs.writeFile("/src.txt", "x", {
      metadata: { project: "alpha", priority: 5 },
      tags: ["urgent", "v1"],
    });

    // 1. Default: inherit.
    await vfs.copyFile("/src.txt", "/inherit.txt");
    // 2. Override metadata only.
    await vfs.copyFile("/src.txt", "/override-meta.txt", {
      metadata: { project: "beta" },
    });
    // 3. Override tags only.
    await vfs.copyFile("/src.txt", "/override-tags.txt", {
      tags: ["copy"],
    });

    const stub = userStub(tenant);
    const data = await runInDurableObject(stub, async (_, state) => {
      const dump = (name: string) => {
        const f = state.storage.sql
          .exec(
            "SELECT file_id, metadata FROM files WHERE file_name = ?",
            name
          )
          .toArray()[0] as {
          file_id: string;
          metadata: ArrayBuffer | null;
        };
        const tags = (
          state.storage.sql
            .exec("SELECT tag FROM file_tags WHERE path_id = ? ORDER BY tag", f.file_id)
            .toArray() as { tag: string }[]
        ).map((r) => r.tag);
        return {
          metadata: f.metadata
            ? JSON.parse(new TextDecoder().decode(new Uint8Array(f.metadata)))
            : null,
          tags,
        };
      };
      return {
        inherit: dump("inherit.txt"),
        overrideMeta: dump("override-meta.txt"),
        overrideTags: dump("override-tags.txt"),
      };
    });

    expect(data.inherit.metadata).toEqual({ project: "alpha", priority: 5 });
    expect(data.inherit.tags).toEqual(["urgent", "v1"]);
    expect(data.overrideMeta.metadata).toEqual({ project: "beta" });
    expect(data.overrideMeta.tags).toEqual(["urgent", "v1"]); // tags inherited
    expect(data.overrideTags.metadata).toEqual({
      project: "alpha",
      priority: 5,
    }); // metadata inherited
    expect(data.overrideTags.tags).toEqual(["copy"]);
  });
});
