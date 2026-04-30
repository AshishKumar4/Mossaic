import { describe, it, expect } from "vitest";
import { env } from "cloudflare:test";
import { vfsShardDOName, vfsUserDOName } from "@core/lib/utils";
import { createVFS, type MossaicEnv } from "../../sdk/src/index";
import type { ShardDO } from "@core/objects/shard/shard-do";

/**
 * ShardDO.getStorageBytes — telemetry RPC (Phase 23 audit Claim 4).
 *
 * Confirms:
 *  - Empty shard returns 0 bytes / 0 unique chunks.
 *  - After a real write, bytesStored matches the chunk-bytes sum.
 *  - softCapBytes is published (9 GB) — the value is observability-only,
 *    no enforcement happens client-side.
 */

interface E {
  MOSSAIC_USER: DurableObjectNamespace;
  MOSSAIC_SHARD: DurableObjectNamespace<ShardDO>;
}
const E = env as unknown as E;
const NS = "default";

function makeEnv(): MossaicEnv {
  return { MOSSAIC_USER: E.MOSSAIC_USER as MossaicEnv["MOSSAIC_USER"] };
}

describe("ShardDO getStorageBytes telemetry RPC", () => {
  it("empty shard reports 0 bytes / 0 unique chunks", async () => {
    const stub = E.MOSSAIC_SHARD.get(
      E.MOSSAIC_SHARD.idFromName(
        vfsShardDOName(NS, "ssb-empty", undefined, 0)
      )
    );
    const stats = await stub.getStorageBytes();
    expect(stats.bytesStored).toBe(0);
    expect(stats.uniqueChunks).toBe(0);
    expect(stats.softCapBytes).toBe(9 * 1024 * 1024 * 1024);
  });

  it("after a chunked write, bytesStored matches stored bytes", async () => {
    const tenant = "ssb-write";
    const vfs = createVFS(makeEnv(), { tenant });
    await vfs.exists("/");

    const bytes = 64 * 1024;
    await vfs.writeFile("/x.bin", new Uint8Array(bytes).fill(7));

    // The chunk could land on any of the 32 shards (rendezvous). Fan
    // the stat queries out in parallel — sequential await across 32
    // DO RPCs is slow under contention from other suites and trips
    // vitest's default 5s per-test timeout.
    const stats = await Promise.all(
      Array.from({ length: 32 }, (_, s) => {
        const stub = E.MOSSAIC_SHARD.get(
          E.MOSSAIC_SHARD.idFromName(vfsShardDOName(NS, tenant, undefined, s))
        );
        return stub.getStorageBytes();
      })
    );
    const total = stats.reduce((a, b) => a + b.bytesStored, 0);
    const unique = stats.reduce((a, b) => a + b.uniqueChunks, 0);
    expect(total).toBe(bytes);
    expect(unique).toBeGreaterThanOrEqual(1);
  }, 15000);

  it("softCapBytes is consistent across shards", async () => {
    const stub0 = E.MOSSAIC_SHARD.get(
      E.MOSSAIC_SHARD.idFromName(
        vfsShardDOName(NS, "ssb-cap-a", undefined, 0)
      )
    );
    const stub1 = E.MOSSAIC_SHARD.get(
      E.MOSSAIC_SHARD.idFromName(
        vfsShardDOName(NS, "ssb-cap-b", undefined, 7)
      )
    );
    const a = await stub0.getStorageBytes();
    const b = await stub1.getStorageBytes();
    expect(a.softCapBytes).toBe(b.softCapBytes);
    // Telemetry-only: confirm the shape carries no `enforced` flag.
    expect(Object.keys(a).sort()).toEqual([
      "bytesStored",
      "softCapBytes",
      "uniqueChunks",
    ]);
  });
});

// `vfsUserDOName` is imported only to pin the import surface; the
// shard-direct test path doesn't call into UserDO. Kept to keep the
// test file consistent with the rest of the suite.
void vfsUserDOName;
