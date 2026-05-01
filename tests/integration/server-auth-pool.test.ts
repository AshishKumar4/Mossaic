import { describe, it, expect } from "vitest";
import { env, runInDurableObject } from "cloudflare:test";
import { vfsUserDOName } from "@core/lib/utils";

/**
 * Phase 32 Fix 2 — server-authoritative pool size.
 *
 * `vfsAppendWriteStream` reads `pool_size` from the tmp `files`
 * row (server-stamped at begin time), not from the
 * client-supplied `handle.poolSize`. A malicious or buggy
 * handle.poolSize cannot bypass placement determinism.
 *
 * Cases:
 *   SAP1. Server records `pool_size` on the tmp row at begin time.
 *   SAP2. A handle with tampered `poolSize` is ignored \u2014 chunks
 *         land where the server's `pool_size` says they should.
 *   SAP3. The recorded `shard_index` matches placeChunk(server pool).
 */

interface E {
  MOSSAIC_USER: DurableObjectNamespace;
}
const E = env as unknown as E;
const NS = "default";

describe("Phase 32 Fix 2 — server-authoritative pool size", () => {
  it("SAP1 — vfsBeginWriteStream stamps pool_size from server quota", async () => {
    const tenant = "sap1-stamped";
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
    );
    const scope = { ns: NS, tenant };
    const handle = await stub.vfsBeginWriteStream(scope, "/s.bin");
    expect(typeof handle.poolSize).toBe("number");
    expect(handle.poolSize).toBeGreaterThanOrEqual(32);

    const recorded = await runInDurableObject(stub, async (_inst, state) => {
      return (
        state.storage.sql
          .exec(
            "SELECT pool_size FROM files WHERE file_id = ?",
            handle.tmpId
          )
          .toArray()[0] as { pool_size: number }
      ).pool_size;
    });
    expect(recorded).toBe(handle.poolSize);

    await stub.vfsAbortWriteStream(scope, handle);
  });

  it("SAP2 — tampered handle.poolSize cannot redirect placement", async () => {
    const tenant = "sap2-tamper";
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
    );
    const scope = { ns: NS, tenant };
    const handle = await stub.vfsBeginWriteStream(scope, "/s.bin");

    // Build a tampered handle: keep tmpId/parentId/leaf but
    // replace poolSize with an unrealistic 1 \u2014 pre-Phase-32 the
    // server would have used 1 here, concentrating the chunk on
    // shard 0.
    const tamperedHandle = {
      ...handle,
      poolSize: 1,
    };

    // Append 1 chunk of \u00d7 INLINE_LIMIT-relevant bytes via the
    // streaming write path. The append-stream surface always
    // routes through `placeChunk` regardless of size; we use a
    // non-inline payload to exercise the chunked-write code
    // path the test asserts on.
    const data = new Uint8Array(20 * 1024).fill(7);
    await stub.vfsAppendWriteStream(scope, tamperedHandle, 0, data);

    const recordedShard = await runInDurableObject(
      stub,
      async (_inst, state) => {
        return (
          state.storage.sql
            .exec(
              "SELECT shard_index FROM file_chunks WHERE file_id = ? LIMIT 1",
              handle.tmpId
            )
            .toArray()[0] as { shard_index: number }
        ).shard_index;
      }
    );

    // Server-authoritative placement uses pool_size=32 (default
    // for fresh tenant). A pool=1 placement would always land on
    // shard 0; with pool=32, it lands on whichever rendezvous
    // winner we get. We can't predict the exact shard without
    // duplicating placeChunk's math here, but we CAN assert the
    // chunk did not concentrate on shard 0 in a statistically
    // significant way: with a deterministic hash of (tenant,
    // tmpId, 0), the rendezvous-32 winner is uniformly
    // distributed across [0, 32), so seeing shard 0 with prob
    // 1/32. The strong assertion is recordedShard === server
    // placement, which we verify by re-running placeChunk with
    // the row's actual pool_size.
    const { placeChunk } = await import("@shared/placement");
    const expected = placeChunk(tenant, handle.tmpId, 0, handle.poolSize);
    expect(recordedShard).toBe(expected);

    await stub.vfsAbortWriteStream(scope, handle);
  });

  it("SAP3 — recorded shard_index uses the server pool_size (chunked tier)", async () => {
    const tenant = "sap3-pool-32";
    const stub = E.MOSSAIC_USER.get(
      E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
    );
    const scope = { ns: NS, tenant };
    // Force chunked tier (> INLINE_LIMIT 16 KiB) so a
    // file_chunks row with shard_index actually exists.
    const data = new Uint8Array(20 * 1024).fill(3);
    await stub.vfsWriteFile(scope, "/p.bin", data);

    const ctx = await runInDurableObject(stub, async (_inst, state) => {
      const row = state.storage.sql
        .exec(
          `SELECT fc.shard_index, f.file_id, f.pool_size FROM file_chunks fc
            JOIN files f ON f.file_id = fc.file_id
           WHERE f.file_name = 'p.bin' LIMIT 1`
        )
        .toArray()[0] as
        | { shard_index: number; file_id: string; pool_size: number }
        | undefined;
      return row;
    });
    expect(ctx).toBeDefined();
    const { placeChunk } = await import("@shared/placement");
    const expected = placeChunk(tenant, ctx!.file_id, 0, ctx!.pool_size);
    expect(ctx!.shard_index).toBe(expected);
  });
});
