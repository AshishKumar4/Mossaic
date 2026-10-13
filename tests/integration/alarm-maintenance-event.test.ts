import { env, runInDurableObject } from "cloudflare:test";
import { afterEach, describe, expect, it, vi } from "vitest";

import { vfsShardDOName, vfsUserDOName } from "@core/lib/utils";
import type { ShardDO } from "@core/objects/shard/shard-do";
import {
  createVFS,
  type MossaicEnv,
  type UserDO,
} from "../../sdk/src/index";
import type { DeleteChunksFailurePhase } from "../test-worker";

/**
 * The UserDO alarm's maintenance runs on ShardDOs, so its own invocation
 * shows no work. It logs one `maintenance_swept` line when it did some, and
 * none when it found nothing, so a reader of this object's logs can tell a
 * sweep that cleaned up from an idle wake.
 */

interface FaultControls {
  testConfigureDeleteChunksFailure(
    fileId: string,
    phase: DeleteChunksFailurePhase,
    remaining: number | null
  ): Promise<void>;
  testClearDeleteChunksFailure(): Promise<void>;
}

interface TestEnv {
  MOSSAIC_USER: DurableObjectNamespace<UserDO>;
  MOSSAIC_SHARD: DurableObjectNamespace<ShardDO & FaultControls>;
}

const E = env as unknown as TestEnv;
const NS = "default";

function envFor(): MossaicEnv {
  return {
    MOSSAIC_USER: E.MOSSAIC_USER as MossaicEnv["MOSSAIC_USER"],
    MOSSAIC_SHARD: E.MOSSAIC_SHARD as unknown as MossaicEnv["MOSSAIC_SHARD"],
  };
}

function userStubFor(tenant: string): DurableObjectStub<UserDO> {
  return E.MOSSAIC_USER.get(E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant)));
}

/** The `maintenance_swept` lines logged while `run` ran. */
async function sweptLines(run: () => Promise<unknown>): Promise<Record<string, unknown>[]> {
  const spy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  try {
    await run();
    return spy.mock.calls
      .map((call) => {
        try {
          return JSON.parse(String(call[0])) as Record<string, unknown>;
        } catch {
          return null;
        }
      })
      .filter((line): line is Record<string, unknown> => line?.event === "maintenance_swept");
  } finally {
    spy.mockRestore();
  }
}

describe("the UserDO alarm's maintenance event", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("is logged once, with what it drained, when the alarm cleaned up on a shard", async () => {
    const tenant = "alarm-event-drained";
    const vfs = createVFS(envFor(), { tenant });
    await vfs.writeFile("/gone.bin", new Uint8Array(32 * 1024).fill(0x0a));

    const { fileId, shardIndex } = await runInDurableObject(userStubFor(tenant), async (_instance, state) => {
      const row = state.storage.sql
        .exec(
          `SELECT f.file_id, c.shard_index FROM files f
             JOIN file_chunks c ON c.file_id = f.file_id
            WHERE f.user_id = ? AND f.file_name = 'gone.bin' AND f.status = 'complete'`,
          tenant
        )
        .toArray()[0] as { file_id: string; shard_index: number };
      return { fileId: row.file_id, shardIndex: row.shard_index };
    });

    // The unlink's own cleanup fails, so the intent waits for the alarm.
    const shard = E.MOSSAIC_SHARD.get(
      E.MOSSAIC_SHARD.idFromName(vfsShardDOName(NS, tenant, undefined, shardIndex))
    );
    await shard.testConfigureDeleteChunksFailure(fileId, "before", null);
    await vfs.unlink("/gone.bin");
    await shard.testClearDeleteChunksFailure();
    await runInDurableObject(userStubFor(tenant), async (_instance, state) => {
      state.storage.sql.exec("UPDATE chunk_cleanup_intents SET next_attempt_at = 0 WHERE ref_id = ?", fileId);
    });

    const lines = await sweptLines(() =>
      runInDurableObject(userStubFor(tenant), (instance) => instance.alarm())
    );

    expect(lines).toHaveLength(1);
    expect(lines[0]).toMatchObject({
      level: "info",
      tenantId: `${NS}::${tenant}`,
      cleanupDrained: 1,
      staleSwept: 0,
    });
  });

  it("is not logged when the alarm found nothing to do", async () => {
    const tenant = "alarm-event-idle";
    const vfs = createVFS(envFor(), { tenant });
    // A completed write arms the stale-upload sweep and leaves it nothing.
    await vfs.writeFile("/kept.bin", new Uint8Array(32 * 1024).fill(0x0b));

    const lines = await sweptLines(() =>
      runInDurableObject(userStubFor(tenant), (instance) => instance.alarm())
    );

    expect(lines).toEqual([]);
  });
});
