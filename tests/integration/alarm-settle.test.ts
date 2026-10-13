import { env, runDurableObjectAlarm, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";

import { vfsShardDOName, vfsUserDOName } from "@core/lib/utils";
import { scheduleAlarmAt } from "@core/objects/user/internal-storage";
import type { ShardDO } from "@core/objects/shard/shard-do";
import {
  createVFS,
  type MossaicEnv,
  type UserDO,
} from "../../sdk/src/index";
import type { DeleteChunksFailurePhase } from "../test-worker";

/**
 * A write arms the UserDO's alarm before its rows exist, so a crash mid-write
 * is reclaimed. A write that completes leaves nothing pending, and takes the
 * alarm back rather than letting it wake the object 10 minutes later to find
 * nothing. An upload still open, a cleanup intent still owed, or a need the
 * alarm was set for besides keeps it.
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

function alarmOf(tenant: string): Promise<number | null> {
  return runInDurableObject(userStubFor(tenant), (_instance, state) => state.storage.getAlarm());
}

describe("the UserDO alarm a write arms", () => {
  it("is taken back when the write completes, inline or chunked", async () => {
    const tenant = "settle-completed";
    const vfs = createVFS(envFor(), { tenant });

    await vfs.writeFile("/small.txt", "hello");
    expect(await alarmOf(tenant)).toBeNull();

    await vfs.writeFile("/large.bin", new Uint8Array(300 * 1024).fill(0x01));
    expect(await alarmOf(tenant)).toBeNull();
  });

  it("is taken back when an overwrite and an unlink complete", async () => {
    const tenant = "settle-overwrite";
    const vfs = createVFS(envFor(), { tenant });

    await vfs.writeFile("/f.bin", new Uint8Array(300 * 1024).fill(0x02));
    await vfs.writeFile("/f.bin", new Uint8Array(300 * 1024).fill(0x03));
    expect(await alarmOf(tenant)).toBeNull();

    await vfs.unlink("/f.bin");
    expect(await alarmOf(tenant)).toBeNull();
  });

  it("is kept while an upload is still open", async () => {
    const tenant = "settle-open-stream";
    const vfs = createVFS(envFor(), { tenant });

    const { stream } = await vfs.createWriteStreamWithHandle("/open.bin");
    const writer = stream.getWriter();
    await writer.write(new Uint8Array(1024).fill(0x04));
    // A write beside it completes, and the open stream's tmp row is still pending.
    await vfs.writeFile("/done.txt", "done");

    expect(await alarmOf(tenant)).not.toBeNull();
    await writer.abort();
  });

  it("is kept while a cleanup intent is still owed, and the alarm that drains it leaves none", async () => {
    const tenant = "settle-intent";
    const vfs = createVFS(envFor(), { tenant });
    await vfs.writeFile("/gone.bin", new Uint8Array(32 * 1024).fill(0x05));

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
    const shard = E.MOSSAIC_SHARD.get(
      E.MOSSAIC_SHARD.idFromName(vfsShardDOName(NS, tenant, undefined, shardIndex))
    );
    await shard.testConfigureDeleteChunksFailure(fileId, "before", null);
    await vfs.unlink("/gone.bin");
    await shard.testClearDeleteChunksFailure();

    expect(await alarmOf(tenant)).not.toBeNull();

    await runInDurableObject(userStubFor(tenant), async (_instance, state) => {
      state.storage.sql.exec("UPDATE chunk_cleanup_intents SET next_attempt_at = 0 WHERE ref_id = ?", fileId);
    });
    // As the platform runs it: the stored alarm cleared, then the handler.
    expect(await runDurableObjectAlarm(userStubFor(tenant))).toBe(true);
    expect(await alarmOf(tenant)).toBeNull();
  });

  it("is kept when it was set for another need besides", async () => {
    const tenant = "settle-other";
    const vfs = createVFS(envFor(), { tenant });
    await vfs.writeFile("/first.txt", "first");

    // A maintenance step still to run (a retention page, a multipart abort) sets it.
    const target = Date.now() + 60_000;
    await runInDurableObject(userStubFor(tenant), (instance) => scheduleAlarmAt(instance, target));
    await vfs.writeFile("/second.txt", "second");

    expect(await alarmOf(tenant)).toBe(target);
  });
});
