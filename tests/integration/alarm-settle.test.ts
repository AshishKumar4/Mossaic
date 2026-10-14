import { env, runDurableObjectAlarm, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";

import { vfsShardDOName, vfsUserDOName } from "@core/lib/utils";
import { scheduleAlarmAt } from "@core/objects/user/internal-storage";
import type { ShardDO } from "@core/objects/shard/shard-do";
import type { UserDOCore } from "@core/objects/user/user-do-core";
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
 * nothing. Anything the alarm still owes keeps it: an upload still open (a
 * WritableStream opened after another write took it back among them), a
 * cleanup intent, a retention operation still running, a need set for another
 * reason, and in the App a published file not yet search-indexed.
 *
 * `MOSSAIC_USER_CORE` is the service deployment's UserDO, with no App layer;
 * `MOSSAIC_USER` is the App's.
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
  MOSSAIC_USER_CORE: DurableObjectNamespace<UserDOCore>;
  MOSSAIC_SHARD: DurableObjectNamespace<ShardDO & FaultControls>;
}

const E = env as unknown as TestEnv;
const NS = "default";

type Deployment = "core" | "app";

function users(deployment: Deployment): DurableObjectNamespace<UserDO> {
  return (deployment === "core" ? E.MOSSAIC_USER_CORE : E.MOSSAIC_USER) as DurableObjectNamespace<UserDO>;
}

function envFor(deployment: Deployment): MossaicEnv {
  return {
    MOSSAIC_USER: users(deployment) as MossaicEnv["MOSSAIC_USER"],
    MOSSAIC_SHARD: E.MOSSAIC_SHARD as unknown as MossaicEnv["MOSSAIC_SHARD"],
  };
}

function userStubFor(deployment: Deployment, tenant: string): DurableObjectStub<UserDO> {
  const namespace = users(deployment);
  return namespace.get(namespace.idFromName(vfsUserDOName(NS, tenant)));
}

function alarmOf(deployment: Deployment, tenant: string): Promise<number | null> {
  return runInDurableObject(userStubFor(deployment, tenant), (_instance, state) => state.storage.getAlarm());
}

describe("the UserDO alarm a write arms", () => {
  it("is taken back when the write completes, inline or chunked", async () => {
    const tenant = "settle-completed";
    const vfs = createVFS(envFor("core"), { tenant });

    await vfs.writeFile("/small.txt", "hello");
    expect(await alarmOf("core", tenant)).toBeNull();

    await vfs.writeFile("/large.bin", new Uint8Array(300 * 1024).fill(0x01));
    expect(await alarmOf("core", tenant)).toBeNull();
  });

  it("is taken back when an overwrite and an unlink complete", async () => {
    const tenant = "settle-overwrite";
    const vfs = createVFS(envFor("core"), { tenant });

    await vfs.writeFile("/f.bin", new Uint8Array(300 * 1024).fill(0x02));
    await vfs.writeFile("/f.bin", new Uint8Array(300 * 1024).fill(0x03));
    expect(await alarmOf("core", tenant)).toBeNull();

    await vfs.unlink("/f.bin");
    expect(await alarmOf("core", tenant)).toBeNull();
  });

  it("is kept while an upload is still open", async () => {
    const tenant = "settle-open-stream";
    const vfs = createVFS(envFor("core"), { tenant });

    const { stream } = await vfs.createWriteStreamWithHandle("/open.bin");
    const writer = stream.getWriter();
    await writer.write(new Uint8Array(1024).fill(0x04));
    // A write beside it completes, and the open stream's tmp row is still pending.
    await vfs.writeFile("/done.txt", "done");

    expect(await alarmOf("core", tenant)).not.toBeNull();
    await writer.abort();
  });

  it("is armed for a WritableStream opened after another write took it back", async () => {
    const tenant = "settle-stream-after";
    const vfs = createVFS(envFor("core"), { tenant });
    await vfs.writeFile("/done.txt", "done");
    expect(await alarmOf("core", tenant)).toBeNull();

    // Below one chunk, so no append runs and nothing else arms the alarm; the
    // caller then abandons the stream.
    const { stream } = await vfs.createWriteStreamWithHandle("/abandoned.bin");
    const writer = stream.getWriter();
    await writer.write(new Uint8Array(1024).fill(0x06));

    expect(await alarmOf("core", tenant)).not.toBeNull();
    await writer.abort();
  });

  it("is kept while a cleanup intent is still owed, and the alarm that drains it leaves none", async () => {
    const tenant = "settle-intent";
    const vfs = createVFS(envFor("core"), { tenant });
    await vfs.writeFile("/gone.bin", new Uint8Array(32 * 1024).fill(0x05));

    const { fileId, shardIndex } = await runInDurableObject(userStubFor("core", tenant), async (_instance, state) => {
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

    expect(await alarmOf("core", tenant)).not.toBeNull();

    await runInDurableObject(userStubFor("core", tenant), async (_instance, state) => {
      state.storage.sql.exec("UPDATE chunk_cleanup_intents SET next_attempt_at = 0 WHERE ref_id = ?", fileId);
    });
    // As the platform runs it: the stored alarm cleared, then the handler.
    expect(await runDurableObjectAlarm(userStubFor("core", tenant))).toBe(true);
    expect(await alarmOf("core", tenant)).toBeNull();
  });

  it("is kept while a retention operation is still running", async () => {
    const tenant = "settle-retention";
    const vfs = createVFS(envFor("core"), { tenant, versioning: "enabled" });
    await vfs.writeFile("/history.txt", "head");
    const stub = userStubFor("core", tenant);
    // 130 inline versions: one step's budget (128) leaves the operation running,
    // and inline versions owe no cleanup intents.
    await runInDurableObject(stub, async (_instance, state) => {
      const sql = state.storage.sql;
      const file = sql
        .exec(
          "SELECT file_id, updated_at FROM files WHERE file_name = 'history.txt' AND user_id = ?",
          tenant
        )
        .toArray()[0] as { file_id: string; updated_at: number };
      for (let i = 0; i < 130; i++) {
        sql.exec(
          `INSERT INTO file_versions
             (path_id, version_id, user_id, size, mode, mtime_ms, deleted,
              inline_data, chunk_size, chunk_count, file_hash, mime_type,
              user_visible)
           VALUES (?, ?, ?, 1, 420, ?, 0, ?, 0, 0, '', 'text/plain', 1)`,
          file.file_id,
          `seed-${i.toString().padStart(6, "0")}`,
          tenant,
          file.updated_at - i - 1,
          new Uint8Array([i & 0xff])
        );
      }
    });

    const step = await stub.vfsDropVersionsStep({ ns: NS, tenant }, "/history.txt", { keepLast: 1 }, "settle-retention-op");
    // The first step may only page the retention order; either way the work is not done.
    expect(step).toEqual({ done: false });
    const running = await runInDurableObject(stub, (_instance, state) =>
      state.storage.sql
        .exec("SELECT 1 FROM version_retention_operations WHERE status = 'running' UNION ALL SELECT 1 FROM schema_maintenance WHERE state <> 'ready'")
        .toArray().length
    );
    expect(running).toBeGreaterThan(0);

    // An unrelated write completes; the retention still owed keeps the alarm.
    await vfs.writeFile("/done.txt", "done");
    expect(await alarmOf("core", tenant)).not.toBeNull();
  });

  it("is kept when it was set for another need besides", async () => {
    const tenant = "settle-other";
    const vfs = createVFS(envFor("core"), { tenant });
    await vfs.writeFile("/first.txt", "first");

    // A maintenance step still to run (a retention page, a multipart abort) sets it.
    const target = Date.now() + 60_000;
    await runInDurableObject(userStubFor("core", tenant), (instance) => scheduleAlarmAt(instance, target));
    await vfs.writeFile("/second.txt", "second");

    expect(await alarmOf("core", tenant)).toBe(target);
  });

  it("is kept in the App while the published file is not yet search-indexed", async () => {
    const tenant = "settle-app-index";
    const vfs = createVFS(envFor("app"), { tenant });

    // As POST /api/vfs/writeFile does it: the file is published, and the index
    // callback never comes.
    await vfs.writeFile("/photo.txt", "a caption");

    expect(await alarmOf("app", tenant)).not.toBeNull();
  });
});
