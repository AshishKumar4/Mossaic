import {
  env,
  runDurableObjectAlarm,
  runInDurableObject,
} from "cloudflare:test";
import { describe, expect, it } from "vitest";

import { vfsShardDOName, vfsUserDOName } from "@core/lib/utils";
import type { ShardDO } from "@core/objects/shard/shard-do";
import { placeChunk } from "@shared/placement";
import { createVFS, type MossaicEnv, type UserDO } from "../../sdk/src/index";
import type {
  DeleteChunksFailurePhase,
  PutChunkFailurePhase,
} from "../test-worker";

interface FaultControls {
  testConfigurePutChunkFailure(
    phase: PutChunkFailurePhase,
    remaining: number | null
  ): Promise<void>;
  testClearPutChunkFailure(): Promise<void>;
  testConfigurePutChunkBlock(): Promise<void>;
  testWaitForPutChunkBlocked(): Promise<void>;
  testReleasePutChunkBlock(): Promise<void>;
  testConfigureAnyDeleteChunksFailure(
    phase: DeleteChunksFailurePhase,
    remaining: number | null
  ): Promise<void>;
  testClearDeleteChunksFailure(): Promise<void>;
}

type TestShardDO = ShardDO & FaultControls;

interface TestEnv {
  MOSSAIC_USER: DurableObjectNamespace<UserDO>;
  MOSSAIC_SHARD: DurableObjectNamespace<TestShardDO>;
}

interface CleanupIntent extends Record<string, SqlStorageValue> {
  ref_id: string;
  shard_index: number;
  attempts: number;
}

const E = env as unknown as TestEnv;
const NS = "default";

function envFor(): MossaicEnv {
  return {
    MOSSAIC_USER: E.MOSSAIC_USER as MossaicEnv["MOSSAIC_USER"],
    MOSSAIC_SHARD:
      E.MOSSAIC_SHARD as unknown as MossaicEnv["MOSSAIC_SHARD"],
  };
}

function userStub(tenant: string): DurableObjectStub<UserDO> {
  return E.MOSSAIC_USER.get(
    E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
  );
}

function shardStub(
  tenant: string,
  shardIndex: number
): DurableObjectStub<TestShardDO> {
  return E.MOSSAIC_SHARD.get(
    E.MOSSAIC_SHARD.idFromName(
      vfsShardDOName(NS, tenant, undefined, shardIndex)
    )
  );
}

function chunkedBytes(fill: number): Uint8Array {
  return new Uint8Array(32 * 1024).fill(fill);
}

async function readIntents(tenant: string): Promise<CleanupIntent[]> {
  return runInDurableObject(userStub(tenant), async (_instance, state) => {
    return state.storage.sql
      .exec<CleanupIntent>(
        `SELECT ref_id, shard_index, attempts
           FROM chunk_cleanup_intents
          ORDER BY ref_id, shard_index`
      )
      .toArray();
  });
}

async function readRefIds(
  tenant: string,
  shardIndex: number
): Promise<string[]> {
  const stub = shardStub(tenant, shardIndex);
  await stub.fetch(new Request("http://internal/stats"));
  return runInDurableObject(stub, async (_instance, state) => {
    const rows = state.storage.sql
      .exec(
        "SELECT DISTINCT file_id FROM chunk_refs WHERE user_id = ? ORDER BY file_id",
        tenant
      )
      .toArray() as { file_id: string }[];
    return rows.map((row) => row.file_id);
  });
}

async function makeIntentEligible(
  tenant: string,
  refId: string
): Promise<void> {
  await runInDurableObject(userStub(tenant), async (_instance, state) => {
    state.storage.sql.exec(
      "UPDATE chunk_cleanup_intents SET next_attempt_at = 0 WHERE ref_id = ?",
      refId
    );
  });
}

async function configureUnknownWriteShard(tenant: string): Promise<void> {
  await Promise.all(
    Array.from({ length: 32 }, async (_, shardIndex) => {
      const shard = shardStub(tenant, shardIndex);
      await shard.testConfigurePutChunkFailure("after", 1);
      await shard.testConfigureAnyDeleteChunksFailure("before", 1);
    })
  );
}

describe("ordinary write-ahead chunk cleanup", () => {
  it("recovers a regular write ref after the put response is lost", async () => {
    const tenant = "ordinary-write-lost-response";
    const vfs = createVFS(envFor(), { tenant });
    await vfs.exists("/initialize");
    await configureUnknownWriteShard(tenant);

    await expect(
      vfs.writeFile("/lost.bin", chunkedBytes(0x31))
    ).rejects.toThrow(/putChunk response loss after mutation/);

    const intents = await readIntents(tenant);
    expect(intents).toHaveLength(1);
    expect(intents[0]?.attempts).toBe(1);
    const intent = intents[0]!;
    expect(await readRefIds(tenant, intent.shard_index)).toEqual([
      intent.ref_id,
    ]);
    expect(await vfs.exists("/lost.bin")).toBe(false);

    const shard = shardStub(tenant, intent.shard_index);
    await shard.testClearPutChunkFailure();
    await shard.testClearDeleteChunksFailure();
    await makeIntentEligible(tenant, intent.ref_id);
    expect(await runDurableObjectAlarm(userStub(tenant))).toBe(true);
    expect(await readIntents(tenant)).toEqual([]);
    expect(await readRefIds(tenant, intent.shard_index)).toEqual([]);
  });

  it("recovers a non-versioned copy retain after the response is lost", async () => {
    const tenant = "ordinary-copy-lost-response";
    const vfs = createVFS(envFor(), { tenant });
    await vfs.writeFile("/src.bin", chunkedBytes(0x41));
    const source = await runInDurableObject(
      userStub(tenant),
      async (_instance, state) => {
        return state.storage.sql
          .exec(
            `SELECT f.file_id, fc.shard_index
               FROM files f
               JOIN file_chunks fc ON fc.file_id = f.file_id
              WHERE f.file_name = 'src.bin' AND fc.chunk_index = 0`
          )
          .toArray()[0] as { file_id: string; shard_index: number };
      }
    );
    const shard = shardStub(tenant, source.shard_index);
    await shard.testConfigurePutChunkFailure("after", 1);
    await shard.testConfigureAnyDeleteChunksFailure("before", 1);

    await expect(vfs.copyFile("/src.bin", "/dest.bin")).rejects.toThrow(
      /putChunk response loss after mutation/
    );

    const intents = await readIntents(tenant);
    expect(intents).toHaveLength(1);
    const intent = intents[0]!;
    expect(intent).toMatchObject({
      shard_index: source.shard_index,
      attempts: 1,
    });
    expect(await readRefIds(tenant, source.shard_index)).toEqual(
      [source.file_id, intent.ref_id].sort()
    );
    expect(await vfs.exists("/dest.bin")).toBe(false);

    await shard.testClearPutChunkFailure();
    await shard.testClearDeleteChunksFailure();
    await makeIntentEligible(tenant, intent.ref_id);
    expect(await runDurableObjectAlarm(userStub(tenant))).toBe(true);
    expect(await readIntents(tenant)).toEqual([]);
    expect(await readRefIds(tenant, source.shard_index)).toEqual([
      source.file_id,
    ]);
  });

  it("lets stale-upload recovery find a stream append ref with no routing row", async () => {
    const tenant = "stream-append-lost-response";
    const stub = userStub(tenant);
    const scope = { ns: NS, tenant } as const;
    const handle = await stub.vfsBeginWriteStream(scope, "/stream.bin");
    const shardIndex = placeChunk(
      tenant,
      handle.tmpId,
      0,
      handle.poolSize
    );
    const shard = shardStub(tenant, shardIndex);
    await shard.testConfigurePutChunkFailure("after", 1);

    await expect(
      stub.vfsAppendWriteStream(scope, handle, 0, chunkedBytes(0x51))
    ).rejects.toThrow(/putChunk response loss after mutation/);

    expect(await readIntents(tenant)).toMatchObject([
      { ref_id: handle.tmpId, shard_index: shardIndex, attempts: 0 },
    ]);
    expect(await readRefIds(tenant, shardIndex)).toEqual([handle.tmpId]);
    await runInDurableObject(stub, async (_instance, state) => {
      state.storage.sql.exec(
        "UPDATE files SET created_at = ? WHERE file_id = ?",
        Date.now() - 2 * 60 * 60 * 1000,
        handle.tmpId
      );
      state.storage.sql.exec(
        "UPDATE write_stream_sessions SET expires_at = ? WHERE tmp_id = ?",
        Date.now() - 1,
        handle.tmpId
      );
      await state.storage.setAlarm(Date.now() - 1);
    });

    await shard.testClearPutChunkFailure();
    expect(await runDurableObjectAlarm(stub)).toBe(true);
    expect(await readIntents(tenant)).toEqual([]);
    expect(await readRefIds(tenant, shardIndex)).toEqual([]);
    await expect(stub.vfsCommitWriteStream(scope, handle)).rejects.toThrow(
      /ENOENT/
    );
  });

  it("does not overwrite a destination that appears while copy retains are blocked", async () => {
    const tenant = "copy-overwrite-false-publication-race";
    const vfs = createVFS(envFor(), { tenant });
    await vfs.writeFile("/src.bin", chunkedBytes(0x61));
    const shardIndex = await runInDurableObject(
      userStub(tenant),
      async (_instance, state) => {
        return (
          state.storage.sql
            .exec(
              `SELECT fc.shard_index
                 FROM files f
                 JOIN file_chunks fc ON fc.file_id = f.file_id
                WHERE f.file_name = 'src.bin' AND fc.chunk_index = 0`
            )
            .toArray()[0] as { shard_index: number }
        ).shard_index;
      }
    );
    const shard = shardStub(tenant, shardIndex);
    await shard.testConfigurePutChunkBlock();

    const copy = vfs.copyFile("/src.bin", "/dest.bin", { overwrite: false });
    await shard.testWaitForPutChunkBlocked();
    await vfs.writeFile("/dest.bin", "concurrent destination");
    await shard.testReleasePutChunkBlock();

    await expect(copy).rejects.toThrow(/EBUSY|EEXIST/);
    expect(await vfs.readFile("/dest.bin", { encoding: "utf8" })).toBe(
      "concurrent destination"
    );
    const cleanupState = await runInDurableObject(
      userStub(tenant),
      async (_instance, state) => ({
        uploading: (
          state.storage.sql
            .exec("SELECT COUNT(*) AS n FROM files WHERE status = 'uploading'")
            .toArray()[0] as { n: number }
        ).n,
        intents: (
          state.storage.sql
            .exec("SELECT COUNT(*) AS n FROM chunk_cleanup_intents")
            .toArray()[0] as { n: number }
        ).n,
      })
    );
    expect(cleanupState).toEqual({ uploading: 0, intents: 0 });
  });
});
