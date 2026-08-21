import {
  env,
  runDurableObjectAlarm,
  runInDurableObject,
} from "cloudflare:test";
import { describe, expect, it } from "vitest";

import { vfsShardDOName, vfsUserDOName } from "@core/lib/utils";
import type { ShardDO } from "@core/objects/shard/shard-do";
import type { UserDO } from "@app/objects/user/user-do";
import { hashChunk } from "@shared/crypto";
import { placeChunk } from "@shared/placement";
import type {
  ClearMultipartStagingFailurePhase,
} from "../test-worker";

interface ShardFaultControls {
  testConfigurePutChunkBlock(): Promise<void>;
  testWaitForPutChunkBlocked(): Promise<void>;
  testReleasePutChunkBlock(): Promise<void>;
  testConfigureDeleteChunksFailure(
    fileId: string,
    phase: "before" | "after",
    remaining: number | null
  ): Promise<void>;
  testClearDeleteChunksFailure(): Promise<void>;
  testConfigureClearMultipartStagingFailure(
    phase: ClearMultipartStagingFailurePhase,
    remaining: number | null
  ): Promise<void>;
  testClearClearMultipartStagingFailure(): Promise<void>;
  testConfigureAnyDeleteChunksFailure(
    phase: "before" | "after",
    remaining: number | null
  ): Promise<void>;
}

type TestUserDO = UserDO;
type TestShardDO = ShardDO & ShardFaultControls;

interface TestEnv {
  MOSSAIC_USER: DurableObjectNamespace<TestUserDO>;
  MOSSAIC_SHARD: DurableObjectNamespace<TestShardDO>;
}

interface MultipartFixture {
  tenant: string;
  user: DurableObjectStub<TestUserDO>;
  shard: DurableObjectStub<TestShardDO>;
  uploadId: string;
}

interface RmrfFixture {
  tenant: string;
  user: DurableObjectStub<TestUserDO>;
  shard: DurableObjectStub<TestShardDO>;
  totalBytes: number;
}

const E = env as unknown as TestEnv;
const NS = "default";

function userStub(tenant: string): DurableObjectStub<TestUserDO> {
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

async function seedMultipart(
  tenant: string,
  expired = false
): Promise<MultipartFixture> {
  const user = userStub(tenant);
  const scope = { ns: NS, tenant } as const;
  const data = new Uint8Array([1, 3, 3, 7]);
  const begin = await user.vfsBeginMultipart(scope, "/upload.bin", {
    size: data.byteLength,
    chunkSize: data.byteLength,
  });
  const shardIndex = placeChunk(tenant, begin.uploadId, 0, begin.poolSize);
  const shard = shardStub(tenant, shardIndex);
  await shard.putChunkMultipart(
    await hashChunk(data),
    data,
    begin.uploadId,
    0,
    tenant,
    begin.sessionToken
  );
  if (expired) {
    await runInDurableObject(user, async (_instance, state) => {
      state.storage.sql.exec(
        "UPDATE upload_sessions SET expires_at = 0 WHERE upload_id = ?",
        begin.uploadId
      );
    });
  }
  return { tenant, user, shard, uploadId: begin.uploadId };
}

async function readMultipartState(fixture: MultipartFixture) {
  const local = await runInDurableObject(
    fixture.user,
    async (_instance, state) => {
      const sql = state.storage.sql;
      const session = sql
        .exec(
          "SELECT status FROM upload_sessions WHERE upload_id = ?",
          fixture.uploadId
        )
        .toArray()[0] as { status: string };
      const tempRows = (
        sql
          .exec("SELECT COUNT(*) AS n FROM files WHERE file_id = ?", fixture.uploadId)
          .toArray()[0] as { n: number }
      ).n;
      const intents = sql
        .exec(
          `SELECT cleanup_kind, attempts
             FROM chunk_cleanup_intents
            WHERE ref_id = ?`,
          fixture.uploadId
        )
        .toArray() as { cleanup_kind: string; attempts: number }[];
      return { status: session.status, tempRows, intents };
    }
  );
  const remote = await runInDurableObject(
    fixture.shard,
    async (_instance, state) => {
      const sql = state.storage.sql;
      return {
        refs: (
          sql
            .exec(
              "SELECT COUNT(*) AS n FROM chunk_refs WHERE file_id = ?",
              fixture.uploadId
            )
            .toArray()[0] as { n: number }
        ).n,
        staging: (
          sql
            .exec(
              "SELECT COUNT(*) AS n FROM upload_chunks WHERE upload_id = ?",
              fixture.uploadId
            )
            .toArray()[0] as { n: number }
        ).n,
      };
    }
  );
  return { ...local, ...remote };
}

async function makeCleanupEligible(
  user: DurableObjectStub<TestUserDO>
): Promise<void> {
  await runInDurableObject(user, async (_instance, state) => {
    state.storage.sql.exec(
      "UPDATE chunk_cleanup_intents SET next_attempt_at = 0"
    );
    await state.storage.setAlarm(Date.now() + 1_000);
  });
}

async function seedRmrf(tenant: string): Promise<RmrfFixture> {
  const user = userStub(tenant);
  const scope = { ns: NS, tenant } as const;
  await user.appGetQuota(tenant);
  await runInDurableObject(user, async (_instance, state) => {
    state.storage.sql.exec(
      "UPDATE quota SET pool_size = 1 WHERE user_id = ?",
      tenant
    );
  });
  await user.vfsMkdir(scope, "/tree", { recursive: true });
  const size = 20 * 1024;
  for (let index = 0; index < 3; index++) {
    await runInDurableObject(user, async (_instance, state) => {
      state.storage.sql.exec(
        "UPDATE quota SET pool_size = 1 WHERE user_id = ?",
        tenant
      );
    });
    await user.vfsWriteFile(
      scope,
      `/tree/file-${index}.bin`,
      new Uint8Array(size).fill(index + 1)
    );
  }
  return {
    tenant,
    user,
    shard: shardStub(tenant, 0),
    totalBytes: size * 3,
  };
}

async function readRmrfState(fixture: RmrfFixture) {
  const local = await runInDurableObject(
    fixture.user,
    async (_instance, state) => {
      const sql = state.storage.sql;
      const quota = sql
        .exec(
          `SELECT storage_used, file_count, inline_bytes_used
             FROM quota WHERE user_id = ?`,
          fixture.tenant
        )
        .toArray()[0] as {
        storage_used: number;
        file_count: number;
        inline_bytes_used: number;
      };
      return {
        files: (
          sql.exec("SELECT COUNT(*) AS n FROM files").toArray()[0] as {
            n: number;
          }
        ).n,
        fileChunks: (
          sql.exec("SELECT COUNT(*) AS n FROM file_chunks").toArray()[0] as {
            n: number;
          }
        ).n,
        intents: sql
          .exec(
            `SELECT ref_id, cleanup_kind, attempts
               FROM chunk_cleanup_intents
              ORDER BY ref_id`
          )
          .toArray() as Array<{
          ref_id: string;
          cleanup_kind: string;
          attempts: number;
        }>,
        quota,
      };
    }
  );
  const remote = await runInDurableObject(
    fixture.shard,
    async (_instance, state) => {
      const sql = state.storage.sql;
      return {
        refs: (
          sql.exec("SELECT COUNT(*) AS n FROM chunk_refs").toArray()[0] as {
            n: number;
          }
        ).n,
        refCount: (
          sql
            .exec("SELECT COALESCE(SUM(ref_count), 0) AS n FROM chunks")
            .toArray()[0] as { n: number }
        ).n,
        bytes: (
          sql
            .exec("SELECT COALESCE(SUM(size), 0) AS n FROM chunks")
            .toArray()[0] as { n: number }
        ).n,
      };
    }
  );
  return { ...local, ...remote };
}

async function reapShard(fixture: RmrfFixture): Promise<void> {
  await runInDurableObject(fixture.shard, async (_instance, state) => {
    state.storage.sql.exec(
      "UPDATE chunks SET deleted_at = 0 WHERE ref_count = 0"
    );
    await state.storage.setAlarm(Date.now() + 1_000);
  });
  expect(await runDurableObjectAlarm(fixture.shard)).toBe(true);
}

describe("multipart cleanup outbox", () => {
  it("keeps abort cleanup durable when deleteChunks fails before mutation", async () => {
    const fixture = await seedMultipart("multipart-abort-before");
    await fixture.shard.testConfigureDeleteChunksFailure(
      fixture.uploadId,
      "before",
      1
    );

    await expect(
      fixture.user.vfsAbortMultipart(
        { ns: NS, tenant: fixture.tenant },
        fixture.uploadId
      )
    ).resolves.toEqual({ ok: true });
    expect(await readMultipartState(fixture)).toMatchObject({
      status: "aborted",
      tempRows: 0,
      refs: 1,
      staging: 1,
      intents: [{ cleanup_kind: "multipart", attempts: 1 }],
    });

    await makeCleanupEligible(fixture.user);
    expect(await runDurableObjectAlarm(fixture.user)).toBe(true);
    expect(await readMultipartState(fixture)).toMatchObject({
      status: "aborted",
      refs: 0,
      staging: 0,
      intents: [],
    });
  });

  it("replays abort after a lost clear-staging response", async () => {
    const fixture = await seedMultipart("multipart-abort-after");
    await fixture.shard.testConfigureClearMultipartStagingFailure("after", 1);

    await expect(
      fixture.user.vfsAbortMultipart(
        { ns: NS, tenant: fixture.tenant },
        fixture.uploadId
      )
    ).resolves.toEqual({ ok: true });
    expect(await readMultipartState(fixture)).toMatchObject({
      status: "aborted",
      tempRows: 0,
      refs: 0,
      staging: 0,
      intents: [{ cleanup_kind: "multipart", attempts: 1 }],
    });

    await makeCleanupEligible(fixture.user);
    expect(await runDurableObjectAlarm(fixture.user)).toBe(true);
    expect(await readMultipartState(fixture)).toMatchObject({
      refs: 0,
      staging: 0,
      intents: [],
    });
    await expect(
      fixture.user.vfsAbortMultipart(
        { ns: NS, tenant: fixture.tenant },
        fixture.uploadId
      )
    ).resolves.toEqual({ ok: true });
  });

  it("replays abort after a lost deleteChunks response", async () => {
    const fixture = await seedMultipart("multipart-abort-delete-after");
    await fixture.shard.testConfigureDeleteChunksFailure(
      fixture.uploadId,
      "after",
      1
    );

    await expect(
      fixture.user.vfsAbortMultipart(
        { ns: NS, tenant: fixture.tenant },
        fixture.uploadId
      )
    ).resolves.toEqual({ ok: true });
    expect(await readMultipartState(fixture)).toMatchObject({
      status: "aborted",
      tempRows: 0,
      refs: 0,
      staging: 1,
      intents: [{ cleanup_kind: "multipart", attempts: 1 }],
    });

    await makeCleanupEligible(fixture.user);
    expect(await runDurableObjectAlarm(fixture.user)).toBe(true);
    expect(await readMultipartState(fixture)).toMatchObject({
      refs: 0,
      staging: 0,
      intents: [],
    });
  });

  it("recovers an expired session through a later alarm", async () => {
    const fixture = await seedMultipart("multipart-expiry-replay", true);
    await fixture.shard.testConfigureClearMultipartStagingFailure("before", 1);

    expect(await runDurableObjectAlarm(fixture.user)).toBe(true);
    expect(await readMultipartState(fixture)).toMatchObject({
      status: "aborted",
      tempRows: 0,
      refs: 0,
      staging: 1,
      intents: [{ cleanup_kind: "multipart", attempts: 1 }],
    });

    await makeCleanupEligible(fixture.user);
    expect(await runDurableObjectAlarm(fixture.user)).toBe(true);
    expect(await readMultipartState(fixture)).toMatchObject({
      status: "aborted",
      refs: 0,
      staging: 0,
      intents: [],
    });
  });

  it("fences an in-flight PUT before abort cleanup", async () => {
    const tenant = "multipart-abort-inflight-fence";
    const user = userStub(tenant);
    const scope = { ns: NS, tenant } as const;
    const data = new Uint8Array([8, 6, 7, 5, 3, 0, 9]);
    const begin = await user.vfsBeginMultipart(scope, "/inflight.bin", {
      size: data.byteLength,
      chunkSize: data.byteLength,
    });
    const shardIndex = placeChunk(tenant, begin.uploadId, 0, begin.poolSize);
    const shard = shardStub(tenant, shardIndex);
    await shard.testConfigurePutChunkBlock();
    const put = shard.putChunkMultipart(
      await hashChunk(data),
      data,
      begin.uploadId,
      0,
      tenant,
      begin.sessionToken
    );
    await shard.testWaitForPutChunkBlocked();

    await expect(user.vfsAbortMultipart(scope, begin.uploadId)).resolves.toEqual({
      ok: true,
    });
    await shard.testReleasePutChunkBlock();
    await expect(put).rejects.toThrow(/EBUSY: multipart upload is aborting/);

    const state = await readMultipartState({
      tenant,
      user,
      shard,
      uploadId: begin.uploadId,
    });
    expect(state).toMatchObject({
      status: "aborted",
      refs: 0,
      staging: 0,
      intents: [],
    });
  });
});

describe.each([
  { phase: "before" as const, expectedRefs: 3 },
  { phase: "after" as const, expectedRefs: 0 },
])("non-versioned rmrf cleanup outbox ($phase)", ({ phase, expectedRefs }) => {
  it("preserves paged routing until alarm replay acknowledges every file", async () => {
    const fixture = await seedRmrf(`rmrf-${phase}`);
    await fixture.shard.testConfigureAnyDeleteChunksFailure(phase, null);

    await expect(
      fixture.user.vfsRemoveRecursive(
        { ns: NS, tenant: fixture.tenant },
        "/tree"
      )
    ).resolves.toEqual({ done: true });
    const failed = await readRmrfState(fixture);
    expect(failed).toMatchObject({
      files: 0,
      fileChunks: 0,
      refs: expectedRefs,
      refCount: expectedRefs,
      bytes: fixture.totalBytes,
      quota: {
        storage_used: 0,
        file_count: 0,
        inline_bytes_used: 0,
      },
    });
    expect(failed.intents).toHaveLength(3);
    expect(
      failed.intents.every(
        (intent) => intent.cleanup_kind === "bulk" && intent.attempts === 1
      )
    ).toBe(true);

    await makeCleanupEligible(fixture.user);
    await fixture.shard.testClearDeleteChunksFailure();
    expect(await runDurableObjectAlarm(fixture.user)).toBe(true);
    expect(await readRmrfState(fixture)).toMatchObject({
      files: 0,
      fileChunks: 0,
      intents: [],
      refs: 0,
      refCount: 0,
      bytes: fixture.totalBytes,
      quota: {
        storage_used: 0,
        file_count: 0,
        inline_bytes_used: 0,
      },
    });

    await reapShard(fixture);
    expect((await fixture.shard.getStorageBytes()).bytesStored).toBe(0);
  });
});
