import {
  env,
  runDurableObjectAlarm,
  runInDurableObject,
} from "cloudflare:test";
import { describe, expect, it } from "vitest";

import { vfsShardDOName, vfsUserDOName } from "@core/lib/utils";
import type { ShardDO } from "@core/objects/shard/shard-do";
import { hashChunk } from "@shared/crypto";
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
  testConfigureDeleteChunksFailure(
    fileId: string,
    phase: DeleteChunksFailurePhase,
    remaining: number | null
  ): Promise<void>;
  testClearDeleteChunksFailure(): Promise<void>;
  testConfigureRestoreChunkRefBlock(): Promise<void>;
  testWaitForRestoreChunkRefBlocked(): Promise<void>;
  testReleaseRestoreChunkRefBlock(): Promise<void>;
  testConfigureMultipartManifestBlock(uploadId: string): Promise<void>;
  testWaitForMultipartManifestBlocked(): Promise<void>;
  testReleaseMultipartManifestBlock(): Promise<void>;
}

type TestShardDO = ShardDO & FaultControls;

interface UserFaultControls {
  testConfigureMaintenanceAlarmFailure(remaining: number): Promise<void>;
  testDropVersionRows(
    scope: { ns: string; tenant: string; sub?: string },
    userId: string,
    pathId: string,
    versionIds: string[]
  ): Promise<number>;
}

type TestUserDO = UserDO & UserFaultControls;

interface TestEnv {
  MOSSAIC_USER: DurableObjectNamespace<TestUserDO>;
  MOSSAIC_SHARD: DurableObjectNamespace<TestShardDO>;
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

function chunkedBytes(fill: number): Uint8Array {
  return new Uint8Array(32 * 1024).fill(fill);
}

async function readUserState(tenant: string): Promise<{
  files: Record<string, SqlStorageValue>[];
  versions: Record<string, SqlStorageValue>[];
  versionChunks: Record<string, SqlStorageValue>[];
  intents: Array<{ ref_id: string; shard_index: number; attempts: number }>;
  quota: Record<string, SqlStorageValue>;
  revision: number;
}> {
  return runInDurableObject(userStub(tenant), async (_instance, state) => {
    const sql = state.storage.sql;
    return {
      files: sql
        .exec(
          `SELECT file_id, file_name, status, head_version_id, metadata
             FROM files ORDER BY file_id`
        )
        .toArray(),
      versions: sql
        .exec(
          `SELECT path_id, version_id, size, metadata
             FROM file_versions ORDER BY version_id`
        )
        .toArray(),
      versionChunks: sql
        .exec(
          `SELECT version_id, chunk_index, chunk_hash, shard_index
             FROM version_chunks ORDER BY version_id, chunk_index`
        )
        .toArray(),
      intents: sql
        .exec(
          `SELECT ref_id, shard_index, attempts
             FROM chunk_cleanup_intents ORDER BY ref_id, shard_index`
        )
        .toArray() as Array<{
        ref_id: string;
        shard_index: number;
        attempts: number;
      }>,
      quota: sql
        .exec(
          `SELECT storage_used, file_count, inline_bytes_used
             FROM quota WHERE user_id = ?`,
          tenant
        )
        .toArray()[0]!,
      revision:
        (
          sql
            .exec(
              "SELECT revision FROM root_folder_revision WHERE user_id = ?",
              tenant
            )
            .toArray()[0] as { revision: number } | undefined
        )?.revision ?? 0,
    };
  });
}

async function readShardRefs(
  tenant: string,
  shardIndex: number
): Promise<Array<{ chunk_hash: string; file_id: string; ref_count: number }>> {
  const stub = shardStub(tenant, shardIndex);
  await stub.fetch(new Request("http://internal/stats"));
  return runInDurableObject(stub, async (_instance, state) => {
    return state.storage.sql
      .exec(
        `SELECT r.chunk_hash, r.file_id, c.ref_count
           FROM chunk_refs r
           JOIN chunks c ON c.hash = r.chunk_hash
          ORDER BY r.chunk_hash, r.file_id`
      )
      .toArray() as Array<{
      chunk_hash: string;
      file_id: string;
      ref_count: number;
    }>;
  });
}

async function sourceShard(
  tenant: string,
  fileName = "src.bin"
): Promise<number> {
  return runInDurableObject(userStub(tenant), async (_instance, state) => {
    return (
      state.storage.sql
        .exec(
          `SELECT vc.shard_index
             FROM files f
             JOIN version_chunks vc ON vc.version_id = f.head_version_id
             WHERE f.file_name = ?`,
          fileName
        )
        .toArray()[0] as { shard_index: number }
    ).shard_index;
  });
}

async function readVersionRoutes(tenant: string, fileName: string): Promise<
  Array<{
    pathId: string;
    headVersionId: string;
    versionId: string;
    refId: string;
    shardIndex: number | null;
  }>
> {
  return runInDurableObject(userStub(tenant), async (_instance, state) => {
    return state.storage.sql
      .exec(
        `SELECT f.file_id AS path_id, f.head_version_id,
                fv.version_id,
                COALESCE(fv.shard_ref_id, f.file_id || '#' || fv.version_id) AS ref_id,
                MIN(vc.shard_index) AS shard_index
           FROM files f
           JOIN file_versions fv ON fv.path_id = f.file_id
           LEFT JOIN version_chunks vc ON vc.version_id = fv.version_id
          WHERE f.file_name = ?
          GROUP BY f.file_id, f.head_version_id, fv.version_id, fv.shard_ref_id,
                   fv.mtime_ms
          ORDER BY fv.mtime_ms, fv.version_id`,
        fileName
      )
      .toArray()
      .map((row) => ({
        pathId: row.path_id as string,
        headVersionId: row.head_version_id as string,
        versionId: row.version_id as string,
        refId: row.ref_id as string,
        shardIndex: row.shard_index as number | null,
      }));
  });
}

describe("versioned publication failure atomicity", () => {
  it("keeps a new chunked path hidden until its shard upload completes", async () => {
    const tenant = "versioned-write-new-path-visibility";
    const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });
    const payload = chunkedBytes(0x21);
    const shardIndex = placeChunk(
      tenant,
      await hashChunk(payload),
      0,
      32
    );
    const shard = shardStub(tenant, shardIndex);
    await shard.testConfigurePutChunkBlock();

    const write = vfs.writeFile("/new.bin", payload);
    await shard.testWaitForPutChunkBlocked();

    const inFlight = await runInDurableObject(
      userStub(tenant),
      async (_instance, state) => {
        const sql = state.storage.sql;
        return {
          live: (
            sql
              .exec(
                "SELECT COUNT(*) AS n FROM files WHERE file_name = 'new.bin' AND status = 'complete'"
              )
              .toArray()[0] as { n: number }
          ).n,
          uploading: (
            sql
              .exec(
                "SELECT COUNT(*) AS n FROM files WHERE file_name LIKE '_vfs_tmp_%' AND status = 'uploading'"
              )
              .toArray()[0] as { n: number }
          ).n,
          versions: (
            sql.exec("SELECT COUNT(*) AS n FROM file_versions").toArray()[0] as {
              n: number;
            }
          ).n,
          versionChunks: (
            sql.exec("SELECT COUNT(*) AS n FROM version_chunks").toArray()[0] as {
              n: number;
            }
          ).n,
          intents: (
            sql
              .exec("SELECT COUNT(*) AS n FROM chunk_cleanup_intents")
              .toArray()[0] as { n: number }
          ).n,
        };
      }
    );
    expect(inFlight).toEqual({
      live: 0,
      uploading: 1,
      versions: 0,
      versionChunks: 0,
      intents: 1,
    });

    await shard.testReleasePutChunkBlock();
    await write;
    expect(await vfs.readFile("/new.bin")).toEqual(payload);
    expect((await readUserState(tenant)).intents).toEqual([]);
  });

  it("never exposes a headless new path and removes synthetic refs when version insertion fails", async () => {
    const tenant = "versioned-write-new-path-failure";
    const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });
    const stub = userStub(tenant);
    await vfs.exists("/init");
    await runInDurableObject(stub, async (_instance, state) => {
      state.storage.sql.exec(`
        CREATE TRIGGER fail_new_versioned_write
        BEFORE INSERT ON file_versions
        BEGIN
          SELECT RAISE(ABORT, 'injected versioned write publication failure');
        END
      `);
    });

    await expect(
      vfs.writeFile("/new.bin", chunkedBytes(0x31))
    ).rejects.toThrow(/injected versioned write publication failure/);

    const local = await readUserState(tenant);
    expect(local.files).toEqual([]);
    expect(local.versions).toEqual([]);
    expect(local.versionChunks).toEqual([]);
    expect(local.intents).toEqual([]);
    const refs = await Promise.all(
      Array.from({ length: 32 }, (_, shardIndex) =>
        readShardRefs(tenant, shardIndex)
      )
    );
    expect(refs.flat()).toEqual([]);
  });

  it("preserves the prior head, metadata, tags, accounting, and revision on publication failure", async () => {
    const tenant = "versioned-write-existing-failure";
    const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });
    const stub = userStub(tenant);
    const prior = chunkedBytes(0x41);
    await vfs.writeFile("/stable.bin", prior, {
      metadata: { generation: 1 },
      tags: ["stable"],
    });
    const before = await readUserState(tenant);
    await runInDurableObject(stub, async (_instance, state) => {
      state.storage.sql.exec(`
        CREATE TRIGGER fail_existing_versioned_write
        BEFORE INSERT ON file_versions
        BEGIN
          SELECT RAISE(ABORT, 'injected existing version publication failure');
        END
      `);
    });

    await expect(
      vfs.writeFile("/stable.bin", chunkedBytes(0x42), {
        metadata: { generation: 2 },
        tags: ["replacement"],
      })
    ).rejects.toThrow(/injected existing version publication failure/);

    expect(await vfs.readFile("/stable.bin")).toEqual(prior);
    expect(await readUserState(tenant)).toEqual(before);
  });
});

describe("chunked versioned copy synthetic-ref cleanup", () => {
  it("cleans a no-op retain failure before shard mutation", async () => {
    const tenant = "versioned-copy-put-before";
    const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });
    await vfs.writeFile("/src.bin", chunkedBytes(0x51));
    const shardIndex = await sourceShard(tenant);
    const shard = shardStub(tenant, shardIndex);
    const before = await readShardRefs(tenant, shardIndex);
    await shard.testConfigurePutChunkFailure("before", 1);

    await expect(vfs.copyFile("/src.bin", "/dest.bin")).rejects.toThrow(
      /injected putChunk failure before mutation/
    );

    expect(await vfs.exists("/dest.bin")).toBe(false);
    expect(await readShardRefs(tenant, shardIndex)).toEqual(before);
    expect((await readUserState(tenant)).intents).toEqual([]);
  });

  it("durably retains cleanup intent after a lost retain response and drains it idempotently", async () => {
    const tenant = "versioned-copy-put-after";
    const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });
    await vfs.writeFile("/src.bin", chunkedBytes(0x61));
    const shardIndex = await sourceShard(tenant);
    const shard = shardStub(tenant, shardIndex);
    const before = await readShardRefs(tenant, shardIndex);
    await shard.testConfigurePutChunkFailure("after", 1);
    await shard.testConfigureAnyDeleteChunksFailure("before", 1);

    await expect(vfs.copyFile("/src.bin", "/dest.bin")).rejects.toThrow(
      /injected putChunk response loss after mutation/
    );

    expect(await vfs.exists("/dest.bin")).toBe(false);
    const failedState = await readUserState(tenant);
    expect(failedState.intents).toHaveLength(1);
    expect(failedState.intents[0]).toMatchObject({
      shard_index: shardIndex,
      attempts: 1,
    });
    const leakedRefId = failedState.intents[0]!.ref_id;
    expect(
      (await readShardRefs(tenant, shardIndex)).some(
        (row) => row.file_id === leakedRefId
      )
    ).toBe(true);

    await shard.testClearPutChunkFailure();
    await shard.testClearDeleteChunksFailure();
    await runInDurableObject(userStub(tenant), async (_instance, state) => {
      state.storage.sql.exec(
        "UPDATE chunk_cleanup_intents SET next_attempt_at = 0 WHERE ref_id = ?",
        leakedRefId
      );
    });
    expect(await runDurableObjectAlarm(userStub(tenant))).toBe(true);
    expect((await readUserState(tenant)).intents).toEqual([]);
    expect(await readShardRefs(tenant, shardIndex)).toEqual(before);
  });
});

describe("stale version publication", () => {
  it("preserves a source overwrite that races versioned rename", async () => {
    const tenant = "stale-rename-source-overwrite";
    const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });
    const sourceBefore = chunkedBytes(0x81);
    const sourceAfter = chunkedBytes(0x82);
    const destinationBefore = chunkedBytes(0x83);
    await vfs.writeFile("/src.bin", sourceBefore);
    await vfs.writeFile("/dest.bin", destinationBefore);
    const shard = shardStub(tenant, await sourceShard(tenant));
    await shard.testConfigurePutChunkBlock();

    const rename = vfs.rename("/src.bin", "/dest.bin");
    await shard.testWaitForPutChunkBlocked();
    await vfs.writeFile("/src.bin", sourceAfter);
    await shard.testReleasePutChunkBlock();

    await expect(rename).rejects.toThrow(/EBUSY|changed during publication/);
    expect(await vfs.readFile("/src.bin")).toEqual(sourceAfter);
    expect(await vfs.readFile("/dest.bin")).toEqual(destinationBefore);
    expect((await readUserState(tenant)).intents).toEqual([]);
  });

  it("does not recreate a source purged while copy retain is blocked", async () => {
    const tenant = "stale-copy-source-purge";
    const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });
    await vfs.writeFile("/src.bin", chunkedBytes(0x84));
    const shard = shardStub(tenant, await sourceShard(tenant));
    await shard.testConfigurePutChunkBlock();

    const copy = vfs.copyFile("/src.bin", "/dest.bin");
    await shard.testWaitForPutChunkBlocked();
    await vfs.purge("/src.bin");
    await shard.testReleasePutChunkBlock();

    await expect(copy).rejects.toThrow();
    expect(await vfs.exists("/src.bin")).toBe(false);
    expect(await vfs.exists("/dest.bin")).toBe(false);
    expect((await readUserState(tenant)).intents).toEqual([]);
  });

  it("preserves a destination overwrite that races chunked copy", async () => {
    const tenant = "stale-copy-destination-overwrite";
    const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });
    const source = chunkedBytes(0x85);
    const destinationAfter = chunkedBytes(0x86);
    await vfs.writeFile("/src.bin", source);
    await vfs.writeFile("/dest.bin", chunkedBytes(0x87));
    const shard = shardStub(tenant, await sourceShard(tenant));
    await shard.testConfigurePutChunkBlock();

    const copy = vfs.copyFile("/src.bin", "/dest.bin");
    await shard.testWaitForPutChunkBlocked();
    await vfs.writeFile("/dest.bin", destinationAfter);
    await shard.testReleasePutChunkBlock();

    await expect(copy).rejects.toThrow(/EBUSY|changed during publication/);
    expect(await vfs.readFile("/src.bin")).toEqual(source);
    expect(await vfs.readFile("/dest.bin")).toEqual(destinationAfter);
    expect((await readUserState(tenant)).intents).toEqual([]);
  });

  it("preserves multipart abort while manifest collection is blocked", async () => {
    const tenant = "stale-multipart-abort";
    const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });
    const payload = chunkedBytes(0x88);
    const handle = await vfs.beginMultipartUpload("/dest.bin", {
      size: payload.byteLength,
      chunkSize: payload.byteLength,
    });
    const put = await vfs.putMultipartChunk(handle, 0, payload);
    const shardIndex = placeChunk(
      tenant,
      handle.uploadId,
      0,
      handle.poolSize
    );
    const shard = shardStub(tenant, shardIndex);
    await shard.testConfigureMultipartManifestBlock(handle.uploadId);

    const finalize = vfs.finalizeMultipartUpload(handle, [put.chunkHash]);
    await shard.testWaitForMultipartManifestBlocked();
    await runInDurableObject(userStub(tenant), async (_instance, state) => {
      state.storage.sql.exec(
        "UPDATE upload_sessions SET status = 'aborted' WHERE upload_id = ?",
        handle.uploadId
      );
    });
    await shard.testReleaseMultipartManifestBlock();

    await expect(finalize).rejects.toThrow(/EBUSY|session changed/);
    expect(await vfs.exists("/dest.bin")).toBe(false);
    const state = await readUserState(tenant);
    expect(state.versions).toEqual([]);
    expect(state.versionChunks).toEqual([]);
    expect(state.intents).toEqual([]);
  });

  it("preserves a newer head while restore ref creation is blocked", async () => {
    const tenant = "stale-restore-head-advance";
    const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });
    const historical = chunkedBytes(0x89);
    const newer = chunkedBytes(0x8a);
    await vfs.writeFile("/history.bin", historical);
    await vfs.writeFile("/history.bin", chunkedBytes(0x8b));
    const [sourceVersion] = await readVersionRoutes(tenant, "history.bin");
    const shard = shardStub(tenant, sourceVersion!.shardIndex!);
    await shard.testConfigureRestoreChunkRefBlock();

    const restore = vfs.restoreVersion(
      "/history.bin",
      sourceVersion!.versionId
    );
    await shard.testWaitForRestoreChunkRefBlocked();
    await vfs.writeFile("/history.bin", newer);
    await shard.testReleaseRestoreChunkRefBlock();

    await expect(restore).rejects.toThrow(/EBUSY|changed during publication/);
    expect(await vfs.readFile("/history.bin")).toEqual(newer);
    expect(await vfs.listVersions("/history.bin")).toHaveLength(3);
    expect((await readUserState(tenant)).intents).toEqual([]);
  });

  it("does not delete a stream row that was already published", async () => {
    const tenant = "stream-abort-after-publication";
    const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });
    const stub = userStub(tenant);
    const scope = { ns: NS, tenant };
    const payload = chunkedBytes(0x8c);
    const handle = await stub.vfsBeginWriteStream(scope, "/stream.bin");
    await stub.vfsAppendWriteStream(scope, handle, 0, payload);
    await stub.vfsCommitWriteStream(scope, handle);

    await stub.vfsAbortWriteStream(scope, handle);

    expect(await vfs.readFile("/stream.bin")).toEqual(payload);
    expect((await readUserState(tenant)).intents).toEqual([]);
  });
});

describe("version retention cleanup outbox", () => {
  for (const phase of ["before", "after"] as const) {
    it(`commits local retention state and replays ${phase}-mutation response loss`, async () => {
      const tenant = `drop-version-${phase}-response-loss`;
      const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });
      const first = chunkedBytes(0x71);
      const second = chunkedBytes(0x72);
      await vfs.writeFile("/history.bin", first);
      await vfs.writeFile("/history.bin", second);
      const [oldVersion, headVersion] = await readVersionRoutes(
        tenant,
        "history.bin"
      );
      expect(oldVersion?.shardIndex).not.toBeNull();
      const shard = shardStub(tenant, oldVersion!.shardIndex!);
      await shard.testConfigureDeleteChunksFailure(
        oldVersion!.refId,
        phase,
        1
      );

      await expect(
        vfs.dropVersions("/history.bin", { keepLast: 1 })
      ).resolves.toEqual({ dropped: 1, kept: 1 });

      const local = await readUserState(tenant);
      expect(local.versions.map((row) => row.version_id)).toEqual([
        headVersion!.versionId,
      ]);
      expect(local.files[0]?.head_version_id).toBe(headVersion!.versionId);
      expect(local.quota).toMatchObject({
        storage_used: second.byteLength,
        file_count: 1,
        inline_bytes_used: 0,
      });
      expect(local.intents).toEqual([
        {
          ref_id: oldVersion!.refId,
          shard_index: oldVersion!.shardIndex,
          attempts: 1,
        },
      ]);
      expect(
        (await readShardRefs(tenant, oldVersion!.shardIndex!)).some(
          (row) => row.file_id === oldVersion!.refId
        )
      ).toBe(phase === "before");

      await shard.testClearDeleteChunksFailure();
      await runInDurableObject(userStub(tenant), async (_instance, state) => {
        state.storage.sql.exec(
          "UPDATE chunk_cleanup_intents SET next_attempt_at = 0 WHERE ref_id = ?",
          oldVersion!.refId
        );
      });
      expect(await runDurableObjectAlarm(userStub(tenant))).toBe(true);
      expect((await readUserState(tenant)).intents).toEqual([]);
      expect(
        (await readShardRefs(tenant, oldVersion!.shardIndex!)).some(
          (row) => row.file_id === oldVersion!.refId
        )
      ).toBe(false);
    });
  }

  it("does not mutate versions when maintenance cannot be armed", async () => {
    const tenant = "drop-version-alarm-first";
    const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });
    await vfs.writeFile("/history.bin", chunkedBytes(0x73));
    await vfs.writeFile("/history.bin", chunkedBytes(0x74));
    const before = await readUserState(tenant);
    await userStub(tenant).testConfigureMaintenanceAlarmFailure(1);

    await expect(
      vfs.dropVersions("/history.bin", { keepLast: 1 })
    ).rejects.toThrow(/injected maintenance alarm failure/);
    expect(await readUserState(tenant)).toEqual(before);
  });

  it("repairs a removed head and keeps accounting aligned", async () => {
    const tenant = "drop-version-head-repair";
    const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });
    const first = chunkedBytes(0x75);
    const second = chunkedBytes(0x76);
    await vfs.writeFile("/history.bin", first);
    await vfs.writeFile("/history.bin", second);
    const [oldVersion, headVersion] = await readVersionRoutes(
      tenant,
      "history.bin"
    );

    await userStub(tenant).testDropVersionRows(
      { ns: NS, tenant },
      tenant,
      headVersion!.pathId,
      [headVersion!.versionId]
    );

    expect(await vfs.readFile("/history.bin")).toEqual(first);
    const state = await readUserState(tenant);
    expect(state.files[0]?.head_version_id).toBe(oldVersion!.versionId);
    expect(state.quota).toMatchObject({
      storage_used: first.byteLength,
      file_count: 1,
      inline_bytes_used: 0,
    });
  });

  it("keeps a surviving tombstone as the head", async () => {
    const tenant = "drop-version-tombstone-head";
    const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });
    await vfs.writeFile("/gone.bin", chunkedBytes(0x77));
    await vfs.unlink("/gone.bin");
    const routes = await readVersionRoutes(tenant, "gone.bin");
    const tombstone = routes.find(
      (route) => route.versionId === route.headVersionId
    );
    const live = routes.find(
      (route) => route.versionId !== route.headVersionId
    );

    await userStub(tenant).testDropVersionRows(
      { ns: NS, tenant },
      tenant,
      tombstone!.pathId,
      [live!.versionId]
    );

    const state = await readUserState(tenant);
    expect(state.files[0]?.head_version_id).toBe(tombstone!.versionId);
    expect(state.quota).toMatchObject({
      storage_used: 0,
      file_count: 0,
      inline_bytes_used: 0,
    });
    expect(await vfs.exists("/gone.bin")).toBe(false);
  });
});

describe("stale upload alarm failures", () => {
  it("reschedules maintenance when a stale temporary row cannot be deleted", async () => {
    const tenant = "stale-upload-reschedule";
    const vfs = createVFS(envFor(), { tenant });
    const stub = userStub(tenant);
    await vfs.exists("/init");
    await runInDurableObject(stub, async (_instance, state) => {
      state.storage.sql.exec(
        `INSERT INTO files
           (file_id, user_id, parent_id, file_name, file_size, file_hash,
            mime_type, chunk_size, chunk_count, pool_size, status, created_at,
            updated_at, mode, node_kind)
         VALUES ('stale-failing-tmp', ?, NULL, '_vfs_tmp_stale-failing-tmp',
                 1, '', 'application/octet-stream', 1, 0, 32, 'uploading',
                 ?, ?, 420, 'file')`,
        tenant,
        Date.now() - 2 * 60 * 60 * 1000,
        Date.now() - 2 * 60 * 60 * 1000
      );
      state.storage.sql.exec(`
        CREATE TRIGGER fail_stale_tmp_delete
        BEFORE DELETE ON files
        WHEN OLD.file_id = 'stale-failing-tmp'
        BEGIN
          SELECT RAISE(ABORT, 'injected stale tmp delete failure');
        END
      `);
      await state.storage.setAlarm(Date.now() - 1);
    });

    await runDurableObjectAlarm(stub);
    const alarm = await runInDurableObject(stub, (_instance, state) =>
      state.storage.getAlarm()
    );
    expect(alarm).not.toBeNull();
    expect(alarm).toBeGreaterThan(Date.now());
  });
});
