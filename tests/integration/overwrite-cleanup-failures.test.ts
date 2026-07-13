import { env, runDurableObjectAlarm, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";

import { vfsShardDOName, vfsUserDOName } from "@core/lib/utils";
import type { ShardDO } from "@core/objects/shard/shard-do";
import { hashChunk } from "@shared/crypto";
import {
  createVFS,
  type MossaicEnv,
  type UserDO,
} from "../../sdk/src/index";
import type { DeleteChunksFailurePhase } from "../test-worker";

interface FaultControls {
  testConfigureDeleteChunksFailure(
    fileId: string,
    phase: DeleteChunksFailurePhase,
    remaining: number | null
  ): Promise<void>;
  testClearDeleteChunksFailure(): Promise<void>;
  testConfigureDeleteChunksBlock(fileId: string): Promise<void>;
  testWaitForDeleteChunksBlocked(): Promise<void>;
  testReleaseDeleteChunksBlock(): Promise<void>;
  testConfigureDeleteChunksConcurrencyProbe(target: number): Promise<void>;
  testWaitForDeleteChunksConcurrency(): Promise<void>;
  testReadDeleteChunksMaxConcurrency(): Promise<number>;
  testReleaseDeleteChunksConcurrencyProbe(): Promise<void>;
}

interface UserFaultControls {
  testConfigureMaintenanceAlarmFailure(remaining: number): Promise<void>;
}

type TestShardDO = ShardDO & FaultControls;
type TestUserDO = UserDO & UserFaultControls;

interface TestEnv {
  MOSSAIC_USER: DurableObjectNamespace<TestUserDO>;
  MOSSAIC_SHARD: DurableObjectNamespace<TestShardDO>;
}

interface ChunkLocation {
  fileId: string;
  hash: string;
  shardIndex: number;
}

interface ShardRefState {
  refCount: number | null;
  fileIds: string[];
}

interface CleanupIntent {
  refId: string;
  shardIndex: number;
  createdAt: number;
  updatedAt: number;
  nextAttemptAt: number;
  attempts: number;
  lastError: string | null;
}

interface LocalPublicationState {
  files: Array<{
    file_id: string;
    file_name: string;
    file_size: number;
    file_hash: string;
    chunk_size: number;
    chunk_count: number;
    status: string;
    deleted_at: number | null;
  }>;
  chunks: Array<{
    file_id: string;
    chunk_index: number;
    chunk_hash: string;
    chunk_size: number;
    shard_index: number;
  }>;
  quota: {
    storage_used: number;
    file_count: number;
    inline_bytes_used: number;
  };
  rootRevision: number;
  renameAuditCount: number;
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

function userStubFor(tenant: string): DurableObjectStub<TestUserDO> {
  return E.MOSSAIC_USER.get(
    E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
  );
}

function shardStubFor(
  tenant: string,
  shardIndex: number
): DurableObjectStub<TestShardDO> {
  const shardName = vfsShardDOName(NS, tenant, undefined, shardIndex);
  return E.MOSSAIC_SHARD.get(E.MOSSAIC_SHARD.idFromName(shardName));
}

function bytes(fill: number): Uint8Array {
  return new Uint8Array(32 * 1024).fill(fill);
}

async function readFileOrNull(
  vfs: ReturnType<typeof createVFS>,
  path: string
): Promise<Uint8Array | null> {
  try {
    return await vfs.readFile(path);
  } catch {
    return null;
  }
}

async function fingerprint(
  data: Uint8Array | null
): Promise<{ byteLength: number; hash: string } | null> {
  if (data === null) return null;
  return { byteLength: data.byteLength, hash: await hashChunk(data) };
}

async function readChunkLocation(
  tenant: string,
  fileName: string
): Promise<ChunkLocation> {
  return runInDurableObject(userStubFor(tenant), async (_instance, state) => {
    const row = state.storage.sql
      .exec(
        `SELECT f.file_id, c.chunk_hash, c.shard_index
           FROM files f
           JOIN file_chunks c ON c.file_id = f.file_id
          WHERE f.user_id = ? AND f.file_name = ? AND f.status = 'complete'`,
        tenant,
        fileName
      )
      .toArray()[0] as
      | { file_id: string; chunk_hash: string; shard_index: number }
      | undefined;
    if (!row) throw new Error(`missing chunked file: ${fileName}`);
    return {
      fileId: row.file_id,
      hash: row.chunk_hash,
      shardIndex: row.shard_index,
    };
  });
}

async function readShardRefState(
  stub: DurableObjectStub<TestShardDO>,
  hash: string
): Promise<ShardRefState> {
  return runInDurableObject(stub, async (_instance, state) => {
    const chunk = state.storage.sql
      .exec("SELECT ref_count FROM chunks WHERE hash = ?", hash)
      .toArray()[0] as { ref_count: number } | undefined;
    const refs = state.storage.sql
      .exec(
        "SELECT file_id FROM chunk_refs WHERE chunk_hash = ? ORDER BY file_id",
        hash
      )
      .toArray() as { file_id: string }[];
    return {
      refCount: chunk?.ref_count ?? null,
      fileIds: refs.map((row) => row.file_id),
    };
  });
}

async function readCleanupIntents(
  tenant: string,
  refId: string
): Promise<CleanupIntent[]> {
  return runInDurableObject(userStubFor(tenant), async (_instance, state) => {
    const rows = state.storage.sql
      .exec(
        `SELECT ref_id, shard_index, created_at, updated_at, next_attempt_at,
                attempts, last_error
           FROM chunk_cleanup_intents
          WHERE ref_id = ?
          ORDER BY shard_index`,
        refId
      )
      .toArray() as {
        ref_id: string;
        shard_index: number;
        created_at: number;
        updated_at: number;
        next_attempt_at: number;
        attempts: number;
        last_error: string | null;
      }[];
    return rows.map((row) => ({
      refId: row.ref_id,
      shardIndex: row.shard_index,
      createdAt: row.created_at,
      updatedAt: row.updated_at,
      nextAttemptAt: row.next_attempt_at,
      attempts: row.attempts,
      lastError: row.last_error,
    }));
  });
}

async function makeCleanupIntentEligible(
  tenant: string,
  refId: string
): Promise<void> {
  await runInDurableObject(userStubFor(tenant), async (_instance, state) => {
    state.storage.sql.exec(
      "UPDATE chunk_cleanup_intents SET next_attempt_at = 0 WHERE ref_id = ?",
      refId
    );
  });
}

async function readAllCleanupIntentIds(tenant: string): Promise<string[]> {
  return runInDurableObject(userStubFor(tenant), async (_instance, state) => {
    const rows = state.storage.sql
      .exec("SELECT ref_id FROM chunk_cleanup_intents ORDER BY ref_id")
      .toArray() as { ref_id: string }[];
    return rows.map((row) => row.ref_id);
  });
}

async function readTenantShardRefIds(tenant: string): Promise<string[]> {
  const ids = await Promise.all(
    Array.from({ length: 32 }, async (_, shardIndex) => {
      const stub = shardStubFor(tenant, shardIndex);
      await stub.fetch(new Request("http://internal/stats"));
      return runInDurableObject(stub, async (_instance, state) => {
        const rows = state.storage.sql
          .exec(
            "SELECT file_id FROM chunk_refs WHERE user_id = ? ORDER BY file_id",
            tenant
          )
          .toArray() as { file_id: string }[];
        return rows.map((row) => row.file_id);
      });
    })
  );
  return ids.flat().sort();
}

async function readCommittedPathState(tenant: string, fileName: string) {
  return runInDurableObject(userStubFor(tenant), async (_instance, state) => {
    const file = state.storage.sql
      .exec(
        `SELECT file_id, file_size, metadata, encryption_mode, encryption_key_id
           FROM files
          WHERE user_id = ? AND file_name = ? AND status = 'complete'`,
        tenant,
        fileName
      )
      .toArray()[0] as {
      file_id: string;
      file_size: number;
      metadata: ArrayBuffer | null;
      encryption_mode: string | null;
      encryption_key_id: string | null;
    };
    const tags = state.storage.sql
      .exec("SELECT tag FROM file_tags WHERE path_id = ? ORDER BY tag", file.file_id)
      .toArray() as { tag: string }[];
    const quota = state.storage.sql
      .exec(
        `SELECT storage_used, file_count, COALESCE(inline_bytes_used, 0) AS inline_bytes_used
           FROM quota WHERE user_id = ?`,
        tenant
      )
      .toArray()[0] as LocalPublicationState["quota"];
    const revision = (
      state.storage.sql
        .exec(
          "SELECT revision FROM root_folder_revision WHERE user_id = ?",
          tenant
        )
        .toArray()[0] as { revision: number }
    ).revision;
    return {
      fileId: file.file_id,
      size: file.file_size,
      metadata:
        file.metadata === null
          ? null
          : JSON.parse(new TextDecoder().decode(new Uint8Array(file.metadata))),
      tags: tags.map((row) => row.tag),
      encryptionMode: file.encryption_mode,
      encryptionKeyId: file.encryption_key_id,
      quota,
      revision,
    };
  });
}

async function readLocalPublicationState(
  tenant: string
): Promise<LocalPublicationState> {
  return runInDurableObject(userStubFor(tenant), async (_instance, state) => {
    const files = state.storage.sql
      .exec(
        `SELECT file_id, file_name, file_size, file_hash, chunk_size,
                chunk_count, status, deleted_at
           FROM files WHERE user_id = ? ORDER BY file_id`,
        tenant
      )
      .toArray() as LocalPublicationState["files"];
    const chunks = state.storage.sql
      .exec(
        `SELECT file_id, chunk_index, chunk_hash, chunk_size, shard_index
           FROM file_chunks
          ORDER BY file_id, chunk_index`
      )
      .toArray() as LocalPublicationState["chunks"];
    const quota = state.storage.sql
      .exec(
        `SELECT storage_used, file_count, COALESCE(inline_bytes_used, 0) AS inline_bytes_used
           FROM quota WHERE user_id = ?`,
        tenant
      )
      .toArray()[0] as LocalPublicationState["quota"];
    const rootRevision = (
      state.storage.sql
        .exec(
          "SELECT revision FROM root_folder_revision WHERE user_id = ?",
          tenant
        )
        .toArray()[0] as { revision: number }
    ).revision;
    const renameAuditCount = (
      state.storage.sql
        .exec("SELECT COUNT(*) AS count FROM audit_log WHERE op = 'rename'")
        .toArray()[0] as { count: number }
    ).count;
    return { files, chunks, quota, rootRevision, renameAuditCount };
  });
}

describe("overwrite cleanup failures at the UserDO/ShardDO boundary", () => {
  it("keeps unlink successful while shard cleanup remains retryable", async () => {
    const tenant = "unlink-cleanup-before";
    const vfs = createVFS(envFor(), { tenant });

    await vfs.writeFile("/gone.bin", bytes(0x09));
    const removed = await readChunkLocation(tenant, "gone.bin");
    const shard = shardStubFor(tenant, removed.shardIndex);
    await shard.testConfigureDeleteChunksFailure(
      removed.fileId,
      "before",
      null
    );

    await vfs.unlink("/gone.bin");

    expect(await vfs.exists("/gone.bin")).toBe(false);
    expect(await readShardRefState(shard, removed.hash)).toEqual({
      refCount: 1,
      fileIds: [removed.fileId],
    });
    expect(await readCleanupIntents(tenant, removed.fileId)).toMatchObject([
      {
        refId: removed.fileId,
        shardIndex: removed.shardIndex,
        attempts: 1,
      },
    ]);

    await makeCleanupIntentEligible(tenant, removed.fileId);
    expect(await runDurableObjectAlarm(userStubFor(tenant))).toBe(true);
    const retried = await readCleanupIntents(tenant, removed.fileId);
    expect(retried).toHaveLength(1);
    expect(retried[0]?.attempts).toBe(2);
    expect(retried[0]?.nextAttemptAt).toBeGreaterThan(Date.now());
    expect(
      await runInDurableObject(userStubFor(tenant), (_instance, state) =>
        state.storage.getAlarm()
      )
    ).toBeGreaterThan(Date.now());

    await shard.testClearDeleteChunksFailure();
    await makeCleanupIntentEligible(tenant, removed.fileId);
    expect(await runDurableObjectAlarm(userStubFor(tenant))).toBe(true);
    expect(await readCleanupIntents(tenant, removed.fileId)).toEqual([]);
    expect(await readShardRefState(shard, removed.hash)).toEqual({
      refCount: 0,
      fileIds: [],
    });
  });

  it("keeps new bytes visible and durably recovers cleanup after deleteChunks fails before mutation", async () => {
    const tenant = "overwrite-cleanup-before";
    const vfs = createVFS(envFor(), { tenant });
    const oldBytes = bytes(0x11);
    const newBytes = bytes(0x22);

    await vfs.writeFile("/dst.bin", oldBytes);
    const old = await readChunkLocation(tenant, "dst.bin");
    const oldShard = shardStubFor(tenant, old.shardIndex);
    await oldShard.testConfigureDeleteChunksFailure(
      old.fileId,
      "before",
      null
    );

    await vfs.writeFile("/dst.bin", newBytes);

    expect
      .soft(await fingerprint(await readFileOrNull(vfs, "/dst.bin")))
      .toEqual(await fingerprint(newBytes));
    expect.soft(await vfs.exists("/dst.bin")).toBe(true);
    expect(await readShardRefState(oldShard, old.hash)).toMatchObject({
      refCount: 1,
      fileIds: [old.fileId],
    });
    const pending = await readCleanupIntents(tenant, old.fileId);
    expect(pending).toHaveLength(1);
    expect(pending[0]).toMatchObject({
      refId: old.fileId,
      shardIndex: old.shardIndex,
      attempts: 1,
    });
    expect(pending[0].lastError).toBeTruthy();
    expect(pending[0].updatedAt).toBeGreaterThanOrEqual(pending[0].createdAt);
    expect(
      await runInDurableObject(userStubFor(tenant), (_instance, state) =>
        state.storage.getAlarm()
      )
    ).not.toBeNull();

    await oldShard.testClearDeleteChunksFailure();
    await makeCleanupIntentEligible(tenant, old.fileId);
    expect(await runDurableObjectAlarm(userStubFor(tenant))).toBe(true);

    expect.soft(await readShardRefState(oldShard, old.hash)).toMatchObject({
      refCount: 0,
      fileIds: [],
    });
    expect(await readCleanupIntents(tenant, old.fileId)).toEqual([]);
    expect
      .soft(await fingerprint(await readFileOrNull(vfs, "/dst.bin")))
      .toEqual(await fingerprint(newBytes));
  });

  it("retries a lost deleteChunks response without double-decrementing shared chunks or losing new bytes", async () => {
    const tenant = "overwrite-cleanup-after";
    const vfs = createVFS(envFor(), { tenant });
    const oldBytes = bytes(0x33);
    const newBytes = bytes(0x44);

    await vfs.writeFile("/dst.bin", oldBytes);
    const old = await readChunkLocation(tenant, "dst.bin");
    const oldShard = shardStubFor(tenant, old.shardIndex);
    const survivorRef = "test-survivor-ref";
    await oldShard.putChunk(
      old.hash,
      new Uint8Array(0),
      survivorRef,
      0,
      tenant
    );
    expect(await readShardRefState(oldShard, old.hash)).toMatchObject({
      refCount: 2,
      fileIds: [old.fileId, survivorRef].sort(),
    });
    await oldShard.testConfigureDeleteChunksFailure(old.fileId, "after", 1);

    await vfs.writeFile("/dst.bin", newBytes);
    expect
      .soft(await fingerprint(await readFileOrNull(vfs, "/dst.bin")))
      .toEqual(await fingerprint(newBytes));

    expect(await readShardRefState(oldShard, old.hash)).toEqual({
      refCount: 1,
      fileIds: [survivorRef],
    });
    expect(await readCleanupIntents(tenant, old.fileId)).toMatchObject([
      {
        refId: old.fileId,
        shardIndex: old.shardIndex,
        attempts: 1,
      },
    ]);
    await makeCleanupIntentEligible(tenant, old.fileId);
    expect(await runDurableObjectAlarm(userStubFor(tenant))).toBe(true);
    expect.soft(await readShardRefState(oldShard, old.hash)).toEqual({
      refCount: 1,
      fileIds: [survivorRef],
    });
    expect(await readCleanupIntents(tenant, old.fileId)).toEqual([]);
    expect
      .soft(await fingerprint(await readFileOrNull(vfs, "/dst.bin")))
      .toEqual(await fingerprint(newBytes));
  });

  it("rolls back the complete local publication when SQL promotion fails", async () => {
    const tenant = "overwrite-promotion-sql-failure";
    const vfs = createVFS(envFor(), { tenant });
    const oldBytes = bytes(0x55);
    const newBytes = bytes(0x66);

    await vfs.writeFile("/dst.bin", oldBytes);
    const before = await readLocalPublicationState(tenant);
    await runInDurableObject(userStubFor(tenant), async (_instance, state) => {
      state.storage.sql.exec(`
        CREATE TRIGGER fail_test_file_promotion
        BEFORE UPDATE OF file_name, status ON files
        WHEN OLD.status = 'uploading' AND NEW.status = 'complete'
        BEGIN
          SELECT RAISE(ABORT, 'injected local promotion failure');
        END
      `);
    });

    const promotionError = await vfs
      .writeFile("/dst.bin", newBytes)
      .catch((err: unknown) => err);
    expect(promotionError).toBeInstanceOf(Error);
    expect((promotionError as Error).message).toMatch(
      /injected local promotion failure/
    );
    expect((promotionError as Error).message).not.toContain("EBUSY");

    expect
      .soft(await fingerprint(await readFileOrNull(vfs, "/dst.bin")))
      .toEqual(await fingerprint(oldBytes));
    expect.soft(await vfs.exists("/dst.bin")).toBe(true);
    expect.soft(await readLocalPublicationState(tenant)).toEqual(before);
    expect(await readCleanupIntents(tenant, before.files[0].file_id)).toEqual([]);
    expect(await readAllCleanupIntentIds(tenant)).toEqual([]);
    expect(await readTenantShardRefIds(tenant)).toEqual([
      before.files[0].file_id,
    ]);
  });

  it("leaves both rename paths unchanged when maintenance alarm scheduling fails", async () => {
    const tenant = "rename-overwrite-alarm-failure";
    const vfs = createVFS(envFor(), { tenant });
    const sourceBytes = bytes(0x61);
    const destinationBytes = bytes(0x62);

    await vfs.writeFile("/source.bin", sourceBytes);
    await vfs.writeFile("/destination.bin", destinationBytes);
    const before = await readLocalPublicationState(tenant);
    await userStubFor(tenant).testConfigureMaintenanceAlarmFailure(1);

    await expect(
      vfs.rename("/source.bin", "/destination.bin")
    ).rejects.toThrow(/injected maintenance alarm failure/);

    expect(await readLocalPublicationState(tenant)).toEqual(before);
    expect(await vfs.readFile("/source.bin")).toEqual(sourceBytes);
    expect(await vfs.readFile("/destination.bin")).toEqual(destinationBytes);
    expect(await readAllCleanupIntentIds(tenant)).toEqual([]);
  });

  it("rolls back destination supersede, source move, accounting, audit, and revision on local rename SQL failure", async () => {
    const tenant = "rename-overwrite-sql-failure";
    const vfs = createVFS(envFor(), { tenant });
    const sourceBytes = bytes(0x63);
    const destinationBytes = bytes(0x64);

    await vfs.writeFile("/source.bin", sourceBytes);
    await vfs.writeFile("/destination.bin", destinationBytes);
    const before = await readLocalPublicationState(tenant);
    await runInDurableObject(userStubFor(tenant), async (_instance, state) => {
      state.storage.sql.exec(`
        CREATE TRIGGER fail_test_rename_displaced_delete
        BEFORE DELETE ON files
        WHEN OLD.user_id = '${tenant}' AND OLD.file_name = 'destination.bin'
        BEGIN
          SELECT RAISE(ABORT, 'injected rename displaced delete failure');
        END
      `);
    });

    await expect(
      vfs.rename("/source.bin", "/destination.bin")
    ).rejects.toThrow(/injected rename displaced delete failure/);

    expect(await readLocalPublicationState(tenant)).toEqual(before);
    expect(await vfs.readFile("/source.bin")).toEqual(sourceBytes);
    expect(await vfs.readFile("/destination.bin")).toEqual(destinationBytes);
    expect(await readAllCleanupIntentIds(tenant)).toEqual([]);
  });

  it.each(["before", "after"] as const)(
    "commits rename-overwrite bytes and accounting while retrying deleteChunks failure %s mutation",
    async (phase) => {
      const tenant = `rename-overwrite-cleanup-${phase}`;
      const vfs = createVFS(envFor(), { tenant });
      const sourceBytes = bytes(0x65);
      const destinationBytes = bytes(0x66);

      await vfs.writeFile("/source.bin", sourceBytes);
      await vfs.writeFile("/destination.bin", destinationBytes);
      const source = await readChunkLocation(tenant, "source.bin");
      const displaced = await readChunkLocation(tenant, "destination.bin");
      const displacedShard = shardStubFor(tenant, displaced.shardIndex);
      const survivorRef = `rename-survivor-${phase}`;
      await displacedShard.putChunk(
        displaced.hash,
        new Uint8Array(0),
        survivorRef,
        0,
        tenant
      );
      const before = await readLocalPublicationState(tenant);
      await displacedShard.testConfigureDeleteChunksFailure(
        displaced.fileId,
        phase,
        1
      );

      await vfs.rename("/source.bin", "/destination.bin");

      expect(await vfs.exists("/source.bin")).toBe(false);
      expect(await vfs.readFile("/destination.bin")).toEqual(sourceBytes);
      const committed = await readLocalPublicationState(tenant);
      expect(committed.files).toMatchObject([
        {
          file_id: source.fileId,
          file_name: "destination.bin",
          file_size: sourceBytes.byteLength,
          status: "complete",
          deleted_at: null,
        },
      ]);
      expect(committed.chunks).toHaveLength(1);
      expect(committed.chunks[0]?.file_id).toBe(source.fileId);
      expect(committed.quota).toEqual({
        storage_used: sourceBytes.byteLength,
        file_count: 1,
        inline_bytes_used: 0,
      });
      expect(committed.rootRevision).toBe(before.rootRevision + 1);
      expect(committed.renameAuditCount).toBe(1);
      expect(await readCleanupIntents(tenant, displaced.fileId)).toMatchObject([
        {
          refId: displaced.fileId,
          shardIndex: displaced.shardIndex,
          attempts: 1,
        },
      ]);
      expect(await readShardRefState(displacedShard, displaced.hash)).toEqual(
        phase === "before"
          ? {
              refCount: 2,
              fileIds: [displaced.fileId, survivorRef].sort(),
            }
          : { refCount: 1, fileIds: [survivorRef] }
      );

      await makeCleanupIntentEligible(tenant, displaced.fileId);
      expect(await runDurableObjectAlarm(userStubFor(tenant))).toBe(true);
      expect(await readCleanupIntents(tenant, displaced.fileId)).toEqual([]);
      expect(await readShardRefState(displacedShard, displaced.hash)).toEqual({
        refCount: 1,
        fileIds: [survivorRef],
      });
      expect(await vfs.readFile("/destination.bin")).toEqual(sourceBytes);
      expect((await readLocalPublicationState(tenant)).quota).toEqual(
        committed.quota
      );
    }
  );

  it("keeps two overwrites serializable while the first cleanup is blocked and fails", async () => {
    const tenant = "overwrite-cleanup-interleaving";
    const stub = userStubFor(tenant);
    const scope = { ns: NS, tenant } as const;
    const oldBytes = bytes(0x71);
    const firstBytes = bytes(0x72);
    const finalBytes = bytes(0x73);

    await stub.vfsWriteFile(scope, "/dst.bin", oldBytes);
    const old = await readChunkLocation(tenant, "dst.bin");
    const oldShard = shardStubFor(tenant, old.shardIndex);
    await oldShard.testConfigureDeleteChunksBlock(old.fileId);
    await oldShard.testConfigureDeleteChunksFailure(old.fileId, "before", null);

    const firstOverwrite = stub.vfsWriteFile(scope, "/dst.bin", firstBytes, {
      metadata: { generation: 1 },
      tags: ["generation-1"],
      encryption: { mode: "convergent", keyId: "serial-key" },
    });
    await oldShard.testWaitForDeleteChunksBlocked();

    await stub.vfsWriteFile(scope, "/dst.bin", finalBytes, {
      metadata: { generation: 2 },
      tags: ["generation-2", "winner"],
      encryption: { mode: "convergent", keyId: "serial-key" },
    });

    const beforeRelease = await readCommittedPathState(tenant, "dst.bin");
    expect(beforeRelease).toMatchObject({
      size: finalBytes.byteLength,
      metadata: { generation: 2 },
      tags: ["generation-2", "winner"],
      encryptionMode: "convergent",
      encryptionKeyId: "serial-key",
      quota: {
        storage_used: finalBytes.byteLength,
        file_count: 1,
        inline_bytes_used: 0,
      },
      revision: 3,
    });

    await oldShard.testReleaseDeleteChunksBlock();
    await firstOverwrite;

    expect(await readCommittedPathState(tenant, "dst.bin")).toEqual(
      beforeRelease
    );
    expect(await stub.vfsReadFile(scope, "/dst.bin")).toEqual(finalBytes);
    expect(await readCleanupIntents(tenant, old.fileId)).toMatchObject([
      { attempts: 1 },
    ]);
    await oldShard.testClearDeleteChunksFailure();
  });

  it("keeps inline quota accounting serializable across a blocked cleanup", async () => {
    const tenant = "overwrite-inline-cleanup-interleaving";
    const stub = userStubFor(tenant);
    const scope = { ns: NS, tenant } as const;
    const oldBytes = bytes(0x81);
    const firstBytes = new Uint8Array(1_024).fill(0x82);
    const finalBytes = new Uint8Array(2_048).fill(0x83);

    await stub.vfsWriteFile(scope, "/dst.bin", oldBytes);
    const old = await readChunkLocation(tenant, "dst.bin");
    const oldShard = shardStubFor(tenant, old.shardIndex);
    await oldShard.testConfigureDeleteChunksBlock(old.fileId);

    const firstOverwrite = stub.vfsWriteFile(scope, "/dst.bin", firstBytes);
    await oldShard.testWaitForDeleteChunksBlocked();
    await stub.vfsWriteFile(scope, "/dst.bin", finalBytes);

    const beforeRelease = await readCommittedPathState(tenant, "dst.bin");
    expect(beforeRelease.quota).toEqual({
      storage_used: finalBytes.byteLength,
      file_count: 1,
      inline_bytes_used: finalBytes.byteLength,
    });
    expect(beforeRelease.revision).toBe(3);

    await oldShard.testReleaseDeleteChunksBlock();
    await firstOverwrite;
    expect(await readCommittedPathState(tenant, "dst.bin")).toEqual(
      beforeRelease
    );
  });

  it("lets a newer eligible cleanup pass more than one batch of backed-off poison rows", async () => {
    const tenant = "cleanup-poison-fairness";
    const stub = userStubFor(tenant);
    const scope = { ns: NS, tenant } as const;
    await stub.vfsExists(scope, "/missing");

    const recoverableRef = "recoverable-ref";
    const recoverableBytes = new Uint8Array([1, 2, 3]);
    const recoverableHash = await hashChunk(recoverableBytes);
    const shard = shardStubFor(tenant, 0);
    const earliestPoisonAttempt = Date.now() + 60 * 60 * 1000;
    await shard.putChunk(
      recoverableHash,
      recoverableBytes,
      recoverableRef,
      0,
      tenant
    );

    await runInDurableObject(stub, async (_instance, state) => {
      const now = Date.now();
      state.storage.transactionSync(() => {
        for (let index = 0; index <= 200; index++) {
          state.storage.sql.exec(
            `INSERT INTO chunk_cleanup_intents
               (ref_id, shard_index, created_at, updated_at, next_attempt_at, attempts, last_error)
             VALUES (?, 0, ?, ?, ?, 3, 'poison')`,
            `poison-${index.toString().padStart(3, "0")}`,
            index,
            now,
            earliestPoisonAttempt + index
          );
        }
        state.storage.sql.exec(
          `INSERT INTO chunk_cleanup_intents
             (ref_id, shard_index, created_at, updated_at, next_attempt_at, attempts, last_error)
           VALUES (?, 0, ?, ?, 0, 0, NULL)`,
          recoverableRef,
          now,
          now
        );
      });
      await state.storage.setAlarm(Date.now() + 1_000);
    });

    expect(await runDurableObjectAlarm(stub)).toBe(true);
    expect(await readCleanupIntents(tenant, recoverableRef)).toEqual([]);
    expect(await readShardRefState(shard, recoverableHash)).toEqual({
      refCount: 0,
      fileIds: [],
    });
    const remaining = await readAllCleanupIntentIds(tenant);
    expect(remaining).toHaveLength(201);
    expect(remaining.every((refId) => refId.startsWith("poison-"))).toBe(true);

    const scheduledAlarm = await runInDurableObject(
      stub,
      (_instance, state) => state.storage.getAlarm()
    );
    expect(scheduledAlarm).not.toBeNull();
    expect(scheduledAlarm).toBeLessThanOrEqual(earliestPoisonAttempt);

    await makeCleanupIntentEligible(tenant, "poison-000");
    expect(await runDurableObjectAlarm(stub)).toBe(true);
    expect(await readCleanupIntents(tenant, "poison-000")).toEqual([]);
    expect(await readAllCleanupIntentIds(tenant)).toHaveLength(200);
  });

  it("caps cleanup fan-out at six concurrent shard calls", async () => {
    const tenant = "cleanup-concurrency-six";
    const stub = userStubFor(tenant);
    const scope = { ns: NS, tenant } as const;
    await stub.vfsExists(scope, "/missing");
    const shard = shardStubFor(tenant, 0);
    await shard.testConfigureDeleteChunksConcurrencyProbe(6);

    await runInDurableObject(stub, async (_instance, state) => {
      const now = Date.now();
      state.storage.transactionSync(() => {
        for (let index = 0; index < 10; index++) {
          state.storage.sql.exec(
            `INSERT INTO chunk_cleanup_intents
               (ref_id, shard_index, created_at, updated_at, next_attempt_at, attempts, last_error)
             VALUES (?, 0, ?, ?, 0, 0, NULL)`,
            `cleanup-${index}`,
            now + index,
            now
          );
        }
      });
      await state.storage.setAlarm(Date.now() + 1_000);
    });

    const alarm = runDurableObjectAlarm(stub);
    await shard.testWaitForDeleteChunksConcurrency();
    expect(await shard.testReadDeleteChunksMaxConcurrency()).toBe(6);
    await shard.testReleaseDeleteChunksConcurrencyProbe();
    expect(await alarm).toBe(true);
    expect(await readAllCleanupIntentIds(tenant)).toEqual([]);
    expect(await shard.testReadDeleteChunksMaxConcurrency()).toBe(6);
  });
});
