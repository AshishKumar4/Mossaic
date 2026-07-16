import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";

import type { ShardDO } from "@core/objects/shard/shard-do";

interface TestEnv {
  MOSSAIC_SHARD: DurableObjectNamespace<ShardDO>;
}

function hasTestBindings(value: object): value is TestEnv {
  return "MOSSAIC_SHARD" in value;
}

if (!hasTestBindings(env)) throw new Error("missing test shard binding");
const E = env;

function shardStub(name: string): DurableObjectStub<ShardDO> {
  return E.MOSSAIC_SHARD.get(E.MOSSAIC_SHARD.idFromName(name));
}

async function seedDistinctRefs(
  stub: DurableObjectStub<ShardDO>,
  fileId: string,
  count: number
): Promise<void> {
  await stub.getStorageBytes();
  await runInDurableObject(stub, (_instance, state) => {
    state.storage.transactionSync(() => {
      for (let index = 0; index < count; index++) {
        const hash = (index + 1).toString(16).padStart(64, "0");
        state.storage.sql.exec(
          `INSERT INTO chunks (hash, data, size, ref_count, created_at)
           VALUES (?, ?, 1, 1, ?)`,
          hash,
          new Uint8Array([index & 0xff]),
          Date.now()
        );
        state.storage.sql.exec(
          `INSERT INTO chunk_refs (chunk_hash, file_id, chunk_index, user_id)
           VALUES (?, ?, ?, 'paged-test')`,
          hash,
          fileId,
          index
        );
      }
    });
  });
}

async function refState(
  stub: DurableObjectStub<ShardDO>,
  fileId: string
): Promise<{ refs: number; refCount: number; marked: number; pages: number }> {
  return runInDurableObject(stub, (_instance, state) => {
    const sql = state.storage.sql;
    return {
      refs: (
        sql
          .exec("SELECT COUNT(*) AS n FROM chunk_refs WHERE file_id = ?", fileId)
          .toArray()[0] as { n: number }
      ).n,
      refCount: (
        sql.exec("SELECT COALESCE(SUM(ref_count), 0) AS n FROM chunks").toArray()[0] as {
          n: number;
        }
      ).n,
      marked: (
        sql
          .exec("SELECT COUNT(*) AS n FROM chunks WHERE deleted_at IS NOT NULL")
          .toArray()[0] as { n: number }
      ).n,
      pages: (
        sql.exec("SELECT COUNT(*) AS n FROM shard_cleanup_pages").toArray()[0] as {
          n: number;
        }
      ).n,
    };
  });
}

describe("ShardDO paged cleanup", () => {
  it("rejects legacy cleanup before mutating a page-sized ref set", async () => {
    const stub = shardStub("paged-cleanup:legacy-bound");
    const fileId = "legacy-bound-file";
    await seedDistinctRefs(stub, fileId, 256);

    const error = await runInDurableObject(stub, async (instance) => {
      try {
        await instance.deleteChunks(fileId);
        return "";
      } catch (cause) {
        return cause instanceof Error ? cause.message : String(cause);
      }
    });
    expect(error).toMatch(/E2BIG.*bounded legacy limit/);
    expect(await refState(stub, fileId)).toMatchObject({
      refs: 256,
      refCount: 256,
      marked: 0,
    });
  });

  it("decrements more than 256 refs to the same hash exactly once across replay", async () => {
    const stub = shardStub("paged-cleanup:same-hash");
    const fileId = "same-hash-file";
    const survivor = "same-hash-survivor";
    const hash = "a".repeat(64);
    await stub.putChunk(hash, new Uint8Array([1]), fileId, 0, "paged-test");
    await stub.putChunk(hash, new Uint8Array(0), survivor, 0, "paged-test");
    await runInDurableObject(stub, (_instance, state) => {
      state.storage.transactionSync(() => {
        for (let index = 1; index < 300; index++) {
          state.storage.sql.exec(
            `INSERT INTO chunk_refs (chunk_hash, file_id, chunk_index, user_id)
             VALUES (?, ?, ?, 'paged-test')`,
            hash,
            fileId,
            index
          );
        }
        state.storage.sql.exec(
          "UPDATE chunks SET ref_count = 301 WHERE hash = ?",
          hash
        );
      });
    });

    const first = await stub.deleteChunksPage(fileId, 0, "same-hash-generation");
    expect(first).toEqual({
      cursor: 256,
      done: false,
      processed: 256,
      marked: 0,
    });
    expect(await refState(stub, fileId)).toMatchObject({ refs: 44, refCount: 45 });

    expect(
      await stub.deleteChunksPage(fileId, 0, "same-hash-generation")
    ).toEqual(first);
    expect(await refState(stub, fileId)).toMatchObject({ refs: 44, refCount: 45 });

    expect(
      await stub.deleteChunksPage(fileId, first.cursor, "same-hash-generation")
    ).toEqual({ cursor: 300, done: true, processed: 44, marked: 0 });
    expect(await refState(stub, fileId)).toMatchObject({
      refs: 0,
      refCount: 1,
      marked: 0,
    });
  });

  it("soft-deletes more than 256 different-hash refs in atomic pages", async () => {
    const stub = shardStub("paged-cleanup:different-hashes");
    const fileId = "different-hash-file";
    await seedDistinctRefs(stub, fileId, 300);

    const first = await stub.deleteChunksPage(fileId, 0, "different-generation");
    expect(first).toEqual({
      cursor: 256,
      done: false,
      processed: 256,
      marked: 256,
    });
    expect(await refState(stub, fileId)).toMatchObject({
      refs: 44,
      refCount: 44,
      marked: 256,
    });

    expect(
      await stub.deleteChunksPage(fileId, first.cursor, "different-generation")
    ).toEqual({ cursor: 300, done: true, processed: 44, marked: 44 });
    expect(await refState(stub, fileId)).toMatchObject({
      refs: 0,
      refCount: 0,
      marked: 300,
    });
  });

  it("rolls back refcounts, soft deletes, refs, and the replay record on a mid-page SQL failure", async () => {
    const stub = shardStub("paged-cleanup:rollback");
    const fileId = "rollback-file";
    await seedDistinctRefs(stub, fileId, 300);
    await runInDurableObject(stub, (_instance, state) => {
      state.storage.sql.exec(`
        CREATE TRIGGER fail_paged_soft_delete
        BEFORE UPDATE OF deleted_at ON chunks
        WHEN NEW.deleted_at IS NOT NULL
        BEGIN
          SELECT RAISE(ABORT, 'injected chunk soft-mark failure');
        END
      `);
    });

    await expect(
      stub.deleteChunksPage(fileId, 0, "rollback-generation")
    ).rejects.toThrow(/injected chunk soft-mark failure/);
    expect(await refState(stub, fileId)).toEqual({
      refs: 300,
      refCount: 300,
      marked: 0,
      pages: 0,
    });

    await runInDurableObject(stub, (_instance, state) => {
      state.storage.sql.exec("DROP TRIGGER fail_paged_soft_delete");
    });
    await expect(
      stub.deleteChunksPage(fileId, 0, "rollback-generation")
    ).resolves.toMatchObject({ cursor: 256, processed: 256 });
  });

  it("pages and exactly replays multipart staging cleanup", async () => {
    const stub = shardStub("paged-cleanup:staging");
    const uploadId = "staging-upload";
    await stub.getStorageBytes();
    await runInDurableObject(stub, (_instance, state) => {
      state.storage.transactionSync(() => {
        for (let index = 0; index < 300; index++) {
          state.storage.sql.exec(
            `INSERT INTO upload_chunks
               (upload_id, chunk_index, chunk_hash, chunk_size, user_id, created_at)
             VALUES (?, ?, ?, 1, 'paged-test', ?)`,
            uploadId,
            index,
            (index + 1).toString(16).padStart(64, "0"),
            Date.now()
          );
        }
      });
    });

    const first = await stub.clearMultipartStagingPage(
      uploadId,
      0,
      "staging-generation"
    );
    expect(first).toEqual({ cursor: 256, done: false, dropped: 256 });
    expect(
      await stub.clearMultipartStagingPage(uploadId, 0, "staging-generation")
    ).toEqual(first);
    expect(
      await stub.clearMultipartStagingPage(
        uploadId,
        first.cursor,
        "staging-generation"
      )
    ).toEqual({ cursor: 300, done: true, dropped: 44 });
  });
});
