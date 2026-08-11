import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";

import { signVFSMultipartToken } from "@core/lib/auth";
import type { ShardDO } from "@core/objects/shard/shard-do";
import { hashChunk } from "@shared/crypto";
import type { EnvCore } from "@shared/types";

/**
 * ShardDO multipart supersession atomicity.
 *
 * A re-PUT under a different hash drops the prior ref, moves two
 * refcounts, registers the new chunk and replaces the staging row.
 * These pin that the four commit together or not at all wherever the
 * failure lands, and that the retry then supersedes from what the
 * rollback left.
 *
 * The faults are SQLite triggers rather than a fault-injecting DO
 * subclass because what has to fail is a statement mid-sequence, which
 * is precisely what `RAISE(ABORT, ...)` produces.
 */

interface TestEnv extends EnvCore {
  MOSSAIC_SHARD: DurableObjectNamespace<ShardDO>;
}

interface ChunkState extends Record<string, SqlStorageValue> {
  hash: string;
  refCount: number;
  actualRefs: number;
  deletedAt: number | null;
  size: number;
}

interface StagingRow extends Record<string, SqlStorageValue> {
  hash: string;
  size: number;
}

interface MultipartState {
  chunks: ChunkState[];
  staging: StagingRow[];
  capacity: number;
}

interface Fixture {
  stub: DurableObjectStub<ShardDO>;
  uploadId: string;
  userId: string;
  token: string;
}

const E = env as unknown as TestEnv;
const encoder = new TextEncoder();

// `readState` orders chunks under SQLite's BINARY collation; expected
// arrays sort the same way rather than by locale.
function byHash(left: { hash: string }, right: { hash: string }): number {
  return left.hash < right.hash ? -1 : 1;
}

async function createFixture(name: string): Promise<Fixture> {
  const uploadId = `upload-${name}`;
  const userId = `user-${name}`;
  const { token } = await signVFSMultipartToken(E, {
    uploadId,
    fenceId: `fence-${name}`,
    userId,
    ns: "default",
    tn: userId,
    poolSize: 1,
    totalChunks: 1,
    chunkSize: 1024,
    totalSize: 1024,
  });
  return {
    stub: E.MOSSAIC_SHARD.get(
      E.MOSSAIC_SHARD.idFromName(`multipart-transaction-${name}`)
    ),
    uploadId,
    userId,
    token,
  };
}

async function put(
  fixture: Fixture,
  data: Uint8Array
): Promise<{
  status: "created" | "deduplicated" | "superseded";
  bytesStored: number;
}> {
  return fixture.stub.putChunkMultipart(
    await hashChunk(data),
    data,
    fixture.uploadId,
    0,
    fixture.userId,
    fixture.token
  );
}

function readState(fixture: Fixture): Promise<MultipartState> {
  return runInDurableObject(fixture.stub, (_instance, state) => {
    const sql = state.storage.sql;
    const chunks = sql
      .exec<ChunkState>(
        `SELECT c.hash,
                c.ref_count AS refCount,
                (SELECT COUNT(*) FROM chunk_refs r WHERE r.chunk_hash = c.hash) AS actualRefs,
                c.deleted_at AS deletedAt,
                c.size
           FROM chunks c
          ORDER BY c.hash`
      )
      .toArray();
    const staging = sql
      .exec<StagingRow>(
        `SELECT chunk_hash AS hash, chunk_size AS size
           FROM upload_chunks
          WHERE upload_id = ? AND chunk_index = 0`,
        fixture.uploadId
      )
      .toArray();
    const capacityRows = sql
      .exec<{ value: number }>(
        "SELECT value FROM shard_meta WHERE key = 'capacity_used_bytes'"
      )
      .toArray();
    return { chunks, staging, capacity: capacityRows[0]?.value ?? 0 };
  });
}

function countFences(fixture: Fixture): Promise<number> {
  return runInDurableObject(
    fixture.stub,
    (_instance, state) =>
      state.storage.sql
        .exec<{ n: number }>("SELECT COUNT(*) AS n FROM multipart_fences")
        .toArray()[0].n
  );
}

function execInShard(fixture: Fixture, ...statements: string[]): Promise<void> {
  return runInDurableObject(fixture.stub, (_instance, state) => {
    for (const statement of statements) state.storage.sql.exec(statement);
  });
}

describe("ShardDO multipart supersession transaction", () => {
  it("rolls back every supersession mutation when staging replacement fails", async () => {
    const fixture = await createFixture("staging-rollback");
    const oldData = encoder.encode("stable payload");
    const newData = encoder.encode("failed replacement");
    const oldHash = await hashChunk(oldData);
    const newHash = await hashChunk(newData);
    await put(fixture, oldData);

    // Dropping the fence lets the failing put re-open it, so the
    // rollback of the fence write is observable alongside the mutation.
    await execInShard(
      fixture,
      `DELETE FROM multipart_fences WHERE upload_id = '${fixture.uploadId}'`,
      `CREATE TRIGGER fail_multipart_staging_replacement
         BEFORE INSERT ON upload_chunks
         WHEN NEW.chunk_hash = '${newHash}'
         BEGIN
           SELECT RAISE(ABORT, 'injected multipart staging failure');
         END`
    );

    await expect(put(fixture, newData)).rejects.toThrow(
      /injected multipart staging failure/
    );

    expect(await readState(fixture)).toEqual({
      chunks: [
        {
          hash: oldHash,
          refCount: 1,
          actualRefs: 1,
          deletedAt: null,
          size: oldData.byteLength,
        },
      ],
      staging: [{ hash: oldHash, size: oldData.byteLength }],
      capacity: oldData.byteLength,
    });
    // The fence this put re-opened rolled back with the mutation it was
    // guarding, so nothing is left claiming the upload is open.
    expect(await countFences(fixture)).toBe(0);

    await execInShard(
      fixture,
      "DROP TRIGGER fail_multipart_staging_replacement"
    );
    expect(await put(fixture, newData)).toEqual({
      status: "superseded",
      bytesStored: newData.byteLength,
    });
    expect(await readState(fixture)).toEqual({
      chunks: [
        {
          hash: oldHash,
          refCount: 0,
          actualRefs: 0,
          deletedAt: expect.any(Number),
          size: oldData.byteLength,
        },
        {
          hash: newHash,
          refCount: 1,
          actualRefs: 1,
          deletedAt: null,
          size: newData.byteLength,
        },
      ].sort(byHash),
      staging: [{ hash: newHash, size: newData.byteLength }],
      capacity: oldData.byteLength + newData.byteLength,
    });
  });

  it("restores the prior chunk's ref when the replacement chunk write fails", async () => {
    const fixture = await createFixture("chunk-write-rollback");
    const oldData = encoder.encode("referenced payload");
    const newData = encoder.encode("unwritable replacement");
    const oldHash = await hashChunk(oldData);
    const newHash = await hashChunk(newData);
    await put(fixture, oldData);

    // Aborts halfway: the prior ref is already dropped, its refcount
    // decremented and the chunk soft-marked by the time this fires.
    await execInShard(
      fixture,
      `CREATE TRIGGER fail_replacement_chunk_write
         BEFORE INSERT ON chunks
         WHEN NEW.hash = '${newHash}'
         BEGIN
           SELECT RAISE(ABORT, 'injected replacement chunk failure');
         END`
    );

    await expect(put(fixture, newData)).rejects.toThrow(
      /injected replacement chunk failure/
    );

    expect(await readState(fixture)).toEqual({
      chunks: [
        {
          hash: oldHash,
          refCount: 1,
          actualRefs: 1,
          deletedAt: null,
          size: oldData.byteLength,
        },
      ],
      staging: [{ hash: oldHash, size: oldData.byteLength }],
      capacity: oldData.byteLength,
    });
  });
});
