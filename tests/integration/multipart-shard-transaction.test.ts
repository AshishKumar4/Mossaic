import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";

import { signVFSMultipartToken } from "@core/lib/auth";
import type { ShardDO } from "@core/objects/shard/shard-do";
import { hashChunk } from "@shared/crypto";
import type { EnvCore } from "@shared/types";

interface ShardFaultControls {
  testConfigurePutChunkMultipartResponseLoss(remaining: number): Promise<void>;
  testConfigureScheduleSweepFailure(remaining: number): Promise<void>;
}

type TestShardDO = ShardDO & ShardFaultControls;

interface TestEnv extends EnvCore {
  MOSSAIC_SHARD: DurableObjectNamespace<TestShardDO>;
}

interface ChunkState {
  hash: string;
  refCount: number;
  actualRefs: number;
  deletedAt: number | null;
  size: number;
}

interface MultipartState {
  chunks: ChunkState[];
  staging: Array<{ hash: string; size: number }>;
  capacity: number;
}

const E = env as unknown as TestEnv;
const encoder = new TextEncoder();

async function createFixture(name: string): Promise<{
  stub: DurableObjectStub<TestShardDO>;
  uploadId: string;
  userId: string;
  token: string;
}> {
  const uploadId = `upload-${name}`;
  const userId = `user-${name}`;
  const fenceId = `fence-${name}`;
  const { token } = await signVFSMultipartToken(E, {
    uploadId,
    fenceId,
    userId,
    ns: "default",
    tn: userId,
    poolSize: 1,
    totalChunks: 1,
    chunkSize: 1024,
    totalSize: 1024,
  });
  const stub = E.MOSSAIC_SHARD.get(
    E.MOSSAIC_SHARD.idFromName(`multipart-transaction-${name}`),
  );
  return { stub, uploadId, userId, token };
}

async function put(
  fixture: Awaited<ReturnType<typeof createFixture>>,
  data: Uint8Array,
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
    fixture.token,
  );
}

async function readState(
  fixture: Awaited<ReturnType<typeof createFixture>>,
): Promise<MultipartState> {
  return runInDurableObject(fixture.stub, (_instance, state) => {
    const sql = state.storage.sql;
    const chunks = sql
      .exec(
        `SELECT c.hash,
                c.ref_count AS refCount,
                (SELECT COUNT(*) FROM chunk_refs r WHERE r.chunk_hash = c.hash) AS actualRefs,
                c.deleted_at AS deletedAt,
                c.size
           FROM chunks c
          ORDER BY c.hash`,
      )
      .toArray() as unknown as ChunkState[];
    const staging = sql
      .exec(
        `SELECT chunk_hash AS hash, chunk_size AS size
           FROM upload_chunks
          WHERE upload_id = ? AND chunk_index = 0`,
        fixture.uploadId,
      )
      .toArray() as unknown as Array<{ hash: string; size: number }>;
    const capacityRow = sql
      .exec("SELECT value FROM shard_meta WHERE key = 'capacity_used_bytes'")
      .toArray()[0] as { value: number } | undefined;
    return { chunks, staging, capacity: capacityRow?.value ?? 0 };
  });
}

function expectRefcountEquality(state: MultipartState): void {
  for (const chunk of state.chunks) {
    expect(chunk.refCount, `refcount for ${chunk.hash}`).toBe(chunk.actualRefs);
  }
}

describe("ShardDO multipart put transaction", () => {
  it("reclaims expired fence rows after the safety grace", async () => {
    const fixture = await createFixture("fence-expiry");
    await put(fixture, encoder.encode("fenced"));
    await fixture.stub.fenceMultipart(
      fixture.uploadId,
      "fence-fence-expiry",
      "finalizing",
      Date.now() + 60_000,
    );
    await runInDurableObject(fixture.stub, (_instance, state) => {
      state.storage.sql.exec(
        "UPDATE multipart_fences SET expires_at = ?",
        Date.now() - 2 * 60 * 60 * 1000,
      );
    });

    expect(
      await runInDurableObject(fixture.stub, (_instance, state) =>
        state.storage.sql
          .exec("SELECT COUNT(*) AS n FROM multipart_fences")
          .toArray()[0],
      ),
    ).toMatchObject({ n: 1 });
    await runInDurableObject(fixture.stub, (instance) => instance.alarm());
    expect(
      await runInDurableObject(fixture.stub, (_instance, state) =>
        state.storage.sql
          .exec("SELECT COUNT(*) AS n FROM multipart_fences")
          .toArray()[0],
      ),
    ).toMatchObject({ n: 0 });
  });

  it("keeps same-hash retries idempotent", async () => {
    const fixture = await createFixture("same-hash");
    const data = encoder.encode("same hash payload");
    const hash = await hashChunk(data);

    expect(await put(fixture, data)).toEqual({
      status: "created",
      bytesStored: data.byteLength,
    });
    expect(await put(fixture, data)).toEqual({
      status: "deduplicated",
      bytesStored: 0,
    });

    const state = await readState(fixture);
    expect(state).toEqual({
      chunks: [
        {
          hash,
          refCount: 1,
          actualRefs: 1,
          deletedAt: null,
          size: data.byteLength,
        },
      ],
      staging: [{ hash, size: data.byteLength }],
      capacity: data.byteLength,
    });
    expectRefcountEquality(state);
  });

  it("does not rewrite an unchanged open fence on chunk retries", async () => {
    const fixture = await createFixture("same-fence-no-write");
    const data = encoder.encode("same fence payload");
    await put(fixture, data);
    await runInDurableObject(fixture.stub, (_instance, state) => {
      state.storage.sql.exec("CREATE TABLE fence_updates (n INTEGER NOT NULL)");
      state.storage.sql.exec("INSERT INTO fence_updates VALUES (0)");
      state.storage.sql.exec(`
        CREATE TRIGGER count_fence_updates AFTER UPDATE ON multipart_fences
        BEGIN
          UPDATE fence_updates SET n = n + 1;
        END
      `);
    });

    await put(fixture, data);
    await put(fixture, data);
    await expect(
      runInDurableObject(fixture.stub, (_instance, state) =>
        state.storage.sql.exec("SELECT n FROM fence_updates").toArray()[0]
      )
    ).resolves.toEqual({ n: 0 });
  });

  it("retries a different-hash supersession after response loss", async () => {
    const fixture = await createFixture("response-loss");
    const oldData = encoder.encode("old payload");
    const newData = encoder.encode("replacement payload");
    const oldHash = await hashChunk(oldData);
    const newHash = await hashChunk(newData);
    await put(fixture, oldData);

    await fixture.stub.testConfigurePutChunkMultipartResponseLoss(1);
    await expect(put(fixture, newData)).rejects.toThrow(
      /injected putChunkMultipart response loss after mutation/,
    );
    expect(await put(fixture, newData)).toEqual({
      status: "deduplicated",
      bytesStored: 0,
    });

    const state = await readState(fixture);
    expect(state).toEqual({
      chunks: [
        {
          hash: oldHash,
          refCount: 0,
          actualRefs: 0,
          deletedAt: expect.any(Number) as number,
          size: oldData.byteLength,
        },
        {
          hash: newHash,
          refCount: 1,
          actualRefs: 1,
          deletedAt: null,
          size: newData.byteLength,
        },
      ].sort((left, right) => left.hash.localeCompare(right.hash)),
      staging: [{ hash: newHash, size: newData.byteLength }],
      capacity: oldData.byteLength + newData.byteLength,
    });
    expectRefcountEquality(state);
  });

  it("does not mutate supersession state when sweep scheduling fails", async () => {
    const fixture = await createFixture("alarm-failure");
    const oldData = encoder.encode("alarm-safe payload");
    const newData = encoder.encode("alarm-safe replacement");
    const oldHash = await hashChunk(oldData);
    const newHash = await hashChunk(newData);
    await put(fixture, oldData);

    await fixture.stub.testConfigureScheduleSweepFailure(1);
    await expect(put(fixture, newData)).rejects.toThrow(
      /injected shard sweep scheduling failure/,
    );

    const failedState = await readState(fixture);
    expect(failedState).toEqual({
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
    expectRefcountEquality(failedState);

    expect(await put(fixture, newData)).toEqual({
      status: "superseded",
      bytesStored: newData.byteLength,
    });
    const retriedState = await readState(fixture);
    expect(retriedState.staging).toEqual([
      { hash: newHash, size: newData.byteLength },
    ]);
    expectRefcountEquality(retriedState);
    expect(
      await runInDurableObject(fixture.stub, (_instance, state) =>
        state.storage.getAlarm(),
      ),
    ).not.toBeNull();
  });

  it("rolls back every supersession mutation when staging replacement fails", async () => {
    const fixture = await createFixture("sql-rollback");
    const oldData = encoder.encode("stable payload");
    const newData = encoder.encode("failed replacement");
    const oldHash = await hashChunk(oldData);
    const newHash = await hashChunk(newData);
    await put(fixture, oldData);

    await runInDurableObject(fixture.stub, (_instance, state) => {
      state.storage.sql.exec(
        "DELETE FROM multipart_fences WHERE upload_id = ?",
        fixture.uploadId,
      );
      state.storage.sql.exec(
        `CREATE TRIGGER fail_multipart_staging_replacement
         BEFORE INSERT ON upload_chunks
         WHEN NEW.chunk_hash = '${newHash}'
         BEGIN
           SELECT RAISE(ABORT, 'injected multipart staging failure');
         END`,
      );
    });

    await expect(put(fixture, newData)).rejects.toThrow(
      /injected multipart staging failure/,
    );

    const state = await readState(fixture);
    expect(state).toEqual({
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
    expect(state.chunks.some((chunk) => chunk.hash === newHash)).toBe(false);
    expectRefcountEquality(state);
    expect(
      await runInDurableObject(fixture.stub, (_instance, durableObjectState) =>
        durableObjectState.storage.sql
          .exec(
            "SELECT COUNT(*) AS n FROM multipart_fences WHERE upload_id = ?",
            fixture.uploadId,
          )
          .toArray()[0],
      ),
    ).toEqual({ n: 0 });
  });
});
