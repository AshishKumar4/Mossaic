import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it, vi } from "vitest";

import { vfsShardDOName, vfsUserDOName } from "@core/lib/utils";
import type { ShardDO } from "@core/objects/shard/shard-do";
import type { UserDO } from "@app/objects/user/user-do";
import { computeFileHash, hashChunk } from "@shared/crypto";
import {
  MULTIPART_FENCE_PAGE_SIZE,
  MULTIPART_HASH_PAGE_SIZE,
  MULTIPART_PLACEMENT_VERSION,
  type MultipartFinalizeProgress,
  type MultipartFinalizeResponse,
} from "@shared/multipart";
import { placeMultipartChunk } from "@shared/placement";

/**
 * The durable multipart finalize machine.
 *
 * Finalize advances one bounded page per call — at most 64 shard fences, at
 * most 256 verified chunks, then one publication — and keeps where it got to
 * in the session row. These pin the properties that makes that safe:
 *
 *   - a pool larger than one fence page is fenced across calls and survives a
 *     Durable Object eviction between them,
 *   - verification advances in 256-chunk pages and carries the running content
 *     hash across them, producing byte-for-byte the digest the one-shot
 *     formula produces over the same chunk hashes,
 *   - a shard whose response is lost leaves the page entirely unapplied and
 *     the next call redoes it,
 *   - a step against a session whose progress moved — forwards or backwards —
 *     underneath it changes nothing, and
 *   - a step replayed after the operation finished returns the recorded
 *     result instead of publishing a second time.
 */

// Fencing and verification instantiate every shard of a fresh tenant's pool,
// which is far more Durable Object cold starts than an ordinary test pays.
vi.setConfig({ testTimeout: 60_000 });

interface UserFaultControls {
  testEvict(): Promise<void>;
}

interface ShardFaultControls {
  testConfigureMultipartManifestBlock(uploadId: string): Promise<void>;
  testWaitForMultipartManifestBlocked(): Promise<void>;
  testReleaseMultipartManifestBlock(): Promise<void>;
  testConfigureMultipartManifestRangeResponseLoss(
    remaining: number
  ): Promise<void>;
}

type TestUserDO = UserDO & UserFaultControls;
type TestShardDO = ShardDO & ShardFaultControls;

interface TestEnv {
  MOSSAIC_USER: DurableObjectNamespace<TestUserDO>;
  MOSSAIC_SHARD: DurableObjectNamespace<TestShardDO>;
}

const TEST_ENV = env as unknown as TestEnv;
const NS = "default";

function scopeFor(tenant: string): { ns: string; tenant: string } {
  return { ns: NS, tenant };
}

function userStub(tenant: string): DurableObjectStub<TestUserDO> {
  return TEST_ENV.MOSSAIC_USER.get(
    TEST_ENV.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
  );
}

function shardStub(
  tenant: string,
  shardIndex: number
): DurableObjectStub<TestShardDO> {
  return TEST_ENV.MOSSAIC_SHARD.get(
    TEST_ENV.MOSSAIC_SHARD.idFromName(
      vfsShardDOName(NS, tenant, undefined, shardIndex)
    )
  );
}

/** Two distinct bytes per index, so every chunk hash in an upload differs. */
function chunkBytes(index: number): Uint8Array {
  return new Uint8Array([index & 0xff, (index >> 8) & 0xff]);
}

const CHUNK_BYTES = 2;

interface Upload {
  tenant: string;
  path: string;
  uploadId: string;
  poolSize: number;
  totalChunks: number;
  hashes: string[];
}

/** Give a fresh tenant a pool wider than one fence page. */
async function widenPool(tenant: string, poolSize: number): Promise<void> {
  await userStub(tenant).vfsExists(scopeFor(tenant), "/");
  await runInDurableObject(
    userStub(tenant),
    (_instance: TestUserDO, state: DurableObjectState) => {
      state.storage.sql.exec(
        `INSERT INTO quota (user_id, pool_size) VALUES (?, ?)
           ON CONFLICT(user_id) DO UPDATE SET pool_size = excluded.pool_size`,
        tenant,
        poolSize
      );
    }
  );
}

/**
 * Begin a session, PUT every chunk onto its deterministic owner shard, and
 * stage the declared manifest in the pages the paged protocol uses.
 */
async function stageUpload(
  tenant: string,
  path: string,
  totalChunks: number
): Promise<Upload> {
  const scope = scopeFor(tenant);
  const begin = await userStub(tenant).vfsBeginMultipart(scope, path, {
    size: totalChunks * CHUNK_BYTES,
    chunkSize: CHUNK_BYTES,
  });
  expect(begin.totalChunks).toBe(totalChunks);

  const hashes: string[] = new Array<string>(totalChunks);
  const CONCURRENCY = 16;
  for (let base = 0; base < totalChunks; base += CONCURRENCY) {
    await Promise.all(
      Array.from(
        { length: Math.min(CONCURRENCY, totalChunks - base) },
        async (_unused, offset) => {
          const index = base + offset;
          const bytes = chunkBytes(index);
          const hash = await hashChunk(bytes);
          hashes[index] = hash;
          await shardStub(
            tenant,
            placeMultipartChunk(
              tenant,
              begin.uploadId,
              index,
              begin.poolSize,
              MULTIPART_PLACEMENT_VERSION
            )
          ).putChunkMultipart(
            hash,
            bytes,
            begin.uploadId,
            index,
            tenant,
            begin.sessionToken
          );
        }
      )
    );
  }

  for (
    let start = 0;
    start < totalChunks;
    start += MULTIPART_HASH_PAGE_SIZE
  ) {
    await userStub(tenant).vfsStageMultipartHashes(
      scope,
      begin.uploadId,
      start,
      hashes.slice(start, start + MULTIPART_HASH_PAGE_SIZE)
    );
  }

  return {
    tenant,
    path,
    uploadId: begin.uploadId,
    poolSize: begin.poolSize,
    totalChunks,
    hashes,
  };
}

function step(upload: Upload): Promise<MultipartFinalizeProgress> {
  return userStub(upload.tenant).vfsFinalizeMultipartStep(
    scopeFor(upload.tenant),
    upload.uploadId
  );
}

/** Drive the machine to completion the way the one-request finalize does. */
async function stepToDone(upload: Upload): Promise<MultipartFinalizeResponse> {
  for (let attempt = 0; attempt < 64; attempt++) {
    const progress = await step(upload);
    if (progress.done) return progress.result;
  }
  throw new Error("finalize did not reach a terminal state");
}

interface FinalizeState {
  status: string;
  phase: string | null;
  fenceCursor: number;
  chunkCursor: number;
  verifyShardCursor: number;
  totalSize: number;
  verified: number;
  routes: number[];
  expected: number;
}

async function readFinalizeState(upload: Upload): Promise<FinalizeState> {
  return runInDurableObject(
    userStub(upload.tenant),
    (_instance: TestUserDO, state: DurableObjectState) => {
      const sql = state.storage.sql;
      const session = sql
        .exec<{
          status: string;
          finalize_phase: string | null;
          finalize_fence_cursor: number;
          finalize_chunk_cursor: number;
          finalize_verify_shard_cursor: number;
          finalize_total_size: number;
        }>(
          `SELECT status, finalize_phase, finalize_fence_cursor,
                  finalize_chunk_cursor, finalize_verify_shard_cursor,
                  finalize_total_size
             FROM upload_sessions WHERE upload_id = ?`,
          upload.uploadId
        )
        .toArray()
        .at(0);
      const count = (table: string): number =>
        sql
          .exec<{ n: number }>(
            `SELECT COUNT(*) AS n FROM ${table} WHERE upload_id = ?`,
            upload.uploadId
          )
          .toArray()[0].n;
      return {
        status: session?.status ?? "missing",
        phase: session?.finalize_phase ?? null,
        fenceCursor: session?.finalize_fence_cursor ?? -1,
        chunkCursor: session?.finalize_chunk_cursor ?? -1,
        verifyShardCursor: session?.finalize_verify_shard_cursor ?? -1,
        totalSize: session?.finalize_total_size ?? -1,
        verified: count("upload_verified_chunks"),
        expected: count("upload_expected_chunks"),
        routes: sql
          .exec<{ shard_index: number }>(
            `SELECT shard_index FROM upload_cleanup_routes
              WHERE upload_id = ? AND cleanup_kind = 'multipart_staging'
              ORDER BY shard_index`,
            upload.uploadId
          )
          .toArray()
          .map((row) => row.shard_index),
      };
    }
  );
}

function ownerShards(upload: Upload): number[] {
  return [
    ...new Set(
      Array.from({ length: upload.totalChunks }, (_unused, index) =>
        placeMultipartChunk(
          upload.tenant,
          upload.uploadId,
          index,
          upload.poolSize,
          MULTIPART_PLACEMENT_VERSION
        )
      )
    ),
  ].sort((a, b) => a - b);
}

function expectedBytes(totalChunks: number): Uint8Array {
  const out = new Uint8Array(totalChunks * CHUNK_BYTES);
  for (let index = 0; index < totalChunks; index++) {
    out.set(chunkBytes(index), index * CHUNK_BYTES);
  }
  return out;
}

async function readSessionColumn(
  upload: Upload,
  column: string
): Promise<number> {
  return runInDurableObject(
    userStub(upload.tenant),
    (_instance: TestUserDO, state: DurableObjectState) =>
      state.storage.sql
        .exec<{ value: number }>(
          `SELECT ${column} AS value FROM upload_sessions WHERE upload_id = ?`,
          upload.uploadId
        )
        .toArray()[0].value
  );
}

async function writeSessionColumn(
  upload: Upload,
  column: string,
  value: number
): Promise<void> {
  await runInDurableObject(
    userStub(upload.tenant),
    (_instance: TestUserDO, state: DurableObjectState) => {
      state.storage.sql.exec(
        `UPDATE upload_sessions SET ${column} = ? WHERE upload_id = ?`,
        value,
        upload.uploadId
      );
    }
  );
}

describe("paged multipart finalize", () => {
  it("fences a pool wider than one page across calls and survives eviction", async () => {
    const tenant = "mp-step-fence-pages";
    const poolSize = MULTIPART_FENCE_PAGE_SIZE + 6;
    await widenPool(tenant, poolSize);
    const upload = await stageUpload(tenant, "/wide-pool.bin", 1);
    expect(upload.poolSize).toBe(poolSize);

    await expect(step(upload)).resolves.toEqual({
      done: false,
      phase: "fencing",
      cursor: MULTIPART_FENCE_PAGE_SIZE,
      total: poolSize,
    });
    expect(await readFinalizeState(upload)).toMatchObject({
      status: "finalizing",
      phase: "fencing",
      fenceCursor: MULTIPART_FENCE_PAGE_SIZE,
      chunkCursor: 0,
      verified: 0,
    });

    // The rest of the pool is fenced by a different instance of the object.
    await expect(userStub(tenant).testEvict()).rejects.toThrow(
      /injected UserDO eviction/
    );
    await expect(step(upload)).resolves.toEqual({
      done: false,
      phase: "verifying",
      cursor: 0,
      total: 1,
    });
    expect(await readFinalizeState(upload)).toMatchObject({
      phase: "verifying",
      fenceCursor: poolSize,
    });

    const result = await stepToDone(upload);
    expect(result.path).toBe("/wide-pool.bin");
    await expect(
      userStub(tenant).vfsReadFile(scopeFor(tenant), "/wide-pool.bin")
    ).resolves.toEqual(expectedBytes(1));
  });

  it("verifies in 256-chunk pages and keeps the one-shot file hash", async () => {
    const tenant = "mp-step-verify-pages";
    const totalChunks = MULTIPART_HASH_PAGE_SIZE + 64;
    const upload = await stageUpload(tenant, "/paged.bin", totalChunks);
    expect(upload.poolSize).toBeLessThanOrEqual(MULTIPART_FENCE_PAGE_SIZE);

    await expect(step(upload)).resolves.toEqual({
      done: false,
      phase: "verifying",
      cursor: 0,
      total: totalChunks,
    });

    await expect(step(upload)).resolves.toEqual({
      done: false,
      phase: "verifying",
      cursor: MULTIPART_HASH_PAGE_SIZE,
      total: totalChunks,
    });
    expect(await readFinalizeState(upload)).toMatchObject({
      phase: "verifying",
      chunkCursor: MULTIPART_HASH_PAGE_SIZE,
      verifyShardCursor: 0,
      verified: MULTIPART_HASH_PAGE_SIZE,
      totalSize: MULTIPART_HASH_PAGE_SIZE * CHUNK_BYTES,
    });

    // The remainder is verified by a different instance of the object, so the
    // file hash below is only reachable if the accumulator survived in the row.
    await expect(userStub(tenant).testEvict()).rejects.toThrow(
      /injected UserDO eviction/
    );
    await expect(step(upload)).resolves.toEqual({
      done: false,
      phase: "publishing",
      cursor: totalChunks,
      total: totalChunks,
    });
    // Routing is recorded while the staging rows naming those shards are
    // still there, so publication never has to re-derive it.
    expect(await readFinalizeState(upload)).toMatchObject({
      phase: "publishing",
      chunkCursor: totalChunks,
      verified: totalChunks,
      totalSize: totalChunks * CHUNK_BYTES,
      routes: ownerShards(upload),
    });

    // Publication is constant-size: it leaves the scratch it no longer reads
    // for the bounded cleaning that follows it rather than dropping the whole
    // manifest in its own transaction.
    await expect(step(upload)).resolves.toEqual({
      done: false,
      phase: "cleaning",
      cursor: 0,
      total: totalChunks,
    });
    const expected: MultipartFinalizeResponse = {
      fileId: upload.uploadId,
      size: totalChunks * CHUNK_BYTES,
      chunkCount: totalChunks,
      fileHash: await computeFileHash(upload.hashes),
      path: "/paged.bin",
      mimeType: "application/octet-stream",
      isEncrypted: false,
    };
    expect(await readFinalizeState(upload)).toMatchObject({
      status: "finalized",
      phase: "cleaning",
      verified: totalChunks,
      routes: [],
    });
    await expect(
      userStub(tenant).vfsReadFile(scopeFor(tenant), "/paged.bin")
    ).resolves.toEqual(expectedBytes(totalChunks));

    const published = await stepToDone(upload);
    expect(published).toEqual(expected);
    expect(await readFinalizeState(upload)).toMatchObject({
      phase: "done",
      verified: 0,
      expected: 0,
    });
  });

  it("replays a finished step without publishing again", async () => {
    const tenant = "mp-step-replay";
    const upload = await stageUpload(tenant, "/replay.bin", 4);
    const result = await stepToDone(upload);

    await expect(step(upload)).resolves.toEqual({
      done: true,
      result,
      fresh: false,
    });
    await expect(step(upload)).resolves.toEqual({
      done: true,
      result,
      fresh: false,
    });
    // The scratch a terminal session no longer reads is gone, and the file it
    // published is the only thing left behind.
    expect(await readFinalizeState(upload)).toMatchObject({
      status: "finalized",
      phase: "done",
      verified: 0,
      expected: 0,
      routes: [],
    });
    await expect(
      userStub(tenant).vfsReadFile(scopeFor(tenant), "/replay.bin")
    ).resolves.toEqual(expectedBytes(4));
    // The one-request entry point answers a session it already finished from
    // what publication recorded: a caller whose response was lost has a
    // published file, and telling it EBUSY would be a lie.
    await expect(
      userStub(tenant).vfsFinalizeMultipart(
        scopeFor(tenant),
        upload.uploadId,
        upload.hashes
      )
    ).resolves.toEqual(result);
  });

  it("resumes a verification page whose shard response was lost", async () => {
    const tenant = "mp-step-response-loss";
    const upload = await stageUpload(tenant, "/lost.bin", 6);
    await step(upload);

    const owners = ownerShards(upload);
    await shardStub(
      tenant,
      owners[0]
    ).testConfigureMultipartManifestRangeResponseLoss(1);

    await expect(step(upload)).rejects.toThrow(
      /EBUSY.*shard manifest collect failed on 1 shard\(s\)/
    );
    // The page applied nothing at all: not the shards that did answer, not
    // the cursor, not the routes.
    expect(await readFinalizeState(upload)).toMatchObject({
      status: "finalizing",
      phase: "verifying",
      chunkCursor: 0,
      verifyShardCursor: 0,
      totalSize: 0,
      verified: 0,
      routes: [],
    });

    const result = await stepToDone(upload);
    expect(result.fileHash).toBe(await computeFileHash(upload.hashes));
    expect(result.size).toBe(6 * CHUNK_BYTES);
    await expect(
      userStub(tenant).vfsReadFile(scopeFor(tenant), "/lost.bin")
    ).resolves.toEqual(expectedBytes(6));
  });

  it.each([
    { what: "advanced", column: "finalize_chunk_cursor", value: 2 },
    { what: "rewound", column: "finalize_fence_cursor", value: 0 },
  ])(
    "refuses a step whose session was $what underneath it",
    async ({ what, column, value }) => {
      const tenant = `mp-step-stale-${what}`;
      const upload = await stageUpload(tenant, "/stale.bin", 4);
      await step(upload);
      expect((await readFinalizeState(upload)).phase).toBe("verifying");

      const blocked = shardStub(tenant, ownerShards(upload)[0]);
      await blocked.testConfigureMultipartManifestBlock(upload.uploadId);
      const inflight = step(upload);
      await blocked.testWaitForMultipartManifestBlocked();
      await writeSessionColumn(upload, column, value);
      await blocked.testReleaseMultipartManifestBlock();

      await expect(inflight).rejects.toThrow(
        /EBUSY.*session changed while verifying/
      );
      // Nothing the refused page computed reached the row.
      expect(await readFinalizeState(upload)).toMatchObject({
        status: "finalizing",
        phase: "verifying",
        verifyShardCursor: 0,
        totalSize: 0,
        verified: 0,
        routes: [],
      });
      expect(await readSessionColumn(upload, column)).toBe(value);
    }
  );

  it("refuses a step whose page another step already recorded", async () => {
    const tenant = "mp-step-stale-page";
    const upload = await stageUpload(tenant, "/raced.bin", 4);
    await step(upload);

    const blocked = shardStub(tenant, ownerShards(upload)[0]);
    await blocked.testConfigureMultipartManifestBlock(upload.uploadId);
    const inflight = step(upload);
    await blocked.testWaitForMultipartManifestBlocked();
    // Exactly what a second step racing this one would have committed: the
    // page's rows and the cursor past them.
    await runInDurableObject(
      userStub(tenant),
      (_instance: TestUserDO, state: DurableObjectState) => {
        for (let index = 0; index < upload.totalChunks; index++) {
          state.storage.sql.exec(
            `INSERT INTO upload_verified_chunks
               (upload_id, chunk_index, chunk_hash, chunk_size, shard_index)
             VALUES (?, ?, ?, ?, ?)`,
            upload.uploadId,
            index,
            upload.hashes[index],
            CHUNK_BYTES,
            placeMultipartChunk(
              tenant,
              upload.uploadId,
              index,
              upload.poolSize,
              MULTIPART_PLACEMENT_VERSION
            )
          );
        }
        state.storage.sql.exec(
          `UPDATE upload_sessions
              SET finalize_chunk_cursor = ?, finalize_phase = 'publishing'
            WHERE upload_id = ?`,
          upload.totalChunks,
          upload.uploadId
        );
      }
    );
    await blocked.testReleaseMultipartManifestBlock();

    // The driver's verdict, not a primary-key error from the rows the losing
    // page tried to write.
    await expect(inflight).rejects.toThrow(
      /EBUSY.*session changed while verifying/
    );
    expect(await readFinalizeState(upload)).toMatchObject({
      status: "finalizing",
      phase: "publishing",
      chunkCursor: upload.totalChunks,
      verified: upload.totalChunks,
      totalSize: 0,
    });
  });

  it("finalizes an upload spanning several pages in one request", async () => {
    const tenant = "mp-step-one-request";
    const totalChunks = MULTIPART_HASH_PAGE_SIZE + 44;
    const scope = scopeFor(tenant);
    const begin = await userStub(tenant).vfsBeginMultipart(
      scope,
      "/one-request.bin",
      { size: totalChunks * CHUNK_BYTES, chunkSize: CHUNK_BYTES }
    );
    const hashes: string[] = [];
    for (let index = 0; index < totalChunks; index++) {
      const bytes = chunkBytes(index);
      const hash = await hashChunk(bytes);
      hashes.push(hash);
      await shardStub(
        tenant,
        placeMultipartChunk(
          tenant,
          begin.uploadId,
          index,
          begin.poolSize,
          MULTIPART_PLACEMENT_VERSION
        )
      ).putChunkMultipart(
        hash,
        bytes,
        begin.uploadId,
        index,
        tenant,
        begin.sessionToken
      );
    }

    // No hash page was staged first: the one-request entry point stages the
    // manifest it was handed and then drives the same machine.
    await expect(
      userStub(tenant).vfsFinalizeMultipart(scope, begin.uploadId, hashes)
    ).resolves.toEqual({
      fileId: begin.uploadId,
      size: totalChunks * CHUNK_BYTES,
      chunkCount: totalChunks,
      fileHash: await computeFileHash(hashes),
      path: "/one-request.bin",
      mimeType: "application/octet-stream",
      isEncrypted: false,
    });
    await expect(
      userStub(tenant).vfsReadFile(scope, "/one-request.bin")
    ).resolves.toEqual(expectedBytes(totalChunks));
  });

  it("refuses a one-request finalize that changes a staged hash", async () => {
    const tenant = "mp-step-hash-conflict";
    const upload = await stageUpload(tenant, "/conflict.bin", 4);
    await step(upload);

    const rewritten = [...upload.hashes];
    rewritten[2] = await hashChunk(chunkBytes(999));
    await expect(
      userStub(tenant).vfsFinalizeMultipart(
        scopeFor(tenant),
        upload.uploadId,
        rewritten
      )
    ).rejects.toThrow(/EBUSY.*chunk 2 differs from the hash staged/);

    // The finalize already in flight is untouched and still completes.
    const result = await stepToDone(upload);
    expect(result.fileHash).toBe(await computeFileHash(upload.hashes));
  });
});
