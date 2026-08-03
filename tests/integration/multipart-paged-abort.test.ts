import {
  env,
  runDurableObjectAlarm,
  runInDurableObject,
} from "cloudflare:test";
import { describe, expect, it, vi } from "vitest";

import { vfsShardDOName, vfsUserDOName } from "@core/lib/utils";
import { vfsAbortMultipartStep } from "@core/objects/user/multipart-upload";
import type { ShardDO } from "@core/objects/shard/shard-do";
import type { UserDO } from "@app/objects/user/user-do";
import { hashChunk } from "@shared/crypto";
import {
  MULTIPART_FENCE_PAGE_SIZE,
  MULTIPART_HASH_PAGE_SIZE,
  MULTIPART_PLACEMENT_VERSION,
  MULTIPART_PROTOCOL_VERSION,
  type MultipartAbortProgress,
} from "@shared/multipart";
import { placeMultipartChunk } from "@shared/placement";
import type { VFSScope } from "@shared/vfs-types";

/**
 * The durable multipart abort machine.
 *
 * An abort undoes an upload, so it is as large as the upload: a terminal fence
 * and a cleanup intent per pool shard, and — once a finalize verified
 * anything — a row per chunk in the scratch tables and in the manifest
 * verification materialised. These pin the properties that make undoing it in
 * pages safe:
 *
 *   - a pool wider than one fence page is fenced and staged across calls, and
 *     survives a Durable Object eviction between them,
 *   - the manifests a verified upload staged are dropped a chunk page at a
 *     time rather than in one transaction,
 *   - no intermediate page reports the abort terminal, and the one-request
 *     entry point refuses rather than claiming a cleanup that has not run,
 *   - a chunk PUT arriving after the fence is rejected,
 *   - a finalize and an abort never both win, and
 *   - the alarm finishes an abort nobody came back for, whether or not the
 *     session's own deadline has passed.
 */

// Fencing and intent staging instantiate every shard of a pool wider than one
// page, and the verified-upload case PUTs two full verification pages of
// chunks — far more Durable Object traffic than an ordinary test does.
vi.setConfig({ testTimeout: 120_000 });

interface UserFaultControls {
  testEvict(): Promise<void>;
}

interface ShardFaultControls {
  testConfigureFenceMultipartFailure(remaining: number | null): Promise<void>;
  testClearFenceMultipartFailure(): Promise<void>;
  testConfigureMultipartManifestBlock(uploadId: string): Promise<void>;
  testWaitForMultipartManifestBlocked(): Promise<void>;
  testReleaseMultipartManifestBlock(): Promise<void>;
}

type TestUserDO = UserDO & UserFaultControls;
type TestShardDO = ShardDO & ShardFaultControls;

interface TestEnv {
  MOSSAIC_USER: DurableObjectNamespace<TestUserDO>;
  MOSSAIC_SHARD: DurableObjectNamespace<TestShardDO>;
}

const TEST_ENV = env as unknown as TestEnv;
const NS = "default";
const CHUNK_BYTES = 2;

/** Two full verification pages and a short one. */
const LARGE_CHUNKS = 2 * MULTIPART_HASH_PAGE_SIZE + 8;

function scopeFor(tenant: string): VFSScope {
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

function expectedBytes(totalChunks: number): Uint8Array {
  const out = new Uint8Array(totalChunks * CHUNK_BYTES);
  for (let index = 0; index < totalChunks; index++) {
    out.set(chunkBytes(index), index * CHUNK_BYTES);
  }
  return out;
}

interface Upload {
  tenant: string;
  uploadId: string;
  poolSize: number;
  totalChunks: number;
  hashes: string[];
  sessionToken: string;
}

/** Fix the tenant's pool so page boundaries never move. */
async function pinPool(tenant: string, poolSize: number): Promise<void> {
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

function ownerShard(upload: Upload, index: number): number {
  return placeMultipartChunk(
    upload.tenant,
    upload.uploadId,
    index,
    upload.poolSize,
    MULTIPART_PLACEMENT_VERSION
  );
}

/** Begin a session, PUT every chunk, and stage the declared manifest. */
async function stageUpload(
  tenant: string,
  path: string,
  poolSize: number,
  totalChunks: number
): Promise<Upload> {
  const scope = scopeFor(tenant);
  await pinPool(tenant, poolSize);
  const begin = await userStub(tenant).vfsBeginMultipart(scope, path, {
    size: totalChunks * CHUNK_BYTES,
    chunkSize: CHUNK_BYTES,
    protocolVersion: MULTIPART_PROTOCOL_VERSION,
  });
  expect(begin.poolSize).toBe(poolSize);
  const upload: Upload = {
    tenant,
    uploadId: begin.uploadId,
    poolSize,
    totalChunks,
    hashes: new Array<string>(totalChunks),
    sessionToken: begin.sessionToken,
  };

  const CONCURRENCY = 16;
  for (let base = 0; base < totalChunks; base += CONCURRENCY) {
    await Promise.all(
      Array.from(
        { length: Math.min(CONCURRENCY, totalChunks - base) },
        async (_unused, offset) => {
          const index = base + offset;
          const bytes = chunkBytes(index);
          const hash = await hashChunk(bytes);
          upload.hashes[index] = hash;
          await shardStub(tenant, ownerShard(upload, index)).putChunkMultipart(
            hash,
            bytes,
            upload.uploadId,
            index,
            tenant,
            begin.sessionToken
          );
        }
      )
    );
  }
  for (let start = 0; start < totalChunks; start += MULTIPART_HASH_PAGE_SIZE) {
    await userStub(tenant).vfsStageMultipartHashes(
      scope,
      upload.uploadId,
      start,
      upload.hashes.slice(start, start + MULTIPART_HASH_PAGE_SIZE)
    );
  }
  return upload;
}

function abortStep(
  upload: Upload,
  allowFinalizing = false
): Promise<MultipartAbortProgress> {
  if (!allowFinalizing) {
    return userStub(upload.tenant).vfsAbortMultipartStep(
      scopeFor(upload.tenant),
      upload.uploadId
    );
  }
  // Taking a finalizing session over is the machine's own privilege, not one
  // the RPC surface grants, so this drives the same page the finalize's
  // release path drives.
  return runInDurableObject(userStub(upload.tenant), (instance: TestUserDO) =>
    vfsAbortMultipartStep(
      instance,
      scopeFor(upload.tenant),
      upload.uploadId,
      true
    )
  );
}

interface AbortState {
  status: string;
  phase: string | null;
  fenceCursor: number;
  intentCursor: number;
  cleanupCursor: number;
  routeCursor: number;
  attempts: number;
  expected: number;
  verified: number;
  manifest: number;
  routes: number;
  intents: number;
  temporaryRows: number;
}

async function readAbortState(upload: Upload): Promise<AbortState> {
  return runInDurableObject(
    userStub(upload.tenant),
    (_instance: TestUserDO, state: DurableObjectState) => {
      const sql = state.storage.sql;
      const session = sql
        .exec<{
          status: string;
          abort_phase: string | null;
          abort_fence_cursor: number;
          abort_intent_cursor: number;
          abort_cleanup_cursor: number;
          abort_old_intent_cursor: number;
          attempts: number;
        }>(
          `SELECT status, abort_phase, abort_fence_cursor, abort_intent_cursor,
                  abort_cleanup_cursor, abort_old_intent_cursor, attempts
             FROM upload_sessions WHERE upload_id = ?`,
          upload.uploadId
        )
        .toArray()
        .at(0);
      const count = (query: string, ...bindings: string[]): number =>
        sql.exec<{ n: number }>(query, ...bindings).toArray()[0].n;
      return {
        status: session?.status ?? "missing",
        phase: session?.abort_phase ?? null,
        fenceCursor: session?.abort_fence_cursor ?? -2,
        intentCursor: session?.abort_intent_cursor ?? -2,
        cleanupCursor: session?.abort_cleanup_cursor ?? -2,
        routeCursor: session?.abort_old_intent_cursor ?? -2,
        attempts: session?.attempts ?? -1,
        expected: count(
          "SELECT COUNT(*) AS n FROM upload_expected_chunks WHERE upload_id = ?",
          upload.uploadId
        ),
        verified: count(
          "SELECT COUNT(*) AS n FROM upload_verified_chunks WHERE upload_id = ?",
          upload.uploadId
        ),
        manifest: count(
          "SELECT COUNT(*) AS n FROM file_chunks WHERE file_id = ?",
          upload.uploadId
        ),
        routes: count(
          "SELECT COUNT(*) AS n FROM upload_cleanup_routes WHERE upload_id = ?",
          upload.uploadId
        ),
        intents: count(
          "SELECT COUNT(*) AS n FROM chunk_cleanup_intents WHERE ref_id = ?",
          upload.uploadId
        ),
        temporaryRows: count(
          "SELECT COUNT(*) AS n FROM files WHERE file_id = ?",
          upload.uploadId
        ),
      };
    }
  );
}

function finalizeStep(upload: Upload): Promise<{ done: boolean }> {
  return userStub(upload.tenant).vfsFinalizeMultipartStep(
    scopeFor(upload.tenant),
    upload.uploadId
  );
}

/** Advance the finalize until it is about to publish. */
async function stepToPublishing(upload: Upload): Promise<void> {
  for (let page = 0; page < 64; page++) {
    const phase = await runInDurableObject(
      userStub(upload.tenant),
      (_instance: TestUserDO, state: DurableObjectState) =>
        state.storage.sql
          .exec<{ finalize_phase: string | null }>(
            "SELECT finalize_phase FROM upload_sessions WHERE upload_id = ?",
            upload.uploadId
          )
          .toArray()[0].finalize_phase
    );
    if (phase === "publishing") return;
    const progress = await finalizeStep(upload);
    if (progress.done) throw new Error("finalize completed before publishing");
  }
  throw new Error("finalize never reached publishing");
}

function putChunk(upload: Upload, index: number): Promise<unknown> {
  return shardStub(upload.tenant, ownerShard(upload, index)).putChunkMultipart(
    upload.hashes[index],
    chunkBytes(index),
    upload.uploadId,
    index,
    upload.tenant,
    upload.sessionToken
  );
}

describe("paged multipart abort", () => {
  it("fences and stages a pool wider than one page across calls", async () => {
    const tenant = "mp-abort-wide-pool";
    const poolSize = MULTIPART_FENCE_PAGE_SIZE + 6;
    const upload = await stageUpload(tenant, "/wide.bin", poolSize, 4);

    await expect(abortStep(upload)).resolves.toEqual({
      done: false,
      phase: "fencing",
      cursor: MULTIPART_FENCE_PAGE_SIZE,
      total: poolSize,
    });
    expect(await readAbortState(upload)).toMatchObject({
      status: "aborting",
      phase: "fencing",
      fenceCursor: MULTIPART_FENCE_PAGE_SIZE,
      intents: 0,
    });

    // The rest of the pool is fenced by a different instance of the object.
    await expect(userStub(tenant).testEvict()).rejects.toThrow(
      /injected UserDO eviction/
    );
    await expect(abortStep(upload)).resolves.toEqual({
      done: false,
      phase: "intents",
      cursor: 0,
      total: poolSize,
    });

    // Every shard of the pool now holds the terminal fence, so a straggler
    // PUT is refused even though the abort is nowhere near terminal.
    await expect(putChunk(upload, 0)).rejects.toThrow(
      /EBUSY.*multipart upload is aborting/
    );
    expect(await readAbortState(upload)).toMatchObject({
      status: "aborting",
      phase: "intents",
      fenceCursor: poolSize,
    });

    // Cleanup intents are staged a page of shards at a time, and the drain
    // that executes them waits for the terminal page.
    await expect(abortStep(upload)).resolves.toEqual({
      done: false,
      phase: "intents",
      cursor: MULTIPART_FENCE_PAGE_SIZE,
      total: poolSize,
    });
    expect(await readAbortState(upload)).toMatchObject({
      intents: MULTIPART_FENCE_PAGE_SIZE,
    });
    await expect(abortStep(upload)).resolves.toEqual({
      done: false,
      phase: "cleanup",
      cursor: 0,
      total: 4,
    });
    expect(await readAbortState(upload)).toMatchObject({
      phase: "cleanup",
      intentCursor: poolSize,
      intents: poolSize,
    });

    // Nothing was verified, so one cleanup page covers the manifest range and
    // the routing phase finds nothing to discard.
    await expect(abortStep(upload)).resolves.toEqual({
      done: false,
      phase: "old_intents",
      cursor: 0,
      total: poolSize,
    });
    await expect(abortStep(upload)).resolves.toEqual({
      done: false,
      phase: "local",
      cursor: 0,
      total: 1,
    });
    // Not one of those pages claimed to be terminal, and the session was not
    // terminal on any of them.
    expect(await readAbortState(upload)).toMatchObject({
      status: "aborting",
      phase: "local",
    });

    await expect(abortStep(upload)).resolves.toEqual({ done: true });
    expect(await readAbortState(upload)).toMatchObject({
      status: "aborted",
      phase: "done",
      expected: 0,
      routes: 0,
      // The terminal page drained the intents it staged, so the shards have
      // already dropped this upload's refs and staging.
      intents: 0,
      temporaryRows: 0,
    });
    expect(
      await runInDurableObject(
        shardStub(tenant, ownerShard(upload, 0)),
        (_instance: TestShardDO, state: DurableObjectState) =>
          state.storage.sql
            .exec<{ n: number }>(
              "SELECT COUNT(*) AS n FROM upload_chunks WHERE upload_id = ?",
              upload.uploadId
            )
            .toArray()[0].n
      )
    ).toBe(0);

    // Idempotent: a caller that lost the response gets the terminal answer.
    await expect(abortStep(upload)).resolves.toEqual({ done: true });
    await expect(
      userStub(tenant).vfsAbortMultipart(scopeFor(tenant), upload.uploadId)
    ).resolves.toEqual({ ok: true });
  });

  it("drops the manifests a verified upload staged a chunk page at a time", async () => {
    const tenant = "mp-abort-verified";
    const upload = await stageUpload(tenant, "/verified.bin", 1, LARGE_CHUNKS);
    await stepToPublishing(upload);
    // Verification materialised the destination manifest and the scratch it
    // was built from — all three as large as the upload.
    expect(await readAbortState(upload)).toMatchObject({
      status: "finalizing",
      expected: LARGE_CHUNKS,
      verified: LARGE_CHUNKS,
      manifest: LARGE_CHUNKS,
      routes: 1,
    });

    // Fencing, then intent staging: one page each for a single-shard pool.
    await expect(abortStep(upload, true)).resolves.toEqual({
      done: false,
      phase: "intents",
      cursor: 0,
      total: 1,
    });
    await expect(abortStep(upload, true)).resolves.toEqual({
      done: false,
      phase: "cleanup",
      cursor: 0,
      total: LARGE_CHUNKS,
    });

    // Each cleanup page takes one chunk page out of every table at once, so
    // the rows go away in observable steps instead of one transaction. The
    // second of them runs on a different instance of the object, so the middle
    // page is only reachable if the cursor survived in the row.
    const remaining: Array<[number, number, number]> = [];
    for (let page = 0; page < 3; page++) {
      if (page === 1) {
        await expect(userStub(tenant).testEvict()).rejects.toThrow(
          /injected UserDO eviction/
        );
      }
      const progress = await abortStep(upload, true);
      const state = await readAbortState(upload);
      remaining.push([state.expected, state.verified, state.manifest]);
      expect(progress.done).toBe(false);
    }
    expect(remaining).toEqual([
      [LARGE_CHUNKS - MULTIPART_HASH_PAGE_SIZE, LARGE_CHUNKS - MULTIPART_HASH_PAGE_SIZE, LARGE_CHUNKS - MULTIPART_HASH_PAGE_SIZE],
      [8, 8, 8],
      [0, 0, 0],
    ]);
    expect(await readAbortState(upload)).toMatchObject({
      status: "aborting",
      phase: "old_intents",
      cleanupCursor: LARGE_CHUNKS,
    });

    // The routing verification recorded is discarded rather than executed:
    // this abort displaces nothing.
    await expect(abortStep(upload, true)).resolves.toEqual({
      done: false,
      phase: "local",
      cursor: 0,
      total: 1,
    });
    expect(await readAbortState(upload)).toMatchObject({ routes: 0 });
    await expect(abortStep(upload, true)).resolves.toEqual({ done: true });
    expect(await readAbortState(upload)).toMatchObject({
      status: "aborted",
      phase: "done",
      expected: 0,
      verified: 0,
      manifest: 0,
      routes: 0,
      temporaryRows: 0,
    });
    // The path the upload was headed for is free again.
    await expect(
      userStub(tenant).vfsExists(scopeFor(tenant), "/verified.bin")
    ).resolves.toBe(false);
  });

  it("refuses rather than reporting a cleanup that has not run", async () => {
    const tenant = "mp-abort-not-terminal";
    const upload = await stageUpload(tenant, "/stuck.bin", 2, 1);
    // One shard of the pool never answers, so the fence page can never finish.
    await shardStub(tenant, 1).testConfigureFenceMultipartFailure(null);

    await expect(
      userStub(tenant).vfsAbortMultipart(scopeFor(tenant), upload.uploadId)
    ).rejects.toThrow(/injected multipart fence failure/);
    // The session is owned by the machine and nothing was reported terminal.
    expect(await readAbortState(upload)).toMatchObject({
      status: "aborting",
      phase: "fencing",
      fenceCursor: 0,
      temporaryRows: 1,
    });

    await shardStub(tenant, 1).testClearFenceMultipartFailure();
    await expect(
      userStub(tenant).vfsAbortMultipart(scopeFor(tenant), upload.uploadId)
    ).resolves.toEqual({ ok: true });
    expect(await readAbortState(upload)).toMatchObject({
      status: "aborted",
      phase: "done",
      temporaryRows: 0,
    });
  });

  it("finishes an unexpired abort nobody came back for", async () => {
    const tenant = "mp-abort-alarm";
    const upload = await stageUpload(tenant, "/abandoned.bin", 2, 2);
    // One page, then walk away — with a deadline a day out, so only the
    // aborting status can bring the sweep back to it.
    const steppedAt = Date.now();
    await expect(abortStep(upload)).resolves.toMatchObject({ done: false });
    expect(await readAbortState(upload)).toMatchObject({ status: "aborting" });
    // A page that left work owed armed the alarm promptly rather than leaving
    // it on the ordinary ten-minute maintenance cadence.
    const armed = await runInDurableObject(
      userStub(tenant),
      (_instance: TestUserDO, state: DurableObjectState) =>
        state.storage.getAlarm()
    );
    expect(armed).not.toBeNull();
    expect(armed).toBeLessThanOrEqual(steppedAt + 5_000);

    expect(await runDurableObjectAlarm(userStub(tenant))).toBe(true);
    expect(await readAbortState(upload)).toMatchObject({
      status: "aborted",
      phase: "done",
      expected: 0,
      intents: 0,
      temporaryRows: 0,
    });
  });

  it("keeps a finalize in progress out of the abort RPC's reach", async () => {
    const tenant = "mp-abort-vs-finalize-rpc";
    const upload = await stageUpload(tenant, "/contested.bin", 1, 3);
    await finalizeStep(upload);
    expect(await readAbortState(upload)).toMatchObject({
      status: "finalizing",
    });

    await expect(
      userStub(tenant).vfsAbortMultipart(scopeFor(tenant), upload.uploadId)
    ).rejects.toThrow(/EBUSY.*finalize is in progress/);
    await expect(abortStep(upload)).rejects.toThrow(
      /EBUSY.*finalize is in progress/
    );
    // Refused without touching the finalize, which still publishes.
    expect(await readAbortState(upload)).toMatchObject({
      status: "finalizing",
      phase: null,
    });
    for (let page = 0; page < 8; page++) {
      if ((await finalizeStep(upload)).done) break;
    }
    await expect(
      userStub(tenant).vfsReadFile(scopeFor(tenant), "/contested.bin")
    ).resolves.toEqual(expectedBytes(3));
    // And once it is published there is nothing left to un-finalize.
    await expect(
      userStub(tenant).vfsAbortMultipart(scopeFor(tenant), upload.uploadId)
    ).rejects.toThrow(/EBUSY.*already finalized/);
  });

  it("refuses a finalize page whose session an abort took over", async () => {
    const tenant = "mp-abort-vs-finalize-page";
    const upload = await stageUpload(tenant, "/raced.bin", 1, 3);
    await finalizeStep(upload);

    const blocked = shardStub(tenant, ownerShard(upload, 0));
    await blocked.testConfigureMultipartManifestBlock(upload.uploadId);
    const verifying = finalizeStep(upload);
    await blocked.testWaitForMultipartManifestBlocked();
    // The release path's privilege, exercised while a verification page is in
    // flight: the abort takes the status that page has to commit against.
    await expect(abortStep(upload, true)).resolves.toMatchObject({
      done: false,
    });
    await blocked.testReleaseMultipartManifestBlock();

    await expect(verifying).rejects.toThrow(
      /EBUSY.*session changed while verifying/
    );
    // Nothing the refused page computed reached the row, and the abort owns
    // the session outright.
    expect(await readAbortState(upload)).toMatchObject({
      status: "aborting",
      verified: 0,
      manifest: 0,
    });
    // Every later finalize page is refused too — the abort has already fenced
    // the shards this upload's bytes are on.
    await expect(finalizeStep(upload)).rejects.toThrow(
      /EBUSY.*session status='aborting'/
    );

    // The public RPC grants no `allowFinalizing`, but the session is already
    // aborting, so this finishes the machine the release path armed.
    await expect(
      userStub(tenant).vfsAbortMultipart(scopeFor(tenant), upload.uploadId)
    ).resolves.toEqual({ ok: true });
    expect(await readAbortState(upload)).toMatchObject({
      status: "aborted",
      phase: "done",
      temporaryRows: 0,
    });
    await expect(
      userStub(tenant).vfsExists(scopeFor(tenant), "/raced.bin")
    ).resolves.toBe(false);
  });
});
