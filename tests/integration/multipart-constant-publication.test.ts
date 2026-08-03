import {
  env,
  runDurableObjectAlarm,
  runInDurableObject,
} from "cloudflare:test";
import { describe, expect, it, vi } from "vitest";

import { vfsShardDOName, vfsUserDOName } from "@core/lib/utils";
import type { ShardDO } from "@core/objects/shard/shard-do";
import type { UserDO } from "@app/objects/user/user-do";
import { computeFileHash, hashChunk } from "@shared/crypto";
import {
  MULTIPART_HASH_PAGE_SIZE,
  MULTIPART_PLACEMENT_VERSION,
  MULTIPART_PROTOCOL_VERSION,
  type MultipartFinalizeProgress,
  type MultipartFinalizeResponse,
} from "@shared/multipart";
import { placeMultipartChunk } from "@shared/placement";
import type { SqlMetrics } from "../bench/counting-sql-storage";

/**
 * Publication, and what it deliberately leaves owed.
 *
 * Publishing is the one step of the finalize machine that cannot be split: it
 * either switches the path onto the new file or it does not. So it is the one
 * step that must not grow with the upload. Verification copies each verified
 * page into the destination manifest, preparation routes the shards a
 * displaced file's bytes live on, and publication only decides — which is what
 * these pin:
 *
 *   - the SQL a publication issues is identical for a four-chunk upload and a
 *     five-hundred-chunk one,
 *   - a non-versioned overwrite leaves the displaced manifest in place and
 *     reaps it a bounded page at a time afterwards,
 *   - a decision that changed after the freeze is refused, not applied,
 *   - a caller that lost the response gets the recorded result back,
 *   - the alarm finishes cleaning nobody came back for, bounded per run, and
 *   - the one-request finalize still works, still succeeds while cleaning is
 *     outstanding, and refuses at begin an upload it could never finalize.
 */

// Publishing a five-hundred-chunk upload pays a shard round-trip per chunk
// PUT, which is far more Durable Object traffic than an ordinary test does.
vi.setConfig({ testTimeout: 120_000 });

interface UserFaultControls {
  testEvict(): Promise<void>;
  testResetSqlMetrics(): Promise<void>;
  testSqlMetrics(): Promise<SqlMetrics>;
}

type TestUserDO = UserDO & UserFaultControls;

interface TestEnv {
  MOSSAIC_USER: DurableObjectNamespace<TestUserDO>;
  MOSSAIC_SHARD: DurableObjectNamespace<ShardDO>;
}

const TEST_ENV = env as unknown as TestEnv;
const NS = "default";
const CHUNK_BYTES = 2;

/** Every upload here lands on one shard, so publication routes exactly one. */
const POOL_SIZE = 1;

/** Two full verification pages and a short one. */
const LARGE_CHUNKS = 2 * MULTIPART_HASH_PAGE_SIZE + 8;

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
): DurableObjectStub<ShardDO> {
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
  path: string;
  uploadId: string;
  totalChunks: number;
  hashes: string[];
}

/**
 * Pin the tenant to a single shard so route counts never move. Every
 * publication grows the pool back from its own byte accounting, so this runs
 * before each upload rather than once per tenant.
 */
async function pinPool(tenant: string): Promise<void> {
  await userStub(tenant).vfsExists(scopeFor(tenant), "/");
  await runInDurableObject(
    userStub(tenant),
    (_instance: TestUserDO, state: DurableObjectState) => {
      state.storage.sql.exec(
        `INSERT INTO quota (user_id, pool_size) VALUES (?, ?)
           ON CONFLICT(user_id) DO UPDATE SET pool_size = excluded.pool_size`,
        tenant,
        POOL_SIZE
      );
    }
  );
}

/** Begin a session, PUT every chunk, and stage the declared manifest. */
async function stageUpload(
  tenant: string,
  path: string,
  totalChunks: number
): Promise<Upload> {
  const scope = scopeFor(tenant);
  await pinPool(tenant);
  const begin = await userStub(tenant).vfsBeginMultipart(scope, path, {
    size: totalChunks * CHUNK_BYTES,
    chunkSize: CHUNK_BYTES,
    protocolVersion: MULTIPART_PROTOCOL_VERSION,
  });
  expect(begin.poolSize).toBe(POOL_SIZE);
  expect(begin.protocolVersion).toBe(MULTIPART_PROTOCOL_VERSION);

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
  for (let start = 0; start < totalChunks; start += MULTIPART_HASH_PAGE_SIZE) {
    await userStub(tenant).vfsStageMultipartHashes(
      scope,
      begin.uploadId,
      start,
      hashes.slice(start, start + MULTIPART_HASH_PAGE_SIZE)
    );
  }
  return { tenant, path, uploadId: begin.uploadId, totalChunks, hashes };
}

function step(upload: Upload): Promise<MultipartFinalizeProgress> {
  return userStub(upload.tenant).vfsFinalizeMultipartStep(
    scopeFor(upload.tenant),
    upload.uploadId
  );
}

/** Advance until the machine is about to publish. */
async function stepToPublishing(upload: Upload): Promise<number> {
  for (let pages = 0; pages < 64; pages++) {
    if ((await readSession(upload)).phase === "publishing") return pages;
    const progress = await step(upload);
    if (progress.done) throw new Error("finalize completed before publishing");
  }
  throw new Error("finalize never reached publishing");
}

async function stepToDone(upload: Upload): Promise<MultipartFinalizeResponse> {
  for (let attempt = 0; attempt < 64; attempt++) {
    const progress = await step(upload);
    if (progress.done) return progress.result;
  }
  throw new Error("finalize did not reach a terminal state");
}

interface SessionState {
  status: string;
  phase: string | null;
  expected: number;
  verified: number;
  routes: number;
}

async function readSession(upload: Upload): Promise<SessionState> {
  return runInDurableObject(
    userStub(upload.tenant),
    (_instance: TestUserDO, state: DurableObjectState) => {
      const sql = state.storage.sql;
      const row = sql
        .exec<{ status: string; finalize_phase: string | null }>(
          "SELECT status, finalize_phase FROM upload_sessions WHERE upload_id = ?",
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
        status: row?.status ?? "missing",
        phase: row?.finalize_phase ?? null,
        expected: count("upload_expected_chunks"),
        verified: count("upload_verified_chunks"),
        routes: count("upload_cleanup_routes"),
      };
    }
  );
}

function countManifestRows(tenant: string, fileId: string): Promise<number> {
  return runInDurableObject(
    userStub(tenant),
    (_instance: TestUserDO, state: DurableObjectState) =>
      state.storage.sql
        .exec<{ n: number }>(
          "SELECT COUNT(*) AS n FROM file_chunks WHERE file_id = ?",
          fileId
        )
        .toArray()[0].n
  );
}

type LiveFile = {
  file_id: string;
  file_size: number;
  file_hash: string;
  chunk_count: number;
};

function readLiveFile(
  tenant: string,
  leaf: string
): Promise<LiveFile | undefined> {
  return runInDurableObject(
    userStub(tenant),
    (_instance: TestUserDO, state: DurableObjectState) =>
      state.storage.sql
        .exec<LiveFile>(
          `SELECT file_id, file_size, file_hash, chunk_count FROM files
            WHERE file_name = ? AND status = 'complete'`,
          leaf
        )
        .toArray()
        .at(0)
  );
}

function countFileRows(tenant: string, fileId: string): Promise<number> {
  return runInDurableObject(
    userStub(tenant),
    (_instance: TestUserDO, state: DurableObjectState) =>
      state.storage.sql
        .exec<{ n: number }>(
          "SELECT COUNT(*) AS n FROM files WHERE file_id = ?",
          fileId
        )
        .toArray()[0].n
  );
}

function countShardRefs(tenant: string, fileId: string): Promise<number> {
  return runInDurableObject(
    shardStub(tenant, 0),
    (_instance: ShardDO, state: DurableObjectState) =>
      state.storage.sql
        .exec<{ n: number }>(
          "SELECT COUNT(*) AS n FROM chunk_refs WHERE file_id = ?",
          fileId
        )
        .toArray()[0].n
  );
}

function mutateSession(
  tenant: string,
  sql: string,
  ...bindings: string[]
): Promise<void> {
  return runInDurableObject(
    userStub(tenant),
    (_instance: TestUserDO, state: DurableObjectState) => {
      state.storage.sql.exec(sql, ...bindings);
    }
  );
}

/**
 * Publish one upload on a tenant that already holds exactly one file, with the
 * SQL the publication costs counted.
 *
 * The prior file is what makes two measurements comparable: publication reads
 * and writes a handful of rows keyed on the destination path, and those scans
 * see one more row on a tenant that has published more often. Holding the
 * tenant's shape fixed leaves the manifest size as the only difference.
 */
async function measurePublication(
  tenant: string,
  totalChunks: number
): Promise<{ cost: SqlMetrics; upload: Upload }> {
  await stepToDone(await stageUpload(tenant, "/warm.bin", 1));
  const upload = await stageUpload(tenant, "/measured.bin", totalChunks);
  await stepToPublishing(upload);
  await userStub(tenant).testResetSqlMetrics();
  const progress = await step(upload);
  expect(progress.done).toBe(false);
  return { cost: await userStub(tenant).testSqlMetrics(), upload };
}

/** The counters that must not move with the manifest size. */
function publicationCost(metrics: SqlMetrics): Omit<SqlMetrics, "other"> {
  return {
    statements: metrics.statements,
    reads: metrics.reads,
    writes: metrics.writes,
    rowsRead: metrics.rowsRead,
    rowsWritten: metrics.rowsWritten,
  };
}

describe("constant-size multipart publication", () => {
  it("costs the same SQL whatever the manifest size", async () => {
    const small = await measurePublication("mp-pub-constant-small", 4);
    const large = await measurePublication(
      "mp-pub-constant-large",
      LARGE_CHUNKS
    );

    expect(publicationCost(large.cost)).toEqual(publicationCost(small.cost));
    // And small in absolute terms: a publication is a decision plus a head
    // switch, not a walk over anything.
    expect(large.cost.statements).toBeLessThanOrEqual(64);
    expect(large.cost.rowsWritten).toBeLessThanOrEqual(64);
    expect(large.cost.rowsRead).toBeLessThanOrEqual(64);

    // The manifest is nonetheless whole: verification wrote it a page at a
    // time, which is the only reason publication had nothing to write.
    const tenant = large.upload.tenant;
    expect(await countManifestRows(tenant, large.upload.uploadId)).toBe(
      LARGE_CHUNKS
    );
    const published = await stepToDone(large.upload);
    expect(published.fileHash).toBe(
      await computeFileHash(large.upload.hashes)
    );
    expect(await readLiveFile(tenant, "measured.bin")).toEqual({
      file_id: large.upload.uploadId,
      file_size: LARGE_CHUNKS * CHUNK_BYTES,
      file_hash: published.fileHash,
      chunk_count: LARGE_CHUNKS,
    });
  });

  it("reaps a displaced manifest in pages after the switch", async () => {
    const tenant = "mp-pub-overwrite";
    const displaced = await stageUpload(tenant, "/over.bin", LARGE_CHUNKS);
    await stepToDone(displaced);
    expect(await countManifestRows(tenant, displaced.uploadId)).toBe(
      LARGE_CHUNKS
    );

    const fresh = await stageUpload(tenant, "/over.bin", 2);
    // fencing, verification, then one routing page per 256 displaced rows.
    const pagesToPublish = await stepToPublishing(fresh);
    expect(pagesToPublish).toBe(2 + Math.ceil(LARGE_CHUNKS / MULTIPART_HASH_PAGE_SIZE));

    await expect(step(fresh)).resolves.toEqual({
      done: false,
      phase: "cleaning",
      cursor: 0,
      total: 2,
    });
    // The switch is durable and constant-size: the path serves the new bytes,
    // the displaced row is gone, and every one of its manifest rows is still
    // there waiting to be paged off.
    await expect(
      userStub(tenant).vfsReadFile(scopeFor(tenant), "/over.bin")
    ).resolves.toEqual(expectedBytes(2));
    expect(await countFileRows(tenant, displaced.uploadId)).toBe(0);
    expect(await countManifestRows(tenant, displaced.uploadId)).toBe(
      LARGE_CHUNKS
    );
    expect(await readSession(fresh)).toMatchObject({
      status: "finalized",
      phase: "cleaning_old_manifest",
      routes: 0,
    });
    // Publication converted the routing it froze into executable cleanup, so
    // the displaced file's shard refs are already gone.
    expect(await countShardRefs(tenant, displaced.uploadId)).toBe(0);

    const remaining: number[] = [];
    for (let page = 0; page < 8; page++) {
      const progress = await step(fresh);
      remaining.push(await countManifestRows(tenant, displaced.uploadId));
      if (progress.done) break;
    }
    expect(remaining).toEqual([264, 8, 0, 0]);
    expect(await readSession(fresh)).toMatchObject({
      phase: "done",
      expected: 0,
      verified: 0,
    });
  });

  it.each([
    {
      what: "a destination that appeared",
      mutate: (tenant: string, uploadId: string) =>
        mutateSession(
          tenant,
          `INSERT INTO files
             (file_id, user_id, parent_id, file_name, file_size, file_hash,
              mime_type, chunk_size, chunk_count, pool_size, status,
              created_at, updated_at, mode, node_kind)
           VALUES (?, ?, NULL, 'frozen.bin', 0, '', 'application/octet-stream',
                   1, 0, 1, 'complete', 1, 1, 420, 'file')`,
          `intruder-${uploadId}`,
          tenant
        ),
      refusal: /the destination changed since the finalize froze/,
    },
    {
      what: "a moved destination path",
      mutate: (tenant: string, uploadId: string) =>
        mutateSession(
          tenant,
          "UPDATE upload_sessions SET leaf = 'elsewhere.bin' WHERE upload_id = ?",
          uploadId
        ),
      refusal: /the destination path changed since the finalize froze/,
    },
    {
      what: "versioning switched on",
      mutate: (tenant: string) =>
        mutateSession(
          tenant,
          "UPDATE quota SET versioning_enabled = 1 WHERE user_id = ?",
          tenant
        ),
      refusal: /versioning changed since the finalize froze/,
    },
    {
      what: "a rewritten metadata blob",
      mutate: (tenant: string, uploadId: string) =>
        mutateSession(
          tenant,
          "UPDATE upload_sessions SET metadata_blob = x'a1' WHERE upload_id = ?",
          uploadId
        ),
      refusal: /the metadata changed since the finalize froze/,
    },
    {
      what: "a rewritten tag set",
      mutate: (tenant: string, uploadId: string) =>
        mutateSession(
          tenant,
          `UPDATE upload_sessions SET tags_json = '["smuggled"]'
            WHERE upload_id = ?`,
          uploadId
        ),
      refusal: /the tag set changed since the finalize froze/,
    },
    {
      what: "a rewritten encryption stamp",
      mutate: (tenant: string, uploadId: string) =>
        mutateSession(
          tenant,
          `UPDATE upload_sessions SET encryption_mode = 'convergent'
            WHERE upload_id = ?`,
          uploadId
        ),
      refusal: /the encryption stamp changed since the finalize froze/,
    },
  ])("refuses to publish against $what", async ({ what, mutate, refusal }) => {
    const tenant = `mp-pub-frozen-${what.replace(/[^a-z]+/g, "-")}`;
    const upload = await stageUpload(tenant, "/frozen.bin", 2);
    await stepToPublishing(upload);

    await mutate(tenant, upload.uploadId);
    await expect(step(upload)).rejects.toThrow(refusal);

    // Nothing this upload staged was published, and the session is released
    // rather than left wedged in a phase whose decision can never hold again.
    expect(await readSession(upload)).toMatchObject({ status: "aborted" });
    expect(await countFileRows(tenant, upload.uploadId)).toBe(0);
    expect(await countManifestRows(tenant, upload.uploadId)).toBe(0);
  });

  it("answers a replay after the response was lost", async () => {
    const tenant = "mp-pub-response-loss";
    const upload = await stageUpload(tenant, "/lost.bin", 3);
    await stepToPublishing(upload);

    // Publication commits; the caller never sees this response, and the object
    // it ran on is gone before anyone asks again.
    const published = await step(upload);
    expect(published.done).toBe(false);
    await expect(userStub(tenant).testEvict()).rejects.toThrow(
      /injected UserDO eviction/
    );

    const expected: MultipartFinalizeResponse = {
      fileId: upload.uploadId,
      size: 3 * CHUNK_BYTES,
      chunkCount: 3,
      fileHash: await computeFileHash(upload.hashes),
      path: "/lost.bin",
      mimeType: "application/octet-stream",
      isEncrypted: false,
    };
    // Both entry points answer from what publication recorded — the stepping
    // one finishes the cleaning still owed, the one-request one returns at once.
    await expect(
      userStub(tenant).vfsFinalizeMultipart(
        scopeFor(tenant),
        upload.uploadId,
        upload.hashes
      )
    ).resolves.toEqual(expected);
    expect(await stepToDone(upload)).toEqual(expected);
    await expect(step(upload)).resolves.toEqual({
      done: true,
      result: expected,
      fresh: false,
    });
  });

  it("finishes cleaning nobody came back for, four sessions per alarm", async () => {
    const tenant = "mp-pub-alarm-cleaning";
    const abandoned: Upload[] = [];
    for (let index = 0; index < 5; index++) {
      const upload = await stageUpload(tenant, `/abandoned-${index}.bin`, 1);
      await stepToPublishing(upload);
      // Publish, then walk away: the caller has its file and never steps again.
      await step(upload);
      abandoned.push(upload);
    }
    const owing = async (): Promise<number> => {
      const states = await Promise.all(abandoned.map(readSession));
      return states.filter((state) => state.phase !== "done").length;
    };
    expect(await owing()).toBe(5);

    // One alarm advances at most four of them, so the fifth is still owed.
    expect(await runDurableObjectAlarm(userStub(tenant))).toBe(true);
    expect(await owing()).toBe(1);
    expect(await runDurableObjectAlarm(userStub(tenant))).toBe(true);
    expect(await owing()).toBe(0);

    for (const upload of abandoned) {
      expect(await readSession(upload)).toMatchObject({
        status: "finalized",
        phase: "done",
        expected: 0,
        verified: 0,
      });
      await expect(
        userStub(tenant).vfsReadFile(scopeFor(tenant), upload.path)
      ).resolves.toEqual(expectedBytes(1));
    }
  });

  it("succeeds in one request while cleaning is still outstanding", async () => {
    const tenant = "mp-pub-one-request";
    const upload = await stageUpload(tenant, "/one-request.bin", LARGE_CHUNKS);

    await expect(
      userStub(tenant).vfsFinalizeMultipart(
        scopeFor(tenant),
        upload.uploadId,
        upload.hashes
      )
    ).resolves.toEqual({
      fileId: upload.uploadId,
      size: LARGE_CHUNKS * CHUNK_BYTES,
      chunkCount: LARGE_CHUNKS,
      fileHash: await computeFileHash(upload.hashes),
      path: "/one-request.bin",
      mimeType: "application/octet-stream",
      isEncrypted: false,
    });
    // It returned the moment publication committed rather than paging the
    // scratch off inside the caller's request.
    expect(await readSession(upload)).toMatchObject({
      status: "finalized",
      phase: "cleaning",
    });
    expect(await readLiveFile(tenant, "one-request.bin")).toMatchObject({
      file_id: upload.uploadId,
      file_size: LARGE_CHUNKS * CHUNK_BYTES,
      chunk_count: LARGE_CHUNKS,
    });

    while ((await readSession(upload)).phase !== "done") {
      expect(await runDurableObjectAlarm(userStub(tenant))).toBe(true);
    }
    expect(await readSession(upload)).toMatchObject({
      expected: 0,
      verified: 0,
    });
  });

  it("refuses at begin an upload one request could never finalize", async () => {
    const tenant = "mp-pub-oversized";
    const scope = scopeFor(tenant);
    // Default pool, 5000 chunks: twenty verification pages of thirty-two shard
    // round-trips each, well past what one invocation may spend.
    const oversized = { size: 5_000 * CHUNK_BYTES, chunkSize: CHUNK_BYTES };
    await expect(
      userStub(tenant).vfsBeginMultipart(scope, "/oversized.bin", oversized)
    ).rejects.toThrow(/shard round-trips.*declare protocolVersion 2/s);
    // Refused before a single chunk was accepted: no session to mint a token
    // against, and no temporary row to reap.
    expect(
      await runInDurableObject(
        userStub(tenant),
        (_instance: TestUserDO, state: DurableObjectState) => {
          const count = (query: string): number =>
            state.storage.sql.exec<{ n: number }>(query).toArray()[0].n;
          return {
            sessions: count("SELECT COUNT(*) AS n FROM upload_sessions"),
            temporary: count(
              "SELECT COUNT(*) AS n FROM files WHERE status = 'uploading'"
            ),
          };
        }
      )
    ).toEqual({ sessions: 0, temporary: 0 });

    // The same upload is fine for a caller that drives finalize in pages.
    const begin = await userStub(tenant).vfsBeginMultipart(
      scope,
      "/oversized.bin",
      { ...oversized, protocolVersion: MULTIPART_PROTOCOL_VERSION }
    );
    expect(begin.totalChunks).toBe(5_000);
    expect(begin.protocolVersion).toBe(MULTIPART_PROTOCOL_VERSION);
  });
});
