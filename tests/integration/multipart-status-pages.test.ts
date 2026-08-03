import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it, vi } from "vitest";

import { vfsShardDOName, vfsUserDOName } from "@core/lib/utils";
import type { ShardDO } from "@core/objects/shard/shard-do";
import type { UserDO } from "@app/objects/user/user-do";
import { hashChunk } from "@shared/crypto";
import {
  MULTIPART_HASH_PAGE_SIZE,
  MULTIPART_PLACEMENT_VERSION,
  MULTIPART_PROTOCOL_VERSION,
  MULTIPART_STATUS_CURSOR_MAX_BYTES,
  MULTIPART_STATUS_ENTRY_PAGE_SIZE,
  MULTIPART_STATUS_SHARD_PAGE_SIZE,
  type MultipartStatusPageResponse,
} from "@shared/multipart";
import { placeMultipartChunk } from "@shared/placement";
import type { VFSScope } from "@shared/vfs-types";

/**
 * Bounded multipart status and resume.
 *
 * Reporting what landed used to walk the whole pool and materialise every
 * staged row, on both sides of the RPC. Both are now pages: at most
 * `MULTIPART_STATUS_SHARD_PAGE_SIZE` shards read and
 * `MULTIPART_STATUS_ENTRY_PAGE_SIZE` entries returned per call, each shard
 * asked only for the rows past the boundary the previous page left. These pin:
 *
 *   - a pool wider than one shard page needs a continuation, and following it
 *     yields exactly the set an unbounded scan would have,
 *   - a shard holding more than one entry page hands it over in pages,
 *   - a session small enough to fit in one page still answers in one, and
 *     resume still reports its whole landed set,
 *   - a continuation is the server's seek state, not the caller's: one from
 *     another upload, another tenant, or a tampered one is refused, and
 *   - no page comes anywhere near what an RPC may serialise.
 */

// Spreading an upload across a pool wider than one status page instantiates
// every shard in it, and the entry-paging case stages two full pages of chunks
// on one shard — far more Durable Object traffic than an ordinary test does.
vi.setConfig({ testTimeout: 120_000 });

interface TestEnv {
  MOSSAIC_USER: DurableObjectNamespace<UserDO>;
  MOSSAIC_SHARD: DurableObjectNamespace<ShardDO>;
}

const TEST_ENV = env as unknown as TestEnv;
const NS = "default";
const CHUNK_BYTES = 2;

/** Two entry pages and a short one, all on one shard. */
const LARGE_CHUNKS = 2 * MULTIPART_STATUS_ENTRY_PAGE_SIZE + 8;

function scopeFor(tenant: string): VFSScope {
  return { ns: NS, tenant };
}

function userStub(tenant: string): DurableObjectStub<UserDO> {
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

function chunkBytes(index: number): Uint8Array {
  return new Uint8Array([index & 0xff, (index >> 8) & 0xff]);
}

interface Upload {
  tenant: string;
  uploadId: string;
  poolSize: number;
  totalChunks: number;
}

async function pinPool(tenant: string, poolSize: number): Promise<void> {
  await userStub(tenant).vfsExists(scopeFor(tenant), "/");
  await runInDurableObject(
    userStub(tenant),
    (_instance: UserDO, state: DurableObjectState) => {
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

/** Begin a session and PUT every chunk onto its deterministic owner shard. */
async function stageUpload(
  tenant: string,
  path: string,
  poolSize: number,
  totalChunks: number
): Promise<Upload> {
  await pinPool(tenant, poolSize);
  const begin = await userStub(tenant).vfsBeginMultipart(
    scopeFor(tenant),
    path,
    {
      size: totalChunks * CHUNK_BYTES,
      chunkSize: CHUNK_BYTES,
      protocolVersion: MULTIPART_PROTOCOL_VERSION,
    }
  );
  expect(begin.poolSize).toBe(poolSize);
  // A fresh session has landed nothing, so its first page is empty.
  expect(begin.landed).toEqual([]);
  const upload: Upload = {
    tenant,
    uploadId: begin.uploadId,
    poolSize,
    totalChunks,
  };

  const CONCURRENCY = 16;
  for (let base = 0; base < totalChunks; base += CONCURRENCY) {
    await Promise.all(
      Array.from(
        { length: Math.min(CONCURRENCY, totalChunks - base) },
        async (_unused, offset) => {
          const index = base + offset;
          const bytes = chunkBytes(index);
          await shardStub(tenant, ownerShard(upload, index)).putChunkMultipart(
            await hashChunk(bytes),
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
  return upload;
}

function status(
  upload: Upload,
  continuation?: string
): Promise<MultipartStatusPageResponse & { status: string }> {
  return userStub(upload.tenant).vfsGetMultipartStatus(
    scopeFor(upload.tenant),
    upload.uploadId,
    continuation
  );
}

/** Follow every continuation, returning each page in order. */
async function readAllPages(
  upload: Upload
): Promise<Array<MultipartStatusPageResponse & { status: string }>> {
  const pages: Array<MultipartStatusPageResponse & { status: string }> = [];
  let continuation: string | undefined;
  for (let page = 0; page < 16; page++) {
    const next = await status(upload, continuation);
    pages.push(next);
    continuation = next.continuation;
    if (continuation === undefined) return pages;
  }
  throw new Error("status paging did not terminate");
}

/** Indices the session's placement puts on a shard below `shardIndex`. */
function indicesBelowShard(upload: Upload, shardIndex: number): number[] {
  return Array.from({ length: upload.totalChunks }, (_u, index) => index).filter(
    (index) => ownerShard(upload, index) < shardIndex
  );
}

describe("bounded multipart status", () => {
  it("pages a pool wider than one shard page", async () => {
    const tenant = "mp-status-wide-pool";
    const poolSize = MULTIPART_STATUS_SHARD_PAGE_SIZE + 6;
    const upload = await stageUpload(tenant, "/wide.bin", poolSize, 200);

    const firstPage = indicesBelowShard(
      upload,
      MULTIPART_STATUS_SHARD_PAGE_SIZE
    );
    // The pool is wider than one page, so the split is real rather than
    // incidental to this placement.
    expect(firstPage.length).toBeLessThan(upload.totalChunks);

    const pages = await readAllPages(upload);
    expect(pages).toHaveLength(2);
    expect(pages[0].landed).toEqual(firstPage);
    expect(pages[0].continuation).toBeTypeOf("string");
    expect(pages[1].continuation).toBeUndefined();
    // Each page is sorted within itself, and together they are exactly what
    // an unbounded scan would have reported.
    expect(
      [...pages[0].landed, ...pages[1].landed].sort((a, b) => a - b)
    ).toEqual(Array.from({ length: upload.totalChunks }, (_u, index) => index));
    expect(pages[0].bytesUploaded + pages[1].bytesUploaded).toBe(
      upload.totalChunks * CHUNK_BYTES
    );
    expect(pages[0]).toMatchObject({
      total: upload.totalChunks,
      status: "open",
    });

    // Resume reports the same first page, and hands over the continuation for
    // the rest rather than pretending the set is complete.
    const resumed = await userStub(tenant).vfsBeginMultipart(
      scopeFor(tenant),
      "/wide.bin",
      {
        size: upload.totalChunks * CHUNK_BYTES,
        resumeFrom: upload.uploadId,
        protocolVersion: MULTIPART_PROTOCOL_VERSION,
      }
    );
    expect(resumed.landed).toEqual(firstPage);
    expect(resumed.continuation).toBeTypeOf("string");
  });

  it("pages a shard holding more than one entry page", async () => {
    const tenant = "mp-status-entry-pages";
    const upload = await stageUpload(tenant, "/deep.bin", 1, LARGE_CHUNKS);

    const pages = await readAllPages(upload);
    expect(pages.map((page) => page.landed.length)).toEqual([
      MULTIPART_STATUS_ENTRY_PAGE_SIZE,
      MULTIPART_STATUS_ENTRY_PAGE_SIZE,
      8,
    ]);
    expect(pages.flatMap((page) => page.landed)).toEqual(
      Array.from({ length: LARGE_CHUNKS }, (_u, index) => index)
    );
    expect(
      pages.reduce((total, page) => total + page.bytesUploaded, 0)
    ).toBe(LARGE_CHUNKS * CHUNK_BYTES);
    // The scan resumed inside the shard rather than restarting it.
    expect(pages[0].continuation).not.toBe(pages[1].continuation);
    expect(pages[2].continuation).toBeUndefined();

    // No page is remotely close to what an RPC may serialise, and the shard
    // read underneath refuses to answer with more than one page at all.
    for (const page of pages) {
      expect(JSON.stringify(page).length).toBeLessThan(64 * 1024);
    }
    await expect(
      shardStub(tenant, 0).getMultipartLanded(
        upload.uploadId,
        -1,
        MULTIPART_STATUS_ENTRY_PAGE_SIZE + 1
      )
    ).rejects.toThrow(/EINVAL: multipart read page/);
    await expect(
      shardStub(tenant, 0).getMultipartManifest(
        upload.uploadId,
        -1,
        MULTIPART_STATUS_ENTRY_PAGE_SIZE + 1
      )
    ).rejects.toThrow(/EINVAL: multipart read page/);
    const manifestPage = await shardStub(tenant, 0).getMultipartManifest(
      upload.uploadId
    );
    expect(manifestPage.rows).toHaveLength(MULTIPART_STATUS_ENTRY_PAGE_SIZE);
  });

  it("answers a small session in one page", async () => {
    const tenant = "mp-status-small";
    const upload = await stageUpload(tenant, "/small.bin", 4, 3);

    const page = await status(upload);
    expect(page).toEqual({
      landed: [0, 1, 2],
      total: 3,
      bytesUploaded: 3 * CHUNK_BYTES,
      expiresAtMs: page.expiresAtMs,
      status: "open",
    });
    expect(page.continuation).toBeUndefined();

    const resumed = await userStub(tenant).vfsBeginMultipart(
      scopeFor(tenant),
      "/small.bin",
      {
        size: 3 * CHUNK_BYTES,
        resumeFrom: upload.uploadId,
        protocolVersion: MULTIPART_PROTOCOL_VERSION,
      }
    );
    expect(resumed.landed).toEqual([0, 1, 2]);
    expect(resumed.continuation).toBeUndefined();
  });

  it("refuses a continuation it did not mint for this upload", async () => {
    const tenant = "mp-status-cursor-scope";
    const poolSize = MULTIPART_STATUS_SHARD_PAGE_SIZE + 2;
    const mine = await stageUpload(tenant, "/mine.bin", poolSize, 4);
    const other = await stageUpload(tenant, "/other.bin", poolSize, 4);
    const continuation = (await status(mine)).continuation;
    if (continuation === undefined) {
      throw new Error("a pool wider than one page must yield a continuation");
    }

    // Same tenant, different upload.
    await expect(status(other, continuation)).rejects.toThrow(
      /EINVAL.*continuation does not belong to this upload/
    );
    // Different tenant: its own session id, someone else's seek state.
    const stranger = await stageUpload(
      "mp-status-cursor-stranger",
      "/theirs.bin",
      poolSize,
      4
    );
    await expect(status(stranger, continuation)).rejects.toThrow(
      /EINVAL.*continuation does not belong to this upload/
    );

    // Tampered, unsigned, empty, and oversized inputs are all refused before
    // anything is read.
    const flipped =
      continuation.slice(0, -2) + (continuation.endsWith("A") ? "B" : "A");
    for (const bad of [
      flipped,
      "not-a-continuation",
      "",
      "x".repeat(MULTIPART_STATUS_CURSOR_MAX_BYTES + 1),
    ]) {
      await expect(status(mine, bad)).rejects.toThrow(/EINVAL/);
    }
    // The genuine one still works, so nothing above consumed it.
    await expect(status(mine, continuation)).resolves.toMatchObject({
      total: 4,
    });
  });

  it("reports a finalized session's page over the manifest page size", async () => {
    const tenant = "mp-status-finalized";
    const upload = await stageUpload(
      tenant,
      "/finalized.bin",
      1,
      MULTIPART_HASH_PAGE_SIZE + 4
    );
    const hashes = await Promise.all(
      Array.from({ length: upload.totalChunks }, (_u, index) =>
        hashChunk(chunkBytes(index))
      )
    );
    for (
      let start = 0;
      start < upload.totalChunks;
      start += MULTIPART_HASH_PAGE_SIZE
    ) {
      await userStub(tenant).vfsStageMultipartHashes(
        scopeFor(tenant),
        upload.uploadId,
        start,
        hashes.slice(start, start + MULTIPART_HASH_PAGE_SIZE)
      );
    }
    for (let page = 0; page < 16; page++) {
      const progress = await userStub(tenant).vfsFinalizeMultipartStep(
        scopeFor(tenant),
        upload.uploadId
      );
      if (progress.done) break;
    }

    // Publication clears the staging its cleanup intents covered, so the
    // landed set a finalized session reports is empty rather than unbounded.
    const page = await status(upload);
    expect(page).toMatchObject({
      landed: [],
      total: upload.totalChunks,
      bytesUploaded: 0,
      status: "finalized",
    });
    expect(page.continuation).toBeUndefined();
  });
});
