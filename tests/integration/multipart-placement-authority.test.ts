import { describe, expect, it } from "vitest";
import { SELF, env, runInDurableObject } from "cloudflare:test";

/**
 * Multipart placement authority.
 *
 * A multipart chunk PUT picks a ShardDO before anything durable has
 * been consulted, so whatever supplies `poolSize` and the placement
 * algorithm decides which shards a caller's bytes can reach. These
 * tests pin that the answer is always the signed session token:
 *
 *   - HTTP and binding PUTs of the same session agree on one shard,
 *   - a mutated handle cannot move a chunk out of the signed pool,
 *   - a forged token routes nowhere that keeps bytes,
 *   - sessions minted before placement versioning stay on rendezvous,
 *   - resume re-freezes the session's own algorithm, and
 *   - finalize refuses a manifest whose chunks are not on the shards
 *     that placement says own them.
 */

import {
  createMossaicHttpClient,
  createVFS,
  type MossaicEnv,
  type MultipartUploadHandle,
  type UserDO,
} from "../../sdk/src/index";
import { hashChunk } from "@shared/crypto";
import {
  MULTIPART_LEGACY_PLACEMENT_VERSION,
  MULTIPART_PLACEMENT_VERSION,
} from "@shared/multipart";
import { placeChunk, placeMultipartChunk } from "@shared/placement";
import { signVFSMultipartToken, signVFSToken } from "@core/lib/auth";
import { vfsShardDOName, vfsUserDOName } from "@core/lib/utils";
import type { ShardDO } from "@core/objects/shard/shard-do";

interface TestEnv {
  JWT_SECRET?: string;
  MOSSAIC_USER: DurableObjectNamespace<UserDO>;
  MOSSAIC_SHARD: DurableObjectNamespace<ShardDO>;
}

const TEST_ENV = env as unknown as TestEnv;
const NS = "default";

interface ShardUploadState {
  refs: number;
  staging: number;
  chunks: number;
  fences: number;
}

/**
 * A binding env that records every shard name the SDK addresses, so a
 * test can assert not just where a chunk landed but where the client
 * even tried to send it.
 */
function bindingEnv(routedShardNames: string[] = []): MossaicEnv {
  return {
    MOSSAIC_USER: TEST_ENV.MOSSAIC_USER as MossaicEnv["MOSSAIC_USER"],
    MOSSAIC_SHARD: {
      idFromName(name: string): DurableObjectId {
        routedShardNames.push(name);
        return TEST_ENV.MOSSAIC_SHARD.idFromName(name);
      },
      get(id: DurableObjectId): unknown {
        return TEST_ENV.MOSSAIC_SHARD.get(id);
      },
    },
  };
}

const selfFetcher: typeof fetch = ((
  input: RequestInfo | URL,
  init?: RequestInit
) => {
  const url =
    typeof input === "string"
      ? input
      : input instanceof URL
        ? input.toString()
        : input.url;
  return SELF.fetch(url, init);
}) as typeof fetch;

async function httpClient(tenant: string) {
  const apiKey = await signVFSToken(TEST_ENV as never, { ns: NS, tenant });
  return createMossaicHttpClient({
    url: "https://mossaic.test",
    apiKey,
    fetcher: selfFetcher,
  });
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

function decodePayload(token: string): Record<string, unknown> {
  const segment = token.split(".")[1];
  const base64 = segment.replace(/-/g, "+").replace(/_/g, "/");
  const binary = atob(base64 + "=".repeat((4 - (base64.length % 4)) % 4));
  const bytes = Uint8Array.from(binary, (char) => char.charCodeAt(0));
  return JSON.parse(new TextDecoder().decode(bytes)) as Record<string, unknown>;
}

function tamperPayload(
  token: string,
  mutate: (payload: Record<string, unknown>) => Record<string, unknown>
): string {
  const [header, , signature] = token.split(".");
  const bytes = new TextEncoder().encode(
    JSON.stringify(mutate(decodePayload(token)))
  );
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  const payload = btoa(binary)
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/, "");
  return `${header}.${payload}.${signature}`;
}

function tamperSignature(token: string): string {
  const parts = token.split(".");
  const signature = parts[2];
  parts[2] = `${signature[0] === "A" ? "B" : "A"}${signature.slice(1)}`;
  return parts.join(".");
}

/**
 * An inflated pool size whose v2 placement for `chunkIndex` escapes the
 * signed pool entirely — the shard a tampered handle would strand refs
 * on, beyond the reach of finalize and abort cleanup.
 *
 * Jump consistent hashing only ever relocates a key onto the newest
 * bucket, so a given key can stay inside the signed pool across a long
 * run of larger pools. Probing geometrically instead of one size at a
 * time makes the search independent of which upload id was minted:
 * a key survives every probe up to `limit` with probability
 * `signedPoolSize / limit`.
 */
function findOutOfPoolPlacement(
  userId: string,
  uploadId: string,
  chunkIndex: number,
  signedPoolSize: number
): { poolSize: number; shardIndex: number } {
  const limit = 2 ** 48;
  for (
    let poolSize = signedPoolSize + 1;
    poolSize <= limit;
    poolSize = Math.min(poolSize * 2, limit)
  ) {
    const shardIndex = placeMultipartChunk(
      userId,
      uploadId,
      chunkIndex,
      poolSize,
      MULTIPART_PLACEMENT_VERSION
    );
    if (shardIndex >= signedPoolSize) return { poolSize, shardIndex };
    if (poolSize === limit) break;
  }
  throw new Error("failed to find an adversarial out-of-pool placement");
}

async function readShardUploadState(
  tenant: string,
  shardIndex: number,
  uploadId: string,
  chunkHash: string
): Promise<ShardUploadState> {
  const stub = shardStub(tenant, shardIndex);
  await stub.fetch(new Request("http://internal/stats"));
  return runInDurableObject(stub, (_instance, state) => {
    const count = (query: string, value: string): number =>
      (state.storage.sql.exec(query, value).toArray()[0] as { n: number }).n;
    return {
      refs: count(
        "SELECT COUNT(*) AS n FROM chunk_refs WHERE file_id = ?",
        uploadId
      ),
      staging: count(
        "SELECT COUNT(*) AS n FROM upload_chunks WHERE upload_id = ?",
        uploadId
      ),
      chunks: count("SELECT COUNT(*) AS n FROM chunks WHERE hash = ?", chunkHash),
      fences: count(
        "SELECT COUNT(*) AS n FROM multipart_fences WHERE upload_id = ?",
        uploadId
      ),
    };
  });
}

async function readStagedHash(
  tenant: string,
  shardIndex: number,
  uploadId: string,
  chunkIndex: number
): Promise<string | null> {
  const stub = shardStub(tenant, shardIndex);
  await stub.fetch(new Request("http://internal/stats"));
  return runInDurableObject(stub, (_instance, state) => {
    const row = state.storage.sql
      .exec(
        `SELECT chunk_hash FROM upload_chunks
          WHERE upload_id = ? AND chunk_index = ?`,
        uploadId,
        chunkIndex
      )
      .toArray()[0] as { chunk_hash: string } | undefined;
    return row?.chunk_hash ?? null;
  });
}

function readSessionRow(
  tenant: string,
  uploadId: string
): Promise<{ status: string; placement_version: number } | undefined> {
  return runInDurableObject(
    userStub(tenant),
    (_instance, state) =>
      state.storage.sql
        .exec(
          "SELECT status, placement_version FROM upload_sessions WHERE upload_id = ?",
          uploadId
        )
        .toArray()[0] as
        | { status: string; placement_version: number }
        | undefined
  );
}

/** Rows a successful finalize would have published for this upload. */
function readPublishedChunkCount(
  tenant: string,
  uploadId: string
): Promise<number> {
  return runInDurableObject(
    userStub(tenant),
    (_instance, state) =>
      (
        state.storage.sql
          .exec(
            `SELECT (SELECT COUNT(*) FROM file_chunks WHERE file_id = ?)
                  + (SELECT COUNT(*) FROM version_chunks
                       WHERE version_id IN (
                         SELECT version_id FROM file_versions WHERE shard_ref_id = ?
                       )) AS n`,
            uploadId,
            uploadId
          )
          .toArray()[0] as { n: number }
      ).n
  );
}

/** Byte `i` of a chunk index, so every chunk hashes differently. */
function chunkBytes(index: number): Uint8Array {
  return new Uint8Array([index + 1]);
}

describe("multipart placement authority", () => {
  it("keeps HTTP PUT, binding PUT, and finalize on one v2 placement", async () => {
    const tenant = "mp-placement-parity";
    const routed: string[] = [];
    const binding = createVFS(bindingEnv(routed), { tenant });
    const http = await httpClient(tenant);
    const handle = await binding.beginMultipartUpload("/parity.bin", {
      size: 2,
      chunkSize: 1,
    });
    expect(decodePayload(handle.sessionToken)).toMatchObject({
      placementVersion: MULTIPART_PLACEMENT_VERSION,
      userId: tenant,
    });

    const first = await http.putMultipartChunk(handle, 0, chunkBytes(0));
    const second = await binding.putMultipartChunk(handle, 1, chunkBytes(1));

    const firstShard = placeMultipartChunk(
      tenant,
      handle.uploadId,
      0,
      handle.poolSize,
      MULTIPART_PLACEMENT_VERSION
    );
    const secondShard = placeMultipartChunk(
      tenant,
      handle.uploadId,
      1,
      handle.poolSize,
      MULTIPART_PLACEMENT_VERSION
    );
    expect(routed).toEqual([vfsShardDOName(NS, tenant, undefined, secondShard)]);
    await expect(
      readStagedHash(tenant, firstShard, handle.uploadId, 0)
    ).resolves.toBe(first.chunkHash);
    await expect(
      readStagedHash(tenant, secondShard, handle.uploadId, 1)
    ).resolves.toBe(second.chunkHash);

    await binding.finalizeMultipartUpload(handle, [
      first.chunkHash,
      second.chunkHash,
    ]);
    await expect(binding.readFile("/parity.bin")).resolves.toEqual(
      new Uint8Array([1, 2])
    );
  });

  it("keeps a pre-versioning session and its versionless token on rendezvous", async () => {
    const tenant = "mp-placement-legacy";
    const routed: string[] = [];
    const vfs = createVFS(bindingEnv(routed), { tenant });
    const user = userStub(tenant);
    const scope = { ns: NS, tenant } as const;
    const begin = await user.vfsBeginMultipart(scope, "/legacy.bin", {
      size: 1,
      chunkSize: 1,
    });
    // Rewrite the session the way an instance that predates placement
    // versioning holds it: v1, and a token minted without the claim.
    await runInDurableObject(user, (_instance, state) => {
      state.storage.sql.exec(
        "UPDATE upload_sessions SET placement_version = ? WHERE upload_id = ?",
        MULTIPART_LEGACY_PLACEMENT_VERSION,
        begin.uploadId
      );
    });
    const fenceId = decodePayload(begin.sessionToken).fenceId;
    if (typeof fenceId !== "string") throw new Error("missing fenceId claim");
    const { token } = await signVFSMultipartToken(TEST_ENV as never, {
      uploadId: begin.uploadId,
      fenceId,
      userId: tenant,
      ns: NS,
      tn: tenant,
      poolSize: begin.poolSize,
      totalChunks: begin.totalChunks,
      chunkSize: begin.chunkSize,
      totalSize: 1,
    });
    expect(decodePayload(token).placementVersion).toBeUndefined();
    const handle: MultipartUploadHandle = {
      uploadId: begin.uploadId,
      path: "/legacy.bin",
      chunkSize: begin.chunkSize,
      expectedChunks: begin.totalChunks,
      poolSize: begin.poolSize,
      sessionToken: token,
      expiresAtMs: begin.expiresAtMs,
    };

    const put = await vfs.putMultipartChunk(handle, 0, chunkBytes(0));

    const legacyShard = placeChunk(tenant, begin.uploadId, 0, begin.poolSize);
    expect(routed).toEqual([vfsShardDOName(NS, tenant, undefined, legacyShard)]);
    await expect(
      readStagedHash(tenant, legacyShard, begin.uploadId, 0)
    ).resolves.toBe(put.chunkHash);
    await user.vfsFinalizeMultipart(scope, begin.uploadId, [put.chunkHash]);
    await expect(vfs.readFile("/legacy.bin")).resolves.toEqual(chunkBytes(0));
  });

  it("routes from the signed poolSize and never writes outside the signed pool", async () => {
    const tenant = "mp-placement-handle-hints";
    const routed: string[] = [];
    const vfs = createVFS(bindingEnv(routed), { tenant });
    const handle = await vfs.beginMultipartUpload("/hints.bin", {
      size: 2,
      chunkSize: 1,
    });
    const adversarial = findOutOfPoolPlacement(
      tenant,
      handle.uploadId,
      0,
      handle.poolSize
    );
    const authoritativeShard = placeMultipartChunk(
      tenant,
      handle.uploadId,
      0,
      handle.poolSize,
      MULTIPART_PLACEMENT_VERSION
    );

    const result = await vfs.putMultipartChunk(
      { ...handle, poolSize: adversarial.poolSize, expectedChunks: 99 },
      0,
      chunkBytes(0)
    );

    expect(result.accepted).toBe(true);
    expect(routed).toEqual([
      vfsShardDOName(NS, tenant, undefined, authoritativeShard),
    ]);
    await expect(
      readShardUploadState(
        tenant,
        authoritativeShard,
        handle.uploadId,
        result.chunkHash
      )
    ).resolves.toEqual({ refs: 1, staging: 1, chunks: 1, fences: 1 });
    await expect(
      readShardUploadState(
        tenant,
        adversarial.shardIndex,
        handle.uploadId,
        result.chunkHash
      )
    ).resolves.toEqual({ refs: 0, staging: 0, chunks: 0, fences: 0 });
  });

  it("bounds the chunk index by the signed totalChunks, not the handle", async () => {
    const tenant = "mp-placement-total-chunks";
    const routed: string[] = [];
    const vfs = createVFS(bindingEnv(routed), { tenant });
    const handle = await vfs.beginMultipartUpload("/bounds.bin", {
      size: 1,
      chunkSize: 1,
    });

    await expect(
      vfs.putMultipartChunk({ ...handle, expectedChunks: 2 }, 1, chunkBytes(1))
    ).rejects.toMatchObject({
      code: "EINVAL",
      message: expect.stringMatching(/out of range \[0, 1\)/),
    });
    expect(routed).toEqual([]);
  });

  it("rejects malformed or self-inconsistent routing claims before routing", async () => {
    const tenant = "mp-placement-claims";
    const routed: string[] = [];
    const vfs = createVFS(bindingEnv(routed), { tenant });
    const handle = await vfs.beginMultipartUpload("/claims.bin", {
      size: 1,
      chunkSize: 1,
    });
    const rewrite = (
      mutate: (payload: Record<string, unknown>) => Record<string, unknown>
    ): MultipartUploadHandle => ({
      ...handle,
      sessionToken: tamperPayload(handle.sessionToken, mutate),
    });
    const attempts: MultipartUploadHandle[] = [
      { ...handle, uploadId: `${handle.uploadId}-other` },
      { ...handle, sessionToken: "not-a-jwt" },
      rewrite((payload) => ({ ...payload, poolSize: 0 })),
      rewrite((payload) => ({ ...payload, totalChunks: "1" })),
      rewrite((payload) => ({ ...payload, userId: "another-user" })),
      rewrite((payload) => ({ ...payload, tn: "another-tenant" })),
      rewrite((payload) => ({ ...payload, placementVersion: 99 })),
      rewrite(({ userId: _userId, ...payload }) => payload),
      rewrite(({ fenceId: _fenceId, ...payload }) => payload),
    ];

    for (const attempt of attempts) {
      await expect(
        vfs.putMultipartChunk(attempt, 0, chunkBytes(0))
      ).rejects.toMatchObject({ code: "EACCES" });
    }
    expect(routed).toEqual([]);
  });

  it("leaves the signature authoritative and forged tokens without orphans", async () => {
    const tenant = "mp-placement-signature";
    const routed: string[] = [];
    const vfs = createVFS(bindingEnv(routed), { tenant });
    const http = await httpClient(tenant);
    const bytes = chunkBytes(0);
    const chunkHash = await hashChunk(bytes);
    const handle = await vfs.beginMultipartUpload("/signature.bin", {
      size: 2,
      chunkSize: 1,
    });
    const adversarial = findOutOfPoolPlacement(
      tenant,
      handle.uploadId,
      0,
      handle.poolSize
    );

    // A payload edit that survives claim validation still routes — and
    // still stores nothing, because the shard re-verifies the HMAC.
    await expect(
      vfs.putMultipartChunk(
        {
          ...handle,
          sessionToken: tamperPayload(handle.sessionToken, (payload) => ({
            ...payload,
            poolSize: adversarial.poolSize,
          })),
        },
        0,
        bytes
      )
    ).rejects.toMatchObject({ code: "EACCES" });
    expect(routed).toEqual([
      vfsShardDOName(NS, tenant, undefined, adversarial.shardIndex),
    ]);
    await expect(
      readShardUploadState(
        tenant,
        adversarial.shardIndex,
        handle.uploadId,
        chunkHash
      )
    ).resolves.toEqual({ refs: 0, staging: 0, chunks: 0, fences: 0 });

    routed.length = 0;
    const downgraded = {
      ...handle,
      sessionToken: tamperPayload(handle.sessionToken, (payload) => ({
        ...payload,
        placementVersion: MULTIPART_LEGACY_PLACEMENT_VERSION,
      })),
    };
    const legacyShard = placeChunk(tenant, handle.uploadId, 0, handle.poolSize);
    await expect(
      vfs.putMultipartChunk(downgraded, 0, bytes)
    ).rejects.toMatchObject({ code: "EACCES" });
    expect(routed).toEqual([vfsShardDOName(NS, tenant, undefined, legacyShard)]);
    await expect(
      readShardUploadState(tenant, legacyShard, handle.uploadId, chunkHash)
    ).resolves.toEqual({ refs: 0, staging: 0, chunks: 0, fences: 0 });
    await expect(
      http.putMultipartChunk(downgraded, 0, bytes)
    ).rejects.toMatchObject({ code: "EACCES" });

    routed.length = 0;
    const signedShard = placeMultipartChunk(
      tenant,
      handle.uploadId,
      0,
      handle.poolSize,
      MULTIPART_PLACEMENT_VERSION
    );
    await expect(
      vfs.putMultipartChunk(
        { ...handle, sessionToken: tamperSignature(handle.sessionToken) },
        0,
        bytes
      )
    ).rejects.toMatchObject({ code: "EACCES" });
    expect(routed).toEqual([vfsShardDOName(NS, tenant, undefined, signedShard)]);
    await expect(
      readShardUploadState(tenant, signedShard, handle.uploadId, chunkHash)
    ).resolves.toEqual({ refs: 0, staging: 0, chunks: 0, fences: 0 });
  });

  it("re-freezes each session's own placement version on resume", async () => {
    const tenant = "mp-placement-resume";
    const vfs = createVFS(bindingEnv(), { tenant });
    const user = userStub(tenant);
    const current = await vfs.beginMultipartUpload("/resume-v2.bin", {
      size: 1,
      chunkSize: 1,
    });
    const legacy = await vfs.beginMultipartUpload("/resume-v1.bin", {
      size: 1,
      chunkSize: 1,
    });
    await runInDurableObject(user, (_instance, state) => {
      state.storage.sql.exec(
        "UPDATE upload_sessions SET placement_version = ? WHERE upload_id = ?",
        MULTIPART_LEGACY_PLACEMENT_VERSION,
        legacy.uploadId
      );
    });

    const resumedCurrent = await vfs.beginMultipartUpload("/resume-v2.bin", {
      size: 1,
      resumeFrom: current.uploadId,
    });
    const resumedLegacy = await vfs.beginMultipartUpload("/resume-v1.bin", {
      size: 1,
      resumeFrom: legacy.uploadId,
    });

    expect(decodePayload(resumedCurrent.sessionToken)).toMatchObject({
      placementVersion: MULTIPART_PLACEMENT_VERSION,
    });
    expect(decodePayload(resumedLegacy.sessionToken)).toMatchObject({
      placementVersion: MULTIPART_LEGACY_PLACEMENT_VERSION,
    });
    await expect(readSessionRow(tenant, current.uploadId)).resolves.toEqual({
      status: "open",
      placement_version: MULTIPART_PLACEMENT_VERSION,
    });
    await expect(readSessionRow(tenant, legacy.uploadId)).resolves.toEqual({
      status: "open",
      placement_version: MULTIPART_LEGACY_PLACEMENT_VERSION,
    });

    // The resumed legacy session still addresses the shards its
    // already-landed chunks are on.
    const put = await vfs.putMultipartChunk(resumedLegacy, 0, chunkBytes(3));
    await expect(
      readStagedHash(
        tenant,
        placeChunk(tenant, legacy.uploadId, 0, legacy.poolSize),
        legacy.uploadId,
        0
      )
    ).resolves.toBe(put.chunkHash);
  });
});

describe("multipart finalize placement verification", () => {
  const CHUNKS = 8;

  /**
   * Begin an upload, stage every chunk through the authoritative path,
   * and hand back the plan so a test can then misplace one chunk the
   * way a direct ShardDO RPC could.
   */
  async function stageUpload(tenant: string, path: string) {
    const vfs = createVFS(bindingEnv(), { tenant });
    const handle = await vfs.beginMultipartUpload(path, {
      size: CHUNKS,
      chunkSize: 1,
    });
    const owners = Array.from({ length: CHUNKS }, (_, index) =>
      placeMultipartChunk(
        tenant,
        handle.uploadId,
        index,
        handle.poolSize,
        MULTIPART_PLACEMENT_VERSION
      )
    );
    const hashes: string[] = [];
    for (let index = 0; index < CHUNKS; index++) {
      const put = await vfs.putMultipartChunk(handle, index, chunkBytes(index));
      hashes.push(put.chunkHash);
    }
    const foreign = owners.find((owner) => owner !== owners[0]);
    if (foreign === undefined) {
      throw new Error("upload placed every chunk on one shard");
    }
    return { vfs, handle, owners, hashes, foreign };
  }

  it("rejects a chunk staged on a shard that does not own its index", async () => {
    const tenant = "mp-finalize-wrong-shard";
    const { vfs, handle, owners, hashes, foreign } = await stageUpload(
      tenant,
      "/wrong-shard.bin"
    );
    // Move chunk 0 off its owner and onto another shard the finalize
    // fan-out legitimately reads, the way a direct RPC could.
    await shardStub(tenant, foreign).putChunkMultipart(
      hashes[0],
      chunkBytes(0),
      handle.uploadId,
      0,
      tenant,
      handle.sessionToken
    );
    await runInDurableObject(shardStub(tenant, owners[0]), (_instance, state) => {
      state.storage.sql.exec(
        "DELETE FROM upload_chunks WHERE upload_id = ? AND chunk_index = 0",
        handle.uploadId
      );
    });

    await expect(
      vfs.finalizeMultipartUpload(handle, hashes)
    ).rejects.toMatchObject({
      code: "EBADF",
      message: expect.stringContaining(
        `finalizeMultipart: chunk 0 staged on shard ${foreign}; expected ${owners[0]}`
      ),
    });
    await expect(readPublishedChunkCount(tenant, handle.uploadId)).resolves.toBe(
      0
    );
    await expect(vfs.abortMultipartUpload(handle)).resolves.toEqual({
      aborted: true,
    });
  });

  it("rejects a chunk index staged on more than one shard", async () => {
    const tenant = "mp-finalize-duplicate-index";
    const { vfs, handle, owners, hashes, foreign } = await stageUpload(
      tenant,
      "/duplicate-index.bin"
    );
    await shardStub(tenant, foreign).putChunkMultipart(
      hashes[0],
      chunkBytes(0),
      handle.uploadId,
      0,
      tenant,
      handle.sessionToken
    );

    await expect(
      vfs.finalizeMultipartUpload(handle, hashes)
    ).rejects.toMatchObject({
      code: "EBADF",
      message: expect.stringContaining(
        "finalizeMultipart: 1 duplicate chunk index(es); first: chunk 0 " +
          `staged on shards ${Math.min(owners[0], foreign)} and ${Math.max(owners[0], foreign)}`
      ),
    });
    await expect(readPublishedChunkCount(tenant, handle.uploadId)).resolves.toBe(
      0
    );
  });
});
