import { describe, expect, it } from "vitest";
import { SELF, env, runInDurableObject } from "cloudflare:test";

import {
  createVFS,
  createMossaicHttpClient,
  type MossaicEnv,
  type MultipartUploadHandle,
  type UserDO,
} from "../../sdk/src/index";
import { hashChunk } from "@shared/crypto";
import {
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

interface ShardUploadState {
  refs: number;
  staging: number;
  chunks: number;
  fences: number;
}

const TEST_ENV = env as unknown as TestEnv;
const NS = "default";

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

async function httpClient(tenant: string) {
  const apiKey = await signVFSToken(TEST_ENV as never, { ns: NS, tenant });
  return createMossaicHttpClient({
    url: "https://mossaic.test",
    apiKey,
    fetcher: (input, init) => SELF.fetch(input, init),
  });
}

function decodePayload(token: string): Record<string, unknown> {
  const segment = token.split(".")[1]!;
  const base64 = segment.replace(/-/g, "+").replace(/_/g, "/");
  const binary = atob(base64 + "=".repeat((4 - (base64.length % 4)) % 4));
  const bytes = Uint8Array.from(binary, (char) => char.charCodeAt(0));
  return JSON.parse(new TextDecoder().decode(bytes)) as Record<string, unknown>;
}

function encodePayload(payload: Record<string, unknown>): string {
  const bytes = new TextEncoder().encode(JSON.stringify(payload));
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary)
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/, "");
}

function tamperPayload(
  token: string,
  mutate: (payload: Record<string, unknown>) => Record<string, unknown>,
): string {
  const [header, , signature] = token.split(".");
  return `${header}.${encodePayload(mutate(decodePayload(token)))}.${signature}`;
}

function tamperSignature(token: string): string {
  const parts = token.split(".");
  const signature = parts[2]!;
  parts[2] = `${signature[0] === "A" ? "B" : "A"}${signature.slice(1)}`;
  return parts.join(".");
}

function findOutOfPoolPlacement(
  userId: string,
  uploadId: string,
  chunkIndex: number,
  signedPoolSize: number,
): { poolSize: number; shardIndex: number } {
  for (
    let poolSize = signedPoolSize + 1;
    poolSize <= signedPoolSize + 4_096;
    poolSize++
  ) {
    const shardIndex = placeMultipartChunk(
      userId,
      uploadId,
      chunkIndex,
      poolSize,
      MULTIPART_PLACEMENT_VERSION,
    );
    if (shardIndex >= signedPoolSize) return { poolSize, shardIndex };
  }
  throw new Error("failed to find an adversarial out-of-pool placement");
}

function shardStub(
  tenant: string,
  shardIndex: number,
): DurableObjectStub<ShardDO> {
  return TEST_ENV.MOSSAIC_SHARD.get(
    TEST_ENV.MOSSAIC_SHARD.idFromName(
      vfsShardDOName(NS, tenant, undefined, shardIndex),
    ),
  );
}

async function readShardUploadState(
  tenant: string,
  shardIndex: number,
  uploadId: string,
  chunkHash: string,
): Promise<ShardUploadState> {
  const stub = shardStub(tenant, shardIndex);
  await stub.fetch(new Request("http://internal/stats"));
  return runInDurableObject(stub, async (_instance, state) => {
    const count = (query: string, value: string): number =>
      (
        state.storage.sql.exec(query, value).toArray()[0] as {
          n: number;
        }
      ).n;
    return {
      refs: count(
        "SELECT COUNT(*) AS n FROM chunk_refs WHERE file_id = ?",
        uploadId,
      ),
      staging: count(
        "SELECT COUNT(*) AS n FROM upload_chunks WHERE upload_id = ?",
        uploadId,
      ),
      chunks: count(
        "SELECT COUNT(*) AS n FROM chunks WHERE hash = ?",
        chunkHash,
      ),
      fences: count(
        "SELECT COUNT(*) AS n FROM multipart_fences WHERE upload_id = ?",
        uploadId,
      ),
    };
  });
}

async function readStagedHash(
  tenant: string,
  shardIndex: number,
  uploadId: string,
  chunkIndex: number,
): Promise<string | null> {
  const stub = shardStub(tenant, shardIndex);
  await stub.fetch(new Request("http://internal/stats"));
  return runInDurableObject(stub, async (_instance, state) => {
    const row = state.storage.sql
      .exec(
        `SELECT chunk_hash FROM upload_chunks
          WHERE upload_id = ? AND chunk_index = ?`,
        uploadId,
        chunkIndex,
      )
      .toArray()[0] as { chunk_hash: string } | undefined;
    return row?.chunk_hash ?? null;
  });
}

describe("binding SDK multipart placement authority", () => {
  it("keeps HTTP PUT, binding PUT, status, and finalize on one v2 placement", async () => {
    const tenant = "sdk-placement-cross-runtime";
    const routed: string[] = [];
    const binding = createVFS(bindingEnv(routed), { tenant });
    const http = await httpClient(tenant);
    const handle = await binding.beginMultipartUpload("/parity.bin", {
      size: 2,
      chunkSize: 1,
    });
    expect(decodePayload(handle.sessionToken)).toMatchObject({
      placementVersion: MULTIPART_PLACEMENT_VERSION,
    });

    const first = await http.putMultipartChunk(handle, 0, new Uint8Array([11]));
    const second = await binding.putMultipartChunk(
      handle,
      1,
      new Uint8Array([22]),
    );
    const firstShard = placeMultipartChunk(
      tenant,
      handle.uploadId,
      0,
      handle.poolSize,
      MULTIPART_PLACEMENT_VERSION,
    );
    const secondShard = placeMultipartChunk(
      tenant,
      handle.uploadId,
      1,
      handle.poolSize,
      MULTIPART_PLACEMENT_VERSION,
    );

    expect(routed).toEqual([
      vfsShardDOName(NS, tenant, undefined, secondShard),
    ]);
    await expect(
      readStagedHash(tenant, firstShard, handle.uploadId, 0),
    ).resolves.toBe(first.chunkHash);
    await expect(
      readStagedHash(tenant, secondShard, handle.uploadId, 1),
    ).resolves.toBe(second.chunkHash);
    await expect(
      http.multipartStatus(handle.uploadId, handle.sessionToken),
    ).resolves.toMatchObject({ landed: [0, 1], total: 2 });

    await binding.finalizeMultipartUpload(handle, [
      first.chunkHash,
      second.chunkHash,
    ]);
    await expect(binding.readFile(handle.path)).resolves.toEqual(
      new Uint8Array([11, 22]),
    );
  });

  it("keeps existing versionless tokens and legacy sessions on rendezvous", async () => {
    const tenant = "sdk-placement-legacy-versionless";
    const routed: string[] = [];
    const vfs = createVFS(bindingEnv(routed), { tenant });
    const user = TEST_ENV.MOSSAIC_USER.get(
      TEST_ENV.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant)),
    );
    const begin = await user.vfsBeginMultipart(
      { ns: NS, tenant },
      "/legacy.bin",
      { size: 1, chunkSize: 1 },
    );
    const claims = decodePayload(begin.sessionToken);
    const fenceId = claims.fenceId;
    if (typeof fenceId !== "string") throw new Error("missing fenceId");
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

    const put = await vfs.putMultipartChunk(handle, 0, new Uint8Array([33]));
    const legacyShard = placeChunk(tenant, begin.uploadId, 0, begin.poolSize);
    expect(routed).toEqual([
      vfsShardDOName(NS, tenant, undefined, legacyShard),
    ]);
    await expect(
      readStagedHash(tenant, legacyShard, begin.uploadId, 0),
    ).resolves.toBe(put.chunkHash);
    await user.vfsFinalizeMultipart(
      { ns: NS, tenant },
      begin.uploadId,
      [put.chunkHash],
    );
    await expect(vfs.readFile(handle.path)).resolves.toEqual(
      new Uint8Array([33]),
    );
  });

  it("routes from signed poolSize/totalChunks and ignores mutable handle hints", async () => {
    const tenant = "sdk-placement-handle-hints";
    const routed: string[] = [];
    const vfs = createVFS(bindingEnv(routed), { tenant });
    const data = new Uint8Array([1, 2]);
    const handle = await vfs.beginMultipartUpload("/hints.bin", {
      size: data.byteLength,
      chunkSize: 1,
    });
    const adversarial = findOutOfPoolPlacement(
      tenant,
      handle.uploadId,
      0,
      handle.poolSize,
    );
    const tamperedHandle: MultipartUploadHandle = {
      ...handle,
      poolSize: adversarial.poolSize,
      expectedChunks: 0,
    };
    const authoritativeShard = placeMultipartChunk(
      tenant,
      handle.uploadId,
      0,
      handle.poolSize,
      MULTIPART_PLACEMENT_VERSION,
    );

    const result = await vfs.putMultipartChunk(
      tamperedHandle,
      0,
      data.subarray(0, 1),
    );

    expect(result.accepted).toBe(true);
    expect(routed).toEqual([
      vfsShardDOName(NS, tenant, undefined, authoritativeShard),
    ]);
    expect(
      await readShardUploadState(
        tenant,
        authoritativeShard,
        handle.uploadId,
        result.chunkHash,
      ),
    ).toEqual({ refs: 1, staging: 1, chunks: 1, fences: 1 });
    expect(
      await readShardUploadState(
        tenant,
        adversarial.shardIndex,
        handle.uploadId,
        result.chunkHash,
      ),
    ).toEqual({ refs: 0, staging: 0, chunks: 0, fences: 0 });
  });

  it("uses signed totalChunks and rejects an inflated expectedChunks before routing", async () => {
    const tenant = "sdk-placement-total-chunks";
    const routed: string[] = [];
    const vfs = createVFS(bindingEnv(routed), { tenant });
    const handle = await vfs.beginMultipartUpload("/bounds.bin", {
      size: 1,
      chunkSize: 1,
    });
    const tamperedHandle = { ...handle, expectedChunks: 2 };

    await expect(
      vfs.putMultipartChunk(tamperedHandle, 1, new Uint8Array([1])),
    ).rejects.toMatchObject({
      code: "EINVAL",
      message: expect.stringMatching(/out of range \[0, 1\)/),
    });
    expect(routed).toEqual([]);
  });

  it("rejects malformed or mismatched routing claims before routing", async () => {
    const tenant = "sdk-placement-claims";
    const routed: string[] = [];
    const vfs = createVFS(bindingEnv(routed), { tenant });
    const handle = await vfs.beginMultipartUpload("/claims.bin", {
      size: 1,
      chunkSize: 1,
    });
    const attempts: MultipartUploadHandle[] = [
      { ...handle, uploadId: `${handle.uploadId}-other` },
      { ...handle, sessionToken: "not-a-jwt" },
      {
        ...handle,
        sessionToken: tamperPayload(handle.sessionToken, (payload) => ({
          ...payload,
          poolSize: 0,
        })),
      },
      {
        ...handle,
        sessionToken: tamperPayload(handle.sessionToken, (payload) => ({
          ...payload,
          totalChunks: "1",
        })),
      },
      {
        ...handle,
        sessionToken: tamperPayload(handle.sessionToken, (payload) => ({
          ...payload,
          userId: "another-user",
        })),
      },
      {
        ...handle,
        sessionToken: tamperPayload(handle.sessionToken, (payload) => ({
          ...payload,
          tn: "another-tenant",
        })),
      },
      {
        ...handle,
        sessionToken: tamperPayload(handle.sessionToken, (payload) => ({
          ...payload,
          placementVersion: 99,
        })),
      },
    ];

    for (const attempt of attempts) {
      await expect(
        vfs.putMultipartChunk(attempt, 0, new Uint8Array([1])),
      ).rejects.toMatchObject({ code: "EACCES" });
    }
    expect(routed).toEqual([]);
  });

  it("leaves signature verification authoritative without creating forged-token orphans", async () => {
    const tenant = "sdk-placement-signature";
    const routed: string[] = [];
    const vfs = createVFS(bindingEnv(routed), { tenant });
    const data = new Uint8Array([7, 8]);
    const chunkHash = await hashChunk(data.subarray(0, 1));
    const handle = await vfs.beginMultipartUpload("/signature.bin", {
      size: data.byteLength,
      chunkSize: 1,
    });
    const adversarial = findOutOfPoolPlacement(
      tenant,
      handle.uploadId,
      0,
      handle.poolSize,
    );
    const forgedPayloadHandle = {
      ...handle,
      sessionToken: tamperPayload(handle.sessionToken, (payload) => ({
        ...payload,
        poolSize: adversarial.poolSize,
      })),
    };

    await expect(
      vfs.putMultipartChunk(forgedPayloadHandle, 0, data.subarray(0, 1)),
    ).rejects.toMatchObject({ code: "EACCES" });
    expect(routed).toEqual([
      vfsShardDOName(NS, tenant, undefined, adversarial.shardIndex),
    ]);
    expect(
      await readShardUploadState(
        tenant,
        adversarial.shardIndex,
        handle.uploadId,
        chunkHash,
      ),
    ).toEqual({ refs: 0, staging: 0, chunks: 0, fences: 0 });

    routed.length = 0;
    const legacyShard = placeChunk(tenant, handle.uploadId, 0, handle.poolSize);
    const placementTamperedHandle = {
      ...handle,
      sessionToken: tamperPayload(handle.sessionToken, (payload) => ({
        ...payload,
        placementVersion: 1,
      })),
    };
    await expect(
      vfs.putMultipartChunk(
        placementTamperedHandle,
        0,
        data.subarray(0, 1),
      ),
    ).rejects.toMatchObject({ code: "EACCES" });
    expect(routed).toEqual([
      vfsShardDOName(NS, tenant, undefined, legacyShard),
    ]);
    expect(
      await readShardUploadState(
        tenant,
        legacyShard,
        handle.uploadId,
        chunkHash,
      ),
    ).toEqual({ refs: 0, staging: 0, chunks: 0, fences: 0 });
    await expect(
      (await httpClient(tenant)).putMultipartChunk(
        placementTamperedHandle,
        0,
        data.subarray(0, 1),
      ),
    ).rejects.toMatchObject({ code: "EACCES" });

    routed.length = 0;
    const signedShard = placeMultipartChunk(
      tenant,
      handle.uploadId,
      0,
      handle.poolSize,
      MULTIPART_PLACEMENT_VERSION,
    );
    await expect(
      vfs.putMultipartChunk(
        { ...handle, sessionToken: tamperSignature(handle.sessionToken) },
        0,
        data.subarray(0, 1),
      ),
    ).rejects.toMatchObject({ code: "EACCES" });
    expect(routed).toEqual([vfsShardDOName(NS, tenant, undefined, signedShard)]);
    expect(
      await readShardUploadState(
        tenant,
        signedShard,
        handle.uploadId,
        chunkHash,
      ),
    ).toEqual({ refs: 0, staging: 0, chunks: 0, fences: 0 });
  });

  it("rejects pre-upgrade tokens with resume guidance and resume remints a usable token", async () => {
    const tenant = "sdk-placement-upgrade";
    const routed: string[] = [];
    const vfs = createVFS(bindingEnv(routed), { tenant });
    const data = new Uint8Array([9]);
    const handle = await vfs.beginMultipartUpload("/upgrade.bin", {
      size: data.byteLength,
      chunkSize: 1,
    });
    const { token: oldToken } = await signVFSMultipartToken(
      TEST_ENV as never,
      {
        uploadId: handle.uploadId,
        ns: NS,
        tn: tenant,
        poolSize: handle.poolSize,
        totalChunks: handle.expectedChunks,
        chunkSize: handle.chunkSize,
        totalSize: data.byteLength,
      },
    );
    const user = TEST_ENV.MOSSAIC_USER.get(
      TEST_ENV.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant)),
    );
    await runInDurableObject(user, async (_instance, state) => {
      state.storage.sql.exec(
        "UPDATE upload_sessions SET fence_id = NULL WHERE upload_id = ?",
        handle.uploadId,
      );
    });

    await expect(
      vfs.putMultipartChunk({ ...handle, sessionToken: oldToken }, 0, data),
    ).rejects.toMatchObject({
      code: "EACCES",
      message: expect.stringMatching(/pre-upgrade.*resumeMultipartUpload.*remint/i),
    });
    expect(routed).toEqual([]);

    const legacyHandle: MultipartUploadHandle = {
      uploadId: handle.uploadId,
      path: handle.path,
      chunkSize: handle.chunkSize,
      expectedChunks: handle.expectedChunks,
      poolSize: handle.poolSize,
      sessionToken: oldToken,
      expiresAtMs: handle.expiresAtMs,
    };
    await expect(vfs.resumeMultipartUpload(legacyHandle)).rejects.toMatchObject({
      code: "EINVAL",
    });
    const resumedPage = await vfs.resumeMultipartUpload(legacyHandle, {
      size: data.byteLength,
    });
    const resumed = resumedPage.handle;
    expect(resumedPage.continuation).toBeUndefined();
    expect(resumed.uploadId).toBe(handle.uploadId);
    expect(resumed.sessionToken).not.toBe(oldToken);
    expect(decodePayload(resumed.sessionToken)).toMatchObject({
      fenceId: expect.any(String),
      userId: tenant,
      placementVersion: MULTIPART_PLACEMENT_VERSION,
    });
    await expect(
      runInDurableObject(user, async (_instance, state) =>
        state.storage.sql
          .exec(
            "SELECT placement_version FROM upload_sessions WHERE upload_id = ?",
            handle.uploadId,
          )
          .toArray()[0],
      ),
    ).resolves.toEqual({ placement_version: MULTIPART_PLACEMENT_VERSION });
    const chunk = await vfs.putMultipartChunk(resumed, 0, data);
    await vfs.finalizeMultipartUpload(resumed, [chunk.chunkHash]);
    await expect(vfs.readFile(handle.path)).resolves.toEqual(data);
  });
});
