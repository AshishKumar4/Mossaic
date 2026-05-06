import { describe, it, expect } from "vitest";
import { env } from "cloudflare:test";
import { userDOName } from "@core/lib/utils";
import type { UserDO } from "../../worker/app/objects/user/user-do";

/**
 * Phase 17.6 — App UserDO multipart RPC tests.
 *
 *   M1.  appBeginMultipart inserts an `upload_sessions` row + a
 *        `files` row in 'uploading' state, returns a valid session
 *        token + chunkSize/totalChunks.
 *   M2.  appFinalizeMultipart promotes 'uploading' → 'complete' on
 *        the legacy `files` row + inserts file_chunks rows + bumps
 *        quota.
 *   M3.  appAbortMultipart marks aborted + drops the legacy `files`
 *        row + is idempotent.
 *   M4.  appGetMultipartStatus returns landed[]/total/bytesUploaded/
 *        expiresAtMs for an open session.
 *   M5.  appOpenManifest wraps `getFileManifest` into the multipart
 *        manifest shape (mimeType + chunks + size + chunkCount).
 */

interface E {
  MOSSAIC_USER: DurableObjectNamespace<UserDO>;
}
const TEST_ENV = env as unknown as E;

function userStub(userId: string) {
  const id = TEST_ENV.MOSSAIC_USER.idFromName(userDOName(userId));
  return TEST_ENV.MOSSAIC_USER.get(id);
}

describe("Phase 17.6 — App UserDO multipart RPCs", () => {
  it("M1 — appBeginMultipart inserts session + uploading file row + returns token", async () => {
    const userId = "rpc-user-m1";
    const stub = userStub(userId);
    // Pre-seed the user row + quota so checkQuota passes.
    await stub.appHandleSignup(`m1-${userId}@x.test`, "password-123");

    const r = await stub.appBeginMultipart(userId, "/foo.bin", { size: 200 });
    expect(r.uploadId).toMatch(/^[A-Za-z0-9_-]+$/);
    expect(r.chunkSize).toBeGreaterThan(0);
    expect(r.totalChunks).toBeGreaterThan(0);
    expect(typeof r.sessionToken).toBe("string");
    expect(r.expiresAtMs).toBeGreaterThan(Date.now());
    expect(r.landed).toEqual([]);
    expect(r.poolSize).toBeGreaterThan(0);
    expect(r.putEndpoint).toBe(`/api/upload/multipart/${r.uploadId}`);
  });

  it("M2 — appFinalizeMultipart promotes file → complete + records chunks", async () => {
    const userId = "rpc-user-m2";
    const stub = userStub(userId);
    await stub.appHandleSignup(`m2-${userId}@x.test`, "password-123");

    // chunkSize defaults via computeChunkSpec; force small so we get
    // multiple chunks (ChunkEvent surface) without slow tests.
    const totalSize = 500;
    const begin = await stub.appBeginMultipart(userId, "/m2.bin", {
      size: totalSize,
      chunkSize: 100,
    });
    expect(begin.totalChunks).toBe(5);

    // Simulate ShardDO chunk PUTs by directly calling
    // putChunkMultipart on each shard. We need actual bytes for hash
    // alignment; reuse `crypto.subtle.digest` via shared helper via
    // worker import.
    const { hashChunk } = await import("../../shared/crypto");
    const hashes: string[] = [];
    for (let i = 0; i < begin.totalChunks; i++) {
      // Shape data per chunk: 100 bytes, distinct per index.
      const slice = new Uint8Array(100).fill(0x40 + i);
      const h = await hashChunk(slice);
      hashes.push(h);
      // Place chunk + put through shard.
      const { legacyAppPlacement } = await import("../../shared/placement");
      const sIdx = legacyAppPlacement.placeChunk(
        { ns: "default", tenant: userId },
        begin.uploadId,
        i,
        begin.poolSize
      );
      const shardName = legacyAppPlacement.shardDOName(
        { ns: "default", tenant: userId },
        sIdx
      );
      const shardId = (env as { MOSSAIC_SHARD: DurableObjectNamespace }).MOSSAIC_SHARD.idFromName(
        shardName
      );
      const shardStub = (
        env as { MOSSAIC_SHARD: DurableObjectNamespace }
      ).MOSSAIC_SHARD.get(shardId) as unknown as {
        putChunkMultipart: (
          hash: string,
          data: Uint8Array,
          uploadId: string,
          idx: number,
          userId: string
        ) => Promise<{ status: string; bytesStored: number }>;
      };
      await shardStub.putChunkMultipart(h, slice, begin.uploadId, i, userId);
    }

    const finalized = await stub.appFinalizeMultipart(
      userId,
      begin.uploadId,
      hashes
    );
    expect(finalized.fileId).toBe(begin.uploadId);
    expect(finalized.size).toBe(500);
    expect(finalized.chunkCount).toBe(5);
    expect(finalized.fileHash).toMatch(/^[0-9a-f]{64}$/);

    // The file row is now in 'complete' state.
    const file = await stub.appGetFile(begin.uploadId);
    expect(file?.status).toBe("complete");
    expect(file?.file_hash).toBe(finalized.fileHash);
    expect(file?.file_size).toBe(500);
  });

  it("M3 — appAbortMultipart drops the session + uploading file row, idempotent", async () => {
    const userId = "rpc-user-m3";
    const stub = userStub(userId);
    await stub.appHandleSignup(`m3-${userId}@x.test`, "password-123");
    const begin = await stub.appBeginMultipart(userId, "/abort.bin", {
      size: 200,
    });
    const r1 = await stub.appAbortMultipart(userId, begin.uploadId);
    expect(r1.ok).toBe(true);

    // The 'uploading' file row should be gone.
    const file = await stub.appGetFile(begin.uploadId);
    expect(file).toBeNull();

    // Second abort is idempotent.
    const r2 = await stub.appAbortMultipart(userId, begin.uploadId);
    expect(r2.ok).toBe(true);
  });

  it("M4 — appGetMultipartStatus returns landed[] + bookkeeping for an open session", async () => {
    const userId = "rpc-user-m4";
    const stub = userStub(userId);
    await stub.appHandleSignup(`m4-${userId}@x.test`, "password-123");
    const begin = await stub.appBeginMultipart(userId, "/status.bin", {
      size: 200,
      chunkSize: 100,
    });
    // No chunks landed yet.
    const s0 = await stub.appGetMultipartStatus(userId, begin.uploadId);
    expect(s0.landed).toEqual([]);
    expect(s0.total).toBe(2);
    expect(s0.bytesUploaded).toBe(0);
    expect(s0.expiresAtMs).toBeGreaterThan(Date.now());
  }, 30_000);

  it("M5 — appOpenManifest wraps legacy getFileManifest into multipart shape", async () => {
    const userId = "rpc-user-m5";
    const stub = userStub(userId);
    await stub.appHandleSignup(`m5-${userId}@x.test`, "password-123");

    // Legacy upload via appCreateFile → appRecordChunk → appCompleteFile.
    const create = await stub.appCreateFile(
      userId,
      "legacy.bin",
      300,
      "application/octet-stream",
      null
    );
    const { hashChunk } = await import("../../shared/crypto");
    const slice = new Uint8Array(300).fill(0x42);
    const h = await hashChunk(slice);
    await stub.appRecordChunk(create.fileId, 0, h, 300, 0);
    await stub.appCompleteFile(create.fileId, h, userId, 300);

    const m = await stub.appOpenManifest(create.fileId);
    expect(m.fileId).toBe(create.fileId);
    expect(m.size).toBe(300);
    expect(m.chunkCount).toBe(create.chunkCount);
    expect(m.mimeType).toBe("application/octet-stream");
    expect(m.chunks.length).toBe(create.chunkCount);
    expect(m.chunks[0]!.hash).toBe(h);
    expect(m.inlined).toBe(false);
  });
});
