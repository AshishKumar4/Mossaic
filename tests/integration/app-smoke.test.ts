import { describe, it, expect } from "vitest";
import { env } from "cloudflare:test";
import type { UserDO } from "@app/objects/user/user-do";

/**
 * App-mode round-trip smoke.
 *
 * Drives the photo-app upload/download flow end-to-end through UserDO +
 * ShardDO:
 *   1. signup → user (typed RPC `appHandleSignup`)
 *   2. typed RPC `appCreateFile` → fileId
 *   3. ShardDO PUT /chunk
 *   4. typed RPC `appRecordChunk`
 *   5. typed RPC `appCompleteFile`
 *   6. typed RPC `appGetFileManifest` → assert chunks + default fields
 *   7. ShardDO GET /chunk/<hash> → assert original bytes
 *
 * Phase 17: replaced the legacy `_legacyFetch` JSON router calls with
 * the typed RPC surface on `UserDO extends UserDOCore`. Behaviour is
 * bit-equivalent — same SQL helpers underneath, same wire shapes
 * preserved at the App's HTTP boundary (worker-smoke covers that).
 */

interface Env {
  MOSSAIC_USER: DurableObjectNamespace<UserDO>;
  MOSSAIC_SHARD: DurableObjectNamespace;
}

const E = env as unknown as Env;
const userStub = (n: string): DurableObjectStub<UserDO> =>
  E.MOSSAIC_USER.get(E.MOSSAIC_USER.idFromName(n));
const shardStub = (n: string) =>
  E.MOSSAIC_SHARD.get(E.MOSSAIC_SHARD.idFromName(n));

async function sha256Hex(data: Uint8Array): Promise<string> {
  const buf = await crypto.subtle.digest("SHA-256", data);
  return Array.from(new Uint8Array(buf))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

describe("App-mode upload/download round-trip", () => {
  it("uploads a single-chunk file and reads it back via manifest + ShardDO", async () => {
    const userId = "smoke-user";
    const shardIdx = 0;
    const userDO = userStub(`user:${userId}`);
    const shardDO = shardStub(`shard:${userId}:${shardIdx}`);

    // 1. signup (creates auth + quota rows) via typed RPC.
    const { userId: realUserId } = await userDO.appHandleSignup(
      "smoke@example.com",
      "password123"
    );

    // 2. files/create via typed RPC.
    const payload = new TextEncoder().encode("hello world from phase 1 smoke");
    const { fileId } = await userDO.appCreateFile(
      realUserId,
      "smoke.txt",
      payload.byteLength,
      "text/plain",
      null
    );

    // 3. PUT chunk to shard
    const chunkHash = await sha256Hex(payload);
    const putRes = await shardDO.fetch(
      new Request("http://internal/chunk", {
        method: "PUT",
        headers: {
          "X-Chunk-Hash": chunkHash,
          "X-File-Id": fileId,
          "X-Chunk-Index": "0",
          "X-User-Id": realUserId,
        },
        body: payload,
      })
    );
    expect(putRes.ok).toBe(true);
    expect(((await putRes.json()) as { status: string }).status).toBe(
      "created"
    );

    // 4. record chunk in UserDO via typed RPC.
    await userDO.appRecordChunk(
      fileId,
      0,
      chunkHash,
      payload.byteLength,
      shardIdx
    );

    // 5. complete via typed RPC.
    const fileHash = await sha256Hex(new TextEncoder().encode(chunkHash));
    await userDO.appCompleteFile(
      fileId,
      fileHash,
      realUserId,
      payload.byteLength
    );

    // 6. manifest — must include chunk + new optional defaults.
    const manifest = await userDO.appGetFileManifest(fileId);
    expect(manifest).not.toBeNull();
    expect(manifest!.fileId).toBe(fileId);
    expect(manifest!.fileSize).toBe(payload.byteLength);
    expect(manifest!.chunks).toHaveLength(1);
    expect(manifest!.chunks[0].hash).toBe(chunkHash);
    expect(manifest!.chunks[0].shardIndex).toBe(shardIdx);
    expect(manifest!.mode).toBe(420);
    expect(manifest!.nodeKind).toBe("file");
    expect(manifest!.symlinkTarget).toBeNull();
    expect(manifest!.inlineData).toBeNull();

    // 7. fetch chunk back from shard
    const getRes = await shardDO.fetch(
      new Request(`http://internal/chunk/${chunkHash}`)
    );
    expect(getRes.ok).toBe(true);
    const echoed = new Uint8Array(await getRes.arrayBuffer());
    expect(echoed.byteLength).toBe(payload.byteLength);
    expect(new TextDecoder().decode(echoed)).toBe(
      "hello world from phase 1 smoke"
    );
  });

  it("listFiles still works with idx_files_parent and returns the new file", async () => {
    const userDO = userStub("user:list-smoke");
    const { userId } = await userDO.appHandleSignup(
      "ls@example.com",
      "abcdef12"
    );

    // Create two files at root via typed RPC.
    for (const name of ["a.txt", "b.txt"]) {
      await userDO.appCreateFile(userId, name, 5, "text/plain", null);
    }

    const list = await userDO.appListFiles(userId, null);
    expect(list.files.map((f) => f.fileName).sort()).toEqual([
      "a.txt",
      "b.txt",
    ]);
  });
});
