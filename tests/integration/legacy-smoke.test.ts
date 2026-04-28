import { describe, it, expect } from "vitest";
import { env } from "cloudflare:test";

/**
 * Phase 1 — Legacy round-trip smoke.
 *
 * Drives the existing pre-VFS flow end-to-end through UserDO + ShardDO
 * after the schema migrations have been applied:
 *   1. signup → user
 *   2. /files/create → fileId
 *   3. ShardDO PUT /chunk
 *   4. /files/chunk (record)
 *   5. /files/complete
 *   6. /files/manifest → assert chunks + new optional fields default safely
 *   7. ShardDO GET /chunk/<hash> → assert original bytes
 *
 * This proves the Phase 1 schema additions do not break the existing
 * upload/download pipeline that the user-facing app depends on.
 */

interface Env {
  USER_DO: DurableObjectNamespace;
  SHARD_DO: DurableObjectNamespace;
}

const E = env as unknown as Env;
const userStub = (n: string) => E.USER_DO.get(E.USER_DO.idFromName(n));
const shardStub = (n: string) => E.SHARD_DO.get(E.SHARD_DO.idFromName(n));

async function sha256Hex(data: Uint8Array): Promise<string> {
  const buf = await crypto.subtle.digest("SHA-256", data);
  return Array.from(new Uint8Array(buf))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

describe("Legacy upload/download round-trip after Phase 1 migrations", () => {
  it("uploads a single-chunk file and reads it back via manifest + ShardDO", async () => {
    const userId = "smoke-user";
    const shardIdx = 0;
    const userDO = userStub(`user:${userId}`);
    const shardDO = shardStub(`shard:${userId}:${shardIdx}`);

    // 1. signup (creates auth + quota rows)
    const signup = await userDO.fetch(
      new Request("http://internal/signup", {
        method: "POST",
        body: JSON.stringify({
          email: "smoke@example.com",
          password: "password123",
        }),
      })
    );
    expect(signup.ok).toBe(true);
    const { userId: realUserId } = (await signup.json()) as { userId: string };

    // 2. files/create
    const payload = new TextEncoder().encode("hello world from phase 1 smoke");
    const create = await userDO.fetch(
      new Request("http://internal/files/create", {
        method: "POST",
        body: JSON.stringify({
          userId: realUserId,
          fileName: "smoke.txt",
          fileSize: payload.byteLength,
          mimeType: "text/plain",
          parentId: null,
        }),
      })
    );
    expect(create.ok).toBe(true);
    const { fileId } = (await create.json()) as {
      fileId: string;
      chunkSize: number;
      chunkCount: number;
      poolSize: number;
    };

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

    // 4. record chunk in UserDO
    const rec = await userDO.fetch(
      new Request("http://internal/files/chunk", {
        method: "POST",
        body: JSON.stringify({
          fileId,
          chunkIndex: 0,
          chunkHash,
          chunkSize: payload.byteLength,
          shardIndex: shardIdx,
        }),
      })
    );
    expect(rec.ok).toBe(true);

    // 5. complete
    const fileHash = await sha256Hex(new TextEncoder().encode(chunkHash));
    const done = await userDO.fetch(
      new Request("http://internal/files/complete", {
        method: "POST",
        body: JSON.stringify({
          fileId,
          fileHash,
          userId: realUserId,
          fileSize: payload.byteLength,
        }),
      })
    );
    expect(done.ok).toBe(true);

    // 6. manifest — must include chunk + new optional defaults
    const manRes = await userDO.fetch(
      new Request(`http://internal/files/manifest/${fileId}`)
    );
    expect(manRes.ok).toBe(true);
    const manifest = (await manRes.json()) as {
      fileId: string;
      fileSize: number;
      chunks: { index: number; hash: string; size: number; shardIndex: number }[];
      mode?: number;
      nodeKind?: string;
      symlinkTarget?: string | null;
      inlineData?: ArrayBuffer | null;
    };
    expect(manifest.fileId).toBe(fileId);
    expect(manifest.fileSize).toBe(payload.byteLength);
    expect(manifest.chunks).toHaveLength(1);
    expect(manifest.chunks[0].hash).toBe(chunkHash);
    expect(manifest.chunks[0].shardIndex).toBe(shardIdx);
    expect(manifest.mode).toBe(420);
    expect(manifest.nodeKind).toBe("file");
    expect(manifest.symlinkTarget).toBeNull();
    expect(manifest.inlineData).toBeNull();

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
    const sup = await userDO.fetch(
      new Request("http://internal/signup", {
        method: "POST",
        body: JSON.stringify({ email: "ls@example.com", password: "abcdef12" }),
      })
    );
    const { userId } = (await sup.json()) as { userId: string };

    // Create two files at root
    for (const name of ["a.txt", "b.txt"]) {
      await userDO.fetch(
        new Request("http://internal/files/create", {
          method: "POST",
          body: JSON.stringify({
            userId,
            fileName: name,
            fileSize: 5,
            mimeType: "text/plain",
            parentId: null,
          }),
        })
      );
    }

    const listRes = await userDO.fetch(
      new Request("http://internal/files/list", {
        method: "POST",
        body: JSON.stringify({ userId, parentId: null }),
      })
    );
    expect(listRes.ok).toBe(true);
    const list = (await listRes.json()) as {
      files: { fileName: string }[];
      folders: unknown[];
    };
    expect(list.files.map((f) => f.fileName).sort()).toEqual(["a.txt", "b.txt"]);
  });
});
