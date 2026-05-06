import { Hono } from "hono";
import type { Env } from "@shared/types";
import { authMiddleware } from "../lib/auth";
import { shardDOName, userDOName } from "../lib/utils";
import { placeChunk } from "@shared/placement";
import { indexFile } from "./search";

const upload = new Hono<{
  Bindings: Env;
  Variables: { userId: string; email: string };
}>();

upload.use("*", authMiddleware());

/**
 * POST /api/upload/init
 * Initialize a new file upload. Returns fileId, chunkSpec, poolSize.
 */
upload.post("/init", async (c) => {
  const userId = c.get("userId");
  const body = await c.req.json<{
    fileName: string;
    fileSize: number;
    mimeType: string;
    parentId?: string | null;
  }>();

  if (!body.fileName || !body.fileSize || !body.mimeType) {
    return c.json({ error: "fileName, fileSize, and mimeType are required" }, 400);
  }

  const doId = c.env.USER_DO.idFromName(userDOName(userId));
  const stub = c.env.USER_DO.get(doId);

  const res = await stub.fetch(
    new Request("http://internal/files/create", {
      method: "POST",
      body: JSON.stringify({
        userId,
        fileName: body.fileName,
        fileSize: body.fileSize,
        mimeType: body.mimeType,
        parentId: body.parentId ?? null,
      }),
    })
  );

  if (!res.ok) {
    const err = (await res.json()) as { error: string };
    return c.json({ error: err.error }, res.status as 400);
  }

  return c.json(await res.json());
});

/**
 * PUT /api/upload/chunk/:fileId/:chunkIndex
 * Upload a single chunk. The worker streams it to the appropriate ShardDO.
 */
upload.put("/chunk/:fileId/:chunkIndex", async (c) => {
  const userId = c.get("userId");
  const fileId = c.req.param("fileId");
  const chunkIndex = parseInt(c.req.param("chunkIndex"));
  const chunkHash = c.req.header("X-Chunk-Hash") || "";
  // Phase 7 NOTE: this legacy route honors the client-supplied
  // `X-Pool-Size` header for back-compat with existing user-facing app
  // clients that read the pool size from `init` and echo it back. The
  // server's true pool_size is in `quota.pool_size` and is the source
  // of truth for the new VFS write path (vfs-ops.ts:poolSizeFor —
  // server-authoritative). If a misbehaving legacy client sends a
  // wrong value here, only THIS request's chunk placement is affected;
  // future writes by the same user re-derive from the server-side
  // quota row. The new VFS path (worker/objects/user/vfs-ops.ts) does
  // NOT read this header and cannot be subverted.
  const poolSize = parseInt(c.req.header("X-Pool-Size") || "32");

  if (!chunkHash) {
    return c.json({ error: "X-Chunk-Hash header is required" }, 400);
  }

  // Determine shard placement (legacy app shard naming)
  const shardIndex = placeChunk(userId, fileId, chunkIndex, poolSize);
  const doName = shardDOName(userId, shardIndex);

  // Stream chunk data to ShardDO (never buffer fully in worker)
  const body = await c.req.arrayBuffer();
  const shardId = c.env.SHARD_DO.idFromName(doName);
  const shardStub = c.env.SHARD_DO.get(shardId);

  const shardRes = await shardStub.fetch(
    new Request("http://internal/chunk", {
      method: "PUT",
      headers: {
        "X-Chunk-Hash": chunkHash,
        "X-File-Id": fileId,
        "X-Chunk-Index": chunkIndex.toString(),
        "X-User-Id": userId,
      },
      body,
    })
  );

  if (!shardRes.ok) {
    return c.json({ error: "Failed to store chunk" }, 500);
  }

  // Record chunk in UserDO
  const userDoId = c.env.USER_DO.idFromName(userDOName(userId));
  const userStub = c.env.USER_DO.get(userDoId);

  await userStub.fetch(
    new Request("http://internal/files/chunk", {
      method: "POST",
      body: JSON.stringify({
        fileId,
        chunkIndex,
        chunkHash,
        chunkSize: body.byteLength,
        shardIndex,
      }),
    })
  );

  const result = await shardRes.json();
  return c.json(result);
});

/**
 * POST /api/upload/complete/:fileId
 * Finalize a file upload.
 */
upload.post("/complete/:fileId", async (c) => {
  const userId = c.get("userId");
  const fileId = c.req.param("fileId");
  const { fileHash } = await c.req.json<{ fileHash: string }>();

  if (!fileHash) {
    return c.json({ error: "fileHash is required" }, 400);
  }

  // Get file info from UserDO
  const userDoId = c.env.USER_DO.idFromName(userDOName(userId));
  const userStub = c.env.USER_DO.get(userDoId);

  const fileRes = await userStub.fetch(
    new Request(`http://internal/files/get/${fileId}`)
  );
  if (!fileRes.ok) {
    return c.json({ error: "File not found" }, 404);
  }
  const file = (await fileRes.json()) as {
    file_size: number;
    file_name: string;
    mime_type: string;
  };

  // Complete the file
  const res = await userStub.fetch(
    new Request("http://internal/files/complete", {
      method: "POST",
      body: JSON.stringify({
        fileId,
        fileHash,
        userId,
        fileSize: file.file_size,
      }),
    })
  );

  if (!res.ok) {
    const err = (await res.json()) as { error: string };
    return c.json({ error: err.error }, 500);
  }

  // Index file for semantic search (fire-and-forget, non-blocking)
  c.executionCtx.waitUntil(
    indexFile(c.env, userId, fileId, file.file_name, file.mime_type, file.file_size)
  );

  return c.json({ ok: true, fileId }, 201);
});

export default upload;
