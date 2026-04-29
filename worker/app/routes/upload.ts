import { Hono } from "hono";
import type { EnvApp as Env } from "@shared/types";
import { authMiddleware } from "@core/lib/auth";
import { legacyAppPlacement } from "@shared/placement";
import { indexFile } from "./search";
import { userStub } from "../lib/user-stub";

const upload = new Hono<{
  Bindings: Env;
  Variables: { userId: string; email: string };
}>();

upload.use("*", authMiddleware());

/**
 * POST /api/upload/init
 * Initialize a new file upload. Returns fileId + chunk spec + poolSize.
 *
 * Phase 17: replaced `stub.fetch("/files/create")` with the typed RPC
 * `UserDO.appCreateFile`. Wire shape is preserved 1:1 so the SPA's
 * `UploadInitResponse` is unchanged.
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
    return c.json(
      { error: "fileName, fileSize, and mimeType are required" },
      400
    );
  }

  const stub = userStub(c.env, userId);

  try {
    const result = await stub.appCreateFile(
      userId,
      body.fileName,
      body.fileSize,
      body.mimeType,
      body.parentId ?? null
    );
    return c.json(result);
  } catch (err) {
    const message = err instanceof Error ? err.message : "Init failed";
    const status = /Quota/i.test(message) ? 403 : 400;
    return c.json({ error: message }, status);
  }
});

/**
 * PUT /api/upload/chunk/:fileId/:chunkIndex
 * Upload a single chunk. The worker streams it to the appropriate
 * ShardDO, then records the chunk in UserDO via typed RPC.
 *
 * Phase 17: ShardDO addressing is unchanged (`shard:userId:idx`)
 * because production chunk bytes live in those legacy DO instances.
 * Routing them to the canonical `vfs:default:userId:sN` namespace
 * would orphan all existing data. The follow-up Phase 17.5
 * (`local/phase-17-plan.md` §5.9) covers ShardDO data migration.
 *
 * The UserDO chunk-record call is a typed RPC (`appRecordChunk`)
 * — the legacy JSON-fetch indirection is gone.
 */
upload.put("/chunk/:fileId/:chunkIndex", async (c) => {
  const userId = c.get("userId");
  const fileId = c.req.param("fileId");
  const chunkIndex = parseInt(c.req.param("chunkIndex"));
  const chunkHash = c.req.header("X-Chunk-Hash") || "";
  // The `X-Pool-Size` header is honored for back-compat with existing
  // SPA clients that read pool size from `init` and echo it back. The
  // server's true pool_size is in `quota.pool_size` (and the new VFS
  // write path reads it server-side via `poolSizeFor`). For the
  // App's legacy chunk path, the legacy header drives placement —
  // misuse only affects this single chunk's placement, not future
  // writes.
  const poolSize = parseInt(c.req.header("X-Pool-Size") || "32");

  if (!chunkHash) {
    return c.json({ error: "X-Chunk-Hash header is required" }, 400);
  }

  // Determine shard placement (legacy app shard naming — see header
  // comment). Phase 17.5: explicit `legacyAppPlacement` dispatch.
  const scope = { ns: "default" as const, tenant: userId };
  const shardIndex = legacyAppPlacement.placeChunk(
    scope,
    fileId,
    chunkIndex,
    poolSize
  );
  const doName = legacyAppPlacement.shardDOName(scope, shardIndex);

  // Stream chunk data to ShardDO (never buffer fully in worker)
  const body = await c.req.arrayBuffer();
  const shardId = c.env.MOSSAIC_SHARD.idFromName(doName);
  const shardStub = c.env.MOSSAIC_SHARD.get(shardId);

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

  // Record chunk in UserDO via typed RPC.
  await userStub(c.env, userId).appRecordChunk(
    fileId,
    chunkIndex,
    chunkHash,
    body.byteLength,
    shardIndex
  );

  const result = await shardRes.json();
  return c.json(result);
});

/**
 * POST /api/upload/complete/:fileId
 * Finalize a file upload — flip status='complete', stamp file_hash,
 * bump quota, schedule semantic indexing.
 *
 * Phase 17: typed RPC `appGetFileManifest` + `appCompleteFile`.
 */
upload.post("/complete/:fileId", async (c) => {
  const userId = c.get("userId");
  const fileId = c.req.param("fileId");
  const { fileHash } = await c.req.json<{ fileHash: string }>();

  if (!fileHash) {
    return c.json({ error: "fileHash is required" }, 400);
  }

  const stub = userStub(c.env, userId);

  // Read the file row to get fileSize + filename + mimeType for
  // quota update and search indexing.
  const file = await stub.appGetFile(fileId);
  if (!file) {
    return c.json({ error: "File not found" }, 404);
  }
  const { file_size: fileSize, file_name: fileName, mime_type: mimeType } = file;

  try {
    await stub.appCompleteFile(fileId, fileHash, userId, fileSize);
  } catch (err) {
    const message = err instanceof Error ? err.message : "Complete failed";
    return c.json({ error: message }, 500);
  }

  // Index file for semantic search (fire-and-forget, non-blocking).
  c.executionCtx.waitUntil(
    indexFile(c.env, userId, fileId, fileName, mimeType, fileSize)
  );

  return c.json({ ok: true, fileId }, 201);
});

export default upload;
