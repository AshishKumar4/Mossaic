import { Hono } from "hono";
import type { EnvApp as Env } from "@shared/types";
import { legacyAppPlacement } from "@shared/placement";
import { userStub } from "../lib/user-stub";

const shared = new Hono<{ Bindings: Env }>();

/**
 * GET /api/shared/:token/photos
 * Public endpoint — no auth required.
 *
 * The token encodes userId + fileIds. For V1, albums are stored
 * client-side, so the share link includes the userId in the token
 * to enable image fetching.
 *
 * Token format: base64(JSON({ userId, fileIds, albumName }))
 *
 * typed RPC `appGetFile` replaces the legacy fetch
 * indirection for each file metadata read.
 */
shared.get("/:token/photos", async (c) => {
  const token = c.req.param("token");

  let decoded: { userId: string; fileIds: string[]; albumName: string };
  try {
    decoded = JSON.parse(atob(token));
  } catch {
    return c.json({ error: "Invalid share token" }, 400);
  }

  const { userId, fileIds, albumName } = decoded;
  if (!userId || !fileIds?.length) {
    return c.json({ error: "Invalid share token" }, 400);
  }

  const stub = userStub(c.env, userId);

  // Fetch file metadata for each file. Done in parallel — each is one
  // typed RPC.
  const fileResults = await Promise.all(
    fileIds.map((fileId) => stub.appGetFile(fileId))
  );

  const photos = [];
  for (const file of fileResults) {
    if (!file) continue;
    if (file.status !== "complete" || !file.mime_type?.startsWith("image/")) {
      continue;
    }
    photos.push({
      fileId: file.file_id,
      fileName: file.file_name,
      fileSize: file.file_size,
      mimeType: file.mime_type,
      createdAt: file.created_at,
    });
  }

  return c.json({ albumName, photos });
});

/**
 * GET /api/shared/:token/image/:fileId
 * Public image serving for shared albums.
 *
 * typed RPC `appGetFileManifest`. ShardDO addressing stays
 * on legacy naming.
 */
shared.get("/:token/image/:fileId", async (c) => {
  const token = c.req.param("token");
  const fileId = c.req.param("fileId");

  let decoded: { userId: string; fileIds: string[] };
  try {
    decoded = JSON.parse(atob(token));
  } catch {
    return c.json({ error: "Invalid share token" }, 400);
  }

  const { userId, fileIds } = decoded;
  if (!userId || !fileIds?.includes(fileId)) {
    return c.json({ error: "Unauthorized" }, 403);
  }

  const manifest = await userStub(c.env, userId).appGetFileManifest(fileId);
  if (!manifest) {
    return c.json({ error: "File not found" }, 404);
  }

  if (manifest.chunks.length === 1) {
    const chunk = manifest.chunks[0];
    const shardId = c.env.MOSSAIC_SHARD.idFromName(
      legacyAppPlacement.shardDOName({ ns: "default", tenant: userId }, chunk.shardIndex)
    );
    const shardStub = c.env.MOSSAIC_SHARD.get(shardId);
    const chunkRes = await shardStub.fetch(
      new Request(`http://internal/chunk/${chunk.hash}`)
    );
    if (!chunkRes.ok) {
      return c.json({ error: "Chunk data not found" }, 404);
    }
    return new Response(chunkRes.body, {
      headers: {
        "Content-Type": manifest.mimeType,
        "Content-Length": chunk.size.toString(),
        "Cache-Control": "public, max-age=86400",
      },
    });
  }

  // Multi-chunk assembly
  const chunkBuffers: ArrayBuffer[] = [];
  for (const chunk of manifest.chunks.sort((a, b) => a.index - b.index)) {
    const shardId = c.env.MOSSAIC_SHARD.idFromName(
      legacyAppPlacement.shardDOName({ ns: "default", tenant: userId }, chunk.shardIndex)
    );
    const shardStub = c.env.MOSSAIC_SHARD.get(shardId);
    const chunkRes = await shardStub.fetch(
      new Request(`http://internal/chunk/${chunk.hash}`)
    );
    if (!chunkRes.ok) {
      return c.json({ error: "Chunk data not found" }, 404);
    }
    chunkBuffers.push(await chunkRes.arrayBuffer());
  }

  const totalSize = chunkBuffers.reduce(
    (sum, buf) => sum + buf.byteLength,
    0
  );
  const combined = new Uint8Array(totalSize);
  let offset = 0;
  for (const buf of chunkBuffers) {
    combined.set(new Uint8Array(buf), offset);
    offset += buf.byteLength;
  }

  return new Response(combined, {
    headers: {
      "Content-Type": manifest.mimeType,
      "Content-Length": totalSize.toString(),
      "Cache-Control": "public, max-age=86400",
    },
  });
});

export default shared;
