import { Hono } from "hono";
import type { EnvApp as Env } from "@shared/types";
import { authMiddleware } from "@core/lib/auth";
import { legacyAppPlacement } from "@shared/placement";
import { userStub } from "../lib/user-stub";

const gallery = new Hono<{
  Bindings: Env;
  Variables: { userId: string; email: string };
}>();

gallery.use("*", authMiddleware());

/**
 * GET /api/gallery/photos
 * List all image files across all folders, sorted by date descending.
 *
 * typed RPC `appGetGalleryPhotos`.
 */
gallery.get("/photos", async (c) => {
  const userId = c.get("userId");
  const photos = await userStub(c.env, userId).appGetGalleryPhotos(userId);
  return c.json({ photos });
});

/**
 * GET /api/gallery/image/:fileId
 * Serve a full image by reassembling all chunks. Streams the response
 * for single-chunk images; concatenates for multi-chunk.
 *
 * typed RPC `appGetFileManifest` replaces the legacy fetch
 * indirection. ShardDO addressing stays on the legacy
 * `shard:userId:idx` namespace.
 *
 * Future: replace the multi-chunk concat with `vfs.createReadStream`
 * once ShardDO data is migrated (/ §5.9). For now the
 * legacy concat preserves byte-equality with the existing live
 * deploy.
 */
gallery.get("/image/:fileId", async (c) => {
  return serveImage(c.env, c.get("userId"), c.req.param("fileId"), 3600);
});

/**
 * GET /api/gallery/thumbnail/:fileId
 * Same content as /image but with a 24h Cache-Control. The browser
 * resizes via object-fit: cover.
 */
gallery.get("/thumbnail/:fileId", async (c) => {
  return serveImage(c.env, c.get("userId"), c.req.param("fileId"), 86400);
});

/**
 * Shared helper: fetch the manifest, then either single-chunk-stream
 * or multi-chunk-concat the bytes back as the response body. Sets
 * private Cache-Control with the supplied max-age.
 */
async function serveImage(
  env: Env,
  userId: string,
  fileId: string,
  maxAge: number
): Promise<Response> {
  const manifest = await userStub(env, userId).appGetFileManifest(fileId);
  if (!manifest) {
    return Response.json({ error: "File not found" }, { status: 404 });
  }

  const cacheHeaders = {
    "Content-Type": manifest.mimeType,
    "Cache-Control": `private, max-age=${maxAge}`,
  };

  // Single chunk — stream directly without buffering.
  if (manifest.chunks.length === 1) {
    const chunk = manifest.chunks[0];
    const shardId = env.MOSSAIC_SHARD.idFromName(
      legacyAppPlacement.shardDOName({ ns: "default", tenant: userId }, chunk.shardIndex)
    );
    const shardStub = env.MOSSAIC_SHARD.get(shardId);
    const chunkRes = await shardStub.fetch(
      new Request(`http://internal/chunk/${chunk.hash}`)
    );
    if (!chunkRes.ok) {
      return Response.json({ error: "Chunk data not found" }, { status: 404 });
    }
    return new Response(chunkRes.body, {
      headers: {
        ...cacheHeaders,
        "Content-Length": manifest.fileSize.toString(),
      },
    });
  }

  // Multi-chunk — fetch and concatenate.
  const chunkBuffers: ArrayBuffer[] = [];
  for (const chunk of manifest.chunks.sort((a, b) => a.index - b.index)) {
    const shardId = env.MOSSAIC_SHARD.idFromName(
      legacyAppPlacement.shardDOName({ ns: "default", tenant: userId }, chunk.shardIndex)
    );
    const shardStub = env.MOSSAIC_SHARD.get(shardId);
    const chunkRes = await shardStub.fetch(
      new Request(`http://internal/chunk/${chunk.hash}`)
    );
    if (!chunkRes.ok) {
      return Response.json({ error: "Chunk data not found" }, { status: 404 });
    }
    chunkBuffers.push(await chunkRes.arrayBuffer());
  }

  const totalSize = chunkBuffers.reduce((sum, buf) => sum + buf.byteLength, 0);
  const combined = new Uint8Array(totalSize);
  let offset = 0;
  for (const buf of chunkBuffers) {
    combined.set(new Uint8Array(buf), offset);
    offset += buf.byteLength;
  }

  return new Response(combined, {
    headers: {
      ...cacheHeaders,
      "Content-Length": totalSize.toString(),
    },
  });
}

export default gallery;
