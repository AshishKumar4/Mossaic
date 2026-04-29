import { Hono } from "hono";
import type { EnvApp as Env } from "@shared/types";
import { authMiddleware } from "@core/lib/auth";
import { userDOName, shardDOName } from "@core/lib/utils";

const download = new Hono<{
  Bindings: Env;
  Variables: { userId: string; email: string };
}>();

download.use("*", authMiddleware());

/**
 * GET /api/download/manifest/:fileId
 * Get the file manifest for download (chunk list with shard locations).
 */
download.get("/manifest/:fileId", async (c) => {
  const userId = c.get("userId");
  const fileId = c.req.param("fileId");

  const doId = c.env.MOSSAIC_USER.idFromName(userDOName(userId));
  const stub = c.env.MOSSAIC_USER.get(doId);

  const res = await stub.fetch(
    new Request(`http://internal/files/manifest/${fileId}`)
  );

  if (!res.ok) {
    return c.json({ error: "File not found" }, 404);
  }

  return c.json(await res.json());
});

/**
 * GET /api/download/chunk/:fileId/:chunkIndex
 * Download a specific chunk. Worker streams it from the ShardDO.
 */
download.get("/chunk/:fileId/:chunkIndex", async (c) => {
  const userId = c.get("userId");
  const fileId = c.req.param("fileId");
  const chunkIndex = parseInt(c.req.param("chunkIndex"));

  // Get manifest to find chunk hash and shard
  const doId = c.env.MOSSAIC_USER.idFromName(userDOName(userId));
  const stub = c.env.MOSSAIC_USER.get(doId);

  const manifestRes = await stub.fetch(
    new Request(`http://internal/files/manifest/${fileId}`)
  );

  if (!manifestRes.ok) {
    return c.json({ error: "File not found" }, 404);
  }

  const manifest = (await manifestRes.json()) as {
    chunks: Array<{ index: number; hash: string; shardIndex: number; size: number }>;
    mimeType: string;
  };

  const chunk = manifest.chunks.find((ch) => ch.index === chunkIndex);
  if (!chunk) {
    return c.json({ error: "Chunk not found" }, 404);
  }

  // Fetch chunk from ShardDO — stream it directly
  const shardId = c.env.MOSSAIC_SHARD.idFromName(
    shardDOName(userId, chunk.shardIndex)
  );
  const shardStub = c.env.MOSSAIC_SHARD.get(shardId);

  const chunkRes = await shardStub.fetch(
    new Request(`http://internal/chunk/${chunk.hash}`)
  );

  if (!chunkRes.ok) {
    return c.json({ error: "Chunk data not found" }, 404);
  }

  // Stream the response directly (never buffer)
  return new Response(chunkRes.body, {
    headers: {
      "Content-Type": "application/octet-stream",
      "Content-Length": chunk.size.toString(),
      "Cache-Control": "public, max-age=31536000, immutable",
      ETag: `"${chunk.hash}"`,
    },
  });
});

export default download;
