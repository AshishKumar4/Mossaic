import { Hono } from "hono";
import type { EnvApp as Env } from "@shared/types";
import { authMiddleware } from "@core/lib/auth";
import { legacyAppPlacement } from "@shared/placement";
import { userStub } from "../lib/user-stub";

const download = new Hono<{
  Bindings: Env;
  Variables: { userId: string; email: string };
}>();

download.use("*", authMiddleware());

/**
 * GET /api/download/manifest/:fileId
 *
 * Phase 17: typed RPC `appGetFileManifest` replaces the legacy
 * `stub.fetch("/files/manifest/:id")`.
 */
download.get("/manifest/:fileId", async (c) => {
  const userId = c.get("userId");
  const fileId = c.req.param("fileId");

  const manifest = await userStub(c.env, userId).appGetFileManifest(fileId);
  if (!manifest) {
    return c.json({ error: "File not found" }, 404);
  }
  return c.json(manifest);
});

/**
 * GET /api/download/chunk/:fileId/:chunkIndex
 *
 * Phase 17: manifest read via typed RPC. ShardDO addressing stays on
 * the legacy `shard:userId:idx` namespace because production chunk
 * bytes live there.
 */
download.get("/chunk/:fileId/:chunkIndex", async (c) => {
  const userId = c.get("userId");
  const fileId = c.req.param("fileId");
  const chunkIndex = parseInt(c.req.param("chunkIndex"));

  const manifest = await userStub(c.env, userId).appGetFileManifest(fileId);
  if (!manifest) {
    return c.json({ error: "File not found" }, 404);
  }

  const chunk = manifest.chunks.find((ch) => ch.index === chunkIndex);
  if (!chunk) {
    return c.json({ error: "Chunk not found" }, 404);
  }

  // Fetch chunk from ShardDO — stream it directly.
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
