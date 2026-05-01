import { Hono } from "hono";
import type { EnvApp as Env } from "@shared/types";
import { createVFS } from "@mossaic/sdk";
import { userStub } from "../lib/user-stub";
import { edgeCacheServe } from "../lib/edge-cache";

const shared = new Hono<{ Bindings: Env }>();

/**
 * GET /api/shared/:token/photos
 * Public endpoint — no auth required.
 *
 * The token encodes userId + fileIds. Albums are stored client-side in
 * V1, so the share link includes the userId in the token to enable
 * image fetching.
 *
 * Token format: base64(JSON({ userId, fileIds, albumName }))
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

  // Fetch file metadata for each file in parallel.
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
 * Public image bytes for shared albums via canonical `vfs.readFile()`.
 *
 * Phase 36 \u2014 Workers Cache wrap. Cache key is
 * `simg/<userId>/<fileId>/<updated_at>`. Auth check
 * (token-includes-fileId) runs BEFORE the cache lookup so a
 * cached response can never serve an unauthorized request \u2014
 * the cache key is per-user but the auth gate is the bouncer.
 * `updated_at` busts the cache on any write to the fileId.
 *
 * Public sharing implies high hit rates (viral links). The
 * `public, max-age=86400` Cache-Control on the response also
 * lets the CDN edge tier cache, double-stacking the win.
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

  const resolved = await userStub(c.env, userId).appGetFilePath(fileId);
  if (!resolved) {
    return c.json({ error: "File not found" }, 404);
  }
  const { path, mimeType, updatedAt } = resolved;

  return edgeCacheServe(
    {
      surfaceTag: "simg",
      namespace: userId,
      fileId,
      updatedAt,
      cacheControl: "public, max-age=86400",
      waitUntil: (p) => c.executionCtx.waitUntil(p),
    },
    async () => {
      const vfs = createVFS(c.env, { tenant: userId });
      try {
        const bytes = await vfs.readFile(path);
        return new Response(bytes, {
          headers: {
            "Content-Type": mimeType,
            "Content-Length": String(bytes.byteLength),
            "Cache-Control": "public, max-age=86400",
          },
        });
      } catch (err) {
        const code = (err as { code?: string }).code;
        if (code === "ENOENT") {
          return c.json({ error: "File not found" }, 404);
        }
        throw err;
      }
    }
  );
});

export default shared;
