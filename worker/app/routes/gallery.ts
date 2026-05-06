import type { Context } from "hono";
import { Hono } from "hono";
import type { EnvApp as Env } from "@shared/types";
import { authMiddleware } from "@core/lib/auth";
import { createVFS } from "@mossaic/sdk";
import { userStub } from "../lib/user-stub";
import { edgeCacheServe } from "@core/lib/edge-cache";

const gallery = new Hono<{
  Bindings: Env;
  Variables: { userId: string; email: string };
}>();

gallery.use("*", authMiddleware());

/**
 * GET /api/gallery/photos
 * List all image files across all folders, sorted by date descending.
 */
gallery.get("/photos", async (c) => {
  const userId = c.get("userId");
  const photos = await userStub(c.env, userId).appGetGalleryPhotos(userId);
  return c.json({ photos });
});

/**
 * GET /api/gallery/image/:fileId
 * Serve the original image bytes via canonical `vfs.readFile(path)`.
 * Browser-side cached for 1 hour.
 */
gallery.get("/image/:fileId", async (c) => {
  return serveImage(c, c.req.param("fileId"), {
    cacheSeconds: 3600,
    variant: null,
    surfaceTag: "gimg",
  });
});

/**
 * GET /api/gallery/thumbnail/:fileId
 * Serve a pre-rendered `thumb` variant via canonical
 * `vfs.readPreview(path, { variant: "thumb" })`. Cached for 24 hours.
 */
gallery.get("/thumbnail/:fileId", async (c) => {
  return serveImage(c, c.req.param("fileId"), {
    cacheSeconds: 86400,
    variant: "thumb",
    surfaceTag: "gthumb",
  });
});

/**
 * Resolve fileId \u2192 path \u2192 bytes. `variant` selects between the
 * original file (`null`) and a renderer-produced variant
 * (`"thumb"`); the caller-controlled `cacheSeconds` flows into the
 * response.
 *
 * Wraps the origin fetch in `edgeCacheServe`. Cache key is
 * `<surfaceTag>/<userId>/<fileId>/<updated_at>`. The `updated_at`
 * token is bumped by every write that mutates this fileId, so a
 * stale cached response is structurally impossible after a write
 * completes. Auth runs FIRST (authMiddleware on the Hono app); the
 * cache lookup is post-auth.
 */
async function serveImage(
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  c: Context<{ Bindings: Env; Variables: { userId: string; email: string } }>,
  fileId: string,
  opts: {
    cacheSeconds: number;
    variant: "thumb" | null;
    surfaceTag: "gthumb" | "gimg";
  }
): Promise<Response> {
  const env = c.env;
  const userId = c.get("userId");
  const resolved = await userStub(env, userId).appGetFilePath(fileId);
  if (!resolved) {
    return Response.json({ error: "File not found" }, { status: 404 });
  }
  const { path, mimeType, updatedAt } = resolved;

  return edgeCacheServe(
    {
      surfaceTag: opts.surfaceTag,
      namespace: userId,
      fileId,
      updatedAt,
      cacheControl: `private, max-age=${opts.cacheSeconds}`,
      waitUntil: (p) => c.executionCtx.waitUntil(p),
    },
    async () => {
      const vfs = createVFS(env, { tenant: userId });
      try {
        if (opts.variant === "thumb") {
          const result = await vfs.readPreview(path, { variant: "thumb" });
          return new Response(result.bytes, {
            headers: {
              "Content-Type": result.mimeType,
              "Cache-Control": `private, max-age=${opts.cacheSeconds}`,
            },
          });
        }
        const bytes = await vfs.readFile(path);
        return new Response(bytes, {
          headers: {
            "Content-Type": mimeType,
            "Content-Length": String(bytes.byteLength),
            "Cache-Control": `private, max-age=${opts.cacheSeconds}`,
          },
        });
      } catch (err) {
        const code = (err as { code?: string }).code;
        if (code === "ENOENT") {
          return Response.json({ error: "File not found" }, { status: 404 });
        }
        throw err;
      }
    }
  );
}

export default gallery;
