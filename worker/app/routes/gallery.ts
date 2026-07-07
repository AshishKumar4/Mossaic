import type { Context } from "hono";
import { Hono } from "hono";
import type { EnvApp as Env } from "@shared/types";
import { authMiddleware } from "@core/lib/auth";
import { createVFS } from "@mossaic/sdk";
import { userStub } from "../lib/user-stub";
import { buildImageResponseHeaders } from "../lib/image-response-security";
import { edgeCacheServe } from "@core/lib/edge-cache";
import { serveBytesWithRange } from "@core/lib/http-range";

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
 * `<surfaceTag>/<userId>/<fileId>/<updated_at>`. Freshness relies on
 * every byte-changing write bumping `updated_at`; this implementation
 * invariant is tested but not formally refined to the Lean cache model.
 * Auth runs FIRST (authMiddleware on the Hono app); the cache lookup is
 * post-auth.
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

  // Range requests bypass the Workers Cache wrapper: the cached
  // entry is a 200 with full bytes, and Workers Cache does NOT
  // slice it to 206 on hit. Serving from origin is correct (the
  // ShardDO read is bounded; <video>/<audio> seek issues
  // bytes=START- followed by sequential GETs that the browser
  // assembles). The full-body 200 path keeps Workers Cache happy.
  const rangeHeader = c.req.header("Range") ?? null;

  if (rangeHeader !== null) {
    const vfs = createVFS(env, { tenant: userId });
    try {
      if (opts.variant === "thumb") {
        const result = await vfs.readPreview(path, { variant: "thumb" });
        return serveBytesWithRange(
          result.bytes,
          rangeHeader,
          buildImageResponseHeaders(
            result.mimeType,
            `private, max-age=${opts.cacheSeconds}`
          )
        );
      }
      const bytes = await vfs.readFile(path);
      return serveBytesWithRange(
        bytes,
        rangeHeader,
        buildImageResponseHeaders(mimeType, `private, max-age=${opts.cacheSeconds}`)
      );
    } catch (err) {
      const code = (err as { code?: string }).code;
      if (code === "ENOENT") {
        return Response.json({ error: "File not found" }, { status: 404 });
      }
      throw err;
    }
  }

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
          const headers = buildImageResponseHeaders(
            result.mimeType,
            `private, max-age=${opts.cacheSeconds}`,
            result.bytes.byteLength
          );
          return new Response(result.bytes, {
            headers,
          });
        }
        const bytes = await vfs.readFile(path);
        const headers = buildImageResponseHeaders(
          mimeType,
          `private, max-age=${opts.cacheSeconds}`,
          bytes.byteLength
        );
        return new Response(bytes, {
          headers,
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
