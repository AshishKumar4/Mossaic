import { Hono } from "hono";
import type { EnvApp as Env } from "@shared/types";
import { authMiddleware } from "@core/lib/auth";
import { createVFS } from "@mossaic/sdk";
import { userStub } from "../lib/user-stub";

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
  return serveImage(c.env, c.get("userId"), c.req.param("fileId"), {
    cacheSeconds: 3600,
    variant: null,
  });
});

/**
 * GET /api/gallery/thumbnail/:fileId
 * Serve a pre-rendered `thumb` variant via canonical
 * `vfs.readPreview(path, { variant: "thumb" })`. Cached for 24 hours.
 */
gallery.get("/thumbnail/:fileId", async (c) => {
  return serveImage(c.env, c.get("userId"), c.req.param("fileId"), {
    cacheSeconds: 86400,
    variant: "thumb",
  });
});

/**
 * Resolve fileId → path → bytes. `variant` selects between the original
 * file (`null`) and a renderer-produced variant (`"thumbnail"`); the
 * caller-controlled `cacheSeconds` flows into the response.
 */
async function serveImage(
  env: Env,
  userId: string,
  fileId: string,
  opts: { cacheSeconds: number; variant: "thumb" | null }
): Promise<Response> {
  const resolved = await userStub(env, userId).appGetFilePath(fileId);
  if (!resolved) {
    return Response.json({ error: "File not found" }, { status: 404 });
  }
  const { path, mimeType } = resolved;

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

export default gallery;
