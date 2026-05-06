import { Hono } from "hono";
import { cors } from "hono/cors";
import type { EnvApp as Env } from "@shared/types";
import { requestIdMiddleware } from "@core/lib/logger";
import authRoutes from "./routes/auth";
import filesRoutes from "./routes/files";
import indexRoutes from "./routes/index";
import foldersRoutes from "./routes/folders";
import analyticsRoutes from "./routes/analytics";
import galleryRoutes from "./routes/gallery";
import sharedRoutes from "./routes/shared";
import searchRoutes from "./routes/search";
import vfsRoutes from "@core/routes/vfs";
import vfsPreviewRoutes, { previewVariant } from "@core/routes/vfs-preview";
import yjsWsRoutes from "@core/routes/vfs-yjs-ws";
import multipartRoutes, { chunkDownload } from "@core/routes/multipart-routes";

// UserDO is the App-side subclass that adds the legacy
// photo-app HTTP routes on top of UserDOCore. Production wrangler
// binds class_name: "UserDO" — the class name preserved verbatim
// from. ShardDO is a pure-Core class re-exported through
// this entry so wrangler can resolve both from one main file.
//
// SearchDO is App-only (CLIP/BGE vector store backing
// the photo-library's /api/search route). It moved from
// `worker/core/objects/search/` to `worker/app/objects/search/`
// and is no longer re-exported from `@mossaic/sdk` — SDK consumers
// who want vector search bring their own DO.
export { UserDO } from "./objects/user/index";
export { ShardDO } from "@core/objects/shard/index";
export { SearchDO } from "./objects/search/index";

const app = new Hono<{ Bindings: Env }>();

// Assign a request-id to every incoming request before any other
// middleware runs. Mirrors `X-Mossaic-Request-Id` onto the response
// so clients can correlate. Cheap (1 randomUUID per request); see
// worker/core/lib/logger.ts.
app.use("/api/*", requestIdMiddleware());

// CORS for development
app.use(
  "/api/*",
  cors({
    origin: "*",
    allowMethods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allowHeaders: [
      "Content-Type",
      "Authorization",
      "X-Chunk-Hash",
      "X-Pool-Size",
      "X-File-Id",
      "X-Chunk-Index",
      "X-Shard-Index",
      "X-User-Id",
      //
      "X-Session-Token",
    ],
  })
);

// Mount API routes
app.route("/api/auth", authRoutes);
app.route("/api/files", filesRoutes);
app.route("/api/index", indexRoutes);
app.route("/api/folders", foldersRoutes);
app.route("/api/analytics", analyticsRoutes);
app.route("/api/gallery", galleryRoutes);
app.route("/api/shared", sharedRoutes);
app.route("/api/search", searchRoutes);
// public Yjs WebSocket upgrade. Mounted BEFORE
// /api/vfs so the more-specific path wins. Bearer-auth gated;
// the photo-app's /api/upload, /api/download, etc. are unaffected.
app.route("/api/vfs/yjs", yjsWsRoutes);
// multipart parallel transfer engine. Mounted BEFORE the
// general /api/vfs HTTP fallback so /api/vfs/multipart/* takes
// precedence.
app.route("/api/vfs/multipart", multipartRoutes);
// cacheable per-chunk download endpoint at
// /api/vfs/chunk/:fileId/:idx — token-auth, immutable cache.
app.route("/api/vfs", chunkDownload);
// Signed preview-variant route at
// /api/vfs/preview-variant/:token. HMAC token IS the auth;
// bytes are content-addressed (CDN-cacheable across all clients).
app.route("/api/vfs", previewVariant);
// preview pipeline + batched manifests. Mounted BEFORE the
// general /api/vfs fallback so the specific paths
// `/readPreview` and `/manifests` take precedence.
app.route("/api/vfs", vfsPreviewRoutes);
// HTTP fallback for non-Worker consumers of the @mossaic/sdk.
// Auth via Bearer VFS token (signVFSToken / verifyVFSToken). Routes
// translate HTTP → typed UserDO RPC. The legacy app's /api/* surface
// above is unaffected.
app.route("/api/vfs", vfsRoutes);

// Health check
app.get("/api/health", (c) => c.json({ status: "ok", timestamp: Date.now() }));

// Fall through to ASSETS for non-API routes (SPA).
//
// Phase 50 hardening: unmatched `/api/*` paths must NOT fall
// through to ASSETS — that returned the SPA's index.html (200) on
// a typo'd API URL, which is confusing for SDK consumers and
// triggers a 500 in test-env where ASSETS is unbound. Now we
// 404 cleanly for `/api/*`, and only non-API misses serve the SPA
// shell. The fallback to a bare 404 when ASSETS itself is missing
// (as in tests) keeps the route deterministic across environments.
app.all("*", (c) => {
  const url = new URL(c.req.url);
  if (url.pathname.startsWith("/api/")) {
    return c.json({ error: "Not Found" }, 404);
  }
  if (
    typeof (c.env as { ASSETS?: { fetch: (req: Request) => Promise<Response> } })
      .ASSETS?.fetch !== "function"
  ) {
    return c.text("Not Found", 404);
  }
  return c.env.ASSETS.fetch(c.req.raw);
});

export default app;
