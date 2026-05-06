/**
 * Core-only Worker entry — used by the service-mode
 * deployment at `deployments/service/wrangler.jsonc`. Exports just
 * the two Durable Object classes (UserDOCore, ShardDO) and a
 * minimal Hono router that mounts only the VFS HTTP fallback
 * and a health check.
 *
 * The legacy photo-app routes (/api/auth, /api/upload, /api/files,
 * /api/folders, /api/gallery, /api/analytics, /api/search,
 * /api/shared, /api/download) live in `worker/app/index.ts` and are
 * NOT exposed here — service-mode is a pure SDK backend.
 *
 * SearchDO is App-only. It backs the photo-library's CLIP/BGE vector
 * search in `worker/app/routes/search.ts` and is not used by any
 * Core surface (UserDOCore + ShardDO never touch it). It lives at
 * `worker/app/objects/search/` and is bound only by the App-mode
 * production wrangler.
 *
 * SDK consumers who want a turn-key Mossaic backend on Cloudflare
 * either:
 *   (a) bind UserDOCore + ShardDO directly via
 *       `script_name: "mossaic-core"` in their own wrangler (Mode B
 *       in the SDK README), letting Cloudflare's edge route DO RPC
 *       across Workers; OR
 *   (b) re-export UserDO + ShardDO from `@mossaic/sdk` inside their
 *       own Worker (Mode A — library mode).
 *
 * Vector search is intentionally NOT part of either mode. Consumers
 * who need semantic search should run their own Vectorize index or
 * a separate Durable Object — Mossaic stays a pure VFS.
 */
import { Hono } from "hono";
import { cors } from "hono/cors";
import type { EnvCore as Env } from "../../shared/types";
import vfsRoutes from "./routes/vfs";
import vfsPreviewRoutes from "./routes/vfs-preview";
import yjsWsRoutes from "./routes/vfs-yjs-ws";
import multipartRoutes, { chunkDownload } from "./routes/multipart-routes";

export { UserDOCore } from "./objects/user/index";
export { ShardDO } from "./objects/shard/index";

const app = new Hono<{ Bindings: Env }>();

app.use(
  "/api/*",
  cors({
    origin: "*",
    allowMethods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allowHeaders: ["Content-Type", "Authorization", "X-Session-Token"],
  })
);

// public Yjs WebSocket upgrade route. Mounted BEFORE the
// /api/vfs/* HTTP fallback so the more-specific path wins. Bearer auth
// (header or Sec-WebSocket-Protocol subprotocol) gates the upgrade;
// the underlying DO's own /yjs/ws handler does the protocol work.
app.route("/api/vfs/yjs", yjsWsRoutes);

// multipart parallel transfer engine. Mounted BEFORE the
// general /api/vfs HTTP fallback so /api/vfs/multipart/* takes
// precedence. Same Bearer-token auth as /api/vfs, plus per-chunk
// HMAC session tokens on PUT.
app.route("/api/vfs/multipart", multipartRoutes);

// cacheable per-chunk download endpoint. Lives at
// /api/vfs/chunk/:fileId/:idx. Validates a download token (no
// Bearer required if the token is presented; download tokens are
// scope-bound). Cache-Control: immutable for hash-addressed bytes.
app.route("/api/vfs", chunkDownload);

// preview pipeline + batched manifests. Specific paths under
// /api/vfs (`/readPreview`, `/manifests`); mounted BEFORE the
// general /api/vfs fallback so they take precedence.
app.route("/api/vfs", vfsPreviewRoutes);

// HTTP fallback for non-Worker consumers — Bearer VFS token
// auth, typed UserDOCore RPC under the hood.
app.route("/api/vfs", vfsRoutes);

app.get("/api/health", (c) =>
  c.json({ status: "ok", timestamp: Date.now(), mode: "core" })
);

app.all("*", (c) => c.json({ error: "not found" }, 404));

export default app;
