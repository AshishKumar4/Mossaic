/**
 * Phase 11: Core-only Worker entry — used by the service-mode
 * deployment at `deployments/service/wrangler.jsonc`. Exports just
 * the two Durable Object classes (UserDOCore, ShardDO) and a
 * minimal Hono router that mounts only the VFS HTTP fallback
 * (Phase 7) and a health check.
 *
 * The legacy photo-app routes (/api/auth, /api/upload, /api/files,
 * /api/folders, /api/gallery, /api/analytics, /api/search,
 * /api/shared, /api/download) live in `worker/app/index.ts` and are
 * NOT exposed here — service-mode is a pure SDK backend.
 *
 * Phase 11.1: SearchDO was App-misclassified as Core in Phase 11.
 * It backs the photo-library's CLIP/BGE vector search in
 * `worker/app/routes/search.ts` and is not used by any Core surface
 * (UserDOCore + ShardDO never touch it). It now lives at
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
import type { Env } from "../../shared/types";
import vfsRoutes from "./routes/vfs";
import yjsWsRoutes from "./routes/vfs-yjs-ws";

export { UserDOCore } from "./objects/user/index";
export { ShardDO } from "./objects/shard/index";

const app = new Hono<{ Bindings: Env }>();

app.use(
  "/api/*",
  cors({
    origin: "*",
    allowMethods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allowHeaders: ["Content-Type", "Authorization"],
  })
);

// Phase 13.5 — public Yjs WebSocket upgrade route. Mounted BEFORE the
// /api/vfs/* HTTP fallback so the more-specific path wins. Bearer auth
// (header or Sec-WebSocket-Protocol subprotocol) gates the upgrade;
// the underlying DO's own /yjs/ws handler does the protocol work.
app.route("/api/vfs/yjs", yjsWsRoutes);

// Phase 7 HTTP fallback for non-Worker consumers — Bearer VFS token
// auth, typed UserDOCore RPC under the hood.
app.route("/api/vfs", vfsRoutes);

app.get("/api/health", (c) =>
  c.json({ status: "ok", timestamp: Date.now(), mode: "core" })
);

app.all("*", (c) => c.json({ error: "not found" }, 404));

export default app;
