/**
 * Phase 11: Core-only Worker entry — used by the service-mode
 * deployment at `deployments/service/wrangler.jsonc`. Exports just
 * the three Durable Object classes (UserDOCore, ShardDO, SearchDO)
 * and a minimal Hono router that mounts only the VFS HTTP fallback
 * (Phase 7) and a health check.
 *
 * The legacy photo-app routes (/api/auth, /api/upload, /api/files,
 * /api/folders, /api/gallery, /api/analytics, /api/search,
 * /api/shared, /api/download) live in `worker/app/index.ts` and are
 * NOT exposed here — service-mode is a pure SDK backend.
 *
 * SDK consumers who want a turn-key Mossaic backend on Cloudflare
 * either:
 *   (a) bind UserDOCore + ShardDO + SearchDO directly via
 *       `script_name: "mossaic-core"` in their own wrangler (Mode B
 *       in the SDK README), letting Cloudflare's edge route DO RPC
 *       across Workers; OR
 *   (b) re-export UserDO + ShardDO + SearchDO from `@mossaic/sdk`
 *       inside their own Worker (Mode A — library mode).
 */
import { Hono } from "hono";
import { cors } from "hono/cors";
import type { Env } from "@shared/types";
import vfsRoutes from "./routes/vfs";

export { UserDOCore } from "./objects/user/index";
export { ShardDO } from "./objects/shard/index";
export { SearchDO } from "./objects/search/index";

const app = new Hono<{ Bindings: Env }>();

app.use(
  "/api/*",
  cors({
    origin: "*",
    allowMethods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allowHeaders: ["Content-Type", "Authorization"],
  })
);

// Phase 7 HTTP fallback for non-Worker consumers — Bearer VFS token
// auth, typed UserDOCore RPC under the hood.
app.route("/api/vfs", vfsRoutes);

app.get("/api/health", (c) =>
  c.json({ status: "ok", timestamp: Date.now(), mode: "core" })
);

app.all("*", (c) => c.json({ error: "not found" }, 404));

export default app;
