import { Hono } from "hono";
import { cors } from "hono/cors";
import type { Env } from "@shared/types";
import authRoutes from "./routes/auth";
import uploadRoutes from "./routes/upload";
import downloadRoutes from "./routes/download";
import filesRoutes from "./routes/files";
import foldersRoutes from "./routes/folders";
import analyticsRoutes from "./routes/analytics";

export { UserDO } from "./objects/user/index";
export { ShardDO } from "./objects/shard/index";

const app = new Hono<{ Bindings: Env }>();

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
    ],
  })
);

// Mount API routes
app.route("/api/auth", authRoutes);
app.route("/api/upload", uploadRoutes);
app.route("/api/download", downloadRoutes);
app.route("/api/files", filesRoutes);
app.route("/api/folders", foldersRoutes);
app.route("/api/analytics", analyticsRoutes);

// Health check
app.get("/api/health", (c) => c.json({ status: "ok", timestamp: Date.now() }));

// Fall through to ASSETS for non-API routes (SPA)
app.all("*", (c) => c.env.ASSETS.fetch(c.req.raw));

export default app;
