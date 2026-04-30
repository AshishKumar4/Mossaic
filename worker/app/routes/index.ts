import { Hono } from "hono";
import type { EnvApp as Env } from "@shared/types";
import { authMiddleware } from "@core/lib/auth";
import { userStub } from "../lib/user-stub";
import { indexFile } from "./search";

const index = new Hono<{
  Bindings: Env;
  Variables: { userId: string; email: string };
}>();

index.use("*", authMiddleware());

/**
 * POST /api/index/file
 *
 * SPA-side callback fired AFTER a canonical `/api/vfs/multipart/finalize`
 * commits a new file. The SPA's `useUpload` hook calls this with the
 * VFS path it just wrote so the App can:
 *
 *  1. Resolve the path → `files.file_id` for the legacy listFiles /
 *     gallery views (the path-walk lives inside the App because the
 *     legacy `files` table is App-domain).
 *  2. Schedule semantic indexing (text + CLIP) on the new file via
 *     `indexFile()` — fire-and-forget through `executionCtx.waitUntil`.
 *
 * The path's bytes are NOT re-read here; `indexFile` re-reads them
 * server-side via canonical `vfs.readFile()` only when CLIP indexing
 * needs the pixels. Keeping the callback lean keeps the upload-finalize
 * latency budget tight.
 */
index.post("/file", async (c) => {
  const userId = c.get("userId");
  const body = await c.req.json<{ path: string }>();
  if (!body.path || typeof body.path !== "string") {
    return c.json({ error: "path is required" }, 400);
  }

  const stub = userStub(c.env, userId);

  // Resolve path → file_id by querying the unified files table on the
  // user's UserDO. The canonical write path inserted the row keyed by
  // (user_id, parent_id, file_name); the SPA's path is just a leading
  // slash + filename for root-level uploads, but to stay schema-honest
  // we walk the same `parent_id` chain in reverse.
  const fileRow = await stub.appResolveFileByPath(userId, body.path);
  if (!fileRow) {
    return c.json({ error: "File not found at path" }, 404);
  }

  c.executionCtx.waitUntil(
    indexFile(
      c.env,
      userId,
      fileRow.file_id,
      fileRow.file_name,
      fileRow.mime_type,
      fileRow.file_size
    )
  );

  return c.json({ ok: true, fileId: fileRow.file_id }, 201);
});

export default index;
