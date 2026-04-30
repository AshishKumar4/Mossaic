import { Hono } from "hono";
import type { EnvApp as Env } from "@shared/types";
import { authMiddleware } from "@core/lib/auth";
import { userStub } from "../lib/user-stub";

const files = new Hono<{
  Bindings: Env;
  Variables: { userId: string; email: string };
}>();

files.use("*", authMiddleware());

/**
 * GET /api/files
 * List files and folders in root or a specific folder.
 *
 * typed RPC `appListFiles` replaces the legacy fetch
 * indirection. Wire shape (`{files, folders}`) is preserved 1:1.
 */
files.get("/", async (c) => {
  const userId = c.get("userId");
  const parentId = c.req.query("parentId") || null;

  const result = await userStub(c.env, userId).appListFiles(userId, parentId);
  return c.json(result);
});

/**
 * DELETE /api/files/:fileId
 * Soft-delete a file and decrement the user's quota.
 *
 * typed RPC `appDeleteFile` (atomic delete + quota update
 * on the DO side, no extra round-trips).
 */
files.delete("/:fileId", async (c) => {
  const userId = c.get("userId");
  const fileId = c.req.param("fileId");

  const result = await userStub(c.env, userId).appDeleteFile(fileId, userId);
  if (!result.ok) {
    return c.json({ error: "File not found" }, 404);
  }
  return c.json({ ok: true });
});

/**
 * GET /api/files/:fileId/path
 *
 * Resolve a `files.file_id` to its absolute VFS path. Used by the SPA
 * `useDownload` hook (and any UI surface that addresses files by id):
 * `parallelDownload(client, path)` requires a real path string, not a
 * fileId. Tenant isolation is enforced by `userStub` resolving the
 * caller's per-tenant UserDO instance — appGetFilePath only walks the
 * `files` + `folders` tables on that one instance.
 */
files.get("/:fileId/path", async (c) => {
  const userId = c.get("userId");
  const fileId = c.req.param("fileId");

  const resolved = await userStub(c.env, userId).appGetFilePath(fileId);
  if (!resolved) {
    return c.json({ error: "File not found" }, 404);
  }
  return c.json({ path: resolved.path, mimeType: resolved.mimeType });
});

export default files;
