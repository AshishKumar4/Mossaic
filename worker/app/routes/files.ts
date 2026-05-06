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

export default files;
