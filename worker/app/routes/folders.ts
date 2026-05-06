import { Hono } from "hono";
import type { EnvApp as Env } from "@shared/types";
import { authMiddleware } from "@core/lib/auth";
import { userStub } from "../lib/user-stub";

const folders = new Hono<{
  Bindings: Env;
  Variables: { userId: string; email: string };
}>();

folders.use("*", authMiddleware());

/**
 * POST /api/folders
 * Create a new folder.
 *
 * Phase 17: typed RPC `appCreateFolder`.
 */
folders.post("/", async (c) => {
  const userId = c.get("userId");
  const { name, parentId } = await c.req.json<{
    name: string;
    parentId?: string | null;
  }>();

  if (!name) {
    return c.json({ error: "Folder name is required" }, 400);
  }

  const folder = await userStub(c.env, userId).appCreateFolder(
    userId,
    name,
    parentId ?? null
  );
  return c.json(folder, 201);
});

/**
 * GET /api/folders/:folderId
 * List contents of a folder + breadcrumb path.
 *
 * Phase 17: typed RPCs `appListFiles` + `appGetFolderPath`.
 */
folders.get("/:folderId", async (c) => {
  const userId = c.get("userId");
  const folderId = c.req.param("folderId");

  const stub = userStub(c.env, userId);
  const [contents, path] = await Promise.all([
    stub.appListFiles(userId, folderId),
    stub.appGetFolderPath(folderId),
  ]);

  return c.json({ ...contents, path });
});

export default folders;
