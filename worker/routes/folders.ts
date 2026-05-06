import { Hono } from "hono";
import type { Env } from "@shared/types";
import { authMiddleware } from "../lib/auth";
import { userDOName } from "../lib/utils";

const folders = new Hono<{
  Bindings: Env;
  Variables: { userId: string; email: string };
}>();

folders.use("*", authMiddleware());

/**
 * POST /api/folders
 * Create a new folder.
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

  const doId = c.env.USER_DO.idFromName(userDOName(userId));
  const stub = c.env.USER_DO.get(doId);

  const res = await stub.fetch(
    new Request("http://internal/folders/create", {
      method: "POST",
      body: JSON.stringify({ userId, name, parentId: parentId ?? null }),
    })
  );

  return c.json(await res.json(), 201);
});

/**
 * GET /api/folders/:folderId
 * List contents of a folder.
 */
folders.get("/:folderId", async (c) => {
  const userId = c.get("userId");
  const folderId = c.req.param("folderId");

  const doId = c.env.USER_DO.idFromName(userDOName(userId));
  const stub = c.env.USER_DO.get(doId);

  const res = await stub.fetch(
    new Request("http://internal/files/list", {
      method: "POST",
      body: JSON.stringify({ userId, parentId: folderId }),
    })
  );

  // Also get folder path for breadcrumbs
  const pathRes = await stub.fetch(
    new Request(`http://internal/folders/path/${folderId}`)
  );

  const contents = await res.json();
  const path = await pathRes.json();

  return c.json({ ...contents, path });
});

export default folders;
