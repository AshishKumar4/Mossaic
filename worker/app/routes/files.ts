import { Hono } from "hono";
import type { EnvApp as Env } from "@shared/types";
import { authMiddleware } from "@core/lib/auth";
import { userDOName } from "@core/lib/utils";

const files = new Hono<{
  Bindings: Env;
  Variables: { userId: string; email: string };
}>();

files.use("*", authMiddleware());

/**
 * GET /api/files
 * List files and folders in root or a specific folder.
 */
files.get("/", async (c) => {
  const userId = c.get("userId");
  const parentId = c.req.query("parentId") || null;

  const doId = c.env.MOSSAIC_USER.idFromName(userDOName(userId));
  const stub = c.env.MOSSAIC_USER.get(doId);

  const res = await stub.fetch(
    new Request("http://internal/files/list", {
      method: "POST",
      body: JSON.stringify({ userId, parentId }),
    })
  );

  return c.json(await res.json());
});

/**
 * DELETE /api/files/:fileId
 * Soft-delete a file.
 */
files.delete("/:fileId", async (c) => {
  const userId = c.get("userId");
  const fileId = c.req.param("fileId");

  const doId = c.env.MOSSAIC_USER.idFromName(userDOName(userId));
  const stub = c.env.MOSSAIC_USER.get(doId);

  const res = await stub.fetch(
    new Request(`http://internal/files/delete/${fileId}`, {
      method: "DELETE",
      headers: { "X-User-Id": userId },
    })
  );

  if (!res.ok) {
    return c.json({ error: "File not found" }, 404);
  }

  return c.json({ ok: true });
});

export default files;
