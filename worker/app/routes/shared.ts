import { Hono } from "hono";
import type { EnvApp as Env } from "@shared/types";
import { createVFS } from "@mossaic/sdk";
import { userStub } from "../lib/user-stub";

const shared = new Hono<{ Bindings: Env }>();

/**
 * GET /api/shared/:token/photos
 * Public endpoint — no auth required.
 *
 * The token encodes userId + fileIds. Albums are stored client-side in
 * V1, so the share link includes the userId in the token to enable
 * image fetching.
 *
 * Token format: base64(JSON({ userId, fileIds, albumName }))
 */
shared.get("/:token/photos", async (c) => {
  const token = c.req.param("token");

  let decoded: { userId: string; fileIds: string[]; albumName: string };
  try {
    decoded = JSON.parse(atob(token));
  } catch {
    return c.json({ error: "Invalid share token" }, 400);
  }

  const { userId, fileIds, albumName } = decoded;
  if (!userId || !fileIds?.length) {
    return c.json({ error: "Invalid share token" }, 400);
  }

  const stub = userStub(c.env, userId);

  // Fetch file metadata for each file in parallel.
  const fileResults = await Promise.all(
    fileIds.map((fileId) => stub.appGetFile(fileId))
  );

  const photos = [];
  for (const file of fileResults) {
    if (!file) continue;
    if (file.status !== "complete" || !file.mime_type?.startsWith("image/")) {
      continue;
    }
    photos.push({
      fileId: file.file_id,
      fileName: file.file_name,
      fileSize: file.file_size,
      mimeType: file.mime_type,
      createdAt: file.created_at,
    });
  }

  return c.json({ albumName, photos });
});

/**
 * GET /api/shared/:token/image/:fileId
 * Public image bytes for shared albums via canonical `vfs.readFile()`.
 */
shared.get("/:token/image/:fileId", async (c) => {
  const token = c.req.param("token");
  const fileId = c.req.param("fileId");

  let decoded: { userId: string; fileIds: string[] };
  try {
    decoded = JSON.parse(atob(token));
  } catch {
    return c.json({ error: "Invalid share token" }, 400);
  }

  const { userId, fileIds } = decoded;
  if (!userId || !fileIds?.includes(fileId)) {
    return c.json({ error: "Unauthorized" }, 403);
  }

  const resolved = await userStub(c.env, userId).appGetFilePath(fileId);
  if (!resolved) {
    return c.json({ error: "File not found" }, 404);
  }
  const { path, mimeType } = resolved;

  const vfs = createVFS(c.env, { tenant: userId });
  try {
    const bytes = await vfs.readFile(path);
    return new Response(bytes, {
      headers: {
        "Content-Type": mimeType,
        "Content-Length": String(bytes.byteLength),
        "Cache-Control": "public, max-age=86400",
      },
    });
  } catch (err) {
    const code = (err as { code?: string }).code;
    if (code === "ENOENT") {
      return c.json({ error: "File not found" }, 404);
    }
    throw err;
  }
});

export default shared;
