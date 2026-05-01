import { Hono } from "hono";
import type { EnvApp as Env } from "@shared/types";
import { createVFS } from "@mossaic/sdk";
import { userStub } from "../lib/user-stub";
import { edgeCacheServe } from "@core/lib/edge-cache";
import { verifyShareToken, VFSConfigError } from "@core/lib/auth";

const shared = new Hono<{ Bindings: Env }>();

/**
 * GET /api/shared/:token/photos
 * Public endpoint — no session auth. The HMAC-signed share token
 * IS the auth; `verifyShareToken` rejects forgeries.
 *
 * Pre-fix (P0-1): the token was an unsigned `base64(JSON({...}))`
 * payload. Anyone who knew or guessed any userId could forge a
 * token and read that user's files. The fix replaces the wire
 * format with an HMAC-signed JWT (`scope: "vfs-share"`) keyed off
 * the same `JWT_SECRET` as the rest of the auth surface, signed
 * via `signShareToken` at the auth-gated mint route
 * (`POST /api/auth/share-token`).
 *
 * Existing pre-fix tokens fail verification (they aren't JWTs and
 * lack the `vfs-share` scope claim) and surface as 403. The SPA
 * re-shares to obtain new tokens. Acceptable: pre-fix tokens
 * convey access freely already and were never security-meaningful.
 */
shared.get("/:token/photos", async (c) => {
  const token = c.req.param("token");

  let payload;
  try {
    payload = await verifyShareToken(c.env, token);
  } catch (err) {
    if (err instanceof VFSConfigError) {
      // JWT_SECRET missing → service mis-configured.
      return c.json({ error: err.message }, 503);
    }
    throw err;
  }
  if (!payload) {
    return c.json({ error: "Invalid or expired share token" }, 403);
  }

  const stub = userStub(c.env, payload.userId);

  // Fetch file metadata for each file in parallel.
  const fileResults = await Promise.all(
    payload.fileIds.map((fileId) => stub.appGetFile(fileId))
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

  return c.json({ albumName: payload.albumName, photos });
});

/**
 * GET /api/shared/:token/image/:fileId
 * Public image bytes for shared albums via canonical `vfs.readFile()`.
 * Same HMAC verification as the manifest endpoint.
 *
 * Workers Cache wrap. Cache key is
 * `simg/<userId>/<fileId>/<updated_at>`. The HMAC verify + the
 * token-includes-fileId check run BEFORE the cache lookup so a
 * cached response can never serve an unauthorized request — the
 * cache key is per-user but the auth gate is the bouncer.
 * `updated_at` busts the cache on any write to the fileId.
 *
 * Public sharing implies high hit rates (viral links). The
 * `public, max-age=86400` Cache-Control on the response also
 * lets the CDN edge tier cache, double-stacking the win.
 */
shared.get("/:token/image/:fileId", async (c) => {
  const token = c.req.param("token");
  const fileId = c.req.param("fileId");

  let payload;
  try {
    payload = await verifyShareToken(c.env, token);
  } catch (err) {
    if (err instanceof VFSConfigError) {
      return c.json({ error: err.message }, 503);
    }
    throw err;
  }
  if (!payload) {
    return c.json({ error: "Invalid or expired share token" }, 403);
  }
  // The token's fileIds bind exactly which files the share grants.
  // Requesting any other fileId → 403.
  if (!payload.fileIds.includes(fileId)) {
    return c.json({ error: "Unauthorized" }, 403);
  }

  const resolved = await userStub(c.env, payload.userId).appGetFilePath(
    fileId
  );
  if (!resolved) {
    return c.json({ error: "File not found" }, 404);
  }
  const { path, mimeType, updatedAt } = resolved;
  const userId = payload.userId;

  return edgeCacheServe(
    {
      surfaceTag: "simg",
      namespace: userId,
      fileId,
      updatedAt,
      cacheControl: "public, max-age=86400",
      waitUntil: (p) => c.executionCtx.waitUntil(p),
    },
    async () => {
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
    }
  );
});

export default shared;
