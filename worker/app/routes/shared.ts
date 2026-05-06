import { Hono } from "hono";
import type { EnvApp as Env } from "@shared/types";
import { userDOName, shardDOName } from "@core/lib/utils";

const shared = new Hono<{ Bindings: Env }>();

/**
 * GET /api/shared/:token/photos
 * Public endpoint — no auth required.
 * The token encodes userId + album data. For V1, albums are stored client-side,
 * so the share link includes the userId in the token to enable image fetching.
 * Token format: base64(JSON({ userId, fileIds }))
 */
shared.get("/:token/photos", async (c) => {
  const token = c.req.param("token");

  try {
    const decoded = JSON.parse(atob(token)) as {
      userId: string;
      fileIds: string[];
      albumName: string;
    };

    const { userId, fileIds, albumName } = decoded;
    if (!userId || !fileIds?.length) {
      return c.json({ error: "Invalid share token" }, 400);
    }

    const doId = c.env.MOSSAIC_USER.idFromName(userDOName(userId));
    const stub = c.env.MOSSAIC_USER.get(doId);

    // Fetch file metadata for each file
    const photos = [];
    for (const fileId of fileIds) {
      const res = await stub.fetch(
        new Request(`http://internal/files/get/${fileId}`)
      );
      if (res.ok) {
        const file = (await res.json()) as Record<string, unknown>;
        if (
          (file.status as string) === "complete" &&
          (file.mime_type as string)?.startsWith("image/")
        ) {
          photos.push({
            fileId: file.file_id as string,
            fileName: file.file_name as string,
            fileSize: file.file_size as number,
            mimeType: file.mime_type as string,
            createdAt: file.created_at as number,
          });
        }
      }
    }

    return c.json({ albumName, photos });
  } catch {
    return c.json({ error: "Invalid share token" }, 400);
  }
});

/**
 * GET /api/shared/:token/image/:fileId
 * Public image serving for shared albums.
 */
shared.get("/:token/image/:fileId", async (c) => {
  const token = c.req.param("token");
  const fileId = c.req.param("fileId");

  try {
    const decoded = JSON.parse(atob(token)) as {
      userId: string;
      fileIds: string[];
    };

    const { userId, fileIds } = decoded;
    if (!userId || !fileIds?.includes(fileId)) {
      return c.json({ error: "Unauthorized" }, 403);
    }

    const doId = c.env.MOSSAIC_USER.idFromName(userDOName(userId));
    const stub = c.env.MOSSAIC_USER.get(doId);

    const manifestRes = await stub.fetch(
      new Request(`http://internal/files/manifest/${fileId}`)
    );

    if (!manifestRes.ok) {
      return c.json({ error: "File not found" }, 404);
    }

    const manifest = (await manifestRes.json()) as {
      fileSize: number;
      mimeType: string;
      chunks: Array<{
        index: number;
        hash: string;
        shardIndex: number;
        size: number;
      }>;
    };

    if (manifest.chunks.length === 1) {
      const chunk = manifest.chunks[0];
      const shardId = c.env.MOSSAIC_SHARD.idFromName(
        shardDOName(userId, chunk.shardIndex)
      );
      const shardStub = c.env.MOSSAIC_SHARD.get(shardId);
      const chunkRes = await shardStub.fetch(
        new Request(`http://internal/chunk/${chunk.hash}`)
      );

      if (!chunkRes.ok) {
        return c.json({ error: "Chunk data not found" }, 404);
      }

      return new Response(chunkRes.body, {
        headers: {
          "Content-Type": manifest.mimeType,
          "Content-Length": chunk.size.toString(),
          "Cache-Control": "public, max-age=86400",
        },
      });
    }

    // Multi-chunk assembly
    const chunkBuffers: ArrayBuffer[] = [];
    for (const chunk of manifest.chunks.sort((a, b) => a.index - b.index)) {
      const shardId = c.env.MOSSAIC_SHARD.idFromName(
        shardDOName(userId, chunk.shardIndex)
      );
      const shardStub = c.env.MOSSAIC_SHARD.get(shardId);
      const chunkRes = await shardStub.fetch(
        new Request(`http://internal/chunk/${chunk.hash}`)
      );
      if (!chunkRes.ok) {
        return c.json({ error: "Chunk data not found" }, 404);
      }
      chunkBuffers.push(await chunkRes.arrayBuffer());
    }

    const totalSize = chunkBuffers.reduce(
      (sum, buf) => sum + buf.byteLength,
      0
    );
    const combined = new Uint8Array(totalSize);
    let offset = 0;
    for (const buf of chunkBuffers) {
      combined.set(new Uint8Array(buf), offset);
      offset += buf.byteLength;
    }

    return new Response(combined, {
      headers: {
        "Content-Type": manifest.mimeType,
        "Content-Length": totalSize.toString(),
        "Cache-Control": "public, max-age=86400",
      },
    });
  } catch {
    return c.json({ error: "Invalid share token" }, 400);
  }
});

export default shared;
