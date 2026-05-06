import { Hono } from "hono";
import type { Env } from "@shared/types";
import { authMiddleware } from "../lib/auth";
import { userDOName, shardDOName } from "../lib/utils";

const gallery = new Hono<{
  Bindings: Env;
  Variables: { userId: string; email: string };
}>();

gallery.use("*", authMiddleware());

/**
 * GET /api/gallery/photos
 * List all image files across all folders, sorted by date descending.
 */
gallery.get("/photos", async (c) => {
  const userId = c.get("userId");

  const doId = c.env.USER_DO.idFromName(userDOName(userId));
  const stub = c.env.USER_DO.get(doId);

  const res = await stub.fetch(
    new Request("http://internal/gallery/photos", {
      method: "POST",
      body: JSON.stringify({ userId }),
    })
  );

  return c.json(await res.json());
});

/**
 * GET /api/gallery/image/:fileId
 * Serve a full image by reassembling all chunks. Streams the response.
 * Supports range requests for large images.
 */
gallery.get("/image/:fileId", async (c) => {
  const userId = c.get("userId");
  const fileId = c.req.param("fileId");

  const doId = c.env.USER_DO.idFromName(userDOName(userId));
  const stub = c.env.USER_DO.get(doId);

  // Get manifest
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

  // For small images (single chunk), stream directly
  if (manifest.chunks.length === 1) {
    const chunk = manifest.chunks[0];
    const shardId = c.env.SHARD_DO.idFromName(
      shardDOName(userId, chunk.shardIndex)
    );
    const shardStub = c.env.SHARD_DO.get(shardId);
    const chunkRes = await shardStub.fetch(
      new Request(`http://internal/chunk/${chunk.hash}`)
    );

    if (!chunkRes.ok) {
      return c.json({ error: "Chunk data not found" }, 404);
    }

    return new Response(chunkRes.body, {
      headers: {
        "Content-Type": manifest.mimeType,
        "Content-Length": manifest.fileSize.toString(),
        "Cache-Control": "private, max-age=3600",
      },
    });
  }

  // For multi-chunk images, fetch all chunks and concatenate
  const chunkBuffers: ArrayBuffer[] = [];
  for (const chunk of manifest.chunks.sort((a, b) => a.index - b.index)) {
    const shardId = c.env.SHARD_DO.idFromName(
      shardDOName(userId, chunk.shardIndex)
    );
    const shardStub = c.env.SHARD_DO.get(shardId);
    const chunkRes = await shardStub.fetch(
      new Request(`http://internal/chunk/${chunk.hash}`)
    );
    if (!chunkRes.ok) {
      return c.json({ error: "Chunk data not found" }, 404);
    }
    chunkBuffers.push(await chunkRes.arrayBuffer());
  }

  // Concatenate all buffers
  const totalSize = chunkBuffers.reduce((sum, buf) => sum + buf.byteLength, 0);
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
      "Cache-Control": "private, max-age=3600",
    },
  });
});

/**
 * GET /api/gallery/thumbnail/:fileId
 * Serve a thumbnail. For V1, just serve the original (browser resizes).
 * The frontend uses object-fit: cover for display.
 */
gallery.get("/thumbnail/:fileId", async (c) => {
  const userId = c.get("userId");
  const fileId = c.req.param("fileId");

  const doId = c.env.USER_DO.idFromName(userDOName(userId));
  const stub = c.env.USER_DO.get(doId);

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

  // For thumbnails, we serve the whole image (browser does the resizing).
  // For small images (<=1MB), just serve the single chunk
  if (manifest.chunks.length === 1) {
    const chunk = manifest.chunks[0];
    const shardId = c.env.SHARD_DO.idFromName(
      shardDOName(userId, chunk.shardIndex)
    );
    const shardStub = c.env.SHARD_DO.get(shardId);
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
        "Cache-Control": "private, max-age=86400",
      },
    });
  }

  // Multi-chunk: assemble
  const chunkBuffers: ArrayBuffer[] = [];
  for (const chunk of manifest.chunks.sort((a, b) => a.index - b.index)) {
    const shardId = c.env.SHARD_DO.idFromName(
      shardDOName(userId, chunk.shardIndex)
    );
    const shardStub = c.env.SHARD_DO.get(shardId);
    const chunkRes = await shardStub.fetch(
      new Request(`http://internal/chunk/${chunk.hash}`)
    );
    if (!chunkRes.ok) {
      return c.json({ error: "Chunk data not found" }, 404);
    }
    chunkBuffers.push(await chunkRes.arrayBuffer());
  }

  const totalSize = chunkBuffers.reduce((sum, buf) => sum + buf.byteLength, 0);
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
      "Cache-Control": "private, max-age=86400",
    },
  });
});

export default gallery;
