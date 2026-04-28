import { Hono } from "hono";
import type { Env } from "@shared/types";
import type { SearchResult, ProviderStatus, VectorSpace } from "@shared/embedding-types";
import { classifyResultType, isClipIndexable, isImageMime } from "@shared/embedding-types";
import { authMiddleware } from "@core/lib/auth";
import { userDOName, shardDOName } from "@core/lib/utils";
import {
  createEmbeddingProviders,
  getBestTextProvider,
  getCLIPProvider,
  CLIPEmbedding,
} from "../lib/embeddings";
import { DOVectorStore } from "../lib/vector-store";

const search = new Hono<{
  Bindings: Env;
  Variables: { userId: string; email: string };
}>();

search.use("*", authMiddleware());

/**
 * POST /api/search
 * Multi-modal semantic search over user's files.
 * Queries both CLIP (image) and text vector spaces, merges results.
 * Body: { query: string, topK?: number }
 */
search.post("/", async (c) => {
  const userId = c.get("userId");
  const { query, topK = 10 } = await c.req.json<{
    query: string;
    topK?: number;
  }>();

  if (!query || query.trim().length === 0) {
    return c.json({ error: "query is required" }, 400);
  }

  const vectorStore = new DOVectorStore(c.env, userId);
  const allResults: SearchResult[] = [];

  // ── CLIP space search (images) ──
  const clip = getCLIPProvider(c.env);
  if (clip) {
    try {
      const [clipVector] = await clip.embed([query]);
      const clipMatches = await vectorStore.query(clipVector, topK, undefined, "clip");
      for (const match of clipMatches) {
        if (match.score <= 0) continue;
        const mimeType = match.metadata?.mimeType || "image/jpeg";
        allResults.push({
          fileId: match.metadata?.fileId || match.id,
          fileName: match.metadata?.fileName || "Unknown",
          score: match.score,
          mimeType,
          resultType: classifyResultType(mimeType),
          space: "clip",
          hasThumbnail: isImageMime(mimeType),
          fileSize: match.metadata?.fileSize ? parseInt(match.metadata.fileSize) : undefined,
          highlight: match.metadata?.text,
        });
      }
    } catch (err) {
      console.error("CLIP search failed:", err);
      // Continue — text search may still work
    }
  }

  // ── Text space search (documents / all files) ──
  try {
    const textProvider = await getBestTextProvider(c.env);
    const [textVector] = await textProvider.embed([query]);
    const textMatches = await vectorStore.query(textVector, topK, undefined, "text");
    for (const match of textMatches) {
      if (match.score <= 0) continue;
      const mimeType = match.metadata?.mimeType || "application/octet-stream";
      allResults.push({
        fileId: match.metadata?.fileId || match.id,
        fileName: match.metadata?.fileName || "Unknown",
        score: match.score,
        mimeType,
        resultType: classifyResultType(mimeType),
        space: "text",
        hasThumbnail: isImageMime(mimeType),
        fileSize: match.metadata?.fileSize ? parseInt(match.metadata.fileSize) : undefined,
        highlight: match.metadata?.text,
      });
    }
  } catch (err) {
    console.error("Text search failed:", err);
  }

  // ── Merge and deduplicate ──
  const merged = mergeResults(allResults, topK);

  return c.json({
    results: merged,
    query,
  });
});

/**
 * GET /api/search/providers
 * List available providers and their status.
 */
search.get("/providers", async (c) => {
  const userId = c.get("userId");
  const embeddingProviders = createEmbeddingProviders(c.env);

  const statuses: ProviderStatus[] = [];

  for (const [name, provider] of embeddingProviders) {
    const available = await provider.isAvailable();
    statuses.push({
      name,
      type: "embedding",
      available,
      dimensions: provider.dimensions,
      space: provider.space,
    });
  }

  // Vector store availability
  statuses.push({
    name: "durable-object",
    type: "vectorStore",
    available: true,
  });

  const hasVectorize = !!(c.env as unknown as Record<string, unknown>).VECTORIZE_INDEX;
  statuses.push({
    name: "vectorize",
    type: "vectorStore",
    available: hasVectorize,
  });

  // Get active config
  const activeEmbedding = await getActiveProvider(c.env, userId, "embedding") || "simple";
  const activeVectorStore = await getActiveProvider(c.env, userId, "vectorStore") || "durable-object";

  // Get index stats (now includes per-space counts)
  let indexedCount = 0;
  let spaces: { space: string; count: number; dimensions: number | null }[] = [];
  try {
    const doId = c.env.SEARCH_DO.idFromName(`search:${userId}`);
    const stub = c.env.SEARCH_DO.get(doId);
    const statsRes = await stub.fetch(new Request("http://internal/stats"));
    if (statsRes.ok) {
      const stats = (await statsRes.json()) as {
        vectorCount: number;
        spaces: { space: string; count: number; dimensions: number | null }[];
      };
      indexedCount = stats.vectorCount;
      spaces = stats.spaces;
    }
  } catch {
    // Ignore — fresh DO
  }

  return c.json({
    providers: statuses,
    active: {
      embedding: activeEmbedding,
      vectorStore: activeVectorStore,
    },
    indexedCount,
    spaces,
  });
});

/**
 * POST /api/search/config
 * Set active provider.
 * Body: { embedding?: string, vectorStore?: string }
 */
search.post("/config", async (c) => {
  const userId = c.get("userId");
  const body = await c.req.json<{ embedding?: string; vectorStore?: string }>();

  const doId = c.env.SEARCH_DO.idFromName(`search:${userId}`);
  const stub = c.env.SEARCH_DO.get(doId);

  const config: Record<string, string> = {};
  if (body.embedding) config["embedding"] = body.embedding;
  if (body.vectorStore) config["vectorStore"] = body.vectorStore;

  const res = await stub.fetch(
    new Request("http://internal/config", {
      method: "POST",
      body: JSON.stringify(config),
    })
  );

  if (!res.ok) {
    return c.json({ error: "Failed to update config" }, 500);
  }

  return c.json({ ok: true, config });
});

/**
 * POST /api/search/reindex
 * Re-embed all user files in both vector spaces.
 */
search.post("/reindex", async (c) => {
  const userId = c.get("userId");

  // Get all user files from UserDO
  const userDoId = c.env.USER_DO.idFromName(userDOName(userId));
  const userStub = c.env.USER_DO.get(userDoId);

  const filesRes = await userStub.fetch(new Request("http://internal/files/list"));
  if (!filesRes.ok) {
    return c.json({ error: "Failed to list files" }, 500);
  }

  const filesData = (await filesRes.json()) as {
    files: { file_id: string; file_name: string; file_size: number; mime_type: string; status: string }[];
  };

  const files = filesData.files.filter((f) => f.status === "complete");

  if (files.length === 0) {
    return c.json({ ok: true, indexed: { text: 0, clip: 0 } });
  }

  const vectorStore = new DOVectorStore(c.env, userId);
  let textIndexed = 0;
  let clipIndexed = 0;

  // ── Text space: embed all files by metadata ──
  const textProvider = await getBestTextProvider(c.env);
  const textBatchSize = textProvider.maxBatchSize;

  for (let i = 0; i < files.length; i += textBatchSize) {
    const batch = files.slice(i, i + textBatchSize);
    const texts = batch.map((f) =>
      buildEmbeddingText(f.file_name, f.mime_type, f.file_size)
    );

    const embeddings = await textProvider.embed(texts);

    const vectors = batch.map((f, j) => ({
      id: f.file_id,
      values: embeddings[j],
      metadata: {
        fileId: f.file_id,
        fileName: f.file_name,
        mimeType: f.mime_type,
        fileSize: f.file_size.toString(),
        isImage: isImageMime(f.mime_type) ? "true" : "false",
        text: texts[j],
      },
    }));

    await vectorStore.upsert(vectors, "text");
    textIndexed += vectors.length;
  }

  // ── CLIP space: embed image files by content ──
  const clip = getCLIPProvider(c.env);
  if (clip) {
    const imageFiles = files.filter((f) => isClipIndexable(f.mime_type, f.file_size));

    for (const file of imageFiles) {
      try {
        const imageBytes = await fetchFileBytes(c.env, userId, file.file_id);
        if (!imageBytes) continue;

        const embedding = await clip.embedImage(imageBytes);
        await vectorStore.upsert(
          [
            {
              id: file.file_id,
              values: embedding,
              metadata: {
                fileId: file.file_id,
                fileName: file.file_name,
                mimeType: file.mime_type,
                fileSize: file.file_size.toString(),
                isImage: "true",
              },
            },
          ],
          "clip"
        );
        clipIndexed++;
      } catch (err) {
        console.error(`CLIP indexing failed for ${file.file_name}:`, err);
      }
    }
  }

  return c.json({
    ok: true,
    indexed: { text: textIndexed, clip: clipIndexed },
    providers: { text: textProvider.name, clip: clip ? "clip" : "unavailable" },
  });
});

// ── Helpers ──

/**
 * Build a text representation of a file for embedding.
 */
export function buildEmbeddingText(
  fileName: string,
  mimeType: string,
  fileSize: number
): string {
  // Extract meaningful parts from filename
  const nameWithoutExt = fileName.replace(/\.[^.]+$/, "");
  const ext = fileName.split(".").pop() || "";

  // Break camelCase and snake_case into words
  const nameWords = nameWithoutExt
    .replace(/([a-z])([A-Z])/g, "$1 $2")
    .replace(/[_\-./]/g, " ")
    .toLowerCase();

  // Human-readable file type
  const typeLabel = mimeType.split("/").join(" ");

  // Size category
  const sizeLabel =
    fileSize < 1024
      ? "tiny"
      : fileSize < 1024 * 1024
        ? "small"
        : fileSize < 10 * 1024 * 1024
          ? "medium"
          : fileSize < 100 * 1024 * 1024
            ? "large"
            : "very large";

  return `${nameWords} ${ext} ${typeLabel} ${sizeLabel} file`;
}

/**
 * Fetch full file bytes by reassembling chunks from ShardDOs.
 * Used for CLIP image indexing. Returns null on failure.
 */
async function fetchFileBytes(
  env: Env,
  userId: string,
  fileId: string
): Promise<Uint8Array | null> {
  try {
    const userDoId = env.USER_DO.idFromName(userDOName(userId));
    const userStub = env.USER_DO.get(userDoId);

    const manifestRes = await userStub.fetch(
      new Request(`http://internal/files/manifest/${fileId}`)
    );
    if (!manifestRes.ok) return null;

    const manifest = (await manifestRes.json()) as {
      fileSize: number;
      chunks: Array<{ index: number; hash: string; shardIndex: number; size: number }>;
    };

    // Single chunk — fast path
    if (manifest.chunks.length === 1) {
      const chunk = manifest.chunks[0];
      const shardId = env.SHARD_DO.idFromName(shardDOName(userId, chunk.shardIndex));
      const shardStub = env.SHARD_DO.get(shardId);
      const chunkRes = await shardStub.fetch(
        new Request(`http://internal/chunk/${chunk.hash}`)
      );
      if (!chunkRes.ok) return null;
      return new Uint8Array(await chunkRes.arrayBuffer());
    }

    // Multi-chunk — reassemble
    const sortedChunks = manifest.chunks.sort((a, b) => a.index - b.index);
    const combined = new Uint8Array(manifest.fileSize);
    let offset = 0;

    for (const chunk of sortedChunks) {
      const shardId = env.SHARD_DO.idFromName(shardDOName(userId, chunk.shardIndex));
      const shardStub = env.SHARD_DO.get(shardId);
      const chunkRes = await shardStub.fetch(
        new Request(`http://internal/chunk/${chunk.hash}`)
      );
      if (!chunkRes.ok) return null;
      const buf = new Uint8Array(await chunkRes.arrayBuffer());
      combined.set(buf, offset);
      offset += buf.byteLength;
    }

    return combined;
  } catch (err) {
    console.error("Failed to fetch file bytes:", err);
    return null;
  }
}

/**
 * Merge results from multiple vector spaces.
 * Deduplicates by fileId (keeping the higher-scoring entry),
 * normalizes scores within each space using min-max,
 * then ranks by normalized score.
 */
function mergeResults(results: SearchResult[], topK: number): SearchResult[] {
  if (results.length === 0) return [];

  // Group by space for normalization
  const bySpace = new Map<VectorSpace, SearchResult[]>();
  for (const r of results) {
    const group = bySpace.get(r.space) || [];
    group.push(r);
    bySpace.set(r.space, group);
  }

  // Min-max normalize within each space
  const normalized: SearchResult[] = [];
  for (const [, group] of bySpace) {
    if (group.length === 0) continue;

    const scores = group.map((r) => r.score);
    const min = Math.min(...scores);
    const max = Math.max(...scores);
    const range = max - min;

    for (const r of group) {
      // Normalize to [0, 1] range within this space
      const normalizedScore = range > 0 ? (r.score - min) / range : 1;
      normalized.push({
        ...r,
        score: Math.round(normalizedScore * 1000) / 1000,
      });
    }
  }

  // Deduplicate by fileId — keep higher score
  const deduped = new Map<string, SearchResult>();
  for (const r of normalized) {
    const existing = deduped.get(r.fileId);
    if (!existing || r.score > existing.score) {
      deduped.set(r.fileId, r);
    }
  }

  // Sort by score descending, take topK
  return Array.from(deduped.values())
    .sort((a, b) => b.score - a.score)
    .slice(0, topK);
}

/**
 * Get active provider from SearchDO config.
 */
async function getActiveProvider(
  env: Env,
  userId: string,
  key: string
): Promise<string | null> {
  try {
    const doId = env.SEARCH_DO.idFromName(`search:${userId}`);
    const stub = env.SEARCH_DO.get(doId);
    const res = await stub.fetch(new Request("http://internal/config"));
    if (!res.ok) return null;
    const config = (await res.json()) as Record<string, string>;
    return config[key] || null;
  } catch {
    return null;
  }
}

/**
 * Index a single file into both vector spaces.
 * Called fire-and-forget from upload complete hook.
 *
 * - Always indexes into text space (filename/metadata embedding)
 * - For CLIP-indexable images, also indexes into clip space (image content embedding)
 */
export async function indexFile(
  env: Env,
  userId: string,
  fileId: string,
  fileName: string,
  mimeType: string,
  fileSize: number
): Promise<void> {
  const vectorStore = new DOVectorStore(env, userId);

  // ── Text space: always index ──
  try {
    const textProvider = await getBestTextProvider(env);
    const text = buildEmbeddingText(fileName, mimeType, fileSize);
    const [embedding] = await textProvider.embed([text]);

    await vectorStore.upsert(
      [
        {
          id: fileId,
          values: embedding,
          metadata: {
            fileId,
            fileName,
            mimeType,
            fileSize: fileSize.toString(),
            isImage: isImageMime(mimeType) ? "true" : "false",
            text,
          },
        },
      ],
      "text"
    );
  } catch (err) {
    console.error("Text indexing failed:", err);
  }

  // ── CLIP space: index if image and within limits ──
  if (isClipIndexable(mimeType, fileSize)) {
    try {
      const clip = getCLIPProvider(env);
      if (clip) {
        const imageBytes = await fetchFileBytes(env, userId, fileId);
        if (imageBytes) {
          const embedding = await clip.embedImage(imageBytes);
          await vectorStore.upsert(
            [
              {
                id: fileId,
                values: embedding,
                metadata: {
                  fileId,
                  fileName,
                  mimeType,
                  fileSize: fileSize.toString(),
                  isImage: "true",
                },
              },
            ],
            "clip"
          );
        }
      }
    } catch (err) {
      console.error("CLIP indexing failed:", err);
    }
  }
}

export default search;
