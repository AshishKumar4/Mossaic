import { Hono } from "hono";
import type { EnvApp as Env } from "@shared/types";
import type { SearchResult, ProviderStatus, VectorSpace } from "@shared/embedding-types";
import { classifyResultType, isClipIndexable, isImageMime } from "@shared/embedding-types";
import { authMiddleware } from "@core/lib/auth";
import { ctxFromHono, logError, logWarn } from "@core/lib/logger";
import { createVFS } from "@mossaic/sdk";
import { userStub } from "../lib/user-stub";
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
      logError(
        "CLIP search failed",
        ctxFromHono(c),
        err,
        { event: "search_clip_failed", query }
      );
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
    logError(
      "Text search failed",
      ctxFromHono(c),
      err,
      { event: "search_text_failed", query }
    );
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

  // typed RPC `appListAllFiles` replaces a stale legacy
  // GET against `/files/list` (which never matched the POST-only
  // handler in the old `_legacyFetch` router and silently 404'd).
  const allFiles = await userStub(c.env, userId).appListAllFiles(userId);
  const files = allFiles
    .filter((f) => f.status === "complete")
    .map((f) => ({
      file_id: f.fileId,
      file_name: f.fileName,
      file_size: f.fileSize,
      mime_type: f.mimeType,
      status: f.status,
    }));

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
        logError(
          "CLIP indexing failed",
          ctxFromHono(c),
          err,
          {
            event: "search_clip_index_failed",
            fileName: file.file_name,
            fileId: file.file_id,
          }
        );
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
 * Fetch full file bytes via canonical `vfs.readFile(path)`. Used for
 * CLIP image indexing. Returns null on failure.
 */
async function fetchFileBytes(
  env: Env,
  userId: string,
  fileId: string
): Promise<Uint8Array | null> {
  try {
    const resolved = await userStub(env, userId).appGetFilePath(fileId);
    if (!resolved) return null;
    const vfs = createVFS(env, { tenant: userId });
    return await vfs.readFile(resolved.path);
  } catch (err) {
    // No Hono context here \u2014 this helper is called from indexFile
    // which itself is invoked from a route. We log without
    // requestId; callers concerned about correlation should log
    // around the helper.
    logError(
      "fetchFileBytes failed",
      { tenantId: `default::${userId}` },
      err,
      { event: "search_fetch_bytes_failed", fileId }
    );
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
    logError(
      "text indexing failed",
      { tenantId: `default::${userId}` },
      err,
      { event: "search_text_index_failed", fileId, fileName }
    );
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
      logError(
        "CLIP indexing failed",
        { tenantId: `default::${userId}` },
        err,
        { event: "search_clip_index_failed", fileId, fileName }
      );
    }
  }

  // Phase 23 Blindspot fix: stamp indexed_at so the reconciler
  // doesn't re-queue this file. We mark on a best-effort basis: if
  // the text-space upsert succeeded the file is searchable, even
  // when CLIP failed. The reconciler errs on the side of NOT
  // re-running for already-marked files (it filters
  // `indexed_at IS NULL`), so partial-CLIP-failure won't be retried.
  // That's acceptable — CLIP recovery is a separate (admin) flow.
  try {
    await userStub(env, userId).appMarkFileIndexed(fileId);
  } catch (err) {
    logError(
      "appMarkFileIndexed failed",
      { tenantId: `default::${userId}` },
      err,
      { event: "search_mark_indexed_failed", fileId }
    );
  }
}

/**
 * Search-index reconciler (Phase 23 Blindspot fix).
 *
 * Sweeps the per-tenant `indexed_at IS NULL` set and re-fires
 * `indexFile` for each. Bounded `limit` (default 25) keeps a single
 * sweep cheap; the alarm cadence determines steady-state catch-up.
 *
 * Idempotent: re-indexing an already-indexed file is just a vector
 * upsert under the same id (overwrites with identical embeddings).
 *
 * Returns the count of files re-fired so the caller can decide
 * whether to schedule a follow-up sweep sooner.
 */
export async function reconcileUnindexedFiles(
  env: Env,
  userId: string,
  limit: number = 25
): Promise<{ reconciled: number }> {
  const stub = userStub(env, userId);
  const rows = await stub.appListUnindexedFiles(userId, limit);
  for (const row of rows) {
    // Run sequentially — concurrent indexFile bursts would compete
    // for the same Workers AI / vector binding budget. Reconciler
    // pace is intentionally slow.
    try {
      await indexFile(
        env,
        userId,
        row.file_id,
        row.file_name,
        row.mime_type,
        row.file_size
      );
    } catch (err) {
      // P1-8 — bump the per-file attempt counter. After 5 failures
      // `appListUnindexedFiles` excludes this row from future
      // reconciler ticks. `appBumpIndexAttempts` returns
      // `capJustHit: true` exactly once on the transition; we log
      // a single `console.error` then so operators see poison
      // files in Logpush without log-spam from the per-tick
      // `console.warn` below.
      try {
        const { capJustHit, attempts } = await stub.appBumpIndexAttempts(
          row.file_id
        );
        if (capJustHit) {
          logError(
            "reconcileUnindexedFiles: file hit attempt cap",
            { tenantId: `default::${userId}` },
            err,
            {
              event: "search_reconcile_attempts_capped",
              fileId: row.file_id,
              attempts,
              cap: 5,
            }
          );
        } else {
          logWarn(
            "reconcileUnindexedFiles: file failed",
            { tenantId: `default::${userId}` },
            {
              event: "search_reconcile_attempt_failed",
              fileId: row.file_id,
              attempts,
              cap: 5,
              errMsg: err instanceof Error ? err.message : String(err),
            }
          );
        }
      } catch (bumpErr) {
        logWarn(
          "reconcileUnindexedFiles: bumpAttempts threw",
          { tenantId: `default::${userId}` },
          {
            event: "search_reconcile_bump_failed",
            fileId: row.file_id,
            errMsg: err instanceof Error ? err.message : String(err),
            bumpErrMsg:
              bumpErr instanceof Error ? bumpErr.message : String(bumpErr),
          }
        );
      }
    }
  }
  return { reconciled: rows.length };
}

export default search;
