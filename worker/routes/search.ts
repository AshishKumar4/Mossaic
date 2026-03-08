import { Hono } from "hono";
import type { Env } from "@shared/types";
import type { SearchResult, ProviderStatus } from "@shared/embedding-types";
import { authMiddleware } from "../lib/auth";
import { userDOName } from "../lib/utils";
import { createEmbeddingProviders } from "../lib/embeddings";
import { DOVectorStore } from "../lib/vector-store";

const search = new Hono<{
  Bindings: Env;
  Variables: { userId: string; email: string };
}>();

search.use("*", authMiddleware());

/**
 * POST /api/search
 * Semantic search over user's files.
 * Body: { query: string, topK?: number, provider?: string }
 */
search.post("/", async (c) => {
  const userId = c.get("userId");
  const { query, topK = 10, provider } = await c.req.json<{
    query: string;
    topK?: number;
    provider?: string;
  }>();

  if (!query || query.trim().length === 0) {
    return c.json({ error: "query is required" }, 400);
  }

  // Determine active embedding provider
  const providers = createEmbeddingProviders(c.env as unknown as Record<string, unknown>);
  const activeProviderName = provider || await getActiveProvider(c.env, userId, "embedding") || "simple";
  const embeddingProvider = providers.get(activeProviderName);

  if (!embeddingProvider) {
    return c.json({ error: `Unknown embedding provider: ${activeProviderName}` }, 400);
  }

  const available = await embeddingProvider.isAvailable();
  if (!available) {
    return c.json({ error: `Embedding provider '${activeProviderName}' is not available` }, 503);
  }

  // Generate query embedding
  const [queryVector] = await embeddingProvider.embed([query]);

  // Query vector store
  const vectorStore = new DOVectorStore(c.env, userId);
  const matches = await vectorStore.query(queryVector, topK);

  // Build search results with file metadata
  const results: SearchResult[] = matches
    .filter((m) => m.score > 0)
    .map((match) => ({
      fileId: match.metadata?.fileId || match.id,
      fileName: match.metadata?.fileName || "Unknown",
      score: Math.round(match.score * 1000) / 1000,
      mimeType: match.metadata?.mimeType || "application/octet-stream",
      highlight: match.metadata?.text,
    }));

  return c.json({
    results,
    provider: activeProviderName,
    query,
  });
});

/**
 * GET /api/search/providers
 * List available providers and their status.
 */
search.get("/providers", async (c) => {
  const userId = c.get("userId");
  const embeddingProviders = createEmbeddingProviders(c.env as unknown as Record<string, unknown>);

  const statuses: ProviderStatus[] = [];

  for (const [name, provider] of embeddingProviders) {
    const available = await provider.isAvailable();
    statuses.push({
      name,
      type: "embedding",
      available,
      dimensions: provider.dimensions,
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

  // Get index stats
  const vectorStore = new DOVectorStore(c.env, userId);
  let indexedCount = 0;
  try {
    const doId = c.env.SEARCH_DO.idFromName(`search:${userId}`);
    const stub = c.env.SEARCH_DO.get(doId);
    const statsRes = await stub.fetch(new Request("http://internal/stats"));
    if (statsRes.ok) {
      const stats = (await statsRes.json()) as { vectorCount: number };
      indexedCount = stats.vectorCount;
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
 * Re-embed all user files. Admin action.
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
    return c.json({ ok: true, indexed: 0 });
  }

  // Determine active embedding provider
  const providers = createEmbeddingProviders(c.env as unknown as Record<string, unknown>);
  const activeProviderName = await getActiveProvider(c.env, userId, "embedding") || "simple";
  const embeddingProvider = providers.get(activeProviderName);

  if (!embeddingProvider) {
    return c.json({ error: `Embedding provider '${activeProviderName}' not found` }, 400);
  }

  // Generate embeddings in batches
  const vectorStore = new DOVectorStore(c.env, userId);
  let indexed = 0;

  for (let i = 0; i < files.length; i += embeddingProvider.maxBatchSize) {
    const batch = files.slice(i, i + embeddingProvider.maxBatchSize);

    const texts = batch.map((f) =>
      buildEmbeddingText(f.file_name, f.mime_type, f.file_size)
    );

    const embeddings = await embeddingProvider.embed(texts);

    const vectors = batch.map((f, j) => ({
      id: f.file_id,
      values: embeddings[j],
      metadata: {
        fileId: f.file_id,
        fileName: f.file_name,
        mimeType: f.mime_type,
        fileSize: f.file_size.toString(),
        text: texts[j],
      },
    }));

    await vectorStore.upsert(vectors);
    indexed += vectors.length;
  }

  return c.json({ ok: true, indexed, provider: activeProviderName });
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
 * Embed and index a single file. Called from upload complete hook.
 */
export async function indexFile(
  env: Env,
  userId: string,
  fileId: string,
  fileName: string,
  mimeType: string,
  fileSize: number
): Promise<void> {
  try {
    const providers = createEmbeddingProviders(env as unknown as Record<string, unknown>);
    const activeProviderName = await getActiveProvider(env, userId, "embedding") || "simple";
    const embeddingProvider = providers.get(activeProviderName);

    if (!embeddingProvider) return;

    const available = await embeddingProvider.isAvailable();
    if (!available) return;

    const text = buildEmbeddingText(fileName, mimeType, fileSize);
    const [embedding] = await embeddingProvider.embed([text]);

    const vectorStore = new DOVectorStore(env, userId);
    await vectorStore.upsert([
      {
        id: fileId,
        values: embedding,
        metadata: {
          fileId,
          fileName,
          mimeType,
          fileSize: fileSize.toString(),
          text,
        },
      },
    ]);
  } catch (err) {
    // Indexing failure should not block uploads
    console.error("Search indexing failed:", err);
  }
}

export default search;
