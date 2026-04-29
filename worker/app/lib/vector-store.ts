import type { VectorStore } from "@shared/embedding-types";
import type { VectorSpace } from "@shared/embedding-types";
import type { EnvApp as Env } from "@shared/types";

// ── Durable Object Vector Store ──
// Stores vectors in the SearchDO Durable Object with SQLite.
// Uses brute-force cosine similarity — fine for <100K vectors.
// Supports multiple vector spaces (clip, text) via the space parameter.

export class DOVectorStore implements VectorStore {
  name = "durable-object";
  private stub: DurableObjectStub;

  constructor(env: Env, userId: string) {
    const doId = env.SEARCH_DO.idFromName(`search:${userId}`);
    this.stub = env.SEARCH_DO.get(doId);
  }

  async upsert(
    vectors: { id: string; values: number[]; metadata?: Record<string, string> }[],
    space: VectorSpace = "text"
  ): Promise<void> {
    const res = await this.stub.fetch(
      new Request("http://internal/vectors/upsert", {
        method: "POST",
        body: JSON.stringify({ vectors, space }),
      })
    );
    if (!res.ok) {
      const err = (await res.json()) as { error: string };
      throw new Error(`DOVectorStore upsert failed: ${err.error}`);
    }
  }

  async query(
    vector: number[],
    topK = 10,
    filter?: Record<string, string>,
    space: VectorSpace = "text"
  ): Promise<{ id: string; score: number; metadata?: Record<string, string> }[]> {
    const res = await this.stub.fetch(
      new Request("http://internal/vectors/query", {
        method: "POST",
        body: JSON.stringify({ vector, topK, filter, space }),
      })
    );
    if (!res.ok) {
      const err = (await res.json()) as { error: string };
      throw new Error(`DOVectorStore query failed: ${err.error}`);
    }
    const data = (await res.json()) as {
      results: { id: string; score: number; metadata?: Record<string, string> }[];
    };
    return data.results;
  }

  async delete(ids: string[], space: VectorSpace = "text"): Promise<void> {
    const res = await this.stub.fetch(
      new Request("http://internal/vectors/delete", {
        method: "POST",
        body: JSON.stringify({ ids, space }),
      })
    );
    if (!res.ok) {
      const err = (await res.json()) as { error: string };
      throw new Error(`DOVectorStore delete failed: ${err.error}`);
    }
  }
}

// ── Cloudflare Vectorize Store ──
// Uses Cloudflare Vectorize binding. Requires VECTORIZE_INDEX binding in wrangler config.
// Note: Vectorize doesn't natively support spaces — we use metadata filtering.

export class CloudflareVectorize implements VectorStore {
  name = "vectorize";
  private index: VectorizeIndex | null;

  constructor(env: Record<string, unknown>) {
    this.index = (env.VECTORIZE_INDEX as VectorizeIndex) ?? null;
  }

  async upsert(
    vectors: { id: string; values: number[]; metadata?: Record<string, string> }[],
    space: VectorSpace = "text"
  ): Promise<void> {
    if (!this.index) throw new Error("Vectorize binding not available");
    await this.index.upsert(
      vectors.map((v) => ({
        id: v.id,
        values: v.values,
        metadata: { ...v.metadata, _space: space },
      }))
    );
  }

  async query(
    vector: number[],
    topK = 10,
    filter?: Record<string, string>,
    space: VectorSpace = "text"
  ): Promise<{ id: string; score: number; metadata?: Record<string, string> }[]> {
    if (!this.index) throw new Error("Vectorize binding not available");
    const result = await this.index.query(vector, {
      topK,
      filter: { ...filter, _space: space },
      returnMetadata: "all",
    });
    return result.matches.map((m) => ({
      id: m.id,
      score: m.score,
      metadata: m.metadata as Record<string, string> | undefined,
    }));
  }

  async delete(ids: string[], _space?: VectorSpace): Promise<void> {
    if (!this.index) throw new Error("Vectorize binding not available");
    await this.index.deleteByIds(ids);
  }

  async isAvailable(): Promise<boolean> {
    return this.index !== null;
  }
}

// ── Store Registry ──

export function createVectorStores(
  env: Env,
  userId: string
): Map<string, VectorStore> {
  const stores = new Map<string, VectorStore>();

  stores.set("durable-object", new DOVectorStore(env, userId));

  const vectorize = new CloudflareVectorize(env as unknown as Record<string, unknown>);
  stores.set("vectorize", vectorize);

  return stores;
}
