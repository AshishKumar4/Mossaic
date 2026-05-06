import type { EmbeddingProvider, ImageEmbeddingProvider, VectorSpace } from "@shared/embedding-types";
import type { Env } from "@shared/types";

// ── Simple Embedding (TF-IDF-like bag-of-words) ──
// Always available — no external dependencies. Not as good as real embeddings
// but makes the feature work out of the box. Used as fallback for local dev.

export class SimpleEmbedding implements EmbeddingProvider {
  name = "simple";
  dimensions = 256;
  maxBatchSize = 100;
  space: VectorSpace = "text";

  constructor() {
    // Build a deterministic vocabulary from common file-related terms
    // We use a hash-based approach so any word maps to a fixed dimension
  }

  async embed(texts: string[]): Promise<number[][]> {
    return texts.map((text) => this.embedSingle(text));
  }

  async isAvailable(): Promise<boolean> {
    return true;
  }

  private embedSingle(text: string): number[] {
    const vector = new Float64Array(this.dimensions);
    const tokens = this.tokenize(text);

    if (tokens.length === 0) return Array.from(vector);

    // Hash each token to a dimension and accumulate TF weights
    const tokenCounts = new Map<string, number>();
    for (const token of tokens) {
      tokenCounts.set(token, (tokenCounts.get(token) || 0) + 1);
    }

    for (const [token, count] of tokenCounts) {
      // Use multiple hash positions per token for better distribution
      const hash1 = this.hashString(token);
      const hash2 = this.hashString(token + "_2");
      const hash3 = this.hashString(token + "_3");

      const idx1 = Math.abs(hash1) % this.dimensions;
      const idx2 = Math.abs(hash2) % this.dimensions;
      const idx3 = Math.abs(hash3) % this.dimensions;

      // TF weight: log(1 + count)
      const weight = Math.log(1 + count);

      // Sign from hash to allow cancellation (like random projection)
      vector[idx1] += (hash1 > 0 ? 1 : -1) * weight;
      vector[idx2] += (hash2 > 0 ? 1 : -1) * weight;
      vector[idx3] += (hash3 > 0 ? 1 : -1) * weight;
    }

    // L2 normalize
    let norm = 0;
    for (let i = 0; i < this.dimensions; i++) {
      norm += vector[i] * vector[i];
    }
    norm = Math.sqrt(norm);

    if (norm > 0) {
      for (let i = 0; i < this.dimensions; i++) {
        vector[i] /= norm;
      }
    }

    return Array.from(vector);
  }

  private tokenize(text: string): string[] {
    return text
      .toLowerCase()
      .replace(/[^a-z0-9\s\-_.]/g, " ")
      .split(/\s+/)
      .filter((t) => t.length > 1 && t.length < 40)
      .flatMap((t) => {
        // Also generate character n-grams for partial matching
        const tokens = [t];
        // Split on common delimiters (dots, dashes, underscores)
        const parts = t.split(/[-_.]/);
        if (parts.length > 1) {
          tokens.push(...parts.filter((p) => p.length > 1));
        }
        return tokens;
      });
  }

  private hashString(str: string): number {
    // FNV-1a hash
    let hash = 0x811c9dc5;
    for (let i = 0; i < str.length; i++) {
      hash ^= str.charCodeAt(i);
      hash = Math.imul(hash, 0x01000193);
    }
    return hash;
  }
}

// ── CLIP Embedding Provider ──
// Uses @cf/openai/clip-vit-base-patch32 via Workers AI.
// Text and image inputs map to the same 512-dim vector space,
// enabling text-to-image search via cosine similarity.

export class CLIPEmbedding implements ImageEmbeddingProvider {
  name = "clip";
  dimensions = 512;
  maxBatchSize = 20;
  space: VectorSpace = "clip";

  private ai: Ai | null;

  constructor(env: Env) {
    this.ai = env.AI ?? null;
  }

  /** Embed text queries into CLIP space (for searching against image vectors) */
  async embed(texts: string[]): Promise<number[][]> {
    if (!this.ai) throw new Error("Workers AI binding not available");

    // CLIP model isn't in the standard type definitions — use untyped call
    const result = await (this.ai as unknown as {
      run(model: string, input: Record<string, unknown>): Promise<{ data: number[][] }>;
    }).run("@cf/openai/clip-vit-base-patch32", { text: texts });

    return result.data;
  }

  /** Embed a raw image into CLIP space (for indexing) */
  async embedImage(imageBytes: Uint8Array): Promise<number[]> {
    if (!this.ai) throw new Error("Workers AI binding not available");

    // CLIP model expects image as number[] of raw bytes
    const result = await (this.ai as unknown as {
      run(model: string, input: Record<string, unknown>): Promise<{ data: number[][] }>;
    }).run("@cf/openai/clip-vit-base-patch32", { image: [...imageBytes] });

    return result.data[0];
  }

  async isAvailable(): Promise<boolean> {
    return this.ai !== null;
  }
}

// ── BGE Text Embedding Provider ──
// Uses @cf/baai/bge-base-en-v1.5 via Workers AI — 768-dim vectors.
// Best for document filenames, paths, and extracted text content.

export class BGETextEmbedding implements EmbeddingProvider {
  name = "bge-text";
  dimensions = 768;
  maxBatchSize = 100;
  space: VectorSpace = "text";

  private ai: Ai | null;

  constructor(env: Env) {
    this.ai = env.AI ?? null;
  }

  async embed(texts: string[]): Promise<number[][]> {
    if (!this.ai) throw new Error("Workers AI binding not available");

    const result = await this.ai.run("@cf/baai/bge-base-en-v1.5", {
      text: texts,
    });

    return (result as { data: number[][] }).data;
  }

  async isAvailable(): Promise<boolean> {
    return this.ai !== null;
  }
}

// ── Legacy Cloudflare AI Embedding (kept for backward compat) ──
// Uses @cf/baai/bge-base-en-v1.5 via env.AI.run()

export class CloudflareAIEmbedding implements EmbeddingProvider {
  name = "cloudflare-ai";
  dimensions = 768;
  maxBatchSize = 100;
  space: VectorSpace = "text";

  private ai: Ai | null;

  constructor(env: Env) {
    this.ai = env.AI ?? null;
  }

  async embed(texts: string[]): Promise<number[][]> {
    if (!this.ai) throw new Error("Workers AI binding not available");

    const result = await this.ai.run("@cf/baai/bge-base-en-v1.5", {
      text: texts,
    });

    return (result as { data: number[][] }).data;
  }

  async isAvailable(): Promise<boolean> {
    return this.ai !== null;
  }
}

// ── Ollama Embedding ──
// Calls Ollama API at a configurable URL. Uses nomic-embed-text model.

export class OllamaEmbedding implements EmbeddingProvider {
  name = "ollama";
  dimensions = 768;
  maxBatchSize = 50;
  space: VectorSpace = "text";

  private baseUrl: string;
  private model: string;

  constructor(baseUrl = "http://localhost:11434", model = "nomic-embed-text") {
    this.baseUrl = baseUrl.replace(/\/$/, "");
    this.model = model;
  }

  async embed(texts: string[]): Promise<number[][]> {
    const results: number[][] = [];

    // Ollama doesn't natively support batch embedding, so we process sequentially
    for (const text of texts) {
      const res = await fetch(`${this.baseUrl}/api/embeddings`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ model: this.model, prompt: text }),
      });

      if (!res.ok) {
        throw new Error(`Ollama embedding failed: ${res.status} ${res.statusText}`);
      }

      const data = (await res.json()) as { embedding: number[] };
      results.push(data.embedding);
    }

    return results;
  }

  async isAvailable(): Promise<boolean> {
    try {
      const res = await fetch(`${this.baseUrl}/api/tags`, {
        signal: AbortSignal.timeout(2000),
      });
      return res.ok;
    } catch {
      return false;
    }
  }
}

// ── Provider Registry ──

export function createEmbeddingProviders(
  env: Env
): Map<string, EmbeddingProvider> {
  const providers = new Map<string, EmbeddingProvider>();

  providers.set("simple", new SimpleEmbedding());
  providers.set("clip", new CLIPEmbedding(env));
  providers.set("bge-text", new BGETextEmbedding(env));
  providers.set("cloudflare-ai", new CloudflareAIEmbedding(env));
  providers.set("ollama", new OllamaEmbedding());

  return providers;
}

/**
 * Get the best available text embedding provider.
 * Prefers BGE > CloudflareAI > Ollama > Simple (fallback).
 */
export async function getBestTextProvider(
  env: Env
): Promise<EmbeddingProvider> {
  const bge = new BGETextEmbedding(env);
  if (await bge.isAvailable()) return bge;

  const cfai = new CloudflareAIEmbedding(env);
  if (await cfai.isAvailable()) return cfai;

  // Ollama usually not available in production, but try
  const ollama = new OllamaEmbedding();
  if (await ollama.isAvailable()) return ollama;

  return new SimpleEmbedding();
}

/**
 * Get the CLIP provider if available, or null.
 */
export function getCLIPProvider(env: Env): CLIPEmbedding | null {
  const clip = new CLIPEmbedding(env);
  // Synchronous check — AI binding presence
  return env.AI ? clip : null;
}
