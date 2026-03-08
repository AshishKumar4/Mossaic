import type { EmbeddingProvider } from "@shared/embedding-types";

// ── Simple Embedding (TF-IDF-like bag-of-words) ──
// Always available — no external dependencies. Not as good as real embeddings
// but makes the feature work out of the box.

export class SimpleEmbedding implements EmbeddingProvider {
  name = "simple";
  dimensions = 256;
  maxBatchSize = 100;

  // Fixed vocabulary of common words for consistent dimensionality
  private vocabulary: string[] = [];
  private vocabIndex = new Map<string, number>();

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

// ── Cloudflare Workers AI Embedding ──
// Uses @cf/baai/bge-base-en-v1.5 via env.AI.run()

export class CloudflareAIEmbedding implements EmbeddingProvider {
  name = "cloudflare-ai";
  dimensions = 768;
  maxBatchSize = 100;

  private ai: { run: (model: string, input: Record<string, unknown>) => Promise<unknown> } | null;

  constructor(env: Record<string, unknown>) {
    this.ai = (env.AI as typeof this.ai) ?? null;
  }

  async embed(texts: string[]): Promise<number[][]> {
    if (!this.ai) throw new Error("Workers AI binding not available");

    const result = (await this.ai.run("@cf/baai/bge-base-en-v1.5", {
      text: texts,
    })) as { data: number[][] };

    return result.data;
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
  env: Record<string, unknown>
): Map<string, EmbeddingProvider> {
  const providers = new Map<string, EmbeddingProvider>();

  providers.set("simple", new SimpleEmbedding());
  providers.set("cloudflare-ai", new CloudflareAIEmbedding(env));
  providers.set("ollama", new OllamaEmbedding());

  return providers;
}
