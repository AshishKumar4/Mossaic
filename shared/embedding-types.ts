// ── Embedding Provider Interface ──

export interface EmbeddingProvider {
  name: string;
  dimensions: number;
  maxBatchSize: number;
  embed(texts: string[]): Promise<number[][]>;
  isAvailable(): Promise<boolean>;
}

// ── Vector Store Interface ──

export interface VectorStore {
  name: string;
  upsert(
    vectors: { id: string; values: number[]; metadata?: Record<string, string> }[]
  ): Promise<void>;
  query(
    vector: number[],
    topK?: number,
    filter?: Record<string, string>
  ): Promise<{ id: string; score: number; metadata?: Record<string, string> }[]>;
  delete(ids: string[]): Promise<void>;
}

// ── Search Result ──

export interface SearchResult {
  fileId: string;
  fileName: string;
  score: number;
  mimeType: string;
  highlight?: string;
}

// ── Provider Config ──

export interface SearchProviderConfig {
  embedding: string;
  vectorStore: string;
}

export interface ProviderStatus {
  name: string;
  type: "embedding" | "vectorStore";
  available: boolean;
  dimensions?: number;
}
