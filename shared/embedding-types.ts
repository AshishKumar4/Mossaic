// ── Vector Spaces ──

/** Identifies which vector space a vector belongs to */
export type VectorSpace = "clip" | "text";

// ── Embedding Provider Interface ──

export interface EmbeddingProvider {
  name: string;
  dimensions: number;
  maxBatchSize: number;
  space: VectorSpace;
  embed(texts: string[]): Promise<number[][]>;
  isAvailable(): Promise<boolean>;
}

/** Provider that can embed raw image bytes (e.g. CLIP) */
export interface ImageEmbeddingProvider extends EmbeddingProvider {
  embedImage(imageBytes: Uint8Array): Promise<number[]>;
}

export function isImageEmbeddingProvider(
  p: EmbeddingProvider
): p is ImageEmbeddingProvider {
  return "embedImage" in p && typeof (p as ImageEmbeddingProvider).embedImage === "function";
}

// ── Vector Store Interface ──

export interface VectorStore {
  name: string;
  upsert(
    vectors: { id: string; values: number[]; metadata?: Record<string, string> }[],
    space?: VectorSpace
  ): Promise<void>;
  query(
    vector: number[],
    topK?: number,
    filter?: Record<string, string>,
    space?: VectorSpace
  ): Promise<{ id: string; score: number; metadata?: Record<string, string> }[]>;
  delete(ids: string[], space?: VectorSpace): Promise<void>;
}

// ── Search Result ──

export type SearchResultType = "image" | "document" | "video" | "audio" | "archive" | "other";

export interface SearchResult {
  fileId: string;
  fileName: string;
  score: number;
  mimeType: string;
  resultType: SearchResultType;
  /** Which vector space produced this match */
  space: VectorSpace;
  /** Whether a thumbnail is available */
  hasThumbnail: boolean;
  /** File size in bytes (if known) */
  fileSize?: number;
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
  space?: VectorSpace;
}

// ── Helpers ──

const IMAGE_MIMES = new Set([
  "image/jpeg", "image/png", "image/gif", "image/webp", "image/svg+xml",
  "image/bmp", "image/tiff", "image/avif", "image/heic", "image/heif",
]);

/** CLIP can process these raster image types (not SVG) */
const CLIP_INDEXABLE_MIMES = new Set([
  "image/jpeg", "image/png", "image/gif", "image/webp",
  "image/bmp", "image/tiff", "image/avif",
]);

/** Max image size for CLIP indexing (10 MB) */
export const CLIP_MAX_IMAGE_SIZE = 10 * 1024 * 1024;

export function isImageMime(mime: string): boolean {
  return IMAGE_MIMES.has(mime) || mime.startsWith("image/");
}

export function isClipIndexable(mime: string, fileSize: number): boolean {
  return CLIP_INDEXABLE_MIMES.has(mime) && fileSize <= CLIP_MAX_IMAGE_SIZE;
}

export function classifyResultType(mimeType: string): SearchResultType {
  if (mimeType.startsWith("image/")) return "image";
  if (mimeType.startsWith("video/")) return "video";
  if (mimeType.startsWith("audio/")) return "audio";
  if (mimeType.includes("zip") || mimeType.includes("archive") || mimeType.includes("tar") || mimeType.includes("gzip"))
    return "archive";
  if (
    mimeType.includes("pdf") || mimeType.includes("text") ||
    mimeType.includes("document") || mimeType.includes("spreadsheet") ||
    mimeType.includes("presentation") || mimeType.includes("json") ||
    mimeType.includes("xml") || mimeType.includes("csv")
  )
    return "document";
  return "other";
}
