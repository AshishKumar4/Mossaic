import { MAX_BLOB_SIZE } from "./constants";

export interface ChunkComputeResult {
  chunkSize: number;
  chunkCount: number;
  lastChunkSize: number;
}

/**
 * Adaptive chunk size based on total file size.
 *
 * - ≤ 1 MB  → single chunk (returns fileSize)
 * - 1–64 MB → 1 MB chunks
 * - 64–512 MB → 1.5 MB chunks
 * - 512 MB+ → 2 MB chunks (DO SQLite blob limit)
 */
export function getAdaptiveChunkSize(fileSize: number): number {
  const _1MB = 1_048_576;
  const _64MB = 64 * _1MB;
  const _512MB = 512 * _1MB;

  if (fileSize <= _1MB) return fileSize;
  if (fileSize <= _64MB) return _1MB;
  if (fileSize <= _512MB) return Math.floor(1.5 * _1MB); // 1,572,864
  return MAX_BLOB_SIZE; // 2 MB — DO blob limit
}

/**
 * Compute chunk specification for a given file size.
 * Uses adaptive chunk sizing: small files are a single chunk,
 * larger files use progressively bigger chunks up to the DO blob limit.
 */
export function computeChunkSpec(fileSize: number): ChunkComputeResult {
  if (fileSize <= 0) {
    return { chunkSize: 0, chunkCount: 0, lastChunkSize: 0 };
  }

  const chunkSize = getAdaptiveChunkSize(fileSize);

  if (fileSize <= chunkSize) {
    return {
      chunkSize: fileSize,
      chunkCount: 1,
      lastChunkSize: fileSize,
    };
  }

  const chunkCount = Math.ceil(fileSize / chunkSize);
  const lastChunkSize = fileSize - (chunkCount - 1) * chunkSize;

  return {
    chunkSize,
    chunkCount,
    lastChunkSize,
  };
}

/**
 * Get the byte offset for a given chunk index.
 */
export function chunkOffset(index: number, chunkSize: number): number {
  return index * chunkSize;
}

/**
 * Get the actual size of a specific chunk (last chunk may be smaller).
 */
export function chunkSizeAt(
  index: number,
  chunkCount: number,
  chunkSize: number,
  lastChunkSize: number
): number {
  return index === chunkCount - 1 ? lastChunkSize : chunkSize;
}
