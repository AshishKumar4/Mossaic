import { CHUNK_SIZE } from "./constants";

export interface ChunkComputeResult {
  chunkSize: number;
  chunkCount: number;
  lastChunkSize: number;
}

/**
 * Compute chunk specification for a given file size.
 * Files <= 1 MB are a single chunk. All others use fixed 1 MB chunks.
 */
export function computeChunkSpec(fileSize: number): ChunkComputeResult {
  if (fileSize <= 0) {
    return { chunkSize: 0, chunkCount: 0, lastChunkSize: 0 };
  }

  if (fileSize <= CHUNK_SIZE) {
    return {
      chunkSize: fileSize,
      chunkCount: 1,
      lastChunkSize: fileSize,
    };
  }

  const chunkCount = Math.ceil(fileSize / CHUNK_SIZE);
  const lastChunkSize = fileSize - (chunkCount - 1) * CHUNK_SIZE;

  return {
    chunkSize: CHUNK_SIZE,
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
