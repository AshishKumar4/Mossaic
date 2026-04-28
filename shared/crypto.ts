import type { ChunkHash } from "./types";

/**
 * Compute SHA-256 hash of chunk data.
 * Returns hex-encoded string (64 chars).
 *
 * NOTE: under strict workers-types `crypto.subtle.digest` accepts
 * `BufferSource` parameterised on `ArrayBuffer`, not `ArrayBufferLike`
 * (which would admit `SharedArrayBuffer`). The `data.buffer as ArrayBuffer`
 * cast narrows the underlying buffer type. The byte payload is unchanged.
 */
export async function hashChunk(data: Uint8Array): Promise<ChunkHash> {
  const view = new Uint8Array(
    data.buffer as ArrayBuffer,
    data.byteOffset,
    data.byteLength
  );
  const digest = await crypto.subtle.digest("SHA-256", view);
  return bufferToHex(digest);
}

/**
 * Compute file-level hash from ordered chunk hashes.
 * File hash = SHA-256(concat(chunkHash[0], chunkHash[1], ...))
 */
export async function computeFileHash(
  chunkHashes: ChunkHash[]
): Promise<ChunkHash> {
  const concat = chunkHashes.join("");
  const data = new TextEncoder().encode(concat);
  const digest = await crypto.subtle.digest("SHA-256", data);
  return bufferToHex(digest);
}

/**
 * Convert an ArrayBuffer to hex string.
 */
export function bufferToHex(buffer: ArrayBuffer): string {
  return Array.from(new Uint8Array(buffer))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

/**
 * Generate a random hex string of the given byte length.
 */
export function randomHex(byteLength: number): string {
  const bytes = new Uint8Array(byteLength);
  crypto.getRandomValues(bytes);
  return bufferToHex(bytes.buffer);
}
