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
  return bytesToHex(new Uint8Array(digest));
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
  return bytesToHex(new Uint8Array(digest));
}

/** Hex-encode a byte array. */
export function bytesToHex(bytes: Uint8Array): string {
  let out = "";
  for (let i = 0; i < bytes.byteLength; i++) {
    out += (bytes[i] ?? 0).toString(16).padStart(2, "0");
  }
  return out;
}

/** Hex-decode (lowercase or uppercase). */
export function hexToBytes(hex: string): Uint8Array {
  if (hex.length % 2 !== 0) throw new Error("hexToBytes: odd-length hex");
  const out = new Uint8Array(hex.length / 2);
  for (let i = 0; i < out.byteLength; i++) {
    const v = parseInt(hex.substr(i * 2, 2), 16);
    if (Number.isNaN(v)) throw new Error("hexToBytes: invalid hex");
    out[i] = v;
  }
  return out;
}

/**
 * Generate a random hex string of the given byte length.
 */
export function randomHex(byteLength: number): string {
  const bytes = new Uint8Array(byteLength);
  crypto.getRandomValues(bytes);
  return bytesToHex(bytes);
}
