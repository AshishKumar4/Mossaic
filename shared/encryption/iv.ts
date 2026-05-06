/**
 * IV derivation + SHA-256 primitive.
 *
 * SHA-256 is co-located here because the convergent-mode IV is built
 * from a SHA-256 of (master, salt, plaintextHash, aadTag). Splitting it
 * into its own module would create a 4-line file with one consumer; the
 * pairing reflects the actual coupling.
 */

import {
  type AadTag,
  IV_LENGTH,
  MASTER_KEY_LENGTH,
  SHA256_LENGTH,
} from "../encryption-types";
import { asView, concat, TE } from "./internal";

/** Raw 32-byte SHA-256 digest. */
export async function sha256(data: Uint8Array): Promise<Uint8Array> {
  const digest = await crypto.subtle.digest("SHA-256", asView(data));
  return new Uint8Array(digest);
}

/**
 * Convergent IV: deterministic 12-byte nonce derived from
 * `(masterRaw || tenantSalt || plaintextHash || aadTag)`.
 *
 * Properties (proof in `lean/Mossaic/Vfs/Encryption.lean`):
 * - Distinct (plaintext, aadTag) → distinct IV.
 * - Distinct tenantSalt → distinct IV under same master/plaintext.
 * - IV uniqueness within (master, salt) holds as long as
 *   (plaintext, aadTag) pairs are unique. Same plaintext under same key
 *   + same aadTag intentionally yields the SAME IV — this is what
 *   enables dedup, and is safe under AES-GCM's IV-deterministic regime
 *   when the message is also identical.
 */
export async function convergentIv(
  masterRaw: Uint8Array,
  tenantSalt: Uint8Array,
  plaintextHash: Uint8Array,
  aadTag: AadTag
): Promise<Uint8Array> {
  if (masterRaw.byteLength !== MASTER_KEY_LENGTH) {
    throw new Error("convergentIv: masterRaw must be 32 bytes");
  }
  if (tenantSalt.byteLength !== SHA256_LENGTH) {
    throw new Error("convergentIv: tenantSalt must be 32 bytes");
  }
  if (plaintextHash.byteLength !== SHA256_LENGTH) {
    throw new Error("convergentIv: plaintextHash must be 32 bytes");
  }
  const tag = TE.encode(aadTag);
  const seed = concat(masterRaw, tenantSalt, plaintextHash, tag);
  const digest = await sha256(seed);
  return digest.subarray(0, IV_LENGTH);
}

/** Random 96-bit IV via `crypto.getRandomValues`. */
export function randomIv(): Uint8Array {
  const iv = new Uint8Array(IV_LENGTH);
  crypto.getRandomValues(iv);
  return iv;
}
