/**
 * Master + per-chunk key derivation, plus AES-KW wrap/unwrap.
 *
 * Three operations live together because they all flow from the master
 * key:
 *  - `deriveMasterFromPassword` turns a UTF-8 password + tenantSalt into
 *    32 raw bytes (PBKDF2-SHA256, 600k iterations by default).
 *  - `deriveChunkKeyConvergent` HKDFs (master, salt, plaintextHash,
 *    aadTag) → AES-GCM-256 CryptoKey for convergent-mode chunks.
 *  - `generateRandomChunkKey` + `wrapKeyAesKw` / `unwrapKeyAesKw` model
 *    the random-mode key lifecycle: a fresh per-chunk key is generated,
 *    wrapped under master via AES-KW (RFC 3394), and stored alongside
 *    the envelope.
 */

import {
  type AadTag,
  MASTER_KEY_LENGTH,
  PBKDF2_DEFAULT_ITERATIONS,
  SHA256_LENGTH,
  WRAPPED_KEY_LENGTH,
} from "../encryption-types";
import { asView, concat, TE } from "./internal";

/**
 * Derive a 32-byte raw master key from a password via PBKDF2-SHA256.
 *
 * @param password UTF-8 password. Caller is responsible for entropy.
 * @param tenantSalt 32-byte stable per-tenant salt (also used as encryption
 *   tenantSalt — these are the same value).
 * @param iterations OWASP 2024 minimum is 600_000; default in
 *   `PBKDF2_DEFAULT_ITERATIONS`.
 * @returns 32-byte raw key. Caller stores it in non-extractable CryptoKey
 *   or KMS; never persists the password.
 */
export async function deriveMasterFromPassword(
  password: string,
  tenantSalt: Uint8Array,
  iterations: number = PBKDF2_DEFAULT_ITERATIONS
): Promise<Uint8Array> {
  if (tenantSalt.byteLength !== SHA256_LENGTH) {
    throw new Error("deriveMasterFromPassword: tenantSalt must be 32 bytes");
  }
  if (iterations < 100_000) {
    throw new Error(
      "deriveMasterFromPassword: iterations < 100_000 is insecure"
    );
  }
  const pwBytes = TE.encode(password);
  const baseKey = await crypto.subtle.importKey(
    "raw",
    asView(pwBytes),
    "PBKDF2",
    false,
    ["deriveBits"]
  );
  const bits = await crypto.subtle.deriveBits(
    {
      name: "PBKDF2",
      salt: asView(tenantSalt),
      iterations,
      hash: "SHA-256",
    },
    baseKey,
    MASTER_KEY_LENGTH * 8
  );
  return new Uint8Array(bits);
}

/**
 * Derive a per-chunk AES-GCM-256 key for convergent mode via HKDF-SHA256.
 *
 * info = "mossaic/v15/chunk" || aadTag
 * salt = tenantSalt
 * ikm  = masterRaw || plaintextHash
 *
 * Determinism: same (master, salt, plaintextHash, aadTag) → same key.
 * This is what makes the convergent-mode envelope deterministic.
 */
export async function deriveChunkKeyConvergent(
  masterRaw: Uint8Array,
  tenantSalt: Uint8Array,
  plaintextHash: Uint8Array,
  aadTag: AadTag
): Promise<CryptoKey> {
  if (masterRaw.byteLength !== MASTER_KEY_LENGTH) {
    throw new Error("deriveChunkKeyConvergent: masterRaw must be 32 bytes");
  }
  if (plaintextHash.byteLength !== SHA256_LENGTH) {
    throw new Error("deriveChunkKeyConvergent: plaintextHash must be 32 bytes");
  }
  const ikm = concat(masterRaw, plaintextHash);
  const baseKey = await crypto.subtle.importKey(
    "raw",
    asView(ikm),
    "HKDF",
    false,
    ["deriveKey"]
  );
  const info = TE.encode("mossaic/v15/chunk/" + aadTag);
  return crypto.subtle.deriveKey(
    {
      name: "HKDF",
      hash: "SHA-256",
      salt: asView(tenantSalt),
      info: asView(info),
    },
    baseKey,
    { name: "AES-GCM", length: 256 },
    false,
    ["encrypt", "decrypt"]
  );
}

/** Generate a fresh random 32-byte AES-GCM CryptoKey for random-mode. */
export async function generateRandomChunkKey(): Promise<{
  key: CryptoKey;
  raw: Uint8Array;
}> {
  const raw = new Uint8Array(MASTER_KEY_LENGTH);
  crypto.getRandomValues(raw);
  const key = await crypto.subtle.importKey(
    "raw",
    asView(raw),
    { name: "AES-GCM", length: 256 },
    true, // extractable so wrapKey can serialize it
    ["encrypt", "decrypt"]
  );
  return { key, raw };
}

/**
 * Wrap a per-chunk AES-GCM key under the master key using AES-KW (RFC 3394).
 * Output is 40 bytes (32-byte key + 8-byte KW IV/checksum).
 */
export async function wrapKeyAesKw(
  rawChunkKey: Uint8Array,
  masterRaw: Uint8Array
): Promise<Uint8Array> {
  if (rawChunkKey.byteLength !== MASTER_KEY_LENGTH) {
    throw new Error("wrapKeyAesKw: rawChunkKey must be 32 bytes");
  }
  if (masterRaw.byteLength !== MASTER_KEY_LENGTH) {
    throw new Error("wrapKeyAesKw: masterRaw must be 32 bytes");
  }
  const wrappingKey = await crypto.subtle.importKey(
    "raw",
    asView(masterRaw),
    { name: "AES-KW", length: 256 },
    false,
    ["wrapKey"]
  );
  const chunkKey = await crypto.subtle.importKey(
    "raw",
    asView(rawChunkKey),
    { name: "AES-GCM", length: 256 },
    true,
    ["encrypt", "decrypt"]
  );
  const wrapped = await crypto.subtle.wrapKey(
    "raw",
    chunkKey,
    wrappingKey,
    "AES-KW"
  );
  return new Uint8Array(wrapped);
}

/** Inverse of {@link wrapKeyAesKw}. Returns a non-extractable AES-GCM key. */
export async function unwrapKeyAesKw(
  wrapped: Uint8Array,
  masterRaw: Uint8Array
): Promise<CryptoKey> {
  if (wrapped.byteLength !== WRAPPED_KEY_LENGTH) {
    throw new Error("unwrapKeyAesKw: wrapped must be 40 bytes");
  }
  if (masterRaw.byteLength !== MASTER_KEY_LENGTH) {
    throw new Error("unwrapKeyAesKw: masterRaw must be 32 bytes");
  }
  const wrappingKey = await crypto.subtle.importKey(
    "raw",
    asView(masterRaw),
    { name: "AES-KW", length: 256 },
    false,
    ["unwrapKey"]
  );
  return crypto.subtle.unwrapKey(
    "raw",
    asView(wrapped),
    wrappingKey,
    "AES-KW",
    { name: "AES-GCM", length: 256 },
    false,
    ["encrypt", "decrypt"]
  );
}
