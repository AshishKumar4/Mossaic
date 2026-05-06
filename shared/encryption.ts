/**
 * opt-in end-to-end encryption primitives — barrel.
 *
 * Pure WebCrypto. NO userspace crypto libraries (no crypto-js, tweetnacl,
 * libsodium, node-forge, node:crypto). Every primitive is a thin wrapper
 * around `crypto.subtle.*` and `crypto.getRandomValues`.
 *
 * Tree-shakeable: no top-level side effects, no imports beyond types.
 *
 * The implementation is split across 5 cohesive modules under
 * `./encryption/`:
 *  - `internal.ts`   — byte helpers (asView, concat, BE int reads/writes,
 *                      bytesEqual). Module-internal; not re-exported.
 *  - `iv.ts`         — `sha256` + `convergentIv` + `randomIv`.
 *  - `keys.ts`       — `deriveMasterFromPassword`,
 *                      `deriveChunkKeyConvergent`,
 *                      `generateRandomChunkKey`,
 *                      `wrapKeyAesKw` / `unwrapKeyAesKw`.
 *  - `envelope.ts`   — `packEnvelope` / `unpackEnvelope` /
 *                      `envelopeHeaderHash` (the on-the-wire layout).
 *  - `chunk.ts`      — `encryptChunk` / `decryptChunk` (one-shot API).
 *
 * See `local/phase-15-plan.md` §3 (envelope layout) and §8 (Lean obligations).
 */

export { sha256, convergentIv, randomIv } from "./encryption/iv";
export {
  deriveMasterFromPassword,
  deriveChunkKeyConvergent,
  generateRandomChunkKey,
  wrapKeyAesKw,
  unwrapKeyAesKw,
} from "./encryption/keys";
export {
  packEnvelope,
  unpackEnvelope,
  envelopeHeaderHash,
} from "./encryption/envelope";
export {
  encryptChunk,
  decryptChunk,
  type EncryptChunkOptions,
  type DecryptChunkOptions,
} from "./encryption/chunk";

// Hex helpers live in `./crypto` (single source of truth).
// Re-exported here so consumers of `@mossaic/sdk/encryption` can keep
// `import { bytesToHex, hexToBytes } from "@mossaic/sdk/encryption"`.
export { bytesToHex, hexToBytes } from "./crypto";

// ─── Public re-exports of constants for downstream consumers ──────────────

export {
  ENVELOPE_VERSION,
  IV_LENGTH,
  AUTH_TAG_LENGTH,
  SHA256_LENGTH,
  MASTER_KEY_LENGTH,
  WRAPPED_KEY_LENGTH,
  PBKDF2_DEFAULT_ITERATIONS,
  KEY_ID_MAX_BYTES,
  MODE_BYTE_CONVERGENT,
  MODE_BYTE_RANDOM,
} from "./encryption-types";
export type {
  AadTag,
  EncryptionMode,
  EncryptionConfig,
  EnvelopeParts,
  FileEncryption,
} from "./encryption-types";
