/**
 * opt-in end-to-end encryption types.
 *
 * Pure type definitions. No runtime dependencies. Tree-shakeable.
 *
 * The Mossaic server NEVER decrypts user data. All encryption /
 * decryption happens in the SDK using WebCrypto only. Master keys
 * never leave the consumer; loss of master key = permanent data loss.
 *
 * See `local/phase-15-plan.md` §3 for the canonical envelope byte layout.
 */

/**
 * Encryption mode for a file or chunk.
 *
 * - `convergent`: deterministic IV/key derivation from plaintext hash.
 *   Identical plaintexts under the same (master, salt) produce identical
 *   envelopes — preserves server-side dedup. Within-tenant equality oracle
 *   is the documented cost (see Lean Theorem 2 in
 *   `lean/Mossaic/Vfs/Encryption.lean`).
 *
 * - `random`: 96-bit random IV; per-chunk DEK wrapped via AES-KW under
 *   the master key. No determinism, no dedup, no equality oracle.
 *   IND-CPA secure (Lean Theorem 1).
 */
export type EncryptionMode = "convergent" | "random";

/**
 * AAD discriminator. Embedded in every envelope. Prevents cross-purpose
 * envelope replay (a chunk envelope can never decrypt as a Yjs frame
 * even under identical key material).
 *
 * - `ck` — file-content chunk
 * - `yj` — Yjs sync_step_2 / update payload
 * - `aw` — Yjs awareness payload
 */
export type AadTag = "ck" | "yj" | "aw";

/**
 * Per-VFS-instance encryption configuration.
 *
 * The masterKey + tenantSalt pair is the consumer's responsibility.
 * Mossaic recommends:
 * - Browser: WebCrypto + IndexedDB non-extractable CryptoKey.
 * - Node/Worker: KMS unwrap on cold start.
 * - CLI: PBKDF2 over an interactive password (`deriveMasterFromPassword`).
 *
 * `keyId` is an opaque label for human-friendly key-rotation tracking.
 * Servers store it but never read its semantics.
 */
export interface EncryptionConfig {
  /** 32-byte raw AES-GCM-256 key material. */
  masterKey: Uint8Array;
  /** 32-byte stable per-tenant salt. */
  tenantSalt: Uint8Array;
  /** Default mode for writes that don't specify per-call. */
  mode?: EncryptionMode;
  /** Free-form ≤128B label, embedded in every envelope. */
  keyId?: string;
}

/**
 * Per-file encryption metadata as surfaced via VFSStat / VersionRow.
 * Server stores `encryption_mode` + `encryption_key_id` columns;
 * the SDK exposes them as a structured object.
 */
export interface FileEncryption {
  mode: EncryptionMode;
  keyId?: string;
}

/**
 * Decoded envelope structure. Result of `unpackEnvelope`.
 *
 * Byte layout (see plan §3):
 * ```
 *   header: version(1) | mode(1) | keyIdLen(2) | keyId(N0)
 *         | iv(12) | aadLen(2) | aadTag(N1)
 *         | extLen(2) | ext(N2)
 *   tail (convergent): plaintextHash(32) | ctLen(4) | ct(N3)
 *   tail (random):     wrappedLen(2) | wrappedKey(N4) | ctLen(4) | ct(N3)
 * ```
 */
export interface EnvelopeParts {
  version: number;
  mode: EncryptionMode;
  keyId: string;
  iv: Uint8Array; // 12 bytes
  aadTag: AadTag;
  ext: Uint8Array;
  /** Set only when mode === "convergent". */
  plaintextHash?: Uint8Array;
  /** Set only when mode === "random". 40 bytes (AES-KW wrapped 32B key). */
  wrappedKey?: Uint8Array;
  /** Ciphertext including trailing 16-byte AES-GCM auth tag. */
  ct: Uint8Array;
}

/** Envelope format version. v1 corresponds to. */
export const ENVELOPE_VERSION = 1;

/** Plaintext sentinel — stored as version byte 0 for non-encrypted blobs. */
export const ENVELOPE_PLAINTEXT_SENTINEL = 0;

/** Mode byte: convergent. */
export const MODE_BYTE_CONVERGENT = 1;
/** Mode byte: random. */
export const MODE_BYTE_RANDOM = 2;

/** AES-GCM nonce size. */
export const IV_LENGTH = 12;
/** AES-GCM auth tag size (suffix of ct). */
export const AUTH_TAG_LENGTH = 16;
/** SHA-256 digest length, used for plaintextHash + tenantSalt. */
export const SHA256_LENGTH = 32;
/** Master-key raw size (AES-256). */
export const MASTER_KEY_LENGTH = 32;
/** AES-KW wraps a 32-byte key into 40 bytes. */
export const WRAPPED_KEY_LENGTH = 40;

/**
 * OWASP-recommended PBKDF2 iteration count for SHA-256 (2024+).
 * Kept as a default; callers may override via `iterations`.
 */
export const PBKDF2_DEFAULT_ITERATIONS = 600_000;

/** Maximum keyId length (bytes, UTF-8 encoded). */
export const KEY_ID_MAX_BYTES = 128;
