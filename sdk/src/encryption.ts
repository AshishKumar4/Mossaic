/**
 * opt-in end-to-end encryption (SDK-side).
 *
 * This module is loaded LAZILY via `await import("./encryption")` from
 * vfs.ts. Consumers who never set `encryption` on createVFS pay zero
 * bundle cost — the encryption code lives in its own tsup chunk.
 *
 * All crypto is delegated to `shared/encryption.ts` (pure WebCrypto).
 * No userspace crypto libraries are used or imported.
 *
 * The Mossaic server NEVER decrypts user data. All `encryptChunk` /
 * `decryptChunk` calls happen client-side. Master keys never leave the
 * VFS instance; loss of master key = permanent data loss.
 *
 * See `local/phase-15-plan.md` §4 for the API contract.
 */

import {
  decryptChunk as decryptChunkPure,
  encryptChunk as encryptChunkPure,
  deriveMasterFromPassword as deriveMasterFromPasswordPure,
  bytesToHex,
  hexToBytes,
} from "@shared/encryption";

// re-export the pure WebCrypto primitives so consumers
// (notably `@mossaic/cli`) can build envelopes directly without
// going through the VFS surface. These are the same functions used
// by `encryptPayload` / `decryptPayload` below.
export {
  encryptChunkPure as encryptChunk,
  decryptChunkPure as decryptChunk,
  bytesToHex,
  hexToBytes,
};
import {
  type AadTag,
  type EncryptionConfig,
  type EncryptionMode,
  type FileEncryption,
  PBKDF2_DEFAULT_ITERATIONS,
} from "@shared/encryption-types";
import { EACCES, EINVAL, VFSFsError } from "./errors";

/**
 * Re-exported types for downstream consumers. Kept on the lazy chunk
 * so `import type { EncryptionConfig } from "@mossaic/sdk/encryption"`
 * doesn't pull the runtime helpers.
 */
export type { EncryptionConfig, EncryptionMode, FileEncryption, AadTag };

/**
 * Typed wrapper for the "file is encrypted but no config on VFS" case.
 * `instanceof ENCRYPTION_REQUIRED` is more obvious than checking
 * `err.code === "EACCES"` and inspecting the message.
 */
export class ENCRYPTION_REQUIRED extends VFSFsError {
  constructor(opts: { syscall?: string; path?: string } = {}) {
    super("EACCES", {
      ...opts,
      message: "EACCES: encryption config required to access this file",
    });
    this.name = "ENCRYPTION_REQUIRED";
  }
}

/**
 * Typed wrapper for AES-GCM auth-tag mismatch — the canonical "wrong
 * key (or wrong tenantSalt, or auth-tag-tampered ciphertext)" signal.
 *
 * Surfaces as EINVAL to match Node.js' error code for "data appears
 * corrupt". Preserves `cause` so consumers who care about the
 * underlying Error can drill in.
 *
 * This class is narrowly scoped to auth-tag mismatches. Envelope
 * structural errors (truncation, unsupported version, AAD-tag
 * cross-purpose mismatch) surface as {@link CORRUPT_ENVELOPE} —
 * those are NOT key-rotation problems and operators debugging
 * "WRONG_KEY storms" need to know the difference.
 */
export class WRONG_KEY extends VFSFsError {
  readonly cause?: unknown;
  constructor(opts: { syscall?: string; path?: string; cause?: unknown } = {}) {
    super("EINVAL", {
      ...(opts.syscall !== undefined ? { syscall: opts.syscall } : {}),
      ...(opts.path !== undefined ? { path: opts.path } : {}),
      message: "EINVAL: decryption failed (wrong master key, wrong tenantSalt, or auth-tag-tampered ciphertext)",
    });
    this.name = "WRONG_KEY";
    if (opts.cause !== undefined) this.cause = opts.cause;
  }
}

/**
 * Typed wrapper for envelope STRUCTURAL failure — bytes don't decode
 * as a valid Mossaic envelope (truncated, unsupported version, AAD
 * cross-purpose mismatch, malformed tail).
 *
 * Without this discrimination, structural errors would surface as
 * `WRONG_KEY`, sending operators chasing key-rotation hypotheses
 * for what is actually a storage-corruption / replay /
 * version-mismatch event. Disambiguation lets dashboards alert
 * separately on "auth-tag mismatches" (key-rotation pressure) vs
 * "envelope corruption" (storage durability / replay-attack
 * pressure).
 *
 * Inherits from {@link WRONG_KEY} so existing `instanceof WRONG_KEY`
 * call-sites keep working — corruption is a strict subset of "the
 * read failed and you should treat the bytes as untrusted". The
 * `name` differs (`CORRUPT_ENVELOPE` vs `WRONG_KEY`) so logging /
 * metrics dashboards can split the streams.
 */
export class CORRUPT_ENVELOPE extends WRONG_KEY {
  constructor(opts: { syscall?: string; path?: string; cause?: unknown } = {}) {
    super(opts);
    this.name = "CORRUPT_ENVELOPE";
    // Override the message inherited from WRONG_KEY to reflect the
    // actual failure mode. (The `code` stays EINVAL — both classes
    // map to the same HTTP status / typed error code.)
    Object.defineProperty(this, "message", {
      value:
        "EINVAL: decryption failed (envelope corrupt: truncated, unsupported version, or AAD cross-purpose mismatch)",
      enumerable: false,
      writable: true,
      configurable: true,
    });
  }
}

/**
 * Encrypt a plaintext payload as a single envelope.
 *
 * The current SDK encrypts the WHOLE file as one envelope (one
 * AES-GCM call). The server stores the envelope as opaque chunk
 * bytes; on read, the SDK fetches the envelope and decrypts in one
 * call. This avoids any chunk-boundary coordination between SDK
 * and server.
 *
 * Multi-chunk envelope-stream encryption (which would unlock
 * per-chunk dedup at the chunked tier) is a future enhancement and
 * uses the same `encryptChunk` primitive over `computeChunkSpec`
 * boundaries.
 *
 * @param plaintext bytes to encrypt
 * @param config the VFS instance's encryption config
 * @param mode override (per-call); falls back to `config.mode`, then convergent
 * @param keyId override (per-call); falls back to `config.keyId`
 * @param aadTag AAD discriminator. 'ck' for file content (default).
 */
export async function encryptPayload(
  plaintext: Uint8Array,
  config: EncryptionConfig,
  mode?: EncryptionMode,
  keyId?: string,
  aadTag: AadTag = "ck"
): Promise<Uint8Array> {
  validateConfig(config);
  const finalMode: EncryptionMode = mode ?? config.mode ?? "convergent";
  const finalKeyId = keyId ?? config.keyId;
  const opts: Parameters<typeof encryptChunkPure>[0] = {
    plaintext,
    masterRaw: config.masterKey,
    tenantSalt: config.tenantSalt,
    mode: finalMode,
    aadTag,
  };
  if (finalKeyId !== undefined) opts.keyId = finalKeyId;
  return encryptChunkPure(opts);
}

/**
 * Decrypt a single envelope payload back to plaintext.
 *
 * Error discrimination: a naive single-bucket `WRONG_KEY` mapping
 * would make operator dashboards conflate auth-tag mismatches
 * (= key rotation / re-keying pressure) with envelope corruption
 * (= storage durability / replay-attack / version-mismatch
 * pressure). They have different remediations.
 *
 * Now:
 *  - WebCrypto AES-GCM auth-tag mismatch → {@link WRONG_KEY}.
 *    Cause: wrong masterKey, wrong tenantSalt, or auth-tag-tampered
 *    ciphertext. Operators see a spike → check key rotation.
 *  - Envelope structure failure (unpackEnvelope throws, AAD tag
 *    cross-purpose, missing plaintextHash, etc.) →
 *    {@link CORRUPT_ENVELOPE}. Cause: storage corruption, version
 *    skew, replay attack, AAD context mix-up. Operators see a
 *    spike → check storage durability + version compatibility.
 *
 * `CORRUPT_ENVELOPE` extends `WRONG_KEY` so existing
 * `instanceof WRONG_KEY` consumers keep working — a corrupt envelope
 * IS still "decryption failed, don't trust the bytes". The
 * additional class name lets monitoring split the alarm streams.
 */
export async function decryptPayload(
  envelope: Uint8Array,
  config: EncryptionConfig,
  aadTag: AadTag = "ck",
  ctx: { path?: string; syscall?: string } = {}
): Promise<Uint8Array> {
  validateConfig(config);
  try {
    return await decryptChunkPure({
      envelope,
      masterRaw: config.masterKey,
      tenantSalt: config.tenantSalt,
      expectedAadTag: aadTag,
    });
  } catch (cause) {
    // Distinguish by error shape. AES-GCM auth-tag mismatches
    // surface as `OperationError` from WebCrypto (or DOMException
    // with name "OperationError" depending on runtime). Everything
    // else originated in our own `throw new Error(...)` calls
    // inside unpackEnvelope / decryptChunk's structural validation.
    if (isAesGcmAuthFailure(cause)) {
      throw new WRONG_KEY({ ...ctx, cause });
    }
    throw new CORRUPT_ENVELOPE({ ...ctx, cause });
  }
}

/**
 * AES-GCM auth-tag failures from WebCrypto have a distinctive
 * shape across runtimes:
 *  - workerd / Cloudflare Workers: throws `Error` with a message
 *    containing "OperationError" or "decryption failed".
 *  - Node 19+ / browser: throws `DOMException` with name
 *    "OperationError".
 *  - Older runtimes (legacy Edge): may throw an empty Error.
 *
 * Our own structural failures all throw `new Error("decryptChunk: …")`
 * or `new Error("unpackEnvelope: …")` — distinctive prefixes. Anything
 * NOT matching one of those prefixes AND coming from
 * `crypto.subtle.decrypt` (which is the only async-await call inside
 * decryptChunkPure) is treated as auth failure.
 */
function isAesGcmAuthFailure(err: unknown): boolean {
  if (typeof err !== "object" || err === null) return false;
  const e = err as { name?: unknown; message?: unknown };
  // Structural / our-own throws have message prefixes we control.
  // Anything matching one of those is NOT AES-GCM auth failure.
  if (typeof e.message === "string") {
    if (
      e.message.startsWith("decryptChunk:") ||
      e.message.startsWith("unpackEnvelope:")
    ) {
      return false;
    }
  }
  // DOMException / Error shape from WebCrypto.
  if (e.name === "OperationError") return true;
  // workerd / Node fallback: Error with a message we don't
  // recognise. Treat as AES-GCM auth failure (the most common
  // residual case once structural failures are filtered).
  return true;
}

/**
 * Validate the shape of the encryption config supplied to createVFS.
 * Throws a typed EINVAL if any field is missing or malformed.
 */
export function validateConfig(config: EncryptionConfig): void {
  if (!config.masterKey || config.masterKey.byteLength !== 32) {
    throw new EINVAL({
      syscall: "createVFS",
      path: "(opts.encryption.masterKey)",
    });
  }
  if (!config.tenantSalt || config.tenantSalt.byteLength !== 32) {
    throw new EINVAL({
      syscall: "createVFS",
      path: "(opts.encryption.tenantSalt)",
    });
  }
  if (config.mode !== undefined && config.mode !== "convergent" && config.mode !== "random") {
    throw new EINVAL({
      syscall: "createVFS",
      path: `(opts.encryption.mode='${String(config.mode)}')`,
    });
  }
  if (config.keyId !== undefined) {
    const utf8 = new TextEncoder().encode(config.keyId).byteLength;
    if (utf8 > 128) {
      throw new EINVAL({
        syscall: "createVFS",
        path: `(opts.encryption.keyId UTF-8 length ${utf8} > 128)`,
      });
    }
  }
}

/**
 * Convenience: derive a 32-byte raw master key from a UTF-8 password.
 * Wraps {@link deriveMasterFromPasswordPure} with the OWASP-2024
 * default iteration count.
 *
 * Recommend caching the result in a non-extractable CryptoKey or KMS
 * stash. PBKDF2-SHA256 at 600k iterations costs ~250-400ms; the
 * consumer pays it ONCE at session start.
 */
export async function deriveMasterFromPassword(
  password: string,
  tenantSalt: Uint8Array,
  iterations: number = PBKDF2_DEFAULT_ITERATIONS
): Promise<Uint8Array> {
  return deriveMasterFromPasswordPure(password, tenantSalt, iterations);
}

/**
 * Resolve the per-call encryption stamp from VFS config + per-call
 * override. Returns `undefined` when the call is plaintext.
 *
 * - `encrypted === false` → undefined (explicit plaintext)
 * - `encrypted === true` → use config.mode/keyId
 * - `encrypted === { mode?, keyId? }` → merge with config defaults
 * - `encrypted === undefined` → undefined (default)
 */
export function resolveCallEncryption(
  config: EncryptionConfig | undefined,
  encrypted:
    | true
    | false
    | { mode?: EncryptionMode; keyId?: string }
    | undefined
): { mode: EncryptionMode; keyId?: string } | undefined {
  if (encrypted === undefined || encrypted === false) return undefined;
  if (!config) {
    // The SDK's writeFile / readFile wraps this and re-maps to EINVAL
    // before any RPC. We throw here so callers get a useful surface.
    throw new EINVAL({
      syscall: "writeFile",
      path: "(encryption opt set without createVFS encryption config)",
    });
  }
  let mode: EncryptionMode;
  let keyId: string | undefined;
  if (encrypted === true) {
    mode = config.mode ?? "convergent";
    keyId = config.keyId;
  } else {
    mode = encrypted.mode ?? config.mode ?? "convergent";
    keyId = encrypted.keyId ?? config.keyId;
  }
  const out: { mode: EncryptionMode; keyId?: string } = { mode };
  if (keyId !== undefined) out.keyId = keyId;
  return out;
}

/**
 * Build the {@link ENCRYPTION_REQUIRED} error consumers see when a
 * readFile / openYDoc on an encrypted file lacks the necessary config.
 * Centralized so the message stays consistent across surfaces.
 */
export function makeEncryptionRequiredError(
  syscall: string,
  path: string
): ENCRYPTION_REQUIRED {
  return new ENCRYPTION_REQUIRED({ syscall, path });
}
