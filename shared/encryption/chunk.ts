/**
 * One-shot chunk encryption / decryption.
 *
 * Each function consumes (plaintext, master, tenantSalt, mode, aadTag) →
 * an opaque envelope (encrypt) or vice versa (decrypt). The envelope
 * format is the source of truth for what the server stores; see
 * {@link packEnvelope} / {@link unpackEnvelope} for the wire layout.
 */

import {
  type AadTag,
  type EncryptionMode,
  ENVELOPE_VERSION,
} from "../encryption-types";
import { asView, bytesEqual, TE } from "./internal";
import { convergentIv, randomIv, sha256 } from "./iv";
import {
  deriveChunkKeyConvergent,
  generateRandomChunkKey,
  unwrapKeyAesKw,
  wrapKeyAesKw,
} from "./keys";
import { packEnvelope, unpackEnvelope } from "./envelope";

export interface EncryptChunkOptions {
  plaintext: Uint8Array;
  masterRaw: Uint8Array;
  tenantSalt: Uint8Array;
  mode: EncryptionMode;
  aadTag: AadTag;
  keyId?: string;
}

/**
 * Encrypt one chunk. Returns the packed envelope.
 *
 * The envelope is what gets written to chunk storage. For convergent mode,
 * the per-chunk key is HKDF-derived from (master, salt, sha256(plaintext),
 * aadTag); the IV is derived from the same inputs. For random mode, a fresh
 * key is generated, AES-KW-wrapped under master, and a 12-byte random IV
 * is drawn.
 */
export async function encryptChunk(
  opts: EncryptChunkOptions
): Promise<Uint8Array> {
  const aadBytes = TE.encode(opts.aadTag);

  if (opts.mode === "convergent") {
    const ptHash = await sha256(opts.plaintext);
    const iv = await convergentIv(
      opts.masterRaw,
      opts.tenantSalt,
      ptHash,
      opts.aadTag
    );
    const key = await deriveChunkKeyConvergent(
      opts.masterRaw,
      opts.tenantSalt,
      ptHash,
      opts.aadTag
    );
    const ctBuf = await crypto.subtle.encrypt(
      { name: "AES-GCM", iv: asView(iv), additionalData: asView(aadBytes) },
      key,
      asView(opts.plaintext)
    );
    return packEnvelope({
      version: ENVELOPE_VERSION,
      mode: "convergent",
      keyId: opts.keyId ?? "",
      iv,
      aadTag: opts.aadTag,
      ext: new Uint8Array(0),
      plaintextHash: ptHash,
      ct: new Uint8Array(ctBuf),
    });
  }

  // random mode
  const { key, raw } = await generateRandomChunkKey();
  const wrappedKey = await wrapKeyAesKw(raw, opts.masterRaw);
  // raw is no longer needed; let GC reclaim. (Best-effort zeroize.)
  raw.fill(0);
  const iv = randomIv();
  const ctBuf = await crypto.subtle.encrypt(
    { name: "AES-GCM", iv: asView(iv), additionalData: asView(aadBytes) },
    key,
    asView(opts.plaintext)
  );
  return packEnvelope({
    version: ENVELOPE_VERSION,
    mode: "random",
    keyId: opts.keyId ?? "",
    iv,
    aadTag: opts.aadTag,
    ext: new Uint8Array(0),
    wrappedKey,
    ct: new Uint8Array(ctBuf),
  });
}

export interface DecryptChunkOptions {
  envelope: Uint8Array;
  masterRaw: Uint8Array;
  tenantSalt: Uint8Array;
  /**
   * Expected AAD tag. Decryption refuses if the envelope's aadTag doesn't
   * match — guards against cross-purpose envelope replay.
   */
  expectedAadTag: AadTag;
}

/**
 * Decrypt one envelope. Throws on:
 * - malformed envelope (bounds-check failure during unpack)
 * - aadTag mismatch
 * - auth-tag mismatch (WebCrypto OperationError)
 *
 * Callers (SDK) re-map all of the above to `VFSError("EINVAL", ...)`.
 */
export async function decryptChunk(
  opts: DecryptChunkOptions
): Promise<Uint8Array> {
  const parts = unpackEnvelope(opts.envelope);
  if (parts.aadTag !== opts.expectedAadTag) {
    throw new Error(
      `decryptChunk: aadTag mismatch (got '${parts.aadTag}', want '${opts.expectedAadTag}')`
    );
  }
  const aadBytes = TE.encode(parts.aadTag);

  let key: CryptoKey;
  if (parts.mode === "convergent") {
    if (!parts.plaintextHash) {
      throw new Error("decryptChunk: convergent envelope missing plaintextHash");
    }
    key = await deriveChunkKeyConvergent(
      opts.masterRaw,
      opts.tenantSalt,
      parts.plaintextHash,
      parts.aadTag
    );
  } else {
    if (!parts.wrappedKey) {
      throw new Error("decryptChunk: random envelope missing wrappedKey");
    }
    key = await unwrapKeyAesKw(parts.wrappedKey, opts.masterRaw);
  }

  const ptBuf = await crypto.subtle.decrypt(
    { name: "AES-GCM", iv: asView(parts.iv), additionalData: asView(aadBytes) },
    key,
    asView(parts.ct)
  );
  const pt = new Uint8Array(ptBuf);

  // Convergent integrity check: the envelope's plaintextHash MUST match
  // SHA-256(decrypted plaintext). AES-GCM auth tag already protects against
  // tampering, but a tampered plaintextHash field (which is OUTSIDE the
  // ciphertext) could otherwise mislead a future re-derivation. Verify.
  if (parts.mode === "convergent") {
    const actualHash = await sha256(pt);
    if (!bytesEqual(actualHash, parts.plaintextHash!)) {
      throw new Error("decryptChunk: plaintextHash integrity check failed");
    }
  }

  return pt;
}
