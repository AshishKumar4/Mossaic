/**
 * opt-in end-to-end encryption primitives.
 *
 * Pure WebCrypto. NO userspace crypto libraries (no crypto-js, tweetnacl,
 * libsodium, node-forge, node:crypto). Every primitive is a thin wrapper
 * around `crypto.subtle.*` and `crypto.getRandomValues`.
 *
 * Tree-shakeable: no top-level side effects, no imports beyond types.
 *
 * See `local/phase-15-plan.md` §3 (envelope layout) and §8 (Lean obligations).
 */

import {
  type AadTag,
  type EncryptionMode,
  type EnvelopeParts,
  AUTH_TAG_LENGTH,
  ENVELOPE_VERSION,
  IV_LENGTH,
  KEY_ID_MAX_BYTES,
  MASTER_KEY_LENGTH,
  MODE_BYTE_CONVERGENT,
  MODE_BYTE_RANDOM,
  PBKDF2_DEFAULT_ITERATIONS,
  SHA256_LENGTH,
  WRAPPED_KEY_LENGTH,
} from "./encryption-types";

// ─── Internal byte helpers ────────────────────────────────────────────────

/**
 * As a stable typed-array view; narrows the underlying buffer type to
 * `ArrayBuffer` (vs `ArrayBufferLike`/`SharedArrayBuffer`) so that the
 * result is assignable to WebCrypto's `BufferSource` parameter under
 * `@cloudflare/workers-types` strict typing.
 *
 * The byte payload is unchanged; this is purely a type narrowing.
 */
function asView(b: Uint8Array): Uint8Array<ArrayBuffer> {
  return new Uint8Array(
    b.buffer as ArrayBuffer,
    b.byteOffset,
    b.byteLength
  ) as Uint8Array<ArrayBuffer>;
}

/** Concatenate Uint8Arrays. */
function concat(...parts: Uint8Array[]): Uint8Array {
  let total = 0;
  for (const p of parts) total += p.byteLength;
  const out = new Uint8Array(total);
  let off = 0;
  for (const p of parts) {
    out.set(p, off);
    off += p.byteLength;
  }
  return out;
}

function writeUint16BE(b: Uint8Array, off: number, v: number): void {
  if (v < 0 || v > 0xffff) throw new RangeError("uint16 overflow");
  b[off] = (v >>> 8) & 0xff;
  b[off + 1] = v & 0xff;
}

function readUint16BE(b: Uint8Array, off: number): number {
  if (off + 2 > b.byteLength) throw new RangeError("uint16 OOB");
  return ((b[off] ?? 0) << 8) | (b[off + 1] ?? 0);
}

function writeUint32BE(b: Uint8Array, off: number, v: number): void {
  if (v < 0 || v > 0xffffffff) throw new RangeError("uint32 overflow");
  b[off] = (v >>> 24) & 0xff;
  b[off + 1] = (v >>> 16) & 0xff;
  b[off + 2] = (v >>> 8) & 0xff;
  b[off + 3] = v & 0xff;
}

function readUint32BE(b: Uint8Array, off: number): number {
  if (off + 4 > b.byteLength) throw new RangeError("uint32 OOB");
  return (
    ((b[off] ?? 0) * 0x1000000 +
      ((b[off + 1] ?? 0) << 16) +
      ((b[off + 2] ?? 0) << 8) +
      (b[off + 3] ?? 0)) >>>
    0
  );
}

const TE = new TextEncoder();
const TD = new TextDecoder();

/** Constant-time-ish equal for short byte strings (header tags). */
function bytesEqual(a: Uint8Array, b: Uint8Array): boolean {
  if (a.byteLength !== b.byteLength) return false;
  let diff = 0;
  for (let i = 0; i < a.byteLength; i++) diff |= (a[i] ?? 0) ^ (b[i] ?? 0);
  return diff === 0;
}

// ─── SHA-256 ──────────────────────────────────────────────────────────────

/** Raw 32-byte SHA-256 digest. */
export async function sha256(data: Uint8Array): Promise<Uint8Array> {
  const digest = await crypto.subtle.digest("SHA-256", asView(data));
  return new Uint8Array(digest);
}

// ─── IV derivation ────────────────────────────────────────────────────────

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

// ─── Master key derivation (PBKDF2) ───────────────────────────────────────

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

// ─── Chunk-key derivation (HKDF for convergent; random for random-mode) ──

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

// ─── AES-KW wrap / unwrap (random-mode only) ──────────────────────────────

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

// ─── Envelope packing / unpacking ─────────────────────────────────────────

/**
 * Pack a parsed envelope into its on-the-wire byte form.
 * See `local/phase-15-plan.md` §3 for the canonical layout.
 */
export function packEnvelope(parts: EnvelopeParts): Uint8Array {
  if (parts.version !== ENVELOPE_VERSION) {
    throw new Error(`packEnvelope: unsupported version ${parts.version}`);
  }
  if (parts.iv.byteLength !== IV_LENGTH) {
    throw new Error("packEnvelope: iv must be 12 bytes");
  }
  if (parts.ct.byteLength < AUTH_TAG_LENGTH) {
    throw new Error("packEnvelope: ct shorter than auth tag");
  }

  const keyIdBytes = TE.encode(parts.keyId);
  if (keyIdBytes.byteLength > KEY_ID_MAX_BYTES) {
    throw new Error(
      `packEnvelope: keyId exceeds ${KEY_ID_MAX_BYTES} bytes UTF-8`
    );
  }
  if (keyIdBytes.byteLength > 0xffff) {
    throw new Error("packEnvelope: keyId length overflows uint16");
  }

  const aadBytes = TE.encode(parts.aadTag);
  if (aadBytes.byteLength > 0xffff) {
    throw new Error("packEnvelope: aadTag length overflows uint16");
  }

  if (parts.ext.byteLength > 0xffff) {
    throw new Error("packEnvelope: ext length overflows uint16");
  }
  if (parts.ct.byteLength > 0xffffffff) {
    throw new Error("packEnvelope: ct length overflows uint32");
  }

  const modeByte =
    parts.mode === "convergent" ? MODE_BYTE_CONVERGENT : MODE_BYTE_RANDOM;

  // Header size: 1 (ver) + 1 (mode) + 2 (keyIdLen) + N0 (keyId) +
  //              12 (iv) + 2 (aadLen) + N1 (aad) + 2 (extLen) + N2 (ext)
  const headerSize =
    1 +
    1 +
    2 +
    keyIdBytes.byteLength +
    IV_LENGTH +
    2 +
    aadBytes.byteLength +
    2 +
    parts.ext.byteLength;

  // Tail size: convergent → 32 (plaintextHash) + 4 (ctLen) + ct
  //            random     →  2 (wrappedLen) + 40 (wrappedKey) + 4 (ctLen) + ct
  let tailSize: number;
  if (parts.mode === "convergent") {
    if (
      !parts.plaintextHash ||
      parts.plaintextHash.byteLength !== SHA256_LENGTH
    ) {
      throw new Error("packEnvelope: convergent envelope requires plaintextHash(32)");
    }
    tailSize = SHA256_LENGTH + 4 + parts.ct.byteLength;
  } else {
    if (!parts.wrappedKey || parts.wrappedKey.byteLength !== WRAPPED_KEY_LENGTH) {
      throw new Error(
        "packEnvelope: random envelope requires wrappedKey(40)"
      );
    }
    tailSize = 2 + WRAPPED_KEY_LENGTH + 4 + parts.ct.byteLength;
  }

  const out = new Uint8Array(headerSize + tailSize);
  let off = 0;

  out[off++] = ENVELOPE_VERSION;
  out[off++] = modeByte;
  writeUint16BE(out, off, keyIdBytes.byteLength);
  off += 2;
  out.set(keyIdBytes, off);
  off += keyIdBytes.byteLength;
  out.set(parts.iv, off);
  off += IV_LENGTH;
  writeUint16BE(out, off, aadBytes.byteLength);
  off += 2;
  out.set(aadBytes, off);
  off += aadBytes.byteLength;
  writeUint16BE(out, off, parts.ext.byteLength);
  off += 2;
  out.set(parts.ext, off);
  off += parts.ext.byteLength;

  if (parts.mode === "convergent") {
    out.set(parts.plaintextHash!, off);
    off += SHA256_LENGTH;
  } else {
    writeUint16BE(out, off, WRAPPED_KEY_LENGTH);
    off += 2;
    out.set(parts.wrappedKey!, off);
    off += WRAPPED_KEY_LENGTH;
  }
  writeUint32BE(out, off, parts.ct.byteLength);
  off += 4;
  out.set(parts.ct, off);
  off += parts.ct.byteLength;

  if (off !== out.byteLength) {
    throw new Error("packEnvelope: internal length mismatch");
  }
  return out;
}

/**
 * Unpack an envelope. Bounds-check failures throw; callers re-map to EINVAL.
 */
export function unpackEnvelope(envelope: Uint8Array): EnvelopeParts {
  let off = 0;
  if (envelope.byteLength < 4) {
    throw new Error("unpackEnvelope: envelope too short");
  }

  const version = envelope[off++] ?? 0;
  if (version !== ENVELOPE_VERSION) {
    throw new Error(`unpackEnvelope: unsupported version ${version}`);
  }
  const modeByte = envelope[off++] ?? 0;
  let mode: EncryptionMode;
  if (modeByte === MODE_BYTE_CONVERGENT) mode = "convergent";
  else if (modeByte === MODE_BYTE_RANDOM) mode = "random";
  else throw new Error(`unpackEnvelope: invalid mode byte ${modeByte}`);

  if (off + 2 > envelope.byteLength) {
    throw new Error("unpackEnvelope: truncated keyIdLen");
  }
  const keyIdLen = readUint16BE(envelope, off);
  off += 2;
  if (off + keyIdLen > envelope.byteLength) {
    throw new Error("unpackEnvelope: truncated keyId");
  }
  const keyIdBytes = envelope.subarray(off, off + keyIdLen);
  off += keyIdLen;
  const keyId = TD.decode(keyIdBytes);

  if (off + IV_LENGTH > envelope.byteLength) {
    throw new Error("unpackEnvelope: truncated iv");
  }
  const iv = envelope.slice(off, off + IV_LENGTH);
  off += IV_LENGTH;

  if (off + 2 > envelope.byteLength) {
    throw new Error("unpackEnvelope: truncated aadLen");
  }
  const aadLen = readUint16BE(envelope, off);
  off += 2;
  if (off + aadLen > envelope.byteLength) {
    throw new Error("unpackEnvelope: truncated aadTag");
  }
  const aadBytes = envelope.subarray(off, off + aadLen);
  off += aadLen;
  const aadStr = TD.decode(aadBytes);
  if (aadStr !== "ck" && aadStr !== "yj" && aadStr !== "aw") {
    throw new Error(`unpackEnvelope: invalid aadTag '${aadStr}'`);
  }
  const aadTag: AadTag = aadStr;

  if (off + 2 > envelope.byteLength) {
    throw new Error("unpackEnvelope: truncated extLen");
  }
  const extLen = readUint16BE(envelope, off);
  off += 2;
  if (off + extLen > envelope.byteLength) {
    throw new Error("unpackEnvelope: truncated ext");
  }
  const ext = envelope.slice(off, off + extLen);
  off += extLen;

  let plaintextHash: Uint8Array | undefined;
  let wrappedKey: Uint8Array | undefined;

  if (mode === "convergent") {
    if (off + SHA256_LENGTH > envelope.byteLength) {
      throw new Error("unpackEnvelope: truncated plaintextHash");
    }
    plaintextHash = envelope.slice(off, off + SHA256_LENGTH);
    off += SHA256_LENGTH;
  } else {
    if (off + 2 > envelope.byteLength) {
      throw new Error("unpackEnvelope: truncated wrappedLen");
    }
    const wrappedLen = readUint16BE(envelope, off);
    off += 2;
    if (wrappedLen !== WRAPPED_KEY_LENGTH) {
      throw new Error(
        `unpackEnvelope: wrappedLen ${wrappedLen} ≠ ${WRAPPED_KEY_LENGTH}`
      );
    }
    if (off + wrappedLen > envelope.byteLength) {
      throw new Error("unpackEnvelope: truncated wrappedKey");
    }
    wrappedKey = envelope.slice(off, off + wrappedLen);
    off += wrappedLen;
  }

  if (off + 4 > envelope.byteLength) {
    throw new Error("unpackEnvelope: truncated ctLen");
  }
  const ctLen = readUint32BE(envelope, off);
  off += 4;
  if (off + ctLen !== envelope.byteLength) {
    throw new Error(
      `unpackEnvelope: ct length mismatch (need ${ctLen}, have ${envelope.byteLength - off})`
    );
  }
  if (ctLen < AUTH_TAG_LENGTH) {
    throw new Error("unpackEnvelope: ct shorter than auth tag");
  }
  const ct = envelope.slice(off, off + ctLen);

  return {
    version,
    mode,
    keyId,
    iv,
    aadTag,
    ext,
    ...(plaintextHash !== undefined ? { plaintextHash } : {}),
    ...(wrappedKey !== undefined ? { wrappedKey } : {}),
    ct,
  };
}

/**
 * Compute the chunk hash that the server stores in `chunks.hash`.
 *
 * = SHA-256 of the envelope HEADER ONLY: everything from byte 0 through
 * the `ctLen` field (inclusive), but EXCLUDING the variable ciphertext
 * payload itself.
 *
 * Why this slice:
 * - Convergent dedup requires identical (master, salt, plaintext, aadTag)
 *   → identical hash. The header above is fully deterministic in convergent
 *   mode for fixed inputs (deterministic IV via {@link convergentIv},
 *   deterministic plaintextHash, fixed keyId/aadTag/ext, deterministic
 *   ctLen since plaintext length is fixed).
 * - Random mode envelopes naturally do NOT collide because their
 *   `iv` and `wrappedKey` are fresh per call.
 * - Including `ctLen` (vs only the pre-tail header) costs nothing for
 *   determinism and keeps the hashed prefix uniform across both modes.
 *
 * The byte range is computed by parsing the envelope ONCE; the result is
 * hashed in a single `crypto.subtle.digest` call.
 */
export async function envelopeHeaderHash(envelope: Uint8Array): Promise<string> {
  const parts = unpackEnvelope(envelope);
  // ct sits at the very end. Header end = envelope.length - ct.length.
  const headerEnd = envelope.byteLength - parts.ct.byteLength;
  const headerOnly = envelope.subarray(0, headerEnd);
  const digest = await sha256(headerOnly);
  return bytesToHex(digest);
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

// ─── Encrypt / decrypt one chunk → one envelope ───────────────────────────

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
};
export type { AadTag, EncryptionMode, EncryptionConfig, EnvelopeParts, FileEncryption } from "./encryption-types";
