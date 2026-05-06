/**
 * Envelope packing / unpacking + content-addressed header hash.
 *
 * The envelope is the on-the-wire byte form for an encrypted chunk; its
 * canonical layout (envelope-version, mode-byte, keyId, IV, AAD tag,
 * extension area, mode-specific tail) lives in
 * `local/phase-15-plan.md` §3 and is mirrored byte-exact by the Lean
 * model in `lean/Mossaic/Vfs/Encryption.lean`.
 */

import {
  type AadTag,
  type EncryptionMode,
  type EnvelopeParts,
  AUTH_TAG_LENGTH,
  ENVELOPE_VERSION,
  IV_LENGTH,
  KEY_ID_MAX_BYTES,
  MODE_BYTE_CONVERGENT,
  MODE_BYTE_RANDOM,
  SHA256_LENGTH,
  WRAPPED_KEY_LENGTH,
} from "../encryption-types";
import { bytesToHex } from "../crypto";
import {
  TD,
  TE,
  readUint16BE,
  readUint32BE,
  writeUint16BE,
  writeUint32BE,
} from "./internal";
import { sha256 } from "./iv";

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
