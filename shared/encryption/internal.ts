/**
 * Internal byte helpers shared across the encryption modules.
 *
 * NOT exported from the public `@shared/encryption` barrel — every helper
 * is module-internal because each is a thin BufferSource adapter / endian
 * primitive that callers shouldn't reach for directly.
 */

/**
 * As a stable typed-array view; narrows the underlying buffer type to
 * `ArrayBuffer` (vs `ArrayBufferLike`/`SharedArrayBuffer`) so that the
 * result is assignable to WebCrypto's `BufferSource` parameter under
 * `@cloudflare/workers-types` strict typing.
 *
 * The byte payload is unchanged; this is purely a type narrowing.
 */
export function asView(b: Uint8Array): Uint8Array<ArrayBuffer> {
  return new Uint8Array(
    b.buffer as ArrayBuffer,
    b.byteOffset,
    b.byteLength
  ) as Uint8Array<ArrayBuffer>;
}

/** Concatenate Uint8Arrays. */
export function concat(...parts: Uint8Array[]): Uint8Array {
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

export function writeUint16BE(b: Uint8Array, off: number, v: number): void {
  if (v < 0 || v > 0xffff) throw new RangeError("uint16 overflow");
  b[off] = (v >>> 8) & 0xff;
  b[off + 1] = v & 0xff;
}

export function readUint16BE(b: Uint8Array, off: number): number {
  if (off + 2 > b.byteLength) throw new RangeError("uint16 OOB");
  return ((b[off] ?? 0) << 8) | (b[off + 1] ?? 0);
}

export function writeUint32BE(b: Uint8Array, off: number, v: number): void {
  if (v < 0 || v > 0xffffffff) throw new RangeError("uint32 overflow");
  b[off] = (v >>> 24) & 0xff;
  b[off + 1] = (v >>> 16) & 0xff;
  b[off + 2] = (v >>> 8) & 0xff;
  b[off + 3] = v & 0xff;
}

export function readUint32BE(b: Uint8Array, off: number): number {
  if (off + 4 > b.byteLength) throw new RangeError("uint32 OOB");
  return (
    ((b[off] ?? 0) * 0x1000000 +
      ((b[off + 1] ?? 0) << 16) +
      ((b[off + 2] ?? 0) << 8) +
      (b[off + 3] ?? 0)) >>>
    0
  );
}

export const TE = new TextEncoder();
export const TD = new TextDecoder();

/** Constant-time-ish equal for short byte strings (header tags). */
export function bytesEqual(a: Uint8Array, b: Uint8Array): boolean {
  if (a.byteLength !== b.byteLength) return false;
  let diff = 0;
  for (let i = 0; i < a.byteLength; i++) diff |= (a[i] ?? 0) ^ (b[i] ?? 0);
  return diff === 0;
}
