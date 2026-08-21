const BLOCK_BYTES = 64;
const LENGTH_BYTES = 8;
const MAX_UINT32 = 0xffffffff;

const INITIAL_WORDS = [
  0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c,
  0x1f83d9ab, 0x5be0cd19,
] as const;

const ROUND_CONSTANTS = new Uint32Array([
  0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1,
  0x923f82a4, 0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3,
  0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786,
  0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
  0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147,
  0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13,
  0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
  0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
  0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a,
  0x5b9cca4f, 0x682e6ff3, 0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208,
  0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
]);

interface Sha256StateData {
  words: Uint32Array;
  tail: Uint8Array;
  totalBytes: number;
}

const SHA256_STATE: unique symbol = Symbol("Sha256State");

export interface Sha256State {
  readonly [SHA256_STATE]: Sha256StateData;
}

export interface SerializedSha256State {
  readonly words: readonly number[];
  readonly tail: readonly number[];
  readonly totalBytes: number;
}

/** Creates an empty SHA-256 accumulator for content identity. */
export function createSha256State(): Sha256State {
  return createState(new Uint32Array(INITIAL_WORDS), new Uint8Array(), 0);
}

/** Adds bytes to an accumulator without retaining processed input. */
export function updateSha256(state: Sha256State, data: Uint8Array): void {
  const value = state[SHA256_STATE];
  const nextTotalBytes = value.totalBytes + data.byteLength;
  if (!Number.isSafeInteger(nextTotalBytes)) {
    throw new RangeError("SHA-256 input exceeds the safe byte-count range");
  }

  let offset = 0;
  if (value.tail.byteLength > 0) {
    const needed = BLOCK_BYTES - value.tail.byteLength;
    if (data.byteLength < needed) {
      const tail = new Uint8Array(value.tail.byteLength + data.byteLength);
      tail.set(value.tail);
      tail.set(data, value.tail.byteLength);
      value.tail = tail;
      value.totalBytes = nextTotalBytes;
      return;
    }

    const block = new Uint8Array(BLOCK_BYTES);
    block.set(value.tail);
    block.set(data.subarray(0, needed), value.tail.byteLength);
    compressBlock(value.words, block, 0);
    value.tail = new Uint8Array();
    offset = needed;
  }

  while (offset + BLOCK_BYTES <= data.byteLength) {
    compressBlock(value.words, data, offset);
    offset += BLOCK_BYTES;
  }

  if (offset < data.byteLength) {
    value.tail = data.slice(offset);
  }
  value.totalBytes = nextTotalBytes;
}

/** Returns a plain JSON-safe snapshot with no aliases to mutable state. */
export function serializeSha256State(
  state: Sha256State
): SerializedSha256State {
  const value = state[SHA256_STATE];
  return {
    words: Array.from(value.words),
    tail: Array.from(value.tail),
    totalBytes: value.totalBytes,
  };
}

/** Restores a snapshot after validating its shape and SHA-256 invariants. */
export function restoreSha256State(serialized: unknown): Sha256State {
  if (!isRecord(serialized)) {
    throw invalidSerializedState("expected an object");
  }

  const serializedWords = serialized.words;
  if (!isUnknownArray(serializedWords) || serializedWords.length !== 8) {
    throw invalidSerializedState("words must contain exactly 8 values");
  }
  const words = new Uint32Array(8);
  for (let i = 0; i < words.length; i++) {
    const word = serializedWords[i];
    if (!isIntegerInRange(word, 0, MAX_UINT32)) {
      throw invalidSerializedState(`words[${i}] must be an unsigned 32-bit integer`);
    }
    words[i] = word;
  }

  const serializedTail = serialized.tail;
  if (!isUnknownArray(serializedTail) || serializedTail.length >= BLOCK_BYTES) {
    throw invalidSerializedState("tail must contain at most 63 bytes");
  }
  const tail = new Uint8Array(serializedTail.length);
  for (let i = 0; i < tail.length; i++) {
    const byte = serializedTail[i];
    if (!isIntegerInRange(byte, 0, 0xff)) {
      throw invalidSerializedState(`tail[${i}] must be a byte`);
    }
    tail[i] = byte;
  }

  const totalBytes = serialized.totalBytes;
  if (
    typeof totalBytes !== "number" ||
    !Number.isSafeInteger(totalBytes) ||
    totalBytes < 0
  ) {
    throw invalidSerializedState("totalBytes must be a non-negative safe integer");
  }
  if (totalBytes % BLOCK_BYTES !== tail.byteLength) {
    throw invalidSerializedState("tail length does not match totalBytes");
  }
  if (totalBytes < BLOCK_BYTES) {
    for (let i = 0; i < words.length; i++) {
      if (words[i] !== INITIAL_WORDS[i]) {
        throw invalidSerializedState("unprocessed input must use the initial words");
      }
    }
  }

  return createState(words, tail, totalBytes);
}

/** Computes the digest without consuming or modifying the accumulator. */
export function digestSha256(state: Sha256State): Uint8Array {
  const value = state[SHA256_STATE];
  const words = new Uint32Array(value.words);
  const finalBlocks = new Uint8Array(value.tail.byteLength < 56 ? 64 : 128);
  finalBlocks.set(value.tail);
  finalBlocks[value.tail.byteLength] = 0x80;

  const lengthOffset = finalBlocks.byteLength - LENGTH_BYTES;
  const bitLengthHigh = Math.floor(value.totalBytes / 0x20000000) >>> 0;
  const bitLengthLow = ((value.totalBytes % 0x20000000) * 8) >>> 0;
  writeUint32BigEndian(finalBlocks, lengthOffset, bitLengthHigh);
  writeUint32BigEndian(finalBlocks, lengthOffset + 4, bitLengthLow);

  for (let offset = 0; offset < finalBlocks.byteLength; offset += BLOCK_BYTES) {
    compressBlock(words, finalBlocks, offset);
  }

  const digest = new Uint8Array(32);
  for (let i = 0; i < words.length; i++) {
    writeUint32BigEndian(digest, i * 4, words[i]);
  }
  return digest;
}

function createState(
  words: Uint32Array,
  tail: Uint8Array,
  totalBytes: number
): Sha256State {
  return { [SHA256_STATE]: { words, tail, totalBytes } };
}

function compressBlock(
  hash: Uint32Array,
  input: Uint8Array,
  offset: number
): void {
  const schedule = new Uint32Array(64);
  for (let i = 0; i < 16; i++) {
    const wordOffset = offset + i * 4;
    schedule[i] =
      ((input[wordOffset] << 24) |
        (input[wordOffset + 1] << 16) |
        (input[wordOffset + 2] << 8) |
        input[wordOffset + 3]) >>>
      0;
  }
  for (let i = 16; i < schedule.length; i++) {
    const x = schedule[i - 15];
    const y = schedule[i - 2];
    const sigma0 = rotateRight(x, 7) ^ rotateRight(x, 18) ^ (x >>> 3);
    const sigma1 = rotateRight(y, 17) ^ rotateRight(y, 19) ^ (y >>> 10);
    schedule[i] =
      (schedule[i - 16] + (sigma0 >>> 0) + schedule[i - 7] + (sigma1 >>> 0)) >>>
      0;
  }

  let a = hash[0];
  let b = hash[1];
  let c = hash[2];
  let d = hash[3];
  let e = hash[4];
  let f = hash[5];
  let g = hash[6];
  let h = hash[7];

  for (let i = 0; i < schedule.length; i++) {
    const sum1 = rotateRight(e, 6) ^ rotateRight(e, 11) ^ rotateRight(e, 25);
    const choice = (e & f) ^ (~e & g);
    const temp1 =
      (h +
        (sum1 >>> 0) +
        (choice >>> 0) +
        ROUND_CONSTANTS[i] +
        schedule[i]) >>>
      0;
    const sum0 = rotateRight(a, 2) ^ rotateRight(a, 13) ^ rotateRight(a, 22);
    const majority = (a & b) ^ (a & c) ^ (b & c);
    const temp2 = ((sum0 >>> 0) + (majority >>> 0)) >>> 0;

    h = g;
    g = f;
    f = e;
    e = (d + temp1) >>> 0;
    d = c;
    c = b;
    b = a;
    a = (temp1 + temp2) >>> 0;
  }

  hash[0] = (hash[0] + a) >>> 0;
  hash[1] = (hash[1] + b) >>> 0;
  hash[2] = (hash[2] + c) >>> 0;
  hash[3] = (hash[3] + d) >>> 0;
  hash[4] = (hash[4] + e) >>> 0;
  hash[5] = (hash[5] + f) >>> 0;
  hash[6] = (hash[6] + g) >>> 0;
  hash[7] = (hash[7] + h) >>> 0;
}

function rotateRight(value: number, bits: number): number {
  return ((value >>> bits) | (value << (32 - bits))) >>> 0;
}

function writeUint32BigEndian(
  output: Uint8Array,
  offset: number,
  value: number
): void {
  output[offset] = value >>> 24;
  output[offset + 1] = value >>> 16;
  output[offset + 2] = value >>> 8;
  output[offset + 3] = value;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isUnknownArray(value: unknown): value is unknown[] {
  return Array.isArray(value);
}

function isIntegerInRange(
  value: unknown,
  minimum: number,
  maximum: number
): value is number {
  return (
    typeof value === "number" &&
    Number.isInteger(value) &&
    value >= minimum &&
    value <= maximum
  );
}

function invalidSerializedState(reason: string): TypeError {
  return new TypeError(`Invalid serialized SHA-256 state: ${reason}`);
}
