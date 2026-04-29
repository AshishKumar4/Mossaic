import { describe, it, expect } from "vitest";
import {
  bytesToHex,
  convergentIv,
  decryptChunk,
  deriveChunkKeyConvergent,
  deriveMasterFromPassword,
  encryptChunk,
  envelopeHeaderHash,
  generateRandomChunkKey,
  hexToBytes,
  packEnvelope,
  randomIv,
  sha256,
  unpackEnvelope,
  unwrapKeyAesKw,
  wrapKeyAesKw,
} from "@shared/encryption";
import {
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
  type AadTag,
  type EnvelopeParts,
} from "@shared/encryption-types";

/**
 * Phase 15 — Step 1 — pure WebCrypto encryption primitives.
 *
 * These tests run in the workerd isolate (vitest-pool-workers). They
 * exercise the envelope codec, IV derivation, key derivation, AES-GCM
 * round-trips, and AES-KW wrap/unwrap — entirely client-side; no DO
 * interaction.
 *
 * Coverage targets per `local/phase-15-plan.md` §11 Step 1 acceptance:
 *   - Round-trips at 0 / 1 / 1024 / 1MB / 8MB.
 *   - AAD mismatch.
 *   - Tampered iv / tampered ct.
 *   - Convergent determinism.
 *   - Random non-determinism.
 *   - Salt distinguishability.
 *   - Version mismatch.
 *   - Edge sizes.
 *   - Empty plaintext.
 */

const SALT_A = new Uint8Array(32).fill(0xa1);
const SALT_B = new Uint8Array(32).fill(0xb2);
const MASTER_A = new Uint8Array(32).fill(0x42);
const MASTER_B = new Uint8Array(32).fill(0xee);

function fillBytes(n: number, byte: number = 0): Uint8Array {
  const b = new Uint8Array(n);
  b.fill(byte);
  return b;
}

function pseudoRandom(n: number, seed: number = 1): Uint8Array {
  // Deterministic PRNG (LCG) for reproducible test fixtures.
  const out = new Uint8Array(n);
  let s = seed >>> 0;
  for (let i = 0; i < n; i++) {
    s = (Math.imul(s, 1664525) + 1013904223) >>> 0;
    out[i] = s & 0xff;
  }
  return out;
}

describe("Phase 15 — encryption primitives", () => {
  // ─── Round-trips at multiple sizes ────────────────────────────────────

  it("round-trips at 0 bytes (convergent)", async () => {
    const pt = new Uint8Array(0);
    const env = await encryptChunk({
      plaintext: pt,
      masterRaw: MASTER_A,
      tenantSalt: SALT_A,
      mode: "convergent",
      aadTag: "ck",
    });
    const out = await decryptChunk({
      envelope: env,
      masterRaw: MASTER_A,
      tenantSalt: SALT_A,
      expectedAadTag: "ck",
    });
    expect(out).toEqual(pt);
  });

  it("round-trips at 1 byte (random)", async () => {
    const pt = new Uint8Array([0x7f]);
    const env = await encryptChunk({
      plaintext: pt,
      masterRaw: MASTER_A,
      tenantSalt: SALT_A,
      mode: "random",
      aadTag: "ck",
    });
    const out = await decryptChunk({
      envelope: env,
      masterRaw: MASTER_A,
      tenantSalt: SALT_A,
      expectedAadTag: "ck",
    });
    expect(out).toEqual(pt);
  });

  it("round-trips at 1 KiB (convergent + random both work)", async () => {
    const pt = pseudoRandom(1024, 7);
    for (const mode of ["convergent", "random"] as const) {
      const env = await encryptChunk({
        plaintext: pt,
        masterRaw: MASTER_A,
        tenantSalt: SALT_A,
        mode,
        aadTag: "ck",
      });
      const out = await decryptChunk({
        envelope: env,
        masterRaw: MASTER_A,
        tenantSalt: SALT_A,
        expectedAadTag: "ck",
      });
      expect(out).toEqual(pt);
    }
  });

  it("round-trips at 1 MiB (convergent)", async () => {
    const pt = pseudoRandom(1024 * 1024, 17);
    const env = await encryptChunk({
      plaintext: pt,
      masterRaw: MASTER_A,
      tenantSalt: SALT_A,
      mode: "convergent",
      aadTag: "ck",
    });
    const out = await decryptChunk({
      envelope: env,
      masterRaw: MASTER_A,
      tenantSalt: SALT_A,
      expectedAadTag: "ck",
    });
    expect(out).toEqual(pt);
  });

  it("round-trips at 8 MiB (random) — exercises larger AES-GCM inputs", async () => {
    const pt = pseudoRandom(8 * 1024 * 1024, 23);
    const env = await encryptChunk({
      plaintext: pt,
      masterRaw: MASTER_A,
      tenantSalt: SALT_A,
      mode: "random",
      aadTag: "ck",
    });
    const out = await decryptChunk({
      envelope: env,
      masterRaw: MASTER_A,
      tenantSalt: SALT_A,
      expectedAadTag: "ck",
    });
    expect(out.byteLength).toBe(pt.byteLength);
    // Spot-check rather than full equality (deep-equal on 8MB is slow).
    expect(out.subarray(0, 64)).toEqual(pt.subarray(0, 64));
    expect(out.subarray(out.byteLength - 64)).toEqual(
      pt.subarray(pt.byteLength - 64)
    );
  });

  // ─── Convergent determinism ───────────────────────────────────────────

  it("convergent: same (master, salt, plaintext, aadTag) → identical envelope", async () => {
    const pt = pseudoRandom(2048, 31);
    const e1 = await encryptChunk({
      plaintext: pt,
      masterRaw: MASTER_A,
      tenantSalt: SALT_A,
      mode: "convergent",
      aadTag: "ck",
      keyId: "v1",
    });
    const e2 = await encryptChunk({
      plaintext: pt,
      masterRaw: MASTER_A,
      tenantSalt: SALT_A,
      mode: "convergent",
      aadTag: "ck",
      keyId: "v1",
    });
    expect(e1).toEqual(e2);
    // And the headerHash is identical (this is what enables dedup).
    const h1 = await envelopeHeaderHash(e1);
    const h2 = await envelopeHeaderHash(e2);
    expect(h1).toEqual(h2);
  });

  it("random: same inputs produce DIFFERENT envelopes (no determinism)", async () => {
    const pt = pseudoRandom(2048, 41);
    const e1 = await encryptChunk({
      plaintext: pt,
      masterRaw: MASTER_A,
      tenantSalt: SALT_A,
      mode: "random",
      aadTag: "ck",
    });
    const e2 = await encryptChunk({
      plaintext: pt,
      masterRaw: MASTER_A,
      tenantSalt: SALT_A,
      mode: "random",
      aadTag: "ck",
    });
    expect(e1).not.toEqual(e2);
    const h1 = await envelopeHeaderHash(e1);
    const h2 = await envelopeHeaderHash(e2);
    expect(h1).not.toEqual(h2);
  });

  // ─── Cross-tenant salt distinguishability ─────────────────────────────

  it("convergent: distinct salts → distinct envelopes for identical plaintext", async () => {
    const pt = pseudoRandom(512, 51);
    const eA = await encryptChunk({
      plaintext: pt,
      masterRaw: MASTER_A,
      tenantSalt: SALT_A,
      mode: "convergent",
      aadTag: "ck",
    });
    const eB = await encryptChunk({
      plaintext: pt,
      masterRaw: MASTER_A,
      tenantSalt: SALT_B,
      mode: "convergent",
      aadTag: "ck",
    });
    expect(eA).not.toEqual(eB);
    expect(await envelopeHeaderHash(eA)).not.toEqual(
      await envelopeHeaderHash(eB)
    );
  });

  // ─── AAD mismatch / cross-purpose envelope replay ─────────────────────

  it("rejects decrypt when expectedAadTag differs from envelope's tag", async () => {
    const pt = pseudoRandom(64, 61);
    const env = await encryptChunk({
      plaintext: pt,
      masterRaw: MASTER_A,
      tenantSalt: SALT_A,
      mode: "convergent",
      aadTag: "ck",
    });
    await expect(
      decryptChunk({
        envelope: env,
        masterRaw: MASTER_A,
        tenantSalt: SALT_A,
        expectedAadTag: "yj", // mismatch
      })
    ).rejects.toThrow(/aadTag mismatch/);
  });

  // ─── Tampered IV → auth-tag failure ───────────────────────────────────

  it("rejects decrypt when IV is tampered (auth-tag failure)", async () => {
    const pt = pseudoRandom(256, 71);
    const env = await encryptChunk({
      plaintext: pt,
      masterRaw: MASTER_A,
      tenantSalt: SALT_A,
      mode: "random",
      aadTag: "ck",
    });
    // Locate iv field: header is version(1)+mode(1)+keyIdLen(2)+keyId(0)
    // → iv at offset 4.
    const tampered = new Uint8Array(env);
    tampered[4] ^= 0x01;
    await expect(
      decryptChunk({
        envelope: tampered,
        masterRaw: MASTER_A,
        tenantSalt: SALT_A,
        expectedAadTag: "ck",
      })
    ).rejects.toThrow();
  });

  it("rejects decrypt when ct is tampered (auth-tag failure)", async () => {
    const pt = pseudoRandom(256, 73);
    const env = await encryptChunk({
      plaintext: pt,
      masterRaw: MASTER_A,
      tenantSalt: SALT_A,
      mode: "convergent",
      aadTag: "ck",
    });
    const tampered = new Uint8Array(env);
    // Flip a byte deep in ct.
    tampered[tampered.byteLength - 5] ^= 0xff;
    await expect(
      decryptChunk({
        envelope: tampered,
        masterRaw: MASTER_A,
        tenantSalt: SALT_A,
        expectedAadTag: "ck",
      })
    ).rejects.toThrow();
  });

  it("rejects decrypt under wrong master key", async () => {
    const pt = pseudoRandom(256, 79);
    const env = await encryptChunk({
      plaintext: pt,
      masterRaw: MASTER_A,
      tenantSalt: SALT_A,
      mode: "convergent",
      aadTag: "ck",
    });
    await expect(
      decryptChunk({
        envelope: env,
        masterRaw: MASTER_B, // wrong master
        tenantSalt: SALT_A,
        expectedAadTag: "ck",
      })
    ).rejects.toThrow();
  });

  // ─── Version mismatch ─────────────────────────────────────────────────

  it("rejects unpack on unsupported version byte", async () => {
    const pt = pseudoRandom(64, 83);
    const env = await encryptChunk({
      plaintext: pt,
      masterRaw: MASTER_A,
      tenantSalt: SALT_A,
      mode: "convergent",
      aadTag: "ck",
    });
    const tampered = new Uint8Array(env);
    tampered[0] = 99; // version byte
    expect(() => unpackEnvelope(tampered)).toThrow(/unsupported version/);
  });

  it("rejects unpack on invalid mode byte", async () => {
    const pt = pseudoRandom(64, 89);
    const env = await encryptChunk({
      plaintext: pt,
      masterRaw: MASTER_A,
      tenantSalt: SALT_A,
      mode: "random",
      aadTag: "ck",
    });
    const tampered = new Uint8Array(env);
    tampered[1] = 99; // mode byte
    expect(() => unpackEnvelope(tampered)).toThrow(/invalid mode byte/);
  });

  // ─── Envelope structure / pack-unpack symmetry ────────────────────────

  it("pack→unpack is a bijection for both modes", async () => {
    const ct = pseudoRandom(80, 91);
    const partsConv: EnvelopeParts = {
      version: ENVELOPE_VERSION,
      mode: "convergent",
      keyId: "kid-v1",
      iv: pseudoRandom(IV_LENGTH, 92),
      aadTag: "ck",
      ext: new Uint8Array(0),
      plaintextHash: pseudoRandom(SHA256_LENGTH, 93),
      ct,
    };
    const packedConv = packEnvelope(partsConv);
    const decodedConv = unpackEnvelope(packedConv);
    expect(decodedConv.version).toBe(partsConv.version);
    expect(decodedConv.mode).toBe(partsConv.mode);
    expect(decodedConv.keyId).toBe(partsConv.keyId);
    expect(decodedConv.iv).toEqual(partsConv.iv);
    expect(decodedConv.aadTag).toBe(partsConv.aadTag);
    expect(decodedConv.plaintextHash).toEqual(partsConv.plaintextHash);
    expect(decodedConv.ct).toEqual(partsConv.ct);

    const partsRand: EnvelopeParts = {
      version: ENVELOPE_VERSION,
      mode: "random",
      keyId: "kid-v2",
      iv: pseudoRandom(IV_LENGTH, 94),
      aadTag: "yj",
      ext: new Uint8Array(0),
      wrappedKey: pseudoRandom(WRAPPED_KEY_LENGTH, 95),
      ct,
    };
    const packedRand = packEnvelope(partsRand);
    const decodedRand = unpackEnvelope(packedRand);
    expect(decodedRand.mode).toBe("random");
    expect(decodedRand.wrappedKey).toEqual(partsRand.wrappedKey);
    expect(decodedRand.ct).toEqual(partsRand.ct);
  });

  // ─── Header hash determinism for convergent dedup ─────────────────────

  it("envelopeHeaderHash is deterministic and excludes ct payload", async () => {
    const pt = pseudoRandom(1024, 101);
    const env1 = await encryptChunk({
      plaintext: pt,
      masterRaw: MASTER_A,
      tenantSalt: SALT_A,
      mode: "convergent",
      aadTag: "ck",
    });
    const env2 = await encryptChunk({
      plaintext: pt,
      masterRaw: MASTER_A,
      tenantSalt: SALT_A,
      mode: "convergent",
      aadTag: "ck",
    });
    const h1 = await envelopeHeaderHash(env1);
    const h2 = await envelopeHeaderHash(env2);
    expect(h1).toEqual(h2);
    expect(h1).toMatch(/^[0-9a-f]{64}$/);
  });

  // ─── PBKDF2 / master-key derivation ───────────────────────────────────

  it("deriveMasterFromPassword: deterministic for fixed password+salt", async () => {
    // Use a small iteration count to keep test fast; PBKDF2 is purely
    // deterministic so behavior is identical to production iteration.
    const k1 = await deriveMasterFromPassword(
      "hunter2",
      SALT_A,
      100_000
    );
    const k2 = await deriveMasterFromPassword(
      "hunter2",
      SALT_A,
      100_000
    );
    expect(k1).toEqual(k2);
    expect(k1.byteLength).toBe(MASTER_KEY_LENGTH);
  });

  it("deriveMasterFromPassword: distinct salts → distinct keys", async () => {
    const kA = await deriveMasterFromPassword("pw", SALT_A, 100_000);
    const kB = await deriveMasterFromPassword("pw", SALT_B, 100_000);
    expect(kA).not.toEqual(kB);
  });

  it("deriveMasterFromPassword: rejects iterations < 100_000", async () => {
    await expect(
      deriveMasterFromPassword("pw", SALT_A, 99_999)
    ).rejects.toThrow(/insecure/);
  });

  // ─── AES-KW wrap / unwrap round-trip (random-mode internal) ───────────

  it("AES-KW wrap → unwrap → encrypt → decrypt is a full round-trip", async () => {
    const { raw } = await generateRandomChunkKey();
    const wrapped = await wrapKeyAesKw(raw, MASTER_A);
    expect(wrapped.byteLength).toBe(WRAPPED_KEY_LENGTH);
    const unwrappedKey = await unwrapKeyAesKw(wrapped, MASTER_A);

    const iv = randomIv();
    const aad = new TextEncoder().encode("ck");
    const ptOriginal = new TextEncoder().encode("hello world");
    const ctBuf = await crypto.subtle.encrypt(
      { name: "AES-GCM", iv, additionalData: aad },
      unwrappedKey,
      ptOriginal
    );

    // Re-derive original key into another importKey to verify wrap symmetry.
    const reKey = await crypto.subtle.importKey(
      "raw",
      raw,
      { name: "AES-GCM", length: 256 },
      false,
      ["decrypt"]
    );
    const ptOut = await crypto.subtle.decrypt(
      { name: "AES-GCM", iv, additionalData: aad },
      reKey,
      ctBuf
    );
    expect(new Uint8Array(ptOut)).toEqual(ptOriginal);
  });

  // ─── Convergent IV property ───────────────────────────────────────────

  it("convergentIv: deterministic under same inputs; differs by aadTag", async () => {
    const ptHash = await sha256(pseudoRandom(512, 111));
    const ivCk = await convergentIv(MASTER_A, SALT_A, ptHash, "ck");
    const ivCk2 = await convergentIv(MASTER_A, SALT_A, ptHash, "ck");
    const ivYj = await convergentIv(MASTER_A, SALT_A, ptHash, "yj");
    expect(ivCk).toEqual(ivCk2);
    expect(ivCk).not.toEqual(ivYj);
    expect(ivCk.byteLength).toBe(IV_LENGTH);
  });

  it("randomIv: 12 bytes, non-deterministic", () => {
    const a = randomIv();
    const b = randomIv();
    expect(a.byteLength).toBe(IV_LENGTH);
    expect(a).not.toEqual(b);
  });

  // ─── deriveChunkKeyConvergent: deterministic ──────────────────────────

  it("deriveChunkKeyConvergent: produces a usable AES-GCM key", async () => {
    const ptHash = await sha256(pseudoRandom(64, 121));
    const k = await deriveChunkKeyConvergent(MASTER_A, SALT_A, ptHash, "ck");
    const iv = await convergentIv(MASTER_A, SALT_A, ptHash, "ck");
    const aad = new TextEncoder().encode("ck");
    const ct = await crypto.subtle.encrypt(
      { name: "AES-GCM", iv, additionalData: aad },
      k,
      new Uint8Array([1, 2, 3])
    );
    const k2 = await deriveChunkKeyConvergent(MASTER_A, SALT_A, ptHash, "ck");
    const pt = await crypto.subtle.decrypt(
      { name: "AES-GCM", iv, additionalData: aad },
      k2,
      ct
    );
    expect(new Uint8Array(pt)).toEqual(new Uint8Array([1, 2, 3]));
  });

  // ─── KeyId field encoding ─────────────────────────────────────────────

  it("packEnvelope rejects keyId > 128 UTF-8 bytes", () => {
    const overflow = "x".repeat(KEY_ID_MAX_BYTES + 1);
    expect(() =>
      packEnvelope({
        version: ENVELOPE_VERSION,
        mode: "convergent",
        keyId: overflow,
        iv: new Uint8Array(IV_LENGTH),
        aadTag: "ck",
        ext: new Uint8Array(0),
        plaintextHash: new Uint8Array(SHA256_LENGTH),
        ct: new Uint8Array(AUTH_TAG_LENGTH),
      })
    ).toThrow(/exceeds/);
  });

  it("UTF-8 keyId round-trips through pack/unpack", async () => {
    const pt = new Uint8Array([42, 43, 44]);
    const env = await encryptChunk({
      plaintext: pt,
      masterRaw: MASTER_A,
      tenantSalt: SALT_A,
      mode: "convergent",
      aadTag: "ck",
      keyId: "🔐-v1-tenant-α",
    });
    const parts = unpackEnvelope(env);
    expect(parts.keyId).toBe("🔐-v1-tenant-α");
  });

  // ─── Hex helpers ──────────────────────────────────────────────────────

  it("bytesToHex / hexToBytes round-trip", () => {
    const a = pseudoRandom(64, 131);
    const hex = bytesToHex(a);
    expect(hex.length).toBe(128);
    expect(hexToBytes(hex)).toEqual(a);
  });

  // ─── Constants sanity (compile-time alignment with plan §3) ───────────

  it("envelope constants align with §3 plan layout", () => {
    expect(IV_LENGTH).toBe(12);
    expect(AUTH_TAG_LENGTH).toBe(16);
    expect(SHA256_LENGTH).toBe(32);
    expect(MASTER_KEY_LENGTH).toBe(32);
    expect(WRAPPED_KEY_LENGTH).toBe(40);
    expect(ENVELOPE_VERSION).toBe(1);
    expect(MODE_BYTE_CONVERGENT).toBe(1);
    expect(MODE_BYTE_RANDOM).toBe(2);
    expect(PBKDF2_DEFAULT_ITERATIONS).toBeGreaterThanOrEqual(600_000);
  });

  // ─── AAD enumeration completeness ─────────────────────────────────────

  it("all AAD tags (ck, yj, aw) round-trip", async () => {
    for (const tag of ["ck", "yj", "aw"] as AadTag[]) {
      const pt = new TextEncoder().encode(`payload for ${tag}`);
      const env = await encryptChunk({
        plaintext: pt,
        masterRaw: MASTER_A,
        tenantSalt: SALT_A,
        mode: "random",
        aadTag: tag,
      });
      const out = await decryptChunk({
        envelope: env,
        masterRaw: MASTER_A,
        tenantSalt: SALT_A,
        expectedAadTag: tag,
      });
      expect(out).toEqual(pt);
    }
  });

  // ─── Convergent integrity check ───────────────────────────────────────

  it("convergent decrypt rejects mutated plaintextHash field", async () => {
    const pt = pseudoRandom(128, 141);
    const env = await encryptChunk({
      plaintext: pt,
      masterRaw: MASTER_A,
      tenantSalt: SALT_A,
      mode: "convergent",
      aadTag: "ck",
    });
    // The plaintextHash sits 32 bytes before the ctLen field.
    // Header layout: version(1)+mode(1)+keyIdLen(2)+keyId(0)+iv(12)+aadLen(2)+aad(2 'ck')+extLen(2)+ext(0)+plaintextHash(32)+ctLen(4)+ct.
    // Header offset of plaintextHash = 1+1+2+0+12+2+2+2+0 = 22.
    // Note: aadLen = 2 bytes, aadTag 'ck' is 2 bytes.
    const tampered = new Uint8Array(env);
    // Find plaintextHash by parsing then mutating.
    const parts = unpackEnvelope(env);
    expect(parts.plaintextHash).toBeDefined();
    // The tampered hash field should no longer equal the actual plaintext hash.
    // We mutate one byte of plaintextHash. Locate it: it sits at
    // (envelope.byteLength - 4 - parts.ct.byteLength - 32).
    const hashOff = env.byteLength - 4 - parts.ct.byteLength - 32;
    tampered[hashOff] ^= 0xff;
    await expect(
      decryptChunk({
        envelope: tampered,
        masterRaw: MASTER_A,
        tenantSalt: SALT_A,
        expectedAadTag: "ck",
      })
    ).rejects.toThrow();
    // The error is either "plaintextHash integrity check failed" (if the
    // mutation made the hash refer to a still-valid ciphertext under the
    // new derived key — extremely unlikely) or AES-GCM auth-tag mismatch
    // (the typical case, since the IV is derived from plaintextHash).
  });
});
