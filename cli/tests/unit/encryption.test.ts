import { describe, it, expect } from "vitest";
import {
  encryptChunk,
  decryptChunk,
  deriveMasterFromPassword,
  bytesToHex,
  hexToBytes,
} from "@mossaic/sdk/encryption";

/**
 * CLI-side unit tests for encryption primitives.
 *
 * These are SDK-encryption smoke tests run in the CLI's vitest pool
 * (Node, not workerd) to confirm that:
 *   E1. WebCrypto primitives work in Node 20+ (subtle.encrypt /
 *       subtle.deriveBits exist; getRandomValues exists).
 *   E2. encryptChunk → decryptChunk round-trip works in Node.
 *   E3. PBKDF2 derives a 32-byte key.
 *   E4. Hex helpers round-trip.
 *   E5. Convergent mode is deterministic across a Node call.
 */

const SALT = new Uint8Array(32).fill(0xa1);
const MASTER = new Uint8Array(32).fill(0x42);

describe("CLI / encryption smoke", () => {
  it("E1 — WebCrypto subtle is available in Node 20+", () => {
    expect(typeof crypto).toBe("object");
    expect(typeof crypto.subtle).toBe("object");
    expect(typeof crypto.getRandomValues).toBe("function");
  });

  it("E2 — encryptChunk → decryptChunk round-trips a small payload", async () => {
    const plaintext = new TextEncoder().encode("hello CLI encryption");
    const env = await encryptChunk({
      plaintext,
      masterRaw: MASTER,
      tenantSalt: SALT,
      mode: "random",
      aadTag: "ck",
    });
    const out = await decryptChunk({
      envelope: env,
      masterRaw: MASTER,
      tenantSalt: SALT,
      expectedAadTag: "ck",
    });
    expect(out).toEqual(plaintext);
  });

  it("E3 — PBKDF2 derives a 32-byte master key", async () => {
    const k = await deriveMasterFromPassword("test-password", SALT, 100_000);
    expect(k.byteLength).toBe(32);
  });

  it("E4 — hex helpers round-trip arbitrary bytes", () => {
    const a = new Uint8Array([0xde, 0xad, 0xbe, 0xef, 0x00, 0xff, 0x42]);
    const hex = bytesToHex(a);
    expect(hex).toBe("deadbeef00ff42");
    const b = hexToBytes(hex);
    expect(b).toEqual(a);
  });

  it("E5 — convergent mode is deterministic", async () => {
    const plaintext = new TextEncoder().encode("identical bytes");
    const e1 = await encryptChunk({
      plaintext,
      masterRaw: MASTER,
      tenantSalt: SALT,
      mode: "convergent",
      aadTag: "ck",
    });
    const e2 = await encryptChunk({
      plaintext,
      masterRaw: MASTER,
      tenantSalt: SALT,
      mode: "convergent",
      aadTag: "ck",
    });
    expect(e1).toEqual(e2);
  });
});
