import { describe, it, expect } from "vitest";
import { SELF, env } from "cloudflare:test";

/**
 * × composition tests.
 *
 * Verifies that per-chunk envelope encryption composes
 * cleanly with multipart parallel transfer via the SDK's
 * `chunkTransform` hook. The hook seals each plaintext chunk into an
 * envelope BEFORE the multipart engine hashes and PUTs it; the
 * server stores the envelope verbatim and hashes it; the inverse
 * transform unseals on download.
 *
 * What we prove here:
 *   1. Random-mode round-trip — plaintext → seal → multipart upload →
 *      multipart download → unseal → identical plaintext.
 *   2. Convergent-mode round-trip — same plaintext + same key produces
 *      identical envelopes (deterministic dedup), and the round-trip
 *      reads back the original plaintext.
 *   3. Hash equality across two convergent uploads — the server-side
 *      `chunkHashList` returned by finalize is identical when the
 *      same plaintext is uploaded twice under the same convergent key.
 *
 * Notes:
 *   - We don't pass `opts.encryption` to `parallelUpload` here. The
 *     server's mode-monotonic stamp is set on the FIRST encrypted
 *     write (existing plumbing in vfsBeginMultipart). The
 *     transport-only test confirms the byte path; the server-side
 *     stamp behavior is covered in own suite.
 *   - We use `encryptPayload` / `decryptPayload` from
 *     `@mossaic/sdk/encryption` as the chunkTransform implementations.
 */

import {
  createMossaicHttpClient,
  parallelUpload,
  parallelDownload,
} from "../../sdk/src/index";
import { encryptPayload, decryptPayload } from "../../sdk/src/encryption";
import { signVFSToken } from "@core/lib/auth";
import type { EncryptionConfig } from "@shared/encryption-types";

interface E {
  JWT_SECRET?: string;
}
const TEST_ENV = env as unknown as E;

const selfFetcher: typeof fetch = ((input: RequestInfo | URL, init?: RequestInit) => {
  const url = typeof input === "string"
    ? input
    : input instanceof URL
    ? input.toString()
    : input.url;
  return SELF.fetch(url, init);
}) as typeof fetch;

async function clientFor(tenant: string) {
  const apiKey = await signVFSToken(TEST_ENV as never, {
    ns: "default",
    tenant,
  });
  return createMossaicHttpClient({
    url: "https://mossaic.test",
    apiKey,
    fetcher: selfFetcher,
  });
}

function makeKey(byte: number): Uint8Array {
  const a = new Uint8Array(32);
  a.fill(byte);
  return a;
}

function makePayload(n: number, seed: number): Uint8Array {
  const out = new Uint8Array(n);
  let x = seed >>> 0;
  for (let i = 0; i < n; i++) {
    x = (x * 1664525 + 1013904223) >>> 0;
    out[i] = x & 0xff;
  }
  return out;
}

function configFor(mode: "convergent" | "random"): EncryptionConfig {
  return {
    masterKey: makeKey(0x42),
    tenantSalt: makeKey(0xa5),
    mode,
  };
}

describe("× multipart with per-chunk encryption", () => {
  it("random mode: plaintext → seal → upload → download → unseal round-trips", async () => {
    const vfs = await clientFor("mp-enc-random-1");
    const config = configFor("random");
    const plaintext = makePayload(400, 1);

    const r = await parallelUpload(vfs, "/enc-random.bin", plaintext, {
      chunkSize: 100, // 4 chunks
      chunkTransform: async (chunk) =>
        encryptPayload(chunk, config, "random", undefined, "ck"),
    });
    expect(r.size).toBeGreaterThan(plaintext.byteLength); // envelope overhead

    const recovered = await parallelDownload(vfs, "/enc-random.bin", {
      chunkTransform: async (envelope) =>
        decryptPayload(envelope, config, "ck"),
    });
    // Strip any trailing zero-padding that the output buffer may have
    // (parallelDownload's `out` is sized to manifest.size which is the
    // envelope-stream size; per-chunk unseal returns shorter plaintext).
    const trimmed = recovered.subarray(0, plaintext.byteLength);
    expect(trimmed).toEqual(plaintext);
  });

  it("convergent mode: identical plaintext → identical envelopes (dedup)", async () => {
    const vfs = await clientFor("mp-enc-convergent-1");
    const config = configFor("convergent");
    const plaintext = makePayload(300, 2);

    const r1 = await parallelUpload(vfs, "/enc-conv-a.bin", plaintext, {
      chunkSize: 100,
      chunkTransform: async (chunk) =>
        encryptPayload(chunk, config, "convergent", undefined, "ck"),
    });
    const r2 = await parallelUpload(vfs, "/enc-conv-b.bin", plaintext, {
      chunkSize: 100,
      chunkTransform: async (chunk) =>
        encryptPayload(chunk, config, "convergent", undefined, "ck"),
    });
    // Convergent: same plaintext + same master/salt → same envelopes
    // → same hashes → same fileHash.
    expect(r1.fileHash).toBe(r2.fileHash);
  });

  it("convergent mode: round-trip recovers plaintext byte-for-byte", async () => {
    const vfs = await clientFor("mp-enc-convergent-2");
    const config = configFor("convergent");
    const plaintext = makePayload(250, 3);

    await parallelUpload(vfs, "/enc-conv-rt.bin", plaintext, {
      chunkSize: 100,
      chunkTransform: async (chunk) =>
        encryptPayload(chunk, config, "convergent", undefined, "ck"),
    });
    const recovered = await parallelDownload(vfs, "/enc-conv-rt.bin", {
      chunkTransform: async (envelope) =>
        decryptPayload(envelope, config, "ck"),
    });
    const trimmed = recovered.subarray(0, plaintext.byteLength);
    expect(trimmed).toEqual(plaintext);
  });
});
