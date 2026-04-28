import { describe, it, expect } from "vitest";
import { env, SELF } from "cloudflare:test";

/**
 * Phase 8 — graceful degradation: transport failures map to typed
 * MossaicUnavailableError, not raw fetch rejection.
 *
 * The SDK's mapServerError + isLikelyUnavailable detect:
 *   - TypeError("Network request failed") from a rejecting global fetch
 *   - "Network connection lost" / "Durable Object hibernation timed out"
 *     / "fetch failed" / "ECONNREFUSED" message patterns
 *
 * These tests simulate those failure modes by feeding the HTTP client
 * a custom fetcher that rejects in known ways. The acceptance: every
 * thrown error is `instanceof MossaicUnavailableError` with the
 * canonical "EMOSSAIC_UNAVAILABLE" code — no raw fetch rejection
 * leaks to the consumer.
 */

import {
  createMossaicHttpClient,
  isLikelyUnavailable,
  MossaicUnavailableError,
  ENOENT,
  VFSFsError,
} from "../../sdk/src/index";
import { signVFSToken } from "@core/lib/auth";

interface E {
  USER_DO: DurableObjectNamespace;
  JWT_SECRET?: string;
}
const TEST_ENV = env as unknown as E;

const selfFetcher: typeof fetch = ((
  input: RequestInfo | URL,
  init?: RequestInit
) => {
  const url =
    typeof input === "string"
      ? input
      : input instanceof URL
      ? input.toString()
      : input.url;
  return SELF.fetch(url, init);
}) as typeof fetch;

async function mintToken(tenant: string): Promise<string> {
  return signVFSToken(TEST_ENV, { ns: "default", tenant });
}

describe("Graceful degradation — transport failures map to MossaicUnavailableError", () => {
  it("network rejection (TypeError fetch failed) → MossaicUnavailableError", async () => {
    const apiKey = await mintToken("gd-net");
    const failingFetcher: typeof fetch = (() => {
      const err = new TypeError("fetch failed");
      return Promise.reject(err);
    }) as typeof fetch;
    const vfs = createMossaicHttpClient({
      url: "https://mossaic.test",
      apiKey,
      fetcher: failingFetcher,
    });
    let caught: unknown = null;
    try {
      await vfs.readFile("/whatever");
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(MossaicUnavailableError);
    expect((caught as VFSFsError).code).toBe("EMOSSAIC_UNAVAILABLE");
  });

  it("ECONNREFUSED-shaped rejection → MossaicUnavailableError", async () => {
    const apiKey = await mintToken("gd-econn");
    const failingFetcher: typeof fetch = (() =>
      Promise.reject(
        new Error("connect ECONNREFUSED 127.0.0.1:443")
      )) as typeof fetch;
    const vfs = createMossaicHttpClient({
      url: "https://mossaic.test",
      apiKey,
      fetcher: failingFetcher,
    });
    await expect(vfs.exists("/x")).rejects.toBeInstanceOf(
      MossaicUnavailableError
    );
  });

  it("'Network connection lost' (workerd-shaped) → MossaicUnavailableError", async () => {
    const apiKey = await mintToken("gd-netlost");
    const failingFetcher: typeof fetch = (() =>
      Promise.reject(new Error("Network connection lost."))) as typeof fetch;
    const vfs = createMossaicHttpClient({
      url: "https://mossaic.test",
      apiKey,
      fetcher: failingFetcher,
    });
    let caught: unknown = null;
    try {
      await vfs.stat("/x");
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(MossaicUnavailableError);
  });

  it("DO hibernation timeout shape → MossaicUnavailableError", async () => {
    const apiKey = await mintToken("gd-hibernate");
    const failingFetcher: typeof fetch = (() =>
      Promise.reject(
        new Error("Durable Object hibernation timed out")
      )) as typeof fetch;
    const vfs = createMossaicHttpClient({
      url: "https://mossaic.test",
      apiKey,
      fetcher: failingFetcher,
    });
    await expect(vfs.exists("/x")).rejects.toBeInstanceOf(
      MossaicUnavailableError
    );
  });

  it("server-side ENOENT error (real DO + happy network) is NOT remapped to Unavailable", async () => {
    // Sanity: the unavailable detector must not over-classify normal
    // application-level errors. ENOENT for a missing file should
    // surface as ENOENT, not Unavailable.
    const apiKey = await mintToken("gd-enoent-pass");
    const vfs = createMossaicHttpClient({
      url: "https://mossaic.test",
      apiKey,
      fetcher: selfFetcher,
    });
    let caught: unknown = null;
    try {
      await vfs.readFile("/missing.txt");
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(ENOENT);
    expect(caught).not.toBeInstanceOf(MossaicUnavailableError);
  });

  it("isLikelyUnavailable correctly classifies common patterns", () => {
    expect(isLikelyUnavailable(new TypeError("fetch failed"))).toBe(true);
    expect(
      isLikelyUnavailable(new Error("Network connection lost."))
    ).toBe(true);
    expect(
      isLikelyUnavailable(new Error("Durable Object hibernation timed out"))
    ).toBe(true);
    expect(
      isLikelyUnavailable(new Error("connect ECONNREFUSED 1.2.3.4:443"))
    ).toBe(true);
    expect(
      isLikelyUnavailable(new Error("read ECONNRESET"))
    ).toBe(true);
    expect(
      isLikelyUnavailable(new Error("Failed to fetch"))
    ).toBe(true);
    expect(
      isLikelyUnavailable(new Error("service binding unavailable"))
    ).toBe(true);

    // Application errors must NOT be classified as unavailable.
    expect(
      isLikelyUnavailable(
        Object.assign(new Error("ENOENT: no such file"), { code: "ENOENT" })
      )
    ).toBe(false);
    expect(
      isLikelyUnavailable(new Error("EISDIR: is a directory"))
    ).toBe(false);
    expect(isLikelyUnavailable(null)).toBe(false);
    expect(isLikelyUnavailable(undefined)).toBe(false);
  });

  it("MossaicUnavailableError is a VFSFsError subclass with stable code/errno", () => {
    const err = new MossaicUnavailableError({ message: "test" });
    expect(err).toBeInstanceOf(VFSFsError);
    expect(err).toBeInstanceOf(MossaicUnavailableError);
    expect(err.code).toBe("EMOSSAIC_UNAVAILABLE");
    expect(err.errno).toBe(-111);
  });
});
