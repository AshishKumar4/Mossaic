import { describe, it, expect } from "vitest";
import { createMossaicHttpClient } from "../../sdk/src/http";

/**
 * Phase 17.6 — `multipartBaseOverride` + `chunkFetchBaseOverride` tests.
 *
 *   B1.  Default (no override) resolves multipart calls under
 *        `/api/vfs/multipart/...`.
 *   B2.  `multipartBaseOverride: "/api/upload/multipart"` redirects
 *        every multipart call to the override path.
 *   B3.  Both overrides are independent: setting `multipartBaseOverride`
 *        does NOT change `chunkFetchBase`, and vice versa.
 *
 * The tests use a stub `fetch` that captures the URL each multipart
 * method tries to hit; we never reach the wire.
 */

interface CapturedCall {
  url: string;
  method: string;
}

function makeStubFetcher(): {
  fetcher: typeof fetch;
  calls: CapturedCall[];
} {
  const calls: CapturedCall[] = [];
  const fetcher: typeof fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
    const url =
      typeof input === "string"
        ? input
        : input instanceof URL
          ? input.toString()
          : input.url;
    const method =
      init?.method ?? (typeof input !== "string" && !(input instanceof URL) ? input.method : "GET");
    calls.push({ url, method });
    // Return a reasonable JSON success body so the SDK's response
    // parsing doesn't blow up.
    return new Response(
      JSON.stringify({
        uploadId: "u",
        chunkSize: 100,
        totalChunks: 1,
        poolSize: 32,
        sessionToken: "tok",
        putEndpoint: "/api/vfs/multipart/u",
        expiresAtMs: Date.now() + 1000,
        landed: [],
      }),
      { status: 200, headers: { "Content-Type": "application/json" } }
    );
  }) as typeof fetch;
  return { fetcher, calls };
}

describe("Phase 17.6 — HTTP base overrides", () => {
  it("B1 — default multipart calls land under /api/vfs/multipart", async () => {
    const { fetcher, calls } = makeStubFetcher();
    const client = createMossaicHttpClient({
      url: "https://app.example.com",
      apiKey: "tok",
      fetcher,
    });
    await client.multipartBegin({ path: "/x.bin", size: 100 });
    expect(calls.length).toBe(1);
    expect(calls[0]!.url).toBe(
      "https://app.example.com/api/vfs/multipart/begin"
    );
    expect(calls[0]!.method).toBe("POST");
  });

  it("B2 — multipartBaseOverride redirects every multipart route", async () => {
    const { fetcher, calls } = makeStubFetcher();
    const client = createMossaicHttpClient({
      url: "https://app.example.com",
      apiKey: "tok",
      fetcher,
      multipartBaseOverride: "/api/upload/multipart",
    });

    await client.multipartBegin({ path: "/x.bin", size: 100 });
    await client.multipartFinalize("u", ["a".repeat(64)]).catch(() => {
      /* finalize body parse may fail since stub returns begin-shaped JSON; we only inspect URL */
    });
    await client.multipartAbort("u").catch(() => {});
    await client.multipartStatus("u", "tok").catch(() => {});
    await client.multipartDownloadToken("/x.bin").catch(() => {});

    const urls = calls.map((c) => c.url);
    expect(urls).toContain(
      "https://app.example.com/api/upload/multipart/begin"
    );
    expect(urls).toContain(
      "https://app.example.com/api/upload/multipart/finalize"
    );
    expect(urls).toContain(
      "https://app.example.com/api/upload/multipart/abort"
    );
    expect(urls).toContain(
      "https://app.example.com/api/upload/multipart/u/status"
    );
    expect(urls).toContain(
      "https://app.example.com/api/upload/multipart/download-token"
    );
    // None of the calls should fall through to the canonical path.
    for (const u of urls) {
      expect(u.includes("/api/vfs/multipart/")).toBe(false);
    }
  });

  it("B3 — chunkFetchBaseOverride redirects fetchChunkByHash to GET endpoint", async () => {
    const { fetcher, calls } = makeStubFetcher();
    const client = createMossaicHttpClient({
      url: "https://app.example.com",
      apiKey: "tok",
      fetcher,
      chunkFetchBaseOverride: "/api/download",
    });
    await client
      .fetchChunkByHash(
        "file-abc",
        2,
        "0".repeat(64),
        "dl-tok",
        "/x.bin"
      )
      .catch(() => {
        // Stub returns JSON; fetchChunkByHash tries to read arrayBuffer
        // — that succeeds even on JSON, so this catch is defensive.
      });
    expect(calls.length).toBe(1);
    expect(calls[0]!.url).toBe(
      "https://app.example.com/api/download/chunk/file-abc/2"
    );
    expect(calls[0]!.method).toBe("GET");
  });

  it("Defaults preserved when neither override is set", async () => {
    const { fetcher, calls } = makeStubFetcher();
    const client = createMossaicHttpClient({
      url: "https://app.example.com",
      apiKey: "tok",
      fetcher,
    });
    await client
      .fetchChunkByHash(
        "file-abc",
        0,
        "0".repeat(64),
        "dl-tok",
        "/x.bin"
      )
      .catch(() => {});
    // Default route is POST /api/vfs/readChunk.
    expect(calls.length).toBe(1);
    expect(calls[0]!.url).toBe("https://app.example.com/api/vfs/readChunk");
    expect(calls[0]!.method).toBe("POST");
  });

  it("Trailing slashes on overrides are normalized", async () => {
    const { fetcher, calls } = makeStubFetcher();
    const client = createMossaicHttpClient({
      url: "https://app.example.com/",
      apiKey: "tok",
      fetcher,
      multipartBaseOverride: "/api/upload/multipart/",
    });
    await client.multipartBegin({ path: "/x.bin", size: 100 });
    expect(calls[0]!.url).toBe(
      "https://app.example.com/api/upload/multipart/begin"
    );
  });
});
