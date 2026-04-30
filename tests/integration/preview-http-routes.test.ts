import { describe, it, expect } from "vitest";
import { SELF, env } from "cloudflare:test";

/**
 * HTTP route gate for the preview pipeline:
 *
 *  - POST /api/vfs/readPreview returns variant bytes with
 *    content-addressed ETag + immutable Cache-Control.
 *  - If-None-Match revalidation returns 304 with no body.
 *  - POST /api/vfs/manifests batches openManifest across paths.
 *  - Errors map: ENOENT → 404, EISDIR → 409, ENOTSUP → 501.
 *  - Bad-token → 401.
 */

import { signVFSToken } from "@core/lib/auth";
import { createMossaicHttpClient } from "../../sdk/src/index";

interface E {
  MOSSAIC_USER: DurableObjectNamespace;
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

async function mintToken(ns: string, tenant: string): Promise<string> {
  return signVFSToken(TEST_ENV, { ns, tenant });
}

async function seed(tenant: string, path: string, content: string, mime: string) {
  const apiKey = await mintToken("default", tenant);
  const vfs = createMossaicHttpClient({
    url: "https://mossaic.test",
    apiKey,
    fetcher: selfFetcher,
  });
  await vfs.writeFile(path, content, { mimeType: mime });
  return apiKey;
}

describe("POST /api/vfs/readPreview", () => {
  it("returns SVG bytes + content-addressed ETag + immutable cache header", async () => {
    const apiKey = await seed(
      "preview-rt-1",
      "/code.ts",
      "function hi(){}\n",
      "text/typescript"
    );
    const res = await SELF.fetch("https://m.test/api/vfs/readPreview", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ path: "/code.ts", variant: "thumb" }),
    });
    expect(res.status).toBe(200);
    expect(res.headers.get("Content-Type")).toBe("image/svg+xml");
    const cc = res.headers.get("Cache-Control") ?? "";
    expect(cc).toContain("public");
    expect(cc).toContain("max-age=31536000");
    expect(cc).toContain("immutable");
    const etag = res.headers.get("ETag");
    expect(etag).toMatch(/^W\/"[0-9a-f]{64}"$/);
    expect(res.headers.get("X-Mossaic-Renderer")).toBe("code-svg");
    expect(res.headers.get("X-Mossaic-Variant-Cache")).toBe("miss");
    const body = await res.text();
    expect(body).toContain("<svg");
  });

  it("revalidation: If-None-Match equal to ETag → 304 with no body", async () => {
    const apiKey = await seed(
      "preview-rt-2",
      "/note.txt",
      "hello",
      "text/plain"
    );
    const first = await SELF.fetch("https://m.test/api/vfs/readPreview", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ path: "/note.txt", variant: "thumb" }),
    });
    const etag = first.headers.get("ETag")!;
    expect(etag).toBeTruthy();
    await first.arrayBuffer();

    const cached = await SELF.fetch("https://m.test/api/vfs/readPreview", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
        "If-None-Match": etag,
      },
      body: JSON.stringify({ path: "/note.txt", variant: "thumb" }),
    });
    expect(cached.status).toBe(304);
    expect(cached.headers.get("ETag")).toBe(etag);
    const buf = await cached.arrayBuffer();
    expect(buf.byteLength).toBe(0);
  });

  it("ENOENT → 404, EISDIR → 409, malformed body → 400", async () => {
    const apiKey = await seed(
      "preview-rt-3",
      "/anchor.txt",
      "anchor",
      "text/plain"
    );

    const enoent = await SELF.fetch("https://m.test/api/vfs/readPreview", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ path: "/missing", variant: "thumb" }),
    });
    expect(enoent.status).toBe(404);
    const ej = (await enoent.json()) as { code: string };
    expect(ej.code).toBe("ENOENT");

    const apiKey2 = await mintToken("default", "preview-rt-3");
    const vfs = createMossaicHttpClient({
      url: "https://mossaic.test",
      apiKey: apiKey2,
      fetcher: selfFetcher,
    });
    await vfs.mkdir("/d", { recursive: true });

    const eisdir = await SELF.fetch("https://m.test/api/vfs/readPreview", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ path: "/d", variant: "thumb" }),
    });
    expect(eisdir.status).toBe(409);

    const bad = await SELF.fetch("https://m.test/api/vfs/readPreview", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ variant: "thumb" }),
    });
    expect(bad.status).toBe(400);
  });

  it("rejects requests without Bearer token (401)", async () => {
    const res = await SELF.fetch("https://m.test/api/vfs/readPreview", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ path: "/any", variant: "thumb" }),
    });
    expect(res.status).toBe(401);
  });

  it("custom variant {width:128, fit:'cover'} validates and returns bytes", async () => {
    const apiKey = await seed(
      "preview-rt-4",
      "/a.txt",
      "abc",
      "text/plain"
    );
    const res = await SELF.fetch("https://m.test/api/vfs/readPreview", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        path: "/a.txt",
        variant: { width: 128, fit: "cover" },
      }),
    });
    expect(res.status).toBe(200);
    expect(res.headers.get("Content-Type")).toBe("image/svg+xml");

    // Bad variant shape → 400.
    const badVar = await SELF.fetch("https://m.test/api/vfs/readPreview", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ path: "/a.txt", variant: { width: -1 } }),
    });
    expect(badVar.status).toBe(400);
  });
});

describe("POST /api/vfs/manifests (batched)", () => {
  it("returns one manifest per path, mixing hits and misses", async () => {
    const apiKey = await seed(
      "manifests-rt-1",
      "/x.txt",
      "x".repeat(100_000),
      "text/plain"
    );
    const apiKey2 = await mintToken("default", "manifests-rt-1");
    const vfs = createMossaicHttpClient({
      url: "https://mossaic.test",
      apiKey: apiKey2,
      fetcher: selfFetcher,
    });
    await vfs.writeFile("/y.txt", "y".repeat(100_000));

    const res = await SELF.fetch("https://m.test/api/vfs/manifests", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ paths: ["/x.txt", "/missing", "/y.txt"] }),
    });
    expect(res.status).toBe(200);
    const body = (await res.json()) as {
      manifests: (
        | { ok: true; manifest: { fileId: string; size: number } }
        | { ok: false; code: string; message: string }
      )[];
    };
    expect(body.manifests).toHaveLength(3);
    expect(body.manifests[0].ok).toBe(true);
    expect(body.manifests[1].ok).toBe(false);
    if (body.manifests[1].ok === false) {
      expect(body.manifests[1].code).toBe("ENOENT");
    }
    expect(body.manifests[2].ok).toBe(true);
  });

  it("rejects non-array paths and oversized batches", async () => {
    const apiKey = await seed(
      "manifests-rt-2",
      "/anchor.txt",
      "anchor",
      "text/plain"
    );
    const bad = await SELF.fetch("https://m.test/api/vfs/manifests", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ paths: "not-an-array" }),
    });
    expect(bad.status).toBe(400);

    const tooMany = await SELF.fetch("https://m.test/api/vfs/manifests", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        paths: Array.from({ length: 257 }, (_, i) => `/p${i}`),
      }),
    });
    expect(tooMany.status).toBe(400);
  });

  it("accepts an empty array (returns empty result)", async () => {
    const apiKey = await seed(
      "manifests-rt-3",
      "/a.txt",
      "a",
      "text/plain"
    );
    const res = await SELF.fetch("https://m.test/api/vfs/manifests", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ paths: [] }),
    });
    expect(res.status).toBe(200);
    const body = (await res.json()) as { manifests: unknown[] };
    expect(body.manifests).toEqual([]);
  });
});
