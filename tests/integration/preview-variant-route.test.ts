import { describe, it, expect } from "vitest";
import { SELF, env } from "cloudflare:test";
import { signVFSToken } from "@core/lib/auth";
import { createMossaicHttpClient } from "../../sdk/src/index";

/**
 * Phase 45 \u2014 signed preview-variant route end-to-end test.
 *
 * The new flow:
 *   1. Auth-gated mint via POST /api/vfs/previewInfo \u2192 returns a
 *      signed URL (`/api/vfs/preview-variant/<token>`).
 *   2. Browser-equivalent GET on the signed URL \u2014 NO Bearer
 *      header. The token IS the auth.
 *   3. The route returns variant bytes with
 *      Cache-Control: public, max-age=31536000, immutable
 *      and ETag: W/"<contentHash>".
 *
 * Cases:
 *   PV1 mint signed URL for an image-bearing path; URL
 *       has the expected `/api/vfs/preview-variant/<token>`
 *       prefix and contains a JWT-shaped token (3 dot-separated
 *       parts, base64url-ish).
 *   PV2 GET on the signed URL returns 200 with bytes + ETag
 *       matching contentHash.
 *   PV3 second GET (warm cache) returns the same response
 *       \u2014 idempotent serving.
 *   PV4 If-None-Match: W/"<contentHash>" returns 304 with no body.
 *   PV5 invalid/tampered token returns 401.
 *   PV6 batched previewInfoMany returns one entry per path; per-
 *       path failures land as `ok: false` rather than 4xx.
 */

interface E {
  MOSSAIC_USER: DurableObjectNamespace;
  MOSSAIC_SHARD: DurableObjectNamespace;
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

async function mintApiKey(ns: string, tenant: string): Promise<string> {
  return signVFSToken(TEST_ENV, { ns, tenant });
}

async function seedImage(
  tenant: string,
  path: string,
  bytes: Uint8Array,
  mime: string
): Promise<string> {
  const apiKey = await mintApiKey("default", tenant);
  const vfs = createMossaicHttpClient({
    url: "https://mossaic.test",
    apiKey,
    fetcher: selfFetcher,
  });
  await vfs.writeFile(path, bytes, { mimeType: mime });
  return apiKey;
}

// 1x1 transparent PNG byte stream. Small enough to flow inline;
// real enough to trigger the icon-card / image-passthrough renderer.
const TINY_PNG = new Uint8Array([
  0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0x00, 0x00, 0x0d,
  0x49, 0x48, 0x44, 0x52, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01,
  0x08, 0x06, 0x00, 0x00, 0x00, 0x1f, 0x15, 0xc4, 0x89, 0x00, 0x00, 0x00,
  0x0d, 0x49, 0x44, 0x41, 0x54, 0x78, 0x9c, 0x62, 0x00, 0x01, 0x00, 0x00,
  0x05, 0x00, 0x01, 0x0d, 0x0a, 0x2d, 0xb4, 0x00, 0x00, 0x00, 0x00, 0x49,
  0x45, 0x4e, 0x44, 0xae, 0x42, 0x60, 0x82,
]);

describe("Phase 45 \u2014 signed preview-variant route", () => {
  it("PV1 \u2014 mint signed URL with JWT-shaped token", async () => {
    const tenant = "pv1-mint";
    const apiKey = await seedImage(
      tenant,
      "/img.png",
      TINY_PNG,
      "image/png"
    );
    const res = await selfFetcher("https://mossaic.test/api/vfs/previewInfo", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ path: "/img.png" }),
    });
    expect(res.status).toBe(200);
    const info = (await res.json()) as {
      url: string;
      token: string;
      etag: string;
      mimeType: string;
      contentHash: string;
    };
    expect(info.url).toMatch(/^\/api\/vfs\/preview-variant\/[A-Za-z0-9_\-.]+$/);
    expect(info.token.split(".").length).toBe(3);
    expect(info.etag).toMatch(/^W\/"[0-9a-f]{64}"$/);
    expect(info.contentHash).toMatch(/^[0-9a-f]{64}$/);
    expect(info.etag).toBe(`W/"${info.contentHash}"`);
  });

  it("PV2 \u2014 GET signed URL returns bytes + ETag", async () => {
    const tenant = "pv2-fetch";
    const apiKey = await seedImage(
      tenant,
      "/photo.png",
      TINY_PNG,
      "image/png"
    );
    const mintRes = await selfFetcher(
      "https://mossaic.test/api/vfs/previewInfo",
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${apiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ path: "/photo.png" }),
      }
    );
    const info = (await mintRes.json()) as {
      url: string;
      etag: string;
      contentHash: string;
    };
    // Critical \u2014 NO Authorization header; the token in the URL is the auth.
    const fetchRes = await selfFetcher(`https://mossaic.test${info.url}`);
    expect(fetchRes.status).toBe(200);
    expect(fetchRes.headers.get("ETag")).toBe(info.etag);
    expect(fetchRes.headers.get("Cache-Control")).toBe(
      "public, max-age=31536000, immutable"
    );
    const bytes = new Uint8Array(await fetchRes.arrayBuffer());
    expect(bytes.byteLength).toBeGreaterThan(0);
  });

  it("PV3 \u2014 second GET serves the same response (warm cache or re-fetch)", async () => {
    const tenant = "pv3-warm";
    const apiKey = await seedImage(
      tenant,
      "/warm.png",
      TINY_PNG,
      "image/png"
    );
    const mintRes = await selfFetcher(
      "https://mossaic.test/api/vfs/previewInfo",
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${apiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ path: "/warm.png" }),
      }
    );
    const info = (await mintRes.json()) as { url: string; etag: string };
    const r1 = await selfFetcher(`https://mossaic.test${info.url}`);
    const b1 = new Uint8Array(await r1.arrayBuffer());
    const r2 = await selfFetcher(`https://mossaic.test${info.url}`);
    const b2 = new Uint8Array(await r2.arrayBuffer());
    expect(r1.status).toBe(200);
    expect(r2.status).toBe(200);
    expect(r1.headers.get("ETag")).toBe(r2.headers.get("ETag"));
    expect(b1.byteLength).toBe(b2.byteLength);
  });

  it("PV4 \u2014 If-None-Match returns 304 with no body", async () => {
    const tenant = "pv4-revalidate";
    const apiKey = await seedImage(
      tenant,
      "/r.png",
      TINY_PNG,
      "image/png"
    );
    const mintRes = await selfFetcher(
      "https://mossaic.test/api/vfs/previewInfo",
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${apiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ path: "/r.png" }),
      }
    );
    const info = (await mintRes.json()) as { url: string; etag: string };
    const res = await selfFetcher(`https://mossaic.test${info.url}`, {
      headers: { "If-None-Match": info.etag },
    });
    expect(res.status).toBe(304);
    expect(res.headers.get("ETag")).toBe(info.etag);
    const buf = await res.arrayBuffer();
    expect(buf.byteLength).toBe(0);
  });

  it("PV5 \u2014 tampered token returns 401", async () => {
    const tenant = "pv5-tamper";
    const apiKey = await seedImage(
      tenant,
      "/secret.png",
      TINY_PNG,
      "image/png"
    );
    const mintRes = await selfFetcher(
      "https://mossaic.test/api/vfs/previewInfo",
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${apiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ path: "/secret.png" }),
      }
    );
    const info = (await mintRes.json()) as { url: string };
    // Mangle the token's payload portion. JWTs are
    // header.payload.signature; flip a byte in the middle segment
    // so the signature no longer verifies.
    const parts = info.url.split("/").pop()!.split(".");
    const tamperedPayload = parts[1].slice(0, -2) + "AA";
    const tamperedToken = [parts[0], tamperedPayload, parts[2]].join(".");
    const tamperedUrl = `/api/vfs/preview-variant/${tamperedToken}`;
    const res = await selfFetcher(`https://mossaic.test${tamperedUrl}`);
    expect(res.status).toBe(401);
    const body = (await res.json()) as { code: string };
    expect(body.code).toBe("EACCES");
  });

  it("PV6 \u2014 previewInfoMany returns per-path entries", async () => {
    const tenant = "pv6-batch";
    const apiKey = await seedImage(
      tenant,
      "/a.png",
      TINY_PNG,
      "image/png"
    );
    await seedImage(tenant, "/b.png", TINY_PNG, "image/png");
    // include one path that doesn't exist
    const res = await selfFetcher(
      "https://mossaic.test/api/vfs/previewInfoMany",
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${apiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          paths: ["/a.png", "/missing.png", "/b.png"],
        }),
      }
    );
    expect(res.status).toBe(200);
    const body = (await res.json()) as {
      results: Array<
        | { path: string; ok: true; info: { url: string } }
        | { path: string; ok: false; code: string }
      >;
    };
    expect(body.results).toHaveLength(3);
    expect(body.results[0].ok).toBe(true);
    expect(body.results[1].ok).toBe(false);
    expect(body.results[2].ok).toBe(true);
    if (body.results[1].ok === false) {
      expect(body.results[1].code).toBe("ENOENT");
    }
  });
});
