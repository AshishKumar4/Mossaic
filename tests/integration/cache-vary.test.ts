import { describe, it, expect } from "vitest";
import { SELF, env, runInDurableObject } from "cloudflare:test";
import type { UserDO } from "@app/objects/user/user-do";
import { signVFSToken } from "@core/lib/auth";
import { vfsUserDOName } from "@core/lib/utils";
import { createMossaicHttpClient } from "../../sdk/src/index";

/**
 * Phase 41 Fix 2 — Vary: Authorization on every cached authed
 * response (audit 40B P1).
 *
 * Background: the public-cached responses on
 *   - /api/vfs/readChunk           (vfs.ts:835)
 *   - /api/vfs/readPreview         (vfs-preview.ts:182, 197)
 *   - /api/vfs/multipart/chunk-... (multipart-routes.ts:662)
 * carry `Cache-Control: public, max-age=31536000, immutable` so an
 * intermediary CDN (Cloudflare's Workers Cache, or any caching proxy
 * upstream) can hold the response. Each response is keyed by a
 * per-tenant URL namespace inside the Workers Cache, but a downstream
 * CDN doesn't see that key — it sees the same URL across tenants
 * differing only by the Bearer token in `Authorization`.
 *
 * Without `Vary: Authorization` on the response, a downstream cache
 * could serve a tenant-A response to a tenant-B request whose URL
 * collides. Phase 41 Fix 2 adds the header.
 *
 * Four pinning tests:
 *   CV1 — readPreview 200 includes `Vary: Authorization`
 *   CV2 — readPreview 304 (If-None-Match revalidation) ALSO includes it
 *   CV3 — readChunk 200 includes it
 *   CV4 — multipart/chunk-download 200 includes it
 */

interface E {
  MOSSAIC_USER: DurableObjectNamespace<UserDO>;
  MOSSAIC_SHARD: DurableObjectNamespace;
  JWT_SECRET?: string;
}
const TEST_ENV = env as unknown as E;

async function mint(tenant: string): Promise<string> {
  return signVFSToken(TEST_ENV as never, { ns: "default", tenant });
}

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

/**
 * Seed a small file via the SDK's HTTP client, returning the Bearer
 * token used. The test's subsequent assertions read responses
 * directly via SELF.fetch so we can inspect the raw `Vary` header.
 */
async function seed(
  tenant: string,
  path: string,
  bytes: Uint8Array,
  mime: string
): Promise<string> {
  const apiKey = await mint(tenant);
  const vfs = createMossaicHttpClient({
    url: "https://m.test",
    apiKey,
    fetcher: selfFetcher,
  });
  await vfs.writeFile(path, bytes, { mimeType: mime });
  return apiKey;
}

/**
 * Cloudflare's Cache API normalises `Vary` to a comma-separated
 * lowercase list. We assert membership of "authorization" (case-
 * insensitive) so the test passes regardless of header
 * canonicalisation.
 */
function expectVaryAuthorization(res: Response): void {
  const v = res.headers.get("Vary");
  expect(v).not.toBeNull();
  const tokens = (v ?? "")
    .split(",")
    .map((s) => s.trim().toLowerCase())
    .filter((s) => s.length > 0);
  expect(tokens).toContain("authorization");
}

describe("Phase 41 Fix 2 — Vary: Authorization on cached authed responses", () => {
  it("CV1 — readPreview 200 carries Vary: Authorization", async () => {
    const apiKey = await seed(
      "cv1-tenant",
      "/code.ts",
      new TextEncoder().encode("function f(){ return 42; }\n"),
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
    // Sanity: it's a real cacheable response.
    const cc = res.headers.get("Cache-Control") ?? "";
    expect(cc).toContain("public");
    // The load-bearing assertion: Vary on Authorization.
    expectVaryAuthorization(res);
    // Drain to avoid stream leak.
    await res.arrayBuffer();
  });

  it("CV2 — readPreview 304 (If-None-Match) also carries Vary: Authorization", async () => {
    const apiKey = await seed(
      "cv2-tenant",
      "/c.ts",
      new TextEncoder().encode("const x = 1;\n"),
      "text/typescript"
    );
    // Cold call → 200 with ETag.
    const cold = await SELF.fetch("https://m.test/api/vfs/readPreview", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ path: "/c.ts", variant: "thumb" }),
    });
    expect(cold.status).toBe(200);
    const etag = cold.headers.get("ETag");
    expect(etag).not.toBeNull();
    await cold.arrayBuffer();

    // Re-fetch with If-None-Match → 304.
    const warm = await SELF.fetch("https://m.test/api/vfs/readPreview", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
        "If-None-Match": etag!,
      },
      body: JSON.stringify({ path: "/c.ts", variant: "thumb" }),
    });
    expect(warm.status).toBe(304);
    expectVaryAuthorization(warm);
  });

  it("CV3 — readChunk 200 carries Vary: Authorization", async () => {
    // readChunk requires a chunked file (>INLINE_LIMIT). Seed a 32 KB
    // payload so the file lives in file_chunks rather than inline_data.
    const apiKey = await seed(
      "cv3-tenant",
      "/big.bin",
      new Uint8Array(32 * 1024).fill(0x5a),
      "application/octet-stream"
    );
    const res = await SELF.fetch("https://m.test/api/vfs/readChunk", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ path: "/big.bin", chunkIndex: 0 }),
    });
    expect(res.status).toBe(200);
    const cc = res.headers.get("Cache-Control") ?? "";
    expect(cc).toContain("public");
    expectVaryAuthorization(res);
    await res.arrayBuffer();
  });

  it("CV4 — multipart/chunk-download 200 carries Vary: Authorization", async () => {
    const tenant = "cv4-tenant";
    // Seed a 2-chunk file via the multipart pipeline so we can mint
    // a download token and exercise the cacheable chunk-download
    // route.
    const bearer = await mint(tenant);
    const beginRes = await SELF.fetch(
      "https://m.test/api/vfs/multipart/begin",
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${bearer}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          path: "/dl.bin",
          size: 200,
          chunkSize: 100,
        }),
      }
    );
    expect(beginRes.status).toBe(200);
    const begin = (await beginRes.json()) as {
      uploadId: string;
      sessionToken: string;
    };
    // Two chunks of 100 bytes each.
    const c0 = new Uint8Array(100).fill(0xa1);
    const c1 = new Uint8Array(100).fill(0xa2);
    async function putChunk(idx: number, bytes: Uint8Array): Promise<string> {
      const r = await SELF.fetch(
        `https://m.test/api/vfs/multipart/${encodeURIComponent(begin.uploadId)}/chunk/${idx}`,
        {
          method: "PUT",
          headers: {
            Authorization: `Bearer ${bearer}`,
            "X-Session-Token": begin.sessionToken,
            "Content-Type": "application/octet-stream",
            "Content-Length": String(bytes.byteLength),
          },
          body: bytes,
        }
      );
      expect(r.status).toBe(200);
      return ((await r.json()) as { hash: string }).hash;
    }
    const h0 = await putChunk(0, c0);
    const h1 = await putChunk(1, c1);
    const finRes = await SELF.fetch(
      "https://m.test/api/vfs/multipart/finalize",
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${bearer}`,
          "X-Session-Token": begin.sessionToken,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          uploadId: begin.uploadId,
          chunkHashList: [h0, h1],
        }),
      }
    );
    expect(finRes.status).toBe(200);

    // Mint a download token.
    const tokRes = await SELF.fetch(
      "https://m.test/api/vfs/multipart/download-token",
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${bearer}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ path: "/dl.bin" }),
      }
    );
    expect(tokRes.status).toBe(200);
    const tok = (await tokRes.json()) as {
      token: string;
      manifest: { fileId: string; chunks: { index: number; hash: string }[] };
    };
    expect(tok.manifest.chunks.length).toBe(2);

    // Look up the shard_index for chunk 0 from file_chunks; the
    // chunk-download route needs it as a query hint (the contract is
    // "caller already has the manifest from /download-token, so
    // hand back the shard index so we don't pay a UserDO RPC").
    const shardIndex = await runInDurableObject(
      TEST_ENV.MOSSAIC_USER.get(
        TEST_ENV.MOSSAIC_USER.idFromName(vfsUserDOName("default", tenant))
      ),
      (_inst, state) => {
        const row = state.storage.sql
          .exec(
            "SELECT shard_index FROM file_chunks WHERE file_id = ? AND chunk_index = 0",
            tok.manifest.fileId
          )
          .toArray()[0] as { shard_index: number } | undefined;
        return row?.shard_index ?? -1;
      }
    );
    expect(shardIndex).toBeGreaterThanOrEqual(0);

    // Hit chunk-download. Route is mounted at /api/vfs/chunk/:fileId/:idx.
    // Auth via Bearer + ?hash= and ?shard= query params (per
    // multipart-routes:567-583).
    const dlUrl = new URL(
      `https://m.test/api/vfs/chunk/${encodeURIComponent(tok.manifest.fileId)}/${tok.manifest.chunks[0].index}`
    );
    dlUrl.searchParams.set("hash", tok.manifest.chunks[0].hash);
    dlUrl.searchParams.set("shard", String(shardIndex));
    const dlRes = await SELF.fetch(dlUrl.toString(), {
      method: "GET",
      headers: {
        Authorization: `Bearer ${tok.token}`,
      },
    });
    expect(dlRes.status).toBe(200);
    const cc = dlRes.headers.get("Cache-Control") ?? "";
    expect(cc).toContain("public");
    expectVaryAuthorization(dlRes);
    await dlRes.arrayBuffer();
  });
});
