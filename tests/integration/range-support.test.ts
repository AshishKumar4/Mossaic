import { describe, it, expect } from "vitest";
import { SELF, env, runInDurableObject } from "cloudflare:test";
import type { UserDO } from "@app/objects/user/user-do";
import { signVFSToken } from "@core/lib/auth";
import { vfsUserDOName } from "@core/lib/utils";
import { createMossaicHttpClient } from "../../sdk/src/index";

/**
 * Range support — pinning tests for `<video>` / `<audio>` seek.
 *
 * Browsers require 206 Partial Content for media seek/scrub. Without
 * Range support, every seek refetches the whole file (Chrome stalls;
 * Safari refuses to play).
 *
 * Surfaces under test:
 *   - /api/vfs/chunk/:fileId/:idx (multipart-routes.ts) — per-chunk
 *     Range. The cache stores the FULL 200; Range hits slice it.
 *
 * Cases:
 *   RG1 — bytes=0-99 → 206 with first 100 bytes
 *   RG2 — bytes=100-200 → 206 with the middle slice
 *   RG3 — bytes=-50 (suffix) → 206 with last 50 bytes
 *   RG4 — no Range header → 200 with full body + Accept-Ranges
 *   RG5 — bytes=99999-99999 (out of range) → 416
 *   RG6 — second hit (cached) → still honours Range, no extra
 *         ShardDO RPC
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

/** Seed a chunked file and return the bearer + manifest + shard hint. */
async function seedChunkedFile(
  tenant: string,
  path: string,
  chunkBytes: Uint8Array
): Promise<{
  bearer: string;
  fileId: string;
  chunkHash: string;
  chunkIndex: number;
  shardIndex: number;
  downloadToken: string;
}> {
  const bearer = await mint(tenant);
  const vfs = createMossaicHttpClient({
    url: "https://m.test",
    apiKey: bearer,
    fetcher: selfFetcher,
  });
  // Use writeFile to land the bytes in the chunked tier (>INLINE_LIMIT).
  await vfs.writeFile(path, chunkBytes, {
    mimeType: "application/octet-stream",
  });

  // Mint a download token (returns the manifest).
  const tokRes = await SELF.fetch(
    "https://m.test/api/vfs/multipart/download-token",
    {
      method: "POST",
      headers: {
        Authorization: `Bearer ${bearer}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ path }),
    }
  );
  expect(tokRes.status).toBe(200);
  const tok = (await tokRes.json()) as {
    token: string;
    manifest: { fileId: string; chunks: { index: number; hash: string }[] };
  };
  expect(tok.manifest.chunks.length).toBeGreaterThanOrEqual(1);
  const ch0 = tok.manifest.chunks[0];

  // Resolve the shard_index hint from file_chunks (the chunk-download
  // route needs ?shard= because manifest doesn't carry it).
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

  return {
    bearer,
    fileId: tok.manifest.fileId,
    chunkHash: ch0.hash,
    chunkIndex: ch0.index,
    shardIndex,
    downloadToken: tok.token,
  };
}

function chunkUrl(opts: {
  fileId: string;
  idx: number;
  hash: string;
  shard: number;
}): string {
  const u = new URL(
    `https://m.test/api/vfs/chunk/${encodeURIComponent(opts.fileId)}/${opts.idx}`
  );
  u.searchParams.set("hash", opts.hash);
  u.searchParams.set("shard", String(opts.shard));
  return u.toString();
}

describe("Range support — /api/vfs/chunk/:fileId/:idx", () => {
  it("RG1 — bytes=0-99 returns 206 with first 100 bytes", async () => {
    const payload = new Uint8Array(20_000);
    for (let i = 0; i < payload.length; i++) payload[i] = i & 0xff;
    const seed = await seedChunkedFile("rg1-tenant", "/v.bin", payload);

    const res = await SELF.fetch(
      chunkUrl({
        fileId: seed.fileId,
        idx: seed.chunkIndex,
        hash: seed.chunkHash,
        shard: seed.shardIndex,
      }),
      {
        method: "GET",
        headers: {
          Authorization: `Bearer ${seed.downloadToken}`,
          Range: "bytes=0-99",
        },
      }
    );
    expect(res.status).toBe(206);
    const range = res.headers.get("Content-Range");
    expect(range).not.toBeNull();
    expect(range!.startsWith("bytes 0-99/")).toBe(true);
    expect(res.headers.get("Content-Length")).toBe("100");
    const body = new Uint8Array(await res.arrayBuffer());
    expect(body.byteLength).toBe(100);
    // First byte is 0, last byte is 99 (per the seed pattern).
    expect(body[0]).toBe(0);
    expect(body[99]).toBe(99);
  });

  it("RG2 — bytes=100-200 returns 206 with the middle slice", async () => {
    const payload = new Uint8Array(20_000);
    for (let i = 0; i < payload.length; i++) payload[i] = i & 0xff;
    const seed = await seedChunkedFile("rg2-tenant", "/v.bin", payload);

    const res = await SELF.fetch(
      chunkUrl({
        fileId: seed.fileId,
        idx: seed.chunkIndex,
        hash: seed.chunkHash,
        shard: seed.shardIndex,
      }),
      {
        method: "GET",
        headers: {
          Authorization: `Bearer ${seed.downloadToken}`,
          Range: "bytes=100-200",
        },
      }
    );
    expect(res.status).toBe(206);
    expect(res.headers.get("Content-Length")).toBe("101");
    const body = new Uint8Array(await res.arrayBuffer());
    expect(body[0]).toBe(100);
    expect(body[100]).toBe(200);
  });

  it("RG3 — bytes=-50 returns 206 with last 50 bytes", async () => {
    const payload = new Uint8Array(20_000);
    for (let i = 0; i < payload.length; i++) payload[i] = (i * 7) & 0xff;
    const seed = await seedChunkedFile("rg3-tenant", "/v.bin", payload);

    // Read the chunk size first (it may be < payload.length when the
    // server sub-divided into multiple chunks — but with 20KB and
    // default 1MB chunkSize, we get exactly 1 chunk).
    const fullRes = await SELF.fetch(
      chunkUrl({
        fileId: seed.fileId,
        idx: seed.chunkIndex,
        hash: seed.chunkHash,
        shard: seed.shardIndex,
      }),
      {
        method: "GET",
        headers: { Authorization: `Bearer ${seed.downloadToken}` },
      }
    );
    expect(fullRes.status).toBe(200);
    const total = Number(fullRes.headers.get("Content-Length"));
    expect(total).toBeGreaterThan(0);
    await fullRes.arrayBuffer();

    const res = await SELF.fetch(
      chunkUrl({
        fileId: seed.fileId,
        idx: seed.chunkIndex,
        hash: seed.chunkHash,
        shard: seed.shardIndex,
      }),
      {
        method: "GET",
        headers: {
          Authorization: `Bearer ${seed.downloadToken}`,
          Range: "bytes=-50",
        },
      }
    );
    expect(res.status).toBe(206);
    expect(res.headers.get("Content-Length")).toBe("50");
    const range = res.headers.get("Content-Range");
    expect(range).not.toBeNull();
    expect(range!).toBe(`bytes ${total - 50}-${total - 1}/${total}`);
  });

  it("RG4 — no Range header returns 200 with full body + Accept-Ranges", async () => {
    const payload = new Uint8Array(20_000).fill(0xab);
    const seed = await seedChunkedFile("rg4-tenant", "/v.bin", payload);

    const res = await SELF.fetch(
      chunkUrl({
        fileId: seed.fileId,
        idx: seed.chunkIndex,
        hash: seed.chunkHash,
        shard: seed.shardIndex,
      }),
      {
        method: "GET",
        headers: { Authorization: `Bearer ${seed.downloadToken}` },
      }
    );
    expect(res.status).toBe(200);
    expect(res.headers.get("Accept-Ranges")).toBe("bytes");
    const body = new Uint8Array(await res.arrayBuffer());
    expect(body.byteLength).toBeGreaterThan(0);
    // First byte equals the seed pattern.
    expect(body[0]).toBe(0xab);
  });

  it("RG5 — bytes=99999999-99999999 (out of range) returns 416", async () => {
    const payload = new Uint8Array(20_000).fill(0xcd);
    const seed = await seedChunkedFile("rg5-tenant", "/v.bin", payload);

    const res = await SELF.fetch(
      chunkUrl({
        fileId: seed.fileId,
        idx: seed.chunkIndex,
        hash: seed.chunkHash,
        shard: seed.shardIndex,
      }),
      {
        method: "GET",
        headers: {
          Authorization: `Bearer ${seed.downloadToken}`,
          Range: "bytes=99999999-99999999",
        },
      }
    );
    expect(res.status).toBe(416);
    const range = res.headers.get("Content-Range");
    expect(range).not.toBeNull();
    // 416 form: bytes <asterisk>/<total>
    expect(range!.startsWith("bytes */")).toBe(true);
  });

  it("RG6 — second Range hit serves correct slice from cached buffer", async () => {
    const payload = new Uint8Array(20_000);
    for (let i = 0; i < payload.length; i++) payload[i] = i & 0xff;
    const seed = await seedChunkedFile("rg6-tenant", "/v.bin", payload);

    // First hit (no Range) — populates the cache with the full 200.
    const cold = await SELF.fetch(
      chunkUrl({
        fileId: seed.fileId,
        idx: seed.chunkIndex,
        hash: seed.chunkHash,
        shard: seed.shardIndex,
      }),
      {
        method: "GET",
        headers: { Authorization: `Bearer ${seed.downloadToken}` },
      }
    );
    expect(cold.status).toBe(200);
    await cold.arrayBuffer();

    // Second hit with Range — should slice the cached body.
    const warm = await SELF.fetch(
      chunkUrl({
        fileId: seed.fileId,
        idx: seed.chunkIndex,
        hash: seed.chunkHash,
        shard: seed.shardIndex,
      }),
      {
        method: "GET",
        headers: {
          Authorization: `Bearer ${seed.downloadToken}`,
          Range: "bytes=10-19",
        },
      }
    );
    expect(warm.status).toBe(206);
    expect(warm.headers.get("Content-Length")).toBe("10");
    const body = new Uint8Array(await warm.arrayBuffer());
    expect(body.byteLength).toBe(10);
    expect(body[0]).toBe(10);
    expect(body[9]).toBe(19);
  });
});
