import { SELF, env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it, vi } from "vitest";

/**
 * The SDK's bounded completion methods against the real server.
 *
 * Parity between the two clients and the cap arithmetic are pinned over a
 * scripted transport in `tests/unit/sdk-bounded-completion.test.ts`. What needs
 * a real server is that the requests those machines issue are ones this server
 * answers, and that its ceilings and the SDK's line up:
 *
 *   - a session one invocation cannot finalize is staged and stepped to a
 *     published file, over the binding RPCs and over the HTTP routes,
 *   - a session that fits keeps costing exactly one finalize request,
 *   - an upload past the completion budget is refused before a chunk hash is
 *     staged, and the bounded pair then finishes that same upload,
 *   - a landed set spread across status pages is reported in full by resume, so
 *     a resumed upload re-PUTs nothing,
 *   - the abort machine is drivable a page at a time, and
 *   - `parallelUpload` round-trips its bytes while driving all of it.
 */

import {
  CompletionBudgetExceededError,
  createMossaicHttpClient,
  createVFS,
  parallelUpload,
  type MossaicEnv,
  type MultipartFinalizeOperation,
  type MultipartUploadHandle,
  type UserDO,
  type VFSBoundedOperationsClient,
} from "../../sdk/src/index";
import { signVFSToken } from "@core/lib/auth";
import { vfsUserDOName } from "@core/lib/utils";
import { hashChunk } from "@shared/crypto";
import { MULTIPART_HASH_PAGE_SIZE } from "@shared/multipart";
import type { EnvCore } from "@shared/types";

// Every wide-pool case fences a 251-shard pool a page at a time, which
// instantiates every shard in it — far more Durable Object cold starts than an
// ordinary test pays for. The wide-pool cases share one tenant so the pool is
// only instantiated once.
vi.setConfig({ testTimeout: 180_000 });

interface TestEnv {
  MOSSAIC_USER: DurableObjectNamespace<UserDO>;
  MOSSAIC_SHARD: DurableObjectNamespace;
  JWT_SECRET?: string;
}

const E = env as unknown as TestEnv;
const NS = "default";
const CHUNK_BYTES = 2;

/**
 * A pool wide enough that finalizing anything at all costs more shard
 * round-trips than one server invocation may spend, which is what puts the
 * session on the paged control plane.
 */
const WIDE_POOL = 251;
const WIDE_TENANT = "mp-bounded-wide";

/** Wide enough to need a second status page, narrow enough to stay cheap. */
const PAGED_STATUS_POOL = 70;

function envFor(): MossaicEnv {
  return {
    MOSSAIC_USER: E.MOSSAIC_USER as MossaicEnv["MOSSAIC_USER"],
    MOSSAIC_SHARD: E.MOSSAIC_SHARD as unknown as MossaicEnv["MOSSAIC_SHARD"],
  };
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

/** An HTTP client that records the route of every request it makes. */
async function httpClientFor(
  tenant: string,
  routes: string[] = []
): Promise<ReturnType<typeof createMossaicHttpClient>> {
  const apiKey = await signVFSToken(env as unknown as EnvCore, {
    ns: NS,
    tenant,
  });
  return createMossaicHttpClient({
    url: "https://mossaic.test",
    apiKey,
    fetcher: (input, init) => {
      const url =
        typeof input === "string"
          ? input
          : input instanceof URL
            ? input.toString()
            : input.url;
      routes.push(new URL(url).pathname);
      return selfFetcher(url, init);
    },
  });
}

/** Pin a tenant's shard pool so the request arithmetic is deterministic. */
async function pinPool(tenant: string, poolSize: number): Promise<void> {
  const stub = E.MOSSAIC_USER.get(
    E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
  );
  await stub.vfsExists({ ns: NS, tenant }, "/");
  await runInDurableObject(stub, (_instance, state: DurableObjectState) => {
    state.storage.sql.exec(
      `INSERT INTO quota (user_id, pool_size) VALUES (?, ?)
         ON CONFLICT(user_id) DO UPDATE SET pool_size = excluded.pool_size`,
      tenant,
      poolSize
    );
  });
}

function chunkBytes(index: number): Uint8Array {
  return new Uint8Array([index & 0xff, (index >> 8) & 0xff]);
}

function sourceBytes(totalChunks: number): Uint8Array {
  const out = new Uint8Array(totalChunks * CHUNK_BYTES);
  for (let index = 0; index < totalChunks; index++) {
    out.set(chunkBytes(index), index * CHUNK_BYTES);
  }
  return out;
}

/** The one method both clients contribute to `uploadChunks`. */
interface ChunkPutter {
  putMultipartChunk(
    handle: MultipartUploadHandle,
    index: number,
    chunk: Uint8Array
  ): Promise<{ chunkHash: string }>;
}

/** PUT every chunk of a session, returning the manifest finalize verifies. */
async function uploadChunks(
  client: ChunkPutter,
  handle: MultipartUploadHandle,
  totalChunks: number
): Promise<string[]> {
  const hashes = new Array<string>(totalChunks);
  const CONCURRENCY = 16;
  for (let base = 0; base < totalChunks; base += CONCURRENCY) {
    await Promise.all(
      Array.from(
        { length: Math.min(CONCURRENCY, totalChunks - base) },
        async (_unused, offset) => {
          const index = base + offset;
          const result = await client.putMultipartChunk(
            handle,
            index,
            chunkBytes(index)
          );
          hashes[index] = result.chunkHash;
        }
      )
    );
  }
  return hashes;
}

describe("SDK bounded completion against the server", () => {
  it("drives a paged finalize over the HTTP routes", async () => {
    await pinPool(WIDE_TENANT, WIDE_POOL);
    const routes: string[] = [];
    const http = await httpClientFor(WIDE_TENANT, routes);
    const handle = await http.beginMultipartUpload("/paged-http.bin", {
      size: CHUNK_BYTES,
      chunkSize: CHUNK_BYTES,
    });
    expect(handle.poolSize).toBe(WIDE_POOL);
    expect(handle.protocolVersion).toBe(2);

    const hashes = await uploadChunks(http, handle, 1);
    const result = await http.finalizeMultipartUpload(handle, hashes);
    expect(result.path).toBe("/paged-http.bin");
    expect(result.size).toBe(CHUNK_BYTES);
    expect(await http.readFile("/paged-http.bin")).toEqual(sourceBytes(1));

    // A session this wide cannot use the one-request route at all, so the
    // paged routes are the ones that carried it.
    expect(routes).toContain("/api/vfs/multipart/hash-page");
    expect(routes).toContain("/api/vfs/multipart/finalize-step");
    expect(routes).not.toContain("/api/vfs/multipart/finalize");
  });

  it("refuses an upload past the budget, then finishes it with the bounded pair", async () => {
    // Two hash pages over a 251-shard pool is knowably past the shared budget,
    // so the refusal lands before a single hash is staged.
    const totalChunks = MULTIPART_HASH_PAGE_SIZE + 1;
    await pinPool(WIDE_TENANT, WIDE_POOL);
    const client = createVFS(envFor(), { tenant: WIDE_TENANT });
    const bounded = client as unknown as VFSBoundedOperationsClient;
    const handle = await client.beginMultipartUpload("/deep.bin", {
      size: totalChunks * CHUNK_BYTES,
      chunkSize: CHUNK_BYTES,
    });
    const hashes = await uploadChunks(client, handle, totalChunks);

    const failure = await client
      .finalizeMultipartUpload(handle, hashes)
      .catch((err: unknown) => err);
    expect(failure).toBeInstanceOf(CompletionBudgetExceededError);
    expect((failure as CompletionBudgetExceededError).code).toBe("EFBIG");
    expect((failure as Error).message).toMatch(
      /drive startFinalizeMultipartUpload\(\) and stepFinalizeMultipartUpload\(\)/
    );

    // Nothing was staged, so the same session finishes through the bounded
    // pair rather than needing a fresh upload.
    let outcome = await bounded.startFinalizeMultipartUpload(handle, hashes);
    while ("operation" in outcome) {
      outcome = await bounded.stepFinalizeMultipartUpload(
        handle,
        hashes,
        outcome.operation as MultipartFinalizeOperation
      );
    }
    expect(outcome).toMatchObject({
      path: "/deep.bin",
      size: totalChunks * CHUNK_BYTES,
    });
    expect(await client.readFile("/deep.bin")).toEqual(
      sourceBytes(totalChunks)
    );
  });

  it("round-trips parallelUpload over the paged control plane", async () => {
    await pinPool(WIDE_TENANT, WIDE_POOL);
    const http = await httpClientFor(WIDE_TENANT);
    const source = sourceBytes(2);
    const result = await parallelUpload(http, "/parallel.bin", source, {
      chunkSize: CHUNK_BYTES,
      concurrency: { initial: 2, max: 4, min: 1 },
    });
    expect(result.size).toBe(source.byteLength);
    expect(await http.readFile("/parallel.bin")).toEqual(source);
  });

  it("keeps a session one invocation can finalize at one request", async () => {
    const routes: string[] = [];
    const http = await httpClientFor("mp-bounded-oneshot", routes);
    const handle = await http.beginMultipartUpload("/small.bin", {
      size: 4 * CHUNK_BYTES,
      chunkSize: CHUNK_BYTES,
    });
    const hashes = await uploadChunks(http, handle, 4);
    await http.finalizeMultipartUpload(handle, hashes);
    expect(
      routes.filter((route) => route === "/api/vfs/multipart/finalize")
    ).toHaveLength(1);
    expect(routes).not.toContain("/api/vfs/multipart/hash-page");
    expect(routes).not.toContain("/api/vfs/multipart/finalize-step");
  });

  it("reports every landed chunk of a paged session on resume", async () => {
    const totalChunks = 200;
    const tenant = "mp-bounded-resume";
    await pinPool(tenant, PAGED_STATUS_POOL);
    const client = createVFS(envFor(), { tenant });
    const bounded = client as unknown as VFSBoundedOperationsClient;
    const handle = await client.beginMultipartUpload("/resume.bin", {
      size: totalChunks * CHUNK_BYTES,
      chunkSize: CHUNK_BYTES,
    });
    await uploadChunks(client, handle, totalChunks);

    // A pool wider than one status shard page cannot answer in one page, so
    // the first page alone is not the landed set.
    const firstPage = await bounded.getMultipartUploadStatusPage(handle);
    expect(firstPage.continuation).toBeTypeOf("string");
    expect(firstPage.landed.length).toBeLessThan(totalChunks);

    const resumed = await bounded.resumeMultipartUpload(handle);
    expect(resumed.landed).toEqual(
      Array.from({ length: totalChunks }, (_unused, index) => index)
    );
    expect(resumed.continuation).toBeUndefined();
    expect(resumed.handle.sessionToken).not.toBe(handle.sessionToken);

    // Every chunk is accounted for, so the resumed upload re-PUTs none of them
    // and finalize still verifies the whole manifest.
    const hashes = await Promise.all(
      resumed.landed.map((index) => hashChunk(chunkBytes(index)))
    );
    await client.finalizeMultipartUpload(resumed.handle, hashes);
    expect(await client.readFile("/resume.bin")).toEqual(
      sourceBytes(totalChunks)
    );
  });

  it("drives the abort machine a page at a time", async () => {
    const tenant = "mp-bounded-abort";
    const routes: string[] = [];
    const http = await httpClientFor(tenant, routes);
    const bounded = http as unknown as VFSBoundedOperationsClient;
    const handle = await http.beginMultipartUpload("/aborted.bin", {
      size: 8 * CHUNK_BYTES,
      chunkSize: CHUNK_BYTES,
    });
    await uploadChunks(http, handle, 4);

    let outcome = await bounded.startAbortMultipartUpload(handle);
    while ("operation" in outcome) {
      outcome = await bounded.stepAbortMultipartUpload(handle, outcome.operation);
    }
    expect(outcome).toEqual({ aborted: true });
    expect(routes).toContain("/api/vfs/multipart/abort-step");
    await expect(http.readFile("/aborted.bin")).rejects.toThrow(/ENOENT/);

    // Idempotent: a session whose abort is already terminal answers without
    // doing anything, so a cleanup path can call this unconditionally.
    await expect(http.abortMultipartUpload(handle)).resolves.toEqual({
      aborted: true,
    });
    await expect(
      bounded.startAbortMultipartUpload(handle)
    ).resolves.toEqual({ aborted: true });
  });
});
