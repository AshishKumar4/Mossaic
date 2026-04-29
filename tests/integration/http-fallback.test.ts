import { describe, it, expect } from "vitest";
import { SELF, env } from "cloudflare:test";

/**
 * Phase 7 — HTTP fallback acceptance gate.
 *
 * Drives the Phase 7 HTTP fallback (worker/routes/vfs.ts) end-to-end:
 *   1. Operator-side: mint a VFS token via signVFSToken (Phase 4).
 *   2. Consumer-side: createMossaicHttpClient({ url, apiKey: token })
 *      with a fetcher that routes through `SELF.fetch` (the
 *      vitest-pool-workers in-process Worker — same code path that
 *      production runs).
 *   3. Round-trip: writeFile → readFile (bytes match), readdir,
 *      stat, mkdir, exists, unlink.
 *   4. Tenant-isolation guard: a token for tenant A cannot read
 *      tenant B's data even when both are in the same DO namespace.
 *   5. Bad-token guards: missing / malformed / wrong-scope rejected
 *      with 401.
 *   6. Error mapping: server-side ENOENT → consumer's ENOENT subclass.
 */

import {
  createMossaicHttpClient,
  ENOENT,
  EACCES,
  VFSFsError,
} from "../../sdk/src/index";
import { signVFSToken, signJWT } from "@core/lib/auth";

interface E {
  MOSSAIC_USER: DurableObjectNamespace;
  JWT_SECRET?: string;
}
const TEST_ENV = env as unknown as E;

/**
 * Build a fetcher that rewrites any URL onto SELF.fetch (the
 * vitest-pool-workers in-process Worker). The HTTP client uses a
 * configurable base URL; we feed it any URL we like and route to
 * SELF here.
 */
const selfFetcher: typeof fetch = ((input: RequestInfo | URL, init?: RequestInit) => {
  const url = typeof input === "string"
    ? input
    : input instanceof URL
    ? input.toString()
    : input.url;
  return SELF.fetch(url, init);
}) as typeof fetch;

async function mintToken(
  ns: string,
  tenant: string,
  sub?: string
): Promise<string> {
  return signVFSToken(TEST_ENV, { ns, tenant, sub });
}

async function client(ns: string, tenant: string, sub?: string) {
  const apiKey = await mintToken(ns, tenant, sub);
  return createMossaicHttpClient({
    url: "https://mossaic.test",
    apiKey,
    fetcher: selfFetcher,
  });
}

describe("HTTP fallback round-trip via /api/vfs", () => {
  it("writeFile → readFile bytes match (octet-stream path)", async () => {
    const vfs = await client("default", "http-roundtrip");
    const payload = new TextEncoder().encode("hello over HTTP");
    await vfs.writeFile("/h.txt", payload);
    const back = await vfs.readFile("/h.txt");
    expect(back.byteLength).toBe(payload.byteLength);
    expect(new TextDecoder().decode(back)).toBe("hello over HTTP");
  });

  it("writeFile (string) + readFile { encoding: 'utf8' } JSON path", async () => {
    const vfs = await client("default", "http-string");
    await vfs.writeFile("/s.txt", "string-payload");
    const back = await vfs.readFile("/s.txt", { encoding: "utf8" });
    expect(back).toBe("string-payload");
  });

  it("readdir, stat, mkdir, exists, unlink full lifecycle", async () => {
    const vfs = await client("default", "http-fs-ops");
    await vfs.mkdir("/d", { recursive: true });
    await vfs.writeFile("/d/a.txt", "alpha");
    await vfs.writeFile("/d/b.txt", "beta!");

    const entries = await vfs.readdir("/d");
    expect(entries.sort()).toEqual(["a.txt", "b.txt"]);

    const stat = await vfs.stat("/d/a.txt");
    expect(stat.isFile()).toBe(true);
    expect(stat.size).toBe(5);

    expect(await vfs.exists("/d/a.txt")).toBe(true);
    expect(await vfs.exists("/d/missing.txt")).toBe(false);

    await vfs.unlink("/d/a.txt");
    expect(await vfs.exists("/d/a.txt")).toBe(false);
  });

  it("rename + chmod + symlink + readlink + lstat", async () => {
    const vfs = await client("default", "http-misc");
    await vfs.writeFile("/orig.txt", "orig");
    await vfs.rename("/orig.txt", "/renamed.txt");
    expect(await vfs.exists("/orig.txt")).toBe(false);
    expect(await vfs.exists("/renamed.txt")).toBe(true);

    await vfs.chmod("/renamed.txt", 0o600);
    const s1 = await vfs.stat("/renamed.txt");
    expect(s1.mode).toBe(0o600);

    await vfs.symlink("/renamed.txt", "/lk");
    const ls = await vfs.lstat("/lk");
    expect(ls.isSymbolicLink()).toBe(true);
    const target = await vfs.readlink("/lk");
    expect(target).toBe("/renamed.txt");
    // stat follows the symlink.
    const fol = await vfs.stat("/lk");
    expect(fol.isFile()).toBe(true);
  });

  it("readManyStat batches misses + hits", async () => {
    const vfs = await client("default", "http-many-stat");
    await vfs.writeFile("/a.txt", "a");
    await vfs.writeFile("/b.txt", "b");
    const stats = await vfs.readManyStat([
      "/a.txt",
      "/missing",
      "/b.txt",
    ]);
    expect(stats).toHaveLength(3);
    expect(stats[0]?.isFile()).toBe(true);
    expect(stats[1]).toBeNull();
    expect(stats[2]?.isFile()).toBe(true);
  });

  it("rmdir + removeRecursive", async () => {
    const vfs = await client("default", "http-rm");
    await vfs.mkdir("/work/sub", { recursive: true });
    await vfs.writeFile("/work/sub/x.txt", "x");
    await vfs.writeFile("/work/y.txt", "y");

    // rmdir on non-empty → ENOTEMPTY (audit H2 fix; matches POSIX).
    let caught: VFSFsError | null = null;
    try {
      await vfs.rmdir("/work");
    } catch (e) {
      caught = e as VFSFsError;
    }
    expect(caught).toBeInstanceOf(VFSFsError);
    expect(caught?.code).toBe("ENOTEMPTY");

    await vfs.removeRecursive("/work");
    expect(await vfs.exists("/work")).toBe(false);
  });

  it("openManifest + readChunk for chunked files", async () => {
    const vfs = await client("default", "http-manifest");
    // Use createMossaicHttpClient → no streams. Use writeFile with
    // a >INLINE_LIMIT payload to force chunked tier.
    const big = new Uint8Array(20 * 1024).fill(0xab);
    await vfs.writeFile("/big.bin", big);

    const m = await vfs.openManifest("/big.bin");
    expect(m.inlined).toBe(false);
    expect(m.size).toBe(big.byteLength);
    expect(m.chunks.length).toBeGreaterThan(0);

    const c0 = await vfs.readChunk("/big.bin", 0);
    expect(c0.byteLength).toBeGreaterThan(0);
    expect(c0[0]).toBe(0xab);
  });
});

describe("HTTP fallback security / scope guards", () => {
  it("token tenant=A cannot read tenant B's data", async () => {
    const vfsA = await client("default", "iso-A");
    const vfsB = await client("default", "iso-B");
    await vfsA.writeFile("/secret.txt", "alpha-only");

    // B's token, asking for the same path, must miss — different DO
    // instance per (ns, tenant) under the hood.
    expect(await vfsB.exists("/secret.txt")).toBe(false);
    let caught: VFSFsError | null = null;
    try {
      await vfsB.readFile("/secret.txt");
    } catch (e) {
      caught = e as VFSFsError;
    }
    expect(caught).toBeInstanceOf(ENOENT);
  });

  it("namespace separation: ns=staging cannot reach ns=prod data for the same tenant", async () => {
    const tenant = "ns-iso";
    const prod = await client("prod", tenant);
    const staging = await client("staging", tenant);
    await prod.writeFile("/x.txt", "p");
    expect(await staging.exists("/x.txt")).toBe(false);
  });

  it("missing Authorization header → 401", async () => {
    const res = await SELF.fetch("https://mossaic.test/api/vfs/exists", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ path: "/" }),
    });
    expect(res.status).toBe(401);
    const body = (await res.json()) as { code: string };
    expect(body.code).toBe("EACCES");
  });

  it("malformed Authorization header → 401", async () => {
    const res = await SELF.fetch("https://mossaic.test/api/vfs/exists", {
      method: "POST",
      headers: {
        Authorization: "NotBearer something",
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ path: "/" }),
    });
    expect(res.status).toBe(401);
  });

  it("legacy login JWT (no scope claim) is rejected → 401", async () => {
    // Mint a legacy login JWT (signJWT — has email, no scope claim).
    // The HTTP fallback must reject it.
    const legacyToken = await signJWT(TEST_ENV, {
      userId: "fake-user-id",
      email: "u@example.com",
    });
    const res = await SELF.fetch("https://mossaic.test/api/vfs/exists", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${legacyToken}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ path: "/" }),
    });
    expect(res.status).toBe(401);
  });

  it("garbage / tampered token → 401", async () => {
    const res = await SELF.fetch("https://mossaic.test/api/vfs/exists", {
      method: "POST",
      headers: {
        Authorization: "Bearer not.a.realjwt",
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ path: "/" }),
    });
    expect(res.status).toBe(401);
  });

  it("HTTP client rejects empty url / apiKey at construction", () => {
    expect(() =>
      createMossaicHttpClient({ url: "", apiKey: "k" })
    ).toThrow(/EINVAL/);
    expect(() =>
      createMossaicHttpClient({ url: "https://x", apiKey: "" })
    ).toThrow(/EINVAL/);
  });

  it("ENOENT error from server maps to typed ENOENT subclass on client", async () => {
    const vfs = await client("default", "http-err");
    let caught: unknown = null;
    try {
      await vfs.readFile("/nope");
    } catch (e) {
      caught = e;
    }
    expect(caught).toBeInstanceOf(ENOENT);
    expect((caught as VFSFsError).code).toBe("ENOENT");
    expect((caught as VFSFsError).path).toBe("/nope");
    // syscall is set by the HTTP client (open).
    expect((caught as VFSFsError).syscall).toBe("open");
  });

  it("unknown method path → 404", async () => {
    const apiKey = await mintToken("default", "http-404");
    const res = await SELF.fetch(
      "https://mossaic.test/api/vfs/notARealMethod",
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${apiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ path: "/" }),
      }
    );
    expect(res.status).toBe(404);
  });

  it("streams throw EINVAL on the HTTP client (v1 unsupported)", async () => {
    const vfs = await client("default", "http-streams");
    await expect(vfs.createReadStream("/x")).rejects.toThrow(/EINVAL/);
    await expect(vfs.createWriteStream("/x")).rejects.toThrow(/EINVAL/);
    await expect(vfs.openReadStream("/x")).rejects.toThrow(/EINVAL/);
  });
});

describe("HTTP fallback latency (informational)", () => {
  it("1KB file round-trip completes (records timing for completion message)", async () => {
    const vfs = await client("default", "http-latency");
    const payload = new Uint8Array(1024).fill(0x55);
    const t0 = performance.now();
    await vfs.writeFile("/k.bin", payload);
    const t1 = performance.now();
    const back = await vfs.readFile("/k.bin");
    const t2 = performance.now();
    expect(back.byteLength).toBe(1024);
    expect(back[0]).toBe(0x55);
    // Log timings — the build-completion message reports the read
    // latency. In Miniflare these are sub-millisecond; the absolute
    // numbers aren't a CI guard, just observability.
    // eslint-disable-next-line no-console
    console.log(
      `[http-fallback timing] writeFile(1KB)=${(t1 - t0).toFixed(2)}ms, readFile(1KB)=${(t2 - t1).toFixed(2)}ms`
    );
  });

  it("EACCES error subclass is also reachable on the client surface", async () => {
    // Sanity: confirm EACCES is exported from @mossaic/sdk and is a
    // VFSFsError subclass. Phase 7 doesn't add new EACCES throws but
    // the export must remain stable.
    expect(typeof EACCES).toBe("function");
    expect(new EACCES().code).toBe("EACCES");
  });
});
