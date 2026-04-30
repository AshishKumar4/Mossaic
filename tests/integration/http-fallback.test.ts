import { describe, it, expect } from "vitest";
import { SELF, env, runInDurableObject } from "cloudflare:test";
import type { UserDO } from "@app/objects/user/user-do";

/**
 * HTTP fallback acceptance gate.
 *
 * Drives the HTTP fallback (worker/routes/vfs.ts) end-to-end:
 *   1. Operator-side: mint a VFS token via signVFSToken.
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
import type { EnvCore } from "@shared/types";

interface E {
  MOSSAIC_USER: DurableObjectNamespace<UserDO>;
  MOSSAIC_SHARD: DurableObjectNamespace;
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
  // Phase 29 — TEST_ENV's MOSSAIC_USER is parameterized over UserDO
  // for the test stub-typing helpers, but `signVFSToken` accepts
  // `EnvCore` whose namespaces are `DurableObjectNamespace`
  // (un-parameterized). The runtime env carries the same bindings;
  // cast through `EnvCore` to satisfy the structural mismatch.
  return signVFSToken(TEST_ENV as unknown as EnvCore, {
    ns,
    tenant,
    sub,
  });
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
    const legacyToken = await signJWT(TEST_ENV as unknown as EnvCore, {
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
    // HTTP fallback's stream methods take 0 args at the type level
    // (they're documented as ENOTSUP/EINVAL in v1); call without
    // the path arg.
    await expect(vfs.createReadStream()).rejects.toThrow(/EINVAL/);
    await expect(vfs.createWriteStream()).rejects.toThrow(/EINVAL/);
    await expect(vfs.openReadStream()).rejects.toThrow(/EINVAL/);
  });
});

// ── multipart envelope for metadata/tags/version on bytes ───
describe("HTTP writeFile multipart envelope (metadata/tags/version parity)", () => {
  it("HTTP writeFile (bytes + multipart) applies metadata + tags", async () => {
    const vfs = await client("default", "http-multipart-meta");
    const payload = new TextEncoder().encode("photo bytes go here");
    await vfs.writeFile("/photo.bin", payload, {
      metadata: { camera: "x100", iso: 400 },
      tags: ["urgent", "client/acme"],
    });
    // Confirm via listFiles.
    const page = await vfs.listFiles({ includeMetadata: true });
    const f = page.items.find((i) => i.path === "/photo.bin");
    expect(f).toBeDefined();
    expect(f!.metadata).toEqual({ camera: "x100", iso: 400 });
    expect([...f!.tags].sort()).toEqual(["client/acme", "urgent"]);
  });

  it("HTTP writeFile (string JSON path) applies metadata + tags + version label", async () => {
    const vfs = await client("default", "http-json-meta");
    // Enable versioning at the DO level (HTTP client doesn't expose
    // adminSetVersioning; reach in via the test env's MOSSAIC_USER stub).
    const tenantStub = TEST_ENV.MOSSAIC_USER.get(
      TEST_ENV.MOSSAIC_USER.idFromName("vfs:default:http-json-meta")
    );
    const tenantStubTyped = tenantStub as unknown as {
      adminSetVersioning(uid: string, enabled: boolean): Promise<unknown>;
    };
    await tenantStubTyped.adminSetVersioning("http-json-meta", true);

    await vfs.writeFile("/note.txt", "json-payload-here", {
      metadata: { project: "alpha" },
      tags: ["draft"],
      version: { label: "first", userVisible: true },
    });
    // The HTTP listVersions route doesn't currently surface `label` /
    // `userVisible`; verify directly via raw SQL in the DO. (Extending
    // the HTTP route is a follow-up and out of scope for.)
    const row = await runInDurableObject(tenantStub, async (_inst, state) => {
      return state.storage.sql
        .exec(
          `SELECT v.label, v.user_visible
             FROM file_versions v
             JOIN files f ON v.path_id = f.file_id
            WHERE f.file_name = 'note.txt'`
        )
        .toArray()[0] as { label: string | null; user_visible: number } | undefined;
    });
    expect(row).toBeDefined();
    expect(row!.label).toBe("first");
    expect(row!.user_visible).toBe(1);
  });

  it("HTTP writeFile (octet-stream path, no opts) is unchanged — backward compat", async () => {
    const vfs = await client("default", "http-octet-bc");
    const payload = new Uint8Array(2048);
    for (let i = 0; i < payload.length; i++) payload[i] = i & 0xff;
    await vfs.writeFile("/raw.bin", payload);
    const back = await vfs.readFile("/raw.bin");
    expect(back.byteLength).toBe(2048);
    expect(back[100]).toBe(100);
  });

  it("HTTP writeFile (multipart) rejects oversize tags with EINVAL (400)", async () => {
    const vfs = await client("default", "http-cap");
    // 33 unique tags > TAGS_MAX_PER_FILE (32) — server-side validation
    // should surface as EINVAL through the mapped error.
    const tooMany: string[] = [];
    for (let i = 0; i < 33; i++) tooMany.push(`t${i}`);
    await expect(
      vfs.writeFile("/cap.bin", new Uint8Array(16), { tags: tooMany })
    ).rejects.toThrow(/EINVAL/);
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
    // VFSFsError subclass. doesn't add new EACCES throws but
    // the export must remain stable.
    expect(typeof EACCES).toBe("function");
    expect(new EACCES().code).toBe("EACCES");
  });
});
