import { describe, it, expect } from "vitest";
import { SELF, env } from "cloudflare:test";

/**
 * SDK round-trip tests for `vfs.readPreview()`. Both the binding
 * client (createVFS) and the HTTP client (createMossaicHttpClient)
 * implement `VFSClient` — this gate proves they round-trip through
 * the same UserDO RPC and produce semantically identical results.
 */

import { createVFS, createMossaicHttpClient } from "../../sdk/src/index";
import type { UserDO } from "@app/objects/user/user-do";
import { signVFSToken } from "@core/lib/auth";
import type { EnvCore } from "@shared/types";

interface E {
  MOSSAIC_USER: DurableObjectNamespace<UserDO>;
  MOSSAIC_SHARD: DurableObjectNamespace;
  JWT_SECRET?: string;
}
const E = env as unknown as E;

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

describe("SDK readPreview — binding client (createVFS)", () => {
  it("returns SVG bytes via the code renderer for a text file", async () => {
    const vfs = createVFS(E as unknown as Parameters<typeof createVFS>[0], {
      namespace: "default",
      tenant: "sdk-rp-bind-1",
    });
    await vfs.writeFile(
      "/code.ts",
      "export const x = 1;\n",
      { mimeType: "text/typescript" }
    );

    const r = await vfs.readPreview("/code.ts", { variant: "thumb" });
    expect(r.mimeType).toBe("image/svg+xml");
    expect(r.rendererKind).toBe("code-svg");
    expect(r.fromVariantTable).toBe(false);
    expect(new TextDecoder().decode(r.bytes)).toContain("<svg");
  });

  it("second call hits the cache (fromVariantTable=true)", async () => {
    const vfs = createVFS(E as unknown as Parameters<typeof createVFS>[0], {
      namespace: "default",
      tenant: "sdk-rp-bind-2",
    });
    await vfs.writeFile("/n.txt", "x", { mimeType: "text/plain" });
    const cold = await vfs.readPreview("/n.txt", { variant: "thumb" });
    const warm = await vfs.readPreview("/n.txt", { variant: "thumb" });
    expect(cold.fromVariantTable).toBe(false);
    expect(warm.fromVariantTable).toBe(true);
    expect(warm.bytes).toEqual(cold.bytes);
  });

  it("propagates ENOENT as VFSFsError subclass", async () => {
    const vfs = createVFS(E as unknown as Parameters<typeof createVFS>[0], {
      namespace: "default",
      tenant: "sdk-rp-bind-3",
    });
    await expect(
      vfs.readPreview("/missing", { variant: "thumb" })
    ).rejects.toThrow(/ENOENT/);
  });
});

describe("SDK readPreview — HTTP client (createMossaicHttpClient)", () => {
  it("HTTP client returns the same bytes as the binding client for the same input", async () => {
    const tenant = "sdk-rp-http-1";
    const vfs = createVFS(E as unknown as Parameters<typeof createVFS>[0], {
      namespace: "default",
      tenant,
    });
    await vfs.writeFile(
      "/parity.ts",
      "const z = 42;\n",
      { mimeType: "text/typescript" }
    );

    const apiKey = await signVFSToken(E as unknown as EnvCore, { ns: "default", tenant });
    const httpVfs = createMossaicHttpClient({
      url: "https://m.test",
      apiKey,
      fetcher: selfFetcher,
    });

    const fromHttp = await httpVfs.readPreview("/parity.ts", {
      variant: "thumb",
    });
    const fromBinding = await vfs.readPreview("/parity.ts", {
      variant: "thumb",
    });

    // Same input + same renderer + content-deterministic SVG → byte
    // equality across transports.
    expect(fromHttp.mimeType).toBe(fromBinding.mimeType);
    expect(fromHttp.rendererKind).toBe(fromBinding.rendererKind);
    expect(new TextDecoder().decode(fromHttp.bytes)).toBe(
      new TextDecoder().decode(fromBinding.bytes)
    );
  });

  it("openManifests batches multiple paths in one HTTP round-trip", async () => {
    const tenant = "sdk-rp-http-2";
    const vfs = createVFS(E as unknown as Parameters<typeof createVFS>[0], {
      namespace: "default",
      tenant,
    });
    // Two large files (>16 KB inline limit) so they have real manifests.
    await vfs.writeFile(
      "/a.bin",
      new Uint8Array(20_000),
      { mimeType: "application/octet-stream" }
    );
    await vfs.writeFile(
      "/b.bin",
      new Uint8Array(20_000),
      { mimeType: "application/octet-stream" }
    );

    const apiKey = await signVFSToken(E as unknown as EnvCore, { ns: "default", tenant });
    const httpVfs = createMossaicHttpClient({
      url: "https://m.test",
      apiKey,
      fetcher: selfFetcher,
    });

    const results = await httpVfs.openManifests([
      "/a.bin",
      "/missing.bin",
      "/b.bin",
    ]);
    expect(results).toHaveLength(3);
    expect(results[0].ok).toBe(true);
    expect(results[1].ok).toBe(false);
    if (results[1].ok === false) {
      expect(results[1].code).toBe("ENOENT");
    }
    expect(results[2].ok).toBe(true);
  });

  it("openManifests on the binding client serializes through DO RPC", async () => {
    const vfs = createVFS(E as unknown as Parameters<typeof createVFS>[0], {
      namespace: "default",
      tenant: "sdk-rp-bind-4",
    });
    await vfs.writeFile("/x.bin", new Uint8Array(20_000));
    const r = await vfs.openManifests(["/x.bin", "/none"]);
    expect(r).toHaveLength(2);
    expect(r[0].ok).toBe(true);
    expect(r[1].ok).toBe(false);
  });
});
