/**
 * E2E M — Preview pipeline (live).
 *
 * Drives `vfs.readPreview()` against the deployed worker. Skipped
 * when `MOSSAIC_E2E_JWT_SECRET` is unset (CI without secrets).
 */

import { describe, it, expect, beforeEach, afterEach, beforeAll } from "vitest";
import { freshTenant, type TenantCtx } from "./helpers/tenant.js";
import { hasSecret, requireSecret } from "./helpers/env.js";

describe.skipIf(!hasSecret())("M — Preview pipeline", () => {
  beforeAll(() => requireSecret());

  let ctx: TenantCtx;
  beforeEach(async () => {
    ctx = await freshTenant();
  });
  afterEach(async () => {
    await ctx.teardown();
  });

  it("M.1 — text file → code-svg renderer; SVG bytes returned", async () => {
    await ctx.vfs.writeFile("/code.ts", "export const x = 1;\n", {
      mimeType: "text/typescript",
    });
    const r = await ctx.vfs.readPreview("/code.ts", { variant: "thumb" });
    expect(r.mimeType).toBe("image/svg+xml");
    expect(r.rendererKind).toBe("code-svg");
    expect(new TextDecoder().decode(r.bytes)).toContain("<svg");
  });

  it("M.2 — second call hits the cache (fromVariantTable=true)", async () => {
    await ctx.vfs.writeFile("/note.txt", "hello", { mimeType: "text/plain" });
    const cold = await ctx.vfs.readPreview("/note.txt", { variant: "thumb" });
    const warm = await ctx.vfs.readPreview("/note.txt", { variant: "thumb" });
    expect(cold.fromVariantTable).toBe(false);
    expect(warm.fromVariantTable).toBe(true);
    // Bytes are byte-identical (content-addressed).
    expect(new TextDecoder().decode(warm.bytes)).toBe(
      new TextDecoder().decode(cold.bytes)
    );
  });

  it("M.3 — ENOENT on missing path", async () => {
    await expect(
      ctx.vfs.readPreview("/missing", { variant: "thumb" })
    ).rejects.toMatchObject({ code: "ENOENT" });
  });

  it("M.4 — openManifests batches paths in one round-trip", async () => {
    // Both files large enough to actually have a chunked manifest.
    await ctx.vfs.writeFile("/a.bin", new Uint8Array(20_000));
    await ctx.vfs.writeFile("/b.bin", new Uint8Array(20_000));
    const r = await ctx.vfs.openManifests(["/a.bin", "/missing.bin", "/b.bin"]);
    expect(r).toHaveLength(3);
    expect(r[0].ok).toBe(true);
    expect(r[1].ok).toBe(false);
    expect(r[2].ok).toBe(true);
  });
});
