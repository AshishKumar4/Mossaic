/**
 * E2E H — Performance smoke (3 advisory cases).
 *
 * These tests print metrics and only fail if performance is grossly
 * broken. The bars are intentionally lenient because we're crossing
 * the public internet to the live worker; numbers tighter than these
 * would just produce noise.
 */

import { describe, it, expect, beforeEach, afterEach, beforeAll } from "vitest";
import { freshTenant, type TenantCtx } from "./helpers/tenant.js";
import { hasSecret, requireSecret } from "./helpers/env.js";

describe.skipIf(!hasSecret())("H — Performance smoke (advisory)", () => {
  beforeAll(() => requireSecret());

  let ctx: TenantCtx;
  beforeEach(async () => { ctx = await freshTenant(); });
  afterEach(async () => { await ctx.teardown(); });

  it("H.1 — listFiles({prefix}) over 50 files completes well under 5s", async () => {
    const N = 50;
    for (let i = 0; i < N; i++) {
      await ctx.vfs.writeFile(`/h1-${String(i).padStart(3, "0")}.txt`, `n=${i}`);
    }
    const t0 = Date.now();
    const page = await ctx.vfs.listFiles({ prefix: "/", limit: N + 10 });
    const dt = Date.now() - t0;
    // eslint-disable-next-line no-console
    console.log(`H.1 listFiles(${N}): ${dt}ms`);
    expect(page.items.length).toBeGreaterThanOrEqual(N);
    expect(dt).toBeLessThan(5_000); // very generous advisory bound
  }, 120_000);

  it("H.2 — readManyStat([20 paths]) round-trip under 3s", async () => {
    const paths = Array.from({ length: 20 }, (_, i) => `/h2-${i}.txt`);
    for (const p of paths) {
      await ctx.vfs.writeFile(p, "x");
    }
    const t0 = Date.now();
    const stats = await ctx.vfs.readManyStat(paths);
    const dt = Date.now() - t0;
    // eslint-disable-next-line no-console
    console.log(`H.2 readManyStat(${paths.length}): ${dt}ms`);
    expect(stats.length).toBe(paths.length);
    expect(dt).toBeLessThan(3_000);
  }, 60_000);

  it("H.3 — 30 sequential writes complete under 30s (rate-limit headroom)", async () => {
    const N = 30;
    const t0 = Date.now();
    for (let i = 0; i < N; i++) {
      await ctx.vfs.writeFile(`/h3-${i}.txt`, `n=${i}`);
    }
    const dt = Date.now() - t0;
    // eslint-disable-next-line no-console
    console.log(`H.3 ${N} sequential writes: ${dt}ms (${(dt / N).toFixed(1)}ms each)`);
    expect(dt).toBeLessThan(30_000);
  }, 60_000);
});
