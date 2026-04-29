/**
 * E2E I — Error surface (5 cases).
 */

import { describe, it, expect, beforeEach, afterEach, beforeAll } from "vitest";
import { createMossaicHttpClient, MossaicUnavailableError } from "@mossaic/sdk/http";
import { freshTenant, type TenantCtx } from "./helpers/tenant.js";
import { hasSecret, requireSecret, ENDPOINT } from "./helpers/env.js";
import { METADATA_MAX_BYTES } from "@mossaic/sdk/http";

describe.skipIf(!hasSecret())("I — Error surface", () => {
  beforeAll(() => requireSecret());

  let ctx: TenantCtx;
  beforeEach(async () => { ctx = await freshTenant(); });
  afterEach(async () => { await ctx.teardown(); });

  it("I.1 — readFile('/nonexistent') → ENOENT instance with code='ENOENT'", async () => {
    try {
      await ctx.vfs.readFile("/nope.txt");
      throw new Error("should have thrown");
    } catch (err) {
      expect((err as { code: string }).code).toBe("ENOENT");
    }
  });

  it("I.2 — calling a non-existent /api/vfs/method → 404 ENOENT (clean structured response)", async () => {
    const r = await fetch(ENDPOINT + "/api/vfs/no-such-method", {
      method: "POST",
      headers: { Authorization: `Bearer ${ctx.token}`, "Content-Type": "application/json" },
      body: JSON.stringify({}),
    });
    expect(r.status).toBe(404);
    const body = (await r.json()) as { code: string };
    expect(body.code).toBe("ENOENT");
  });

  it("I.3 — unreachable endpoint → MossaicUnavailableError", async () => {
    // 127.0.0.1:1 is reserved + closed. fetch will reject.
    const dead = createMossaicHttpClient({
      url: "http://127.0.0.1:1",
      apiKey: ctx.token,
    });
    try {
      await dead.stat("/x");
      throw new Error("should have thrown");
    } catch (err) {
      expect(err instanceof MossaicUnavailableError).toBe(true);
    }
  });

  it("I.4 — oversized metadata → EINVAL", async () => {
    const oversize = "x".repeat(METADATA_MAX_BYTES + 100);
    await expect(
      ctx.vfs.writeFile("/i4.bin", "x", { metadata: { huge: oversize } }),
    ).rejects.toMatchObject({ code: "EINVAL" });
  });

  it("I.5 — rate-limit smoke: rapid burst either succeeds or surfaces EAGAIN", async () => {
    // Default per-tenant rate limit is 100 ops/sec, 200 burst. We
    // fire 25 ops in parallel — well under the burst — so we expect
    // success here. The advisory check is that errors, when they
    // happen, are typed EAGAIN and not opaque.
    const promises: Promise<unknown>[] = [];
    for (let i = 0; i < 25; i++) {
      promises.push(
        ctx.vfs.writeFile(`/i5-${i}.txt`, "x").catch((err) => err),
      );
    }
    const results = await Promise.all(promises);
    let anyErr = false;
    for (const r of results) {
      if (r instanceof Error) {
        anyErr = true;
        expect((r as { code?: string }).code).toMatch(/^E(AGAIN|MOSSAIC_UNAVAILABLE)$/);
      }
    }
    // It's OK if zero errors — that just means we didn't trip the
    // limit. The point is shape, not occurrence.
    void anyErr;
  }, 60_000);
});
