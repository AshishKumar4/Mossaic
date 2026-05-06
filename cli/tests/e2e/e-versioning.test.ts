/**
 * E2E E — Versioning incl. userVisible monotonicity (8 cases).
 *
 * The Service worker reads the per-tenant `quota.versioning_enabled`
 * column lazily; the SDK's `createVFS({ versioning: 'enabled' })`
 * latches that flag on first write. Over the HTTP fallback there is
 * no `versioning` option in `createMossaicHttpClient`, so we trigger
 * the latch by toggling versioning manually via the binding-mode
 * adminSetVersioning RPC. Without that, listVersions returns [] and
 * versioning behavior is byte-equivalent to non-versioning.
 *
 * To keep this Node-only test self-contained, we use the existing
 * markVersion route to flip on writeFile-as-versioned semantics by
 * sending an explicit version field; the worker enables versioning
 * for the tenant lazily when the version block is requested.
 *
 * If the live deploy has versioning OFF for a fresh tenant by default
 * (which it does), `listVersions` will return an empty array even
 * after a few writes. We therefore use the `restoreVersion` /
 * `markVersion` calls — both of which require versioning — and
 * gracefully skip individual assertions when the deployment doesn't
 * support per-tenant versioning auto-enable from HTTP.
 *
 * NOTE: the SDK's HttpVFS does NOT expose adminSetVersioning. To
 * exercise versioning against the live deploy we need a one-time
 * server flip. We work around by writing through the `version` opt
 * in writeFile — the worker treats that as a hint to lazily enable
 * versioning for the tenant. If the deploy doesn't honor that
 * (current Phase 13.5 behavior), the tests assert the no-versioning
 * baseline instead.
 */

import { describe, it, expect, beforeEach, afterEach, beforeAll } from "vitest";
import { freshTenant, type TenantCtx } from "./helpers/tenant.js";
import { hasSecret, requireSecret, ENDPOINT } from "./helpers/env.js";

// Helper: enable versioning on a tenant via the Phase 13.5 admin
// HTTP route. The Service worker exposes
// `POST /api/vfs/admin/setVersioning { enabled: true }` which calls
// `UserDOCore.adminSetVersioning(userId, true)` server-side. The
// userId is derived from the verified token scope, so this is safe
// for the per-tenant CLI to call.
async function enableVersioning(ctx: TenantCtx): Promise<void> {
  const r = await fetch(ctx.endpoint + "/api/vfs/admin/setVersioning", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${ctx.token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ enabled: true }),
  });
  if (!r.ok) {
    const body = await r.text();
    throw new Error(`enableVersioning: HTTP ${r.status} ${body}`);
  }
}

describe.skipIf(!hasSecret())("E — Versioning", () => {
  beforeAll(() => requireSecret());

  let ctx: TenantCtx;
  beforeEach(async () => {
    ctx = await freshTenant();
    await enableVersioning(ctx);
    void ENDPOINT;
  });
  afterEach(async () => { await ctx.teardown(); });

  it("E.1 — three writeFile calls produce three versions (newest first)", async () => {
    await ctx.vfs.writeFile("/v.txt", "v1");
    await ctx.vfs.writeFile("/v.txt", "v2");
    await ctx.vfs.writeFile("/v.txt", "v3");
    const versions = await ctx.vfs.listVersions("/v.txt");
    expect(versions.length).toBeGreaterThanOrEqual(3);
    // newest-first: monotonically decreasing mtimeMs.
    for (let i = 1; i < versions.length; i++) {
      expect(versions[i - 1].mtimeMs).toBeGreaterThanOrEqual(versions[i].mtimeMs);
    }
  });

  it("E.2 — readFile-by-version returns historical bytes", async () => {
    await ctx.vfs.writeFile("/r.txt", "first");
    await ctx.vfs.writeFile("/r.txt", "second");
    const back = await ctx.vfs.readFile("/r.txt", { encoding: "utf8" });
    expect(back).toBe("second");
    const versions = await ctx.vfs.listVersions("/r.txt");
    const oldest = versions[versions.length - 1];
    const r = await fetch(ENDPOINT + "/api/vfs/readFile", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${ctx.token}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ path: "/r.txt", versionId: oldest.id }),
    });
    expect(r.ok).toBe(true);
    const buf = new Uint8Array(await r.arrayBuffer());
    expect(new TextDecoder().decode(buf)).toBe("first");
  });

  it("E.3 — unlink writes a tombstone version", async () => {
    await ctx.vfs.writeFile("/t.txt", "x");
    await ctx.vfs.unlink("/t.txt");
    expect(await ctx.vfs.exists("/t.txt")).toBe(false);
    const versions = await ctx.vfs.listVersions("/t.txt");
    expect(versions.length).toBeGreaterThanOrEqual(1);
    expect(versions[0].deleted).toBe(true);
  });

  it("E.4 — restoreVersion creates a new live version", async () => {
    await ctx.vfs.writeFile("/r4.txt", "alpha");
    const versions = await ctx.vfs.listVersions("/r4.txt");
    const source = versions[0];
    await ctx.vfs.writeFile("/r4.txt", "beta");
    expect(await ctx.vfs.readFile("/r4.txt", { encoding: "utf8" })).toBe("beta");
    const r = await ctx.vfs.restoreVersion("/r4.txt", source.id);
    expect(typeof r.id).toBe("string");
    expect(await ctx.vfs.readFile("/r4.txt", { encoding: "utf8" })).toBe("alpha");
  });

  it("E.5 — restoreVersion of a tombstone → EINVAL", async () => {
    await ctx.vfs.writeFile("/r5.txt", "alive");
    await ctx.vfs.unlink("/r5.txt");
    const versions = await ctx.vfs.listVersions("/r5.txt");
    const tombstone = versions.find((v) => v.deleted);
    expect(tombstone).toBeDefined();
    await expect(
      ctx.vfs.restoreVersion("/r5.txt", tombstone!.id),
    ).rejects.toMatchObject({ code: "EINVAL" });
  });

  it("E.6 — dropVersions({}) keeps only the head", async () => {
    for (let i = 0; i < 4; i++) {
      await ctx.vfs.writeFile("/d.txt", `n=${i}`);
    }
    const before = await ctx.vfs.listVersions("/d.txt");
    expect(before.length).toBeGreaterThanOrEqual(4);
    const r = await ctx.vfs.dropVersions("/d.txt", {});
    expect(r.dropped).toBeGreaterThanOrEqual(3);
    expect(r.kept).toBe(1);
  });

  it("E.7 — cross-version dedup: identical payload across versions returns same size", async () => {
    const payload = new Uint8Array(20 * 1024).fill(0xab);
    await ctx.vfs.writeFile("/dd.bin", payload);
    await ctx.vfs.writeFile("/dd.bin", payload);
    await ctx.vfs.writeFile("/dd.bin", payload);
    const stat = await ctx.vfs.stat("/dd.bin");
    expect(stat.size).toBe(payload.byteLength);
    const v = await ctx.vfs.listVersions("/dd.bin");
    expect(v.length).toBeGreaterThanOrEqual(3);
    expect(
      v.every((row) => row.size === payload.byteLength || row.deleted),
    ).toBe(true);
  });

  it("E.8 — userVisible monotonicity: markVersion(...,{userVisible:false}) is rejected", async () => {
    await ctx.vfs.writeFile("/uv.txt", "x", {
      version: { label: "first", userVisible: true },
    });
    const versions = await ctx.vfs.listVersions("/uv.txt");
    const target = versions[0];
    await ctx.vfs.markVersion("/uv.txt", target.id, {
      label: "renamed",
      userVisible: true,
    });
    await expect(
      ctx.vfs.markVersion("/uv.txt", target.id, { userVisible: false }),
    ).rejects.toMatchObject({ code: "EINVAL" });
    const visible = await ctx.vfs.listVersions("/uv.txt", {
      userVisibleOnly: true,
    });
    expect(visible.length).toBeGreaterThanOrEqual(1);
  });
});
