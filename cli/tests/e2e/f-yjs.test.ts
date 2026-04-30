/**
 * E2E F — Yjs (6 cases).
 *
 * Requires the public Yjs WS upgrade route at /api/vfs/yjs/ws (added
 * in `worker/core/routes/vfs-yjs-ws.ts`). The route
 * forwards Bearer-authenticated WS upgrades to the per-tenant
 * UserDOCore via stub.fetch.
 */

import { describe, it, expect, beforeEach, afterEach, beforeAll } from "vitest";
import { freshTenant, type TenantCtx } from "./helpers/tenant.js";
import { hasSecret, requireSecret } from "./helpers/env.js";
import { openYDocOverWs } from "../../src/yjs-ws.js";
import { VFS_MODE_YJS_BIT } from "@mossaic/sdk/http";

async function setYjsMode(ctx: TenantCtx, path: string, enabled: boolean): Promise<void> {
  const r = await fetch(ctx.endpoint + "/api/vfs/setYjsMode", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${ctx.token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ path, enabled }),
  });
  if (!r.ok) {
    const body = await r.text();
    throw Object.assign(
      new Error(`setYjsMode HTTP ${r.status}: ${body}`),
      { code: (() => { try { return (JSON.parse(body) as { code?: string }).code; } catch { return undefined; } })() },
    );
  }
}

describe.skipIf(!hasSecret())("F — Yjs", () => {
  beforeAll(() => requireSecret());

  let ctx: TenantCtx;
  beforeEach(async () => { ctx = await freshTenant(); });
  afterEach(async () => { await ctx.teardown(); });

  it("F.1 — setYjsMode(true) flips the mode bit on stat.mode", async () => {
    await ctx.vfs.writeFile("/notes.md", "# hello\n");
    const before = await ctx.vfs.stat("/notes.md");
    expect(before.mode & VFS_MODE_YJS_BIT).toBe(0);
    await setYjsMode(ctx, "/notes.md", true);
    const after = await ctx.vfs.stat("/notes.md");
    expect(after.mode & VFS_MODE_YJS_BIT).toBe(VFS_MODE_YJS_BIT);
  });

  it("F.2 — setYjsMode(false) after enable → EINVAL (demote rejected)", async () => {
    await ctx.vfs.writeFile("/d.md", "");
    await setYjsMode(ctx, "/d.md", true);
    await expect(setYjsMode(ctx, "/d.md", false)).rejects.toMatchObject({
      code: "EINVAL",
    });
  });

  it("F.3 — 2-client sync: clientA insert propagates to clientB", async () => {
    await ctx.vfs.writeFile("/sync.md", "");
    await setYjsMode(ctx, "/sync.md", true);
    const a = await openYDocOverWs({
      endpoint: ctx.endpoint,
      token: ctx.token,
      path: "/sync.md",
    });
    const b = await openYDocOverWs({
      endpoint: ctx.endpoint,
      token: ctx.token,
      path: "/sync.md",
    });
    try {
      await Promise.all([a.synced, b.synced]);
      a.doc.transact(() => {
        a.doc.getText("content").insert(0, "DRAFT — ");
      });
      const deadline = Date.now() + 4000;
      while (Date.now() < deadline) {
        if (b.doc.getText("content").toString().startsWith("DRAFT — ")) break;
        await new Promise((r) => setTimeout(r, 50));
      }
      expect(b.doc.getText("content").toString()).toMatch(/^DRAFT —/);
    } finally {
      await a.close();
      await b.close();
    }
  }, 30_000);

  it("F.4 — Awareness round-trip: A and B see each other's local state", async () => {
    await ctx.vfs.writeFile("/aware.md", "");
    await setYjsMode(ctx, "/aware.md", true);
    const a = await openYDocOverWs({
      endpoint: ctx.endpoint,
      token: ctx.token,
      path: "/aware.md",
    });
    const b = await openYDocOverWs({
      endpoint: ctx.endpoint,
      token: ctx.token,
      path: "/aware.md",
    });
    try {
      await Promise.all([a.synced, b.synced]);
      a.awareness.setLocalState({ name: "alice", cursor: 1 });
      b.awareness.setLocalState({ name: "bob", cursor: 7 });

      // Wait for propagation. y-protocols emits 'change' after each remote
      // update arrives.
      const deadline = Date.now() + 4000;
      while (Date.now() < deadline) {
        const aStates = Array.from(a.awareness.getStates().values()) as Array<{ name?: string }>;
        const bStates = Array.from(b.awareness.getStates().values()) as Array<{ name?: string }>;
        const aSeesBob = aStates.some((s) => s?.name === "bob");
        const bSeesAlice = bStates.some((s) => s?.name === "alice");
        if (aSeesBob && bSeesAlice) break;
        await new Promise((r) => setTimeout(r, 50));
      }
      const aStates = Array.from(a.awareness.getStates().values()) as Array<{ name?: string }>;
      const bStates = Array.from(b.awareness.getStates().values()) as Array<{ name?: string }>;
      expect(aStates.some((s) => s?.name === "bob")).toBe(true);
      expect(bStates.some((s) => s?.name === "alice")).toBe(true);
    } finally {
      await a.close();
      await b.close();
    }
  }, 30_000);

  it("F.5 — Zero-oplog assertion: awareness frames don't add user-visible versions", async () => {
    // Enable versioning so we can count user-visible versions.
    await fetch(ctx.endpoint + "/api/vfs/admin/setVersioning", {
      method: "POST",
      headers: { Authorization: `Bearer ${ctx.token}`, "Content-Type": "application/json" },
      body: JSON.stringify({ enabled: true }),
    });
    await ctx.vfs.writeFile("/zero.md", "", { version: { label: "init", userVisible: true } });
    await setYjsMode(ctx, "/zero.md", true);

    const versionsBefore = await ctx.vfs.listVersions("/zero.md", { userVisibleOnly: true });
    const a = await openYDocOverWs({
      endpoint: ctx.endpoint, token: ctx.token, path: "/zero.md",
    });
    try {
      await a.synced;
      // Awareness churn — many updates, no doc changes.
      for (let i = 0; i < 10; i++) {
        a.awareness.setLocalState({ name: "alice", cursor: i, ts: Date.now() });
        await new Promise((r) => setTimeout(r, 30));
      }
      // Give the server a moment to persist anything it would persist.
      await new Promise((r) => setTimeout(r, 500));
      const versionsAfter = await ctx.vfs.listVersions("/zero.md", { userVisibleOnly: true });
      // No NEW user-visible versions from awareness frames alone.
      expect(versionsAfter.length).toBe(versionsBefore.length);
    } finally {
      await a.close();
    }
  }, 30_000);

  it("F.6 — flush({label}) produces a user-visible version row", async () => {
    await fetch(ctx.endpoint + "/api/vfs/admin/setVersioning", {
      method: "POST",
      headers: { Authorization: `Bearer ${ctx.token}`, "Content-Type": "application/json" },
      body: JSON.stringify({ enabled: true }),
    });
    await ctx.vfs.writeFile("/flush.md", "", { version: { label: "init", userVisible: true } });
    await setYjsMode(ctx, "/flush.md", true);

    const a = await openYDocOverWs({
      endpoint: ctx.endpoint, token: ctx.token, path: "/flush.md",
    });
    try {
      await a.synced;
      a.doc.transact(() => {
        a.doc.getText("content").insert(0, "edited content");
      });
      // Let the update reach the server.
      await new Promise((r) => setTimeout(r, 200));
      const r = await a.flush({ label: "save-1" });
      expect(typeof r.checkpointSeq).toBe("number");
      const versions = await ctx.vfs.listVersions("/flush.md", { userVisibleOnly: true });
      const labelled = versions.find((v) => v.label === "save-1");
      expect(labelled).toBeDefined();
      expect(labelled!.userVisible).toBe(true);
    } finally {
      await a.close();
    }
  }, 30_000);
});
