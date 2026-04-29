/**
 * E2E A — Auth & scope isolation (5 cases).
 */

import { describe, it, expect, beforeAll, beforeEach, afterEach } from "vitest";
import { SignJWT } from "jose";
import { createMossaicHttpClient } from "@mossaic/sdk/http";
import { freshTenant, type TenantCtx } from "./helpers/tenant.js";
import { ENDPOINT, SECRET, hasSecret, requireSecret } from "./helpers/env.js";
import { ulid } from "ulid";

describe.skipIf(!hasSecret())("A — Auth & scope isolation", () => {
  beforeAll(() => requireSecret());

  let ctx: TenantCtx;
  beforeEach(async () => {
    ctx = await freshTenant();
  });
  afterEach(async () => {
    await ctx.teardown();
  });

  it("A.1 — token without scope:'vfs' claim is rejected (401 EACCES)", async () => {
    // Mint a token by hand WITHOUT the scope claim.
    const tok = await new SignJWT({ ns: "default", tn: ctx.tenant })
      .setProtectedHeader({ alg: "HS256" })
      .setIssuedAt()
      .setExpirationTime(Date.now() + 60_000)
      .sign(new TextEncoder().encode(SECRET));
    const r = await fetch(ENDPOINT + "/api/vfs/stat", {
      method: "POST",
      headers: { Authorization: "Bearer " + tok, "Content-Type": "application/json" },
      body: JSON.stringify({ path: "/x" }),
    });
    expect(r.status).toBe(401);
    const body = (await r.json()) as { code: string };
    expect(body.code).toBe("EACCES");
  });

  it("A.2 — token signed with wrong secret is rejected (401)", async () => {
    const tok = await new SignJWT({ scope: "vfs", ns: "default", tn: ctx.tenant })
      .setProtectedHeader({ alg: "HS256" })
      .setIssuedAt()
      .setExpirationTime(Date.now() + 60_000)
      .sign(new TextEncoder().encode("not-the-real-secret-" + ulid()));
    const r = await fetch(ENDPOINT + "/api/vfs/stat", {
      method: "POST",
      headers: { Authorization: "Bearer " + tok, "Content-Type": "application/json" },
      body: JSON.stringify({ path: "/x" }),
    });
    expect(r.status).toBe(401);
  });

  it("A.3 — expired token is rejected", async () => {
    // Use an exp value clearly in the past in seconds-since-epoch
    // (jose accepts both ms and s shapes for exp; we want unambiguous
    // "expired" so use a small absolute number ⇒ year-1970 in s).
    const tok = await new SignJWT({ scope: "vfs", ns: "default", tn: ctx.tenant })
      .setProtectedHeader({ alg: "HS256" })
      .setIssuedAt(1_000)
      .setExpirationTime(2_000) // 2000 seconds since epoch == 1970-01-01 00:33:20
      .sign(new TextEncoder().encode(SECRET));
    const r = await fetch(ENDPOINT + "/api/vfs/stat", {
      method: "POST",
      headers: { Authorization: "Bearer " + tok, "Content-Type": "application/json" },
      body: JSON.stringify({ path: "/x" }),
    });
    expect(r.status).toBe(401);
  });

  it("A.4 — token for tenant X cannot read a path written under tenant Y", async () => {
    const otherCtx = await freshTenant();
    try {
      await otherCtx.vfs.writeFile("/secret.txt", "hidden");
      // Use ctx (different tenant) to try to read it.
      await expect(ctx.vfs.readFile("/secret.txt")).rejects.toMatchObject({
        code: "ENOENT",
      });
    } finally {
      await otherCtx.teardown();
    }
  });

  it("A.5 — sub-tenant boundary: alice cannot read bob's file", async () => {
    const tn = "e2e-" + ulid().toLowerCase();
    const aliceTok = await new SignJWT({ scope: "vfs", ns: "default", tn, sub: "alice" })
      .setProtectedHeader({ alg: "HS256" })
      .setIssuedAt()
      .setExpirationTime(Date.now() + 60_000)
      .sign(new TextEncoder().encode(SECRET));
    const bobTok = await new SignJWT({ scope: "vfs", ns: "default", tn, sub: "bob" })
      .setProtectedHeader({ alg: "HS256" })
      .setIssuedAt()
      .setExpirationTime(Date.now() + 60_000)
      .sign(new TextEncoder().encode(SECRET));
    const alice = createMossaicHttpClient({ url: ENDPOINT, apiKey: aliceTok });
    const bob = createMossaicHttpClient({ url: ENDPOINT, apiKey: bobTok });
    try {
      await bob.writeFile("/bob-secret.txt", "for bob only");
      await expect(alice.readFile("/bob-secret.txt")).rejects.toMatchObject({
        code: "ENOENT",
      });
    } finally {
      // Best-effort cleanup of both sub-tenants.
      try { await bob.removeRecursive("/bob-secret.txt"); } catch {}
    }
  });
});
