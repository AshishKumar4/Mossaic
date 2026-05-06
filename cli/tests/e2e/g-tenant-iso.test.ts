/**
 * E2E G — Tenant isolation across ns / tenant / sub (4 cases).
 */

import { describe, it, expect, beforeAll } from "vitest";
import { ulid } from "ulid";
import { freshTenant } from "./helpers/tenant.js";
import { hasSecret, requireSecret } from "./helpers/env.js";

describe.skipIf(!hasSecret())("G — Tenant isolation", () => {
  beforeAll(() => requireSecret());

  it("G.1 — different tenants → fully isolated", async () => {
    const a = await freshTenant();
    const b = await freshTenant();
    try {
      await a.vfs.writeFile("/secret.txt", "from-a");
      await expect(b.vfs.readFile("/secret.txt")).rejects.toMatchObject({
        code: "ENOENT",
      });
    } finally {
      await a.teardown();
      await b.teardown();
    }
  });

  it("G.2 — different ns → fully isolated", async () => {
    const prod = await freshTenant({ ns: "prod" });
    const staging = await freshTenant({ ns: "staging" });
    try {
      await prod.vfs.writeFile("/x.txt", "prod");
      await expect(staging.vfs.readFile("/x.txt")).rejects.toMatchObject({
        code: "ENOENT",
      });
    } finally {
      await prod.teardown();
      await staging.teardown();
    }
  });

  it("G.3 — different sub under same tenant → isolated", async () => {
    const tn = "e2e-" + ulid().toLowerCase();
    const alice = await freshTenant();
    // Use the same tenant string across two different subs by minting
    // explicitly via altClient.
    const aliceTok = await alice.mintToken({ tenant: tn, sub: "alice" });
    const bobTok = await alice.mintToken({ tenant: tn, sub: "bob" });
    const aliceClient = alice.altClient(aliceTok);
    const bobClient = alice.altClient(bobTok);
    try {
      await aliceClient.writeFile("/sub-secret.txt", "alice");
      await expect(bobClient.readFile("/sub-secret.txt")).rejects.toMatchObject({
        code: "ENOENT",
      });
    } finally {
      // Clean up alice's tree.
      try {
        const entries = await aliceClient.readdir("/");
        for (const e of entries) {
          try { await aliceClient.removeRecursive("/" + e); } catch {}
        }
      } catch {}
      await alice.teardown();
    }
  });

  it("G.4 — tenant name with disallowed char (':') is rejected by token-or-server", async () => {
    const ctx = await freshTenant();
    try {
      // Mint a token for an invalid-tenant string. The server-side
      // verifyVFSToken will accept the token (jose just verifies sig),
      // but the DO-side scope validation in vfsUserDOName / scope
      // checks rejects ':' with EINVAL.
      const tok = await ctx.mintToken({ tenant: "ev:il" });
      const c = ctx.altClient(tok);
      await expect(c.stat("/")).rejects.toMatchObject({
        code: expect.stringMatching(/^E(INVAL|ACCES)$/),
      });
    } finally {
      await ctx.teardown();
    }
  });
});
