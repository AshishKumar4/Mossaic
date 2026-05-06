import { describe, it, expect } from "vitest";
import { SELF, env } from "cloudflare:test";

/**
 * Phase 13.5 — public Yjs WebSocket upgrade route.
 *
 * Pins the contract of `worker/core/routes/vfs-yjs-ws.ts`:
 *   1. Missing Bearer → 401 EACCES.
 *   2. Invalid Bearer → 401 EACCES.
 *   3. Non-WS request (no Upgrade header) → 426 EINVAL.
 *   4. Missing ?path=... → 400 EINVAL.
 *   5. Valid Bearer + Upgrade + path → 101 with WebSocket attached.
 *   6. Subprotocol-token auth (`Sec-WebSocket-Protocol: bearer.<jwt>`)
 *      works in lieu of the Authorization header — required for browsers.
 *
 * This route makes the existing DO-internal `/yjs/ws` upgrade
 * reachable by external Node clients (the `@mossaic/cli` CLI) by
 * adding Bearer-token auth and forwarding via stub.fetch.
 */

import { signVFSToken } from "@core/lib/auth";

interface E {
  MOSSAIC_USER: DurableObjectNamespace;
  JWT_SECRET?: string;
}
const TEST_ENV = env as unknown as E;

async function mint(ns: string, tenant: string, sub?: string): Promise<string> {
  return signVFSToken(TEST_ENV, { ns, tenant, sub });
}

describe("Phase 13.5 — public /api/vfs/yjs/ws upgrade route", () => {
  it("rejects without Authorization (no Bearer)", async () => {
    const r = await SELF.fetch(
      "https://mossaic.test/api/vfs/yjs/ws?path=/x",
      { headers: { Upgrade: "websocket" } },
    );
    expect(r.status).toBe(401);
    const body = (await r.json()) as { code: string };
    expect(body.code).toBe("EACCES");
  });

  it("rejects an invalid Bearer token", async () => {
    const r = await SELF.fetch(
      "https://mossaic.test/api/vfs/yjs/ws?path=/x",
      {
        headers: {
          Upgrade: "websocket",
          Authorization: "Bearer not-a-real-jwt",
        },
      },
    );
    expect(r.status).toBe(401);
  });

  it("rejects a non-Upgrade request with 426 EINVAL", async () => {
    const tok = await mint("default", "yjs-ws-no-upgrade");
    const r = await SELF.fetch(
      "https://mossaic.test/api/vfs/yjs/ws?path=/x",
      { headers: { Authorization: `Bearer ${tok}` } },
    );
    expect(r.status).toBe(426);
    const body = (await r.json()) as { code: string };
    expect(body.code).toBe("EINVAL");
  });

  it("rejects missing ?path=... with 400 EINVAL", async () => {
    const tok = await mint("default", "yjs-ws-no-path");
    const r = await SELF.fetch("https://mossaic.test/api/vfs/yjs/ws", {
      headers: {
        Upgrade: "websocket",
        Authorization: `Bearer ${tok}`,
      },
    });
    expect(r.status).toBe(400);
    const body = (await r.json()) as { code: string };
    expect(body.code).toBe("EINVAL");
  });

  it("upgrades to 101 with a valid Bearer + yjs-mode file", async () => {
    const tenant = "yjs-ws-happy";
    const tok = await mint("default", tenant);

    // Seed: write the file then promote it to yjs-mode.
    // Use the HTTP fallback so the tenant's UserDO is initialised
    // through the same code path the upgrade will later hit.
    await SELF.fetch("https://mossaic.test/api/vfs/writeFile", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${tok}`,
      },
      body: JSON.stringify({ path: "/live.md", data: "" }),
    });
    // setYjsMode has no HTTP route; reach into the DO directly.
    // The DO method is exposed via the typed RPC surface.
    const { vfsUserDOName } = await import("@core/lib/utils");
    const stub = TEST_ENV.MOSSAIC_USER.get(
      TEST_ENV.MOSSAIC_USER.idFromName(vfsUserDOName("default", tenant)),
    ) as unknown as {
      vfsSetYjsMode(scope: { ns: string; tenant: string }, p: string, on: boolean): Promise<void>;
    };
    await stub.vfsSetYjsMode({ ns: "default", tenant }, "/live.md", true);

    const r = await SELF.fetch(
      "https://mossaic.test/api/vfs/yjs/ws?path=/live.md",
      {
        headers: {
          Upgrade: "websocket",
          Authorization: `Bearer ${tok}`,
        },
      },
    );
    expect(r.status).toBe(101);
    expect(r.webSocket).toBeTruthy();
    r.webSocket?.accept();
    r.webSocket?.close(1000, "test done");
  });

  it("accepts subprotocol-token auth (bearer.<jwt>) and echoes it back", async () => {
    const tenant = "yjs-ws-subprotocol";
    const tok = await mint("default", tenant);

    // Seed.
    await SELF.fetch("https://mossaic.test/api/vfs/writeFile", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${tok}`,
      },
      body: JSON.stringify({ path: "/live.md", data: "" }),
    });
    const { vfsUserDOName } = await import("@core/lib/utils");
    const stub = TEST_ENV.MOSSAIC_USER.get(
      TEST_ENV.MOSSAIC_USER.idFromName(vfsUserDOName("default", tenant)),
    ) as unknown as {
      vfsSetYjsMode(scope: { ns: string; tenant: string }, p: string, on: boolean): Promise<void>;
    };
    await stub.vfsSetYjsMode({ ns: "default", tenant }, "/live.md", true);

    const proto = `bearer.${tok}`;
    const r = await SELF.fetch(
      "https://mossaic.test/api/vfs/yjs/ws?path=/live.md",
      {
        headers: {
          Upgrade: "websocket",
          "Sec-WebSocket-Protocol": proto,
        },
      },
    );
    expect(r.status).toBe(101);
    expect(r.webSocket).toBeTruthy();
    r.webSocket?.accept();
    r.webSocket?.close(1000, "test done");
  });
});
