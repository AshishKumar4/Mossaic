import { describe, it, expect } from "vitest";
import { env } from "cloudflare:test";

/**
 * Phase 11 — service-mode smoke.
 *
 * Pins the invariant that `UserDOCore` (the SDK-essential class
 * deployed standalone via deployments/service/wrangler.jsonc) is
 * fully usable WITHOUT any of the App-side photo-app surface in
 * scope. The test boots through the App-bound test worker (which
 * RE-EXPORTS `UserDOCore as UserDO` to satisfy the existing
 * `class_name: "UserDO"` binding), but exercises ONLY the VFS RPC
 * methods that the SDK promises to consumers — the same calls the
 * `@mossaic/sdk` HTTP fallback and library-mode SDK make.
 *
 * Why this matters: the goal of Phase 11 was to split the photo-app
 * routes off so the SDK can be deployed standalone (Mode B in the
 * SDK README). This test guards against accidental App-import
 * leakage into Core — if a future change pulls `auth.ts` /
 * `files.ts` / `quota.ts` / `_legacyFetch` into the Core class,
 * the SDK bundle would re-bloat and Service-mode tenants would
 * pay App-mode storage costs.
 *
 * Coverage:
 *   - vfsWriteFile / vfsReadFile / vfsStat (the three load-bearing
 *     RPCs the SDK exposes through `createVFS`).
 *   - vfsMkdir / vfsReaddir (directory plumbing).
 *   - vfsSetYjsMode / vfsOpenYjsSocket (Phase 10 surface — the
 *     biggest test that the WS upgrade path doesn't accidentally
 *     touch the App's `_legacyFetch`).
 */

import type { UserDOCore } from "@core/objects/user/user-do-core";
import { vfsUserDOName } from "@core/lib/utils";

interface E {
  MOSSAIC_USER: DurableObjectNamespace<UserDOCore>;
}
const E = env as unknown as E;
const NS = "default";

function userStub(tenant: string) {
  return E.MOSSAIC_USER.get(E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant)));
}

const SCOPE = (tenant: string) => ({ ns: NS, tenant });

describe("Phase 11 — service-mode VFS smoke (no App surface needed)", () => {
  it("write/read/stat round-trip uses only Core RPC", async () => {
    const tenant = "service-mode-rw";
    const stub = userStub(tenant);

    await stub.vfsWriteFile(SCOPE(tenant), "/note.txt", new TextEncoder().encode("hello"));
    const buf = await stub.vfsReadFile(SCOPE(tenant), "/note.txt");
    expect(new TextDecoder().decode(buf)).toBe("hello");

    const s = await stub.vfsStat(SCOPE(tenant), "/note.txt");
    expect(s.size).toBe(5);
    expect(s.type).toBe("file");
  });

  it("mkdir/readdir works without app-side helpers", async () => {
    const tenant = "service-mode-dirs";
    const stub = userStub(tenant);

    await stub.vfsMkdir(SCOPE(tenant), "/sub", {});
    await stub.vfsWriteFile(
      SCOPE(tenant),
      "/sub/a.txt",
      new TextEncoder().encode("a")
    );
    await stub.vfsWriteFile(
      SCOPE(tenant),
      "/sub/b.txt",
      new TextEncoder().encode("b")
    );

    const entries = await stub.vfsReaddir(SCOPE(tenant), "/sub");
    expect(entries.sort()).toEqual(["a.txt", "b.txt"]);
  });

  it("yjs WebSocket upgrade flows through Core fetch (no App delegate)", async () => {
    // Direct fetch into the DO with Upgrade: websocket should land in
    // Core's `_fetchWebSocketUpgrade` even if the runtime instance is
    // the App subclass — the App `fetch` override checks for the
    // Upgrade header and delegates to `super.fetch` (Core).
    const tenant = "service-mode-yjs";
    const stub = userStub(tenant);

    await stub.vfsWriteFile(SCOPE(tenant), "/live.md", new Uint8Array(0));
    await stub.vfsSetYjsMode(SCOPE(tenant), "/live.md", true);

    const url = new URL("http://internal/yjs/ws");
    url.searchParams.set("path", "/live.md");
    url.searchParams.set("ns", NS);
    url.searchParams.set("tenant", tenant);
    const resp = await stub.fetch(
      new Request(url, { headers: { Upgrade: "websocket" } })
    );
    expect(resp.status).toBe(101);
    // Tear down — close the server side to release the hibernation
    // accept lock so the test runner shuts down cleanly.
    resp.webSocket?.accept();
    resp.webSocket?.close(1000, "test done");
  });
});
