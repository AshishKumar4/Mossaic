import { describe, it, expect, beforeEach } from "vitest";
import {
  getTransferClient,
  resetTransferClient,
} from "../../src/lib/transfer-client";
import { api } from "../../src/lib/api";

/**
 * `transfer-client.ts` factory tests.
 *
 *   T1. `getTransferClient()` constructs a canonical HttpVFS that
 *       targets `/api/vfs/multipart/*` and `/api/vfs/chunk/*`.
 *   T2. The cached client is returned on subsequent calls (singleton).
 *   T3. `resetTransferClient()` clears the cache; the next call
 *       rebuilds against a fresh VFS token.
 *   T4. `getTransferClient()` rejects when no App session JWT is
 *       set on `api` (auth-bridge mint requires the session).
 *
 * The factory is async because the auth-bridge mint requires a
 * network round-trip. The token round-trip is stubbed via a
 * `globalThis.fetch` shim so the test runs without network.
 *
 * `window.location.origin` is stubbed because workerd doesn't
 * expose `window` natively; the SPA factory uses it for the `url`
 * field.
 */

const origGlobal = globalThis as {
  window?: { location: { origin: string } };
  fetch?: typeof fetch;
};
const previousWindow = origGlobal.window;
const previousFetch = origGlobal.fetch;

/**
 * Install a minimal stub for `globalThis.fetch` that responds to the
 * auth-bridge mint endpoint (`/api/auth/vfs-token`) with a
 * deterministic token + future expiry. Other URLs are unhandled
 * (the tests below don't exercise them).
 */
function installFetchStub(token: string, expiresAtMs: number): void {
  origGlobal.fetch = (async (
    input: RequestInfo | URL,
    _init?: RequestInit
  ): Promise<Response> => {
    const url = typeof input === "string" ? input : input.toString();
    if (url.endsWith("/api/auth/vfs-token")) {
      return new Response(JSON.stringify({ token, expiresAtMs }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    }
    return new Response("not stubbed", { status: 500 });
  }) as typeof fetch;
}

beforeEach(() => {
  origGlobal.window = { location: { origin: "https://app.example.com" } };
  resetTransferClient();
  api.setToken(null);
});

// Restore after suite (tests run sequentially in vitest-pool-workers).
const restoreGlobals = (): void => {
  if (previousWindow === undefined) delete origGlobal.window;
  else origGlobal.window = previousWindow;
  if (previousFetch === undefined) delete origGlobal.fetch;
  else origGlobal.fetch = previousFetch;
};

describe("transfer-client — getTransferClient / resetTransferClient", () => {
  it("T1 — getTransferClient builds a canonical HttpVFS authenticated by a bridge-minted VFS token", async () => {
    api.setToken("session-jwt-abc");
    installFetchStub("vfs-token-xyz", Date.now() + 15 * 60 * 1000);
    const c = await getTransferClient();
    expect(c).toBeDefined();
    expect(typeof c.multipartBegin).toBe("function");
  });

  it("T2 — same instance returned on subsequent calls (cached)", async () => {
    api.setToken("session-jwt-abc");
    installFetchStub("vfs-token-xyz", Date.now() + 15 * 60 * 1000);
    const a = await getTransferClient();
    const b = await getTransferClient();
    expect(a).toBe(b);
  });

  it("T3 — resetTransferClient drops the cache; next call rebuilds with a fresh token", async () => {
    api.setToken("session-jwt-abc");
    installFetchStub("vfs-token-1", Date.now() + 15 * 60 * 1000);
    const a = await getTransferClient();
    // Reset and re-mint with a different token; the next
    // getTransferClient must build a NEW HttpVFS instance.
    resetTransferClient();
    installFetchStub("vfs-token-2", Date.now() + 15 * 60 * 1000);
    const b = await getTransferClient();
    expect(a).not.toBe(b);
  });

  it("T4 — getTransferClient rejects when no App session JWT is set", async () => {
    api.setToken(null);
    await expect(getTransferClient()).rejects.toThrow();
    restoreGlobals();
  });
});
