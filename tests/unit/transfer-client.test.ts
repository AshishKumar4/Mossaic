import { describe, it, expect, beforeEach } from "vitest";
import {
  getTransferClient,
  resetTransferClient,
} from "../../src/lib/transfer-client";
import { api } from "../../src/lib/api";

/**
 * `transfer-client.ts` factory tests.
 *
 *   T1. `getTransferClient()` constructs a canonical HttpVFS instance
 *       synchronously; the Bearer token is resolved per-request via the
 *       SDK's `apiKey` callback (which delegates to `api.getVfsToken`).
 *   T2. The cached client is returned on subsequent calls (singleton).
 *   T3. `resetTransferClient()` clears the cache AND the underlying
 *       VFS-token cache so the next call mints a fresh token.
 *   T4. With no App session JWT set, the client builds; the FIRST SDK
 *       call that triggers `getVfsToken()` rejects (auth-bridge needs
 *       the session).
 *   T5. After token expiry, the cached HttpVFS instance is reused but
 *       a NEW token is minted on the next request — the rotation is
 *       transparent to long-lived consumers.
 *
 * `window.location.origin` is stubbed because workerd doesn't expose
 * `window` natively; the SPA factory uses it for the `url` field.
 * The auth-bridge mint endpoint is stubbed via `globalThis.fetch`.
 */

const origGlobal = globalThis as {
  window?: { location: { origin: string } };
  fetch?: typeof fetch;
};
const previousWindow = origGlobal.window;
const previousFetch = origGlobal.fetch;

/**
 * Install a stub for `globalThis.fetch` that responds to the
 * auth-bridge mint with a controllable token + expiry. The stub also
 * records every mint URL hit so tests can assert remint behavior.
 */
function installFetchStub(
  next: () => { token: string; expiresAtMs: number }
): { mintCount: number } {
  const counter = { mintCount: 0 };
  origGlobal.fetch = (async (
    input: RequestInfo | URL,
    _init?: RequestInit
  ): Promise<Response> => {
    const url = typeof input === "string" ? input : input.toString();
    if (url.endsWith("/api/auth/vfs-token")) {
      counter.mintCount++;
      const { token, expiresAtMs } = next();
      return new Response(JSON.stringify({ token, expiresAtMs }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    }
    return new Response("not stubbed", { status: 500 });
  }) as typeof fetch;
  return counter;
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
  it("T1 — getTransferClient builds a canonical HttpVFS synchronously", () => {
    api.setToken("session-jwt-abc");
    const c = getTransferClient();
    expect(c).toBeDefined();
    expect(typeof c.multipartBegin).toBe("function");
  });

  it("T2 — same instance returned on subsequent calls (cached)", () => {
    api.setToken("session-jwt-abc");
    const a = getTransferClient();
    const b = getTransferClient();
    expect(a).toBe(b);
  });

  it("T3 — resetTransferClient drops the cache; next call rebuilds", () => {
    api.setToken("session-jwt-abc");
    const a = getTransferClient();
    resetTransferClient();
    const b = getTransferClient();
    expect(a).not.toBe(b);
  });

  it("T4 — without session JWT, the client builds but the first SDK call rejects", async () => {
    // Build succeeds (no network).
    const c = getTransferClient();
    expect(c).toBeDefined();
    // First SDK call triggers getVfsToken → no session → ApiError.
    await expect(c.stat("/x")).rejects.toThrow();
    restoreGlobals();
  });

  it("T5 — token rotation is transparent: cached client mints a fresh token on next request after expiry", async () => {
    api.setToken("session-jwt-abc");
    let n = 0;
    const counter = installFetchStub(() => {
      n++;
      // First call: 1ms TTL (immediately stale). Second call: future.
      return n === 1
        ? { token: `vfs-${n}`, expiresAtMs: Date.now() + 1 }
        : { token: `vfs-${n}`, expiresAtMs: Date.now() + 15 * 60 * 1000 };
    });

    // Resolve the token directly (simulating an SDK request).
    const t1 = await api.getVfsToken();
    expect(t1).toBe("vfs-1");
    expect(counter.mintCount).toBe(1);

    // Wait past the 1ms TTL; the cache should re-mint.
    await new Promise((r) => setTimeout(r, 100));
    const t2 = await api.getVfsToken();
    expect(t2).toBe("vfs-2");
    expect(counter.mintCount).toBe(2);

    // The cached client itself is still the same instance.
    const c1 = getTransferClient();
    const c2 = getTransferClient();
    expect(c1).toBe(c2);
    restoreGlobals();
  });
});
