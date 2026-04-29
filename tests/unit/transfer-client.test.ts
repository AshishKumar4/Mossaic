import { describe, it, expect, beforeEach, vi } from "vitest";
import {
  getTransferClient,
  resetTransferClient,
} from "../../src/lib/transfer-client";
import { api } from "../../src/lib/api";

/**
 * Phase 17.6 — `transfer-client.ts` factory tests.
 *
 *   T1. `getTransferClient()` constructs an HttpVFS configured for
 *       the App's `/api/upload/multipart` and `/api/download` routes.
 *   T2. The cached client is returned on subsequent calls (singleton).
 *   T3. `resetTransferClient()` clears the cache; the next call
 *       rebuilds against the current `api.getToken()`.
 *   T4. `getTransferClient()` throws when no token is set on `api`.
 *
 * The tests stub `window.location.origin` because the SPA factory
 * uses it for the `url` field. workerd doesn't expose `window`
 * natively; we provide a minimal stand-in.
 */

// Inject a `window` stub so `window.location.origin` works in workerd.
const origGlobal = globalThis as { window?: { location: { origin: string } } };
const previousWindow = origGlobal.window;
beforeEach(() => {
  origGlobal.window = { location: { origin: "https://app.example.com" } };
  resetTransferClient();
  api.setToken(null);
});

// Restore after the suite — tests run sequentially in vitest-pool-workers.
const restoreWindow = (): void => {
  if (previousWindow === undefined) {
    delete origGlobal.window;
  } else {
    origGlobal.window = previousWindow;
  }
};

describe("Phase 17.6 — getTransferClient / resetTransferClient", () => {
  it("T1 — getTransferClient builds an HttpVFS against the App's JWT + override routes", () => {
    api.setToken("test-jwt-abc");
    const c = getTransferClient();
    expect(c).toBeDefined();
    expect(typeof c.multipartBegin).toBe("function");
    // Confirm the override is in effect by inspecting what the
    // client's URL builder produces. The internal routing isn't
    // public, so we exercise it via a second test (B2 in
    // http-base-override.test.ts) — here we confirm the factory
    // constructed the client without throwing.
  });

  it("T2 — same instance returned on subsequent calls (cached)", () => {
    api.setToken("test-jwt-abc");
    const a = getTransferClient();
    const b = getTransferClient();
    expect(a).toBe(b);
  });

  it("T3 — resetTransferClient drops the cache; next call rebuilds with current token", () => {
    api.setToken("token-1");
    const a = getTransferClient();
    resetTransferClient();
    api.setToken("token-2");
    const b = getTransferClient();
    expect(a).not.toBe(b);
  });

  it("T4 — getTransferClient throws when no token is set", () => {
    api.setToken(null);
    expect(() => getTransferClient()).toThrow(/no API token/);
    restoreWindow();
  });
});
