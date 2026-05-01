import { describe, it, expect, vi, afterEach } from "vitest";
import {
  ctxFromHono,
  generateRequestId,
  logError,
  logInfo,
  logWarn,
  REQUEST_ID_HEADER,
  REQUEST_ID_VAR,
  requestIdMiddleware,
} from "@core/lib/logger";

/**
 * Phase 42 \u2014 structured logger tests.
 *
 * The logger emits JSON-stringified single-line console.* output
 * so Workers Logs / Logpush parses it as structured fields.
 *
 * Cases:
 *   LG1 logInfo emits JSON with ts/level/msg.
 *   LG2 logWarn / logError include error code + msg + stack.
 *   LG3 ctxFromHono extracts requestId + tenantId from a
 *       Hono-shaped context.
 *   LG4 requestIdMiddleware mints + propagates a request-id and
 *       sets the response header.
 *   LG5 requestIdMiddleware honors caller-supplied valid id.
 *   LG6 requestIdMiddleware rejects malformed id (re-mints).
 */

describe("Phase 42 \u2014 structured logger", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("LG1 \u2014 logInfo emits JSON with ts/level/msg", () => {
    const spy = vi.spyOn(console, "log").mockImplementation(() => undefined);
    logInfo("hello", { requestId: "rid-1", tenantId: "default::t1" }, {
      event: "test_event",
      extra: 42,
    });
    expect(spy).toHaveBeenCalledTimes(1);
    const arg = spy.mock.calls[0][0] as string;
    const payload = JSON.parse(arg);
    expect(payload.level).toBe("info");
    expect(payload.msg).toBe("hello");
    expect(payload.requestId).toBe("rid-1");
    expect(payload.tenantId).toBe("default::t1");
    expect(payload.event).toBe("test_event");
    expect(payload.extra).toBe(42);
    expect(typeof payload.ts).toBe("number");
  });

  it("LG2 \u2014 logError surfaces error code + msg + stack", () => {
    const spy = vi.spyOn(console, "error").mockImplementation(() => undefined);
    const err = Object.assign(new Error("boom"), { code: "EBLAMMO" });
    logError("kaboom", { tenantId: "default::t2" }, err, {
      event: "explosion",
    });
    const payload = JSON.parse(spy.mock.calls[0][0] as string);
    expect(payload.level).toBe("error");
    expect(payload.errCode).toBe("EBLAMMO");
    expect(payload.errMsg).toBe("boom");
    expect(typeof payload.errStack).toBe("string");
    expect(payload.event).toBe("explosion");
  });

  it("LG2b \u2014 logWarn emits warn level", () => {
    const spy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    logWarn("careful", {}, { event: "near_cap" });
    expect(spy).toHaveBeenCalledTimes(1);
    const payload = JSON.parse(spy.mock.calls[0][0] as string);
    expect(payload.level).toBe("warn");
    expect(payload.msg).toBe("careful");
  });

  it("LG3 \u2014 ctxFromHono extracts requestId + tenantId from VFSScope", () => {
    const fakeC = {
      var: {
        [REQUEST_ID_VAR]: "abc-123",
        scope: { ns: "default", tenant: "alice", sub: undefined },
      },
      get: () => undefined,
    };
    const ctx = ctxFromHono(fakeC);
    expect(ctx.requestId).toBe("abc-123");
    expect(ctx.tenantId).toBe("default::alice");
  });

  it("LG3b \u2014 ctxFromHono includes sub when present", () => {
    const fakeC = {
      var: {
        [REQUEST_ID_VAR]: "rid-7",
        scope: { ns: "default", tenant: "alice", sub: "bob" },
      },
      get: () => undefined,
    };
    const ctx = ctxFromHono(fakeC);
    expect(ctx.tenantId).toBe("default::alice::bob");
  });

  it("LG3c \u2014 ctxFromHono falls back to userId when no scope", () => {
    const fakeC = {
      var: { [REQUEST_ID_VAR]: "rid-9" },
      get: (k: string) => (k === "userId" ? "user-xyz" : undefined),
    };
    const ctx = ctxFromHono(fakeC);
    expect(ctx.tenantId).toBe("default::user-xyz");
  });

  it("LG4 \u2014 requestIdMiddleware mints + propagates", async () => {
    const sets: Record<string, unknown> = {};
    const headers = new Headers();
    const fakeC = {
      req: { header: () => undefined },
      res: { headers },
      set: (k: string, v: unknown) => {
        sets[k] = v;
      },
    };
    let nextCalled = false;
    const middleware = requestIdMiddleware();
    await middleware(fakeC, async () => {
      nextCalled = true;
    });
    expect(nextCalled).toBe(true);
    const reqId = sets[REQUEST_ID_VAR] as string;
    expect(typeof reqId).toBe("string");
    expect(reqId.length).toBeGreaterThan(0);
    expect(headers.get(REQUEST_ID_HEADER)).toBe(reqId);
  });

  it("LG5 \u2014 requestIdMiddleware honors caller-supplied valid id", async () => {
    const sets: Record<string, unknown> = {};
    const headers = new Headers();
    const incoming = "client-supplied-id-12345";
    const fakeC = {
      req: { header: (h: string) => (h === REQUEST_ID_HEADER ? incoming : undefined) },
      res: { headers },
      set: (k: string, v: unknown) => {
        sets[k] = v;
      },
    };
    await requestIdMiddleware()(fakeC, async () => undefined);
    expect(sets[REQUEST_ID_VAR]).toBe(incoming);
    expect(headers.get(REQUEST_ID_HEADER)).toBe(incoming);
  });

  it("LG6 \u2014 requestIdMiddleware rejects malformed id (re-mints)", async () => {
    const sets: Record<string, unknown> = {};
    const headers = new Headers();
    const malformed = "not<valid>id";
    const fakeC = {
      req: { header: (h: string) => (h === REQUEST_ID_HEADER ? malformed : undefined) },
      res: { headers },
      set: (k: string, v: unknown) => {
        sets[k] = v;
      },
    };
    await requestIdMiddleware()(fakeC, async () => undefined);
    expect(sets[REQUEST_ID_VAR]).not.toBe(malformed);
    expect(typeof sets[REQUEST_ID_VAR]).toBe("string");
  });

  it("LG7 \u2014 generateRequestId produces unique strings", () => {
    const ids = new Set<string>();
    for (let i = 0; i < 32; i++) ids.add(generateRequestId());
    expect(ids.size).toBe(32);
    for (const id of ids) {
      expect(id).toMatch(/^[a-f0-9]+$/i);
    }
  });
});
