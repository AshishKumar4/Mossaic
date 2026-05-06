import { describe, it, expect } from "vitest";
import { jwtVerify, errors as joseErrors } from "jose";
import { mintToken } from "../../src/jwt.js";

/**
 * CLI JWT ms-vs-seconds regression test.
 *
 * Pre-fix: `cli/src/jwt.ts:69` did
 *
 *   .setExpirationTime(Date.now() + (input.ttlMs ?? 60 * 60 * 1000))
 *
 * jose's `setExpirationTime(numeric)` interprets a numeric input as
 * seconds-since-epoch (RFC 7519 `exp` claim). `Date.now()` returns
 * milliseconds, so the resulting `exp` claim was ~1000× larger than
 * intended — putting expiration in the year ~57000 and rendering
 * the `--ttl` flag effectively decorative. The worker-side
 * `signVFSToken` always did the right thing
 * (`Math.floor((Date.now() + ttlMs) / 1000)`); this regression
 * pins the CLI to the same shape.
 *
 * Cases:
 *   T1. 1h TTL emits `exp` ~3600 seconds from now (NOT ~3.6e6).
 *   T2. Token verifies before TTL elapses (positive control).
 *   T3. Token rejected after TTL elapses (uses an explicit `currentDate`
 *       passed to jose's verify — avoids real-time mocking which is
 *       brittle in the workers / threads pool).
 */

describe("CLI JWT ttlMs is interpreted in seconds (T1, T2, T3, T4)", () => {
  const SECRET = "test-secret-for-jwt-ms-fix-32-chars-minimum!";

  it("T1 — 1h TTL emits exp ~3600 seconds from now (not ~3.6e6)", async () => {
    const tokenIssuedAtSec = Math.floor(Date.now() / 1000);
    const tok = await mintToken({
      secret: SECRET,
      ns: "default",
      tenant: "t1",
      ttlMs: 60 * 60 * 1000, // 1 hour
    });
    const { payload } = await jwtVerify(tok, new TextEncoder().encode(SECRET));
    const expSec = payload.exp as number;
    const ttlSec = expSec - tokenIssuedAtSec;

    // Pre-fix: ttlSec would have been ~3.6e9 (because exp was ms-shaped).
    // Post-fix: ttlSec is ~3600 ± clock-skew jitter.
    expect(ttlSec).toBeGreaterThanOrEqual(3590);
    expect(ttlSec).toBeLessThanOrEqual(3610);

    // Hard sanity: exp must NOT be ms-shaped. Today's ms-since-epoch
    // is ~1.7e12; today's seconds-since-epoch is ~1.7e9. Anything
    // above 1e11 means we shipped milliseconds (year-5138 ceiling).
    expect(expSec).toBeLessThan(1e11);
  });

  it("T2 — token verifies before TTL elapses (positive control)", async () => {
    const tok = await mintToken({
      secret: SECRET,
      ns: "default",
      tenant: "t2",
      ttlMs: 60 * 60 * 1000, // 1 hour
    });
    // Default verify uses the current clock — 1h is well in the future.
    const { payload } = await jwtVerify(tok, new TextEncoder().encode(SECRET));
    expect(payload.scope).toBe("vfs");
    expect(payload.tn).toBe("t2");
  });

  it("T3 — token rejected after TTL elapses (jose JWTExpired)", async () => {
    const tok = await mintToken({
      secret: SECRET,
      ns: "default",
      tenant: "t3",
      ttlMs: 5_000, // 5 seconds
    });
    // Verify against a timestamp 1 hour in the future — far past
    // the 5-second TTL. jose throws `JWTExpired` when `currentDate`
    // is after `exp`.
    const future = new Date(Date.now() + 60 * 60 * 1000);
    let caught: unknown = null;
    try {
      await jwtVerify(tok, new TextEncoder().encode(SECRET), {
        currentDate: future,
      });
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(joseErrors.JWTExpired);
  });

  it("T4 — pre-fix ms-shaped exp claim would NOT have triggered T3 (sanity check)", async () => {
    // This case documents the pre-fix bug: an ms-shaped exp claim
    // (exp ~= Date.now() in milliseconds) is interpreted by jose as
    // a seconds-since-epoch number ~year-57000. A `currentDate` 1h
    // in the future is still microscopic compared to year-57000,
    // so verify SUCCEEDS — the token effectively never expires.
    //
    // We construct that pre-fix shape manually (NOT via mintToken,
    // which is now fixed) using jose's SignJWT directly.
    const { SignJWT } = await import("jose");
    const tok = await new SignJWT({ scope: "vfs", ns: "default", tn: "t4" })
      .setProtectedHeader({ alg: "HS256" })
      .setIssuedAt()
      .setExpirationTime(Date.now() + 5_000) // BUG: ms-shaped (year 57000)
      .sign(new TextEncoder().encode(SECRET));

    // 1h-future verify should PASS against the ms-shaped exp.
    const future = new Date(Date.now() + 60 * 60 * 1000);
    const { payload } = await jwtVerify(
      tok,
      new TextEncoder().encode(SECRET),
      { currentDate: future }
    );
    expect(payload.scope).toBe("vfs");

    // Even a 100-day-future verify would pass — pinning that the
    // pre-fix shape's "expiration" is in a far-future year.
    const tenYears = new Date(Date.now() + 10 * 365 * 24 * 60 * 60 * 1000);
    const { payload: p2 } = await jwtVerify(
      tok,
      new TextEncoder().encode(SECRET),
      { currentDate: tenYears }
    );
    expect(p2.scope).toBe("vfs");
  });
});
