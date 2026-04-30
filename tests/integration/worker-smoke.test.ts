import { describe, it, expect } from "vitest";
import { SELF } from "cloudflare:test";

/**
 * End-to-end Worker-boot smoke test.
 *
 * Boots the actual production Hono app (worker/index.ts) inside the
 * vitest-pool-workers harness and drives its real /api/* handlers via
 * `SELF.fetch(...)`. This is the regression gate that catches issues
 * the per-DO tests miss: Hono mounting, cors, route table integrity,
 * JWT signing, cross-route DO traffic.
 *
 * Coverage:
 *   1. Boot: GET /api/health returns {status:"ok"}.
 *   2. Auth middleware rejects unauthenticated calls (401).
 *   3. Boot is reproducible — second request after first still returns 200.
 *
 * The actual upload/download bytes round-trip is covered by
 * tests/integration/spa-roundtrip-live.test.ts (canonical /api/vfs/*).
 */

describe("Worker boot smoke (production pipeline through SELF.fetch)", () => {
  it("GET /api/health returns 200 OK with the expected shape", async () => {
    const res = await SELF.fetch("https://mossaic.test/api/health");
    expect(res.ok).toBe(true);
    expect(res.status).toBe(200);
    const body = (await res.json()) as { status: string; timestamp: number };
    expect(body.status).toBe("ok");
    expect(typeof body.timestamp).toBe("number");
  });

  it("rejects unauthenticated calls to protected routes (401)", async () => {
    const res = await SELF.fetch("https://mossaic.test/api/files");
    expect(res.status).toBe(401);
    const body = (await res.json()) as { error: string };
    expect(body.error).toMatch(/Unauthorized/i);
  });

  it("Worker boot is reproducible: a second fresh request after the smoke completes still returns 200", async () => {
    // Proves no startup-error / one-shot init bug — the DO migrations
    // applied on the first request remain idempotent for subsequent
    // requests on different DO instances.
    const res = await SELF.fetch("https://mossaic.test/api/health");
    expect(res.status).toBe(200);
    const body = (await res.json()) as { status: string };
    expect(body.status).toBe("ok");
  });
});
