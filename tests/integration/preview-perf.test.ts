import { describe, it, expect } from "vitest";
import { env } from "cloudflare:test";

/**
 * Preview pipeline perf gate.
 *
 * Targets a 100-file gallery cold-render cost. The default
 * registry uses pure-JS renderers when IMAGES is unbound (which
 * the test miniflare omits intentionally — see
 * `tests/wrangler.test.jsonc`); icon-card is the universal
 * fallback. Each cold render takes ~5 ms in workerd; 100 files at
 * ~5 ms/each ≈ 500 ms with serial DO execution.
 */

import type { UserDO } from "@app/objects/user/user-do";

interface E {
  MOSSAIC_USER: DurableObjectNamespace<UserDO>;
}
const E = env as unknown as E;

// Benchmark — logs wall-clock for operator visibility but
// asserts only RELATIVE perf (warm < cold) to stay stable under
// shared-workerd scheduling jitter when the full suite runs.
//
// Documented isolated numbers (last measured against an
// otherwise-empty workerd instance):
//   100-file cold render: 472–586 ms
//   100-file warm render: 270–303 ms
//
// Run with `pnpm test tests/integration/preview-perf.test.ts` for
// the cleanest measurement.

describe("Preview perf gate", () => {
  it("warm path is faster than cold path for 100 files", { timeout: 60_000 }, async () => {
    const stub = E.MOSSAIC_USER.get(E.MOSSAIC_USER.idFromName("perf:p100"));
    const { userId } = await stub.appHandleSignup("perf@e.com", "abcd1234");
    const scope = { ns: "default", tenant: userId };

    // Seed 100 small text files so the icon-card / code-svg renderer fires.
    for (let i = 0; i < 100; i++) {
      await stub.vfsWriteFile(
        scope,
        `/file-${i.toString().padStart(3, "0")}.txt`,
        new TextEncoder().encode(`content-${i}\n`),
        { mimeType: "text/plain" }
      );
    }

    // Cold: each call renders + persists.
    const cold0 = Date.now();
    for (let i = 0; i < 100; i++) {
      await stub.vfsReadPreview(
        scope,
        `/file-${i.toString().padStart(3, "0")}.txt`,
        { variant: "thumb" }
      );
    }
    const coldMs = Date.now() - cold0;
    console.log(`100-file cold render: ${coldMs}ms`);
    // Loose absolute bound — catches catastrophic regressions
    // (e.g. accidental N²) but stays inside the test runner's
    // worst-case jitter envelope on shared workerd. Isolated
    // measurements should beat this by 50×.
    expect(coldMs).toBeLessThan(60_000);

    // Warm: every call hits the variant cache row.
    const warm0 = Date.now();
    for (let i = 0; i < 100; i++) {
      const r = await stub.vfsReadPreview(
        scope,
        `/file-${i.toString().padStart(3, "0")}.txt`,
        { variant: "thumb" }
      );
      expect(r.fromVariantTable).toBe(true);
    }
    const warmMs = Date.now() - warm0;
    console.log(`100-file warm render: ${warmMs}ms`);
    // Warm path skips renderer dispatch entirely (one row lookup +
    // one shard fetch per call). The load-bearing assertion is
    // relative: warm MUST be measurably faster than cold. The
    // exact wall-clock is sensitive to test-runner load, so we
    // tolerate noise but require a meaningful win — at least 25%
    // faster than cold (which corresponds to renderer dispatch
    // being the dominant cost on cold, the load-bearing claim).
    expect(warmMs).toBeLessThan(coldMs * 0.75);
  });
});
