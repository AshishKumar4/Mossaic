import { describe, it, expect } from "vitest";
import { _renderSlotStateForTests } from "@core/objects/user/preview-variants";

/**
 * Preview pre-gen concurrency bound (Phase 23 audit Claim 7).
 *
 * Pre-fix, `preGenerateStandardVariants` would fan out one
 * `renderAndStoreVariant` per variant per file with no upper bound on
 * concurrency across files — a bulk import would 429 the Cloudflare
 * Images binding and silently drop variants.
 *
 * Post-fix, all pre-gen renders pass through a module-level Promise
 * semaphore (`withRenderSlot`) capped at MAX_CONCURRENT_RENDERS.
 *
 * This test pins the public-ish surface of the concurrency limiter:
 *  - The cap is observable for monitoring (`_renderSlotStateForTests`).
 *  - The cap is the audit-recommended value (4-8 → 6).
 *  - Idle state is { inFlight: 0, waiting: 0 }.
 *
 * The end-to-end "200 concurrent finalizes don't all render at once"
 * behaviour is exercised by the existing pre-gen-variants suite via
 * `preview-perf.test.ts`; this file pins the primitive.
 */

describe("preview pre-gen concurrency bound", () => {
  it("publishes a sane MAX_CONCURRENT_RENDERS in the audit-recommended 4-8 range", () => {
    const s = _renderSlotStateForTests();
    expect(s.max).toBeGreaterThanOrEqual(4);
    expect(s.max).toBeLessThanOrEqual(8);
  });

  it("reports zero in-flight + zero waiters at module idle", () => {
    const s = _renderSlotStateForTests();
    expect(s.inFlight).toBe(0);
    expect(s.waiting).toBe(0);
  });
});
