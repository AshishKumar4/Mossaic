import { describe, it, expect } from "vitest";
import { getPlacement } from "@core/lib/placement-resolver";
import { canonicalPlacement } from "@shared/placement";

/**
 * Phase 17.5 — placement-resolver dispatch tests.
 *
 * The resolver unconditionally returns `canonicalPlacement` for the v1
 * implementation. Every server-side site that consults this resolver
 * is canonical-by-construction. The legacy App routes call
 * `legacyAppPlacement.*` directly (no resolver round-trip).
 *
 *   R1.  App-shaped scope still resolves to canonicalPlacement (the
 *        canonical sites never serve App routes; a "legacy-shaped" scope
 *        showing up at a canonical site means a test or SDK consumer
 *        deliberately constructed it).
 *   R2.  SDK-shaped scope resolves to canonicalPlacement.
 *   R3.  Custom-namespace scope resolves to canonicalPlacement.
 *   R4.  Sub-tenant scope resolves to canonicalPlacement.
 *   R5.  Resolver is pure: same scope returns the same Placement
 *        reference (canonicalPlacement is a singleton).
 *
 * If/when Phase 18 introduces config-driven dispatch (e.g. an
 * `env.PLACEMENT_RULES` lookup), R1–R4 may diverge. v1 is
 * intentionally simple to keep the canonical site behavior
 * byte-identical with the pre-17.5 inline `vfsShardDOName(...)` calls.
 */

describe("Phase 17.5 — getPlacement(scope)", () => {
  it("R1 — App-shaped scope resolves to canonicalPlacement (canonical sites never serve App routes)", () => {
    const scope = { ns: "default", tenant: "user-123" };
    expect(getPlacement(scope)).toBe(canonicalPlacement);
  });

  it("R2 — SDK-shaped scope resolves to canonicalPlacement", () => {
    const scope = { ns: "default", tenant: "acme", sub: "alice" };
    expect(getPlacement(scope)).toBe(canonicalPlacement);
  });

  it("R3 — custom-namespace scope resolves to canonicalPlacement", () => {
    const scope = { ns: "prod", tenant: "acme" };
    expect(getPlacement(scope)).toBe(canonicalPlacement);
  });

  it("R4 — sub-tenant scope resolves to canonicalPlacement", () => {
    const scope = { ns: "default", tenant: "user-123", sub: "subuser" };
    expect(getPlacement(scope)).toBe(canonicalPlacement);
  });

  it("R5 — resolver is pure: same scope returns the same singleton", () => {
    const scope = { ns: "default", tenant: "alice" };
    expect(getPlacement(scope)).toBe(getPlacement(scope));
    expect(getPlacement(scope)).toBe(canonicalPlacement);
  });
});
