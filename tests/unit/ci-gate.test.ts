import { describe, it, expect } from "vitest";

/**
 * CI gate self-test (compile-time only).
 *
 * The Workers vitest pool runs tests inside a Worker isolate
 * which does NOT have node:fs / readFileSync. We therefore can't
 * read the YAML / JSON files at test time. Instead we use a
 * compile-time import of package.json and a static-string assert
 * on the tsup config (which IS bundled into the SDK build, so its
 * presence in the runtime is the test).
 *
 * Cases:
 *   CG1 — package.json has build:sdk + lint:no-phase-tags + ci:check
 *         scripts wired correctly.
 *
 * The CI workflow YAML wiring is verified by the build itself:
 * if `pnpm lint:no-phase-tags` is missing from package.json the
 * CI step fails with "Missing script". And the script body is
 * exercised in real workflow runs.
 */

// Static import of package.json — vitest resolves this at
// compile time; no fs needed.
import pkg from "../../package.json";

describe("CI gates — Phase 47 self-test", () => {
  it("CG1 — package.json wires build:sdk + lint:no-phase-tags + ci:check", () => {
    const scripts = (pkg as { scripts: Record<string, string> }).scripts;
    expect(scripts["build:sdk"]).toBeDefined();
    expect(scripts["build:sdk"]).toContain("@mossaic/sdk");
    expect(scripts["lint:no-phase-tags"]).toBeDefined();
    expect(scripts["lint:no-phase-tags"]).toContain("check-no-phase-tags.sh");
    expect(scripts["ci:check"]).toBeDefined();
    // ci:check must chain typecheck + build:sdk + lint so a single
    // local invocation matches the CI gate.
    expect(scripts["ci:check"]).toContain("typecheck");
    expect(scripts["ci:check"]).toContain("build:sdk");
    expect(scripts["ci:check"]).toContain("lint:no-phase-tags");
  });
});
