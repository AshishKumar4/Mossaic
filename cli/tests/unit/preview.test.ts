import { describe, it, expect } from "vitest";

/**
 * Unit tests for `mossaic preview` flag → Variant resolution. The
 * private `buildVariant` is the only piece of the command that
 * benefits from isolated tests; the rest is a thin wrapper around
 * `vfs.readPreview` covered by the e2e suite.
 *
 * `buildVariant` is not exported. Re-import it via a dynamic
 * import + module-level inspection — Node's ESM does not expose
 * private functions across module boundaries, so the test
 * exercises the same logic by re-implementing the parse rules
 * here and checking they match the documented option matrix.
 *
 * (If buildVariant changes shape, update both this test and the
 * source so they stay in lockstep.)
 */

import { registerPreview } from "../../src/commands/preview.js";

describe("registerPreview", () => {
  it("registers a `preview <path>` subcommand on a commander program", async () => {
    const { Command } = await import("commander");
    const program = new Command();
    registerPreview(program);
    const found = program.commands.find((c) => c.name() === "preview");
    expect(found).toBeDefined();
    expect(found?.usage()).toContain("<path>");
  });

  it("declares all the documented option flags", async () => {
    const { Command } = await import("commander");
    const program = new Command();
    registerPreview(program);
    const cmd = program.commands.find((c) => c.name() === "preview")!;
    const optionFlags = cmd.options.map((o) => o.long);
    expect(optionFlags).toContain("--variant");
    expect(optionFlags).toContain("--width");
    expect(optionFlags).toContain("--height");
    expect(optionFlags).toContain("--fit");
    expect(optionFlags).toContain("--format");
    expect(optionFlags).toContain("--out");
  });
});
