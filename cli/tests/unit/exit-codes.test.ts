import { describe, it, expect } from "vitest";
import {
  ENOENT,
  EACCES,
  EINVAL,
  EAGAIN,
  MossaicUnavailableError,
} from "@mossaic/sdk/http";
import { exitCodeFor, formatError } from "../../src/exit-codes.js";

describe("exit code mapping", () => {
  it("VFSError → 1", () => {
    expect(exitCodeFor(new ENOENT())).toBe(1);
    expect(exitCodeFor(new EACCES())).toBe(1);
    expect(exitCodeFor(new EINVAL())).toBe(1);
    expect(exitCodeFor(new EAGAIN())).toBe(1);
  });

  it("MossaicUnavailableError → 2", () => {
    expect(exitCodeFor(new MossaicUnavailableError())).toBe(2);
  });

  it("plain Error → 1", () => {
    expect(exitCodeFor(new Error("boom"))).toBe(1);
  });

  it("non-error → 1", () => {
    expect(exitCodeFor("string thrown")).toBe(1);
    expect(exitCodeFor(undefined)).toBe(1);
    expect(exitCodeFor(null)).toBe(1);
  });
});

describe("formatError", () => {
  it("prefixes ENOENT with code + path", () => {
    const out = formatError(new ENOENT({ syscall: "stat", path: "/x" }));
    expect(out).toMatch(/ENOENT/);
    expect(out).toMatch(/'\/x'/);
    expect(out.startsWith("mossaic:")).toBe(true);
  });

  it("formats plain Error as 'mossaic: <msg>'", () => {
    expect(formatError(new Error("boom"))).toBe("mossaic: boom");
  });

  it("formats unknown thrown value", () => {
    expect(formatError(42)).toBe("mossaic: 42");
  });
});
