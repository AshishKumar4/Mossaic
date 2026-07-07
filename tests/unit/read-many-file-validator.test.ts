/**
 * Boundary validator for `readManyFile` HTTP response
 * (`apps/mossaic/shared/vfs-types.ts:parseReadManyFileBytes`).
 *
 * RFC-009: every external boundary must validate, not cast. Server
 * drift (extra fields, missing fields, wrong types) surfaces here as
 * a TypeError instead of corrupting a downstream Uint8Array.
 */
import { describe, expect, it } from "vitest";
import { parseReadManyFileBytes } from "../../shared/vfs-types";

function b64(s: string): string {
  return btoa(s);
}

describe("parseReadManyFileBytes", () => {
  it("decodes base64 entries in order", () => {
    const out = parseReadManyFileBytes({
      bytes: [{ base64: b64("hello") }, { base64: b64("world") }],
    });
    expect(out.length).toBe(2);
    expect(new TextDecoder().decode(out[0]!)).toBe("hello");
    expect(new TextDecoder().decode(out[1]!)).toBe("world");
  });

  it("preserves null entries (missing paths)", () => {
    const out = parseReadManyFileBytes({
      bytes: [null, { base64: b64("x") }, null],
    });
    expect(out[0]).toBeNull();
    expect(new TextDecoder().decode(out[1]!)).toBe("x");
    expect(out[2]).toBeNull();
  });

  it("accepts an empty bytes array", () => {
    expect(parseReadManyFileBytes({ bytes: [] })).toEqual([]);
  });

  it("rejects non-object top-level shapes", () => {
    expect(() => parseReadManyFileBytes(null)).toThrowError(/expected object, got null/);
    expect(() => parseReadManyFileBytes(42)).toThrowError(/expected object, got number/);
    expect(() => parseReadManyFileBytes("oops")).toThrowError(/expected object, got string/);
  });

  it("rejects missing or non-array bytes field", () => {
    expect(() => parseReadManyFileBytes({})).toThrowError(/bytes: expected array/);
    expect(() => parseReadManyFileBytes({ bytes: "nope" })).toThrowError(/bytes: expected array/);
    expect(() => parseReadManyFileBytes({ bytes: { base64: "x" } })).toThrowError(
      /bytes: expected array/,
    );
  });

  it("rejects entry with wrong type", () => {
    expect(() => parseReadManyFileBytes({ bytes: ["raw-string"] })).toThrowError(
      /bytes\[0\]: expected object\|null/,
    );
    expect(() => parseReadManyFileBytes({ bytes: [42] })).toThrowError(
      /bytes\[0\]: expected object\|null/,
    );
  });

  it("rejects entry missing or non-string base64 field", () => {
    expect(() => parseReadManyFileBytes({ bytes: [{}] })).toThrowError(
      /bytes\[0\]\.base64: expected string/,
    );
    expect(() => parseReadManyFileBytes({ bytes: [{ base64: 1 }] })).toThrowError(
      /bytes\[0\]\.base64: expected string/,
    );
    expect(() =>
      parseReadManyFileBytes({ bytes: [{ base64: b64("ok") }, { base64: null }] }),
    ).toThrowError(/bytes\[1\]\.base64: expected string/);
  });
});
