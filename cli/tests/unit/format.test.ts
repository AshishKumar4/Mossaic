import { describe, it, expect } from "vitest";
import { VFSStat } from "@mossaic/sdk/http";
import {
  formatList,
  formatStat,
  formatStatMany,
  formatFindItems,
  formatVersions,
  parseMetadataFlag,
} from "../../src/format.js";

const mkStat = (over: Partial<{ type: "file" | "dir" | "symlink"; mode: number; size: number; mtimeMs: number; uid: number; gid: number; ino: number }> = {}) =>
  new VFSStat({
    type: over.type ?? "file",
    mode: over.mode ?? 0o644,
    size: over.size ?? 100,
    mtimeMs: over.mtimeMs ?? 1700000000000,
    uid: over.uid ?? 1,
    gid: over.gid ?? 1,
    ino: over.ino ?? 12345,
  });

describe("formatList", () => {
  it("plain text: one entry per line", () => {
    expect(formatList(["a", "b"], { json: false })).toBe("a\nb\n");
    expect(formatList([], { json: false })).toBe("");
  });
  it("json: stringified array", () => {
    expect(formatList(["a", "b"], { json: true })).toBe('["a","b"]\n');
  });
});

describe("formatStat", () => {
  it("plain text encodes mode in octal", () => {
    const out = formatStat(mkStat({ mode: 0o600 }), "/x", { json: false });
    expect(out).toMatch(/mode=0600/);
    expect(out).toMatch(/\/x/);
  });
  it("json includes path + size + mtimeMs + ino", () => {
    const out = formatStat(mkStat(), "/x", { json: true });
    const parsed = JSON.parse(out);
    expect(parsed.path).toBe("/x");
    expect(parsed.size).toBe(100);
    expect(parsed.ino).toBe(12345);
    expect(parsed.mtimeMs).toBe(1700000000000);
  });
});

describe("formatStatMany", () => {
  it("nulls render as 'missing'", () => {
    const out = formatStatMany(
      [
        { path: "/a", stat: mkStat() },
        { path: "/b", stat: null },
      ],
      { json: false },
    );
    expect(out).toMatch(/missing\s+\/b/);
  });
  it("json keeps null", () => {
    const out = formatStatMany(
      [{ path: "/a", stat: null }],
      { json: true },
    );
    expect(JSON.parse(out)[0].stat).toBe(null);
  });
});

describe("formatFindItems", () => {
  it("text shows size, path, tags", () => {
    const out = formatFindItems(
      [
        {
          path: "/a",
          pathId: "id1",
          stat: mkStat({ size: 99 }),
          tags: ["x", "y"],
        },
      ],
      { json: false },
    );
    expect(out).toMatch(/99\s+\/a\s+\[x,y\]/);
  });
  it("json strips stat to just size+mtimeMs", () => {
    const out = formatFindItems(
      [
        {
          path: "/a",
          pathId: "id1",
          stat: mkStat(),
          tags: [],
        },
      ],
      { json: true },
    );
    const parsed = JSON.parse(out);
    expect(parsed[0].size).toBe(100);
    expect(parsed[0].pathId).toBe("id1");
  });
});

describe("formatVersions", () => {
  it("text shows id\\tisotime\\tsize\\tflags", () => {
    const out = formatVersions(
      [
        { id: "v1", mtimeMs: 1700000000000, size: 5, mode: 0o644, deleted: false, label: "save", userVisible: true },
      ],
      { json: false },
    );
    expect(out).toMatch(/^v1\t/);
    expect(out).toMatch(/label=save/);
    expect(out).toMatch(/visible/);
  });
});

describe("parseMetadataFlag", () => {
  it("undefined → undefined", () => {
    expect(parseMetadataFlag(undefined)).toBe(undefined);
  });
  it("'null' → null", () => {
    expect(parseMetadataFlag("null")).toBe(null);
  });
  it("valid JSON object", () => {
    expect(parseMetadataFlag('{"a":1}')).toEqual({ a: 1 });
  });
  it("invalid JSON throws", () => {
    expect(() => parseMetadataFlag("not json")).toThrow(/not valid JSON/);
  });
  it("array rejected", () => {
    expect(() => parseMetadataFlag("[1,2]")).toThrow(/object or null/);
  });
});
