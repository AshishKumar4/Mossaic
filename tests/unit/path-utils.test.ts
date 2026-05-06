import { describe, it, expect } from "vitest";
import { pathFromParentId } from "../../src/lib/path-utils";

/**
 * `path-utils.ts` tests.
 *
 *   P1. `pathFromParentId(null, "foo.jpg")` returns `/foo.jpg`.
 *   P2. `pathFromParentId("folder-id", "foo.jpg")` returns `/foo.jpg`
 *       (parentId is informational at the SPA layer; the canonical
 *       write path keys the row by (user_id, parent_id, file_name)
 *       and the App's /api/index/file callback re-resolves by path).
 */

describe("path-utils", () => {
  it("P1 — pathFromParentId(null, 'foo.jpg') → '/foo.jpg'", () => {
    expect(pathFromParentId(null, "foo.jpg")).toBe("/foo.jpg");
  });

  it("P2 — pathFromParentId('folder-id', 'photo.jpg') → '/photo.jpg' (parentId not encoded in path)", () => {
    expect(pathFromParentId("folder-id-abc", "photo.jpg")).toBe("/photo.jpg");
  });
});
