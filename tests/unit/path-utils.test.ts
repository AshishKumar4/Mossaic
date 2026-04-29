import { describe, it, expect } from "vitest";
import { pathFromParentId, pathFromFileId } from "../../src/lib/path-utils";

/**
 * Phase 17.6 — `path-utils.ts` tests.
 *
 *   P1. `pathFromParentId(null, "foo.jpg")` returns `/foo.jpg`.
 *   P2. `pathFromParentId("folder-id", "foo.jpg")` returns
 *       `/foo.jpg` (parentId is informational; the App route uses
 *       it from `metadata.parentId`).
 *   P3. `pathFromFileId("01abc...")` returns the fileId unchanged
 *       (the App's download-token route accepts a fileId in the
 *       `path` field).
 */

describe("Phase 17.6 — path/parentId reconciliation", () => {
  it("P1 — pathFromParentId(null, 'foo.jpg') → '/foo.jpg'", () => {
    expect(pathFromParentId(null, "foo.jpg")).toBe("/foo.jpg");
  });

  it("P2 — pathFromParentId('folder-id', 'foo.jpg') → '/foo.jpg' (parentId not encoded in path)", () => {
    expect(pathFromParentId("folder-id-abc", "photo.jpg")).toBe("/photo.jpg");
  });

  it("P3 — pathFromFileId returns the fileId unchanged", () => {
    expect(pathFromFileId("01HQXXYY")).toBe("01HQXXYY");
    expect(pathFromFileId("file-with-dashes-123")).toBe(
      "file-with-dashes-123"
    );
  });
});
