import { describe, it, expect } from "vitest";
import { env } from "cloudflare:test";

/**
 * SDK surface for `folderRevision()` and `readManyFile()`.
 *
 * `folderRevision(path)` is the folder-surface cache-bust oracle.
 * `readManyFile(paths)` mirrors `readManyStat` with the 256-path cap.
 */

import { createVFS, type MossaicEnv, type UserDO, EINVAL } from "../../sdk/src/index";

interface E {
  MOSSAIC_USER: DurableObjectNamespace<UserDO>;
  MOSSAIC_SHARD: DurableObjectNamespace;
}
const E = env as unknown as E;

function envFor(): MossaicEnv {
  return {
    MOSSAIC_USER: E.MOSSAIC_USER as MossaicEnv["MOSSAIC_USER"],
    MOSSAIC_SHARD: E.MOSSAIC_SHARD as unknown as MossaicEnv["MOSSAIC_SHARD"],
  };
}

describe("folderRevision SDK method", () => {
  it("returns 0 on fresh tenant for the root folder", async () => {
    const vfs = createVFS(envFor(), { tenant: "fr-sdk-empty" });
    const r = await vfs.folderRevision("/");
    expect(r.revision).toBe(0);
  });

  it("strictly increases after a covered mutation", async () => {
    const vfs = createVFS(envFor(), { tenant: "fr-sdk-bump" });
    await vfs.writeFile("/note.txt", "hi");
    const before = await vfs.folderRevision("/");
    await vfs.chmod("/note.txt", 0o600);
    const after = await vfs.folderRevision("/");
    expect(after.revision).toBeGreaterThan(before.revision);
  });

  it("returns the nested folder's revision when asked for /sub", async () => {
    const vfs = createVFS(envFor(), { tenant: "fr-sdk-nested" });
    await vfs.mkdir("/sub");
    await vfs.writeFile("/sub/a.txt", "hi");
    const before = await vfs.folderRevision("/sub");
    await vfs.chmod("/sub/a.txt", 0o600);
    const after = await vfs.folderRevision("/sub");
    expect(after.revision).toBeGreaterThan(before.revision);
  });

  it("rejects non-folder path with ENOTDIR", async () => {
    const vfs = createVFS(envFor(), { tenant: "fr-sdk-not-dir" });
    await vfs.writeFile("/note.txt", "hi");
    await expect(vfs.folderRevision("/note.txt")).rejects.toThrow();
  });
});

describe("readManyFile SDK method", () => {
  it("returns bytes in order for present paths", async () => {
    const vfs = createVFS(envFor(), { tenant: "rmf-present" });
    await vfs.writeFile("/a.txt", "hello");
    await vfs.writeFile("/b.txt", "world");
    const bytes = await vfs.readManyFile(["/a.txt", "/b.txt"]);
    expect(bytes.length).toBe(2);
    expect(new TextDecoder().decode(bytes[0]!)).toBe("hello");
    expect(new TextDecoder().decode(bytes[1]!)).toBe("world");
  });

  it("returns null for missing paths interleaved with present ones", async () => {
    const vfs = createVFS(envFor(), { tenant: "rmf-mixed" });
    await vfs.writeFile("/a.txt", "hello");
    const bytes = await vfs.readManyFile([
      "/missing.txt",
      "/a.txt",
      "/also-missing.txt",
    ]);
    expect(bytes[0]).toBeNull();
    expect(new TextDecoder().decode(bytes[1]!)).toBe("hello");
    expect(bytes[2]).toBeNull();
  });

  it("returns [] for empty input", async () => {
    const vfs = createVFS(envFor(), { tenant: "rmf-empty" });
    const bytes = await vfs.readManyFile([]);
    expect(bytes).toEqual([]);
  });

  it("rejects >256 paths with EINVAL (mirrors previewInfoMany cap)", async () => {
    const vfs = createVFS(envFor(), { tenant: "rmf-cap" });
    const paths = Array.from({ length: 257 }, (_, i) => `/p${i}.txt`);
    await expect(vfs.readManyFile(paths)).rejects.toBeInstanceOf(EINVAL);
  });
});
