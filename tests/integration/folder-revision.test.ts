import { describe, it, expect } from "vitest";
import { env } from "cloudflare:test";

/**
 * Phase 46 — `listChildren.revision` per-folder mutation counter.
 *
 * Pinned invariants:
 *   FR1.  writeFile bumps the parent folder's revision.
 *   FR2.  unlink bumps the parent folder's revision.
 *   FR3.  mkdir bumps the parent folder's revision (and the new
 *         folder's own revision starts at 0).
 *   FR4.  rmdir bumps the parent folder's revision.
 *   FR5.  rename within the same folder bumps once; cross-folder
 *         rename bumps both src and dst parents.
 *   FR6.  archive / unarchive bump the file's parent folder.
 *   FR7.  Strict-monotonic: each bump increments by exactly 1, no
 *         skips, no duplicates.
 *
 * The counter starts at 0 for any new folder and is observable via
 * `listChildren(...).revision`. Two reads of the same folder with
 * no intervening mutation MUST return the same revision.
 */

import {
  createVFS,
  type MossaicEnv,
  type UserDO,
} from "../../sdk/src/index";
import { vfsUserDOName } from "@core/lib/utils";

interface E {
  MOSSAIC_USER: DurableObjectNamespace<UserDO>;
  MOSSAIC_SHARD: DurableObjectNamespace;
}
const E = env as unknown as E;
const NS = "default";

function envFor(): MossaicEnv {
  return {
    MOSSAIC_USER: E.MOSSAIC_USER as MossaicEnv["MOSSAIC_USER"],
    MOSSAIC_SHARD: E.MOSSAIC_SHARD as unknown as MossaicEnv["MOSSAIC_SHARD"],
  };
}

async function rev(
  vfs: ReturnType<typeof createVFS>,
  path: string
): Promise<number> {
  const r = await vfs.listChildren(path);
  return r.revision;
}

describe("folder revision — mutation bumps (FR1..FR7)", () => {
  it("FR1 — writeFile bumps parent revision", async () => {
    const vfs = createVFS(envFor(), { tenant: "p46-fr-write" });
    await vfs.mkdir("/d");
    const r0 = await rev(vfs, "/d");
    await vfs.writeFile("/d/a.txt", "x");
    const r1 = await rev(vfs, "/d");
    expect(r1).toBe(r0 + 1);
  });

  it("FR2 — unlink bumps parent revision", async () => {
    const vfs = createVFS(envFor(), { tenant: "p46-fr-unlink" });
    await vfs.mkdir("/d");
    await vfs.writeFile("/d/a.txt", "x");
    const before = await rev(vfs, "/d");
    await vfs.unlink("/d/a.txt");
    const after = await rev(vfs, "/d");
    expect(after).toBe(before + 1);
  });

  it("FR3 — mkdir bumps parent revision; new folder starts at 0", async () => {
    const vfs = createVFS(envFor(), { tenant: "p46-fr-mkdir" });
    const rootBefore = await rev(vfs, "/");
    await vfs.mkdir("/d");
    const rootAfter = await rev(vfs, "/");
    expect(rootAfter).toBe(rootBefore + 1);
    const newFolderRev = await rev(vfs, "/d");
    expect(newFolderRev).toBe(0);
  });

  it("FR4 — rmdir bumps parent revision", async () => {
    const vfs = createVFS(envFor(), { tenant: "p46-fr-rmdir" });
    await vfs.mkdir("/d/sub", { recursive: true });
    const before = await rev(vfs, "/d");
    await vfs.rmdir("/d/sub");
    const after = await rev(vfs, "/d");
    expect(after).toBe(before + 1);
  });

  it("FR5a — rename within same folder bumps once", async () => {
    const vfs = createVFS(envFor(), { tenant: "p46-fr-rename-same" });
    await vfs.mkdir("/d");
    await vfs.writeFile("/d/a.txt", "x");
    const before = await rev(vfs, "/d");
    await vfs.rename("/d/a.txt", "/d/b.txt");
    const after = await rev(vfs, "/d");
    expect(after).toBe(before + 1);
  });

  it("FR5b — cross-folder rename bumps both parents", async () => {
    const vfs = createVFS(envFor(), { tenant: "p46-fr-rename-cross" });
    await vfs.mkdir("/src");
    await vfs.mkdir("/dst");
    await vfs.writeFile("/src/a.txt", "x");
    const srcBefore = await rev(vfs, "/src");
    const dstBefore = await rev(vfs, "/dst");
    await vfs.rename("/src/a.txt", "/dst/a.txt");
    const srcAfter = await rev(vfs, "/src");
    const dstAfter = await rev(vfs, "/dst");
    expect(srcAfter).toBe(srcBefore + 1);
    expect(dstAfter).toBe(dstBefore + 1);
  });

  it("FR6 — archive/unarchive bump parent revision", async () => {
    const vfs = createVFS(envFor(), { tenant: "p46-fr-archive" });
    await vfs.mkdir("/d");
    await vfs.writeFile("/d/a.txt", "x");
    const before = await rev(vfs, "/d");
    await vfs.archive("/d/a.txt");
    const afterArchive = await rev(vfs, "/d");
    expect(afterArchive).toBe(before + 1);
    await vfs.unarchive("/d/a.txt");
    const afterUnarchive = await rev(vfs, "/d");
    expect(afterUnarchive).toBe(afterArchive + 1);
  });

  it("FR7 — strict-monotonic: 5 mutations produce 5 consecutive revisions", async () => {
    const vfs = createVFS(envFor(), { tenant: "p46-fr-mono" });
    await vfs.mkdir("/d");
    const start = await rev(vfs, "/d");
    await vfs.writeFile("/d/a.txt", "1");
    await vfs.writeFile("/d/b.txt", "2");
    await vfs.writeFile("/d/c.txt", "3");
    await vfs.unlink("/d/a.txt");
    await vfs.rename("/d/b.txt", "/d/bb.txt");
    const end = await rev(vfs, "/d");
    expect(end).toBe(start + 5);
  });

  it("FR-stable — two reads with no intervening mutation return identical revision", async () => {
    const vfs = createVFS(envFor(), { tenant: "p46-fr-stable" });
    await vfs.mkdir("/d");
    await vfs.writeFile("/d/a.txt", "1");
    const r1 = await rev(vfs, "/d");
    const r2 = await rev(vfs, "/d");
    expect(r2).toBe(r1);
  });
});
