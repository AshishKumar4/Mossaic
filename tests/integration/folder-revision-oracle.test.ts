import { describe, it, expect } from "vitest";
import { env, runInDurableObject } from "cloudflare:test";

/**
 * Folder-revision oracle completeness.
 *
 * Every attribute mutation that affects a folder listing's
 * `vfsListChildren` / `vfsListFiles` / `vfsReadManyStat` response MUST
 * bump the parent folder's `revision` counter. Without this the
 * folder-surface cache will serve stale entries after an in-place
 * mutation that doesn't move bytes.
 *
 * FR8..FR14 below pin one gap each.
 */

import { createVFS, type MossaicEnv, type UserDO } from "../../sdk/src/index";
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
function userStub(tenant: string) {
  return E.MOSSAIC_USER.get(E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant)));
}

// Root-folder revision lives in its own table; the synthetic `parent_id IS NULL`
// rows don't carry counters. Read both shapes so tests can target either.
async function readRootRevision(tenant: string): Promise<number> {
  return runInDurableObject(userStub(tenant), async (_inst, state) => {
    const r = state.storage.sql
      .exec("SELECT revision FROM root_folder_revision WHERE user_id = ?", tenant)
      .toArray()[0] as { revision: number } | undefined;
    return r?.revision ?? 0;
  });
}

async function readFolderRevisionByPath(tenant: string, name: string): Promise<number> {
  return runInDurableObject(userStub(tenant), async (_inst, state) => {
    const r = state.storage.sql
      .exec(
        "SELECT revision FROM folders WHERE user_id = ? AND name = ? AND parent_id IS NULL",
        tenant,
        name,
      )
      .toArray()[0] as { revision: number } | undefined;
    return r?.revision ?? 0;
  });
}

async function readFileUpdatedAt(tenant: string, fileName: string): Promise<number> {
  return runInDurableObject(userStub(tenant), async (_inst, state) => {
    const r = state.storage.sql
      .exec(
        "SELECT updated_at FROM files WHERE user_id = ? AND file_name = ?",
        tenant,
        fileName,
      )
      .toArray()[0] as { updated_at: number } | undefined;
    return r?.updated_at ?? 0;
  });
}

describe("folder-revision oracle: attribute mutations bump parent (FR8..FR14)", () => {
  it("FR8: chmod on a root-level file bumps root revision", async () => {
    const tenant = "fr8-chmod";
    const vfs = createVFS(envFor(), { tenant });
    await vfs.writeFile("/note.txt", "hi");
    const before = await readRootRevision(tenant);
    await vfs.chmod("/note.txt", 0o600);
    const after = await readRootRevision(tenant);
    expect(after).toBeGreaterThan(before);
  });

  it("FR9: patchMetadata bumps parent revision", async () => {
    const tenant = "fr9-patch-meta";
    const vfs = createVFS(envFor(), { tenant });
    await vfs.writeFile("/note.txt", "hi");
    const before = await readRootRevision(tenant);
    await vfs.patchMetadata("/note.txt", { project: "alpha" });
    const after = await readRootRevision(tenant);
    expect(after).toBeGreaterThan(before);
  });

  it("FR10: patchMetadata addTags bumps parent + files.updated_at", async () => {
    const tenant = "fr10-add-tags";
    const vfs = createVFS(envFor(), { tenant });
    await vfs.writeFile("/note.txt", "hi");
    const revBefore = await readRootRevision(tenant);
    const updBefore = await readFileUpdatedAt(tenant, "note.txt");
    // Add a 2ms wait to guarantee Date.now() advances under coarse clocks.
    await new Promise((r) => setTimeout(r, 2));
    await vfs.patchMetadata("/note.txt", undefined, { addTags: ["urgent"] });
    const revAfter = await readRootRevision(tenant);
    const updAfter = await readFileUpdatedAt(tenant, "note.txt");
    expect(revAfter).toBeGreaterThan(revBefore);
    expect(updAfter).toBeGreaterThan(updBefore);
  });

  it("FR11: patchMetadata removeTags bumps parent + files.updated_at", async () => {
    const tenant = "fr11-remove-tags";
    const vfs = createVFS(envFor(), { tenant });
    await vfs.writeFile("/note.txt", "hi", { tags: ["urgent"] });
    const revBefore = await readRootRevision(tenant);
    const updBefore = await readFileUpdatedAt(tenant, "note.txt");
    await new Promise((r) => setTimeout(r, 2));
    await vfs.patchMetadata("/note.txt", undefined, { removeTags: ["urgent"] });
    const revAfter = await readRootRevision(tenant);
    const updAfter = await readFileUpdatedAt(tenant, "note.txt");
    expect(revAfter).toBeGreaterThan(revBefore);
    expect(updAfter).toBeGreaterThan(updBefore);
  });

  it("FR12: markVersion(label) bumps parent revision", async () => {
    const tenant = "fr12-mark-version";
    const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });
    await vfs.writeFile("/note.txt", "hi");
    const all = await vfs.listVersions("/note.txt");
    expect(all.length).toBeGreaterThan(0);
    const before = await readRootRevision(tenant);
    await vfs.markVersion("/note.txt", all[0].id, { label: "first" });
    const after = await readRootRevision(tenant);
    expect(after).toBeGreaterThan(before);
  });

  it("FR13: setYjsMode promotion bumps parent revision", async () => {
    const tenant = "fr13-yjs-mode";
    const vfs = createVFS(envFor(), { tenant });
    await vfs.writeFile("/doc.md", "hi");
    const before = await readRootRevision(tenant);
    await userStub(tenant).vfsSetYjsMode({ ns: NS, tenant }, "/doc.md", true);
    const after = await readRootRevision(tenant);
    expect(after).toBeGreaterThan(before);
  });

  it("FR14: chmod on a nested folder bumps the GRANDPARENT (folder's parent)", async () => {
    // The folder is the mutated child; its parent's listing changes because
    // the folder row's `mode` is part of stat returned by readManyStat.
    const tenant = "fr14-chmod-folder";
    const vfs = createVFS(envFor(), { tenant });
    await vfs.mkdir("/d");
    const before = await readRootRevision(tenant);
    await vfs.chmod("/d", 0o700);
    const after = await readRootRevision(tenant);
    expect(after).toBeGreaterThan(before);
  });

  it("removeRecursive bumps rmrf-root's parent; post-rmrf read sees fresh", async () => {
    const tenant = "fr-rmrf";
    const vfs = createVFS(envFor(), { tenant });
    await vfs.mkdir("/tree/sub", { recursive: true });
    await vfs.writeFile("/tree/sub/a.txt", "a");
    await vfs.writeFile("/tree/sub/b.txt", "b");
    const rootBefore = await readRootRevision(tenant);
    await vfs.removeRecursive("/tree");
    const rootAfter = await readRootRevision(tenant);
    expect(rootAfter).toBeGreaterThan(rootBefore);
    expect(await vfs.exists("/tree")).toBe(false);
  });

  it("chained mutations produce strictly monotonic revisions (non-vacuity)", async () => {
    const tenant = "fr-chain";
    const vfs = createVFS(envFor(), { tenant });
    await vfs.writeFile("/a.txt", "hi");
    const r0 = await readRootRevision(tenant);
    await vfs.chmod("/a.txt", 0o600);
    const r1 = await readRootRevision(tenant);
    await vfs.patchMetadata("/a.txt", { x: 1 });
    const r2 = await readRootRevision(tenant);
    await vfs.patchMetadata("/a.txt", undefined, { addTags: ["t1"] });
    const r3 = await readRootRevision(tenant);
    expect(r1).toBeGreaterThan(r0);
    expect(r2).toBeGreaterThan(r1);
    expect(r3).toBeGreaterThan(r2);
  });
});
