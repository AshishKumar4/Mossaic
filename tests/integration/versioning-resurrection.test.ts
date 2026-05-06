import { describe, it, expect } from "vitest";
import { env } from "cloudflare:test";

/**
 * Phase 26 — versioning resurrection (audit gap G3 / §7.4).
 *
 * Existing `versioning.test.ts` covers the unlink → tombstone path
 * (line 181) and the restoreVersion-of-tombstone EINVAL case (line
 * 160). It does NOT cover the natural recovery flows:
 *
 *   R1. write → unlink → write same path. The second write must
 *       insert a new LIVE version (`deleted=0`) and update
 *       `files.head_version_id` to it. Subsequent stat / readFile
 *       MUST return the new live bytes (NOT ENOENT).
 *   R2. write → unlink → restoreVersion(prior_live_id). Restore
 *       inserts a new live version cloned from the source; head
 *       flips back to live. listFiles surfaces the path again.
 *   R3. After R1, listFiles shows the path EXACTLY ONCE — the
 *       resurrection must not double-count the row.
 *   R4. listVersions returns the full chain
 *       (live₂, tombstone, live₁) for the resurrection case.
 */

import {
  createVFS,
  type MossaicEnv,
  type UserDO,
} from "../../sdk/src/index";

interface E {
  MOSSAIC_USER: DurableObjectNamespace<UserDO>;
}
const E = env as unknown as E;

function envFor(): MossaicEnv {
  return { MOSSAIC_USER: E.MOSSAIC_USER as MossaicEnv["MOSSAIC_USER"] };
}

describe("versioning resurrection — write → unlink → write (R1, R3, R4)", () => {
  it("R1 — second write resurrects a tombstoned path; stat returns the new live bytes", async () => {
    const tenant = "ver-resurrect-write";
    const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });

    await vfs.writeFile("/x.txt", "first");
    await vfs.unlink("/x.txt"); // tombstone

    // Second write to the same path. Must succeed (no EEXIST, no
    // ENOENT) and must produce a live head version.
    await vfs.writeFile("/x.txt", "second");

    // Stat / readFile / exists must all see the live bytes.
    const stat = await vfs.stat("/x.txt");
    expect(stat.type).toBe("file");
    expect(stat.size).toBe("second".length);

    const back = await vfs.readFile("/x.txt", { encoding: "utf8" });
    expect(back).toBe("second");

    expect(await vfs.exists("/x.txt")).toBe(true);
  });

  it("R3 — listFiles after resurrection shows the path EXACTLY ONCE", async () => {
    const tenant = "ver-resurrect-list-once";
    const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });

    await vfs.writeFile("/x.txt", "first");
    await vfs.unlink("/x.txt");
    await vfs.writeFile("/x.txt", "second");

    const list = await vfs.listFiles({ orderBy: "name" });
    const matches = list.items.filter((i) => i.path === "/x.txt");
    expect(matches.length).toBe(1);
    expect(matches[0].stat?.size).toBe("second".length);
  });

  it("R4 — listVersions returns the full chain (live₂, tombstone, live₁)", async () => {
    const tenant = "ver-resurrect-history";
    const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });

    await vfs.writeFile("/x.txt", "first");
    await vfs.unlink("/x.txt");
    await vfs.writeFile("/x.txt", "second");

    const versions = await vfs.listVersions("/x.txt");
    expect(versions).toHaveLength(3);
    // Newest first: live(second), tombstone, live(first).
    expect(versions[0].deleted).toBe(false);
    expect(versions[0].size).toBe("second".length);
    expect(versions[1].deleted).toBe(true);
    expect(versions[2].deleted).toBe(false);
    expect(versions[2].size).toBe("first".length);
  });

  it("R1b — chunked resurrection (>16KB write after tombstone)", async () => {
    const tenant = "ver-resurrect-chunked";
    const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });

    await vfs.writeFile("/big.bin", "small first");
    await vfs.unlink("/big.bin");

    const payload = new Uint8Array(20_000);
    for (let i = 0; i < payload.length; i++) payload[i] = (i * 7) & 0xff;
    await vfs.writeFile("/big.bin", payload);

    const back = await vfs.readFile("/big.bin");
    expect(back.length).toBe(payload.length);
    expect(back[0]).toBe(payload[0]);
    expect(back[19_999]).toBe(payload[19_999]);
  });
});

describe("versioning resurrection — restoreVersion of a prior live version (R2)", () => {
  it("R2 — restoreVersion of a prior live id after tombstone resurrects the path", async () => {
    const tenant = "ver-resurrect-restore";
    const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });

    await vfs.writeFile("/x.txt", "alive");
    const beforeUnlink = await vfs.listVersions("/x.txt");
    expect(beforeUnlink).toHaveLength(1);
    const liveId = beforeUnlink[0].id;
    expect(beforeUnlink[0].deleted).toBe(false);

    await vfs.unlink("/x.txt"); // tombstone

    // restoreVersion of the PRIOR LIVE id (not the tombstone).
    const r = await vfs.restoreVersion("/x.txt", liveId);
    expect(typeof r.id).toBe("string");
    expect(r.id).not.toBe(liveId);

    // Head is now live again.
    const back = await vfs.readFile("/x.txt", { encoding: "utf8" });
    expect(back).toBe("alive");

    // listFiles surfaces the path once.
    const list = await vfs.listFiles({ orderBy: "name" });
    const matches = list.items.filter((i) => i.path === "/x.txt");
    expect(matches.length).toBe(1);

    // History: live(restored), tombstone, live(original).
    const after = await vfs.listVersions("/x.txt");
    expect(after).toHaveLength(3);
    expect(after[0].deleted).toBe(false);
    expect(after[0].id).toBe(r.id);
    expect(after[1].deleted).toBe(true);
    expect(after[2].deleted).toBe(false);
    expect(after[2].id).toBe(liveId);
  });
});
