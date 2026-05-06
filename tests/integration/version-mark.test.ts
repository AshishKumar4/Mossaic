import { describe, it, expect } from "vitest";
import { env, runInDurableObject } from "cloudflare:test";

/**
 * Phase 12 — markVersion + listVersions(userVisibleOnly) + Yjs flush.
 *
 * Pinned invariants:
 *   V1. writeFile on a versioning tenant defaults to user_visible=1.
 *   V2. listVersions(userVisibleOnly:true) filters to the 1-rows.
 *   V3. markVersion(userVisible=true) flips a 0-row to 1.
 *   V4. markVersion(userVisible=false) → EINVAL (monotonic).
 *   V5. markVersion(label="foo") sets / replaces the label.
 *   V6. Yjs flush() creates a user-visible version row with label
 *       (when versioning is on).
 *   V7. Yjs opportunistic compaction stays user_visible=0.
 */

import { createVFS, type MossaicEnv, type UserDO, EINVAL } from "../../sdk/src/index";
import { openYDoc } from "../../sdk/src/yjs";
import { vfsUserDOName } from "@core/lib/utils";

interface E {
  USER_DO: DurableObjectNamespace<UserDO>;
}
const E = env as unknown as E;
const NS = "default";

function envFor(): MossaicEnv {
  return { MOSSAIC_USER: E.USER_DO as MossaicEnv["MOSSAIC_USER"] };
}
function userStub(tenant: string) {
  return E.USER_DO.get(E.USER_DO.idFromName(vfsUserDOName(NS, tenant)));
}

describe("Phase 12 — version flags from writeFile (V1, V2)", () => {
  it("writeFile creates user_visible=1 by default; listVersions filters", async () => {
    const tenant = "p12-mv-default";
    const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });
    await vfs.writeFile("/a.txt", "v1");
    await vfs.writeFile("/a.txt", "v2");
    const all = await vfs.listVersions("/a.txt");
    expect(all.length).toBe(2);
    expect(all.every((v) => v.userVisible === true)).toBe(true);

    const visible = await vfs.listVersions("/a.txt", {
      userVisibleOnly: true,
    });
    expect(visible.length).toBe(2);
  });

  it("listVersions(userVisibleOnly:true) excludes user_visible=0 rows", async () => {
    const tenant = "p12-mv-filter";
    const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });
    await vfs.writeFile("/a.txt", "v1");

    // Inject a user_visible=0 row directly to simulate an
    // opportunistic compaction. We use raw SQL to keep the test
    // independent of the Yjs path (covered separately below).
    const stub = userStub(tenant);
    await runInDurableObject(stub, async (_, state) => {
      const fid = (
        state.storage.sql
          .exec("SELECT file_id FROM files WHERE file_name='a.txt'")
          .toArray()[0] as { file_id: string }
      ).file_id;
      state.storage.sql.exec(
        `INSERT INTO file_versions
           (path_id, version_id, user_id, size, mode, mtime_ms, deleted,
            inline_data, chunk_size, chunk_count, file_hash, mime_type,
            user_visible, label, metadata)
         VALUES (?, 'fake-internal', ?, 1, 420, ?, 0, X'78', 0, 0, '',
                 'application/octet-stream', 0, NULL, NULL)`,
        fid,
        tenant,
        Date.now()
      );
    });

    const all = await vfs.listVersions("/a.txt");
    expect(all.length).toBe(2);
    const visible = await vfs.listVersions("/a.txt", {
      userVisibleOnly: true,
    });
    expect(visible.length).toBe(1);
  });
});

describe("Phase 12 — markVersion (V3, V4, V5)", () => {
  it("flips user_visible 0 → 1", async () => {
    const tenant = "p12-mv-flip";
    const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });
    await vfs.writeFile("/a.txt", "v1");
    const stub = userStub(tenant);
    // Inject a user_visible=0 row.
    const internalId = "fake-internal";
    await runInDurableObject(stub, async (_, state) => {
      const fid = (
        state.storage.sql
          .exec("SELECT file_id FROM files WHERE file_name='a.txt'")
          .toArray()[0] as { file_id: string }
      ).file_id;
      state.storage.sql.exec(
        `INSERT INTO file_versions
           (path_id, version_id, user_id, size, mode, mtime_ms, deleted,
            inline_data, chunk_size, chunk_count, file_hash, mime_type,
            user_visible, label, metadata)
         VALUES (?, ?, ?, 1, 420, ?, 0, X'78', 0, 0, '', 'text/plain', 0, NULL, NULL)`,
        fid,
        internalId,
        tenant,
        Date.now()
      );
    });

    await vfs.markVersion("/a.txt", internalId, { userVisible: true });
    const visible = await vfs.listVersions("/a.txt", {
      userVisibleOnly: true,
    });
    expect(visible.map((v) => v.id)).toContain(internalId);
  });

  it("rejects userVisible=false with EINVAL (monotonic)", async () => {
    const tenant = "p12-mv-monotonic";
    const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });
    await vfs.writeFile("/a.txt", "v1");
    const all = await vfs.listVersions("/a.txt");
    await expect(
      vfs.markVersion("/a.txt", all[0].id, { userVisible: false })
    ).rejects.toBeInstanceOf(EINVAL);
  });

  it("sets label", async () => {
    const tenant = "p12-mv-label";
    const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });
    await vfs.writeFile("/a.txt", "v1");
    const all = await vfs.listVersions("/a.txt");
    await vfs.markVersion("/a.txt", all[0].id, { label: "milestone-1" });
    const after = await vfs.listVersions("/a.txt");
    expect(after[0].label).toBe("milestone-1");
  });

  it("rejects label > 128 chars with EINVAL", async () => {
    const tenant = "p12-mv-label-cap";
    const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });
    await vfs.writeFile("/a.txt", "v1");
    const all = await vfs.listVersions("/a.txt");
    await expect(
      vfs.markVersion("/a.txt", all[0].id, { label: "x".repeat(200) })
    ).rejects.toBeInstanceOf(EINVAL);
  });
});

describe("Phase 12 — Yjs flush() (V6)", () => {
  it("flush creates a user-visible version row when versioning is on", async () => {
    const tenant = "p12-mv-yjs-flush";
    const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });
    await vfs.writeFile("/live.md", "");
    await vfs.setYjsMode("/live.md", true);
    await vfs.writeFile("/live.md", "draft 1");

    const handle = await openYDoc(vfs, "/live.md");
    await handle.synced;
    const text = handle.doc.getText("content");
    text.insert(text.length, " — saving");

    const flushResult = await handle.flush({ label: "milestone-1" });
    expect(flushResult.versionId).not.toBeNull();
    expect(flushResult.checkpointSeq).toBeGreaterThanOrEqual(0);

    await handle.close();

    const versions = await vfs.listVersions("/live.md", {
      userVisibleOnly: true,
    });
    const flushed = versions.find((v) => v.id === flushResult.versionId);
    expect(flushed).toBeDefined();
    expect(flushed!.label).toBe("milestone-1");
  });

  it("flush returns null versionId when versioning is off", async () => {
    const tenant = "p12-mv-yjs-noversioning";
    const vfs = createVFS(envFor(), { tenant }); // versioning off
    await vfs.writeFile("/live.md", "");
    await vfs.setYjsMode("/live.md", true);
    await vfs.writeFile("/live.md", "draft");
    const handle = await openYDoc(vfs, "/live.md");
    await handle.synced;
    const r = await handle.flush();
    expect(r.versionId).toBeNull();
    await handle.close();
  });
});
