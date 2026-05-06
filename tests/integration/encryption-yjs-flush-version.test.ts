import { describe, it, expect } from "vitest";
import { env } from "cloudflare:test";

/**
 * Phase 52 — encrypted-yjs flush emits a user-visible version row
 * (Phase 38 follow-up).
 *
 * Background: Phase 38 added user-visible version rows on plain-yjs
 * `flushYjs`. Encrypted-yjs files used a separate compaction surface
 * (`vfsCompactEncryptedYjs`) because the server can't materialise an
 * encrypted Y.Doc — the client must build the checkpoint envelope
 * locally. That surface had no `userVisible` plumbing, so
 * `vfs.compactYjs()` on an encrypted file silently lost its
 * checkpoint history from `listVersions`.
 *
 * Phase 52 fix: thread `{userVisible, label}` through the encrypted
 * compaction RPC so a user-flush emits a `file_versions` row whose
 * `inline_data` IS the checkpoint envelope (encrypted; reader
 * decrypts locally).
 *
 * Also: the handle's `flush()` now routes encrypted files to
 * `compactYjs({userVisible: true, label})` instead of the plain-yjs
 * server-driven `_flushYjs` (which would fail to materialise an
 * encrypted doc on the server).
 *
 * Tests:
 *   EFV1 — vfs.compactYjs(path, {userVisible:true, label}) on a
 *          versioning-on encrypted-yjs tenant emits a version row
 *          with the supplied label and userVisible=true.
 *   EFV2 — handle.flush({label}) on an encrypted-yjs file routes
 *          through compactYjs and emits the same version row.
 *   EFV3 — versioning-OFF tenant: compactYjs still succeeds and
 *          returns the checkpoint result, but versionId is undefined
 *          (no row emitted).
 */

import {
  createVFS,
  type MossaicEnv,
  type UserDO,
  type EncryptionConfig,
} from "../../sdk/src/index";
import { openYDoc } from "../../sdk/src/yjs";

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

function makeKey(byte: number): Uint8Array {
  const a = new Uint8Array(32);
  a.fill(byte);
  return a;
}

describe("Phase 52 — encrypted-yjs flush emits user-visible version row", () => {
  it("EFV1 — vfs.compactYjs({userVisible:true, label}) on versioning-on tenant emits a version row", async () => {
    const tenant = "efv1-tenant";
    const cfg: EncryptionConfig = {
      masterKey: makeKey(0xe1),
      tenantSalt: makeKey(0xf1),
    };
    const vfs = createVFS(envFor(), {
      tenant,
      encryption: cfg,
      versioning: "enabled",
    });
    await vfs.writeFile("/encdoc.yj", "", { encrypted: true });
    await vfs.setYjsMode("/encdoc.yj", true);

    // Generate some ops.
    const handle = await openYDoc(vfs, "/encdoc.yj");
    await handle.synced;
    for (let i = 0; i < 3; i++) {
      handle.doc.getText("content").insert(0, `e${i}-`);
    }
    await new Promise((r) => setTimeout(r, 400));
    await handle.close();

    // Compact with userVisible=true and a label. The encrypted
    // compaction path now emits a file_versions row.
    const result = await vfs.compactYjs("/encdoc.yj", {
      userVisible: true,
      label: "v1.0-checkpoint",
    });
    expect(result).not.toBeNull();
    expect(result!.versionId).toBeDefined();
    expect(typeof result!.versionId).toBe("string");

    // listVersions on the encrypted-yjs file surfaces the new row
    // with the supplied label.
    const versions = await vfs.listVersions("/encdoc.yj");
    expect(versions.length).toBeGreaterThanOrEqual(1);
    const newest = versions[0];
    expect(newest.id).toBe(result!.versionId);
    expect(newest.label).toBe("v1.0-checkpoint");
    expect(newest.userVisible).toBe(true);
    expect(newest.deleted).toBe(false);
  });

  it("EFV2 — handle.flush({label}) on an encrypted-yjs file emits a version row via compactYjs", async () => {
    const tenant = "efv2-tenant";
    const cfg: EncryptionConfig = {
      masterKey: makeKey(0xe2),
      tenantSalt: makeKey(0xf2),
    };
    const vfs = createVFS(envFor(), {
      tenant,
      encryption: cfg,
      versioning: "enabled",
    });
    await vfs.writeFile("/h.yj", "", { encrypted: true });
    await vfs.setYjsMode("/h.yj", true);

    const handle = await openYDoc(vfs, "/h.yj");
    await handle.synced;
    handle.doc.getText("content").insert(0, "hello");
    await new Promise((r) => setTimeout(r, 200));

    // The handle's flush() now routes encrypted files through
    // compactYjs (Phase 52 P3 #8). On a versioning-on tenant
    // with userVisible: true (implicit from handle.flush), the
    // server emits a file_versions row.
    const flushResult = await handle.flush({ label: "ck-1" });
    expect(flushResult.versionId).not.toBeNull();
    await handle.close();

    const versions = await vfs.listVersions("/h.yj");
    expect(versions.length).toBeGreaterThanOrEqual(1);
    expect(versions[0].label).toBe("ck-1");
    expect(versions[0].userVisible).toBe(true);
  });

  it("EFV3 — versioning OFF: compactYjs still succeeds; versionId is undefined", async () => {
    const tenant = "efv3-tenant";
    const cfg: EncryptionConfig = {
      masterKey: makeKey(0xe3),
      tenantSalt: makeKey(0xf3),
    };
    const vfs = createVFS(envFor(), { tenant, encryption: cfg });
    // versioning NOT enabled
    await vfs.writeFile("/x.yj", "", { encrypted: true });
    await vfs.setYjsMode("/x.yj", true);

    const handle = await openYDoc(vfs, "/x.yj");
    await handle.synced;
    handle.doc.getText("content").insert(0, "abc");
    await new Promise((r) => setTimeout(r, 200));
    await handle.close();

    const result = await vfs.compactYjs("/x.yj", {
      userVisible: true,
      label: "ignored-v-off",
    });
    expect(result).not.toBeNull();
    // Without versioning enabled, no version row is emitted; the
    // checkpoint still happens but there's nothing to surface.
    expect(result!.versionId).toBeUndefined();
  });
});
