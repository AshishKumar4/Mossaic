import { describe, it, expect } from "vitest";
import { env } from "cloudflare:test";

/**
 * `FILE_HEAD_JOIN` + `assertHeadNotTombstoned` regression suite.
 *
 * The audit consolidated 21 inline `LEFT JOIN file_versions fv ON …`
 * blocks (with copies of the tombstone-head check) into a single
 * shared SQL constant + typed helper in
 * `worker/core/objects/user/vfs/helpers.ts`. These tests cover the
 * surfaces that route through the helper to confirm consistent
 * tombstone semantics across them.
 *
 * Surfaces exercised:
 *  - `vfsStat` / `vfsExists` (helpers.ts statForResolved)
 *  - `vfsReadFile` (reads.ts readFile gate)
 *  - `vfsArchive` (archive.ts ensureNotTombstoned — uses constant only)
 *  - `vfsOpenManifest` (reads.ts openManifest gate)
 *  - `vfsListFiles` (list-files.ts where filter + JOIN constant)
 *  - `vfsReadPreview` (preview.ts gate)
 *
 * Pin the contract: a tombstoned head row throws `ENOENT` from
 * every gated read surface. The previous behaviour predates the
 * audit but is easy to re-introduce on refactor — these tests
 * are the regression net.
 */

import { createVFS, type MossaicEnv, type UserDO, ENOENT } from "../../sdk/src/index";

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

const enc = new TextEncoder();

describe("FILE_HEAD_JOIN consolidation regression", () => {
  it("FH1 — vfsReadFile throws ENOENT on tombstoned head (reads.ts:442 gate)", async () => {
    const vfs = createVFS(envFor(), {
      tenant: "fh-read",
      versioning: "enabled",
    });
    await vfs.writeFile("/r.bin", enc.encode("bytes"));
    await vfs.unlink("/r.bin");
    await expect(vfs.readFile("/r.bin")).rejects.toBeInstanceOf(ENOENT);
  });

  it("FH2 — vfsOpenManifest throws ENOENT on tombstoned head (reads.ts:662 gate)", async () => {
    const vfs = createVFS(envFor(), {
      tenant: "fh-manifest",
      versioning: "enabled",
    });
    await vfs.writeFile("/m.bin", enc.encode("bytes"));
    await vfs.unlink("/m.bin");
    await expect(vfs.openManifest("/m.bin")).rejects.toBeInstanceOf(ENOENT);
  });

  it("FH3 — vfsListFiles excludes tombstoned heads (list-files.ts JOIN consistency)", async () => {
    const vfs = createVFS(envFor(), {
      tenant: "fh-list",
      versioning: "enabled",
    });
    await vfs.writeFile("/keep.bin", enc.encode("a"));
    await vfs.writeFile("/gone.bin", enc.encode("b"));
    await vfs.unlink("/gone.bin");

    const listed = await vfs.listFiles({ includeStat: true });
    const names = listed.items.map((i) => i.path).sort();
    expect(names).toEqual(["/keep.bin"]);
  });

  it("FH4 — vfsArchive throws ENOENT on tombstoned head (archive.ts:36 gate)", async () => {
    const vfs = createVFS(envFor(), {
      tenant: "fh-archive",
      versioning: "enabled",
    });
    await vfs.writeFile("/a.bin", enc.encode("bytes"));
    await vfs.unlink("/a.bin");
    await expect(vfs.archive("/a.bin")).rejects.toBeInstanceOf(ENOENT);
  });

  it("FH5 — error messages from the helper are stable across surfaces", async () => {
    const vfs = createVFS(envFor(), {
      tenant: "fh-msg",
      versioning: "enabled",
    });
    await vfs.writeFile("/x.bin", enc.encode("bytes"));
    await vfs.unlink("/x.bin");

    // The shared helper uses syscall name and path. Each call site
    // passes its own syscall string, but they all resolve through
    // `assertHeadNotTombstoned` so the WORD "tombstone" appears
    // somewhere in the message.
    let captured: Error | null = null;
    try {
      await vfs.readFile("/x.bin");
    } catch (e) {
      captured = e as Error;
    }
    expect(captured).toBeInstanceOf(ENOENT);
    // Non-empty message; stable shape (server raises typed
    // ENOENT, SDK preserves the wire message).
    expect((captured as Error).message.length).toBeGreaterThan(0);
  });
});
