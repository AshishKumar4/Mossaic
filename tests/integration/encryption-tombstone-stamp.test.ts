import { describe, it, expect } from "vitest";
import { env, runInDurableObject } from "cloudflare:test";
import { vfsUserDOName } from "@core/lib/utils";

/**
 * Phase 28 Fix 3 — commitVersion preserves encryption stamp on tombstone.
 *
 * Pre-fix, when `vfsUnlink` (or `vfsRename` overwrite, or
 * `vfsRemoveRecursive`) called `commitVersion(deleted: true)` without
 * passing `args.encryption`, the function stamped NULL into both
 * the tombstone `file_versions` row AND `files.encryption_mode`.
 * For an encrypted file, this:
 *   1. Lost the prior live version's encryption key on the tombstone
 *      (history stamp diverges from reality).
 *   2. Worse: stamped `files.encryption_mode = NULL` so a subsequent
 *      `restoreVersion` of the prior live version would update head
 *      pointer but the head's `files` stamp would still be NULL —
 *      readers would treat the bytes as plaintext.
 *
 * Post-fix: when `args.deleted` is true and `args.encryption` is
 * omitted, inherit from the prior live version. Both the tombstone
 * row and the `files` head stamp now reflect the actual envelope.
 *
 * Cases:
 *   ETS1. encrypted file → unlink → tombstone row carries the prior
 *         encryption stamp (not NULL).
 *   ETS2. encrypted file → unlink → `files.encryption_mode` is NOT
 *         clobbered to NULL (still reflects the encryption envelope).
 *   ETS3. plaintext file → unlink → tombstone row's encryption stamp
 *         is NULL (regression guard — no spurious inheritance).
 */

interface E {
  MOSSAIC_USER: DurableObjectNamespace;
}
const E = env as unknown as E;
const NS = "default";

function userStub(tenant: string) {
  return E.MOSSAIC_USER.get(
    E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
  );
}

const enc = new TextEncoder();

describe("Phase 28 Fix 3 — encryption stamp preserved on tombstone", () => {
  it("ETS1 — encrypted file → unlink → tombstone version inherits encryption stamp", async () => {
    const tenant = "ets1-encrypted";
    const stub = userStub(tenant);
    const scope = { ns: NS, tenant };
    await (
      stub as unknown as {
        adminSetVersioning(t: string, e: boolean): Promise<unknown>;
      }
    ).adminSetVersioning(tenant, true);

    // Encrypted write — convergent mode.
    await stub.vfsWriteFile(scope, "/secret.bin", enc.encode("payload-1"), {
      encryption: { mode: "convergent" },
    });
    const beforeUnlink = await stub.vfsListVersions(scope, "/secret.bin");
    expect(beforeUnlink.length).toBe(1);
    expect(beforeUnlink[0]!.encryption).toBeTruthy();
    expect(beforeUnlink[0]!.encryption!.mode).toBe("convergent");

    // Unlink — writes a tombstone version (no `encryption` arg).
    await stub.vfsUnlink(scope, "/secret.bin");

    // The tombstone row's encryption_mode column should be inherited
    // from the prior live version.
    const tombStamp = await runInDurableObject(stub, async (_inst, state) => {
      const row = state.storage.sql
        .exec(
          `SELECT encryption_mode, encryption_key_id, deleted
             FROM file_versions
            WHERE deleted = 1
            ORDER BY mtime_ms DESC
            LIMIT 1`
        )
        .toArray()[0] as
        | {
            encryption_mode: string | null;
            encryption_key_id: string | null;
            deleted: number;
          }
        | undefined;
      return row;
    });
    expect(tombStamp).toBeTruthy();
    expect(tombStamp!.deleted).toBe(1);
    // Phase 28 Fix 3 — tombstone inherits prior live stamp.
    expect(tombStamp!.encryption_mode).toBe("convergent");
  });

  it("ETS2 — encrypted file → unlink → files.encryption_mode is NOT clobbered to NULL", async () => {
    const tenant = "ets2-files-stamp";
    const stub = userStub(tenant);
    const scope = { ns: NS, tenant };
    await (
      stub as unknown as {
        adminSetVersioning(t: string, e: boolean): Promise<unknown>;
      }
    ).adminSetVersioning(tenant, true);

    await stub.vfsWriteFile(scope, "/s.bin", enc.encode("p"), {
      encryption: { mode: "convergent" },
    });

    // Pre-unlink: files.encryption_mode is "convergent".
    const before = await runInDurableObject(stub, async (_inst, state) => {
      const r = state.storage.sql
        .exec(
          "SELECT encryption_mode FROM files WHERE file_name = 's.bin'"
        )
        .toArray()[0] as { encryption_mode: string | null };
      return r.encryption_mode;
    });
    expect(before).toBe("convergent");

    await stub.vfsUnlink(scope, "/s.bin");

    // Phase 28 Fix 3 — files.encryption_mode NOT clobbered to NULL.
    const after = await runInDurableObject(stub, async (_inst, state) => {
      const r = state.storage.sql
        .exec(
          "SELECT encryption_mode FROM files WHERE file_name = 's.bin'"
        )
        .toArray()[0] as { encryption_mode: string | null };
      return r.encryption_mode;
    });
    expect(after).toBe("convergent");
  });

  it("ETS3 — plaintext file → unlink → tombstone encryption_mode is NULL (regression guard)", async () => {
    const tenant = "ets3-plaintext";
    const stub = userStub(tenant);
    const scope = { ns: NS, tenant };
    await (
      stub as unknown as {
        adminSetVersioning(t: string, e: boolean): Promise<unknown>;
      }
    ).adminSetVersioning(tenant, true);

    await stub.vfsWriteFile(scope, "/p.bin", enc.encode("plain"));
    await stub.vfsUnlink(scope, "/p.bin");

    const tombStamp = await runInDurableObject(stub, async (_inst, state) => {
      const row = state.storage.sql
        .exec(
          `SELECT encryption_mode FROM file_versions WHERE deleted = 1 ORDER BY mtime_ms DESC LIMIT 1`
        )
        .toArray()[0] as { encryption_mode: string | null };
      return row.encryption_mode;
    });
    // Plaintext source had encryption_mode = NULL; the tombstone
    // inheritance correctly picks up NULL (no spurious value).
    expect(tombStamp).toBeNull();
  });
});
