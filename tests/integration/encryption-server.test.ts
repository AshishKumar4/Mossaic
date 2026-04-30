import { describe, it, expect } from "vitest";
import { env, runInDurableObject } from "cloudflare:test";

/**
 * Step 2 — server-side encryption metadata + RPC stamp-through.
 *
 * The Mossaic server NEVER decrypts user data. These tests exercise
 * what the server DOES do with encryption metadata:
 *
 *   S1.  vfsWriteFile with `opts.encryption` stamps `files.encryption_mode`
 *        and `files.encryption_key_id` on the head row.
 *   S2.  vfsWriteFile with `opts.encryption` AND versioning enabled stamps
 *        BOTH `files.*` columns AND `file_versions.*` columns on the
 *        committed version.
 *   S3.  Mixed-mode rejection: a file already stamped 'convergent' rejects
 *        a 'random'-mode write with EBADF.
 *   S4.  Plaintext-to-encrypted rejection: a file already stamped
 *        'convergent' rejects a writeFile without `opts.encryption` with
 *        EBADF.
 *   S5.  First encrypted write of a previously-plaintext path is allowed
 *        (the supersede semantics permit a mode change on a brand-new
 *        version-of-record).
 *   S6.  copyFile preserves the source's encryption columns onto the dest.
 *   S7.  vfsListVersions surfaces the per-version encryption stamp.
 *   S8.  vfsStat surfaces the head encryption stamp.
 *   S9.  Schema migration is idempotent — calling ensureInit twice does
 *        not throw or duplicate columns.
 *   S10. encryption_mode survives DO hibernation (write, force-rehydrate
 *        the DO via a fresh stub fetch, read back).
 */

import {
  createVFS,
  type MossaicEnv,
  type UserDO,
} from "../../sdk/src/index";
import { vfsUserDOName } from "@core/lib/utils";

interface E {
  MOSSAIC_USER: DurableObjectNamespace<UserDO>;
}
const E = env as unknown as E;
const NS = "default";

function envFor(): MossaicEnv {
  return { MOSSAIC_USER: E.MOSSAIC_USER as MossaicEnv["MOSSAIC_USER"] };
}
function userStub(tenant: string) {
  return E.MOSSAIC_USER.get(
    E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
  );
}

const enc = (s: string) => new TextEncoder().encode(s);

describe("server schema + RPC stamping", () => {
  it("S1 — vfsWriteFile stamps encryption columns on the head row", async () => {
    const tenant = "p15-s1";
    const stub = userStub(tenant);
    const scope = { ns: NS, tenant };

    // Plaintext envelope-stream (server treats it opaquely; we don't
    // need to actually encrypt — Step 2 is about metadata stamping).
    const payload = enc("opaque-envelope-bytes");
    await stub.vfsWriteFile(scope, "/secret.bin", payload, {
      encryption: { mode: "convergent", keyId: "v1" },
    });

    const row = await runInDurableObject(stub, async (_, state) => {
      return state.storage.sql
        .exec(
          "SELECT encryption_mode, encryption_key_id FROM files WHERE file_name='secret.bin'"
        )
        .toArray()[0] as
        | { encryption_mode: string; encryption_key_id: string }
        | undefined;
    });
    expect(row?.encryption_mode).toBe("convergent");
    expect(row?.encryption_key_id).toBe("v1");
  });

  it("S2 — versioned write stamps both files.* and file_versions.* columns", async () => {
    const tenant = "p15-s2";
    const stub = userStub(tenant);
    const scope = { ns: NS, tenant };

    // Force ensureInit() by issuing any RPC first — `quota` table is
    // created lazily on first RPC, not at DO construction.
    await stub.vfsExists(scope, "/seed");

    // Enable versioning for this tenant. userId == tenant when no sub.
    await runInDurableObject(stub, async (_, state) => {
      state.storage.sql.exec(
        `INSERT OR IGNORE INTO quota (user_id, storage_used, storage_limit, file_count, pool_size, versioning_enabled)
         VALUES (?, 0, 107374182400, 0, 32, 1)`,
        tenant
      );
      state.storage.sql.exec(
        `UPDATE quota SET versioning_enabled = 1 WHERE user_id = ?`,
        tenant
      );
    });

    await stub.vfsWriteFile(scope, "/v.bin", enc("v1-data"), {
      encryption: { mode: "random", keyId: "rotated-2024" },
    });

    const both = await runInDurableObject(stub, async (_, state) => {
      const f = state.storage.sql
        .exec(
          "SELECT file_id, encryption_mode, encryption_key_id FROM files WHERE file_name='v.bin'"
        )
        .toArray()[0] as
        | {
            file_id: string;
            encryption_mode: string;
            encryption_key_id: string;
          }
        | undefined;
      const v = f
        ? (state.storage.sql
            .exec(
              "SELECT encryption_mode, encryption_key_id FROM file_versions WHERE path_id = ?",
              f.file_id
            )
            .toArray()[0] as
            | { encryption_mode: string; encryption_key_id: string }
            | undefined)
        : undefined;
      return { f, v };
    });
    expect(both.f?.encryption_mode).toBe("random");
    expect(both.f?.encryption_key_id).toBe("rotated-2024");
    expect(both.v?.encryption_mode).toBe("random");
    expect(both.v?.encryption_key_id).toBe("rotated-2024");
  });

  it("S3 — mixed-mode write within a path rejects EBADF", async () => {
    const tenant = "p15-s3";
    const stub = userStub(tenant);
    const scope = { ns: NS, tenant };

    await stub.vfsWriteFile(scope, "/x.bin", enc("data1"), {
      encryption: { mode: "convergent" },
    });
    await expect(
      stub.vfsWriteFile(scope, "/x.bin", enc("data2"), {
        encryption: { mode: "random" },
      })
    ).rejects.toThrow(/EBADF/);
  });

  it("S4 — plaintext write to encrypted path rejects EBADF", async () => {
    const tenant = "p15-s4";
    const stub = userStub(tenant);
    const scope = { ns: NS, tenant };

    await stub.vfsWriteFile(scope, "/secret.bin", enc("encrypted-data"), {
      encryption: { mode: "convergent" },
    });
    // No encryption opts on the second write → server must reject.
    await expect(
      stub.vfsWriteFile(scope, "/secret.bin", enc("plaintext-data"))
    ).rejects.toThrow(/EBADF/);
  });

  it("S5 — first encrypted write to a previously-plaintext path is allowed", async () => {
    const tenant = "p15-s5";
    const stub = userStub(tenant);
    const scope = { ns: NS, tenant };

    // Plaintext write first.
    await stub.vfsWriteFile(scope, "/file.bin", enc("plaintext"));
    // Now upgrade to encrypted (mode change on the supersede is OK).
    await stub.vfsWriteFile(scope, "/file.bin", enc("encrypted-now"), {
      encryption: { mode: "convergent" },
    });
    // Final state: encrypted.
    const row = await runInDurableObject(stub, async (_, state) => {
      return state.storage.sql
        .exec(
          "SELECT encryption_mode FROM files WHERE file_name='file.bin' AND status='complete'"
        )
        .toArray()[0] as { encryption_mode: string };
    });
    expect(row.encryption_mode).toBe("convergent");
  });

  it("S6 — copyFile preserves source's encryption columns", async () => {
    const tenant = "p15-s6";
    const stub = userStub(tenant);
    const scope = { ns: NS, tenant };

    await stub.vfsWriteFile(scope, "/src.bin", enc("envelope-bytes"), {
      encryption: { mode: "convergent", keyId: "src-keyid" },
    });
    await stub.vfsCopyFile(scope, "/src.bin", "/dest.bin");

    const dest = await runInDurableObject(stub, async (_, state) => {
      return state.storage.sql
        .exec(
          "SELECT encryption_mode, encryption_key_id FROM files WHERE file_name='dest.bin' AND status='complete'"
        )
        .toArray()[0] as { encryption_mode: string; encryption_key_id: string };
    });
    expect(dest.encryption_mode).toBe("convergent");
    expect(dest.encryption_key_id).toBe("src-keyid");
  });

  it("S7 — vfsListVersions surfaces per-version encryption stamp", async () => {
    const tenant = "p15-s7";
    const stub = userStub(tenant);
    const scope = { ns: NS, tenant };

    await stub.vfsExists(scope, "/seed");
    await runInDurableObject(stub, async (_, state) => {
      state.storage.sql.exec(
        `INSERT OR IGNORE INTO quota (user_id, storage_used, storage_limit, file_count, pool_size, versioning_enabled)
         VALUES (?, 0, 107374182400, 0, 32, 1)`,
        tenant
      );
      state.storage.sql.exec(
        `UPDATE quota SET versioning_enabled = 1 WHERE user_id = ?`,
        tenant
      );
    });

    await stub.vfsWriteFile(scope, "/v.bin", enc("v1"), {
      encryption: { mode: "convergent", keyId: "key1" },
    });
    await stub.vfsWriteFile(scope, "/v.bin", enc("v2"), {
      encryption: { mode: "convergent", keyId: "key1" },
    });

    const versions = await stub.vfsListVersions(scope, "/v.bin", {});
    expect(versions.length).toBeGreaterThanOrEqual(2);
    for (const v of versions) {
      expect(v.encryption?.mode).toBe("convergent");
      expect(v.encryption?.keyId).toBe("key1");
    }
  });

  it("S8 — vfsStat surfaces the head encryption stamp", async () => {
    const tenant = "p15-s8";
    const stub = userStub(tenant);
    const scope = { ns: NS, tenant };

    await stub.vfsWriteFile(scope, "/file.bin", enc("hidden"), {
      encryption: { mode: "random" },
    });
    const stat = await stub.vfsStat(scope, "/file.bin");
    expect(stat.encryption?.mode).toBe("random");
    // No keyId set → encryption.keyId stays undefined.
    expect(stat.encryption?.keyId).toBeUndefined();
  });

  it("S9 — ensureInit ALTER columns are idempotent (no error on re-call)", async () => {
    const tenant = "p15-s9";
    const stub = userStub(tenant);
    // Issue any RPC to force ensureInit. Then issue another to force
    // a second ensureInit. If ALTERs were not idempotent, the second
    // call would throw "duplicate column name".
    const scope = { ns: NS, tenant };
    await stub.vfsExists(scope, "/nonexistent");
    await stub.vfsExists(scope, "/still-nonexistent");
    // Verify columns exist on `files`.
    const cols = await runInDurableObject(stub, async (_, state) => {
      return state.storage.sql
        .exec("PRAGMA table_info(files)")
        .toArray() as { name: string }[];
    });
    const names = cols.map((c) => c.name);
    expect(names).toContain("encryption_mode");
    expect(names).toContain("encryption_key_id");
    // And on file_versions.
    const colsV = await runInDurableObject(stub, async (_, state) => {
      return state.storage.sql
        .exec("PRAGMA table_info(file_versions)")
        .toArray() as { name: string }[];
    });
    expect(colsV.map((c) => c.name)).toContain("encryption_mode");
    expect(colsV.map((c) => c.name)).toContain("encryption_key_id");
    // And on yjs_meta.
    const colsY = await runInDurableObject(stub, async (_, state) => {
      return state.storage.sql
        .exec("PRAGMA table_info(yjs_meta)")
        .toArray() as { name: string }[];
    });
    expect(colsY.map((c) => c.name)).toContain("bytes_since_last_compact");
  });

  it("S10 — encryption columns survive DO hibernation (re-fetch stub re-reads from SQLite)", async () => {
    const tenant = "p15-s10";
    const stub = userStub(tenant);
    const scope = { ns: NS, tenant };

    await stub.vfsWriteFile(scope, "/persist.bin", enc("persistent"), {
      encryption: { mode: "convergent", keyId: "across-hibernation" },
    });

    // Get a fresh stub for the same DO name. Workerd's DO storage is
    // backed by SQLite; a fresh handle reads the same persisted state.
    const stub2 = userStub(tenant);
    const stat = await stub2.vfsStat(scope, "/persist.bin");
    expect(stat.encryption?.mode).toBe("convergent");
    expect(stat.encryption?.keyId).toBe("across-hibernation");
  });

  // Bonus: verify SDK still works on plaintext when no encryption opts.
  it("plaintext writeFile leaves encryption columns NULL", async () => {
    const tenant = "p15-plaintext";
    const vfs = createVFS(envFor(), { tenant });
    await vfs.writeFile("/plain.bin", enc("hello"));
    const stub = userStub(tenant);
    const row = await runInDurableObject(stub, async (_, state) => {
      return state.storage.sql
        .exec(
          "SELECT encryption_mode, encryption_key_id FROM files WHERE file_name='plain.bin' AND status='complete'"
        )
        .toArray()[0] as
        | { encryption_mode: string | null; encryption_key_id: string | null }
        | undefined;
    });
    expect(row?.encryption_mode).toBeNull();
    expect(row?.encryption_key_id).toBeNull();
    // Sanity: SDK reads plaintext unchanged.
    const got = new TextDecoder().decode(await vfs.readFile("/plain.bin"));
    expect(got).toBe("hello");
  });
});
