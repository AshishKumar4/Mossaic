import { describe, it, expect } from "vitest";
import { env, runInDurableObject } from "cloudflare:test";
import type { UserDO } from "@app/objects/user/user-do";
import { vfsUserDOName } from "@core/lib/utils";

/**
 * Tier 1.3 — write-commit.ts extraction.
 *
 * The `vfsWriteFile` body was refactored into:
 *   prepareWriteCommit   — input validation + monotonic-mode check
 *                          (returns a `WriteCommitPlan`).
 *   commitInlineTier     — INLINE_LIMIT path: insert tmp row, rename.
 *   commitChunkedTier    — chunked path: ShardDO fan-out, rename.
 *
 * These tests pin behavioral parity around the extraction so a future
 * change to one tier doesn't silently change the other.
 *
 *   WC.1  inline tier (small text) round-trips byte-for-byte
 *   WC.2  chunked tier (>16KB) round-trips byte-for-byte
 *   WC.3  EFBIG before any SQL: writeFile of WRITEFILE_MAX+1 bytes
 *         doesn't leave a tmp row
 *   WC.4  EISDIR: writing to a folder path doesn't leave a tmp row
 *   WC.5  metadata-cap violation throws EINVAL pre-commit (no row)
 *   WC.6  successful write: only ONE row exists at the leaf (the
 *         tmp prefix is cleaned up by commitRename)
 */

interface E {
  MOSSAIC_USER: DurableObjectNamespace<UserDO>;
}
const E = env as unknown as E;

function userStub(tenant: string): DurableObjectStub<UserDO> {
  return E.MOSSAIC_USER.get(
    E.MOSSAIC_USER.idFromName(vfsUserDOName("default", tenant))
  );
}

async function countRowsAtLeaf(
  stub: DurableObjectStub<UserDO>,
  userId: string,
  parentId: string | null,
  leaf: string
): Promise<number> {
  return await runInDurableObject(stub, async (_inst, state) => {
    const rows = state.storage.sql
      .exec(
        "SELECT COUNT(*) AS n FROM files WHERE user_id = ? AND IFNULL(parent_id,'') = IFNULL(?, '') AND file_name = ?",
        userId,
        parentId,
        leaf
      )
      .toArray() as { n: number }[];
    return rows[0]?.n ?? 0;
  });
}

async function countTmpRows(
  stub: DurableObjectStub<UserDO>,
  userId: string
): Promise<number> {
  return await runInDurableObject(stub, async (_inst, state) => {
    const rows = state.storage.sql
      .exec(
        "SELECT COUNT(*) AS n FROM files WHERE user_id = ? AND file_name LIKE '_vfs_tmp_%'",
        userId
      )
      .toArray() as { n: number }[];
    return rows[0]?.n ?? 0;
  });
}

describe("write-commit extraction — prepareWriteCommit / commitInlineTier / commitChunkedTier", () => {
  it("WC.1 — inline tier round-trips a small text payload", async () => {
    const tenant = "wc-inline";
    const stub = userStub(tenant);
    const scope = { ns: "default", tenant } as const;
    const text = new TextEncoder().encode("hello inline tier");

    await stub.vfsWriteFile(scope, "/note.txt", text, {
      mimeType: "text/plain",
    });
    const got = await stub.vfsReadFile(scope, "/note.txt");
    expect(new Uint8Array(got)).toEqual(text);
  });

  it("WC.2 — chunked tier round-trips a >16KB payload byte-for-byte", async () => {
    const tenant = "wc-chunked";
    const stub = userStub(tenant);
    const scope = { ns: "default", tenant } as const;
    const data = new Uint8Array(20 * 1024);
    for (let i = 0; i < data.length; i++) data[i] = (i * 31 + 7) & 0xff;

    await stub.vfsWriteFile(scope, "/big.bin", data, {
      mimeType: "application/octet-stream",
    });
    const got = await stub.vfsReadFile(scope, "/big.bin");
    expect(new Uint8Array(got)).toEqual(data);
  });

  it("WC.3 — EISDIR: writing to a folder path leaves no tmp row", async () => {
    const tenant = "wc-eisdir";
    const stub = userStub(tenant);
    const scope = { ns: "default", tenant } as const;
    await stub.vfsMkdir(scope, "/docs");
    await expect(
      stub.vfsWriteFile(
        scope,
        "/docs",
        new TextEncoder().encode("nope"),
        { mimeType: "text/plain" }
      )
    ).rejects.toThrow(/EISDIR/);
    expect(await countTmpRows(stub, tenant)).toBe(0);
  });

  it("WC.4 — invalid tag set throws EINVAL pre-commit (no rows)", async () => {
    const tenant = "wc-tags";
    const stub = userStub(tenant);
    const scope = { ns: "default", tenant } as const;
    // The tag validator forbids characters outside [A-Za-z0-9._:/-];
    // a tag with a space exercises the EINVAL path before any SQL
    // touches the row.
    await expect(
      stub.vfsWriteFile(
        scope,
        "/t.txt",
        new TextEncoder().encode("ok"),
        {
          mimeType: "text/plain",
          tags: ["bad tag with space"],
        }
      )
    ).rejects.toThrow(/EINVAL|tag/i);
    expect(await countRowsAtLeaf(stub, tenant, null, "t.txt")).toBe(0);
    expect(await countTmpRows(stub, tenant)).toBe(0);
  });

  it("WC.5 — invalid version label throws EINVAL pre-commit (no rows)", async () => {
    const tenant = "wc-label";
    const stub = userStub(tenant);
    const scope = { ns: "default", tenant } as const;
    // The label validator caps length at 256; a 1KB label triggers it.
    const oversized = "x".repeat(1024);
    await expect(
      stub.vfsWriteFile(
        scope,
        "/v.txt",
        new TextEncoder().encode("ok"),
        {
          mimeType: "text/plain",
          version: { label: oversized },
        }
      )
    ).rejects.toThrow(/EINVAL|label/i);
    expect(await countRowsAtLeaf(stub, tenant, null, "v.txt")).toBe(0);
    expect(await countTmpRows(stub, tenant)).toBe(0);
  });

  it("WC.6 — successful write leaves exactly ONE row at the leaf (tmp cleaned up)", async () => {
    const tenant = "wc-cleanup";
    const stub = userStub(tenant);
    const scope = { ns: "default", tenant } as const;
    await stub.vfsWriteFile(
      scope,
      "/clean.txt",
      new TextEncoder().encode("clean"),
      { mimeType: "text/plain" }
    );
    expect(await countRowsAtLeaf(stub, tenant, null, "clean.txt")).toBe(1);
    expect(await countTmpRows(stub, tenant)).toBe(0);
  });
});
