import { describe, it, expect } from "vitest";
import { env, runInDurableObject } from "cloudflare:test";

/**
 * Phase 27 follow-up — close the same data-destruction class for
 * write-stream commit, recursive remove, and copy-file under
 * versioning ON. Sub-agent (a) found these three remaining FAIL
 * paths after the original Phase 27 multipart fix.
 *
 *   FX1. vfsRemoveRecursive on a versioning-ON tenant tombstones
 *        each file (preserving history) instead of hard-deleting.
 *   FX2. vfsCommitWriteStream on a versioning-ON tenant creates
 *        a file_versions row and preserves prior history when the
 *        destination already had versions.
 *   FX3. vfsCopyFile chunked over a versioned destination
 *        preserves the destination's prior versions.
 *   FX4. vfsCopyFile inline over a versioned destination preserves
 *        the destination's prior versions.
 */

import { vfsUserDOName } from "@core/lib/utils";

interface E {
  MOSSAIC_USER: DurableObjectNamespace;
}
const TEST_ENV = env as unknown as E;
const NS = "default";

function userStub(tenant: string) {
  return TEST_ENV.MOSSAIC_USER.get(
    TEST_ENV.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
  );
}

const enc = new TextEncoder();

describe("Phase 27 follow-ups — vfsRemoveRecursive (FX1)", () => {
  it("FX1 — versioning ON: vfsRemoveRecursive tombstones each file; history survives in file_versions", async () => {
    const tenant = "fx1-rmrf-versioned";
    const stub = userStub(tenant);
    const scope = { ns: NS, tenant };
    await (
      stub as unknown as {
        adminSetVersioning(t: string, e: boolean): Promise<unknown>;
      }
    ).adminSetVersioning(tenant, true);

    await stub.vfsMkdir(scope, "/d", { recursive: true });
    await stub.vfsWriteFile(scope, "/d/a.txt", enc.encode("a-bytes"));
    await stub.vfsWriteFile(scope, "/d/b.txt", enc.encode("b-bytes"));

    // listVersions before removal — both files have 1 version each.
    const aBefore = await stub.vfsListVersions(scope, "/d/a.txt");
    expect(aBefore.length).toBe(1);
    const bBefore = await stub.vfsListVersions(scope, "/d/b.txt");
    expect(bBefore.length).toBe(1);

    // Recursive remove.
    await stub.vfsRemoveRecursive(scope, "/d");

    // Post-removal: listings hide the files, but file_versions rows
    // still exist (history preserved).
    const fileVersionRows = await runInDurableObject(stub, async (_inst, s) => {
      const rows = s.storage.sql
        .exec(
          "SELECT path_id, deleted, size FROM file_versions ORDER BY path_id, mtime_ms"
        )
        .toArray() as { path_id: string; deleted: number; size: number }[];
      return rows;
    });
    // Each file got: 1 original version + 1 tombstone version.
    // 2 files × 2 rows = 4.
    expect(fileVersionRows.length).toBe(4);
    const tombstones = fileVersionRows.filter((r) => r.deleted === 1);
    expect(tombstones.length).toBe(2);
    const live = fileVersionRows.filter((r) => r.deleted === 0);
    expect(live.length).toBe(2);
    // Original bytes survive — sizes match.
    expect(live.map((r) => r.size).sort()).toEqual([7, 7]);
  });

  it("FX1b — versioning OFF: vfsRemoveRecursive still hard-deletes (byte-equivalent to pre-Phase-27)", async () => {
    const tenant = "fx1-rmrf-nonversioned";
    const stub = userStub(tenant);
    const scope = { ns: NS, tenant };

    await stub.vfsMkdir(scope, "/d", { recursive: true });
    await stub.vfsWriteFile(scope, "/d/a.txt", enc.encode("a"));
    await stub.vfsRemoveRecursive(scope, "/d");

    const fileRows = await runInDurableObject(stub, async (_inst, s) => {
      const rows = s.storage.sql
        .exec("SELECT COUNT(*) AS n FROM files WHERE user_id = ?", tenant)
        .toArray()[0] as { n: number };
      return rows.n;
    });
    expect(fileRows).toBe(0);
  });
});

describe("Phase 27 follow-ups — vfsCommitWriteStream (FX2)", () => {
  it("FX2 — versioning ON: stream commit over a prior versioned file preserves history; new head reads new bytes", async () => {
    const tenant = "fx2-stream-versioned";
    const stub = userStub(tenant);
    const scope = { ns: NS, tenant };
    await (
      stub as unknown as {
        adminSetVersioning(t: string, e: boolean): Promise<unknown>;
      }
    ).adminSetVersioning(tenant, true);

    // Seed: writeFile creates v1 + v2 with distinguishable sizes.
    const v1Bytes = enc.encode("first-version-stream-bytes");
    const v2Bytes = enc.encode("v2");
    await stub.vfsWriteFile(scope, "/s.bin", v1Bytes);
    await stub.vfsWriteFile(scope, "/s.bin", v2Bytes);
    const before = await stub.vfsListVersions(scope, "/s.bin");
    expect(before.length).toBe(2);

    // beginWriteStream + appendWriteStream + commitWriteStream over
    // the same path.
    const handle = await stub.vfsBeginWriteStream(scope, "/s.bin");
    const streamBytes = enc.encode("v3-stream-via-handle");
    await stub.vfsAppendWriteStream(scope, handle, 0, streamBytes);
    await stub.vfsCommitWriteStream(scope, handle);

    // Phase 27 — stream commit must preserve v1 + v2.
    const after = await stub.vfsListVersions(scope, "/s.bin");
    expect(after.length).toBe(3);

    // Head bytes are v3.
    const head = await stub.vfsReadFile(scope, "/s.bin");
    expect(new Uint8Array(head)).toEqual(streamBytes);

    // v1 readable by versionId — distinguishable size.
    const v1 = before.find((v) => v.size === v1Bytes.byteLength);
    expect(v1).toBeTruthy();
    const v1Read = await stub.vfsReadFile(scope, "/s.bin", {
      versionId: v1!.versionId,
    });
    expect(new Uint8Array(v1Read)).toEqual(v1Bytes);
  });
});

describe("Phase 27 follow-ups — vfsCopyFile preserves dst history (FX3, FX4)", () => {
  it("FX3 — versioning ON, chunked copy over an existing versioned dst preserves dst history", async () => {
    const tenant = "fx3-copy-chunked-preserves";
    const stub = userStub(tenant);
    const scope = { ns: NS, tenant };
    await (
      stub as unknown as {
        adminSetVersioning(t: string, e: boolean): Promise<unknown>;
      }
    ).adminSetVersioning(tenant, true);

    // Seed dst with 2 versions of distinguishable bytes.
    const dstV1 = enc.encode("dst-version-one-bytes");
    const dstV2 = enc.encode("dst-v2");
    await stub.vfsWriteFile(scope, "/dst.bin", dstV1);
    await stub.vfsWriteFile(scope, "/dst.bin", dstV2);
    const dstBefore = await stub.vfsListVersions(scope, "/dst.bin");
    expect(dstBefore.length).toBe(2);

    // Seed src with bytes that hit the chunked tier (>16 KB inline limit).
    const srcBytes = new Uint8Array(20 * 1024).fill(7);
    await stub.vfsWriteFile(scope, "/src.bin", srcBytes);

    // Copy src → dst (overwrite).
    await stub.vfsCopyFile(scope, "/src.bin", "/dst.bin", { overwrite: true });

    // Phase 27 — dst's prior history must survive.
    const dstAfter = await stub.vfsListVersions(scope, "/dst.bin");
    expect(dstAfter.length).toBe(3);

    // Head bytes are src's bytes.
    const head = await stub.vfsReadFile(scope, "/dst.bin");
    expect(new Uint8Array(head).length).toBe(srcBytes.byteLength);

    // dst-v1 still readable by versionId — distinguishable size.
    const v1 = dstBefore.find((v) => v.size === dstV1.byteLength);
    expect(v1).toBeTruthy();
    const v1Bytes = await stub.vfsReadFile(scope, "/dst.bin", {
      versionId: v1!.versionId,
    });
    expect(new Uint8Array(v1Bytes)).toEqual(dstV1);
  });

  it("FX4 — versioning ON, inline copy over an existing versioned dst preserves dst history", async () => {
    const tenant = "fx4-copy-inline-preserves";
    const stub = userStub(tenant);
    const scope = { ns: NS, tenant };
    await (
      stub as unknown as {
        adminSetVersioning(t: string, e: boolean): Promise<unknown>;
      }
    ).adminSetVersioning(tenant, true);

    // Seed dst with 2 versions, distinguishable sizes.
    const origV1 = enc.encode("original-version-one");
    const origV2 = enc.encode("orig-v2");
    await stub.vfsWriteFile(scope, "/d.bin", origV1);
    await stub.vfsWriteFile(scope, "/d.bin", origV2);
    const before = await stub.vfsListVersions(scope, "/d.bin");
    expect(before.length).toBe(2);

    // Seed src in non-versioned form. We need an inline-tier source
    // for this test; a small writeFile produces inline bytes.
    await stub.vfsWriteFile(scope, "/s.bin", enc.encode("src-tiny"));

    // Copy src → dst.
    await stub.vfsCopyFile(scope, "/s.bin", "/d.bin", { overwrite: true });

    const after = await stub.vfsListVersions(scope, "/d.bin");
    expect(after.length).toBe(3);

    const head = await stub.vfsReadFile(scope, "/d.bin");
    expect(new Uint8Array(head)).toEqual(enc.encode("src-tiny"));

    // orig-v1 still readable, distinguishable size.
    const v1 = before.find((v) => v.size === origV1.byteLength);
    expect(v1).toBeTruthy();
    const v1Bytes = await stub.vfsReadFile(scope, "/d.bin", {
      versionId: v1!.versionId,
    });
    expect(new Uint8Array(v1Bytes)).toEqual(origV1);
  });
});
