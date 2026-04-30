import { describe, it, expect } from "vitest";
import { SELF, env, runInDurableObject } from "cloudflare:test";
import { listVersionsVia } from "./helpers";

/**
 * Phase 27 — multipart × versioning correctness.
 *
 * Pre-Phase-27, multipart finalize routed unconditionally through
 * `commitRename`. With versioning ON this caused silent data
 * destruction: prior versions were hard-deleted via
 * `hardDeleteFileRow`'s shard fan-out, no `file_versions` row was
 * inserted, and `head_version_id` stayed NULL. Phase 26's
 * sub-agent finding pinned the broken behaviour; this file pins the
 * POST-FIX behaviour.
 *
 *   MV1. Multipart finalize on a versioning-ON tenant DOES create a
 *        `file_versions` row and sets `head_version_id`.
 *   MV2. Multipart-overwrite of a writeFile-versioned file
 *        PRESERVES prior history; `listVersions` returns ≥3
 *        entries (v1, v2, multipart-version).
 *   MV3. `listVersions` on a multipart-uploaded file under
 *        versioning-on returns the multipart version itself
 *        (length ≥ 1, not 0).
 *   MV4. `restoreVersion` of a pre-multipart version succeeds:
 *        `vfs.readFile` after restore returns the historical bytes,
 *        confirming the prior version's chunks were not destroyed.
 *
 * Drives multipart end-to-end via SELF.fetch — same HTTP path the
 * SDK / SPA use. SQL-level assertions go through runInDurableObject.
 */

import { signVFSToken } from "@core/lib/auth";
import { hashChunk } from "@shared/crypto";
import { vfsUserDOName } from "@core/lib/utils";
import type { UserDO } from "@app/objects/user/user-do";

interface E {
  MOSSAIC_USER: DurableObjectNamespace<UserDO>;
  MOSSAIC_SHARD: DurableObjectNamespace;
  JWT_SECRET?: string;
}
const TEST_ENV = env as unknown as E;
const NS = "default";

async function mint(tenant: string): Promise<string> {
  return signVFSToken(TEST_ENV as never, { ns: NS, tenant });
}

function userStub(tenant: string) {
  return TEST_ENV.MOSSAIC_USER.get(
    TEST_ENV.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
  );
}

interface BeginRes {
  uploadId: string;
  chunkSize: number;
  totalChunks: number;
  sessionToken: string;
}

async function beginMP(opts: {
  tenant: string;
  path: string;
  size: number;
  chunkSize?: number;
}): Promise<BeginRes & { bearer: string }> {
  const bearer = await mint(opts.tenant);
  const r = await SELF.fetch("https://test/api/vfs/multipart/begin", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${bearer}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      path: opts.path,
      size: opts.size,
      chunkSize: opts.chunkSize,
    }),
  });
  if (!r.ok) {
    throw new Error(`beginMP: ${r.status} ${await r.text()}`);
  }
  const body = (await r.json()) as BeginRes;
  return { ...body, bearer };
}

async function putMP(
  bearer: string,
  uploadId: string,
  idx: number,
  bytes: Uint8Array,
  sessionToken: string
): Promise<string> {
  const r = await SELF.fetch(
    `https://test/api/vfs/multipart/${encodeURIComponent(uploadId)}/chunk/${idx}`,
    {
      method: "PUT",
      headers: {
        Authorization: `Bearer ${bearer}`,
        "X-Session-Token": sessionToken,
        "Content-Type": "application/octet-stream",
        "Content-Length": String(bytes.byteLength),
      },
      body: bytes,
    }
  );
  if (!r.ok) {
    throw new Error(`putMP idx=${idx}: ${r.status} ${await r.text()}`);
  }
  const body = (await r.json()) as { hash: string };
  return body.hash;
}

async function finalizeMP(
  bearer: string,
  uploadId: string,
  chunkHashList: string[]
): Promise<void> {
  const r = await SELF.fetch("https://test/api/vfs/multipart/finalize", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${bearer}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ uploadId, chunkHashList }),
  });
  if (!r.ok) {
    throw new Error(`finalizeMP: ${r.status} ${await r.text()}`);
  }
}

/**
 * End-to-end multipart write through the same HTTP surface the SDK
 * uses. Pre-computes chunk hashes (the server recomputes and
 * rejects on divergence, so this MUST match).
 */
async function multipartWrite(
  tenant: string,
  path: string,
  payload: Uint8Array,
  chunkSizeHint = 16 * 1024
): Promise<void> {
  const begin = await beginMP({
    tenant,
    path,
    size: payload.byteLength,
    chunkSize: chunkSizeHint,
  });
  const cs = begin.chunkSize;
  const hashes: string[] = [];
  for (let i = 0; i < begin.totalChunks; i++) {
    const start = i * cs;
    const end = Math.min(start + cs, payload.byteLength);
    const slice = payload.subarray(start, end);
    const h = await putMP(
      begin.bearer,
      begin.uploadId,
      i,
      slice,
      begin.sessionToken
    );
    hashes.push(h);
    expect(h).toBe(await hashChunk(slice));
  }
  await finalizeMP(begin.bearer, begin.uploadId, hashes);
}

const PAYLOAD_SIZE = 32 * 1024; // 2 chunks at 16 KB

function makePayload(seed: number): Uint8Array {
  const a = new Uint8Array(PAYLOAD_SIZE);
  let s = seed >>> 0;
  for (let i = 0; i < a.length; i++) {
    s = (Math.imul(s, 1664525) + 1013904223) >>> 0;
    a[i] = s & 0xff;
  }
  return a;
}

describe("multipart × versioning — Phase 27 correct semantics (MV1, MV3)", () => {
  it("MV1 — multipart finalize on a versioning-ON tenant CREATES a file_versions row and sets head_version_id", async () => {
    const tenant = "mv-finalize-creates-version";
    const stub = userStub(tenant);
    await (
      stub as unknown as {
        adminSetVersioning(t: string, e: boolean): Promise<unknown>;
      }
    ).adminSetVersioning(tenant, true);

    const payload = makePayload(0xa1);
    await multipartWrite(tenant, "/large.bin", payload);

    const state = await runInDurableObject(stub, async (_inst, s) => {
      const row = s.storage.sql
        .exec(
          "SELECT file_id, head_version_id FROM files WHERE file_name = 'large.bin' AND status = 'complete'"
        )
        .toArray()[0] as
        | { file_id: string; head_version_id: string | null }
        | undefined;
      const versionCount = (
        s.storage.sql
          .exec(
            "SELECT COUNT(*) AS n FROM file_versions WHERE path_id = ?",
            row?.file_id ?? ""
          )
          .toArray()[0] as { n: number }
      ).n;
      // Confirm version_chunks were written for the head version.
      const versionChunkCount = row?.head_version_id
        ? (
            s.storage.sql
              .exec(
                "SELECT COUNT(*) AS n FROM version_chunks WHERE version_id = ?",
                row.head_version_id
              )
              .toArray()[0] as { n: number }
          ).n
        : 0;
      // shard_ref_id should be the uploadId; we can't easily fetch
      // the original uploadId from the test, but we can confirm it
      // is non-NULL for multipart-finalized versions (Phase 27 stamp).
      const refIdRow = row?.head_version_id
        ? (s.storage.sql
            .exec(
              "SELECT shard_ref_id FROM file_versions WHERE version_id = ?",
              row.head_version_id
            )
            .toArray()[0] as { shard_ref_id: string | null } | undefined)
        : undefined;
      return {
        row,
        versionCount,
        versionChunkCount,
        shardRefId: refIdRow?.shard_ref_id ?? null,
      };
    });

    expect(state.row).toBeTruthy();
    // Phase 27 — head_version_id is set, file_versions row exists.
    expect(state.row!.head_version_id).not.toBeNull();
    expect(state.versionCount).toBe(1);
    // version_chunks rows mirror the multipart chunks.
    expect(state.versionChunkCount).toBe(2);
    // shard_ref_id is stamped (non-NULL) for multipart-finalized
    // versions so dropVersionRows can fan out the right refId.
    expect(state.shardRefId).not.toBeNull();
    expect(typeof state.shardRefId).toBe("string");
  });

  it("MV3 — listVersions on a multipart-uploaded file under versioning ON returns 1 entry; bytes readable", async () => {
    const tenant = "mv-listversions-one";
    const stub = userStub(tenant);
    await (
      stub as unknown as {
        adminSetVersioning(t: string, e: boolean): Promise<unknown>;
      }
    ).adminSetVersioning(tenant, true);

    const payload = makePayload(0xb2);
    await multipartWrite(tenant, "/big.bin", payload);

    const versions = await listVersionsVia(stub, 
      { ns: NS, tenant },
      "/big.bin"
    );
    // Phase 27 — exactly 1 version row from the multipart finalize.
    expect(versions).toHaveLength(1);
    expect(versions[0]!.deleted).toBe(false);
    expect(versions[0]!.size).toBe(payload.byteLength);

    const back = await stub.vfsReadFile({ ns: NS, tenant }, "/big.bin");
    expect(new Uint8Array(back).length).toBe(payload.byteLength);
    // Bytes round-trip.
    expect(new Uint8Array(back)).toEqual(payload);
  });
});

describe("multipart × versioning — overwrite preserves history (MV2)", () => {
  it("MV2 — multipart-overwrite of an existing writeFile-versioned file PRESERVES prior versions", async () => {
    const tenant = "mv-overwrite-preserves";
    const stub = userStub(tenant);
    await (
      stub as unknown as {
        adminSetVersioning(t: string, e: boolean): Promise<unknown>;
      }
    ).adminSetVersioning(tenant, true);

    const scope = { ns: NS, tenant };

    // 1. Two writeFile passes — DO insert file_versions rows.
    await stub.vfsWriteFile(scope, "/p.bin", new TextEncoder().encode("v1"));
    await stub.vfsWriteFile(scope, "/p.bin", new TextEncoder().encode("v2"));
    const before = await listVersionsVia(stub, scope, "/p.bin");
    expect(before.length).toBe(2);

    // 2. Multipart-overwrite — Phase 27 must preserve prior history.
    const payload = makePayload(0xc3);
    await multipartWrite(tenant, "/p.bin", payload);

    // 3. Phase 27 — listVersions returns ≥3 (v1, v2, multipart).
    const list = await listVersionsVia(stub, scope, "/p.bin");
    expect(list.length).toBe(3);
    // Newest-first ordering — multipart is the head.
    expect(list[0]!.size).toBe(payload.byteLength);

    // 4. The multipart bytes are the new head.
    const back = await stub.vfsReadFile(scope, "/p.bin");
    expect(back.byteLength).toBe(payload.byteLength);

    // 5. Older versions still readable by versionId.
    const v1Row = list.find((v) => v.size === 2 /* "v1" / "v2" */);
    expect(v1Row).toBeTruthy();
    const olderBytes = await stub.vfsReadFile(scope, "/p.bin", {
      versionId: v1Row!.versionId,
    });
    // It's either "v1" or "v2"; both have length 2.
    expect(olderBytes.byteLength).toBe(2);
  });
});

describe("multipart × versioning — restoreVersion (MV4)", () => {
  it("MV4 — restoreVersion of a pre-multipart writeFile version succeeds; readFile returns historical bytes", async () => {
    const tenant = "mv-restore-after-multipart";
    const stub = userStub(tenant);
    await (
      stub as unknown as {
        adminSetVersioning(t: string, e: boolean): Promise<unknown>;
      }
    ).adminSetVersioning(tenant, true);

    const scope = { ns: NS, tenant };

    // 1. writeFile v1 — historical version we'll restore.
    const v1Bytes = new TextEncoder().encode("history v1 bytes");
    await stub.vfsWriteFile(scope, "/q.bin", v1Bytes);
    const afterV1 = await listVersionsVia(stub, scope, "/q.bin");
    expect(afterV1.length).toBe(1);
    const v1Id = afterV1[0]!.versionId;

    // 2. Multipart-overwrite — Phase 27 preserves v1.
    const payload = makePayload(0xd4);
    await multipartWrite(tenant, "/q.bin", payload);

    const afterMP = await listVersionsVia(stub, scope, "/q.bin");
    expect(afterMP.length).toBe(2);

    // 3. Restore v1 — chunks of v1 must still be reachable on shards.
    await stub.vfsRestoreVersion(scope, "/q.bin", v1Id);

    // 4. After restore: head bytes equal the historical v1 bytes.
    const restored = await stub.vfsReadFile(scope, "/q.bin");
    expect(new Uint8Array(restored)).toEqual(v1Bytes);

    // 5. listVersions now has 3 entries (v1, multipart, restore-of-v1
    //    inserted as a new version row pointing at v1's content).
    const afterRestore = await listVersionsVia(stub, scope, "/q.bin");
    expect(afterRestore.length).toBe(3);
  });
});
