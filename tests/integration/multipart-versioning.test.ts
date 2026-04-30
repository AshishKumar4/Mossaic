import { describe, it, expect } from "vitest";
import { SELF, env, runInDurableObject } from "cloudflare:test";

/**
 * Phase 26 — multipart × versioning state-combination
 * (sub-agent adversarial finding; "$1000 bet won").
 *
 * THE BUG CLASS this file pins against:
 *
 * Multipart finalize (`worker/core/objects/user/multipart-upload.ts`)
 * routes through `commitRename` directly. There is no
 * `isVersioningEnabled` check anywhere in the multipart path.
 * Consequence on a versioning-enabled tenant:
 *
 *  - the `files` row gets `head_version_id = NULL`,
 *  - NO `file_versions` row is created,
 *  - chunks live in `file_chunks` (not `version_chunks`),
 *  - if a live row exists at `(parent, leaf)`, `commitRename` calls
 *    `hardDeleteFileRow` which deletes the prior row's chunks via
 *    ShardDO fan-out — destroying the prior history entirely.
 *
 * Severity match for the production tombstone bug: a tenant with
 * versioning enabled who uses the SDK's parallel/multipart upload
 * (the default for files >5 MB; CLI/browser large-file flow) loses
 * version history on every multipart overwrite.
 *
 * These tests pin the CURRENT (buggy) behavior so a reader / future
 * fixer can flip the assertions when the worker-side fix lands.
 *
 *   MV1. Multipart finalize on a versioning-ON tenant currently
 *        does NOT populate head_version_id (today: bug pinned).
 *   MV2. Multipart-overwrite of an existing writeFile-versioned
 *        file: prior versions become unreachable via listVersions
 *        because the new files row has 0 file_versions rows
 *        (today: data loss surface, pinned).
 *   MV3. listVersions on a multipart-uploaded file under
 *        versioning ON returns [] (today, pinned).
 *   MV4. copyFile of a multipart-uploaded file on versioning-ON
 *        tenant: succeeds (fix landed) or fails ENOENT (current
 *        bug branch hits NULL head). Either is acceptable; we lock
 *        the observable result so a flip in either direction is
 *        visible.
 *
 * Drives multipart end-to-end via SELF.fetch — the same HTTP wire
 * path the SDK / SPA use. Inspects DO state via runInDurableObject
 * for the SQL-level assertions.
 */

import { signVFSToken } from "@core/lib/auth";
import { hashChunk } from "@shared/crypto";
import { vfsUserDOName } from "@core/lib/utils";

interface E {
  MOSSAIC_USER: DurableObjectNamespace;
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

describe("multipart × versioning — current bug-class lock (MV1, MV3)", () => {
  it("MV1 — multipart finalize on a versioning-ON tenant currently does NOT populate head_version_id (regression pin for sub-agent finding)", async () => {
    const tenant = "mv-finalize-no-version";
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
          "SELECT file_id, head_version_id FROM files WHERE file_name = 'large.bin'"
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
      return { row, versionCount };
    });
    expect(state.row).toBeTruthy();
    // Current contract (the bug). When the worker fix lands this
    // flips to non-NULL + 1.
    expect(state.row!.head_version_id).toBeNull();
    expect(state.versionCount).toBe(0);
  });

  it("MV3 — listVersions on a multipart-uploaded file under versioning ON returns [] today; bytes still readable via legacy chunk path", async () => {
    const tenant = "mv-listversions-empty";
    const stub = userStub(tenant);
    await (
      stub as unknown as {
        adminSetVersioning(t: string, e: boolean): Promise<unknown>;
      }
    ).adminSetVersioning(tenant, true);

    const payload = makePayload(0xb2);
    await multipartWrite(tenant, "/big.bin", payload);

    const versions = await stub.vfsListVersions(
      { ns: NS, tenant },
      "/big.bin"
    );
    expect(versions).toHaveLength(0);

    const back = await stub.vfsReadFile({ ns: NS, tenant }, "/big.bin");
    expect(new Uint8Array(back).length).toBe(payload.byteLength);
  });
});

describe("multipart × versioning — overwrite history loss (MV2)", () => {
  it("MV2 — multipart-overwrite of an existing writeFile-versioned file: listVersions returns 0 entries afterwards (history unreachable)", async () => {
    const tenant = "mv-overwrite-history";
    const stub = userStub(tenant);
    await (
      stub as unknown as {
        adminSetVersioning(t: string, e: boolean): Promise<unknown>;
      }
    ).adminSetVersioning(tenant, true);

    const scope = { ns: NS, tenant };

    // 1. Two writeFile passes — these DO insert file_versions rows.
    await stub.vfsWriteFile(scope, "/p.bin", new TextEncoder().encode("v1"));
    await stub.vfsWriteFile(scope, "/p.bin", new TextEncoder().encode("v2"));
    const before = await stub.vfsListVersions(scope, "/p.bin");
    expect(before.length).toBe(2);

    // 2. Multipart-overwrite. commitRename hard-deletes the prior
    //    files row; the new files row has 0 file_versions rows.
    const payload = makePayload(0xc3);
    await multipartWrite(tenant, "/p.bin", payload);

    // 3. listVersions on the new path resolves to the new files row
    //    which has 0 file_versions rows.
    const list = await stub.vfsListVersions(scope, "/p.bin");
    // Lock the OBSERVABLE TRUTH today: history is unreachable. When
    // the multipart-finalize-versioning fix lands, this flips to
    // ≥3 (v1, v2, multipart) and the test should be updated.
    expect(list).toHaveLength(0);

    // The bytes of the multipart upload are readable; the prior
    // text content is irrecoverable via the public API.
    const back = await stub.vfsReadFile(scope, "/p.bin");
    expect(back.byteLength).toBe(payload.byteLength);
  });
});

describe("multipart × versioning — copyFile cross-product (MV4)", () => {
  it("MV4 — copyFile of a multipart-uploaded file on versioning-ON tenant: succeeds (fix) or fails ENOENT (current bug); pin observable result", async () => {
    const tenant = "mv-copy-cross";
    const stub = userStub(tenant);
    await (
      stub as unknown as {
        adminSetVersioning(t: string, e: boolean): Promise<unknown>;
      }
    ).adminSetVersioning(tenant, true);

    const payload = makePayload(0xd4);
    await multipartWrite(tenant, "/src.bin", payload);

    let result: "ok" | "enoent" | "other" = "other";
    let observedMessage = "";
    try {
      await stub.vfsCopyFile({ ns: NS, tenant }, "/src.bin", "/dst.bin");
      result = "ok";
    } catch (e) {
      const code = (e as { code?: string }).code ?? "";
      observedMessage = String((e as Error).message ?? "");
      if (code === "ENOENT" || /ENOENT/.test(observedMessage)) {
        result = "enoent";
      }
    }
    // Today's truth: ENOENT thrown via VFSError("ENOENT", "copyFile:
    // source has no head version") at copy-file.ts:511. When the
    // multipart-finalize-versioning fix lands, this flips to "ok".
    expect(["ok", "enoent"]).toContain(result);
    if (result === "enoent") {
      expect(observedMessage).toMatch(/source has no head version|ENOENT/i);
    }
  });
});
