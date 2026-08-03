import { describe, expect, it } from "vitest";
import { SELF, env, runInDurableObject } from "cloudflare:test";

/**
 * Expected-hash page staging.
 *
 * The manifest a client declares arrives before finalize, in pages of at most
 * `MULTIPART_HASH_PAGE_SIZE`. These tests pin the contract the finalize will
 * later depend on:
 *
 *   - a page is accepted only at the session's contiguous cursor, so the
 *     staged set can never contain a hole,
 *   - a lost response can be retried byte-identically and changes nothing,
 *   - the same indices carrying different hashes are refused,
 *   - a page reaching past the signed chunk count is refused,
 *   - an oversized page is refused by the route before it costs a DO turn,
 *     and by the RPC for every other caller, and
 *   - a session that has left the open state accepts nothing more.
 */

import type { UserDO } from "@app/objects/user/user-do";
import { signVFSToken } from "@core/lib/auth";
import { vfsUserDOName } from "@core/lib/utils";
import { MULTIPART_HASH_PAGE_SIZE } from "@shared/multipart";
import type { EnvCore } from "@shared/types";

interface TestEnv extends Omit<EnvCore, "MOSSAIC_USER"> {
  MOSSAIC_USER: DurableObjectNamespace<UserDO>;
}

const TEST_ENV = env as unknown as TestEnv;
const NS = "default";

/** A distinct, well-formed chunk hash per index. */
const hashFor = (index: number): string =>
  index.toString(16).padStart(64, "0");

const page = (start: number, length: number): string[] =>
  Array.from({ length }, (_, offset) => hashFor(start + offset));

function user(tenant: string): DurableObjectStub<UserDO> {
  return TEST_ENV.MOSSAIC_USER.get(
    TEST_ENV.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
  );
}

function scopeFor(tenant: string): { ns: string; tenant: string } {
  return { ns: NS, tenant };
}

async function beginSession(
  tenant: string,
  path: string,
  size: number
): Promise<string> {
  const begin = await user(tenant).vfsBeginMultipart(scopeFor(tenant), path, {
    size,
    chunkSize: 1,
  });
  expect(begin.totalChunks).toBe(size);
  return begin.uploadId;
}

interface StagedState {
  cursor: number;
  hashes: string[];
}

/** The durable truth: the session cursor and every row staged under it. */
async function stagedState(
  tenant: string,
  uploadId: string
): Promise<StagedState> {
  return runInDurableObject(
    user(tenant),
    (_instance: UserDO, state: DurableObjectState) => {
      const sql = state.storage.sql;
      const session = sql
        .exec<{ staged_hash_cursor: number }>(
          "SELECT staged_hash_cursor FROM upload_sessions WHERE upload_id = ?",
          uploadId
        )
        .toArray()
        .at(0);
      const rows = sql
        .exec<{ chunk_hash: string }>(
          `SELECT chunk_hash FROM upload_expected_chunks
            WHERE upload_id = ? ORDER BY chunk_index`,
          uploadId
        )
        .toArray();
      return {
        cursor: session?.staged_hash_cursor ?? -1,
        hashes: rows.map((row) => row.chunk_hash),
      };
    }
  );
}

async function setSessionStatus(
  tenant: string,
  uploadId: string,
  status: string
): Promise<void> {
  await runInDurableObject(
    user(tenant),
    (_instance: UserDO, state: DurableObjectState) => {
      state.storage.sql.exec(
        "UPDATE upload_sessions SET status = ? WHERE upload_id = ?",
        status,
        uploadId
      );
    }
  );
}

interface RouteResult {
  status: number;
  body: { code?: string; message?: string; staged?: number; total?: number };
}

async function postHashPage(
  tenant: string,
  body: unknown
): Promise<RouteResult> {
  const bearer = await signVFSToken(TEST_ENV, { ns: NS, tenant });
  const res = await SELF.fetch("https://test/api/vfs/multipart/hash-page", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${bearer}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  });
  return { status: res.status, body: await res.json() };
}

describe("multipart expected hash pages", () => {
  it("persists pages and advances the contiguous cursor", async () => {
    const tenant = "hash-pages-persist";
    const uploadId = await beginSession(tenant, "/large.bin", 300);
    const first = page(0, MULTIPART_HASH_PAGE_SIZE);
    const second = page(MULTIPART_HASH_PAGE_SIZE, 44);
    const scope = scopeFor(tenant);

    await expect(
      user(tenant).vfsStageMultipartHashes(scope, uploadId, 0, first)
    ).resolves.toEqual({ staged: 256, total: 300 });
    await expect(
      user(tenant).vfsStageMultipartHashes(scope, uploadId, 256, second)
    ).resolves.toEqual({ staged: 300, total: 300 });

    expect(await stagedState(tenant, uploadId)).toEqual({
      cursor: 300,
      hashes: [...first, ...second],
    });
  });

  it("accepts an identical replay of a staged page without rewriting it", async () => {
    const tenant = "hash-pages-replay";
    const uploadId = await beginSession(tenant, "/replay.bin", 8);
    const scope = scopeFor(tenant);
    const first = page(0, 4);
    await user(tenant).vfsStageMultipartHashes(scope, uploadId, 0, first);
    await user(tenant).vfsStageMultipartHashes(scope, uploadId, 4, page(4, 4));
    const before = await stagedState(tenant, uploadId);

    await expect(
      user(tenant).vfsStageMultipartHashes(scope, uploadId, 0, first)
    ).resolves.toEqual({ staged: 8, total: 8 });
    await expect(
      user(tenant).vfsStageMultipartHashes(scope, uploadId, 1, page(1, 2))
    ).resolves.toEqual({ staged: 8, total: 8 });

    expect(await stagedState(tenant, uploadId)).toEqual(before);
  });

  it("refuses a replay that changes a staged hash", async () => {
    const tenant = "hash-pages-conflict";
    const uploadId = await beginSession(tenant, "/conflict.bin", 4);
    const scope = scopeFor(tenant);
    await user(tenant).vfsStageMultipartHashes(scope, uploadId, 0, page(0, 4));
    const before = await stagedState(tenant, uploadId);

    await expect(
      user(tenant).vfsStageMultipartHashes(scope, uploadId, 1, [
        hashFor(1),
        hashFor(999),
      ])
    ).rejects.toThrow(/EBUSY.*conflicting replay at index 2/);

    expect(await stagedState(tenant, uploadId)).toEqual(before);
  });

  it("refuses a replay whose staged rows have gone missing", async () => {
    // A replay is only a no-op because the rows it names are still there.
    // If cleanup has taken them, saying "already staged" would hand the
    // finalize a manifest that no longer exists.
    const tenant = "hash-pages-missing";
    const uploadId = await beginSession(tenant, "/missing.bin", 4);
    const scope = scopeFor(tenant);
    await user(tenant).vfsStageMultipartHashes(scope, uploadId, 0, page(0, 4));
    await runInDurableObject(
      user(tenant),
      (_instance: UserDO, state: DurableObjectState) => {
        state.storage.sql.exec(
          "DELETE FROM upload_expected_chunks WHERE upload_id = ? AND chunk_index = 3",
          uploadId
        );
      }
    );

    await expect(
      user(tenant).vfsStageMultipartHashes(scope, uploadId, 0, page(0, 4))
    ).rejects.toThrow(/EBUSY.*conflicting replay at index 3/);
  });

  it("refuses a replay straddling the cursor", async () => {
    const tenant = "hash-pages-straddle";
    const uploadId = await beginSession(tenant, "/straddle.bin", 8);
    const scope = scopeFor(tenant);
    await user(tenant).vfsStageMultipartHashes(scope, uploadId, 0, page(0, 4));

    await expect(
      user(tenant).vfsStageMultipartHashes(scope, uploadId, 2, page(2, 4))
    ).rejects.toThrow(/EBUSY.*straddles the staged hash cursor/);
    expect((await stagedState(tenant, uploadId)).cursor).toBe(4);
  });

  it("refuses a page that starts past the cursor", async () => {
    const tenant = "hash-pages-hole";
    const uploadId = await beginSession(tenant, "/hole.bin", 8);
    const scope = scopeFor(tenant);

    await expect(
      user(tenant).vfsStageMultipartHashes(scope, uploadId, 1, page(1, 2))
    ).rejects.toThrow(/EINVAL.*must start at contiguous cursor 0/);

    await user(tenant).vfsStageMultipartHashes(scope, uploadId, 0, page(0, 4));
    await expect(
      user(tenant).vfsStageMultipartHashes(scope, uploadId, 5, page(5, 2))
    ).rejects.toThrow(/EINVAL.*must start at contiguous cursor 4/);
    expect(await stagedState(tenant, uploadId)).toEqual({
      cursor: 4,
      hashes: page(0, 4),
    });
  });

  it("refuses a page reaching past the session's total chunks", async () => {
    const tenant = "hash-pages-range";
    const uploadId = await beginSession(tenant, "/range.bin", 4);
    const scope = scopeFor(tenant);

    await expect(
      user(tenant).vfsStageMultipartHashes(scope, uploadId, 0, page(0, 5))
    ).rejects.toThrow(/EINVAL.*\[0, 5\) exceeds totalChunks 4/);
    await user(tenant).vfsStageMultipartHashes(scope, uploadId, 0, page(0, 4));
    await expect(
      user(tenant).vfsStageMultipartHashes(scope, uploadId, 4, page(4, 1))
    ).rejects.toThrow(/EINVAL.*\[4, 5\) exceeds totalChunks 4/);
  });

  it("refuses malformed page shapes without staging anything", async () => {
    const tenant = "hash-pages-shape";
    const uploadId = await beginSession(tenant, "/shape.bin", 300);
    const scope = scopeFor(tenant);

    await expect(
      user(tenant).vfsStageMultipartHashes(
        scope,
        uploadId,
        0,
        page(0, MULTIPART_HASH_PAGE_SIZE + 1)
      )
    ).rejects.toThrow(/EINVAL.*1\.\.256 hashes, got 257/);
    await expect(
      user(tenant).vfsStageMultipartHashes(scope, uploadId, 0, [])
    ).rejects.toThrow(/EINVAL.*1\.\.256 hashes, got 0/);
    await expect(
      user(tenant).vfsStageMultipartHashes(scope, uploadId, -1, page(0, 1))
    ).rejects.toThrow(/EINVAL.*startIndex -1/);
    await expect(
      user(tenant).vfsStageMultipartHashes(scope, uploadId, 0, ["not-a-hash"])
    ).rejects.toThrow(/EINVAL.*hashes\[0\]/);
    await expect(
      user(tenant).vfsStageMultipartHashes(scope, uploadId, 0, [
        hashFor(0),
        hashFor(0xabc).toUpperCase(),
      ])
    ).rejects.toThrow(/EINVAL.*hashes\[1\]/);

    expect(await stagedState(tenant, uploadId)).toEqual({
      cursor: 0,
      hashes: [],
    });
  });

  it("refuses staging against an unknown session", async () => {
    const tenant = "hash-pages-unknown";
    await beginSession(tenant, "/known.bin", 4);

    await expect(
      user(tenant).vfsStageMultipartHashes(
        scopeFor(tenant),
        "no-such-upload",
        0,
        page(0, 1)
      )
    ).rejects.toThrow(/ENOENT.*session not found/);
  });

  it("refuses staging once the session leaves the open state", async () => {
    const tenant = "hash-pages-state";
    const scope = scopeFor(tenant);
    const finalizing = await beginSession(tenant, "/finalizing.bin", 4);
    const aborted = await beginSession(tenant, "/aborted.bin", 4);
    await user(tenant).vfsStageMultipartHashes(scope, finalizing, 0, page(0, 2));
    await setSessionStatus(tenant, finalizing, "finalizing");
    await user(tenant).vfsAbortMultipart(scope, aborted);

    await expect(
      user(tenant).vfsStageMultipartHashes(scope, finalizing, 2, page(2, 2))
    ).rejects.toThrow(/EBUSY.*status='finalizing'/);
    // Even an exact replay of an accepted page is refused: the session's
    // staged set is no longer the client's to declare.
    await expect(
      user(tenant).vfsStageMultipartHashes(scope, finalizing, 0, page(0, 2))
    ).rejects.toThrow(/EBUSY.*status='finalizing'/);
    await expect(
      user(tenant).vfsStageMultipartHashes(scope, aborted, 0, page(0, 2))
    ).rejects.toThrow(/EBUSY.*status='aborted'/);

    expect(await stagedState(tenant, finalizing)).toEqual({
      cursor: 2,
      hashes: page(0, 2),
    });
  });

  describe("POST /api/vfs/multipart/hash-page", () => {
    it("stages a page and reports progress", async () => {
      const tenant = "hash-pages-route-ok";
      const uploadId = await beginSession(tenant, "/route.bin", 6);

      await expect(
        postHashPage(tenant, { uploadId, startIndex: 0, hashes: page(0, 4) })
      ).resolves.toEqual({ status: 200, body: { staged: 4, total: 6 } });
      await expect(
        postHashPage(tenant, { uploadId, startIndex: 4, hashes: page(4, 2) })
      ).resolves.toEqual({ status: 200, body: { staged: 6, total: 6 } });

      expect(await stagedState(tenant, uploadId)).toEqual({
        cursor: 6,
        hashes: page(0, 6),
      });
    });

    it("refuses an oversized page before dispatching to the DO", async () => {
      const tenant = "hash-pages-route-oversized";
      await beginSession(tenant, "/route-oversized.bin", 4);

      // Both layers refuse an oversized page, so the message is what
      // identifies which one answered. This is the route's wording, and the
      // uploadId names no session, so nothing durable was consulted either.
      const result = await postHashPage(tenant, {
        uploadId: "no-such-upload",
        startIndex: 0,
        hashes: page(0, MULTIPART_HASH_PAGE_SIZE + 1),
      });

      expect(result).toEqual({
        status: 400,
        body: {
          code: "EINVAL",
          message: "body.hashes must hold 1..256 strings",
        },
      });
    });

    it.each([
      ["a missing uploadId", { startIndex: 0, hashes: [hashFor(0)] }],
      ["an empty page", { uploadId: "u", startIndex: 0, hashes: [] }],
      ["a non-array page", { uploadId: "u", startIndex: 0, hashes: "nope" }],
      [
        "a fractional startIndex",
        { uploadId: "u", startIndex: 1.5, hashes: [hashFor(0)] },
      ],
      [
        "a negative startIndex",
        { uploadId: "u", startIndex: -1, hashes: [hashFor(0)] },
      ],
    ])("refuses %s with 400", async (_reason, body) => {
      const result = await postHashPage("hash-pages-route-shape", body);
      expect(result.status).toBe(400);
      expect(result.body.code).toBe("EINVAL");
    });

    it("maps RPC refusals onto their HTTP status", async () => {
      const tenant = "hash-pages-route-errors";
      const uploadId = await beginSession(tenant, "/route-errors.bin", 4);
      await postHashPage(tenant, {
        uploadId,
        startIndex: 0,
        hashes: page(0, 2),
      });

      await expect(
        postHashPage(tenant, {
          uploadId: "no-such-upload",
          startIndex: 0,
          hashes: page(0, 1),
        })
      ).resolves.toMatchObject({ status: 404, body: { code: "ENOENT" } });
      await expect(
        postHashPage(tenant, {
          uploadId,
          startIndex: 0,
          hashes: [hashFor(99)],
        })
      ).resolves.toMatchObject({ status: 409, body: { code: "EBUSY" } });
      await expect(
        postHashPage(tenant, {
          uploadId,
          startIndex: 2,
          hashes: page(2, 3),
        })
      ).resolves.toMatchObject({ status: 400, body: { code: "EINVAL" } });

      expect(await stagedState(tenant, uploadId)).toEqual({
        cursor: 2,
        hashes: page(0, 2),
      });
    });
  });
});
