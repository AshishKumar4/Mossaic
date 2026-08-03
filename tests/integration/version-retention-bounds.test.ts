import {
  SELF,
  env,
  runDurableObjectAlarm,
  runInDurableObject,
} from "cloudflare:test";
import { describe, expect, it, vi } from "vitest";

import { signVFSToken } from "@core/lib/auth";
import { vfsUserDOName } from "@core/lib/utils";
import type { EnvCore } from "@shared/types";
import type { DropVersionsStepResult } from "@shared/vfs-types";
import {
  createMossaicHttpClient,
  createVFS,
  type MossaicEnv,
  type UserDO,
} from "../../sdk/src/index";

/**
 * Retention over a history nobody can process in one turn.
 *
 * A retention policy talks about a whole history, and the only honest way to
 * apply one to a deep history is a bounded operation that resumes. These pin
 * what "bounded" and "resumes" have to mean:
 *
 *   - one invocation visits at most 128 versions, reaps at most 200 manifest
 *     rows, and stages at most 128 shard cleanup routes,
 *   - the scan seeks by `(mtime_ms, version_id)` through the index built for
 *     it, so a late page costs what the first page cost — including when every
 *     version shares an mtime,
 *   - the policy means the same thing it always did: the head counts as the
 *     first `keepLast`, `exceptVersions` is additive, and the head survives,
 *   - a caller that lost a response resumes the same operation rather than
 *     starting a second pass over what is left,
 *   - the one-call contract every released client speaks still answers with
 *     counts, and refuses — before mutating anything — the histories and
 *     manifests it could not finish,
 *   - operations nobody resumed, and operations that finished long ago, are
 *     pruned in bounded batches.
 */

// Seeding thousand-version histories and stepping them to completion is far
// more Durable Object SQL than an ordinary test does.
vi.setConfig({ testTimeout: 120_000 });

interface TestEnv {
  MOSSAIC_USER: DurableObjectNamespace<UserDO>;
  MOSSAIC_SHARD: DurableObjectNamespace;
}

const E = env as unknown as TestEnv;
const NS = "default";
const HISTORY_PATH = "/history.txt";

/** Versions one invocation may visit, mirroring the server's own bound. */
const SCAN_LIMIT = 128;

function envFor(): MossaicEnv {
  return {
    MOSSAIC_USER: E.MOSSAIC_USER as MossaicEnv["MOSSAIC_USER"],
    MOSSAIC_SHARD: E.MOSSAIC_SHARD as unknown as MossaicEnv["MOSSAIC_SHARD"],
  };
}

function scopeFor(tenant: string): { ns: string; tenant: string } {
  return { ns: NS, tenant };
}

function userStub(tenant: string): DurableObjectStub<UserDO> {
  return E.MOSSAIC_USER.get(
    E.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
  );
}

interface SeededHistory {
  tenant: string;
  vfs: ReturnType<typeof createVFS>;
  stub: DurableObjectStub<UserDO>;
  pathId: string;
  headVersionId: string;
  /** Seeded ids, newest first — the order retention visits them in. */
  versionIds: string[];
}

/**
 * A path whose head came from a real write, plus `count` inline versions
 * underneath it. Seeding the history directly is the only way to reach the
 * depths these bounds are about; the rows carry the same shape
 * `commitVersion` writes.
 *
 * `mtimeStride` of zero gives every seeded version the same mtime, which is
 * the case the `(mtime_ms, version_id)` ordering exists for.
 */
async function seedHistory(
  tenant: string,
  count: number,
  mtimeStride = 1
): Promise<SeededHistory> {
  const vfs = createVFS(envFor(), { tenant, versioning: "enabled" });
  await vfs.writeFile(HISTORY_PATH, "head");
  const stub = userStub(tenant);
  const seeded = await runInDurableObject(stub, (_instance, state) => {
    const sql = state.storage.sql;
    const file = sql
      .exec<{ file_id: string; head_version_id: string; updated_at: number }>(
        `SELECT file_id, head_version_id, updated_at FROM files
          WHERE file_name = 'history.txt' AND user_id = ?`,
        tenant
      )
      .toArray()[0];
    if (file === undefined) throw new Error("seed: head version is missing");
    const versionIds: string[] = [];
    for (let index = 0; index < count; index++) {
      const versionId = `seed-${index.toString().padStart(6, "0")}`;
      versionIds.push(versionId);
      sql.exec(
        `INSERT INTO file_versions
           (path_id, version_id, user_id, size, mode, mtime_ms, deleted,
            inline_data, chunk_size, chunk_count, file_hash, mime_type,
            user_visible)
         VALUES (?, ?, ?, 1, 420, ?, 0, ?, 0, 0, '', 'text/plain', 1)`,
        file.file_id,
        versionId,
        tenant,
        file.updated_at - 1 - index * mtimeStride,
        new Uint8Array([index & 0xff])
      );
    }
    sql.exec(
      `UPDATE quota
          SET storage_used = storage_used + ?, inline_bytes_used = inline_bytes_used + ?
        WHERE user_id = ?`,
      count,
      count,
      tenant
    );
    return {
      pathId: file.file_id,
      headVersionId: file.head_version_id,
      versionIds,
    };
  });
  return { tenant, vfs, stub, ...seeded };
}

/** Manifest rows for one seeded version, spread over `shardCount` shards. */
async function seedManifest(
  seeded: SeededHistory,
  versionId: string,
  rows: number,
  shardCount: number
): Promise<void> {
  await runInDurableObject(seeded.stub, (_instance, state) => {
    for (let index = 0; index < rows; index++) {
      state.storage.sql.exec(
        `INSERT INTO version_chunks
           (version_id, chunk_index, chunk_hash, chunk_size, shard_index)
         VALUES (?, ?, ?, 1, ?)`,
        versionId,
        index,
        index.toString(16).padStart(64, "0"),
        index % shardCount
      );
    }
  });
}

/**
 * Operation rows in a given state, as the step surface would have written
 * them. Seeded directly because the caps they exercise are about how many rows
 * exist, not about how each one got there.
 */
async function seedOperations(
  seeded: SeededHistory,
  status: "running" | "done",
  count: number
): Promise<void> {
  await runInDurableObject(seeded.stub, (_instance, state) => {
    const now = Date.now();
    for (let index = 0; index < count; index++) {
      state.storage.sql.exec(
        `INSERT INTO version_retention_operations
           (operation_id, user_id, path_id, policy_json, status,
            plan_generation, plan_head_version_id, cursor_mtime_ms,
            cursor_version_id, remaining_keep, dropped, kept,
            pending_version_id, pending_mtime_ms, pending_ref_id,
            created_at, updated_at)
         VALUES (?, ?, ?, '{}', ?, 0, ?, NULL, NULL, 0, 0, 0, NULL, NULL,
                 NULL, ?, ?)`,
        `seeded-${index.toString().padStart(6, "0")}`,
        seeded.tenant,
        seeded.pathId,
        status,
        seeded.headVersionId,
        now - count + index,
        now - count + index
      );
    }
  });
}

function countRows(
  stub: DurableObjectStub<UserDO>,
  table: string,
  where = ""
): Promise<number> {
  return runInDurableObject(stub, (_instance, state) => {
    const row = state.storage.sql
      .exec<{ n: number }>(`SELECT COUNT(*) AS n FROM ${table} ${where}`)
      .toArray()[0];
    return row === undefined ? 0 : row.n;
  });
}

function step(
  seeded: SeededHistory,
  policy: Parameters<UserDO["vfsDropVersionsStep"]>[2],
  operationId: string
): Promise<DropVersionsStepResult> {
  return seeded.stub.vfsDropVersionsStep(
    scopeFor(seeded.tenant),
    HISTORY_PATH,
    policy,
    operationId
  );
}

/** Drive an operation to completion, reporting how many steps it took. */
async function stepToCompletion(
  seeded: SeededHistory,
  policy: Parameters<UserDO["vfsDropVersionsStep"]>[2],
  operationId: string,
  maxSteps = 64
): Promise<{ result: DropVersionsStepResult; steps: number }> {
  for (let steps = 1; steps <= maxSteps; steps++) {
    const result = await step(seeded, policy, operationId);
    if (result.done) return { result, steps };
  }
  throw new Error(`retention did not finish within ${maxSteps} steps`);
}

describe("bounded version retention", () => {
  it("completes a history deeper than one step across steps", async () => {
    const seeded = await seedHistory("retention-deep-history", 300);

    const first = await step(seeded, {}, "deep-history");
    expect(first).toEqual({ done: false });
    // The step visits the head — kept — and then drops the rest of its budget.
    expect(await countRows(seeded.stub, "file_versions")).toBe(
      301 - (SCAN_LIMIT - 1)
    );

    const { result, steps } = await stepToCompletion(
      seeded,
      {},
      "deep-history"
    );
    expect(result).toEqual({ done: true, dropped: 300, kept: 1 });
    // 301 versions visited at 128 a step, after the one above.
    expect(steps).toBe(2);
    expect(await countRows(seeded.stub, "file_versions")).toBe(1);
    expect(
      await seeded.vfs.readFile(HISTORY_PATH, { encoding: "utf8" })
    ).toBe("head");
  });

  it("reaps at most 200 manifest rows in one step", async () => {
    const seeded = await seedHistory("retention-manifest-bound", 1);
    const [oldest] = seeded.versionIds;
    if (oldest === undefined) throw new Error("seed: version is missing");
    await seedManifest(seeded, oldest, 500, 1);

    expect(await step(seeded, {}, "manifest-bound")).toEqual({ done: false });
    expect(await countRows(seeded.stub, "version_chunks")).toBe(300);
    expect(await step(seeded, {}, "manifest-bound")).toEqual({ done: false });
    expect(await countRows(seeded.stub, "version_chunks")).toBe(100);

    const { result } = await stepToCompletion(seeded, {}, "manifest-bound");
    expect(result).toEqual({ done: true, dropped: 1, kept: 1 });
    expect(await countRows(seeded.stub, "version_chunks")).toBe(0);
  });

  it("stages at most 128 cleanup routes in one step", async () => {
    const seeded = await seedHistory("retention-route-bound", 1);
    const [oldest] = seeded.versionIds;
    if (oldest === undefined) throw new Error("seed: version is missing");
    // One shard per manifest row, so the route budget binds before the
    // manifest budget and the page is reaped as a prefix.
    await seedManifest(seeded, oldest, 200, 200);

    expect(await step(seeded, {}, "route-bound")).toEqual({ done: false });
    expect(await countRows(seeded.stub, "version_chunks")).toBe(200 - 128);

    const { result } = await stepToCompletion(seeded, {}, "route-bound");
    expect(result).toEqual({ done: true, dropped: 1, kept: 1 });
    expect(await countRows(seeded.stub, "version_chunks")).toBe(0);
  });

  it("seeks a late page for what the first page cost", async () => {
    const seeded = await seedHistory("retention-seek-cost", 1100);
    const plan = await runInDurableObject(seeded.stub, (_instance, state) =>
      state.storage.sql
        .exec<{ detail: string }>(
          `EXPLAIN QUERY PLAN
             SELECT version_id, mtime_ms FROM file_versions
              WHERE path_id = ? AND (mtime_ms, version_id) < (?, ?)
              ORDER BY mtime_ms DESC, version_id DESC LIMIT 1`,
          seeded.pathId,
          0,
          ""
        )
        .toArray()
        .map((row) => row.detail)
        .join(" | ")
    );
    expect(plan).toContain("idx_file_versions_retention_seek");
    expect(plan).not.toContain("TEMP B-TREE");
    expect(plan).not.toContain("SCAN file_versions");

    // The rows the seek itself touches, from the top of the history and from a
    // cursor a thousand versions into it. A predicate that re-derived its
    // position by excluding everything already visited would read the whole
    // visited prefix here instead of seeking past it.
    const seekCost = async (afterIndex: number): Promise<number> =>
      runInDurableObject(seeded.stub, (_instance, state) => {
        const cursor = state.storage.sql
          .exec<{ version_id: string; mtime_ms: number }>(
            `SELECT version_id, mtime_ms FROM file_versions
              WHERE path_id = ?
              ORDER BY mtime_ms DESC, version_id DESC LIMIT 1 OFFSET ?`,
            seeded.pathId,
            afterIndex
          )
          .toArray()[0];
        if (cursor === undefined) throw new Error("seek: cursor is missing");
        const seek = state.storage.sql.exec(
          `SELECT version_id, mtime_ms FROM file_versions
            WHERE path_id = ? AND (mtime_ms, version_id) < (?, ?)
            ORDER BY mtime_ms DESC, version_id DESC LIMIT 1`,
          seeded.pathId,
          cursor.mtime_ms,
          cursor.version_id
        );
        seek.toArray();
        return seek.rowsRead;
      });

    const firstPage = await seekCost(0);
    const latePage = await seekCost(1000);
    expect(firstPage).toBeLessThanOrEqual(4);
    expect(latePage).toBeLessThanOrEqual(firstPage + 1);
  });

  it("visits every version exactly once when they share an mtime", async () => {
    const seeded = await seedHistory("retention-equal-mtime", 300, 0);
    await runInDurableObject(seeded.stub, (_instance, state) => {
      state.storage.sql.exec(
        "UPDATE file_versions SET mtime_ms = 1700000000000"
      );
    });

    expect(await step(seeded, {}, "equal-mtime")).toEqual({ done: false });
    // Restart safety is the cursor: it names one row of the tied batch, and
    // the descending pair order says which of them are still unvisited.
    const cursor = await runInDurableObject(seeded.stub, (_instance, state) =>
      state.storage.sql
        .exec<{ cursor_mtime_ms: number; cursor_version_id: string }>(
          `SELECT cursor_mtime_ms, cursor_version_id
             FROM version_retention_operations WHERE operation_id = ?`,
          "equal-mtime"
        )
        .toArray()[0]
    );
    expect(cursor).toEqual({
      cursor_mtime_ms: 1700000000000,
      // Ties break on version_id descending, so the step walked seed-000299
      // down to here — and the next step resumes strictly below this pair.
      cursor_version_id: "seed-000172",
    });

    const { result } = await stepToCompletion(seeded, {}, "equal-mtime");
    expect(result).toEqual({ done: true, dropped: 300, kept: 1 });
    expect(await countRows(seeded.stub, "file_versions")).toBe(1);
  });

  it("restarts the plan when a write lands a new head mid-operation", async () => {
    const seeded = await seedHistory("retention-plan-fence", 260);
    expect(await step(seeded, { keepLast: 1 }, "plan-fence")).toEqual({
      done: false,
    });

    // "Keep the newest one" now means a different version, so the remaining
    // plan is void: the scan restarts and the version it was keeping is not.
    await seeded.vfs.writeFile(HISTORY_PATH, "new-head");
    const { result } = await stepToCompletion(
      seeded,
      { keepLast: 1 },
      "plan-fence"
    );
    expect(result).toEqual({ done: true, dropped: 261, kept: 1 });
    expect(
      await seeded.vfs.readFile(HISTORY_PATH, { encoding: "utf8" })
    ).toBe("new-head");
    // The restart bumps the plan fence, and the counts it kept — never the
    // bytes it already reaped — are what it recomputes.
    const operation = await runInDurableObject(seeded.stub, (_instance, state) =>
      state.storage.sql
        .exec<{ plan_generation: number; status: string }>(
          `SELECT plan_generation, status FROM version_retention_operations
            WHERE operation_id = ?`,
          "plan-fence"
        )
        .toArray()[0]
    );
    expect(operation).toEqual({ plan_generation: 1, status: "done" });
    expect(
      await countRows(seeded.stub, "audit_log", "WHERE op = 'dropVersions'")
    ).toBe(1);
  });

  it("counts the head toward keepLast and adds exceptVersions on top", async () => {
    const seeded = await seedHistory("retention-policy-additive", 300);
    const exceptVersions = [
      seeded.versionIds[150],
      seeded.versionIds[280],
    ].filter((versionId): versionId is string => versionId !== undefined);
    expect(exceptVersions).toHaveLength(2);

    const { result } = await stepToCompletion(
      seeded,
      { keepLast: 3, exceptVersions },
      "policy-additive"
    );
    // keepLast 3 = the head plus the two newest seeded versions; the two
    // exceptions survive without spending a keepLast slot.
    expect(result).toEqual({ done: true, dropped: 296, kept: 5 });
    const survivors = await seeded.vfs.listVersions(HISTORY_PATH, {
      limit: 400,
    });
    expect(survivors.map((version) => version.id).sort()).toEqual(
      [
        seeded.headVersionId,
        seeded.versionIds[0],
        seeded.versionIds[1],
        ...exceptVersions,
      ].sort()
    );
  });

  it("keeps the head when the policy targets every version", async () => {
    const seeded = await seedHistory("retention-head-survives", 200);

    const { result } = await stepToCompletion(
      seeded,
      { olderThan: Date.now() + 3_600_000 },
      "head-survives"
    );
    expect(result).toEqual({ done: true, dropped: 200, kept: 1 });
    expect(
      await seeded.vfs.readFile(HISTORY_PATH, { encoding: "utf8" })
    ).toBe("head");
  });

  it("resumes the same operation after a lost response", async () => {
    const seeded = await seedHistory("retention-response-loss", 300);
    const operationId = "response-loss";

    // The caller never saw this response, so it repeats the call. The step it
    // repeats must continue the walk, not restart it.
    expect(await step(seeded, {}, operationId)).toEqual({ done: false });
    const { result } = await stepToCompletion(seeded, {}, operationId);
    expect(result).toEqual({ done: true, dropped: 300, kept: 1 });

    // And the terminal response is replayable: the recorded counts come back
    // without a second pass and without a second audit entry.
    expect(await step(seeded, {}, operationId)).toEqual({
      done: true,
      dropped: 300,
      kept: 1,
    });
    expect(
      await countRows(seeded.stub, "audit_log", "WHERE op = 'dropVersions'")
    ).toBe(1);
    expect(await countRows(seeded.stub, "file_versions")).toBe(1);
  });

  it("refuses an operation id reused with different parameters", async () => {
    const seeded = await seedHistory("retention-parameter-drift", 200);
    expect(await step(seeded, { keepLast: 2 }, "drift")).toEqual({
      done: false,
    });

    await expect(step(seeded, { keepLast: 3 }, "drift")).rejects.toThrow(
      /EINVAL.*different parameters/
    );
    await expect(step(seeded, {}, "")).rejects.toThrow(
      /EINVAL.*invalid operation id/
    );
    await expect(step(seeded, { keepLast: -1 }, "bad-policy")).rejects.toThrow(
      /EINVAL.*keepLast/
    );
  });

  it("keeps the one-call contract for a history it can finish", async () => {
    const seeded = await seedHistory("retention-legacy-bounded", 100);

    await expect(
      seeded.stub.vfsDropVersions(scopeFor(seeded.tenant), HISTORY_PATH, {})
    ).resolves.toEqual({ dropped: 100, kept: 1 });
    expect(await countRows(seeded.stub, "file_versions")).toBe(1);
  });

  it("refuses a one-call retention it cannot finish, before mutating", async () => {
    const seeded = await seedHistory("retention-legacy-deep", 200);

    await expect(
      seeded.stub.vfsDropVersions(scopeFor(seeded.tenant), HISTORY_PATH, {})
    ).rejects.toThrow(/EFBIG.*one-call retention capability/);
    expect(await countRows(seeded.stub, "file_versions")).toBe(201);
  });

  it("refuses an oversized legacy manifest before mutating", async () => {
    const seeded = await seedHistory("retention-legacy-manifest", 2);
    const [, older] = seeded.versionIds;
    if (older === undefined) throw new Error("seed: version is missing");
    await seedManifest(seeded, older, 200, 4);

    await expect(
      seeded.stub.vfsDropVersions(scopeFor(seeded.tenant), HISTORY_PATH, {})
    ).rejects.toThrow(/EFBIG.*one-call retention capability/);
    expect(await countRows(seeded.stub, "file_versions")).toBe(3);
    expect(await countRows(seeded.stub, "version_chunks")).toBe(200);

    // A manifest inside the bound still goes through the one-call path.
    await runInDurableObject(seeded.stub, (_instance, state) => {
      state.storage.sql.exec(
        "DELETE FROM version_chunks WHERE chunk_index >= 100"
      );
    });
    await expect(
      seeded.stub.vfsDropVersions(scopeFor(seeded.tenant), HISTORY_PATH, {})
    ).resolves.toEqual({ dropped: 2, kept: 1 });
    expect(await countRows(seeded.stub, "version_chunks")).toBe(0);
  });

  it("drives the bounded steps from the binding client's dropVersions", async () => {
    const seeded = await seedHistory("retention-sdk-binding", 300);

    // The published shape does not change for a history that needs paging.
    await expect(seeded.vfs.dropVersions(HISTORY_PATH, {})).resolves.toEqual({
      dropped: 300,
      kept: 1,
    });
    expect(await countRows(seeded.stub, "file_versions")).toBe(1);
  });

  it("exposes the bounded surface for callers that own the pacing", async () => {
    const seeded = await seedHistory("retention-sdk-steps", 300);

    let progress = await seeded.vfs.startDropVersions(HISTORY_PATH, {});
    expect(progress).toEqual({
      done: false,
      operation: {
        kind: "drop-versions",
        operationId: expect.stringMatching(/^dv-/),
      },
    });
    const operation = progress.operation;
    while (!progress.done) {
      progress = await seeded.vfs.stepDropVersions(
        HISTORY_PATH,
        {},
        operation
      );
      expect(progress.operation).toEqual(operation);
    }
    expect(progress).toMatchObject({ done: true, dropped: 300, kept: 1 });
  });

  it("drives the bounded steps over HTTP", async () => {
    const tenant = "retention-http";
    const seeded = await seedHistory(tenant, 300);
    const apiKey = await signVFSToken(env as unknown as EnvCore, {
      ns: NS,
      tenant,
    });
    const requested: string[] = [];
    const http = createMossaicHttpClient({
      url: "https://mossaic.test",
      apiKey,
      fetcher: async (input, init) => {
        const url =
          typeof input === "string"
            ? input
            : input instanceof URL
              ? input.toString()
              : input.url;
        requested.push(new URL(url).pathname);
        return await SELF.fetch(url, init);
      },
    });

    await expect(http.dropVersions(HISTORY_PATH, {})).resolves.toEqual({
      dropped: 300,
      kept: 1,
    });
    // The one-call route is tried first, so a server without the step route
    // answers this exactly as it always did.
    expect(requested[0]).toBe("/api/vfs/dropVersions");
    expect(requested.slice(1)).toEqual([
      "/api/vfs/dropVersionsStep",
      "/api/vfs/dropVersionsStep",
      "/api/vfs/dropVersionsStep",
    ]);
    expect(await countRows(seeded.stub, "file_versions")).toBe(1);
  });

  it("prunes operations nobody resumed and operations long finished", async () => {
    const seeded = await seedHistory("retention-prune", 200);
    expect(await step(seeded, {}, "abandoned")).toEqual({ done: false });
    const { result } = await stepToCompletion(seeded, { keepLast: 1 }, "settled");
    expect(result).toMatchObject({ done: true });
    expect(
      await countRows(seeded.stub, "version_retention_operations")
    ).toBe(2);

    // Age both rows past their windows: a running operation stops being
    // resumable after a day, a finished one stops being replayable after a
    // week. Neither holds a half-reaped version, so both are prunable.
    await runInDurableObject(seeded.stub, (_instance, state) => {
      state.storage.sql.exec(
        `UPDATE version_retention_operations
            SET updated_at = ? WHERE status = 'running'`,
        Date.now() - 25 * 60 * 60 * 1000
      );
      state.storage.sql.exec(
        `UPDATE version_retention_operations
            SET updated_at = ? WHERE status = 'done'`,
        Date.now() - 8 * 24 * 60 * 60 * 1000
      );
    });

    // Any later step runs the same bounded maintenance pass.
    expect(await step(seeded, {}, "after-prune")).toMatchObject({
      done: true,
    });
    const remaining = await runInDurableObject(seeded.stub, (_instance, state) =>
      state.storage.sql
        .exec<{ operation_id: string }>(
          "SELECT operation_id FROM version_retention_operations"
        )
        .toArray()
        .map((row) => row.operation_id)
    );
    expect(remaining).toEqual(["after-prune"]);
  });

  it("finishes an operation whose path went away underneath it", async () => {
    const seeded = await seedHistory("retention-path-gone", 200);
    expect(await step(seeded, {}, "path-gone")).toEqual({ done: false });
    // The rows the operation still owes outlive the path itself, and the alarm
    // addresses the operation by path id, so it has to finish rather than fail
    // on a path that is no longer there.
    await runInDurableObject(seeded.stub, (_instance, state) => {
      state.storage.sql.exec("DELETE FROM files WHERE file_id = ?", seeded.pathId);
    });

    for (let tick = 0; tick < 8; tick++) {
      if (!(await runDurableObjectAlarm(seeded.stub))) break;
      const status = await runInDurableObject(seeded.stub, (_instance, state) =>
        state.storage.sql
          .exec<{ status: string }>(
            "SELECT status FROM version_retention_operations WHERE operation_id = ?",
            "path-gone"
          )
          .toArray()[0]
      );
      if (status?.status === "done") break;
    }

    expect(await countRows(seeded.stub, "file_versions")).toBe(0);
  });

  it("evicts the oldest finished operations once the retained set is full", async () => {
    const seeded = await seedHistory("retention-retained-cap", 1);
    await seedOperations(seeded, "done", 129);

    // Any creation runs the bounded maintenance pass, which evicts a batch
    // rather than the whole overflow.
    expect(await step(seeded, {}, "after-cap")).toMatchObject({ done: true });
    expect(
      await countRows(seeded.stub, "version_retention_operations")
    ).toBe(129 - 32 + 1);
  });

  it("refuses more operations in flight than a tenant may hold", async () => {
    const seeded = await seedHistory("retention-inflight-cap", 1);
    await seedOperations(seeded, "running", 65);

    await expect(step(seeded, {}, "over-capacity")).rejects.toThrow(
      /EBUSY.*too many retention operations in flight/
    );
    // Stepping one that already exists is not a new operation, so it still
    // works while the tenant is at its cap.
    expect(await step(seeded, {}, "seeded-000000")).toMatchObject({
      done: true,
    });
  });

  it("finishes an abandoned operation from the maintenance alarm", async () => {
    const seeded = await seedHistory("retention-alarm-resume", 300);
    expect(await step(seeded, {}, "abandoned-by-caller")).toEqual({
      done: false,
    });

    // Nobody is coming back for it, so the alarm is what has to finish it.
    for (let tick = 0; tick < 8; tick++) {
      const ran = await runDurableObjectAlarm(seeded.stub);
      if (!ran) break;
      const status = await runInDurableObject(seeded.stub, (_instance, state) =>
        state.storage.sql
          .exec<{ status: string }>(
            "SELECT status FROM version_retention_operations WHERE operation_id = ?",
            "abandoned-by-caller"
          )
          .toArray()[0]
      );
      if (status?.status === "done") break;
    }

    expect(await step(seeded, {}, "abandoned-by-caller")).toEqual({
      done: true,
      dropped: 300,
      kept: 1,
    });
    expect(await countRows(seeded.stub, "file_versions")).toBe(1);
  });
});
