import {
  env,
  runDurableObjectAlarm,
  runInDurableObject,
} from "cloudflare:test";
import { describe, expect, it } from "vitest";

import { vfsShardDOName, vfsUserDOName } from "@core/lib/utils";
import {
  MULTIPART_MAX_ABORT_ATTEMPTS,
  sweepExpiredMultipartSessions,
} from "@core/objects/user/multipart-upload";
import type { ShardDO } from "@core/objects/shard/shard-do";
import type { UserDO } from "@app/objects/user/user-do";
import { MULTIPART_PROTOCOL_VERSION } from "@shared/multipart";
import type { VFSScope } from "@shared/vfs-types";

/**
 * What may and may not poison a multipart session.
 *
 * A poisoned session is abandoned: its status stops the sweep from selecting
 * it, and whatever the abort still owed stays owed until an operator looks at
 * it. That is the right disposition for exactly one kind of failure — state
 * this Durable Object can inspect and has proven inconsistent, which every
 * replay would trip over identically. It is the wrong disposition for a shard
 * that is not answering, however long it has not been answering, because the
 * work is still there to do.
 *
 * These pin that split, and the counter both policies read:
 *
 *   - a fence that never lands retries past the cap without ever poisoning,
 *     and completes the moment the shard comes back,
 *   - an abort phase no version of this machine writes is retried up to the
 *     cap and only then abandoned, keeping the phase it stuck in,
 *   - a page that makes progress clears the retry bookkeeping, so `attempts`
 *     counts consecutive failures rather than lifetime ones, and
 *   - the sweep takes an aborting session whatever its deadline, and an open
 *     one only once its deadline has passed.
 */

interface ShardFaultControls {
  testConfigureFenceMultipartFailure(remaining: number | null): Promise<void>;
  testClearFenceMultipartFailure(): Promise<void>;
}

type TestShardDO = ShardDO & ShardFaultControls;

interface TestEnv {
  MOSSAIC_USER: DurableObjectNamespace<UserDO>;
  MOSSAIC_SHARD: DurableObjectNamespace<TestShardDO>;
}

const TEST_ENV = env as unknown as TestEnv;
const NS = "default";
const POOL_SIZE = 2;

function scopeFor(tenant: string): VFSScope {
  return { ns: NS, tenant };
}

function userStub(tenant: string): DurableObjectStub<UserDO> {
  return TEST_ENV.MOSSAIC_USER.get(
    TEST_ENV.MOSSAIC_USER.idFromName(vfsUserDOName(NS, tenant))
  );
}

function shardStub(
  tenant: string,
  shardIndex: number
): DurableObjectStub<TestShardDO> {
  return TEST_ENV.MOSSAIC_SHARD.get(
    TEST_ENV.MOSSAIC_SHARD.idFromName(
      vfsShardDOName(NS, tenant, undefined, shardIndex)
    )
  );
}

/** A session with a fixed pool and a deadline a day out. */
async function beginSession(tenant: string, path: string): Promise<string> {
  await userStub(tenant).vfsExists(scopeFor(tenant), "/");
  await runInDurableObject(
    userStub(tenant),
    (_instance: UserDO, state: DurableObjectState) => {
      state.storage.sql.exec(
        `INSERT INTO quota (user_id, pool_size) VALUES (?, ?)
           ON CONFLICT(user_id) DO UPDATE SET pool_size = excluded.pool_size`,
        tenant,
        POOL_SIZE
      );
    }
  );
  const begin = await userStub(tenant).vfsBeginMultipart(
    scopeFor(tenant),
    path,
    { size: 2, chunkSize: 2, protocolVersion: MULTIPART_PROTOCOL_VERSION }
  );
  expect(begin.poolSize).toBe(POOL_SIZE);
  return begin.uploadId;
}

interface SessionState {
  status: string;
  phase: string | null;
  attempts: number;
  retryAt: number;
}

function readSession(
  tenant: string,
  uploadId: string
): Promise<SessionState | undefined> {
  return runInDurableObject(
    userStub(tenant),
    (_instance: UserDO, state: DurableObjectState) =>
      state.storage.sql
        .exec<{
          status: string;
          abort_phase: string | null;
          attempts: number;
          abort_retry_at: number;
        }>(
          `SELECT status, abort_phase, attempts, abort_retry_at
             FROM upload_sessions WHERE upload_id = ?`,
          uploadId
        )
        .toArray()
        .map((row) => ({
          status: row.status,
          phase: row.abort_phase,
          attempts: row.attempts,
          retryAt: row.abort_retry_at,
        }))
        .at(0)
  );
}

function mutateSession(
  tenant: string,
  sql: string,
  ...bindings: (string | number)[]
): Promise<void> {
  return runInDurableObject(
    userStub(tenant),
    (_instance: UserDO, state: DurableObjectState) => {
      state.storage.sql.exec(sql, ...bindings);
    }
  );
}

/**
 * Bring a backed-off session's deadline forward. Stands in for the passage of
 * the backoff the previous failure wrote, which these tests assert separately.
 */
function makeSweepDue(tenant: string, uploadId: string): Promise<void> {
  return mutateSession(
    tenant,
    "UPDATE upload_sessions SET abort_retry_at = 0 WHERE upload_id = ?",
    uploadId
  );
}

function sweep(tenant: string): Promise<{ swept: number; remaining: boolean }> {
  return runInDurableObject(userStub(tenant), (instance: UserDO) =>
    sweepExpiredMultipartSessions(instance, (userId) => ({
      ns: NS,
      tenant: userId,
    }))
  );
}

describe("multipart abort poison policy", () => {
  it("retries an unreachable shard past the cap without poisoning", async () => {
    const tenant = "mp-poison-transient";
    const uploadId = await beginSession(tenant, "/transient.bin");
    // The second shard of the pool never answers, so no fence page can finish.
    await shardStub(tenant, 1).testConfigureFenceMultipartFailure(null);
    await expect(
      userStub(tenant).vfsAbortMultipart(scopeFor(tenant), uploadId)
    ).rejects.toThrow(/injected multipart fence failure/);

    const observed: Array<{ status: string; attempts: number }> = [];
    let previousRetryAt = 0;
    for (let attempt = 1; attempt <= MULTIPART_MAX_ABORT_ATTEMPTS + 2; attempt++) {
      await makeSweepDue(tenant, uploadId);
      // `remaining` is about what is *due*: the failure below backs the
      // session off, so the next tick is owed to the alarm's own deadline
      // query rather than to the sweep's tight cadence.
      expect(await sweep(tenant)).toEqual({ swept: 1, remaining: false });
      const state = await readSession(tenant, uploadId);
      observed.push({
        status: state?.status ?? "missing",
        attempts: state?.attempts ?? -1,
      });
      // Each failure backs the session off further than the last one did.
      const retryAt = state?.retryAt ?? 0;
      expect(retryAt).toBeGreaterThan(previousRetryAt);
      previousRetryAt = retryAt;
    }
    // Seven consecutive remote failures, cap five: still aborting, every one
    // of them counted, none of them a verdict on the session.
    expect(observed).toEqual(
      Array.from({ length: MULTIPART_MAX_ABORT_ATTEMPTS + 2 }, (_u, i) => ({
        status: "aborting",
        attempts: i + 1,
      }))
    );
    expect(await readSession(tenant, uploadId)).toMatchObject({
      phase: "fencing",
    });

    // The work was owed all along, so it completes the moment it can.
    await shardStub(tenant, 1).testClearFenceMultipartFailure();
    await makeSweepDue(tenant, uploadId);
    expect(await sweep(tenant)).toEqual({ swept: 1, remaining: false });
    expect(await readSession(tenant, uploadId)).toEqual({
      status: "aborted",
      phase: "done",
      attempts: 0,
      retryAt: 0,
    });
  });

  it("abandons a corrupt abort phase only past the cap", async () => {
    const tenant = "mp-poison-corrupt";
    const uploadId = await beginSession(tenant, "/corrupt.bin");
    await userStub(tenant).vfsAbortMultipartStep(scopeFor(tenant), uploadId);
    // A phase no version of this machine writes: every replay fails on it.
    await mutateSession(
      tenant,
      "UPDATE upload_sessions SET abort_phase = 'bogus' WHERE upload_id = ?",
      uploadId
    );

    for (let attempt = 1; attempt < MULTIPART_MAX_ABORT_ATTEMPTS; attempt++) {
      await makeSweepDue(tenant, uploadId);
      expect(await sweep(tenant)).toEqual({ swept: 1, remaining: false });
      expect(await readSession(tenant, uploadId)).toMatchObject({
        status: "aborting",
        phase: "bogus",
        attempts: attempt,
      });
    }

    await makeSweepDue(tenant, uploadId);
    expect(await sweep(tenant)).toEqual({ swept: 1, remaining: false });
    // Abandoned, and still carrying the phase it stuck in so an operator can
    // see where it was.
    expect(await readSession(tenant, uploadId)).toEqual({
      status: "poisoned",
      phase: "bogus",
      attempts: MULTIPART_MAX_ABORT_ATTEMPTS,
      retryAt: 0,
    });

    // Structurally invisible to the sweep, and refused rather than retried.
    await makeSweepDue(tenant, uploadId);
    expect(await sweep(tenant)).toEqual({ swept: 0, remaining: false });
    await expect(
      userStub(tenant).vfsAbortMultipart(scopeFor(tenant), uploadId)
    ).rejects.toThrow(/EBUSY.*session status='poisoned'/);
    expect(await readSession(tenant, uploadId)).toMatchObject({
      status: "poisoned",
      attempts: MULTIPART_MAX_ABORT_ATTEMPTS,
    });
  });

  it("keeps a backed-off abort on the alarm's own deadline", async () => {
    const tenant = "mp-poison-deadline";
    const uploadId = await beginSession(tenant, "/backed-off.bin");
    // Armed but not yet fenced, so nothing else on this object owes the alarm
    // a deadline of its own except the session's day-out expiry.
    await shardStub(tenant, 1).testConfigureFenceMultipartFailure(null);
    await expect(
      userStub(tenant).vfsAbortMultipartStep(scopeFor(tenant), uploadId)
    ).rejects.toThrow(/injected multipart fence failure/);
    await shardStub(tenant, 1).testClearFenceMultipartFailure();

    const retryAt = Date.now() + 300_000;
    await mutateSession(
      tenant,
      "UPDATE upload_sessions SET abort_retry_at = ? WHERE upload_id = ?",
      retryAt,
      uploadId
    );
    expect(await runDurableObjectAlarm(userStub(tenant))).toBe(true);
    // Not due, so the sweep left it alone — and the alarm came back for its
    // deadline anyway rather than waiting out the session's expiry.
    expect(await readSession(tenant, uploadId)).toMatchObject({
      status: "aborting",
    });
    expect(
      await runInDurableObject(
        userStub(tenant),
        (_instance: UserDO, state: DurableObjectState) =>
          state.storage.getAlarm()
      )
    ).toBe(retryAt);
  });

  it("clears the retry bookkeeping on a page that makes progress", async () => {
    const tenant = "mp-poison-reset";
    const uploadId = await beginSession(tenant, "/reset.bin");
    await userStub(tenant).vfsAbortMultipartStep(scopeFor(tenant), uploadId);
    // One failure short of the cap, with a backoff still to run.
    await mutateSession(
      tenant,
      `UPDATE upload_sessions SET attempts = ?, abort_retry_at = ?
        WHERE upload_id = ?`,
      MULTIPART_MAX_ABORT_ATTEMPTS - 1,
      Date.now() + 600_000,
      uploadId
    );
    // Backed off, so the sweep leaves it alone.
    expect(await sweep(tenant)).toEqual({ swept: 0, remaining: false });

    await makeSweepDue(tenant, uploadId);
    expect(await sweep(tenant)).toEqual({ swept: 1, remaining: false });
    expect(await readSession(tenant, uploadId)).toEqual({
      status: "aborted",
      phase: "done",
      attempts: 0,
      retryAt: 0,
    });
  });

  it("sweeps an aborting session whatever its deadline, an open one only past it", async () => {
    const tenant = "mp-poison-selection";
    const open = await beginSession(tenant, "/open.bin");
    const aborting = await beginSession(tenant, "/aborting.bin");
    await userStub(tenant).vfsAbortMultipartStep(scopeFor(tenant), aborting);

    // Both deadlines are a day out. Only the aborting session is due.
    expect(await sweep(tenant)).toEqual({ swept: 1, remaining: false });
    expect(await readSession(tenant, aborting)).toMatchObject({
      status: "aborted",
      phase: "done",
    });
    expect(await readSession(tenant, open)).toMatchObject({ status: "open" });

    await mutateSession(
      tenant,
      "UPDATE upload_sessions SET expires_at = ? WHERE upload_id = ?",
      Date.now() - 60_000,
      open
    );
    expect(await sweep(tenant)).toEqual({ swept: 1, remaining: false });
    expect(await readSession(tenant, open)).toMatchObject({
      status: "aborted",
      phase: "done",
    });
  });

  it("carries the abort control columns with their documented defaults", async () => {
    const tenant = "mp-poison-columns";
    await userStub(tenant).vfsExists(scopeFor(tenant), "/");
    const columns = await runInDurableObject(
      userStub(tenant),
      (_instance: UserDO, state: DurableObjectState) =>
        Object.fromEntries(
          state.storage.sql
            .exec<{ name: string; dflt_value: string | null }>(
              "PRAGMA table_info(upload_sessions)"
            )
            .toArray()
            .map((column) => [column.name, column.dflt_value])
        )
    );
    expect(columns).toMatchObject({
      attempts: "0",
      abort_phase: null,
      abort_fence_cursor: "0",
      abort_intent_cursor: "0",
      abort_cleanup_cursor: "0",
      // Seeks by shard index, so it starts one step below zero.
      abort_old_intent_cursor: "-1",
      abort_retry_at: "0",
    });
  });
});
