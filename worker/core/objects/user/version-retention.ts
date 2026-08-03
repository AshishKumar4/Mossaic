/**
 * Bounded, resumable version retention.
 *
 * A retention policy is a statement about a whole history, and a history is as
 * deep as its owner made it — so applying one cannot be a single transaction.
 * This module is that policy expressed as a durable operation on the shared
 * control plane in `lib/paged-operation`: one invocation walks a bounded
 * prefix of the remaining history, drops what the policy does not keep, and
 * persists exactly where it got to. A caller that lost its response, an
 * evicted Durable Object, or an abandoned client all resume from the same row
 * instead of starting again.
 *
 * ── the scan ──────────────────────────────────────────────────────────
 *
 * Versions are visited newest-first by `(mtime_ms DESC, version_id DESC)`, and
 * the cursor is the last visited pair. Resuming is therefore a row-value range
 * seek — `(mtime_ms, version_id) < (cursor…)` — served end-to-end by
 * `idx_file_versions_retention_seek`. That ordering is total even when a batch
 * of versions shares an mtime, which is what makes an interrupted scan restart
 * safe: the pair a step committed cannot be re-visited, and no unvisited pair
 * can be skipped. The alternative — "everything newer than the cursor mtime,
 * minus what I already saw" — rescans the prefix on every step and turns a
 * deep history quadratic.
 *
 * ── what one invocation may do ────────────────────────────────────────
 *
 * At most `DROP_VERSIONS_SCAN_LIMIT` versions are visited,
 * `DROP_VERSIONS_MANIFEST_LIMIT` manifest rows are reaped, and
 * `DROP_VERSIONS_CLEANUP_ROUTE_LIMIT` shard cleanup routes are staged. Every
 * loop in here is bounded by one of those three budgets or by the cursor
 * advancing, so no input makes an invocation unbounded.
 *
 * One invocation is one page, so the driver's `runOperationPages` loop is not
 * consumed here: the caller stepping the operation, or the maintenance alarm
 * resuming it, is the loop. What the driver does own is every persisted
 * transition — `commitOperationTransition` under the `(status,
 * plan_generation)` fence, with `done` as the terminal phase no page can leave.
 *
 * ── the order a version comes apart in ────────────────────────────────
 *
 * A version's metadata row dies first, then its manifest is reaped a page at a
 * time and each page stages the cleanup its shards are owed. That order is
 * load-bearing: once the `file_versions` row is gone no reader can resolve the
 * version, so no reader can be handed bytes whose chunk refs are already being
 * reaped. The reverse order would let a shard reap chunks a still-readable
 * version claims. `pending_version_id` is the record of a version caught
 * between the two, which is why an operation holding one is never pruned.
 *
 * ── the plan fence ────────────────────────────────────────────────────
 *
 * "Keep the newest N" is a statement about the history as it was when the
 * operation started, so a version committed mid-operation invalidates the
 * remaining plan. Every commit flips `files.head_version_id`, so the head is
 * the fence: when it moves, `plan_generation` advances, the cursor restarts,
 * and the keep budget is recomputed against the new head. `dropped` survives
 * that reset because those bytes are already gone; `kept` does not, because it
 * describes the scan being restarted.
 */

import { VFSError } from "../../../../shared/vfs-types";
import type {
  DropVersionsPolicy,
  DropVersionsStepResult,
  VFSScope,
} from "../../../../shared/vfs-types";
import {
  commitOperationTransition,
  heldProgress,
  type PagedOperationTable,
  type RetryPolicy,
} from "../../lib/paged-operation";
import { pruneOldestRows } from "../../lib/row-retention";
import {
  lastSqlChanges,
  scheduleStaleUploadSweep,
  stageChunkCleanupIntent,
  transactionSync,
} from "./internal-storage";
import type { UserDOCore as UserDO } from "./user-do-core";
import { drainChunkCleanupIntents } from "./vfs-ops";
import { insertAuditLog } from "./vfs/audit-log";
import { recordWriteUsage, userIdFor } from "./vfs/helpers";
import { shardRefId } from "./vfs-versions";

/** Versions one invocation may visit, kept or dropped. */
const DROP_VERSIONS_SCAN_LIMIT = 128;

/** `version_chunks` rows one invocation may reap. */
const DROP_VERSIONS_MANIFEST_LIMIT = 200;

/**
 * Distinct `(ref_id, shard_index)` cleanup routes one invocation may stage.
 * A manifest page whose shards would exceed it is reaped as a prefix, so the
 * budget throttles the fan-out without ever blocking progress.
 */
const DROP_VERSIONS_CLEANUP_ROUTE_LIMIT = 128;

/**
 * The deepest history and the largest drop-set manifest the one-call legacy
 * contract accepts. Both are what a single invocation provably finishes:
 * beyond them the caller has to use the bounded step surface.
 */
const DROP_VERSIONS_LEGACY_VERSION_MAX = DROP_VERSIONS_SCAN_LIMIT;
const DROP_VERSIONS_LEGACY_MANIFEST_MAX = Math.min(
  DROP_VERSIONS_MANIFEST_LIMIT,
  DROP_VERSIONS_CLEANUP_ROUTE_LIMIT
);

const DROP_VERSIONS_KEEP_LAST_MAX = 100_000;
const DROP_VERSIONS_EXCEPT_MAX = 1_000;

/** Caller-supplied identifiers are keys and audit payloads, so they are shaped. */
const RETENTION_ID_PATTERN = /^[A-Za-z0-9._:-]+$/;
const RETENTION_ID_MAX_LENGTH = 128;

/**
 * How long an operation nobody comes back for stays resumable. It is not a
 * deadline on the work — the maintenance alarm finishes running operations —
 * but on the client's ability to resume this one by id.
 */
const RUNNING_RETENTION_TTL_MS = 24 * 60 * 60 * 1000;

/**
 * How long a completed operation keeps answering with its recorded counts, so
 * a caller that lost the terminal response reads it instead of recounting.
 */
const COMPLETED_RETENTION_TTL_MS = 7 * 24 * 60 * 60 * 1000;

/** Completed operations retained regardless of age, oldest evicted first. */
const COMPLETED_RETENTION_MAX = 128;

/** Operations one maintenance pass may prune, per predicate. */
const RETENTION_PRUNE_LIMIT = 32;

/** Operations one tenant may have in flight at once. */
const RUNNING_RETENTION_MAX = 64;

/** Shard refs handed to one cleanup drain call. */
const RETENTION_DRAIN_REF_BATCH = 32;

const RETENTION_STATUSES = ["running", "done"] as const;
type RetentionStatus = (typeof RETENTION_STATUSES)[number];

/**
 * Backoff a resumed retention would wait between attempts. Only the driver's
 * claim plane reads it, and a retention operation is addressed directly by its
 * operation id rather than claimed, so it is declared here as the operation's
 * stated policy while the maintenance alarm's fixed cadence is what actually
 * paces resumption.
 *
 * The driver's `PoisonPolicy` is deliberately not consumed. A retention step
 * has no remote call inside it, so its only failures are local: either the
 * caller sees the throw and decides, or the alarm retries deterministically.
 * There is no attempt count at which abandoning a half-reaped version becomes
 * correct — the manifest rows it left are still owed — which is exactly the
 * case the poison policy says must never be abandoned.
 */
const VERSION_RETENTION_RETRY: RetryPolicy = {
  baseMs: 1_000,
  maxMs: 60_000,
  maxDoublings: 6,
};

/**
 * The control-plane columns `lib/paged-operation` fences this machine on.
 *
 * `plan_generation` is the operation's own fence rather than a mirror of
 * anything: it advances exactly when the plan restarts, which is the one event
 * that legitimately rewinds the seek cursor and the keep budget. The
 * per-version bookkeeping below it — the cursor pair, the pending version — is
 * not declared, because it restarts for every version without any declared
 * outer component advancing. That is domain state written inside the
 * transition's transaction, which is the boundary the driver's rule draws.
 *
 * The claim and retry column sets are omitted: this operation is addressed by
 * one caller at a time, and the whole step is a single synchronous
 * transaction, so `(status, plan_generation)` is the entire fence it needs.
 */
const VERSION_RETENTION_OPERATION: PagedOperationTable<RetentionStatus> = {
  table: "version_retention_operations",
  keyColumns: ["operation_id"],
  phases: {
    column: "status",
    forward: RETENTION_STATUSES,
    terminal: ["done"],
  },
  cursorColumns: ["plan_generation"],
  retry: VERSION_RETENTION_RETRY,
};

interface RetentionOperationRow extends Record<string, SqlStorageValue> {
  operation_id: string;
  user_id: string;
  path_id: string;
  policy_json: string;
  status: string;
  plan_generation: number;
  plan_head_version_id: string | null;
  cursor_mtime_ms: number | null;
  cursor_version_id: string | null;
  remaining_keep: number;
  dropped: number;
  kept: number;
  pending_version_id: string | null;
  pending_mtime_ms: number | null;
  pending_ref_id: string | null;
}

/** One version as the scan sees it. */
interface RetentionCandidate extends Record<string, SqlStorageValue> {
  version_id: string;
  mtime_ms: number;
}

interface RetentionManifestRow extends Record<string, SqlStorageValue> {
  chunk_index: number;
  shard_index: number;
}

/** Whether a version survives, and whether surviving used a `keepLast` slot. */
interface RetentionKeepDecision {
  keep: boolean;
  consumesKeepSlot: boolean;
}

// ── policy ───────────────────────────────────────────────────────────

function validateRetentionId(value: unknown, label: string): string {
  if (
    typeof value !== "string" ||
    value.length === 0 ||
    value.length > RETENTION_ID_MAX_LENGTH ||
    !RETENTION_ID_PATTERN.test(value)
  ) {
    throw new VFSError("EINVAL", `dropVersions: invalid ${label}`);
  }
  return value;
}

/**
 * Narrow an untrusted policy to the canonical form persisted with the
 * operation. The serialization is the compare-and-set token a resumed step is
 * checked against, so the key order here is part of the contract: it must be
 * stable across a round trip through `policy_json`.
 */
function validateDropVersionsPolicy(policy: unknown): DropVersionsPolicy {
  if (policy === null || typeof policy !== "object" || Array.isArray(policy)) {
    throw new VFSError("EINVAL", "dropVersions: policy must be an object");
  }
  const input: Record<string, unknown> = { ...policy };
  const normalized: DropVersionsPolicy = {};
  if (input.olderThan !== undefined) {
    if (
      typeof input.olderThan !== "number" ||
      !Number.isFinite(input.olderThan)
    ) {
      throw new VFSError("EINVAL", "dropVersions: olderThan must be finite");
    }
    normalized.olderThan = input.olderThan;
  }
  if (input.keepLast !== undefined) {
    if (
      typeof input.keepLast !== "number" ||
      !Number.isInteger(input.keepLast) ||
      input.keepLast < 0 ||
      input.keepLast > DROP_VERSIONS_KEEP_LAST_MAX
    ) {
      throw new VFSError(
        "EINVAL",
        `dropVersions: keepLast must be an integer from 0 to ${DROP_VERSIONS_KEEP_LAST_MAX}`
      );
    }
    normalized.keepLast = input.keepLast;
  }
  if (input.exceptVersions !== undefined) {
    if (!Array.isArray(input.exceptVersions)) {
      throw new VFSError(
        "EINVAL",
        "dropVersions: exceptVersions must be an array"
      );
    }
    if (input.exceptVersions.length > DROP_VERSIONS_EXCEPT_MAX) {
      throw new VFSError(
        "EINVAL",
        `dropVersions: exceptVersions exceeds ${DROP_VERSIONS_EXCEPT_MAX} entries`
      );
    }
    normalized.exceptVersions = input.exceptVersions.map((versionId) =>
      validateRetentionId(versionId, "exceptVersions id")
    );
  }
  return normalized;
}

/** `keepLast` slots left once the head has taken the first of them. */
function remainingKeepFor(
  policy: DropVersionsPolicy,
  headVersionId: string | null
): number {
  return Math.max(0, (policy.keepLast ?? 0) - (headVersionId === null ? 0 : 1));
}

/**
 * Apply the policy to one version. Each keep rule is additive, and only the
 * "newest N" rule consumes a budget — an explicitly kept version and a version
 * kept for its age both survive without spending a `keepLast` slot.
 */
function retentionKeepDecision(
  policy: DropVersionsPolicy,
  headVersionId: string | null,
  exceptVersions: ReadonlySet<string>,
  candidate: RetentionCandidate,
  remainingKeep: number
): RetentionKeepDecision {
  const explicitlyKept =
    candidate.version_id === headVersionId ||
    exceptVersions.has(candidate.version_id);
  const consumesKeepSlot = !explicitlyKept && remainingKeep > 0;
  const keepForAge =
    policy.olderThan !== undefined && candidate.mtime_ms >= policy.olderThan;
  return {
    keep: explicitlyKept || consumesKeepSlot || keepForAge,
    consumesKeepSlot,
  };
}

// ── operation rows ───────────────────────────────────────────────────

function readRetentionOperation(
  durableObject: UserDO,
  operationId: string
): RetentionOperationRow | undefined {
  return durableObject.sql
    .exec<RetentionOperationRow>(
      `SELECT operation_id, user_id, path_id, policy_json, status,
              plan_generation, plan_head_version_id, cursor_mtime_ms,
              cursor_version_id, remaining_keep, dropped, kept,
              pending_version_id, pending_mtime_ms, pending_ref_id
         FROM version_retention_operations
        WHERE operation_id = ?`,
      operationId
    )
    .toArray()[0];
}

/**
 * Refuse a tenant more operations in flight than it can plausibly be driving.
 * A threshold probe rather than a count: the answer only depends on whether an
 * n-th row exists.
 */
function assertRetentionCapacity(durableObject: UserDO): void {
  const overflow = durableObject.sql
    .exec(
      `SELECT 1 AS one FROM version_retention_operations
        WHERE status = 'running' LIMIT 1 OFFSET ?`,
      RUNNING_RETENTION_MAX
    )
    .toArray();
  if (overflow.length > 0) {
    throw new VFSError(
      "EBUSY",
      "dropVersions: too many retention operations in flight"
    );
  }
}

/**
 * Bounded maintenance of the operation table: forget operations nobody resumed
 * inside the resume window, forget completed ones past their replay window,
 * and evict the oldest completed ones once the retained set exceeds its cap.
 *
 * An operation holding a pending version is excluded from the first predicate:
 * its manifest rows are still owed, and the row is the only record of that, so
 * the maintenance alarm finishes it instead.
 */
function pruneRetentionOperations(durableObject: UserDO, now: number): void {
  const oldest = {
    table: VERSION_RETENTION_OPERATION.table,
    keyColumn: "operation_id",
    timestamp: "updated_at",
    limit: RETENTION_PRUNE_LIMIT,
  } as const;
  pruneOldestRows(durableObject, {
    ...oldest,
    terminal: "status = 'running' AND pending_version_id IS NULL",
    olderThan: now - RUNNING_RETENTION_TTL_MS,
  });
  pruneOldestRows(durableObject, {
    ...oldest,
    terminal: "status = 'done'",
    olderThan: now - COMPLETED_RETENTION_TTL_MS,
  });
  const overflow = durableObject.sql
    .exec(
      `SELECT 1 AS one FROM version_retention_operations
        WHERE status = 'done' ORDER BY updated_at DESC LIMIT 1 OFFSET ?`,
      COMPLETED_RETENTION_MAX
    )
    .toArray();
  if (overflow.length === 0) return;
  pruneOldestRows(durableObject, { ...oldest, terminal: "status = 'done'" });
}

/** The path a retention plan is built against; its head is the plan fence. */
interface RetentionPlanFile extends Record<string, SqlStorageValue> {
  head_version_id: string | null;
}

/**
 * The plan's view of the path.
 *
 * An operation can outlive the path it was started for — the last version of a
 * path takes its `files` row with it — and the work it still owes is real:
 * whatever version rows and manifest rows remain are owed either way. A missing
 * row is therefore read as "no head", which keeps nothing and lets the
 * operation finish reaping instead of failing forever.
 */
function readRetentionFile(
  durableObject: UserDO,
  pathId: string,
  userId: string
): RetentionPlanFile {
  return (
    durableObject.sql
      .exec<RetentionPlanFile>(
        "SELECT head_version_id FROM files WHERE file_id = ? AND user_id = ?",
        pathId,
        userId
      )
      .toArray()[0] ?? { head_version_id: null }
  );
}

/** Record a fresh operation and read back the row every step then works on. */
function startRetentionOperation(
  durableObject: UserDO,
  operationId: string,
  userId: string,
  pathId: string,
  policyJson: string,
  headVersionId: string | null,
  remainingKeep: number,
  now: number
): RetentionOperationRow {
  durableObject.sql.exec(
    `INSERT INTO version_retention_operations
       (operation_id, user_id, path_id, policy_json, status, plan_generation,
        plan_head_version_id, cursor_mtime_ms, cursor_version_id,
        remaining_keep, dropped, kept, pending_version_id, pending_mtime_ms,
        pending_ref_id, created_at, updated_at)
     VALUES (?, ?, ?, ?, 'running', 0, ?, NULL, NULL, ?, 0, 0, NULL, NULL,
             NULL, ?, ?)`,
    operationId,
    userId,
    pathId,
    policyJson,
    headVersionId,
    remainingKeep,
    now,
    now
  );
  const operation = readRetentionOperation(durableObject, operationId);
  if (operation === undefined) {
    throw new VFSError(
      "EBUSY",
      `dropVersions: operation ${operationId} was not persisted`
    );
  }
  return operation;
}

/**
 * Persist one transition under the driver's compare-and-set. `expected` is the
 * progress the step read before it mutated anything, so a row that moved
 * underneath — and a row whose operation already finished — changes no rows.
 */
function commitRetentionAdvance(
  durableObject: UserDO,
  operation: RetentionOperationRow,
  expected: Readonly<Record<string, SqlStorageValue>>,
  now: number
): void {
  const committed = commitOperationTransition(
    durableObject,
    VERSION_RETENTION_OPERATION,
    { operation_id: operation.operation_id },
    expected,
    {
      status: operation.status,
      plan_generation: operation.plan_generation,
      plan_head_version_id: operation.plan_head_version_id,
      cursor_mtime_ms: operation.cursor_mtime_ms,
      cursor_version_id: operation.cursor_version_id,
      remaining_keep: operation.remaining_keep,
      dropped: operation.dropped,
      kept: operation.kept,
      pending_version_id: operation.pending_version_id,
      pending_mtime_ms: operation.pending_mtime_ms,
      pending_ref_id: operation.pending_ref_id,
      updated_at: now,
    }
  );
  if (!committed) {
    throw new VFSError(
      "EBUSY",
      "dropVersions: retention operation changed during the step"
    );
  }
}

// ── the scan ─────────────────────────────────────────────────────────

/**
 * The next unvisited version, newest first.
 *
 * The row-value range is the whole resume mechanism: it is a seek to the
 * cursor's position in `idx_file_versions_retention_seek` followed by one row,
 * whatever the depth already visited.
 */
function nextRetentionCandidate(
  durableObject: UserDO,
  operation: RetentionOperationRow
): RetentionCandidate | undefined {
  const cursorVersionId = operation.cursor_version_id;
  if (cursorVersionId === null) {
    return durableObject.sql
      .exec<RetentionCandidate>(
        `SELECT version_id, mtime_ms FROM file_versions
          WHERE path_id = ?
          ORDER BY mtime_ms DESC, version_id DESC LIMIT 1`,
        operation.path_id
      )
      .toArray()[0];
  }
  const cursorMtimeMs = operation.cursor_mtime_ms;
  if (cursorMtimeMs === null) {
    throw new VFSError("EBUSY", "dropVersions: retention cursor is incomplete");
  }
  return durableObject.sql
    .exec<RetentionCandidate>(
      `SELECT version_id, mtime_ms FROM file_versions
        WHERE path_id = ? AND (mtime_ms, version_id) < (?, ?)
        ORDER BY mtime_ms DESC, version_id DESC LIMIT 1`,
      operation.path_id,
      cursorMtimeMs,
      cursorVersionId
    )
    .toArray()[0];
}

/** Budgets one invocation spends, and the refs its staging left to drain. */
interface RetentionStepBudget {
  scans: number;
  manifestRows: number;
  cleanupRoutes: number;
  /** `${refId}\u0000${shardIndex}` routes already staged by this invocation. */
  readonly stagedRoutes: Set<string>;
  readonly stagedRefIds: Set<string>;
}

/**
 * Delete one version's metadata row and account for the bytes it held,
 * returning the shard ref its chunks are owed under. A version that is already
 * gone returns nothing — something else reaped it.
 *
 * The head is never a candidate, so a row that is present and still refuses to
 * delete means the history moved in a way this step cannot reconcile, which the
 * caller retries against a re-read plan rather than papering over.
 */
function dropRetentionVersionMetadata(
  durableObject: UserDO,
  operation: RetentionOperationRow,
  candidate: RetentionCandidate
): string | undefined {
  const version = durableObject.sql
    .exec<
      {
        size: number;
        deleted: number;
        inline_bytes: number | null;
        shard_ref_id: string | null;
      } & Record<string, SqlStorageValue>
    >(
      `SELECT size, deleted, LENGTH(inline_data) AS inline_bytes, shard_ref_id
         FROM file_versions
        WHERE path_id = ? AND version_id = ? AND user_id = ?`,
      operation.path_id,
      candidate.version_id,
      operation.user_id
    )
    .toArray()[0];
  if (version === undefined) return undefined;

  durableObject.sql.exec(
    `DELETE FROM file_versions
      WHERE path_id = ? AND version_id = ? AND user_id = ?`,
    operation.path_id,
    candidate.version_id,
    operation.user_id
  );
  if (lastSqlChanges(durableObject) !== 1) {
    throw new VFSError(
      "EBUSY",
      `dropVersions: version ${candidate.version_id} changed during retention`
    );
  }
  if (version.deleted === 0) {
    recordWriteUsage(
      durableObject,
      operation.user_id,
      -version.size,
      0,
      -(version.inline_bytes ?? 0)
    );
  }
  return (
    version.shard_ref_id ??
    shardRefId(operation.path_id, candidate.version_id)
  );
}

/**
 * Whether a manifest page finished the version it belonged to. `exhausted`
 * always means a budget ran out with rows still owed, so suspending on it can
 * never stall: the next invocation starts with full budgets.
 */
type RetentionManifestPage = "reaped" | "exhausted";

/**
 * Reap one page of the pending version's manifest, staging the cleanup each
 * shard is owed before the rows that name it disappear.
 *
 * The page is truncated at the first row whose shard would exceed the route
 * budget, and only that prefix is deleted, so the routes stay bounded while
 * every invocation still makes progress. Callers must not call this without
 * manifest budget left — a zero-row page would then be indistinguishable from
 * a manifest that is genuinely gone.
 */
function reapRetentionManifestPage(
  durableObject: UserDO,
  refId: string,
  versionId: string,
  budget: RetentionStepBudget,
  now: number
): RetentionManifestPage {
  const limit = budget.manifestRows;
  const page = durableObject.sql
    .exec<RetentionManifestRow>(
      `SELECT chunk_index, shard_index FROM version_chunks
        WHERE version_id = ? ORDER BY chunk_index LIMIT ?`,
      versionId,
      limit
    )
    .toArray();
  if (page.length === 0) return "reaped";

  let taken = 0;
  let lastIndex = 0;
  for (const row of page) {
    const route = `${refId}\u0000${row.shard_index}`;
    if (!budget.stagedRoutes.has(route)) {
      if (budget.cleanupRoutes === 0) break;
      stageChunkCleanupIntent(durableObject, refId, row.shard_index, now);
      budget.stagedRoutes.add(route);
      budget.stagedRefIds.add(refId);
      budget.cleanupRoutes--;
    }
    lastIndex = row.chunk_index;
    taken++;
  }
  if (taken === 0) return "exhausted";

  durableObject.sql.exec(
    "DELETE FROM version_chunks WHERE version_id = ? AND chunk_index <= ?",
    versionId,
    lastIndex
  );
  budget.manifestRows -= taken;
  return taken === page.length && page.length < limit ? "reaped" : "exhausted";
}

// ── one bounded step ─────────────────────────────────────────────────

interface RetentionStepOutcome {
  readonly step: DropVersionsStepResult;
  readonly refIds: readonly string[];
}

/**
 * Advance one retention operation by one bounded, durable step.
 *
 * `operationId` is the caller's: repeating a call with the same id resumes the
 * same operation, and repeating it after the operation finished returns the
 * counts it recorded rather than starting a second pass over what is left.
 * Reusing an id with a different path or policy is refused, because the answer
 * would describe neither.
 */
export async function dropVersionsStep(
  durableObject: UserDO,
  scope: VFSScope,
  userId: string,
  pathId: string,
  untrustedPolicy: unknown,
  untrustedOperationId: unknown
): Promise<DropVersionsStepResult> {
  const operationId = validateRetentionId(untrustedOperationId, "operation id");
  const policy = validateDropVersionsPolicy(untrustedPolicy);
  const policyJson = JSON.stringify(policy);

  // The maintenance alarm is what finishes an operation whose caller walks
  // away, so it is armed before anything is mutated.
  await scheduleStaleUploadSweep(durableObject);
  pruneRetentionOperations(durableObject, Date.now());

  const outcome = transactionSync(durableObject, () =>
    runRetentionStep(durableObject, userId, pathId, policy, policyJson, operationId)
  );
  // A drain addresses its refs by name in one statement, and Durable Object
  // SQL binds at most 100 parameters, so a step that dropped a version per
  // scan slot hands them over in batches. Every batch is durable either way:
  // what a drain does not reach stays in the outbox for the alarm.
  for (
    let cursor = 0;
    cursor < outcome.refIds.length;
    cursor += RETENTION_DRAIN_REF_BATCH
  ) {
    await drainChunkCleanupIntents(
      durableObject,
      scope,
      outcome.refIds.slice(cursor, cursor + RETENTION_DRAIN_REF_BATCH)
    );
  }
  return outcome.step;
}

function runRetentionStep(
  durableObject: UserDO,
  userId: string,
  pathId: string,
  policy: DropVersionsPolicy,
  policyJson: string,
  operationId: string
): RetentionStepOutcome {
  const now = Date.now();
  const file = readRetentionFile(durableObject, pathId, userId);
  const existing = readRetentionOperation(durableObject, operationId);
  if (
    existing !== undefined &&
    (existing.user_id !== userId ||
      existing.path_id !== pathId ||
      existing.policy_json !== policyJson)
  ) {
    throw new VFSError(
      "EINVAL",
      `dropVersions: operation ${operationId} was started with different parameters`
    );
  }
  if (existing?.status === "done") {
    return {
      step: { done: true, dropped: existing.dropped, kept: existing.kept },
      refIds: [],
    };
  }
  if (existing === undefined) assertRetentionCapacity(durableObject);
  const operation =
    existing ??
    startRetentionOperation(
      durableObject,
      operationId,
      userId,
      pathId,
      policyJson,
      file.head_version_id,
      remainingKeepFor(policy, file.head_version_id),
      now
    );
  const expected = heldProgress(VERSION_RETENTION_OPERATION, operation);

  const budget: RetentionStepBudget = {
    scans: DROP_VERSIONS_SCAN_LIMIT,
    manifestRows: DROP_VERSIONS_MANIFEST_LIMIT,
    cleanupRoutes: DROP_VERSIONS_CLEANUP_ROUTE_LIMIT,
    stagedRoutes: new Set<string>(),
    stagedRefIds: new Set<string>(),
  };
  const exceptVersions = new Set(policy.exceptVersions ?? []);
  const suspend = (): RetentionStepOutcome => {
    commitRetentionAdvance(durableObject, operation, expected, now);
    return { step: { done: false }, refIds: [...budget.stagedRefIds] };
  };

  for (;;) {
    const pendingVersionId = operation.pending_version_id;
    if (pendingVersionId !== null) {
      const pendingMtimeMs = operation.pending_mtime_ms;
      const pendingRefId = operation.pending_ref_id;
      if (pendingMtimeMs === null || pendingRefId === null) {
        throw new VFSError(
          "EBUSY",
          `dropVersions: version ${pendingVersionId} has an incomplete reap record`
        );
      }
      if (budget.manifestRows === 0) return suspend();
      const page = reapRetentionManifestPage(
        durableObject,
        pendingRefId,
        pendingVersionId,
        budget,
        now
      );
      if (page === "exhausted") return suspend();
      // The manifest is gone, so the cursor may finally step past the version
      // it belonged to.
      operation.cursor_mtime_ms = pendingMtimeMs;
      operation.cursor_version_id = pendingVersionId;
      operation.pending_version_id = null;
      operation.pending_mtime_ms = null;
      operation.pending_ref_id = null;
    }

    if (operation.plan_head_version_id !== file.head_version_id) {
      operation.plan_generation++;
      operation.plan_head_version_id = file.head_version_id;
      operation.cursor_mtime_ms = null;
      operation.cursor_version_id = null;
      operation.remaining_keep = remainingKeepFor(policy, file.head_version_id);
      operation.kept = 0;
    }

    if (budget.scans === 0) return suspend();
    const candidate = nextRetentionCandidate(durableObject, operation);
    if (candidate === undefined) {
      operation.status = "done";
      if (operation.dropped + operation.kept > 0) {
        insertAuditLog(durableObject, {
          op: "dropVersions",
          actor: userId,
          target: pathId,
          payload: JSON.stringify({
            dropped: operation.dropped,
            kept: operation.kept,
            policy,
          }),
        });
      }
      commitRetentionAdvance(durableObject, operation, expected, now);
      return {
        step: {
          done: true,
          dropped: operation.dropped,
          kept: operation.kept,
        },
        refIds: [...budget.stagedRefIds],
      };
    }

    budget.scans--;
    const decision = retentionKeepDecision(
      policy,
      file.head_version_id,
      exceptVersions,
      candidate,
      operation.remaining_keep
    );
    if (decision.keep) {
      if (decision.consumesKeepSlot) operation.remaining_keep--;
      operation.kept++;
      operation.cursor_mtime_ms = candidate.mtime_ms;
      operation.cursor_version_id = candidate.version_id;
      continue;
    }

    const refId = dropRetentionVersionMetadata(
      durableObject,
      operation,
      candidate
    );
    if (refId === undefined) {
      // Something else reaped the row between the seek and here; the cursor
      // still has to step past it or the scan would stall on it forever.
      operation.cursor_mtime_ms = candidate.mtime_ms;
      operation.cursor_version_id = candidate.version_id;
      continue;
    }
    operation.dropped++;
    operation.pending_version_id = candidate.version_id;
    operation.pending_mtime_ms = candidate.mtime_ms;
    operation.pending_ref_id = refId;
  }
}

// ── the one-call legacy contract ─────────────────────────────────────

/**
 * Refuse a one-call retention that a single bounded invocation could not
 * finish, before it mutates anything.
 *
 * Both checks are threshold probes: whether an n-th row exists, never how many
 * rows there are. A history deeper than one scan budget, or a drop set whose
 * manifests exceed one manifest budget, needs the bounded step surface — and
 * saying so before the first delete is what keeps the legacy contract's
 * "either the counts or an error" shape honest.
 */
export function assertLegacyDropVersionsBounded(
  durableObject: UserDO,
  userId: string,
  pathId: string,
  untrustedPolicy: unknown
): void {
  const policy = validateDropVersionsPolicy(untrustedPolicy);
  const file = readRetentionFile(durableObject, pathId, userId);
  const deeper = durableObject.sql
    .exec(
      `SELECT 1 AS one FROM file_versions WHERE path_id = ?
        ORDER BY mtime_ms DESC, version_id DESC LIMIT 1 OFFSET ?`,
      pathId,
      DROP_VERSIONS_LEGACY_VERSION_MAX
    )
    .toArray();
  if (deeper.length > 0) {
    throw new VFSError(
      "EFBIG",
      "dropVersions: history exceeds the one-call retention capability; use the bounded step surface"
    );
  }

  const candidates = durableObject.sql
    .exec<RetentionCandidate>(
      `SELECT version_id, mtime_ms FROM file_versions WHERE path_id = ?
        ORDER BY mtime_ms DESC, version_id DESC LIMIT ?`,
      pathId,
      DROP_VERSIONS_LEGACY_VERSION_MAX
    )
    .toArray();
  const exceptVersions = new Set(policy.exceptVersions ?? []);
  let remainingKeep = remainingKeepFor(policy, file.head_version_id);
  const dropVersionIds: string[] = [];
  for (const candidate of candidates) {
    const decision = retentionKeepDecision(
      policy,
      file.head_version_id,
      exceptVersions,
      candidate,
      remainingKeep
    );
    if (!decision.keep) {
      dropVersionIds.push(candidate.version_id);
      continue;
    }
    if (decision.consumesKeepSlot) remainingKeep--;
  }
  if (dropVersionIds.length === 0) return;

  // The drop set travels as one JSON parameter rather than one placeholder
  // each: Durable Object SQL binds at most 100 parameters per statement, and a
  // drop set may hold more ids than that. `version_id IN (json_each …)` still
  // seeks the manifest's primary key once per id.
  const overflow = durableObject.sql
    .exec(
      `SELECT 1 AS one FROM version_chunks
        WHERE version_id IN (SELECT value FROM json_each(?))
        LIMIT 1 OFFSET ?`,
      JSON.stringify(dropVersionIds),
      DROP_VERSIONS_LEGACY_MANIFEST_MAX
    )
    .toArray();
  if (overflow.length > 0) {
    throw new VFSError(
      "EFBIG",
      "dropVersions: manifests exceed the one-call retention capability; use the bounded step surface"
    );
  }
}

// ── resumption ───────────────────────────────────────────────────────

/**
 * Advance the longest-waiting running operation by one step.
 *
 * Nobody is obliged to come back for an operation they abandoned, and until it
 * finishes its already-dropped versions may still hold chunk refs on their
 * shards — so the maintenance alarm is what guarantees it completes. One step
 * per tick keeps the alarm itself bounded; `remaining` is what asks it to come
 * back.
 */
export async function resumeVersionRetention(
  durableObject: UserDO,
  scope: VFSScope
): Promise<{ remaining: boolean }> {
  const userId = userIdFor(scope);
  const operation = durableObject.sql
    .exec<
      { operation_id: string; path_id: string; policy_json: string } & Record<
        string,
        SqlStorageValue
      >
    >(
      `SELECT operation_id, path_id, policy_json
         FROM version_retention_operations
        WHERE status = 'running'
        ORDER BY updated_at LIMIT 1`
    )
    .toArray()[0];
  if (operation !== undefined) {
    const policy: unknown = JSON.parse(operation.policy_json);
    await dropVersionsStep(
      durableObject,
      scope,
      userId,
      operation.path_id,
      policy,
      operation.operation_id
    );
  }
  const remaining = durableObject.sql
    .exec(
      `SELECT 1 AS one FROM version_retention_operations
        WHERE status = 'running' LIMIT 1`
    )
    .toArray();
  return { remaining: remaining.length > 0 };
}
