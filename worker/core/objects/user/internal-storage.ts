import { MULTIPART_TERMINAL_RETENTION_MS } from "../../../../shared/multipart";

interface StorageCapability {
  state: DurableObjectState;
  storage: DurableObjectStorage;
  sql: SqlStorage;
}

export const ChunkCleanupKind = Object.freeze({
  Chunks: "chunks",
  Multipart: "multipart",
  MultipartStaging: "multipart_staging",
  Bulk: "bulk",
} as const);
export type ChunkCleanupKind =
  (typeof ChunkCleanupKind)[keyof typeof ChunkCleanupKind];

type SynchronousResult<T> = T extends PromiseLike<unknown> ? never : T;

function isPromiseLike(value: unknown): value is PromiseLike<unknown> {
  return (
    value !== null &&
    (typeof value === "object" || typeof value === "function") &&
    "then" in value &&
    typeof value.then === "function"
  );
}

export function transactionSync<T>(
  durableObject: StorageCapability,
  closure: () => SynchronousResult<T>
): T;
export function transactionSync<T>(
  durableObject: StorageCapability,
  closure: () => T
): T {
  return durableObject.storage.transactionSync(() => {
    const result = closure();
    if (isPromiseLike(result)) {
      throw new TypeError("transactionSync callback must be synchronous");
    }
    return result;
  });
}

export function runWithConcurrencyBlocked<T>(
  durableObject: StorageCapability,
  closure: () => Promise<T>
): Promise<T> {
  return durableObject.state.blockConcurrencyWhile(closure);
}

/**
 * Why an object's alarm is set, as this isolate last set it. `"pending"`: only
 * state its SQL shows needs it (an upload in progress, a chunk-cleanup intent,
 * a session's expiry; {@link nextPendingMaintenanceAt}), so a write that leaves
 * nothing of that pending may take it back ({@link settlePendingSweep}).
 * `"other"`: anything else does too (a maintenance page still to run, a
 * retention step), and it is kept. Unknown once the isolate is evicted, and
 * then kept.
 */
export type AlarmReason = "pending" | "other";

const alarmReasons = new WeakMap<StorageCapability, AlarmReason>();

export async function scheduleAlarmAt(
  durableObject: StorageCapability,
  target: number,
  reason: AlarmReason = "other"
): Promise<void> {
  const current = await durableObject.storage.getAlarm();
  // An alarm already set serves this need too, so it is kept for the strongest
  // reason it has; one set before this isolate saw why counts as "other".
  const held =
    current === null ? undefined : alarmReasons.get(durableObject) ?? "other";
  alarmReasons.set(
    durableObject,
    reason === "other" || held === "other" ? "other" : "pending"
  );
  if (current === null || current > target) {
    await durableObject.storage.setAlarm(target);
  }
}

/** The platform cleared the alarm to run it; what the handler sets is new. */
export function alarmRunning(durableObject: StorageCapability): void {
  alarmReasons.delete(durableObject);
}

export function scheduleStaleUploadSweep(
  durableObject: StorageCapability
): Promise<void> {
  return scheduleAlarmAt(durableObject, Date.now() + 10 * 60 * 1000, "pending");
}

/**
 * Runs `insert`, which leaves rows the stale-upload sweep reclaims, with the
 * sweep armed before it (a crash mid-method) and again after it: a write that
 * finished in between saw nothing pending and may have taken the first back.
 */
export async function withStaleUploadSweep<T>(
  durableObject: StorageCapability,
  insert: () => T
): Promise<T> {
  await scheduleStaleUploadSweep(durableObject);
  const inserted = insert();
  await scheduleStaleUploadSweep(durableObject);
  return inserted;
}

export function scheduleChunkCleanupSweep(
  durableObject: StorageCapability,
  nextAttemptAt: number
): Promise<void> {
  return scheduleAlarmAt(
    durableObject,
    Math.max(Date.now() + 1_000, nextAttemptAt),
    "pending"
  );
}

/**
 * When the earliest pending state an object's SQL shows needs its alarm: a
 * chunk-cleanup intent's next attempt, an upload or write-stream session's
 * expiry, a terminal session's retention, a tmp upload row's staleness. Null
 * when nothing is pending.
 */
export function nextPendingMaintenanceAt(
  durableObject: StorageCapability,
  now: number = Date.now()
): number | null {
  const nextCleanupAttempt = durableObject.sql
    .exec<{ next_attempt_at: number | null }>(
      `SELECT MIN(next_attempt_at) AS next_attempt_at
         FROM chunk_cleanup_intents
        WHERE state IN ('pending', 'in_flight')`
    )
    .toArray()[0]?.next_attempt_at;
  const nextUploadExpiry = durableObject.sql
    .exec<{ deadline: number | null }>(
      `SELECT MIN(deadline) AS deadline FROM (
         SELECT expires_at AS deadline FROM write_stream_sessions
         UNION ALL
          SELECT CASE
                   WHEN abort_retry_at > ?
                     THEN abort_retry_at
                   ELSE expires_at
                 END AS deadline
            FROM upload_sessions WHERE status IN ('open', 'finalizing', 'aborting')
          UNION ALL
          SELECT COALESCE(terminal_at, created_at) + ? AS deadline
            FROM upload_sessions
           WHERE status IN ('finalized', 'aborted', 'poisoned')
          UNION ALL
         SELECT f.created_at + 3600000 AS deadline
           FROM files f
           LEFT JOIN write_stream_sessions ws ON ws.tmp_id = f.file_id
           LEFT JOIN upload_sessions us ON us.upload_id = f.file_id
          WHERE f.status = 'uploading'
            AND f.file_name LIKE '_vfs_tmp_%'
            AND ws.tmp_id IS NULL AND us.upload_id IS NULL
       )`,
      now,
      MULTIPART_TERMINAL_RETENTION_MS
    )
    .toArray()[0]?.deadline;
  const deadlines = [nextCleanupAttempt, nextUploadExpiry].filter(
    (at): at is number => at !== null && at !== undefined
  );
  return deadlines.length === 0 ? null : Math.min(...deadlines);
}

/**
 * Takes back an object's alarm once nothing it was set for is pending: called
 * as a write, a delete or a cleanup drain finishes. A completed write armed
 * the stale-upload sweep and left it nothing, so the alarm would wake the
 * object 10 minutes later to find nothing to do.
 */
export async function settlePendingSweep(
  durableObject: StorageCapability
): Promise<void> {
  if (alarmReasons.get(durableObject) !== "pending") return;
  if (nextPendingMaintenanceAt(durableObject) !== null) return;
  alarmReasons.delete(durableObject);
  await durableObject.storage.deleteAlarm();
}

export function stageChunkCleanupIntent(
  durableObject: StorageCapability,
  refId: string,
  shardIndex: number,
  now: number,
  nextAttemptAt = now,
  cleanupKind: ChunkCleanupKind = ChunkCleanupKind.Chunks,
  provisional = false
): void {
  const existing = durableObject.sql
    .exec(
      `SELECT state FROM chunk_cleanup_intents
        WHERE ref_id = ? AND shard_index = ?`,
      refId,
      shardIndex
    )
    .toArray()[0] as { state: string } | undefined;
  if (existing?.state === "in_flight") {
    throw new Error("cleanup intent is already in flight");
  }
  durableObject.sql.exec(
    `INSERT INTO chunk_cleanup_intents
       (ref_id, shard_index, cleanup_kind, state, generation,
        cleanup_generation, cleanup_cursor, cleanup_phase, provisional,
        created_at, updated_at, next_attempt_at, attempts, last_error)
     VALUES (?, ?, ?, 'pending', 0, ?, 0, ?, ?, ?, ?, ?, 0, NULL)
      ON CONFLICT(ref_id, shard_index) DO UPDATE SET
       cleanup_kind = CASE
          WHEN chunk_cleanup_intents.cleanup_kind = 'multipart'
            OR excluded.cleanup_kind = 'multipart' THEN 'multipart'
          WHEN (chunk_cleanup_intents.cleanup_kind = 'chunks'
                  AND excluded.cleanup_kind = 'multipart_staging')
            OR (chunk_cleanup_intents.cleanup_kind = 'multipart_staging'
                  AND excluded.cleanup_kind = 'chunks') THEN 'multipart'
          WHEN chunk_cleanup_intents.cleanup_kind = 'multipart_staging'
            OR excluded.cleanup_kind = 'multipart_staging'
            THEN 'multipart_staging'
          WHEN chunk_cleanup_intents.cleanup_kind = 'bulk'
            OR excluded.cleanup_kind = 'bulk' THEN 'bulk'
          ELSE 'chunks'
       END,
       state = 'pending',
       generation = chunk_cleanup_intents.generation + 1,
       cleanup_generation = CASE
         WHEN (chunk_cleanup_intents.cleanup_kind = 'chunks'
                 AND excluded.cleanup_kind = 'multipart_staging')
           OR (chunk_cleanup_intents.cleanup_kind = 'multipart_staging'
                 AND excluded.cleanup_kind = 'chunks')
           THEN excluded.cleanup_generation
         ELSE chunk_cleanup_intents.cleanup_generation
       END,
       cleanup_cursor = CASE
         WHEN (chunk_cleanup_intents.cleanup_kind = 'chunks'
                 AND excluded.cleanup_kind = 'multipart_staging')
           OR (chunk_cleanup_intents.cleanup_kind = 'multipart_staging'
                 AND excluded.cleanup_kind = 'chunks') THEN 0
         ELSE chunk_cleanup_intents.cleanup_cursor
       END,
       cleanup_phase = CASE
         WHEN (chunk_cleanup_intents.cleanup_kind = 'chunks'
                 AND excluded.cleanup_kind = 'multipart_staging')
           OR (chunk_cleanup_intents.cleanup_kind = 'multipart_staging'
                 AND excluded.cleanup_kind = 'chunks') THEN 'chunks'
         ELSE chunk_cleanup_intents.cleanup_phase
       END,
       provisional = excluded.provisional,
       updated_at = excluded.updated_at,
       next_attempt_at = MIN(chunk_cleanup_intents.next_attempt_at, excluded.next_attempt_at)`,
    refId,
    shardIndex,
    cleanupKind,
    crypto.randomUUID(),
    cleanupKind === ChunkCleanupKind.MultipartStaging ? "staging" : "chunks",
    provisional ? 1 : 0,
    now,
    now,
    nextAttemptAt
  );
}

export function lastSqlChanges(durableObject: StorageCapability): number {
  return (
    durableObject.sql.exec("SELECT changes() AS n").toArray()[0] as {
      n: number;
    }
  ).n;
}
