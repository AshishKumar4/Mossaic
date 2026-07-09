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

export async function scheduleAlarmAt(
  durableObject: StorageCapability,
  target: number
): Promise<void> {
  const current = await durableObject.storage.getAlarm();
  if (current === null || current > target) {
    await durableObject.storage.setAlarm(target);
  }
}

export function scheduleStaleUploadSweep(
  durableObject: StorageCapability
): Promise<void> {
  return scheduleAlarmAt(durableObject, Date.now() + 10 * 60 * 1000);
}

export function scheduleChunkCleanupSweep(
  durableObject: StorageCapability,
  nextAttemptAt: number
): Promise<void> {
  return scheduleAlarmAt(
    durableObject,
    Math.max(Date.now() + 1_000, nextAttemptAt)
  );
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
       (ref_id, shard_index, cleanup_kind, state, generation, provisional,
        created_at, updated_at, next_attempt_at, attempts, last_error)
     VALUES (?, ?, ?, 'pending', 0, ?, ?, ?, ?, 0, NULL)
     ON CONFLICT(ref_id, shard_index) DO UPDATE SET
       cleanup_kind = CASE
         WHEN chunk_cleanup_intents.cleanup_kind = 'multipart'
           OR excluded.cleanup_kind = 'multipart' THEN 'multipart'
         WHEN chunk_cleanup_intents.cleanup_kind = 'bulk'
           OR excluded.cleanup_kind = 'bulk' THEN 'bulk'
         ELSE 'chunks'
       END,
       state = 'pending',
       generation = chunk_cleanup_intents.generation + 1,
       provisional = excluded.provisional,
       updated_at = excluded.updated_at,
       next_attempt_at = MIN(chunk_cleanup_intents.next_attempt_at, excluded.next_attempt_at)`,
    refId,
    shardIndex,
    cleanupKind,
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

/** Convert pre-publication multipart rollback intents into staging-only work. */
export function retainMultipartStagingCleanup(
  durableObject: StorageCapability,
  uploadId: string,
  now: number
): void {
  const guards = durableObject.sql
    .exec(
      `SELECT state, provisional FROM chunk_cleanup_intents
        WHERE ref_id = ?`,
      uploadId
    )
    .toArray() as { state: string; provisional: number }[];
  if (
    guards.length === 0 ||
    guards.some((guard) => guard.state !== "pending" || guard.provisional === 0)
  ) {
    throw new Error("multipart cleanup guard unavailable for publication");
  }
  durableObject.sql.exec(
    `UPDATE chunk_cleanup_intents
        SET cleanup_kind = ?, state = 'pending', generation = generation + 1,
            provisional = 0,
            updated_at = ?, next_attempt_at = ?,
            attempts = 0, last_error = NULL
      WHERE ref_id = ?`,
    ChunkCleanupKind.MultipartStaging,
    now,
    now,
    uploadId
  );
}
