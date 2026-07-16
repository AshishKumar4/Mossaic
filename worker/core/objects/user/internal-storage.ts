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
