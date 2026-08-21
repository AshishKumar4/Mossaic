import type { UserDOCore as UserDO } from "../user-do-core";
import type { ShardDO } from "../../shard/shard-do";
import {
  VFSError,
  type VFSScope,
} from "../../../../../shared/vfs-types";
import {
  INLINE_LIMIT,
  INLINE_TIER_CAP,
  WRITEFILE_MAX,
} from "../../../../../shared/inline";
import { hashChunk } from "../../../../../shared/crypto";
import { computeChunkSpec } from "../../../../../shared/chunking";
import { generateId, vfsShardDOName } from "../../../lib/utils";
import { logWarn } from "../../../lib/logger";
import { placeChunk, POOL_FULL } from "../../../../../shared/placement";
import { loadFullShards } from "../shard-capacity";
import {
  commitVersion,
  insertVersionChunk,
  isVersioningEnabled,
  placeChunkForVersion,
  shardRefId,
} from "../vfs-versions";
import {
  validateLabel,
  validateMetadata,
  validateTags,
} from "../../../../../shared/metadata-validate";
import { bumpTagMtimes, replaceTags } from "../metadata-tags";
import {
  enforceModeMonotonic,
  stampFileEncryption,
  type EncryptionStampOpts,
} from "../encryption-stamp";
import {
  ChunkCleanupKind,
  lastSqlChanges,
  scheduleChunkCleanupSweep,
  scheduleStaleUploadSweep,
  stageChunkCleanupIntent,
  transactionSync,
} from "../internal-storage";
import {
  bumpFolderRevision,
  findLiveFile,
  folderExists,
  poolSizeFor,
  recordWriteUsage,
  resolveParent,
  userIdFor,
} from "./helpers";

/**
 * Top-level write + commit protocol.
 *
 * `vfsWriteFile` is the user-facing entry point for atomic file
 * writes; `commitRename`, `abortTempFile`, and `hardDeleteFileRow`
 * constitute the atomic-write protocol modeled by
 * `Mossaic.Vfs.AtomicWrite` in Lean. They are co-located here
 * because `vfsWriteFile` is the protocol's defining caller; other
 * callers (write-streams, mutations, multipart-upload, copy-file)
 * are derivative.
 *
 * All writes go through one of three shapes:
 *   1. Inline: file ≤ INLINE_LIMIT → temp row + commit transaction, no shards.
 *   2. Chunked: hash + place + putChunk RPC per chunk + recordChunk row +
 *      single commit-rename UPDATE.
 *   3. Folder/symlink/rename/chmod: pure SQL on the UserDO (in sibling
 *      modules).
 *
 * Atomicity is delivered by:
 *   - an explicit synchronous `transactionSync` around commit-time SQL;
 *     Cloudflare may interleave requests at `await` boundaries, so a whole
 *     RPC method is not treated as a transaction
 *   - UNIQUE partial index on (user_id, parent_id, file_name)
 *     WHERE status != 'deleted' ⇒ a genuine destination conflict surfaces
 *     as EBUSY
 *   - Temp-id-then-rename for writeFile ⇒ a partially-written tmp row
 *     never shadows the live file_name; readFile of the path returns the
 *     prior content until commit flips status='complete'
 *
 * GC: stage durable `(ref_id, shard_index)` intents in the same UserDO SQL
 * transaction that deletes files+file_chunks, then acknowledge each intent
 * only after its typed ShardDO deleteChunks RPC resolves. ShardDO's alarm
 * sweeper performs the actual blob delete after the 30s grace window.
 */

const CLEANUP_BATCH_LIMIT = 200;
const CLEANUP_CONCURRENCY = 6;
const CLEANUP_BACKOFF_BASE_MS = 60_000;
const CLEANUP_BACKOFF_MAX_MS = 6 * 60 * 60 * 1000;
const CLEANUP_CLAIM_LEASE_MS = 5 * 60 * 1000;
const SYNTHETIC_REF_CLEANUP_GRACE_MS = 60 * 60 * 1000;

interface CleanupIntentRow extends Record<string, SqlStorageValue> {
  ref_id: string;
  shard_index: number;
  cleanup_kind: ChunkCleanupKind;
  attempts: number;
  generation: number;
  cleanup_generation: string;
  cleanup_cursor: number;
  cleanup_phase: string;
  provisional: number;
}

class CommitConflictError extends Error {}

function isDestinationUniqueConstraintError(err: unknown): boolean {
  if (!(err instanceof Error)) return false;
  return (
    err.message.includes("uniq_files_parent_name") ||
    (err.message.includes("UNIQUE constraint failed") &&
      err.message.includes("files."))
  );
}

function errorMessage(err: unknown): string {
  return (err instanceof Error ? err.message : String(err)).slice(0, 2_000);
}

function cleanupBackoffMs(attempts: number): number {
  return Math.min(
    CLEANUP_BACKOFF_BASE_MS * 2 ** Math.min(attempts, 16),
    CLEANUP_BACKOFF_MAX_MS
  );
}

/**
 * Write-ahead cleanup for temporary and synthetic shard refs. The maintenance
 * alarm is durable before the intent, and the intent is durable before the
 * caller issues the ShardDO RPC. Alarm replay waits for the stale-upload
 * grace so it cannot race an in-flight publication; explicit ref-scoped
 * drains bypass that deadline on a known failure.
 */
export async function stageChunkCleanupIntents(
  durableObject: UserDO,
  refId: string,
  shardIndexes: Iterable<number>
): Promise<void> {
  const uniqueShardIndexes = [...new Set(shardIndexes)];
  const unstagedShardIndexes = uniqueShardIndexes.filter((shardIndex) => {
    const existing = durableObject.sql
      .exec(
        `SELECT 1 FROM chunk_cleanup_intents
          WHERE ref_id = ? AND shard_index = ?`,
        refId,
        shardIndex
      )
      .toArray();
    return existing.length === 0;
  });
  if (unstagedShardIndexes.length === 0) return;

  await scheduleStaleUploadSweep(durableObject);
  const now = Date.now();
  transactionSync(durableObject, () => {
    for (const shardIndex of unstagedShardIndexes) {
      stageChunkCleanupIntent(
        durableObject,
        refId,
        shardIndex,
        now,
        now + SYNTHETIC_REF_CLEANUP_GRACE_MS,
        ChunkCleanupKind.Chunks,
        true
      );
    }
  });
}

/** Delete only from a caller's successful local publication transaction. */
export function disarmChunkCleanupIntents(
  durableObject: UserDO,
  refId: string
): void {
  const guards = durableObject.sql
    .exec(
      `SELECT state, provisional FROM chunk_cleanup_intents
        WHERE ref_id = ?`,
      refId
    )
    .toArray() as { state: string; provisional: number }[];
  if (
    guards.length === 0 ||
    guards.some((guard) => guard.state !== "pending" || guard.provisional === 0)
  ) {
    throw new VFSError("EBUSY", "cleanup guard unavailable for publication");
  }
  durableObject.sql.exec(
    "DELETE FROM chunk_cleanup_intents WHERE ref_id = ? AND state = 'pending'",
    refId
  );
}

/**
 * Hard-delete a file row + its file_chunks and durably stage one cleanup
 * intent for each unique shard the file's primary chunks lived on. Local
 * deletion, accounting, and intent creation commit in one synchronous SQL
 * transaction. Shard RPCs run only after that commit; failures remain in
 * the intent table for alarm replay.
 *
 * Used by:
 *   - vfsUnlink (direct delete)
 *   - the supersede branch of commit-rename (overwrite)
 *   - vfsRename when the destination is occupied (replace semantics)
 *   - vfsRemoveRecursive for each touched file
 *
 * Subrequest cost: U fan-out RPCs to ShardDOs (one per unique shard).
 */
export async function hardDeleteFileRow(
  durableObject: UserDO,
  userId: string,
  scope: VFSScope,
  fileId: string,
  precondition?: {
    staleAt: number;
  }
): Promise<void> {
  await scheduleStaleUploadSweep(durableObject);
  let deleted = false;
  transactionSync(durableObject, () => {
    if (precondition) {
      const current = durableObject.sql
        .exec(
          `SELECT f.status, f.created_at, f.file_name,
                  ws.expires_at AS stream_expires_at,
                  us.expires_at AS multipart_expires_at,
                  us.status AS multipart_status
             FROM files f
             LEFT JOIN write_stream_sessions ws ON ws.tmp_id = f.file_id
             LEFT JOIN upload_sessions us ON us.upload_id = f.file_id
            WHERE f.file_id = ?`,
          fileId
        )
        .toArray()[0] as
        | {
            status: string;
            created_at: number;
            file_name: string;
            stream_expires_at: number | null;
            multipart_expires_at: number | null;
            multipart_status: string | null;
          }
        | undefined;
      const isExpired = current
        ? current.stream_expires_at !== null
          ? current.stream_expires_at <= precondition.staleAt
          : current.multipart_expires_at !== null
            ? current.multipart_status === "open" &&
              current.multipart_expires_at <= precondition.staleAt
            : current.created_at < precondition.staleAt - 60 * 60 * 1000
        : false;
      if (
        !current ||
        current.status !== "uploading" ||
        !current.file_name.startsWith("_vfs_tmp_") ||
        !isExpired
      ) {
        return;
      }
    }
    hardDeleteFileRowLocal(durableObject, userId, fileId);
    deleted = true;
  });
  if (!deleted) return;
  await drainChunkCleanupIntents(durableObject, scope, fileId);
}

export function hardDeleteFileRowLocal(
  durableObject: UserDO,
  userId: string,
  fileId: string
): void {
  // Full byte / file accounting on hard-delete.
  //
  // Read `(status, file_size, inline_data)` BEFORE the delete
  // cascade so we can issue ONE negative `recordWriteUsage` call
  // covering all three counters: storage_used, file_count, and
  // (for inline rows) inline_bytes_used.
  //
  // Without symmetric decrements, `storage_used` / `file_count`
  // would monotonically inflate for any tenant that ever deleted.
  // The `pool_size_monotonic` invariant is preserved — negative
  // storage_used deltas are clamped at 0 by `recordWriteUsage`
  // (helpers.ts:421) and the pool-recompute is gated on
  // `newPool > row.pool_size` so shrink is impossible. Cosmetic
  // counters now reflect reality.
  //
  // The `status='uploading'` branch (tmp-row reaper sweeps;
  // multipart-abort) was never accounted as a positive delta in
  // `commitInlineTier` / `commitChunkedTier` / multipart finalize,
  // so we do NOT decrement it. The `'complete'` and `'deleted'`
  // (post-supersede) statuses both decrement — see the BUG #1 gate
  // below.
  const accountingRow = durableObject.sql
    .exec(
      "SELECT status, file_size, inline_data FROM files WHERE file_id = ?",
      fileId
    )
    .toArray()[0] as
    | {
        status: string;
        file_size: number;
        inline_data: ArrayBuffer | null;
      }
    | undefined;

  // Group by shard before deleting the chunk routing rows.
  const shardRows = durableObject.sql
    .exec(
      "SELECT DISTINCT shard_index FROM file_chunks WHERE file_id = ?",
      fileId
    )
    .toArray() as { shard_index: number }[];

  const now = Date.now();
  for (const { shard_index } of shardRows) {
    stageChunkCleanupIntent(durableObject, fileId, shard_index, now);
  }

  // Intents must exist before the routing rows and file metadata disappear.
  // `transactionSync` rolls this whole local transition back on any throw.
  durableObject.sql.exec("DELETE FROM file_chunks WHERE file_id = ?", fileId);
  // drop any tags + version rows still referencing this
  // file_id. Tags are per-pathId; for non-versioning tenants, hard
  // delete also reaps the path identity, so the tags must go too.
  // For versioning-on tenants, hardDeleteFileRow is reachable only
  // when versioning is OFF (it's the non-versioned write supersede
  // path); the versioning path uses dropVersions for its own GC.
  durableObject.sql.exec("DELETE FROM file_tags WHERE path_id = ?", fileId);
  durableObject.sql.exec(
    "DELETE FROM write_stream_sessions WHERE tmp_id = ?",
    fileId
  );
  durableObject.sql.exec("DELETE FROM files WHERE file_id = ?", fileId);

  // Single decrement call covering storage_used, file_count, and
  // inline_bytes_used in one SQL UPDATE. recordWriteUsage clamps
  // all three at zero (helpers.ts:421-425) so a partial historical
  // deficit (tenants who accumulated rmrf/unlink inflation before
  // this gate shipped) doesn't underflow.
  //
  // Gate is `status !== 'uploading'`, NOT `status === 'complete'`.
  // The two callers that drive the overwrite/rename flow
  // (`commitRename` write-commit.ts:~1145, `vfsRename`
  // mutations.ts:~530) flip status to `'deleted'` BEFORE invoking
  // hardDeleteFileRow on the displaced row. Under a
  // `status === 'complete'` gate the inline-bytes decrement would
  // be skipped on every overwrite — `inline_bytes_used` would
  // monotonically inflate by `file_size` per overwrite cycle, so
  // INLINE_TIER_CAP would fire earlier than 1 GiB.
  //
  // What the gate must exclude is the `'uploading'` status — tmp
  // rows reaped by the stale-upload sweeper / multipart-abort were
  // never positive-counted by `commitInlineTier` /
  // `commitChunkedTier` / `vfsFinalizeMultipart` (each of those
  // calls `recordWriteUsage(..., +file_size, +1)` AFTER the
  // status flip to `'complete'`). The post-supersede `'deleted'`
  // and the post-commit `'complete'` statuses are both
  // legitimately positive-counted, so both decrement.
  //
  // Symmetry guarantee: every code path that flows positive bytes
  // into the quota counters writes `'complete'` first; the only
  // way a row disappears is through hardDeleteFileRow (this
  // function), `dropVersionRows` (vfs-versions.ts), or rmrf-batch
  // (mutations.ts non-versioning branch). All three now
  // decrement symmetrically.
  if (accountingRow && accountingRow.status !== "uploading") {
    const inlineDelta = accountingRow.inline_data
      ? -accountingRow.inline_data.byteLength
      : 0;
    recordWriteUsage(
      durableObject,
      userId,
      -accountingRow.file_size,
      -1,
      inlineDelta
    );
  }
}

/**
 * Drain a bounded cleanup-intent batch with at most six shard RPCs in flight.
 * Ordinary refs use deleteChunks, multipart refs also clear staging, and bulk
 * refs group into deleteManyChunks by shard. An intent is acknowledged only
 * after its complete protocol resolves; any throw retains every affected row.
 */
export async function drainChunkCleanupIntents(
  durableObject: UserDO,
  scope: VFSScope,
  refId?: string | readonly string[]
): Promise<void> {
  const eligibleAt = Date.now();
  transactionSync(durableObject, () => {
    durableObject.sql.exec(
      `UPDATE chunk_cleanup_intents
          SET state = 'pending', generation = generation + 1,
              updated_at = ?, next_attempt_at = ?
        WHERE rowid IN (
          SELECT rowid FROM chunk_cleanup_intents
           WHERE state = 'in_flight' AND next_attempt_at <= ?
           ORDER BY next_attempt_at, created_at, ref_id, shard_index
           LIMIT ?
        )`,
      eligibleAt,
      eligibleAt,
      eligibleAt,
      CLEANUP_BATCH_LIMIT
    );
  });
  if (refId !== undefined) {
    const abandonedRefIds = typeof refId === "string" ? [refId] : [...refId];
    if (abandonedRefIds.length > 0) {
      const placeholders = abandonedRefIds.map(() => "?").join(",");
      transactionSync(durableObject, () => {
        durableObject.sql.exec(
          `UPDATE chunk_cleanup_intents SET provisional = 0
            WHERE rowid IN (
              SELECT rowid FROM chunk_cleanup_intents
               WHERE state = 'pending' AND ref_id IN (${placeholders})
               ORDER BY created_at, ref_id, shard_index LIMIT ?
            )`,
          ...abandonedRefIds,
          CLEANUP_BATCH_LIMIT
        );
        durableObject.sql.exec(
          `DELETE FROM chunk_cleanup_intents
            WHERE rowid IN (
              SELECT rowid FROM chunk_cleanup_intents
               WHERE state = 'cleaned' AND ref_id IN (${placeholders})
               ORDER BY created_at, ref_id, shard_index LIMIT ?
            )`,
          ...abandonedRefIds,
          CLEANUP_BATCH_LIMIT
        );
      });
    }
  }
  let rows: CleanupIntentRow[];
  if (refId === undefined) {
    rows = durableObject.sql
      .exec<CleanupIntentRow>(
        `SELECT ref_id, shard_index, cleanup_kind, attempts, generation,
                cleanup_generation, cleanup_cursor, cleanup_phase, provisional
           FROM chunk_cleanup_intents
          WHERE state = 'pending' AND next_attempt_at <= ?
           ORDER BY next_attempt_at, created_at, ref_id, shard_index
           LIMIT ?`,
        eligibleAt,
        CLEANUP_BATCH_LIMIT
      )
      .toArray();
  } else if (typeof refId === "string") {
    rows = durableObject.sql
      .exec<CleanupIntentRow>(
        `SELECT ref_id, shard_index, cleanup_kind, attempts, generation,
                cleanup_generation, cleanup_cursor, cleanup_phase, provisional
           FROM chunk_cleanup_intents
          WHERE ref_id = ? AND state = 'pending'
          ORDER BY created_at, shard_index
          LIMIT ?`,
        refId,
        CLEANUP_BATCH_LIMIT
      )
      .toArray();
  } else if (refId.length === 0) {
    rows = [];
  } else {
    const placeholders = refId.map(() => "?").join(",");
    rows = durableObject.sql
      .exec<CleanupIntentRow>(
        `SELECT ref_id, shard_index, cleanup_kind, attempts, generation,
                cleanup_generation, cleanup_cursor, cleanup_phase, provisional
           FROM chunk_cleanup_intents
          WHERE ref_id IN (${placeholders}) AND state = 'pending'
          ORDER BY created_at, ref_id, shard_index
          LIMIT ?`,
        ...refId,
        CLEANUP_BATCH_LIMIT
      )
      .toArray();
  }

  const env = durableObject.envPublic;
  const shardNs = env.MOSSAIC_SHARD as unknown as DurableObjectNamespace<ShardDO>;

  let cursor = 0;
  let retrySchedulingNeeded = rows.length === CLEANUP_BATCH_LIMIT;
  async function drainOne(row: CleanupIntentRow): Promise<void> {
    const shardName = vfsShardDOName(
      scope.ns,
      scope.tenant,
      scope.sub,
      row.shard_index
    );
    const stub = shardNs.get(shardNs.idFromName(shardName));
    for (let phaseCalls = 0; phaseCalls < 2; phaseCalls++) {
      const claimed = transactionSync(durableObject, () => {
        const current = durableObject.sql
          .exec(
            `SELECT state, generation FROM chunk_cleanup_intents
              WHERE ref_id = ? AND shard_index = ?`,
            row.ref_id,
            row.shard_index
          )
          .toArray()[0] as
          | { state: string; generation: number }
          | undefined;
        if (
          !current ||
          current.state !== "pending" ||
          current.generation !== row.generation
        ) {
          return false;
        }
        if (row.cleanup_generation === "") {
          row.cleanup_generation = crypto.randomUUID();
          durableObject.sql.exec(
            `UPDATE chunk_cleanup_intents SET cleanup_generation = ?
              WHERE ref_id = ? AND shard_index = ?
                AND state = 'pending' AND generation = ?
                AND cleanup_generation = ''`,
            row.cleanup_generation,
            row.ref_id,
            row.shard_index,
            row.generation
          );
        }
        if (
          row.cleanup_kind === ChunkCleanupKind.MultipartStaging &&
          row.cleanup_phase !== "staging"
        ) {
          row.cleanup_phase = "staging";
          durableObject.sql.exec(
            `UPDATE chunk_cleanup_intents SET cleanup_phase = 'staging'
              WHERE ref_id = ? AND shard_index = ?
                AND state = 'pending' AND generation = ?`,
            row.ref_id,
            row.shard_index,
            row.generation
          );
        }
        durableObject.sql.exec(
          `UPDATE chunk_cleanup_intents
              SET state = 'in_flight', generation = generation + 1,
                  updated_at = ?, next_attempt_at = ?
            WHERE ref_id = ? AND shard_index = ?
              AND state = 'pending' AND generation = ?`,
          Date.now(),
          Date.now() + CLEANUP_CLAIM_LEASE_MS,
          row.ref_id,
          row.shard_index,
          row.generation
        );
        row.generation++;
        return true;
      });
      if (!claimed) return;

      let result: { cursor: number; done: boolean };
      try {
        result =
          row.cleanup_phase === "staging"
            ? await stub.clearMultipartStagingPage(
                row.ref_id,
                row.cleanup_cursor,
                row.cleanup_generation
              )
            : await stub.deleteChunksPage(
                row.ref_id,
                row.cleanup_cursor,
                row.cleanup_generation
              );
      } catch (err) {
        retrySchedulingNeeded = true;
        const failedAt = Date.now();
        transactionSync(durableObject, () => {
          durableObject.sql.exec(
            `UPDATE chunk_cleanup_intents
                SET state = 'pending', generation = generation + 1,
                    attempts = attempts + 1, updated_at = ?,
                    next_attempt_at = ?, last_error = ?
              WHERE ref_id = ? AND shard_index = ?
                AND state = 'in_flight' AND generation = ?`,
            failedAt,
            failedAt + cleanupBackoffMs(row.attempts),
            errorMessage(err),
            row.ref_id,
            row.shard_index,
            row.generation
          );
        });
        return;
      }

      if (!result.done) {
        retrySchedulingNeeded = true;
        const progressedAt = Date.now();
        transactionSync(durableObject, () => {
          durableObject.sql.exec(
            `UPDATE chunk_cleanup_intents
                SET state = 'pending', generation = generation + 1,
                    cleanup_cursor = ?, updated_at = ?, next_attempt_at = ?,
                    last_error = NULL
              WHERE ref_id = ? AND shard_index = ?
                AND state = 'in_flight' AND generation = ?`,
            result.cursor,
            progressedAt,
            progressedAt,
            row.ref_id,
            row.shard_index,
            row.generation
          );
        });
        return;
      }

      if (
        row.cleanup_kind === ChunkCleanupKind.Multipart &&
        row.cleanup_phase === "chunks"
      ) {
        const progressedAt = Date.now();
        transactionSync(durableObject, () => {
          durableObject.sql.exec(
            `UPDATE chunk_cleanup_intents
                SET state = 'pending', generation = generation + 1,
                    cleanup_phase = 'staging', cleanup_cursor = 0,
                    updated_at = ?, next_attempt_at = ?, last_error = NULL
              WHERE ref_id = ? AND shard_index = ?
                AND state = 'in_flight' AND generation = ?`,
            progressedAt,
            progressedAt,
            row.ref_id,
            row.shard_index,
            row.generation
          );
        });
        row.generation++;
        row.cleanup_phase = "staging";
        row.cleanup_cursor = 0;
        continue;
      }

      transactionSync(durableObject, () => {
        if (row.provisional !== 0) {
          durableObject.sql.exec(
            `UPDATE chunk_cleanup_intents
                SET state = 'cleaned', generation = generation + 1,
                    cleanup_cursor = ?, updated_at = ?, last_error = NULL
              WHERE ref_id = ? AND shard_index = ?
                AND state = 'in_flight' AND generation = ?`,
            result.cursor,
            Date.now(),
            row.ref_id,
            row.shard_index,
            row.generation
          );
        } else {
          durableObject.sql.exec(
            `DELETE FROM chunk_cleanup_intents
              WHERE ref_id = ? AND shard_index = ?
                AND state = 'in_flight' AND generation = ?`,
            row.ref_id,
            row.shard_index,
            row.generation
          );
        }
      });
      return;
    }
  }

  async function lane(): Promise<void> {
    while (true) {
      const row = rows[cursor++];
      if (row === undefined) return;
      await drainOne(row);
    }
  }

  await Promise.all(
    Array.from(
      { length: Math.min(CLEANUP_CONCURRENCY, rows.length) },
      () => lane()
    )
  );

  // A ref-scoped drain is called by the operation that has conclusively
  // abandoned publication. Once cleanup completed, its terminal guard no
  // longer needs to block a publisher; no-scope alarm drains retain the guard
  // so a still-running delayed publisher must fail closed.
  if (refId !== undefined) {
    const refIds = typeof refId === "string" ? [refId] : [...refId];
    if (refIds.length > 0) {
      const placeholders = refIds.map(() => "?").join(",");
      transactionSync(durableObject, () => {
        durableObject.sql.exec(
          `DELETE FROM chunk_cleanup_intents
            WHERE state = 'cleaned' AND ref_id IN (${placeholders})`,
          ...refIds
        );
      });
    }
  }

  if (retrySchedulingNeeded) {
    const nextAttempt = durableObject.sql
      .exec<{ next_attempt_at: number | null }>(
        `SELECT MIN(next_attempt_at) AS next_attempt_at
           FROM chunk_cleanup_intents
          WHERE state IN ('pending', 'in_flight')`
      )
      .toArray()[0]?.next_attempt_at;
    if (nextAttempt !== null && nextAttempt !== undefined) {
      await scheduleChunkCleanupSweep(durableObject, nextAttempt);
    }
  }
}

// ── writeFile ──────────────────────────────────────────────────────────

/**
 * writeFile — POSIX-style atomic file write.
 *
 *   1. Resolve parent → (parentId, leaf). Parent must exist and be a dir.
 *   2. If a folder already occupies (parentId, leaf) → EISDIR.
 *   3. Cap at WRITEFILE_MAX → EFBIG.
 *   4. Inline tier (≤ INLINE_LIMIT): insert an uploading temp row carrying
 *      inline_data, then publish it through the same commit transaction.
 *   5. Chunked tier:
 *      a. Insert tmp file row with status='uploading',
 *         file_name='_vfs_tmp_<id>'. (The leading underscore prefix
 *         keeps it out of the UNIQUE-on-non-deleted index for the real
 *         leaf name; uploading rows are not 'deleted' but they DO
 *         occupy the unique index — using a tmp name avoids that
 *         collision while we stream chunks.)
 *      b. Chunk + hash + placeChunk + putChunk RPC + recordChunk row.
 *      c. Commit in one `transactionSync`: supersede the live destination,
 *         promote the temp row, inherit path metadata, stage cleanup intents,
 *         delete displaced local rows, and bump the folder revision.
 *      d. After publication commits, drain cleanup intents through ShardDO;
 *         unacknowledged work remains durable for alarm retry.
 *      e. On any error before commit, abort: hard-delete the tmp row +
 *         its tmp chunks. Caller surface: the path either doesn't
 *         exist (no prior file) or still resolves to the prior
 *         contents (with a prior file).
 *
 * Concurrency: Cloudflare may interleave the upload phases at their awaits,
 * but each synchronous publication transaction is serialized. The later
 * transaction supersedes the earlier committed destination, preserving
 * last-writer-wins behavior.
 */
/**
 * versioning-ON write path.
 *
 * Pure-function-ish (depends on durableObject + ShardDO env, but no
 * cross-method state). Invariant we want a future TSLean proof to
 * cover: after a successful return, the path has exactly one new
 * head version row whose chunk_refs match the new content. On any
 * thrown error before the commit, NO version row exists and
 * ShardDO chunk_refs for `${pathId}#${versionId}` are reaped.
 */
async function vfsWriteFileVersioned(
  durableObject: UserDO,
  scope: VFSScope,
  userId: string,
  parentId: string | null,
  leaf: string,
  data: Uint8Array,
  mode: number,
  mimeType: string,
  now: number,
  requireVacantDestination: boolean,
  /**
   * Optional metadata + tags + version flags. Applied BEFORE
   * commitVersion so the snapshot captures them. Caller is responsible
   * for validation against the caps in `shared/metadata-caps.ts`.
   */
  meta: {
    metadataEncoded?: Uint8Array | null | undefined;
    tags?: readonly string[] | undefined;
    versionUserVisible?: boolean;
    versionLabel?: string;
    /** Encryption stamp for this version. */
    encryption?: { mode: "convergent" | "random"; keyId?: string };
  } = {},
  retryInitialPublicationConflict = true
): Promise<void> {
  const existing = durableObject.sql
    .exec(
      `SELECT file_id FROM files
        WHERE user_id=? AND IFNULL(parent_id,'')=IFNULL(?,'') AND file_name=? AND status='complete'`,
      userId,
      parentId,
      leaf
    )
    .toArray()[0] as { file_id: string } | undefined;
  if (existing && requireVacantDestination) {
    throw new VFSError(
      "EEXIST",
      `writeFile: destination exists and overwrite=false: ${leaf}`
    );
  }
  const pathId = existing?.file_id ?? generateId();
  const versionId = generateId();
  let tmpId: string | undefined;

  if (!existing) {
    tmpId = pathId;
    await scheduleStaleUploadSweep(durableObject);
    const tmpName = `_vfs_tmp_${tmpId}`;
    durableObject.sql.exec(
      `INSERT INTO files (file_id, user_id, parent_id, file_name, file_size, file_hash, mime_type, chunk_size, chunk_count, pool_size, status, created_at, updated_at, mode, node_kind)
       VALUES (?, ?, ?, ?, 0, '', ?, 0, 0, ?, 'uploading', ?, ?, ?, 'file')`,
      pathId,
      userId,
      parentId,
      tmpName,
      mimeType,
      poolSizeFor(durableObject, userId),
      now,
      now,
      mode
    );
  }

  let metadataForVersion: Uint8Array | null = null;
  if (meta.metadataEncoded === null) {
    metadataForVersion = null;
  } else if (meta.metadataEncoded !== undefined) {
    metadataForVersion = meta.metadataEncoded;
  } else if (existing) {
    const row = durableObject.sql
      .exec("SELECT metadata FROM files WHERE file_id = ?", pathId)
      .toArray()[0] as { metadata: ArrayBuffer | null } | undefined;
    metadataForVersion = row?.metadata ? new Uint8Array(row.metadata) : null;
  }

  let refId: string | undefined;
  try {
    if (data.byteLength <= INLINE_LIMIT) {
      const inlineUsed = (
        durableObject.sql
          .exec(
            "SELECT COALESCE(inline_bytes_used, 0) AS used FROM quota WHERE user_id = ?",
            userId
          )
          .toArray()[0] as { used: number } | undefined
      )?.used ?? 0;
      if (inlineUsed + data.byteLength <= INLINE_TIER_CAP) {
        await publishVersionedWrite(
          durableObject,
          scope,
          userId,
          parentId,
          leaf,
          pathId,
          tmpId,
          () => {
            enforceModeMonotonic(
              durableObject,
              userId,
              parentId,
              leaf,
              meta.encryption
            );
            applyVersionedWriteSideEffects(
              durableObject,
              userId,
              pathId,
              meta,
              now
            );
            commitVersion(durableObject, {
              pathId,
              versionId,
              userId,
              size: data.byteLength,
              mode,
              mtimeMs: now,
              chunkSize: 0,
              chunkCount: 0,
              fileHash: "",
              mimeType,
              inlineData: data,
              userVisible: meta.versionUserVisible ?? true,
              label: meta.versionLabel,
              metadata: metadataForVersion,
              encryption: meta.encryption,
            });
          }
        );
        return;
      }
      if (inlineUsed < INLINE_TIER_CAP) {
        const tenantId = scope.sub
          ? `${scope.ns}::${scope.tenant}::${scope.sub}`
          : `${scope.ns}::${scope.tenant}`;
        logWarn(
          "inline tier cap first crossing (versioned)",
          { tenantId },
          {
            event: "inline_tier_cap_first_crossing",
            versioned: true,
            inlineBytesUsed: inlineUsed,
            capBytes: INLINE_TIER_CAP,
            incomingByteLength: data.byteLength,
          }
        );
      }
    }

    const { chunkSize, chunkCount } = computeChunkSpec(data.byteLength);
    const poolSize = poolSizeFor(durableObject, userId);
    const shardNs = durableObject.envPublic
      .MOSSAIC_SHARD as unknown as DurableObjectNamespace<ShardDO>;
    refId = shardRefId(pathId, versionId);
    const chunks = await Promise.all(
      Array.from({ length: chunkCount }, async (_, chunkIndex) => {
        const start = chunkIndex * chunkSize;
        const slice = data.subarray(
          start,
          Math.min(start + chunkSize, data.byteLength)
        );
        const hash = await hashChunk(slice);
        const shardIndex = placeChunkForVersion(
          durableObject,
          scope,
          hash,
          poolSize
        );
        return {
          chunk_index: chunkIndex,
          chunk_hash: hash,
          chunk_size: slice.byteLength,
          shard_index: shardIndex,
          data: slice,
        };
      })
    );
    await stageChunkCleanupIntents(
      durableObject,
      refId,
      chunks.map((chunk) => chunk.shard_index)
    );
    const publishedChunks = new Array<{
      chunk_index: number;
      chunk_hash: string;
      chunk_size: number;
      shard_index: number;
    }>(chunkCount);
    const fileHashByIdx = new Array<string>(chunkCount);
    let cursor = 0;
    async function lane(): Promise<void> {
      while (true) {
        const chunkIndex = cursor++;
        if (chunkIndex >= chunkCount) return;
        const chunk = chunks[chunkIndex]!;
        const shardName = vfsShardDOName(
          scope.ns,
          scope.tenant,
          scope.sub,
          chunk.shard_index
        );
        const stub = shardNs.get(shardNs.idFromName(shardName));
        await stub.putChunk(
          chunk.chunk_hash,
          chunk.data,
          refId!,
          chunkIndex,
          userId
        );
        publishedChunks[chunkIndex] = chunk;
        fileHashByIdx[chunkIndex] = chunk.chunk_hash;
      }
    }
    const lanes = Array.from(
      { length: Math.min(8, chunkCount) },
      () => lane()
    );
    const uploadResults = await Promise.allSettled(lanes);
    const uploadFailure = uploadResults.find(
      (result): result is PromiseRejectedResult => result.status === "rejected"
    );
    if (uploadFailure) throw uploadFailure.reason;

    const fileHash = await hashChunk(
      new TextEncoder().encode(fileHashByIdx.join(""))
    );
    await publishVersionedWrite(
      durableObject,
      scope,
      userId,
      parentId,
      leaf,
      pathId,
      tmpId,
      () => {
        enforceModeMonotonic(
          durableObject,
          userId,
          parentId,
          leaf,
          meta.encryption
        );
        for (const chunk of publishedChunks) {
          insertVersionChunk(durableObject, versionId, chunk);
        }
        applyVersionedWriteSideEffects(
          durableObject,
          userId,
          pathId,
          meta,
          now
        );
        commitVersion(durableObject, {
          pathId,
          versionId,
          userId,
          size: data.byteLength,
          mode,
          mtimeMs: now,
          chunkSize,
          chunkCount,
          fileHash,
          mimeType,
          inlineData: null,
          userVisible: meta.versionUserVisible ?? true,
          label: meta.versionLabel,
          metadata: metadataForVersion,
          encryption: meta.encryption,
        });
        disarmChunkCleanupIntents(durableObject, refId!);
      }
    );
  } catch (err) {
    durableObject.sql.exec(
      "DELETE FROM version_chunks WHERE version_id = ?",
      versionId
    );
    if (tmpId !== undefined) {
      await abortTempFile(durableObject, userId, scope, tmpId);
    }
    if (refId !== undefined) {
      await drainChunkCleanupIntents(durableObject, scope, refId);
    }

    if (
      retryInitialPublicationConflict &&
      !requireVacantDestination &&
      tmpId !== undefined &&
      err instanceof VFSError &&
      err.code === "EBUSY" &&
      err.message.includes("destination appeared during commit")
    ) {
      return vfsWriteFileVersioned(
        durableObject,
        scope,
        userId,
        parentId,
        leaf,
        data,
        mode,
        mimeType,
        now,
        false,
        meta,
        false
      );
    }
    throw err;
  }
}

async function publishVersionedWrite(
  durableObject: UserDO,
  scope: VFSScope,
  userId: string,
  parentId: string | null,
  leaf: string,
  pathId: string,
  tmpId: string | undefined,
  finalizeLocal: () => void
): Promise<void> {
  if (tmpId !== undefined) {
    await commitRename(durableObject, userId, scope, tmpId, parentId, leaf, {
      requireVacantDestination: true,
      finalizeLocal,
    });
    return;
  }

  await scheduleStaleUploadSweep(durableObject);
  transactionSync(durableObject, () => {
    const live = findLiveFile(durableObject, userId, parentId, leaf);
    if (live?.file_id !== pathId) {
      throw new VFSError(
        "EBUSY",
        "writeFile: versioned path changed during commit"
      );
    }
    finalizeLocal();
    bumpFolderRevision(durableObject, userId, parentId);
  });
}

function applyVersionedWriteSideEffects(
  durableObject: UserDO,
  userId: string,
  pathId: string,
  meta: {
    metadataEncoded?: Uint8Array | null | undefined;
    tags?: readonly string[] | undefined;
  },
  mtimeMs: number
): void {
  if (meta.metadataEncoded !== undefined) {
    durableObject.sql.exec(
      "UPDATE files SET metadata = ?, updated_at = ? WHERE file_id = ?",
      meta.metadataEncoded,
      mtimeMs,
      pathId
    );
  }
  if (meta.tags !== undefined) {
    replaceTags(durableObject, userId, pathId, meta.tags);
  } else {
    bumpTagMtimes(durableObject, pathId, mtimeMs);
  }
}

/**
 * Extended writeFile options. All fields are optional and default
 * to behavior bit-identical to a plain `writeFile` call.
 *
 * - `metadata`: undefined → no change; null → CLEAR; object → SET.
 * - `tags`: undefined → no change; [] → drop all; [...] → REPLACE.
 * - `version.label`: optional ≤128-char human-readable label.
 * - `version.userVisible`: defaults to true for explicit writes.
 *   YjsRuntime opportunistic compactions pass false; explicit
 *   flush() passes true.
 */
export interface VFSWriteFileOpts {
  mode?: number;
  mimeType?: string;
  metadata?: Record<string, unknown> | null;
  tags?: readonly string[];
  version?: { label?: string; userVisible?: boolean };
  /**
   * opt-in end-to-end encryption.
   *
   * When set, the worker stamps `files.encryption_mode` and
   * `files.encryption_key_id` (and the corresponding `file_versions`
   * columns when versioning is on). Mode-history-monotonic: a write
   * that disagrees with the existing path's mode is rejected EBADF.
   *
   * The `data` payload is treated identically to plaintext bytes
   * regardless of this opt — the SDK has already produced an
   * envelope-stream by this point. The server NEVER decrypts.
   */
  encryption?: { mode: "convergent" | "random"; keyId?: string };
}

/**
 * Resolved + validated write-commit input. Produced by
 * {@link prepareWriteCommit} and consumed by both
 * {@link commitInlineTier} and {@link commitChunkedTier}.
 *
 * All input validation has already happened by the time a `WriteCommitPlan`
 * exists — so `executeWriteCommit` can focus on storage-layer ordering
 * (insert tmp row → push chunks → commitRename → post-commit side
 * effects) without re-validating.
 */
export interface WriteCommitPlan {
  userId: string;
  parentId: string | null;
  leaf: string;
  mode: number;
  mimeType: string;
  /** Pre-validated, encoded metadata blob (or undefined for "no change"). */
  metadataEncoded: Uint8Array | null | undefined;
  /** Pre-validated tag set (undefined → unchanged). */
  tags: readonly string[] | undefined;
  /** Optional encryption stamp (mode-history monotonicity already enforced). */
  encryption: { mode: "convergent" | "random"; keyId?: string } | undefined;
  /** Internal copyFile publication CAS; ordinary writeFile remains overwrite. */
  requireVacantDestination: boolean;
  /** Wall-clock millisecond timestamp captured before any SQL touches the row. */
  now: number;
}

/**
 * Validate caller-supplied write opts and resolve the canonical target.
 *
 * Throws VFSError BEFORE any SQL touches the row:
 * - `EFBIG` if `data` exceeds WRITEFILE_MAX.
 * - `EISDIR` if the target path is a directory.
 * - `EINVAL` from metadata/tags/version validators on cap violation.
 * - `EBADF` from `enforceModeMonotonic` when the encryption mode
 *   disagrees with the existing path's history.
 *
 * Returns a {@link WriteCommitPlan} that the tier-specific commit
 * helpers consume. Idempotent: calling twice with the same args
 * resolves the same plan (modulo `now`).
 */
async function prepareWriteCommit(
  durableObject: UserDO,
  scope: VFSScope,
  path: string,
  byteLength: number,
  opts: VFSWriteFileOpts,
  requireVacantDestination: boolean
): Promise<WriteCommitPlan> {
  const userId = userIdFor(scope);
  const { parentId, leaf } = resolveParent(durableObject, userId, path);

  if (byteLength > WRITEFILE_MAX) {
    throw new VFSError(
      "EFBIG",
      `writeFile: ${byteLength} > WRITEFILE_MAX ${WRITEFILE_MAX}`
    );
  }
  if (folderExists(durableObject, userId, parentId, leaf)) {
    throw new VFSError("EISDIR", `writeFile: target is a directory: ${path}`);
  }

  // validate metadata + tags BEFORE any SQL touches the
  // row. Validators throw VFSError("EINVAL", ...) on cap violation.
  let metadataEncoded: Uint8Array | null | undefined;
  if (opts.metadata === null) {
    metadataEncoded = null; // explicit clear
  } else if (opts.metadata !== undefined) {
    metadataEncoded = validateMetadata(opts.metadata).encoded;
  }
  if (opts.tags !== undefined) {
    validateTags(opts.tags);
  }
  if (opts.version?.label !== undefined) {
    validateLabel(opts.version.label);
  }

  // validate encryption opts shape and enforce mode-history
  // monotonicity. Both checks throw VFSError before any SQL touches
  // the row, so a rejected write leaves the existing path untouched.
  if (opts.encryption) {
    const { validateEncryptionOpts, enforceModeMonotonic } = await import(
      "../encryption-stamp"
    );
    validateEncryptionOpts(opts.encryption);
    enforceModeMonotonic(durableObject, userId, parentId, leaf, opts.encryption);
  } else {
    // Plaintext write: still need to check we're not silently writing
    // plaintext to an encrypted path.
    const { enforceModeMonotonic } = await import("../encryption-stamp");
    enforceModeMonotonic(durableObject, userId, parentId, leaf, undefined);
  }

  return {
    userId,
    parentId,
    leaf,
    mode: opts.mode ?? 0o644,
    mimeType: opts.mimeType ?? "application/octet-stream",
    metadataEncoded,
    tags: opts.tags,
    encryption: opts.encryption,
    requireVacantDestination,
    now: Date.now(),
  };
}

/**
 * Execute the inline-tier commit: insert a tmp `files` row carrying the
 * full payload in `inline_data`, then atomically publish the rename,
 * side effects, and accounting through {@link commitRename}.
 *
 * Two-phase commit pattern: insert with tmp name first so concurrent
 * readers either see the prior file or the new one — never a
 * half-formed inline_data row at the live name.
 */
async function commitInlineTier(
  durableObject: UserDO,
  scope: VFSScope,
  plan: WriteCommitPlan,
  data: Uint8Array
): Promise<void> {
  const tmpId = generateId();
  const tmpName = `_vfs_tmp_${tmpId}`;
  durableObject.sql.exec(
    `INSERT INTO files (file_id, user_id, parent_id, file_name, file_size, file_hash, mime_type, chunk_size, chunk_count, pool_size, status, created_at, updated_at, mode, node_kind, inline_data)
     VALUES (?, ?, ?, ?, ?, '', ?, 0, 0, ?, 'uploading', ?, ?, ?, 'file', ?)`,
    tmpId,
    plan.userId,
    plan.parentId,
    tmpName,
    data.byteLength,
    plan.mimeType,
    poolSizeFor(durableObject, plan.userId),
    plan.now,
    plan.now,
    plan.mode,
    data
  );
  // H1: schedule the stale-upload sweep so this row is reclaimed
  // even if commitRename never runs (DO crash mid-method).
  await scheduleStaleUploadSweep(durableObject);
  await commitRename(durableObject, plan.userId, scope, tmpId, plan.parentId, plan.leaf, {
    requireVacantDestination: plan.requireVacantDestination,
    publicationEncryption: plan.encryption ?? null,
    finalizeLocal: () => {
      applyPostCommitSideEffects(
        durableObject,
        plan.userId,
        tmpId,
        plan.metadataEncoded,
        plan.tags,
        plan.now,
        plan.encryption,
        false
      );
      recordWriteUsage(
        durableObject,
        plan.userId,
        data.byteLength,
        1,
        data.byteLength
      );
    },
  });
}

/**
 * Execute the chunked-tier commit: chunk `data`, fan out PUTs to
 * ShardDOs with bounded concurrency (8 lanes), record `file_chunks`,
 * stamp `file_hash`, then atomically publish the rename, side effects,
 * and accounting.
 *
 * H3: parallel chunk PUTs. Concurrency cap = 8 (same rationale as the
 * read path: stays well inside the Workers concurrent-subrequest limit
 * and saturates typical bandwidth). Per-chunk file_chunks INSERT is
 * sync SQL inside the DO single-thread so SQL ordering is preserved
 * without coordination.
 *
 * On any throw mid-stream, {@link abortTempFile} reclaims the tmp row
 * + already-pushed chunks so we never leak storage on a failed write.
 */
async function commitChunkedTier(
  durableObject: UserDO,
  scope: VFSScope,
  plan: WriteCommitPlan,
  data: Uint8Array
): Promise<void> {
  const { chunkSize, chunkCount } = computeChunkSpec(data.byteLength);
  const tmpId = generateId();
  const tmpName = `_vfs_tmp_${tmpId}`;
  let poolSize = poolSizeFor(durableObject, plan.userId);

  // Load the skip-set once per write batch. Cold cache (empty
  // Set) = byte-equivalent to pure-rendezvous deterministic top-1
  // placement.
  let fullShards = loadFullShards(durableObject);

  // If every shard in the pool is full, force a pool-size bump
  // BEFORE the tmp row insert so the row records the post-growth
  // pool. We trigger growth by a 5 GiB \"phantom\" delta: it
  // doesn't change `storage_used`'s ground truth (we pass 0 for
  // bytes) but it forces the pool-size recompute. The simpler
  // alternative \u2014 directly UPDATE quota.pool_size += 1 \u2014
  // bypasses Lean's monotonicity invariant proof; using
  // recordWriteUsage keeps the proof trivially valid because
  // writes only ever grow the pool.
  if (fullShards.size >= poolSize) {
    // Bump the pool. We add `BYTES_PER_SHARD` to storage_used
    // virtually, then immediately consume the headroom \u2014 but
    // since recordWriteUsage caps the recomputation to
    // `BASE_POOL + floor(storage_used / BYTES_PER_SHARD)`, this
    // grows pool_size by at most 1. After the bump, the new
    // shard is non-full (it's empty) and placement succeeds.
    durableObject.sql.exec(
      `UPDATE quota
          SET pool_size = pool_size + 1
        WHERE user_id = ?`,
      plan.userId
    );
    poolSize = poolSize + 1;
    // Re-read \u2014 the new shard is not in the cache so it's
    // implicitly non-full.
    fullShards = loadFullShards(durableObject);
    const tenantId = scope.sub
      ? `${scope.ns}::${scope.tenant}::${scope.sub}`
      : `${scope.ns}::${scope.tenant}`;
    logWarn(
      "pool growth forced by full shards",
      { tenantId },
      {
        event: "pool_growth_forced_by_full_shards",
        newPoolSize: poolSize,
        fullShardCount: fullShards.size,
      }
    );
  }

  durableObject.sql.exec(
    `INSERT INTO files (file_id, user_id, parent_id, file_name, file_size, file_hash, mime_type, chunk_size, chunk_count, pool_size, status, created_at, updated_at, mode, node_kind)
     VALUES (?, ?, ?, ?, ?, '', ?, ?, ?, ?, 'uploading', ?, ?, ?, 'file')`,
    tmpId,
    plan.userId,
    plan.parentId,
    tmpName,
    data.byteLength,
    plan.mimeType,
    chunkSize,
    chunkCount,
    poolSize,
    plan.now,
    plan.now,
    plan.mode
  );
  await scheduleStaleUploadSweep(durableObject);

  const env = durableObject.envPublic;
  const shardNs = env.MOSSAIC_SHARD as unknown as DurableObjectNamespace<ShardDO>;
  try {
    const chunks = await Promise.all(
      Array.from({ length: chunkCount }, async (_, i) => {
        const start = i * chunkSize;
        const end = Math.min(start + chunkSize, data.byteLength);
        const bytes = data.subarray(start, end);
        const hash = await hashChunk(bytes);
        const shardIndex = placeChunk(
          userIdFor(scope),
          tmpId,
          i,
          poolSize,
          fullShards
        );
        if (shardIndex === POOL_FULL) {
          throw new VFSError(
            "EBUSY",
            "writeFile: every shard at soft cap; pool growth required"
          );
        }
        return { bytes, hash, shardIndex };
      })
    );
    await stageChunkCleanupIntents(
      durableObject,
      tmpId,
      chunks.map((chunk) => chunk.shardIndex)
    );
    const fileHashParts = new Array<string>(chunkCount);
    const CONCURRENCY = 8;
    let cursor = 0;
    async function uploadOne(i: number): Promise<void> {
      const chunk = chunks[i]!;
      const shardName = vfsShardDOName(
        scope.ns,
        scope.tenant,
        scope.sub,
        chunk.shardIndex
      );
      const stub = shardNs.get(shardNs.idFromName(shardName));
      await stub.putChunk(
        chunk.hash,
        chunk.bytes,
        tmpId,
        i,
        plan.userId
      );
      durableObject.sql.exec(
        `INSERT OR REPLACE INTO file_chunks (file_id, chunk_index, chunk_hash, chunk_size, shard_index)
         VALUES (?, ?, ?, ?, ?)`,
        tmpId,
        i,
        chunk.hash,
        chunk.bytes.byteLength,
        chunk.shardIndex
      );
      fileHashParts[i] = chunk.hash;
    }
    async function lane(): Promise<void> {
      while (true) {
        const i = cursor++;
        if (i >= chunkCount) return;
        await uploadOne(i);
      }
    }
    const lanes: Promise<void>[] = [];
    for (let w = 0; w < Math.min(CONCURRENCY, chunkCount); w++) {
      lanes.push(lane());
    }
    const uploadResults = await Promise.allSettled(lanes);
    const uploadFailure = uploadResults.find(
      (result): result is PromiseRejectedResult => result.status === "rejected"
    );
    if (uploadFailure) throw uploadFailure.reason;

    const fileHash = await hashChunk(
      new TextEncoder().encode(fileHashParts.join(""))
    );
    durableObject.sql.exec(
      "UPDATE files SET file_hash = ? WHERE file_id = ?",
      fileHash,
      tmpId
    );
  } catch (err) {
    await abortTempFile(durableObject, plan.userId, scope, tmpId);
    throw err;
  }

  await commitRename(durableObject, plan.userId, scope, tmpId, plan.parentId, plan.leaf, {
    requireVacantDestination: plan.requireVacantDestination,
    publicationEncryption: plan.encryption ?? null,
    finalizeLocal: () => {
      applyPostCommitSideEffects(
        durableObject,
        plan.userId,
        tmpId,
        plan.metadataEncoded,
        plan.tags,
        plan.now,
        plan.encryption,
        false
      );
      recordWriteUsage(durableObject, plan.userId, data.byteLength, 1);
      disarmChunkCleanupIntents(durableObject, tmpId);
    },
  });
}

export async function vfsWriteFile(
  durableObject: UserDO,
  scope: VFSScope,
  path: string,
  data: Uint8Array,
  opts: VFSWriteFileOpts = {},
  requireVacantDestination = false
): Promise<void> {
  const plan = await prepareWriteCommit(
    durableObject,
    scope,
    path,
    data.byteLength,
    opts,
    requireVacantDestination
  );

  // yjs-mode fork. If the target file already exists with mode_yjs=1,
  // route the bytes through YjsRuntime: the data becomes the new value
  // of Y.Text("content") under origin "writeFile". Versioning fork is
  // bypassed — the yjs op log IS the history; explicit checkpoints
  // come from compaction (not from writeFile).
  const yjsRow = durableObject.sql
    .exec(
      `SELECT file_id, mode_yjs FROM files
         WHERE user_id=? AND IFNULL(parent_id,'')=IFNULL(?,'')
           AND file_name=? AND status='complete'`,
      plan.userId,
      plan.parentId,
      plan.leaf
    )
    .toArray()[0] as { file_id: string; mode_yjs: number } | undefined;
  if (yjsRow && requireVacantDestination) {
    throw new VFSError(
      "EEXIST",
      `writeFile: destination exists and overwrite=false: ${path}`
    );
  }
  if (yjsRow && yjsRow.mode_yjs === 1) {
    const { writeYjsBytes } = await import("../yjs");
    await writeYjsBytes(
      durableObject,
      scope,
      plan.userId,
      yjsRow.file_id,
      poolSizeFor(durableObject, plan.userId),
      data
    );
    applyPostCommitSideEffects(
      durableObject,
      plan.userId,
      yjsRow.file_id,
      plan.metadataEncoded,
      plan.tags,
      Date.now(),
      plan.encryption
    );
    return;
  }

  // Versioning fork. With versioning ON, every writeFile creates a
  // new file_versions row + per-version synthetic shard key; the
  // `files` row is just the stable identity holding the head pointer.
  if (isVersioningEnabled(durableObject, plan.userId)) {
    return vfsWriteFileVersioned(
      durableObject,
      scope,
      plan.userId,
      plan.parentId,
      plan.leaf,
      data,
      plan.mode,
      plan.mimeType,
      plan.now,
      plan.requireVacantDestination,
      {
        metadataEncoded: plan.metadataEncoded,
        tags: plan.tags,
        versionUserVisible: opts.version?.userVisible ?? true,
        versionLabel: opts.version?.label,
        encryption: plan.encryption,
      }
    );
  }

  // Tier dispatch. The inline tier embeds bytes in `files.inline_data`
  // (≤ INLINE_LIMIT); the chunked tier fans out to ShardDOs via
  // bounded-concurrency PUTs.
  //
  // Graceful migration. A tenant approaching the INLINE_TIER_CAP
  // (1 GiB cumulative inline bytes) spills NEW
  // tiny writes to the chunked tier instead of further loading
  // the UserDO's SQLite. Pre-existing inline rows are read
  // identically by `vfsReadFile` (it checks `inline_data IS NOT
  // NULL` first); the cap is a write-side gate, not a read-side
  // migration. `quota.inline_bytes_used` is maintained by
  // `recordWriteUsage`'s `deltaInlineBytes` parameter; cold
  // tenants (`COALESCE(NULL, 0)`) start at 0 and inline freely.
  if (data.byteLength <= INLINE_LIMIT) {
    const inlineUsed = (
      durableObject.sql
        .exec(
          "SELECT COALESCE(inline_bytes_used, 0) AS used FROM quota WHERE user_id = ?",
          plan.userId
        )
        .toArray()[0] as { used: number } | undefined
    )?.used ?? 0;
    if (inlineUsed + data.byteLength <= INLINE_TIER_CAP) {
      return commitInlineTier(durableObject, scope, plan, data);
    }
    // Spill to chunked tier; the per-write structured warning lets
    // operators see when a tenant first crosses the cap.
    if (inlineUsed < INLINE_TIER_CAP) {
      const tenantId = scope.sub
        ? `${scope.ns}::${scope.tenant}::${scope.sub}`
        : `${scope.ns}::${scope.tenant}`;
      logWarn(
        "inline tier cap first crossing",
        { tenantId },
        {
          event: "inline_tier_cap_first_crossing",
          inlineBytesUsed: inlineUsed,
          capBytes: INLINE_TIER_CAP,
          incomingByteLength: data.byteLength,
        }
      );
    }
  }
  return commitChunkedTier(durableObject, scope, plan, data);
}

/**
 * Apply metadata, tags, and encryption to a canonical file row. This is
 * synchronous SQL so rename-based callers can invoke it from their local
 * publication transaction; the Yjs path invokes it directly.
 *
 * - metadataEncoded === undefined: no change.
 * - metadataEncoded === null: clear (UPDATE files SET metadata=NULL).
 * - metadataEncoded === bytes: set.
 * - tags === undefined: bump existing tag mtimes only (so list-by-tag
 *   reflects the new write recency).
 * - tags === []: drop all tags.
 * - tags === [...]: replace the entire tag set.
 *
 * The versioned write path bakes these into commitVersion and is
 * NOT routed through here.
 */
function applyPostCommitSideEffects(
  durableObject: UserDO,
  userId: string,
  pathId: string,
  metadataEncoded: Uint8Array | null | undefined,
  tags: readonly string[] | undefined,
  mtimeMs: number,
  /**
   * optional encryption stamp. Mode-history-monotonicity
   * was already enforced at the top of `vfsWriteFile`; this just
   * applies the column UPDATE to the freshly-committed row.
   *
   * - undefined → no change to existing encryption columns. Note
   *   that for the chunked/inline write paths the freshly-inserted
   *   row already has NULL columns (defaults), so undefined here is
   *   correct for plaintext writes.
   * - { mode, keyId? } → stamp the columns.
   */
  encryption?: { mode: "convergent" | "random"; keyId?: string },
  bumpEncryptionRevision = true
): void {
  if (metadataEncoded !== undefined) {
    durableObject.sql.exec(
      "UPDATE files SET metadata = ?, updated_at = ? WHERE file_id = ?",
      metadataEncoded,
      mtimeMs,
      pathId
    );
  }
  if (tags !== undefined) {
    replaceTags(durableObject, userId, pathId, tags);
  } else {
    bumpTagMtimes(durableObject, pathId, mtimeMs);
  }
  if (encryption !== undefined) {
    stampFileEncryption(
      durableObject,
      pathId,
      encryption,
      bumpEncryptionRevision ? userId : undefined
    );
  }
}

export interface CommitRenameOptions {
  /** Reject publication if a live destination appeared after preflight. */
  requireVacantDestination?: boolean;
  /** Require an occupied destination to retain the captured identity/head. */
  expectedDestination?: {
    fileId: string;
    headVersionId: string | null;
  };
  /** Synchronous identity checks that run before any publication mutation. */
  preconditionLocal?: () => void;
  /** Recheck the destination encryption mode after all remote awaits. */
  publicationEncryption?: EncryptionStampOpts | null;
  /** Synchronous SQL-only effects included in the publication transaction. */
  finalizeLocal?: () => void;
}

/**
 * Commit-rename: flip the tmp row to the real leaf, superseding any
 * existing live file at the destination. Destination supersede, temp
 * promotion, inheritance, displaced-row deletion/accounting, cleanup-intent
 * staging, and folder revision all run in one synchronous `transactionSync`.
 *
 * The prior live row is marked `deleted` to free the partial unique-index
 * slot, the temp row is promoted to `complete`, inherited metadata is copied,
 * and the displaced row is removed before the transaction commits.
 *
 * No network or crypto await occurs inside that transaction. After local
 * publication commits, cleanup intents are drained through idempotent
 * ShardDO RPCs; failures do not undo or hide the published file.
 */
export async function commitRename(
  durableObject: UserDO,
  userId: string,
  scope: VFSScope,
  tmpId: string,
  parentId: string | null,
  leaf: string,
  options: CommitRenameOptions = {}
): Promise<void> {
  await scheduleStaleUploadSweep(durableObject);
  let supersededId: string | undefined;
  try {
    transactionSync(durableObject, () => {
      const tempReady = durableObject.sql
        .exec(
          `SELECT 1 FROM files
            WHERE file_id=? AND user_id=? AND status='uploading'
              AND IFNULL(parent_id,'')=IFNULL(?,'')`,
          tmpId,
          userId,
          parentId
        )
        .toArray();
      if (tempReady.length !== 1) {
        throw new CommitConflictError("temporary file changed during commit");
      }
      options.preconditionLocal?.();
      if (Object.hasOwn(options, "publicationEncryption")) {
        enforceModeMonotonic(
          durableObject,
          userId,
          parentId,
          leaf,
          options.publicationEncryption ?? undefined
        );
      }

      const live = findLiveFile(durableObject, userId, parentId, leaf);
      if (options.expectedDestination) {
        const destination = durableObject.sql
          .exec(
            `SELECT head_version_id FROM files
              WHERE file_id = ? AND user_id = ? AND status = 'complete'
                AND IFNULL(parent_id, '') = IFNULL(?, '') AND file_name = ?`,
            options.expectedDestination.fileId,
            userId,
            parentId,
            leaf
          )
          .toArray()[0] as { head_version_id: string | null } | undefined;
        if (
          live?.file_id !== options.expectedDestination.fileId ||
          destination?.head_version_id !==
            options.expectedDestination.headVersionId
        ) {
          throw new CommitConflictError("destination changed during commit");
        }
      } else if (live && options.requireVacantDestination) {
        throw new CommitConflictError("destination appeared during commit");
      }
      const now = Date.now();
      if (live) {
        durableObject.sql.exec(
          `UPDATE files
              SET status='deleted', deleted_at=?, updated_at=?
            WHERE file_id=? AND user_id=? AND status='complete'`,
          now,
          now,
          live.file_id,
          userId
        );
        if (lastSqlChanges(durableObject) !== 1) {
          throw new CommitConflictError("destination changed during commit");
        }
        supersededId = live.file_id;
      }

      durableObject.sql.exec(
        `UPDATE files
            SET file_name=?, status='complete', updated_at=?
          WHERE file_id=? AND user_id=? AND status='uploading'
            AND IFNULL(parent_id,'')=IFNULL(?,'')`,
        leaf,
        now,
        tmpId,
        userId,
        parentId
      );
      if (lastSqlChanges(durableObject) !== 1) {
        throw new CommitConflictError("temporary file changed during commit");
      }

      // carry forward metadata + tags from the youngest
      // superseded row IFF the new tmp row hasn't already had them
      // set explicitly. This makes tags + metadata behave like
      // `mode` — properties of the path, not bound to the file_id.
      // Without this, a `writeFile(path, bytes)` call (no opts) on
      // a path that previously had tags would silently lose them
      // because the new tmp row's file_id is fresh.
      //
      // The destination row remains readable inside this SQL transaction
      // until inheritance is complete and the local hard-delete runs.
      if (supersededId !== undefined) {
        const fromId = supersededId;
        // Copy metadata only if tmp doesn't already have one. The
        // Rename callers apply explicit metadata in the finalizer below,
        // so NULL here means the inherited value remains the default.
        const tmpMeta = durableObject.sql
          .exec("SELECT metadata FROM files WHERE file_id=?", tmpId)
          .toArray()[0] as { metadata: ArrayBuffer | null } | undefined;
        if (!tmpMeta || tmpMeta.metadata === null) {
          durableObject.sql.exec(
            `UPDATE files SET metadata = (SELECT metadata FROM files WHERE file_id=?)
              WHERE file_id=?`,
            fromId,
            tmpId
          );
        }
        // Copy tags from the superseded row to the tmp row — only
        // if the tmp row has no tags yet (i.e. the writer didn't
        // explicitly pass tags=[] or tags=[...]).
        const tmpTagCount = (
          durableObject.sql
            .exec(
              "SELECT COUNT(*) AS n FROM file_tags WHERE path_id=?",
              tmpId
            )
            .toArray()[0] as { n: number }
        ).n;
        if (tmpTagCount === 0) {
          durableObject.sql.exec(
            `INSERT OR IGNORE INTO file_tags (path_id, tag, user_id, mtime_ms)
             SELECT ?, tag, user_id, ? FROM file_tags WHERE path_id=?`,
            tmpId,
            now,
            fromId
          );
        }
        hardDeleteFileRowLocal(durableObject, userId, supersededId);
      }

      options.finalizeLocal?.();
      bumpFolderRevision(durableObject, userId, parentId);
    });
  } catch (err) {
    await abortTempFile(durableObject, userId, scope, tmpId);
    if (
      err instanceof CommitConflictError ||
      isDestinationUniqueConstraintError(err)
    ) {
      throw new VFSError(
        "EBUSY",
        `writeFile: destination conflict: ${errorMessage(err)}`
      );
    }
    throw err;
  }

  if (supersededId !== undefined) {
    await drainChunkCleanupIntents(durableObject, scope, supersededId);
  }
}

/**
 * Abort a temp file write: hard-delete the tmp `files` row, drop any
 * already-recorded `file_chunks`, and stage chunk GC for each touched shard.
 * Idempotent: safe to call on a tmp_id that no longer exists.
 */
export async function abortTempFile(
  durableObject: UserDO,
  userId: string,
  scope: VFSScope,
  tmpId: string
): Promise<void> {
  await scheduleStaleUploadSweep(durableObject);
  let aborted = false;
  transactionSync(durableObject, () => {
    const row = durableObject.sql
      .exec(
        "SELECT status FROM files WHERE file_id = ? AND user_id = ?",
        tmpId,
        userId
      )
      .toArray()[0] as { status: string } | undefined;
    if (row?.status !== "uploading") return;
    hardDeleteFileRowLocal(durableObject, userId, tmpId);
    aborted = true;
  });
  if (aborted) {
    await drainChunkCleanupIntents(durableObject, scope, tmpId);
  }
}
