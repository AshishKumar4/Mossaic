/**
 * Multipart parallel transfer engine, server-side.
 *
 * This module implements multipart session, hash staging, and finalization
 * RPCs. Finalization is resumable across Durable Object eviction:
 *
 *   - `vfsBeginMultipart` — mints a session, inserts a tmp `files` row
 *     (status='uploading'), inserts an `upload_sessions` row, signs an
 *     HMAC session token. Single UserDO turn; zero ShardDO RPCs.
 *
 *   - `vfsAbortMultipart` — persists and advances bounded fencing, cleanup
 *     intent, staged-row, and local-delete phases. Alarms resume unfinished
 *     work after a caller disconnects.
 *
 *   - `vfsStageMultipartHashes` persists at most 256 expected hashes.
 *
 *   - `vfsFinalizeMultipartStep` fences at most 64 shards, verifies at most
 *     256 chunks, then publishes the persisted manifest in one local
 *     transaction. `vfsFinalizeMultipart` is the bounded legacy adapter.
 *
 * The chunk PUT path lives entirely in the routes layer (not here) —
 * it doesn't touch UserDO at all, by design (Hard Constraint 1 from
 * the plan: UserDO touched only at session boundaries).
 *
 * Plan reference: `local/phase-16-plan.md` §2.2, §2.6, §2.7.
 */

import type { UserDOCore as UserDO } from "./user-do-core";
import type { ShardDO } from "../shard/shard-do";
import {
  VFSError,
  type VFSScope,
} from "../../../../shared/vfs-types";
import { computeChunkSpec } from "../../../../shared/chunking";
import { generateId, vfsShardDOName } from "../../lib/utils";
import { logError } from "../../lib/logger";
import { placeMultipartChunk } from "../../../../shared/placement";
import {
  signVFSMultipartToken,
  signVFSMultipartStatusCursor,
  verifyVFSMultipartStatusCursor,
} from "../../lib/auth";
import {
  MULTIPART_DEFAULT_TTL_MS,
  MULTIPART_FENCE_PAGE_SIZE,
  MULTIPART_HASH_PAGE_SIZE,
  MULTIPART_MAX_OPEN_SESSIONS_PER_TENANT,
  MULTIPART_LEGACY_PLACEMENT_VERSION,
  MULTIPART_PLACEMENT_VERSION,
  MULTIPART_PROTOCOL_VERSION,
  MULTIPART_STATUS_CURSOR_MAX_BYTES,
  MULTIPART_STATUS_ENTRY_PAGE_SIZE,
  MULTIPART_STATUS_SHARD_PAGE_SIZE,
  MULTIPART_TERMINAL_RETENTION_MS,
  type MultipartAbortProgress,
  type MultipartBeginResponse,
  type MultipartFinalizeProgress,
  type MultipartFinalizeResponse,
  type MultipartPlacementVersion,
  type MultipartStatusPageResponse,
} from "../../../../shared/multipart";
import {
  userIdFor,
  resolveParent,
  poolSizeFor,
  recordWriteUsage,
  folderExists,
  bumpFolderRevision,
  drainChunkCleanupIntents,
} from "./vfs-ops";
import { hardDeleteFileRowLocal } from "./vfs/write-commit";
import {
  commitVersionChecked,
  isVersioningEnabled,
  type VersionedFileExpectation,
} from "./vfs-versions";
import {
  validateLabel,
  validateMetadata,
  validateTags,
} from "../../../../shared/metadata-validate";
import {
  enforceModeMonotonic,
  validateEncryptionOpts,
  type EncryptionStampOpts,
} from "./encryption-stamp";
import { bytesToHex } from "../../../../shared/crypto";
import {
  createSha256State,
  digestSha256,
  restoreSha256State,
  serializeSha256State,
  updateSha256,
} from "../../../../shared/incremental-sha256";
import {
  ChunkCleanupKind,
  lastSqlChanges,
  scheduleAlarmAt,
  scheduleStaleUploadSweep,
  stageChunkCleanupIntent,
  transactionSync,
} from "./internal-storage";

export interface VFSBeginMultipartOpts {
  size: number;
  protocolVersion?: number;
  chunkSize?: number;
  mode?: number;
  mimeType?: string;
  metadata?: Record<string, unknown> | null;
  tags?: readonly string[];
  version?: { label?: string; userVisible?: boolean };
  encryption?: { mode: "convergent" | "random"; keyId?: string };
  resumeFrom?: string;
  ttlMs?: number;
}

const MULTIPART_LEGACY_MAX_POOL_SIZE = MULTIPART_FENCE_PAGE_SIZE;
const MULTIPART_LEGACY_FINALIZE_STEP_LIMIT = 10;

function maintainStagedHashCursor(durableObject: UserDO, uploadId: string): void {
  durableObject.sql.exec(
    `UPDATE upload_sessions
        SET staged_hash_cursor = COALESCE((
          SELECT MAX(chunk_index) + 1 FROM upload_expected_chunks
           WHERE upload_id = upload_sessions.upload_id
        ), 0)
      WHERE upload_id = ? AND staged_hash_cursor = 0
        AND EXISTS (
          SELECT 1 FROM upload_expected_chunks WHERE upload_id = ? LIMIT 1
        )`,
    uploadId,
    uploadId
  );
}

export function vfsStageMultipartHashes(
  durableObject: UserDO,
  scope: VFSScope,
  uploadId: string,
  startIndex: number,
  hashes: readonly string[]
): { staged: number; total: number } {
  const userId = userIdFor(scope);
  maintainStagedHashCursor(durableObject, uploadId);
  if (!Number.isInteger(startIndex) || startIndex < 0) {
    throw new VFSError("EINVAL", "stageMultipartHashes: invalid startIndex");
  }
  if (hashes.length === 0 || hashes.length > MULTIPART_HASH_PAGE_SIZE) {
    throw new VFSError(
      "EINVAL",
      `stageMultipartHashes: page must contain 1..${MULTIPART_HASH_PAGE_SIZE} hashes`
    );
  }
  const session = durableObject.sql
    .exec(
      `SELECT total_chunks, status, staged_hash_cursor FROM upload_sessions
        WHERE upload_id = ? AND user_id = ?`,
      uploadId,
      userId
    )
    .toArray()[0] as
    | { total_chunks: number; status: string; staged_hash_cursor: number }
    | undefined;
  if (!session) {
    throw new VFSError("ENOENT", "stageMultipartHashes: session not found");
  }
  if (startIndex + hashes.length > session.total_chunks) {
    throw new VFSError("EINVAL", "stageMultipartHashes: page exceeds session range");
  }
  for (let offset = 0; offset < hashes.length; offset++) {
    if (!/^[0-9a-f]{64}$/.test(hashes[offset]!)) {
      throw new VFSError(
        "EINVAL",
        `stageMultipartHashes: hashes[${offset}] is invalid`
      );
    }
  }
  if (session.status === "finalized") {
    return { staged: session.total_chunks, total: session.total_chunks };
  }
  if (session.status !== "open" && session.status !== "finalizing") {
    throw new VFSError(
      "EBUSY",
      `stageMultipartHashes: session status='${session.status}'`
    );
  }
  const endIndex = startIndex + hashes.length;
  if (startIndex > session.staged_hash_cursor) {
    throw new VFSError(
      "EINVAL",
      `stageMultipartHashes: page must start at contiguous cursor ${session.staged_hash_cursor}`
    );
  }
  if (startIndex < session.staged_hash_cursor) {
    if (endIndex > session.staged_hash_cursor) {
      throw new VFSError(
        "EBUSY",
        "stageMultipartHashes: replay overlaps the staged hash cursor"
      );
    }
    const prior = durableObject.sql
      .exec(
        `SELECT chunk_index, chunk_hash FROM upload_expected_chunks
          WHERE upload_id = ? AND chunk_index >= ? AND chunk_index < ?
          ORDER BY chunk_index`,
        uploadId,
        startIndex,
        endIndex
      )
      .toArray() as Array<{ chunk_index: number; chunk_hash: string }>;
    const mismatch = prior.findIndex(
      (row, offset) =>
        row.chunk_index !== startIndex + offset || row.chunk_hash !== hashes[offset]
    );
    if (prior.length !== hashes.length || mismatch !== -1) {
      throw new VFSError(
        "EBUSY",
        `stageMultipartHashes: conflicting replay at index ${startIndex + Math.max(0, mismatch)}`
      );
    }
    return { staged: session.staged_hash_cursor, total: session.total_chunks };
  }
  if (session.status === "finalizing") {
    throw new VFSError(
      "EBUSY",
      "stageMultipartHashes: finalize already started"
    );
  }

  transactionSync(durableObject, () => {
    for (let offset = 0; offset < hashes.length; offset++) {
      const chunkIndex = startIndex + offset;
      const hash = hashes[offset]!;
      durableObject.sql.exec(
        `INSERT INTO upload_expected_chunks
           (upload_id, chunk_index, chunk_hash) VALUES (?, ?, ?)`,
        uploadId,
        chunkIndex,
        hash
      );
    }
    durableObject.sql.exec(
      `UPDATE upload_sessions SET staged_hash_cursor = ?
        WHERE upload_id = ? AND user_id = ? AND status = 'open'
          AND staged_hash_cursor = ?`,
      endIndex,
      uploadId,
      userId,
      startIndex
    );
    if (lastSqlChanges(durableObject) !== 1) {
      throw new VFSError("EBUSY", "stageMultipartHashes: cursor changed");
    }
  });
  return { staged: endIndex, total: session.total_chunks };
}

interface UploadSessionRow {
  upload_id: string;
  fence_id: string | null;
  user_id: string;
  parent_id: string | null;
  leaf: string;
  total_size: number;
  total_chunks: number;
  chunk_size: number;
  pool_size: number;
  placement_version: MultipartPlacementVersion;
  expires_at: number;
  status: string;
  encryption_mode: string | null;
  encryption_key_id: string | null;
  metadata_blob: ArrayBuffer | null;
  tags_json: string | null;
  version_label: string | null;
  version_user_visible: number | null;
  mode: number;
  mime_type: string;
  created_at: number;
  finalize_phase: string | null;
  finalize_fence_cursor: number;
  finalize_chunk_cursor: number;
  finalize_verify_shard_cursor: number;
  finalize_total_size: number;
  finalize_sha_state: string | null;
  finalize_context: string | null;
  finalize_cleanup_cursor: number;
  finalize_result: string | null;
  staged_hash_cursor: number;
  finalize_old_manifest_cursor: number;
  finalize_intent_cursor: number;
  finalize_old_intent_cursor: number;
  finalize_old_cleanup_cursor: number;
  abort_phase: string | null;
  abort_fence_cursor: number;
  abort_intent_cursor: number;
  abort_cleanup_cursor: number;
  abort_old_intent_cursor: number;
  abort_retry_at: number;
  terminal_at: number | null;
}

class MultipartLocalCorruptionError extends VFSError {
  constructor(message: string) {
    super("EBUSY", message);
  }
}

interface MultipartFinalizeContext {
  schema: 1;
  versioning: boolean;
  pathId: string;
  versionId: string | null;
  expectedDestination: {
    fileId: string;
    headVersionId: string | null;
  } | null;
  committedAt: number;
  metadataPresent: boolean;
  metadataBase64: string | null;
  tagsPresent: boolean;
  tags: string[];
  /** Present only on sessions transitioned by the first paged release. */
  legacyPath?: string;
}

function shardNs(durableObject: UserDO): DurableObjectNamespace<ShardDO> {
  return durableObject.envPublic
    .MOSSAIC_SHARD as unknown as DurableObjectNamespace<ShardDO>;
}

export async function vfsFinalizeMultipartStep(
  durableObject: UserDO,
  scope: VFSScope,
  uploadId: string
): Promise<MultipartFinalizeProgress> {
  const userId = userIdFor(scope);
  maintainStagedHashCursor(durableObject, uploadId);
  let session = durableObject.sql
    .exec(
      "SELECT * FROM upload_sessions WHERE upload_id = ? AND user_id = ?",
      uploadId,
      userId
    )
    .toArray()[0] as unknown as UploadSessionRow | undefined;
  if (!session) throw new VFSError("ENOENT", "finalizeMultipartStep: session not found");
  if (session.status === "finalized") {
    return await cleanupFinalizedMultipart(durableObject, scope, session);
  }
  if (session.status === "open") {
    freezeMultipartFinalizeContext(durableObject, userId, uploadId);
    session = durableObject.sql
      .exec(
        "SELECT * FROM upload_sessions WHERE upload_id = ? AND user_id = ?",
        uploadId,
        userId
      )
      .toArray()[0] as unknown as UploadSessionRow;
  }
  if (session.status !== "finalizing") {
    throw new VFSError("EBUSY", `finalizeMultipartStep: session status='${session.status}'`);
  }
  if (session.finalize_context === null) {
    await requestMultipartAbort(durableObject, scope, uploadId, true);
    throw new VFSError(
      "EBUSY",
      "finalizeMultipartStep: pre-upgrade finalizing session was aborted"
    );
  }
  const context = parseFinalizeContext(session.finalize_context);
  if (session.finalize_phase === "verifying") {
    try {
      const start = session.finalize_chunk_cursor;
      const end = Math.min(
        start + MULTIPART_HASH_PAGE_SIZE,
        session.total_chunks
      );
      const expected = durableObject.sql
        .exec(
          `SELECT chunk_index, chunk_hash FROM upload_expected_chunks
            WHERE upload_id = ? AND chunk_index >= ? AND chunk_index < ?
            ORDER BY chunk_index`,
          uploadId,
          start,
          end
        )
        .toArray() as Array<{ chunk_index: number; chunk_hash: string }>;
      if (expected.length !== end - start) {
        throw new VFSError(
          "EINVAL",
          "finalizeMultipartStep: expected hash page is incomplete"
        );
      }
      const expectedHashes = new Map(
        expected.map((row) => [row.chunk_index, row.chunk_hash])
      );
      const shardStart = session.finalize_verify_shard_cursor;
      const shardEnd = Math.min(
        shardStart + MULTIPART_FENCE_PAGE_SIZE,
        session.pool_size
      );

      if (shardStart < session.pool_size) {
        const ns = shardNs(durableObject);
        const manifests = await Promise.all(
          Array.from({ length: shardEnd - shardStart }, async (_, offset) => {
            const shardIndex = shardStart + offset;
            const shardName = vfsShardDOName(
              scope.ns,
              scope.tenant,
              scope.sub,
              shardIndex
            );
            const manifest = await ns
              .get(ns.idFromName(shardName))
              .getMultipartManifestRange(uploadId, start, end);
            return { shardIndex, rows: manifest.rows };
          })
        );
        const priorRows = durableObject.sql
          .exec(
            `SELECT chunk_index, shard_index FROM upload_verified_chunks
              WHERE upload_id = ? AND chunk_index >= ? AND chunk_index < ?`,
            uploadId,
            start,
            end
          )
          .toArray() as Array<{ chunk_index: number; shard_index: number }>;
        const seen = new Map(
          priorRows.map((row) => [row.chunk_index, row.shard_index])
        );
        const landed: Array<{
          idx: number;
          hash: string;
          size: number;
          shardIndex: number;
        }> = [];
        for (const manifest of manifests) {
          for (const row of manifest.rows) {
            const priorShard = seen.get(row.idx);
            if (priorShard !== undefined) {
              throw new VFSError(
                "EBADF",
                `finalizeMultipartStep: duplicate chunk index ${row.idx} across shards ${priorShard} and ${manifest.shardIndex}`
              );
            }
            seen.set(row.idx, manifest.shardIndex);
            landed.push({ ...row, shardIndex: manifest.shardIndex });
          }
        }
        for (const row of landed) {
          const expectedShard = placeMultipartChunk(
            userId,
            uploadId,
            row.idx,
            session.pool_size,
            session.placement_version
          );
          if (expectedShard !== row.shardIndex) {
            throw new VFSError(
              "EBADF",
              `finalizeMultipartStep: chunk ${row.idx} landed on shard ${row.shardIndex}; expected shard ${expectedShard}`
            );
          }
          if (expectedHashes.get(row.idx) !== row.hash) {
            throw new VFSError(
              "EBADF",
              `finalizeMultipartStep: chunk ${row.idx} hash divergence`
            );
          }
        }
        transactionSync(durableObject, () => {
          for (const row of landed) {
            durableObject.sql.exec(
              `INSERT INTO upload_verified_chunks
                 (upload_id, chunk_index, chunk_hash, chunk_size, shard_index)
               VALUES (?, ?, ?, ?, ?)`,
              uploadId,
              row.idx,
              row.hash,
              row.size,
              row.shardIndex
            );
          }
          durableObject.sql.exec(
            `UPDATE upload_sessions
                SET finalize_verify_shard_cursor = ?
              WHERE upload_id = ? AND user_id = ? AND status = 'finalizing'
                AND finalize_phase = 'verifying' AND finalize_chunk_cursor = ?
                AND finalize_verify_shard_cursor = ?`,
            shardEnd,
            uploadId,
            userId,
            start,
            shardStart
          );
          if (lastSqlChanges(durableObject) !== 1) {
            throw new VFSError(
              "EBUSY",
              "finalizeMultipartStep: verify shard cursor changed"
            );
          }
        });
        if (shardEnd < session.pool_size) {
          return {
            done: false,
            phase: "verifying",
            cursor: start,
            total: session.total_chunks,
          };
        }
      }

      const verified = durableObject.sql
        .exec(
          `SELECT chunk_index, chunk_hash, chunk_size, shard_index
             FROM upload_verified_chunks
            WHERE upload_id = ? AND chunk_index >= ? AND chunk_index < ?
            ORDER BY chunk_index`,
          uploadId,
          start,
          end
        )
        .toArray() as Array<{
          chunk_index: number;
          chunk_hash: string;
          chunk_size: number;
          shard_index: number;
        }>;
      for (let offset = 0; offset < expected.length; offset++) {
        const expectedRow = expected[offset]!;
        const actual = verified[offset];
        if (!actual || actual.chunk_index !== expectedRow.chunk_index) {
          throw new VFSError(
            "ENOENT",
            `finalizeMultipartStep: chunk ${expectedRow.chunk_index} not landed`
          );
        }
      }
      let pageBytes = 0;
      const state = restoreSha256State(
        JSON.parse(session.finalize_sha_state ?? "null")
      );
      const encoder = new TextEncoder();
      for (const row of verified) {
        pageBytes += row.chunk_size;
        updateSha256(state, encoder.encode(row.chunk_hash));
      }
      const serializedState = JSON.stringify(serializeSha256State(state));
      const completedPhase = needsOldManifestPreparation(context)
        ? "preparing"
        : "publishing";
      transactionSync(durableObject, () => {
        if (context.versioning) {
          if (context.versionId === null) {
            throw new VFSError(
              "EINVAL",
              "finalizeMultipartStep: missing version id"
            );
          }
          durableObject.sql.exec(
            `INSERT INTO version_chunks
               (version_id, chunk_index, chunk_hash, chunk_size, shard_index)
             SELECT ?, chunk_index, chunk_hash, chunk_size, shard_index
               FROM upload_verified_chunks
              WHERE upload_id = ? AND chunk_index >= ? AND chunk_index < ?
              ORDER BY chunk_index`,
            context.versionId,
            uploadId,
            start,
            end
          );
        } else {
          durableObject.sql.exec(
            `INSERT INTO file_chunks
               (file_id, chunk_index, chunk_hash, chunk_size, shard_index)
             SELECT ?, chunk_index, chunk_hash, chunk_size, shard_index
               FROM upload_verified_chunks
              WHERE upload_id = ? AND chunk_index >= ? AND chunk_index < ?
              ORDER BY chunk_index`,
            uploadId,
            uploadId,
            start,
            end
          );
        }
        for (const shardIndex of new Set(
          verified.map((row) => row.shard_index)
        )) {
          durableObject.sql.exec(
            `INSERT OR IGNORE INTO upload_cleanup_routes
               (upload_id, cleanup_kind, shard_index) VALUES (?, ?, ?)`,
            uploadId,
            ChunkCleanupKind.MultipartStaging,
            shardIndex
          );
        }
        durableObject.sql.exec(
          `UPDATE upload_sessions
              SET finalize_chunk_cursor = ?, finalize_verify_shard_cursor = 0,
                  finalize_total_size = finalize_total_size + ?,
                  finalize_sha_state = ?,
                  finalize_phase = CASE WHEN ? >= total_chunks THEN ? ELSE 'verifying' END
            WHERE upload_id = ? AND user_id = ? AND status = 'finalizing'
              AND finalize_phase = 'verifying' AND finalize_chunk_cursor = ?
              AND finalize_verify_shard_cursor = ?`,
          end,
          pageBytes,
          serializedState,
          end,
          completedPhase,
          uploadId,
          userId,
          start,
          session.pool_size
        );
        if (lastSqlChanges(durableObject) !== 1) {
          throw new VFSError(
            "EBUSY",
            "finalizeMultipartStep: chunk cursor changed"
          );
        }
      });
      return {
        done: false,
        phase: end >= session.total_chunks ? completedPhase : "verifying",
        cursor: end,
        total: session.total_chunks,
      };
    } catch (error) {
      if (isDeterministicFinalizeError(error)) {
        await abortFinalizingMultipart(durableObject, scope, session);
      }
      throw error;
    }
  }
  if (session.finalize_phase === "preparing") {
    try {
      return prepareMultipartOverwrite(durableObject, session, context);
    } catch (error) {
      if (isDeterministicFinalizeError(error)) {
        await abortFinalizingMultipart(durableObject, scope, session);
      }
      throw error;
    }
  }
  if (session.finalize_phase === "publishing") {
    try {
      return await publishMultipart(durableObject, session);
    } catch (error) {
      if (isDeterministicFinalizeError(error)) {
        await abortFinalizingMultipart(durableObject, scope, session);
      }
      throw error;
    }
  }
  if (session.finalize_phase !== "fencing") {
    throw new VFSError(
      "EBUSY",
      `finalizeMultipartStep: invalid phase='${session.finalize_phase ?? "null"}'`
    );
  }

  const start = session.finalize_fence_cursor;
  const end = Math.min(start + MULTIPART_FENCE_PAGE_SIZE, session.pool_size);
  if (session.fence_id !== null) {
    const ns = shardNs(durableObject);
    await Promise.all(
      Array.from({ length: end - start }, async (_, offset) => {
        const shardIndex = start + offset;
        const shardName = vfsShardDOName(scope.ns, scope.tenant, scope.sub, shardIndex);
        await ns
          .get(ns.idFromName(shardName))
          .fenceMultipart(uploadId, session!.fence_id!, "finalizing", session!.expires_at);
      })
    );
  }
  transactionSync(durableObject, () => {
    durableObject.sql.exec(
      `UPDATE upload_sessions
          SET finalize_fence_cursor = ?,
              finalize_phase = CASE WHEN ? >= pool_size THEN 'verifying' ELSE 'fencing' END
        WHERE upload_id = ? AND user_id = ? AND status = 'finalizing'
          AND finalize_phase = 'fencing' AND finalize_fence_cursor = ?`,
      end,
      end,
      uploadId,
      userId,
      start
    );
    if (lastSqlChanges(durableObject) !== 1) {
      throw new VFSError("EBUSY", "finalizeMultipartStep: fence cursor changed");
    }
  });
  return {
    done: false,
    phase: end >= session.pool_size ? "verifying" : "fencing",
    cursor: end >= session.pool_size ? 0 : end,
    total: end >= session.pool_size ? session.total_chunks : session.pool_size,
  };
}

function needsOldManifestPreparation(context: MultipartFinalizeContext): boolean {
  return !context.versioning && context.expectedDestination !== null;
}

function prepareMultipartOverwrite(
  durableObject: UserDO,
  session: UploadSessionRow,
  context: MultipartFinalizeContext
): MultipartFinalizeProgress {
  if (!needsOldManifestPreparation(context)) {
    durableObject.sql.exec(
      `UPDATE upload_sessions SET finalize_phase = 'publishing'
        WHERE upload_id = ? AND user_id = ? AND status = 'finalizing'
          AND finalize_phase = 'preparing'`,
      session.upload_id,
      session.user_id
    );
    if (lastSqlChanges(durableObject) !== 1) {
      throw new VFSError("EBUSY", "finalizeMultipartStep: prepare phase changed");
    }
    return {
      done: false,
      phase: "publishing",
      cursor: session.total_chunks,
      total: session.total_chunks,
    };
  }
  if (context.expectedDestination === null) {
    throw new VFSError("EBUSY", "finalizeMultipartStep: missing destination");
  }

  const oldFileId = context.expectedDestination.fileId;
  const rows = durableObject.sql
    .exec(
      `SELECT chunk_index, shard_index FROM file_chunks
        WHERE file_id = ? AND chunk_index > ?
        ORDER BY chunk_index LIMIT ?`,
      oldFileId,
      session.finalize_old_manifest_cursor,
      MULTIPART_HASH_PAGE_SIZE + 1
    )
    .toArray() as Array<{ chunk_index: number; shard_index: number }>;
  const page = rows.slice(0, MULTIPART_HASH_PAGE_SIZE);
  const hasMore = rows.length > MULTIPART_HASH_PAGE_SIZE;
  const cursor = page.at(-1)?.chunk_index ?? session.finalize_old_manifest_cursor;
  transactionSync(durableObject, () => {
    for (const shardIndex of new Set(page.map((row) => row.shard_index))) {
      durableObject.sql.exec(
        `INSERT OR IGNORE INTO upload_cleanup_routes
           (upload_id, cleanup_kind, shard_index) VALUES (?, ?, ?)`,
        session.upload_id,
        ChunkCleanupKind.Chunks,
        shardIndex
      );
    }
    durableObject.sql.exec(
      `UPDATE upload_sessions
          SET finalize_old_manifest_cursor = ?, finalize_phase = ?
        WHERE upload_id = ? AND user_id = ? AND status = 'finalizing'
          AND finalize_phase = 'preparing'
          AND finalize_old_manifest_cursor = ?`,
      cursor,
      hasMore ? "preparing" : "publishing",
      session.upload_id,
      session.user_id,
      session.finalize_old_manifest_cursor
    );
    if (lastSqlChanges(durableObject) !== 1) {
      throw new VFSError("EBUSY", "finalizeMultipartStep: prepare cursor changed");
    }
  });
  return {
    done: false,
    phase: hasMore ? "preparing" : "publishing",
    cursor: Math.max(cursor, 0),
    total: Math.max(cursor + (hasMore ? 1 : 0), 0),
  };
}

function freezeMultipartFinalizeContext(
  durableObject: UserDO,
  userId: string,
  uploadId: string
): void {
  const candidateVersionId = generateId();
  transactionSync(durableObject, () => {
    const session = durableObject.sql
      .exec(
        "SELECT * FROM upload_sessions WHERE upload_id = ? AND user_id = ?",
        uploadId,
        userId
      )
      .toArray()[0] as unknown as UploadSessionRow | undefined;
    if (!session) {
      throw new VFSError("ENOENT", "finalizeMultipartStep: session not found");
    }
    if (session.status !== "open") {
      throw new VFSError(
        "EBUSY",
        `finalizeMultipartStep: session status='${session.status}'`
      );
    }
    if (session.expires_at < Date.now()) {
      throw new VFSError(
        "EBUSY",
        `finalizeMultipartStep: session expired at ${session.expires_at}`
      );
    }
    if (session.staged_hash_cursor !== session.total_chunks) {
      throw new VFSError(
        "EINVAL",
        `finalizeMultipartStep: staged ${session.staged_hash_cursor}/${session.total_chunks} expected hashes`
      );
    }
    const temp = durableObject.sql
      .exec(
        `SELECT 1 FROM files
          WHERE file_id = ? AND user_id = ? AND status = 'uploading'
            AND IFNULL(parent_id, '') = IFNULL(?, '')`,
        uploadId,
        userId,
        session.parent_id
      )
      .toArray();
    if (temp.length !== 1) {
      throw new VFSError("EBUSY", "finalizeMultipartStep: temporary file changed");
    }

    const encryption = sessionEncryption(session);
    enforceModeMonotonic(
      durableObject,
      userId,
      session.parent_id,
      session.leaf,
      encryption ?? undefined
    );
    const versioning = isVersioningEnabled(durableObject, userId);
    const destination = durableObject.sql
      .exec(
        `SELECT file_id, head_version_id FROM files
          WHERE user_id = ? AND IFNULL(parent_id, '') = IFNULL(?, '')
            AND file_name = ? AND status = 'complete'`,
        userId,
        session.parent_id,
        session.leaf
      )
      .toArray()[0] as
      | {
          file_id: string;
          head_version_id: string | null;
        }
      | undefined;
    const metadata =
      session.metadata_blob === null || session.metadata_blob.byteLength === 0
          ? null
          : new Uint8Array(session.metadata_blob);
    const tags = session.tags_json === null
      ? []
      : (JSON.parse(session.tags_json) as string[]);
    const context: MultipartFinalizeContext = {
      schema: 1,
      versioning,
      pathId: versioning && destination ? destination.file_id : uploadId,
      versionId: versioning ? candidateVersionId : null,
      expectedDestination: destination
        ? {
            fileId: destination.file_id,
            headVersionId: destination.head_version_id,
          }
        : null,
      committedAt: Date.now(),
      metadataPresent: session.metadata_blob !== null,
      metadataBase64: metadata === null ? null : encodeBase64(metadata),
      tagsPresent: session.tags_json !== null,
      tags,
    };
    durableObject.sql.exec(
      `UPDATE upload_sessions
          SET status = 'finalizing', finalize_phase = 'fencing',
              finalize_fence_cursor = 0, finalize_chunk_cursor = 0,
              finalize_verify_shard_cursor = 0,
              finalize_total_size = 0, finalize_sha_state = ?,
              finalize_context = ?, finalize_old_manifest_cursor = -1,
              finalize_intent_cursor = -1,
              finalize_old_intent_cursor = -1,
              finalize_old_cleanup_cursor = -1
        WHERE upload_id = ? AND user_id = ? AND status = 'open'`,
      JSON.stringify(serializeSha256State(createSha256State())),
      JSON.stringify(context),
      uploadId,
      userId
    );
    if (lastSqlChanges(durableObject) !== 1) {
      throw new VFSError("EBUSY", "finalizeMultipartStep: session changed");
    }
  });
}

async function publishMultipart(
  durableObject: UserDO,
  session: UploadSessionRow
): Promise<MultipartFinalizeProgress> {
  const context = parseFinalizeContext(session.finalize_context);
  const metadata =
    context.metadataBase64 === null ? null : decodeBase64(context.metadataBase64);
  const tagsJson = JSON.stringify(context.tags);
  const userId = session.user_id;
  const now = context.committedAt;
  const terminalAt = Date.now();
  const encryption = sessionEncryption(session);

  await scheduleStaleUploadSweep(durableObject);
  const result = finalizedMultipartResult(
    session,
    context,
    reconstructFinalizedPath(
      durableObject,
      userId,
      session.parent_id,
      session.leaf
    )
  );
  transactionSync(durableObject, () => {
    const current = durableObject.sql
      .exec(
        `SELECT status, finalize_phase, finalize_context, finalize_chunk_cursor
           FROM upload_sessions WHERE upload_id = ? AND user_id = ?`,
        session.upload_id,
        userId
      )
      .toArray()[0] as
      | {
          status: string;
          finalize_phase: string | null;
          finalize_context: string | null;
          finalize_chunk_cursor: number;
        }
      | undefined;
    if (
      !current ||
      current.status !== "finalizing" ||
      current.finalize_phase !== "publishing" ||
      current.finalize_context !== session.finalize_context ||
      current.finalize_chunk_cursor !== session.total_chunks
    ) {
      throw new VFSError("EBUSY", "finalizeMultipartStep: session changed");
    }
    const temp = durableObject.sql
      .exec(
        `SELECT 1 FROM files
          WHERE file_id = ? AND user_id = ? AND status = 'uploading'
            AND IFNULL(parent_id, '') = IFNULL(?, '')`,
        session.upload_id,
        userId,
        session.parent_id
      )
      .toArray();
    if (temp.length !== 1) {
      throw new VFSError("EBUSY", "finalizeMultipartStep: temporary file changed");
    }

    const live = durableObject.sql
      .exec(
        `SELECT file_id, head_version_id, file_size, inline_data, metadata FROM files
          WHERE user_id = ? AND IFNULL(parent_id, '') = IFNULL(?, '')
            AND file_name = ? AND status = 'complete'`,
        userId,
        session.parent_id,
        session.leaf
      )
      .toArray()[0] as
      | {
          file_id: string;
          head_version_id: string | null;
          file_size: number;
          inline_data: ArrayBuffer | null;
          metadata: ArrayBuffer | null;
        }
      | undefined;
    const destinationChanged =
      context.expectedDestination === null
        ? live !== undefined
        : live?.file_id !== context.expectedDestination.fileId ||
          live?.head_version_id !== context.expectedDestination.headVersionId;
    if (destinationChanged) {
      throw new VFSError("EBUSY", "finalizeMultipartStep: destination changed");
    }
    enforceModeMonotonic(
      durableObject,
      userId,
      session.parent_id,
      session.leaf,
      encryption ?? undefined
    );

    if (context.versioning) {
      if (context.versionId === null) {
        throw new VFSError("EINVAL", "finalizeMultipartStep: missing version id");
      }
      if (context.expectedDestination === null) {
        durableObject.sql.exec(
          `UPDATE files
              SET file_name = ?, file_size = ?, file_hash = ?, chunk_count = ?,
                  status = 'complete', updated_at = ?, metadata = ?,
                  encryption_mode = ?, encryption_key_id = ?
            WHERE file_id = ? AND user_id = ? AND status = 'uploading'
              AND IFNULL(parent_id, '') = IFNULL(?, '')`,
          session.leaf,
          result.size,
          result.fileHash,
          session.total_chunks,
          now,
          context.metadataPresent ? metadata : null,
          encryption?.mode ?? null,
          encryption?.keyId ?? null,
          session.upload_id,
          userId,
          session.parent_id
        );
        if (lastSqlChanges(durableObject) !== 1) {
          throw new VFSError("EBUSY", "finalizeMultipartStep: temporary file changed");
        }
      } else if (context.metadataPresent) {
        durableObject.sql.exec(
          "UPDATE files SET metadata = ? WHERE file_id = ?",
          metadata,
          context.pathId
        );
      }
      if (context.tagsPresent) {
        durableObject.sql.exec(
          "DELETE FROM file_tags WHERE path_id = ?",
          context.pathId
        );
        durableObject.sql.exec(
          `INSERT INTO file_tags (path_id, tag, user_id, mtime_ms)
           SELECT ?, CAST(value AS TEXT), ?, ? FROM json_each(?)`,
          context.pathId,
          userId,
          now,
          tagsJson
        );
      }
      const expectation: VersionedFileExpectation = {
        fileId: context.pathId,
        userId,
        parentId: session.parent_id,
        fileName: session.leaf,
        headVersionId: context.expectedDestination?.headVersionId ?? null,
      };
      commitVersionChecked(
        durableObject,
        {
          pathId: context.pathId,
          versionId: context.versionId,
          userId,
          size: result.size,
          mode: session.mode,
          mtimeMs: now,
          chunkSize: session.chunk_size,
          chunkCount: session.total_chunks,
          fileHash: result.fileHash,
          mimeType: session.mime_type,
          inlineData: null,
          userVisible: session.version_user_visible !== 0,
          label: session.version_label,
          metadata: context.metadataPresent
            ? metadata
            : live?.metadata
              ? new Uint8Array(live.metadata)
              : null,
          shardRefId: session.upload_id,
          encryption: encryption ?? undefined,
        },
        expectation,
        "finalizeMultipartStep"
      );
      if (context.expectedDestination !== null) {
        durableObject.sql.exec(
          "DELETE FROM file_chunks WHERE file_id = ?",
          session.upload_id
        );
        durableObject.sql.exec(
          "DELETE FROM file_tags WHERE path_id = ?",
          session.upload_id
        );
        durableObject.sql.exec(
          "DELETE FROM files WHERE file_id = ? AND status = 'uploading'",
          session.upload_id
        );
      }
    } else {
      if (live !== undefined) {
        if (!context.tagsPresent) {
          durableObject.sql.exec(
            "DELETE FROM file_tags WHERE path_id = ?",
            session.upload_id
          );
          durableObject.sql.exec(
            `INSERT OR IGNORE INTO file_tags (path_id, tag, user_id, mtime_ms)
             SELECT ?, tag, user_id, ? FROM file_tags WHERE path_id = ?`,
            session.upload_id,
            now,
            live.file_id
          );
        }
        durableObject.sql.exec(
          "DELETE FROM file_tags WHERE path_id = ?",
          live.file_id
        );
        durableObject.sql.exec(
          "DELETE FROM write_stream_sessions WHERE tmp_id = ?",
          live.file_id
        );
        durableObject.sql.exec("DELETE FROM files WHERE file_id = ?", live.file_id);
        recordWriteUsage(
          durableObject,
          userId,
          -live.file_size,
          -1,
          live.inline_data === null ? 0 : -live.inline_data.byteLength
        );
      }
      durableObject.sql.exec(
        `UPDATE files
            SET file_name = ?, file_size = ?, file_hash = ?, chunk_count = ?,
                status = 'complete', updated_at = ?, metadata = ?,
                encryption_mode = ?, encryption_key_id = ?
          WHERE file_id = ? AND user_id = ? AND status = 'uploading'
            AND IFNULL(parent_id, '') = IFNULL(?, '')`,
        session.leaf,
        result.size,
        result.fileHash,
        session.total_chunks,
        now,
        context.metadataPresent
          ? metadata
          : live?.metadata
            ? new Uint8Array(live.metadata)
            : null,
        encryption?.mode ?? null,
        encryption?.keyId ?? null,
        session.upload_id,
        userId,
        session.parent_id
      );
      if (lastSqlChanges(durableObject) !== 1) {
        throw new VFSError("EBUSY", "finalizeMultipartStep: temporary file changed");
      }
      if (context.tagsPresent) {
        durableObject.sql.exec(
          "DELETE FROM file_tags WHERE path_id = ?",
          session.upload_id
        );
        durableObject.sql.exec(
          `INSERT INTO file_tags (path_id, tag, user_id, mtime_ms)
           SELECT ?, CAST(value AS TEXT), ?, ? FROM json_each(?)`,
          session.upload_id,
          userId,
          now,
          tagsJson
        );
      }
      recordWriteUsage(durableObject, userId, result.size, 1);
    }

    const cleanupPhase = firstFinalizedCleanupPhase(session, context);
    bumpFolderRevision(durableObject, userId, session.parent_id);
    durableObject.sql.exec(
       `UPDATE upload_sessions
           SET status = 'finalized',
               finalize_phase = ?, finalize_cleanup_cursor = 0,
              finalize_intent_cursor = -1,
              finalize_old_intent_cursor = -1,
              finalize_old_cleanup_cursor = -1,
               finalize_result = ?, terminal_at = ?
        WHERE upload_id = ? AND user_id = ? AND status = 'finalizing'
          AND finalize_phase = 'publishing' AND finalize_context = ?`,
      cleanupPhase,
      JSON.stringify(result),
      terminalAt,
      session.upload_id,
      userId,
      session.finalize_context
    );
    if (lastSqlChanges(durableObject) !== 1) {
      throw new VFSError("EBUSY", "finalizeMultipartStep: session changed");
    }
  });

  const cleanupPhase = firstFinalizedCleanupPhase(session, context);
  if (cleanupPhase === "done") {
    compactTerminalMultipartSession(durableObject, session.upload_id);
  }
  return cleanupPhase === "done"
    ? { done: true, result, fresh: true }
    : {
        done: false,
        phase: "cleaning",
        cursor: 0,
        total: Math.max(session.total_chunks, 1),
      };
}

function compactTerminalMultipartSession(
  durableObject: UserDO,
  uploadId: string
): void {
  durableObject.sql.exec(
    `UPDATE upload_sessions
        SET metadata_blob = NULL, tags_json = NULL,
            finalize_context = NULL, finalize_sha_state = NULL
      WHERE upload_id = ?
        AND ((status = 'finalized' AND finalize_phase = 'done'
              AND finalize_result IS NOT NULL)
          OR (status = 'aborted' AND abort_phase = 'done'))`,
    uploadId
  );
}

function firstFinalizedCleanupPhase(
  session: UploadSessionRow,
  context: MultipartFinalizeContext
): string {
  if (session.total_chunks > 0) return "cleaning_upload_intents";
  if (needsOldManifestPreparation(context)) return "cleaning_old_intents";
  return "done";
}

function finalizedMultipartResult(
  session: UploadSessionRow,
  context: MultipartFinalizeContext,
  path: string
): MultipartFinalizeResponse {
  if (session.finalize_sha_state === null) {
    throw new VFSError("EBUSY", "finalizeMultipartStep: missing hash state");
  }
  const state = restoreSha256State(JSON.parse(session.finalize_sha_state));
  return {
    fileId: context.pathId,
    versionId: context.versionId ?? "",
    size: session.finalize_total_size,
    chunkCount: session.total_chunks,
    fileHash: bytesToHex(digestSha256(state)),
    path,
    mimeType: session.mime_type,
    isEncrypted: session.encryption_mode !== null,
  };
}

async function cleanupFinalizedMultipart(
  durableObject: UserDO,
  scope: VFSScope,
  session: UploadSessionRow
): Promise<MultipartFinalizeProgress> {
  const result =
    session.finalize_result === null
      ? legacyFinalizedMultipartResult(durableObject, session)
      : parseFinalizeResult(session.finalize_result);
  if (session.finalize_phase === null) {
    durableObject.sql.exec(
      `UPDATE upload_sessions
          SET finalize_phase = 'done', finalize_cleanup_cursor = total_chunks
        WHERE upload_id = ? AND status = 'finalized' AND finalize_phase IS NULL`,
      session.upload_id
    );
    compactTerminalMultipartSession(durableObject, session.upload_id);
    return { done: true, result, fresh: false };
  }
  if (session.finalize_phase === "done") {
    compactTerminalMultipartSession(durableObject, session.upload_id);
    return { done: true, result, fresh: false };
  }
  const context = parseFinalizeContext(session.finalize_context);
  if (session.finalize_phase === "cleaning_upload_intents") {
    const rows = durableObject.sql
      .exec(
        `SELECT shard_index FROM upload_cleanup_routes
          WHERE upload_id = ? AND cleanup_kind = ? AND shard_index > ?
          ORDER BY shard_index LIMIT ?`,
        session.upload_id,
        ChunkCleanupKind.MultipartStaging,
        session.finalize_intent_cursor,
        MULTIPART_FENCE_PAGE_SIZE + 1
      )
      .toArray() as Array<{ shard_index: number }>;
    const page = rows.slice(0, MULTIPART_FENCE_PAGE_SIZE);
    const hasMore = rows.length > MULTIPART_FENCE_PAGE_SIZE;
    const cursor = page.at(-1)?.shard_index ?? session.finalize_intent_cursor;
    const nextPhase = hasMore
      ? "cleaning_upload_intents"
      : needsOldManifestPreparation(context)
        ? "cleaning_old_intents"
        : "cleaning";
    const now = Date.now();
    transactionSync(durableObject, () => {
      for (const row of page) {
        stageChunkCleanupIntent(
          durableObject,
          session.upload_id,
          row.shard_index,
          now,
          now,
          ChunkCleanupKind.MultipartStaging
        );
        durableObject.sql.exec(
          `DELETE FROM upload_cleanup_routes
            WHERE upload_id = ? AND cleanup_kind = ? AND shard_index = ?`,
          session.upload_id,
          ChunkCleanupKind.MultipartStaging,
          row.shard_index
        );
      }
      updateFinalizedCleanupPhase(
        durableObject,
        session,
        "cleaning_upload_intents",
        nextPhase,
        "finalize_intent_cursor",
        cursor
      );
    });
    if (page.length > 0) {
      await drainChunkCleanupIntents(durableObject, scope, session.upload_id);
    }
    return {
      done: false,
      phase: "cleaning",
      cursor: Math.max(cursor, 0),
      total: session.pool_size,
    };
  }
  if (session.finalize_phase === "cleaning_old_intents") {
    if (!needsOldManifestPreparation(context)) {
    throw new MultipartLocalCorruptionError(
      "finalizeMultipartStep: old cleanup has no frozen destination"
    );
    }
    if (context.expectedDestination === null) {
      throw new MultipartLocalCorruptionError(
        "finalizeMultipartStep: missing destination"
      );
    }
    const oldFileId = context.expectedDestination.fileId;
    const rows = durableObject.sql
      .exec(
        `SELECT shard_index FROM upload_cleanup_routes
          WHERE upload_id = ? AND cleanup_kind = ? AND shard_index > ?
          ORDER BY shard_index LIMIT ?`,
        session.upload_id,
        ChunkCleanupKind.Chunks,
        session.finalize_old_intent_cursor,
        MULTIPART_FENCE_PAGE_SIZE + 1
      )
      .toArray() as Array<{ shard_index: number }>;
    const page = rows.slice(0, MULTIPART_FENCE_PAGE_SIZE);
    const hasMore = rows.length > MULTIPART_FENCE_PAGE_SIZE;
    const cursor = page.at(-1)?.shard_index ?? session.finalize_old_intent_cursor;
    const now = Date.now();
    transactionSync(durableObject, () => {
      for (const row of page) {
        stageChunkCleanupIntent(
          durableObject,
          oldFileId,
          row.shard_index,
          now,
          now,
          ChunkCleanupKind.Chunks
        );
        durableObject.sql.exec(
          `DELETE FROM upload_cleanup_routes
            WHERE upload_id = ? AND cleanup_kind = ? AND shard_index = ?`,
          session.upload_id,
          ChunkCleanupKind.Chunks,
          row.shard_index
        );
      }
      updateFinalizedCleanupPhase(
        durableObject,
        session,
        "cleaning_old_intents",
        hasMore ? "cleaning_old_intents" : "cleaning_old_manifest",
        "finalize_old_intent_cursor",
        cursor
      );
    });
    if (page.length > 0) {
      await drainChunkCleanupIntents(durableObject, scope, oldFileId);
    }
    return {
      done: false,
      phase: "cleaning",
      cursor: Math.max(cursor, 0),
      total: session.pool_size,
    };
  }
  if (session.finalize_phase === "cleaning_old_manifest") {
    if (!needsOldManifestPreparation(context)) {
      throw new VFSError(
        "EBUSY",
        "finalizeMultipartStep: old manifest cleanup has no frozen destination"
      );
    }
    if (context.expectedDestination === null) {
      throw new VFSError("EBUSY", "finalizeMultipartStep: missing destination");
    }
    const oldFileId = context.expectedDestination.fileId;
    const rows = durableObject.sql
      .exec(
        `SELECT chunk_index FROM file_chunks
          WHERE file_id = ? AND chunk_index > ?
          ORDER BY chunk_index LIMIT ?`,
        oldFileId,
        session.finalize_old_cleanup_cursor,
        MULTIPART_HASH_PAGE_SIZE + 1
      )
      .toArray() as Array<{ chunk_index: number }>;
    const page = rows.slice(0, MULTIPART_HASH_PAGE_SIZE);
    const hasMore = rows.length > MULTIPART_HASH_PAGE_SIZE;
    const cursor = page.at(-1)?.chunk_index ?? session.finalize_old_cleanup_cursor;
    const nextPhase = hasMore
      ? "cleaning_old_manifest"
      : session.total_chunks > 0
        ? "cleaning"
        : "done";
    transactionSync(durableObject, () => {
      if (page.length > 0) {
        durableObject.sql.exec(
          `DELETE FROM file_chunks
            WHERE file_id = ? AND chunk_index > ? AND chunk_index <= ?`,
          oldFileId,
          session.finalize_old_cleanup_cursor,
          cursor
        );
      }
      updateFinalizedCleanupPhase(
        durableObject,
        session,
        "cleaning_old_manifest",
        nextPhase,
        "finalize_old_cleanup_cursor",
        cursor
      );
    });
    if (nextPhase === "done") {
      compactTerminalMultipartSession(durableObject, session.upload_id);
      return { done: true, result, fresh: true };
    }
    return {
      done: false,
      phase: "cleaning",
      cursor: Math.max(cursor, 0),
      total: Math.max(cursor + (hasMore ? 1 : 0), 1),
    };
  }
  if (session.finalize_phase !== "cleaning") {
    throw new MultipartLocalCorruptionError(
      `finalizeMultipartStep: invalid finalized phase='${session.finalize_phase ?? "null"}'`
    );
  }
  const start = session.finalize_cleanup_cursor;
  const end = Math.min(start + MULTIPART_HASH_PAGE_SIZE, session.total_chunks);
  transactionSync(durableObject, () => {
    durableObject.sql.exec(
      `DELETE FROM upload_expected_chunks
        WHERE upload_id = ? AND chunk_index >= ? AND chunk_index < ?`,
      session.upload_id,
      start,
      end
    );
    durableObject.sql.exec(
      `DELETE FROM upload_verified_chunks
        WHERE upload_id = ? AND chunk_index >= ? AND chunk_index < ?`,
      session.upload_id,
      start,
      end
    );
    durableObject.sql.exec(
      `UPDATE upload_sessions
          SET finalize_cleanup_cursor = ?,
              finalize_phase = CASE WHEN ? >= total_chunks THEN 'done' ELSE 'cleaning' END,
              metadata_blob = CASE WHEN ? >= total_chunks THEN NULL ELSE metadata_blob END,
              tags_json = CASE WHEN ? >= total_chunks THEN NULL ELSE tags_json END,
              finalize_context = CASE WHEN ? >= total_chunks THEN NULL ELSE finalize_context END,
              finalize_sha_state = CASE WHEN ? >= total_chunks THEN NULL ELSE finalize_sha_state END
        WHERE upload_id = ? AND status = 'finalized'
          AND finalize_phase = 'cleaning' AND finalize_cleanup_cursor = ?`,
      end,
      end,
      end,
      end,
      end,
      end,
      session.upload_id,
      start
    );
    if (lastSqlChanges(durableObject) !== 1) {
      throw new VFSError("EBUSY", "finalizeMultipartStep: cleanup cursor changed");
    }
  });
  if (end >= session.total_chunks) {
    return { done: true, result, fresh: true };
  }
  return { done: false, phase: "cleaning", cursor: end, total: session.total_chunks };
}

function updateFinalizedCleanupPhase(
  durableObject: UserDO,
  session: UploadSessionRow,
  expectedPhase: string,
  nextPhase: string,
  cursorColumn:
    | "finalize_intent_cursor"
    | "finalize_old_intent_cursor"
    | "finalize_old_cleanup_cursor",
  cursor: number
): void {
  durableObject.sql.exec(
    `UPDATE upload_sessions SET ${cursorColumn} = ?, finalize_phase = ?
      WHERE upload_id = ? AND user_id = ? AND status = 'finalized'
        AND finalize_phase = ? AND ${cursorColumn} = ?`,
    cursor,
    nextPhase,
    session.upload_id,
    session.user_id,
    expectedPhase,
    session[cursorColumn]
  );
  if (lastSqlChanges(durableObject) !== 1) {
    throw new VFSError("EBUSY", "finalizeMultipartStep: cleanup cursor changed");
  }
}

function legacyFinalizedMultipartResult(
  durableObject: UserDO,
  session: UploadSessionRow
): MultipartFinalizeResponse {
  let result: MultipartFinalizeResponse | undefined;
  if (
    session.finalize_context !== null &&
    session.finalize_sha_state !== null
  ) {
    const context = parseFinalizeContext(session.finalize_context);
    if (context.legacyPath !== undefined) {
      result = finalizedMultipartResult(session, context, context.legacyPath);
    }
  }
  const path = reconstructFinalizedPath(
    durableObject,
    session.user_id,
    session.parent_id,
    session.leaf
  );
  if (result === undefined) {
    const versions = durableObject.sql
      .exec(
        `SELECT path_id, version_id, size, chunk_count, file_hash, mime_type,
                encryption_mode
           FROM file_versions
          WHERE user_id = ? AND shard_ref_id = ?
          LIMIT 2`,
        session.user_id,
        session.upload_id
      )
      .toArray() as Array<{
        path_id: string;
        version_id: string;
        size: number;
        chunk_count: number;
        file_hash: string;
        mime_type: string;
        encryption_mode: string | null;
      }>;
    if (versions.length > 1) {
      throw new VFSError(
        "EBUSY",
        "finalizeMultipartStep: ambiguous pre-upgrade terminal result"
      );
    }
    const version = versions[0];
    if (version !== undefined) {
      result = {
        fileId: version.path_id,
        versionId: version.version_id,
        size: version.size,
        chunkCount: version.chunk_count,
        fileHash: version.file_hash,
        path,
        mimeType: version.mime_type,
        isEncrypted: version.encryption_mode !== null,
      };
    }
  }
  if (result === undefined) {
    const file = durableObject.sql
      .exec(
        `SELECT file_id, file_size, chunk_count, file_hash, mime_type,
                encryption_mode
           FROM files
          WHERE file_id = ? AND user_id = ? AND status = 'complete'`,
        session.upload_id,
        session.user_id
      )
      .toArray()[0] as
      | {
          file_id: string;
          file_size: number;
          chunk_count: number;
          file_hash: string;
          mime_type: string;
          encryption_mode: string | null;
        }
      | undefined;
    if (file !== undefined) {
      result = {
        fileId: file.file_id,
        versionId: "",
        size: file.file_size,
        chunkCount: file.chunk_count,
        fileHash: file.file_hash,
        path,
        mimeType: file.mime_type,
        isEncrypted: file.encryption_mode !== null,
      };
    }
  }
  if (result === undefined) {
    throw new VFSError(
      "EBUSY",
      "finalizeMultipartStep: cannot reconstruct pre-upgrade terminal result"
    );
  }
  result = parseFinalizeResult(JSON.stringify(result));
  durableObject.sql.exec(
    `UPDATE upload_sessions SET finalize_result = ?
      WHERE upload_id = ? AND user_id = ? AND status = 'finalized'
        AND finalize_result IS NULL`,
    JSON.stringify(result),
    session.upload_id,
    session.user_id
  );
  return result;
}

function parseFinalizeResult(value: string | null): MultipartFinalizeResponse {
  if (value === null) {
    throw new VFSError("EBUSY", "finalizeMultipartStep: missing terminal result");
  }
  let result: MultipartFinalizeResponse;
  try {
    result = JSON.parse(value) as MultipartFinalizeResponse;
  } catch {
    throw new VFSError("EBUSY", "finalizeMultipartStep: invalid terminal result");
  }
  if (
    typeof result.fileId !== "string" ||
    typeof result.versionId !== "string" ||
    !Number.isSafeInteger(result.size) ||
    !Number.isSafeInteger(result.chunkCount) ||
    !/^[0-9a-f]{64}$/.test(result.fileHash) ||
    typeof result.path !== "string" ||
    typeof result.mimeType !== "string" ||
    typeof result.isEncrypted !== "boolean"
  ) {
    throw new VFSError("EBUSY", "finalizeMultipartStep: invalid terminal result");
  }
  return result;
}

function isDeterministicFinalizeError(error: unknown): boolean {
  return (
    error instanceof VFSError &&
    (error.code === "ENOENT" ||
      error.code === "EBADF" ||
      error.code === "EINVAL" ||
      (error.code === "EBUSY" && error.message.includes("destination changed")))
  );
}

async function abortFinalizingMultipart(
  durableObject: UserDO,
  scope: VFSScope,
  session: UploadSessionRow
): Promise<void> {
  await requestMultipartAbort(durableObject, scope, session.upload_id, true);
}

function parseFinalizeContext(value: string | null): MultipartFinalizeContext {
  if (value === null) {
    throw new MultipartLocalCorruptionError(
      "finalizeMultipartStep: missing frozen context"
    );
  }
  let context: MultipartFinalizeContext;
  try {
    const parsed = JSON.parse(value) as MultipartFinalizeContext & {
      path?: string;
    };
    context = {
      ...parsed,
      metadataPresent: parsed.metadataPresent ?? true,
      tagsPresent: parsed.tagsPresent ?? true,
      ...(typeof parsed.path === "string" ? { legacyPath: parsed.path } : {}),
    };
  } catch {
    throw new MultipartLocalCorruptionError(
      "finalizeMultipartStep: invalid frozen context"
    );
  }
  if (
    context.schema !== 1 ||
    typeof context.versioning !== "boolean" ||
    typeof context.pathId !== "string" ||
    (context.versionId !== null && typeof context.versionId !== "string") ||
    (context.expectedDestination !== null &&
      (typeof context.expectedDestination !== "object" ||
        typeof context.expectedDestination.fileId !== "string" ||
        (context.expectedDestination.headVersionId !== null &&
          typeof context.expectedDestination.headVersionId !== "string"))) ||
    !Number.isSafeInteger(context.committedAt) ||
    typeof context.metadataPresent !== "boolean" ||
    (context.metadataBase64 !== null &&
      typeof context.metadataBase64 !== "string") ||
    !Array.isArray(context.tags) ||
    typeof context.tagsPresent !== "boolean" ||
    context.tags.some((tag) => typeof tag !== "string")
  ) {
    throw new MultipartLocalCorruptionError(
      "finalizeMultipartStep: invalid frozen context"
    );
  }
  return context;
}

function sessionEncryption(
  session: UploadSessionRow
): EncryptionStampOpts | null {
  if (session.encryption_mode === null) return null;
  return {
    mode: session.encryption_mode as "convergent" | "random",
    ...(session.encryption_key_id === null
      ? {}
      : { keyId: session.encryption_key_id }),
  };
}

function encodeBase64(bytes: Uint8Array): string {
  let binary = "";
  for (let offset = 0; offset < bytes.byteLength; offset += 8_192) {
    binary += String.fromCharCode(...bytes.subarray(offset, offset + 8_192));
  }
  return btoa(binary);
}

function decodeBase64(value: string): Uint8Array {
  const binary = atob(value);
  return Uint8Array.from(binary, (char) => char.charCodeAt(0));
}

/**
 * Begin a multipart upload session. One UserDO turn; zero ShardDO
 * RPCs. Validates metadata/tags/version up front so the caller fails
 * fast at begin rather than late at finalize.
 *
 * On `resumeFrom`: looks up the existing session row, validates that
 * it's still open and matches the (parent, leaf, total_size,
 * total_chunks, chunk_size) the resumer passed (so a stale session
 * id from a prior different upload cannot be hijacked), and queries
 * each shard in the pool for already-landed chunk indices. Returns
 * the union as `landed[]`.
 */
export async function vfsBeginMultipart(
  durableObject: UserDO,
  scope: VFSScope,
  path: string,
  opts: VFSBeginMultipartOpts
): Promise<MultipartBeginResponse> {
  const userId = userIdFor(scope);

  if (
    opts.protocolVersion !== undefined &&
    opts.protocolVersion !== MULTIPART_PROTOCOL_VERSION
  ) {
    throw new VFSError(
      "EINVAL",
      `beginMultipart: unsupported protocolVersion ${opts.protocolVersion}`
    );
  }

  // Validate inputs up front.
  if (
    typeof opts.size !== "number" ||
    !Number.isFinite(opts.size) ||
    !Number.isInteger(opts.size) ||
    opts.size < 0
  ) {
    throw new VFSError("EINVAL", `beginMultipart: size must be a non-negative integer (got ${opts.size})`);
  }
  if (opts.metadata !== undefined && opts.metadata !== null) {
    validateMetadata(opts.metadata);
  }
  if (opts.tags !== undefined) {
    validateTags(opts.tags);
  }
  if (opts.version?.label !== undefined) {
    validateLabel(opts.version.label);
  }
  validateEncryptionOpts(opts.encryption);

  // Resume branch — must run before parent/folder validation so that
  // a resume-of-a-previously-existing session still works even if
  // intervening writes changed the parent dir state.
  if (opts.resumeFrom !== undefined) {
    return await resumeMultipart(durableObject, scope, userId, opts);
  }

  // Cold begin — resolve parent + reject folder collisions.
  const { parentId, leaf } = resolveParent(durableObject, userId, path);
  if (folderExists(durableObject, userId, parentId, leaf)) {
    throw new VFSError(
      "EISDIR",
      `beginMultipart: target is a directory: ${path}`
    );
  }
  // enforce mode-history-monotonic across multipart writes,
  // exactly as `vfsWriteFile` does.
  const incomingEncryption: EncryptionStampOpts | undefined = opts.encryption
    ? { mode: opts.encryption.mode, keyId: opts.encryption.keyId }
    : undefined;
  enforceModeMonotonic(
    durableObject,
    userId,
    parentId,
    leaf,
    incomingEncryption
  );

  // Compute server-authoritative chunk spec. Honour client hint
  // when it falls within sane bounds: any positive integer up to
  // 2 MiB (the SQLite blob ceiling). The lower bound is intentionally
  // permissive so tests and small-file experimentation work without
  // being bumped up to a 1 MB chunk; production callers will use the
  // adaptive ladder via `computeChunkSpec` (no hint), which is what
  // gets returned when no hint is provided.
  const { chunkSize: serverChunkSize, chunkCount: serverChunkCount } =
    computeChunkSpec(opts.size);
  const chunkSize =
    opts.chunkSize !== undefined &&
    Number.isInteger(opts.chunkSize) &&
    opts.chunkSize > 0 &&
    opts.chunkSize <= 2 * 1024 * 1024
      ? opts.chunkSize
      : serverChunkSize;
  // Sanity: if the client tried a wildly off chunk size that yields
  // an absurd chunkCount, fall back to server-authoritative.
  const finalChunkSize =
    chunkSize === 0 && opts.size > 0 ? serverChunkSize : chunkSize;
  const finalTotalChunks =
    finalChunkSize === 0 ? 0 : Math.ceil(opts.size / finalChunkSize);
  void serverChunkCount; // unused but documents the parallel spec

  const tmpId = generateId();
  const fenceId = generateId();
  const poolSize = poolSizeFor(durableObject, userId);
  const placementVersion =
    opts.protocolVersion === MULTIPART_PROTOCOL_VERSION
      ? MULTIPART_PLACEMENT_VERSION
      : MULTIPART_LEGACY_PLACEMENT_VERSION;
  assertMultipartClientCanFinalize(
    finalTotalChunks,
    poolSize,
    opts.protocolVersion
  );
  const now = Date.now();
  const requestedTtl =
    typeof opts.ttlMs === "number" && opts.ttlMs > 0
      ? opts.ttlMs
      : MULTIPART_DEFAULT_TTL_MS;
  const { token, expiresAtMs } = await signVFSMultipartToken(
    durableObject.envPublic,
    {
      uploadId: tmpId,
      fenceId,
      userId,
      ns: scope.ns,
      tn: scope.tenant,
      sub: scope.sub,
      poolSize,
      placementVersion,
      totalChunks: finalTotalChunks,
      chunkSize: finalChunkSize,
      totalSize: opts.size,
    },
    requestedTtl
  );

  // Insert the tmp `files` row — same shape as `vfsBeginWriteStream`,
  // with an additional `total_chunks` field (added in ensureInit) so
  // finalize can sanity-check.
  const mode = opts.mode ?? 0o644;
  const mimeType = opts.mimeType ?? "application/octet-stream";
  const tmpName = `_vfs_tmp_${tmpId}`;
  let metadataBlob: Uint8Array | null = null;
  if (opts.metadata === null) {
    metadataBlob = new Uint8Array(0);
  } else if (opts.metadata !== undefined) {
    metadataBlob = validateMetadata(opts.metadata).encoded;
  }
  const tagsJson =
    opts.tags !== undefined ? JSON.stringify([...opts.tags]) : null;
  await scheduleStaleUploadSweep(durableObject);
  transactionSync(durableObject, () => {
    const activeCount = (
      durableObject.sql
        .exec(
          `SELECT COUNT(*) AS n FROM upload_sessions
            WHERE user_id = ? AND status IN ('open', 'finalizing', 'aborting')`,
          userId
        )
        .toArray()[0] as { n: number }
    ).n;
    if (activeCount >= MULTIPART_MAX_OPEN_SESSIONS_PER_TENANT) {
      throw new VFSError(
        "EBUSY",
        `beginMultipart: tenant has ${activeCount} active sessions (cap ${MULTIPART_MAX_OPEN_SESSIONS_PER_TENANT}); abort or finalize before opening more`
      );
    }
    durableObject.sql.exec(
      `INSERT INTO files (file_id, user_id, parent_id, file_name, file_size, file_hash, mime_type, chunk_size, chunk_count, pool_size, status, created_at, updated_at, mode, node_kind)
       VALUES (?, ?, ?, ?, ?, '', ?, ?, 0, ?, 'uploading', ?, ?, ?, 'file')`,
      tmpId,
      userId,
      parentId,
      tmpName,
      opts.size,
      mimeType,
      finalChunkSize,
      poolSize,
      now,
      now,
      mode
    );
    durableObject.sql.exec(
      `INSERT INTO upload_sessions
         (upload_id, fence_id, user_id, parent_id, leaf, total_size, total_chunks, chunk_size, pool_size, placement_version, expires_at, status,
            encryption_mode, encryption_key_id, metadata_blob, tags_json, version_label, version_user_visible, mode, mime_type, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'open', ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      tmpId,
      fenceId,
      userId,
      parentId,
      leaf,
      opts.size,
      finalTotalChunks,
      finalChunkSize,
      poolSize,
      placementVersion,
      expiresAtMs,
      incomingEncryption?.mode ?? null,
      incomingEncryption?.keyId ?? null,
      metadataBlob,
      tagsJson,
      opts.version?.label ?? null,
      opts.version?.userVisible === undefined
        ? null
        : opts.version.userVisible
          ? 1
          : 0,
      mode,
      mimeType,
      now
    );
  });

  return {
    uploadId: tmpId,
    chunkSize: finalChunkSize,
    totalChunks: finalTotalChunks,
    poolSize,
    sessionToken: token,
    putEndpoint: `/api/vfs/multipart/${tmpId}`,
    expiresAtMs,
    landed: [],
    protocolVersion: MULTIPART_PROTOCOL_VERSION,
  };
}

/**
 * Resume an existing multipart session. Re-mints a session token (so
 * the caller's token is fresh even if the prior one expired) and
 * returns the first bounded landed page. SDK callers follow the signed
 * continuation when they request the complete set.
 *
 * Validates that the prior session is still 'open' and not expired —
 * a finalized or aborted session cannot be resumed; the caller must
 * begin a fresh upload.
 */
async function resumeMultipart(
  durableObject: UserDO,
  scope: VFSScope,
  userId: string,
  opts: VFSBeginMultipartOpts
): Promise<MultipartBeginResponse> {
  const uploadId = opts.resumeFrom!;
  const row = durableObject.sql
    .exec(
      `SELECT * FROM upload_sessions WHERE upload_id = ? AND user_id = ?`,
      uploadId,
      userId
    )
    .toArray()[0] as unknown as UploadSessionRow | undefined;
  if (!row) {
    throw new VFSError("ENOENT", `resumeMultipart: session not found: ${uploadId}`);
  }
  if (row.status !== "open") {
    throw new VFSError(
      "EBUSY",
      `resumeMultipart: session status='${row.status}'; only 'open' is resumable`
    );
  }
  if (row.expires_at < Date.now()) {
    throw new VFSError(
      "EBUSY",
      `resumeMultipart: session expired at ${row.expires_at}`
    );
  }
  assertMultipartClientCanFinalize(
    row.total_chunks,
    row.pool_size,
    opts.protocolVersion
  );
  // Validate alignment if the caller passed dimensions — defends
  // against accidentally hijacking another tenant's session id.
  if (opts.size !== row.total_size) {
    throw new VFSError(
      "EINVAL",
      `resumeMultipart: size mismatch (session=${row.total_size}, caller=${opts.size})`
    );
  }

  const landedPage = await readMultipartLandedPage(
    durableObject,
    scope,
    userId,
    row,
    { shardIndex: 0, afterIndex: -1 }
  );

  // Re-mint the session token (extending the expiry).
  const requestedTtl =
    typeof opts.ttlMs === "number" && opts.ttlMs > 0
      ? opts.ttlMs
      : MULTIPART_DEFAULT_TTL_MS;
  const fenceId = row.fence_id ?? generateId();
  const { token, expiresAtMs } = await signVFSMultipartToken(
    durableObject.envPublic,
    {
      uploadId,
      fenceId,
      userId,
      ns: scope.ns,
      tn: scope.tenant,
      sub: scope.sub,
      poolSize: row.pool_size,
      placementVersion: row.placement_version,
      totalChunks: row.total_chunks,
      chunkSize: row.chunk_size,
      totalSize: row.total_size,
    },
    requestedTtl
  );
  // Update the session row's expires_at to reflect the new token.
  durableObject.sql.exec(
    "UPDATE upload_sessions SET expires_at = ?, fence_id = ? WHERE upload_id = ?",
    expiresAtMs,
    fenceId,
    uploadId
  );

  return {
    uploadId,
    chunkSize: row.chunk_size,
    totalChunks: row.total_chunks,
    poolSize: row.pool_size,
    sessionToken: token,
    putEndpoint: `/api/vfs/multipart/${uploadId}`,
    expiresAtMs,
    landed: landedPage.landed,
    ...(landedPage.continuation === undefined
      ? {}
      : { continuation: landedPage.continuation }),
    protocolVersion: MULTIPART_PROTOCOL_VERSION,
  };
}

function assertMultipartClientCanFinalize(
  totalChunks: number,
  poolSize: number,
  protocolVersion: number | undefined
): void {
  if (
    protocolVersion === MULTIPART_PROTOCOL_VERSION ||
    (totalChunks <= MULTIPART_HASH_PAGE_SIZE &&
      poolSize <= MULTIPART_LEGACY_MAX_POOL_SIZE)
  ) {
    return;
  }
  throw new VFSError(
    "EINVAL",
    "beginMultipart: this client cannot finalize the upload; upgrade to multipart protocol v2 before uploading chunks"
  );
}

/**
 * Abort a multipart upload. Idempotent: aborting a session that is
 * already 'aborted' is a no-op; aborting a 'finalized' session
 * raises EBUSY (cannot un-finalize).
 *
 * Each call advances at most one page per abort phase. Cleanup intents commit
 * before the outbox runs the idempotent `deleteChunks` and
 * `clearMultipartStaging` protocol.
 */
export async function vfsAbortMultipart(
  durableObject: UserDO,
  scope: VFSScope,
  uploadId: string,
  allowFinalizing = true
): Promise<{ ok: true }> {
  const state = await beginMultipartAbort(
    durableObject,
    scope,
    uploadId,
    allowFinalizing
  );
  if (state.done) return { ok: true };

  const progress = await advanceMultipartAbortBounded(
    durableObject,
    scope,
    uploadId,
    state.userId
  );
  if (progress.done) return { ok: true };
  await scheduleAlarmAt(durableObject, Date.now() + 1_000);
  throw new VFSError(
    "EBUSY",
    "abortMultipart: cleanup is still in progress; use abort steps"
  );
}

export async function vfsAbortMultipartStep(
  durableObject: UserDO,
  scope: VFSScope,
  uploadId: string,
  allowFinalizing = true
): Promise<MultipartAbortProgress> {
  const state = await beginMultipartAbort(
    durableObject,
    scope,
    uploadId,
    allowFinalizing
  );
  if (state.done) return { done: true };
  const progress = await advanceMultipartAbortPage(
    durableObject,
    scope,
    uploadId,
    state.userId
  );
  if (!progress.done) await scheduleAlarmAt(durableObject, Date.now() + 1_000);
  return progress;
}

async function requestMultipartAbort(
  durableObject: UserDO,
  scope: VFSScope,
  uploadId: string,
  allowFinalizing: boolean
): Promise<MultipartAbortProgress> {
  const state = await beginMultipartAbort(
    durableObject,
    scope,
    uploadId,
    allowFinalizing
  );
  if (state.done) return { done: true };
  const progress = await advanceMultipartAbortBounded(
    durableObject,
    scope,
    uploadId,
    state.userId
  );
  if (!progress.done) await scheduleAlarmAt(durableObject, Date.now() + 1_000);
  return progress;
}

async function beginMultipartAbort(
  durableObject: UserDO,
  scope: VFSScope,
  uploadId: string,
  allowFinalizing: boolean
): Promise<{ done: boolean; userId: string }> {
  const userId = userIdFor(scope);
  const row = durableObject.sql
    .exec(
      `SELECT * FROM upload_sessions WHERE upload_id = ? AND user_id = ?`,
      uploadId,
      userId
    )
    .toArray()[0] as unknown as UploadSessionRow | undefined;
  if (!row) {
    throw new VFSError("ENOENT", `abortMultipart: session not found: ${uploadId}`);
  }
  if (row.status === "finalized") {
    throw new VFSError(
      "EBUSY",
      `abortMultipart: session is already finalized; cannot un-finalize`
    );
  }
  if (row.status === "aborted") return { done: true, userId };
  if (row.status === "finalizing" && !allowFinalizing) {
    throw new VFSError("EBUSY", "abortMultipart: finalize is in progress");
  }

  await scheduleStaleUploadSweep(durableObject);
  transactionSync(durableObject, () => {
    const current = durableObject.sql
      .exec(
        `SELECT status FROM upload_sessions
          WHERE upload_id = ? AND user_id = ?`,
        uploadId,
        userId
      )
      .toArray()[0] as
      | { status: string }
      | undefined;
    if (!current) {
      throw new VFSError(
        "ENOENT",
        `abortMultipart: session not found: ${uploadId}`
      );
    }
    if (current.status === "finalized") {
      throw new VFSError(
        "EBUSY",
        "abortMultipart: session is already finalized; cannot un-finalize"
      );
    }
    if (current.status === "aborted") return;
    if (current.status === "finalizing" && !allowFinalizing) {
      throw new VFSError("EBUSY", "abortMultipart: finalize is in progress");
    }

    if (current.status === "open" || current.status === "finalizing") {
      durableObject.sql.exec(
        `UPDATE upload_sessions
            SET status = 'aborting', abort_phase = 'fencing',
                abort_fence_cursor = 0, abort_intent_cursor = 0,
                abort_cleanup_cursor = 0, abort_old_intent_cursor = -1,
                attempts = 0, abort_retry_at = 0
          WHERE upload_id = ? AND user_id = ? AND status = ?`,
        uploadId,
        userId,
        current.status
      );
      if (lastSqlChanges(durableObject) !== 1) {
        throw new VFSError("EBUSY", "abortMultipart: session changed");
      }
    } else if (current.status === "aborting") {
      durableObject.sql.exec(
        `UPDATE upload_sessions SET abort_phase = 'fencing'
          WHERE upload_id = ? AND user_id = ? AND status = 'aborting'
            AND abort_phase IS NULL`,
        uploadId,
        userId
      );
    } else {
      throw new VFSError(
        "EBUSY",
        `abortMultipart: session status='${current.status}'`
      );
    }
  });

  return { done: false, userId };
}

const MULTIPART_ABORT_PHASE_PAGES_PER_CALL = 5;

async function advanceMultipartAbortBounded(
  durableObject: UserDO,
  scope: VFSScope,
  uploadId: string,
  userId: string
): Promise<MultipartAbortProgress> {
  let progress: MultipartAbortProgress = {
    done: false,
    phase: "fencing",
    cursor: 0,
    total: 0,
  };
  for (let page = 0; page < MULTIPART_ABORT_PHASE_PAGES_PER_CALL; page++) {
    progress = await advanceMultipartAbortPage(
      durableObject,
      scope,
      uploadId,
      userId
    );
    if (progress.done) return progress;
  }
  return progress;
}

async function advanceMultipartAbortPage(
  durableObject: UserDO,
  scope: VFSScope,
  uploadId: string,
  userId: string
): Promise<MultipartAbortProgress> {
  const session = durableObject.sql
    .exec(
      "SELECT * FROM upload_sessions WHERE upload_id = ? AND user_id = ?",
      uploadId,
      userId
    )
    .toArray()[0] as unknown as UploadSessionRow | undefined;
  if (!session) throw new VFSError("ENOENT", "abortMultipart: session not found");
  if (session.status === "aborted") return { done: true };
  if (session.status !== "aborting" || session.abort_phase === null) {
    throw new MultipartLocalCorruptionError(
      "abortMultipart: invalid abort state"
    );
  }

  if (session.abort_phase === "fencing") {
    const start = session.abort_fence_cursor;
    const end = Math.min(start + MULTIPART_FENCE_PAGE_SIZE, session.pool_size);
    const fenceId = session.fence_id;
    if (fenceId !== null) {
      const ns = shardNs(durableObject);
      await Promise.all(
        Array.from({ length: end - start }, async (_, offset) => {
          const shardIndex = start + offset;
          const shardName = vfsShardDOName(
            scope.ns,
            scope.tenant,
            scope.sub,
            shardIndex
          );
          await ns
            .get(ns.idFromName(shardName))
            .fenceMultipart(
              uploadId,
              fenceId,
              "aborting",
              session.expires_at
            );
        })
      );
    }
    updateAbortCursor(
      durableObject,
      session,
      "fencing",
      end >= session.pool_size ? "intents" : "fencing",
      "abort_fence_cursor",
      end
    );
    return end >= session.pool_size
      ? {
          done: false,
          phase: "intents",
          cursor: 0,
          total: session.pool_size,
        }
      : {
          done: false,
          phase: "fencing",
          cursor: end,
          total: session.pool_size,
        };
  }

  if (session.abort_phase === "intents") {
    const start = session.abort_intent_cursor;
    const end = Math.min(start + MULTIPART_FENCE_PAGE_SIZE, session.pool_size);
    const now = Date.now();
    transactionSync(durableObject, () => {
      for (let shardIndex = start; shardIndex < end; shardIndex++) {
        stageChunkCleanupIntent(
          durableObject,
          uploadId,
          shardIndex,
          now,
          now,
          ChunkCleanupKind.Multipart
        );
      }
      durableObject.sql.exec(
        `DELETE FROM upload_cleanup_routes
             WHERE upload_id = ? AND shard_index >= ? AND shard_index < ?`,
        uploadId,
        start,
        end
      );
      updateAbortCursor(
        durableObject,
        session,
        "intents",
        end >= session.pool_size ? "cleanup" : "intents",
        "abort_intent_cursor",
        end
      );
    });
    if (end > start) {
      await drainChunkCleanupIntents(durableObject, scope, uploadId);
    }
    return end >= session.pool_size
      ? {
          done: false,
          phase: "cleanup",
          cursor: 0,
          total: session.total_chunks,
        }
      : {
          done: false,
          phase: "intents",
          cursor: end,
          total: session.pool_size,
        };
  }

  if (session.abort_phase === "cleanup") {
    const start = session.abort_cleanup_cursor;
    const end = Math.min(start + MULTIPART_HASH_PAGE_SIZE, session.total_chunks);
    const context =
      session.finalize_context === null
        ? null
        : parseFinalizeContext(session.finalize_context);
    const nextPhase =
      end < session.total_chunks
        ? "cleanup"
        : context !== null && needsOldManifestPreparation(context)
          ? "old_intents"
          : "local";
    transactionSync(durableObject, () => {
      durableObject.sql.exec(
        `DELETE FROM upload_expected_chunks
          WHERE upload_id = ? AND chunk_index >= ? AND chunk_index < ?`,
        uploadId,
        start,
        end
      );
      durableObject.sql.exec(
        `DELETE FROM upload_verified_chunks
          WHERE upload_id = ? AND chunk_index >= ? AND chunk_index < ?`,
        uploadId,
        start,
        end
      );
      durableObject.sql.exec(
        `DELETE FROM file_chunks
          WHERE file_id = ? AND chunk_index >= ? AND chunk_index < ?`,
        uploadId,
        start,
        end
      );
      if (context?.versionId !== null && context?.versionId !== undefined) {
        durableObject.sql.exec(
          `DELETE FROM version_chunks
            WHERE version_id = ? AND chunk_index >= ? AND chunk_index < ?`,
          context.versionId,
          start,
          end
        );
      }
      updateAbortCursor(
        durableObject,
        session,
        "cleanup",
        nextPhase,
        "abort_cleanup_cursor",
        end
      );
    });
    return {
      done: false,
      phase: nextPhase,
      cursor: nextPhase === "cleanup" ? end : 0,
      total:
        nextPhase === "cleanup"
          ? session.total_chunks
          : Math.max(session.pool_size, 1),
    };
  }

  if (session.abort_phase === "old_intents") {
    const context = parseFinalizeContext(session.finalize_context);
    if (!needsOldManifestPreparation(context)) {
      throw new MultipartLocalCorruptionError(
        "abortMultipart: invalid old-intent cleanup"
      );
    }
    if (context.expectedDestination === null) {
      throw new MultipartLocalCorruptionError(
        "abortMultipart: missing destination"
      );
    }
    const rows = durableObject.sql
      .exec(
        `SELECT shard_index FROM upload_cleanup_routes
            WHERE upload_id = ? AND cleanup_kind = ? AND shard_index > ?
            ORDER BY shard_index LIMIT ?`,
        session.upload_id,
        ChunkCleanupKind.Chunks,
        session.abort_old_intent_cursor,
        MULTIPART_FENCE_PAGE_SIZE + 1
      )
      .toArray() as Array<{ shard_index: number }>;
    const intentPage = rows.slice(0, MULTIPART_FENCE_PAGE_SIZE);
    const hasMore = rows.length > MULTIPART_FENCE_PAGE_SIZE;
    const cursor =
      intentPage.at(-1)?.shard_index ?? session.abort_old_intent_cursor;
    transactionSync(durableObject, () => {
      for (const row of intentPage) {
        durableObject.sql.exec(
          `DELETE FROM upload_cleanup_routes
              WHERE upload_id = ? AND cleanup_kind = ? AND shard_index = ?`,
          session.upload_id,
          ChunkCleanupKind.Chunks,
          row.shard_index
        );
      }
      updateAbortCursor(
        durableObject,
        session,
        "old_intents",
        hasMore ? "old_intents" : "local",
        "abort_old_intent_cursor",
        cursor
      );
    });
    return hasMore
      ? {
          done: false,
          phase: "old_intents",
          cursor,
          total: session.pool_size,
        }
      : { done: false, phase: "local", cursor: 0, total: 1 };
  }

  if (session.abort_phase === "local") {
    transactionSync(durableObject, () => {
      hardDeleteFileRowLocal(durableObject, userId, uploadId);
      durableObject.sql.exec(
        `UPDATE upload_sessions
            SET status = 'aborted', abort_phase = 'done', attempts = 0,
                abort_retry_at = 0, terminal_at = ?
            WHERE upload_id = ? AND user_id = ? AND status = 'aborting'
              AND abort_phase = 'local'`,
        Date.now(),
        uploadId,
        userId
      );
      if (lastSqlChanges(durableObject) !== 1) {
        throw new VFSError("EBUSY", "abortMultipart: local phase changed");
      }
    });
    compactTerminalMultipartSession(durableObject, uploadId);
    return { done: true };
  }

  throw new MultipartLocalCorruptionError(
    `abortMultipart: invalid phase='${session.abort_phase}'`
  );
}

function updateAbortCursor(
  durableObject: UserDO,
  session: UploadSessionRow,
  expectedPhase: string,
  nextPhase: string,
  cursorColumn:
    | "abort_fence_cursor"
    | "abort_intent_cursor"
    | "abort_cleanup_cursor"
    | "abort_old_intent_cursor",
  cursor: number
): void {
  durableObject.sql.exec(
    `UPDATE upload_sessions
        SET ${cursorColumn} = ?, abort_phase = ?, attempts = 0,
            abort_retry_at = 0
      WHERE upload_id = ? AND user_id = ? AND status = 'aborting'
        AND abort_phase = ? AND ${cursorColumn} = ?`,
    cursor,
    nextPhase,
    session.upload_id,
    session.user_id,
    expectedPhase,
    session[cursorColumn]
  );
  if (lastSqlChanges(durableObject) !== 1) {
    throw new VFSError("EBUSY", "abortMultipart: cursor changed");
  }
}

/** Bounded adapter for clients using the original one-request finalize RPC. */
export async function vfsFinalizeMultipart(
  durableObject: UserDO,
  scope: VFSScope,
  uploadId: string,
  chunkHashList: readonly string[]
): Promise<MultipartFinalizeResponse> {
  const userId = userIdFor(scope);
  const session = durableObject.sql
    .exec(
      `SELECT * FROM upload_sessions WHERE upload_id = ? AND user_id = ?`,
      uploadId,
      userId
    )
    .toArray()[0] as unknown as UploadSessionRow | undefined;
  if (!session) {
    throw new VFSError("ENOENT", `finalizeMultipart: session not found: ${uploadId}`);
  }
  if (session.status === "finalized") {
    return session.finalize_result === null
      ? legacyFinalizedMultipartResult(durableObject, session)
      : parseFinalizeResult(session.finalize_result);
  }
  if (session.status !== "open" && session.status !== "finalizing") {
    throw new VFSError(
      "EBUSY",
      `finalizeMultipart: session status='${session.status}'`
    );
  }
  if (session.expires_at < Date.now()) {
    throw new VFSError(
      "EBUSY",
      `finalizeMultipart: session expired at ${session.expires_at}`
    );
  }
  if (session.status === "finalizing" && session.finalize_context === null) {
    await vfsFinalizeMultipartStep(durableObject, scope, uploadId);
    throw new VFSError(
      "EBUSY",
      "finalizeMultipart: pre-upgrade finalizing session was aborted"
    );
  }
  if (chunkHashList.length !== session.total_chunks) {
    throw new VFSError(
      "EINVAL",
      `finalizeMultipart: chunkHashList length ${chunkHashList.length} != totalChunks ${session.total_chunks}`
    );
  }
  if (
    chunkHashList.length > MULTIPART_HASH_PAGE_SIZE ||
    session.pool_size > MULTIPART_LEGACY_MAX_POOL_SIZE
  ) {
    throw new VFSError(
      "EINVAL",
      "finalizeMultipart: upload requires paged hash staging and finalize steps"
    );
  }
  for (let index = 0; index < chunkHashList.length; index++) {
    if (!/^[0-9a-f]{64}$/.test(chunkHashList[index]!)) {
      throw new VFSError(
        "EINVAL",
        `finalizeMultipart: chunkHashList[${index}] is not a 64-char lowercase hex string`
      );
    }
  }
  if (session.status === "finalizing") {
    const staged = durableObject.sql
      .exec(
        `SELECT chunk_index, chunk_hash FROM upload_expected_chunks
          WHERE upload_id = ? ORDER BY chunk_index`,
        uploadId
      )
      .toArray() as Array<{ chunk_index: number; chunk_hash: string }>;
    if (
      staged.length !== chunkHashList.length ||
      staged.some(
        (row, index) =>
          row.chunk_index !== index || row.chunk_hash !== chunkHashList[index]
      )
    ) {
      throw new VFSError(
        "EBUSY",
        "finalizeMultipart: hash list differs from the in-progress finalize"
      );
    }
  } else if (chunkHashList.length > 0) {
    vfsStageMultipartHashes(
      durableObject,
      scope,
      uploadId,
      0,
      chunkHashList
    );
  }
  try {
    for (let step = 0; step < MULTIPART_LEGACY_FINALIZE_STEP_LIMIT; step++) {
      const progress = await vfsFinalizeMultipartStep(
        durableObject,
        scope,
        uploadId
      );
      if (progress.done) return progress.result;
      const persisted = readPersistedMultipartResult(
        durableObject,
        userId,
        uploadId
      );
      if (persisted !== null) return persisted;
    }
    const persisted = readPersistedMultipartResult(
      durableObject,
      userId,
      uploadId
    );
    if (persisted !== null) return persisted;
    throw new VFSError("EBUSY", "finalizeMultipart: bounded finalize did not finish");
  } catch (error) {
    const current = durableObject.sql
      .exec(
        `SELECT status, finalize_phase, finalize_result FROM upload_sessions
          WHERE upload_id = ? AND user_id = ?`,
        uploadId,
        userId
      )
      .toArray()[0] as
      | {
          status: string;
          finalize_phase: string | null;
          finalize_result: string | null;
        }
      | undefined;
    if (current?.status === "finalized") {
      return parseFinalizeResult(current.finalize_result);
    }
    if (
      current?.status === "finalizing" &&
      current.finalize_phase === "publishing"
    ) {
      await requestMultipartAbort(durableObject, scope, uploadId, true);
    }
    throw error;
  }
}

function readPersistedMultipartResult(
  durableObject: UserDO,
  userId: string,
  uploadId: string
): MultipartFinalizeResponse | null {
  const row = durableObject.sql
    .exec(
      `SELECT status, finalize_result FROM upload_sessions
        WHERE upload_id = ? AND user_id = ?`,
      uploadId,
      userId
    )
    .toArray()[0] as
    | { status: string; finalize_result: string | null }
    | undefined;
  return row?.status === "finalized"
    ? parseFinalizeResult(row.finalize_result)
    : null;
}

/**
 * Walk the `folders` parent_id chain to reconstruct an absolute path
 * for a just-finalized file. Capped at 256 hops to defend against
 * pathological cycles in malformed rows.
 */
function reconstructFinalizedPath(
  durableObject: UserDO,
  userId: string,
  parentId: string | null,
  leaf: string
): string {
  const segments: string[] = [leaf];
  let cursor: string | null = parentId;
  for (let i = 0; i < 256 && cursor !== null; i++) {
    const row = durableObject.sql
      .exec(
        "SELECT parent_id, name FROM folders WHERE folder_id = ? AND user_id = ?",
        cursor,
        userId
      )
      .toArray()[0] as { parent_id: string | null; name: string } | undefined;
    if (!row) break;
    segments.unshift(row.name);
    cursor = row.parent_id ?? null;
  }
  return "/" + segments.join("/");
}

interface MultipartStatusPosition {
  shardIndex: number;
  afterIndex: number;
}

async function decodeMultipartStatusPosition(
  durableObject: UserDO,
  scope: VFSScope,
  userId: string,
  uploadId: string,
  continuation: string | undefined
): Promise<MultipartStatusPosition> {
  if (continuation === undefined) return { shardIndex: 0, afterIndex: -1 };
  if (
    continuation.length === 0 ||
    continuation.length > MULTIPART_STATUS_CURSOR_MAX_BYTES
  ) {
    throw new VFSError("EINVAL", "getMultipartStatus: invalid continuation");
  }
  const cursor = await verifyVFSMultipartStatusCursor(
    durableObject.envPublic,
    continuation
  );
  if (
    cursor === null ||
    cursor.uploadId !== uploadId ||
    cursor.userId !== userId ||
    cursor.ns !== scope.ns ||
    cursor.tn !== scope.tenant ||
    cursor.sub !== scope.sub
  ) {
    throw new VFSError(
      "EINVAL",
      "getMultipartStatus: invalid continuation for this upload and tenant"
    );
  }
  return { shardIndex: cursor.shardIndex, afterIndex: cursor.afterIndex };
}

function expectedMultipartChunkSize(session: UploadSessionRow, index: number): number {
  return Math.max(
    0,
    Math.min(session.chunk_size, session.total_size - index * session.chunk_size)
  );
}

async function readMultipartLandedPage(
  durableObject: UserDO,
  scope: VFSScope,
  userId: string,
  session: UploadSessionRow,
  start: MultipartStatusPosition
): Promise<MultipartStatusPageResponse> {
  if (start.shardIndex > session.pool_size) {
    throw new VFSError("EINVAL", "getMultipartStatus: continuation is out of range");
  }

  const ns = shardNs(durableObject);
  const landed: number[] = [];
  let bytesUploaded = 0;
  let shardIndex = start.shardIndex;
  let afterIndex = start.afterIndex;
  let shardsInspected = 0;
  let rowsInspected = 0;

  while (
    shardIndex < session.pool_size &&
    shardsInspected < MULTIPART_STATUS_SHARD_PAGE_SIZE &&
    rowsInspected < MULTIPART_STATUS_ENTRY_PAGE_SIZE
  ) {
    const limit = MULTIPART_STATUS_ENTRY_PAGE_SIZE - rowsInspected;
    const shardName = vfsShardDOName(
      scope.ns,
      scope.tenant,
      scope.sub,
      shardIndex
    );
    const stub = ns.get(ns.idFromName(shardName));
    const response = await stub.getMultipartLanded(
      session.upload_id,
      afterIndex,
      limit
    );
    shardsInspected++;

    const candidates: Array<{ index: number; size: number | undefined }> = [];
    for (
      let offset = 0;
      offset < response.idx.length && candidates.length < limit;
      offset++
    ) {
      const index = response.idx[offset];
      if (Number.isSafeInteger(index) && index > afterIndex) {
        candidates.push({ index, size: response.sizes?.[offset] });
      }
    }

    for (const candidate of candidates) {
      rowsInspected++;
      afterIndex = candidate.index;
      if (
        candidate.index < 0 ||
        candidate.index >= session.total_chunks ||
        placeMultipartChunk(
          userId,
          session.upload_id,
          candidate.index,
          session.pool_size,
          session.placement_version
        ) !== shardIndex
      ) {
        continue;
      }
      landed.push(candidate.index);
      bytesUploaded +=
        typeof candidate.size === "number" &&
        Number.isSafeInteger(candidate.size) &&
        candidate.size >= 0
          ? candidate.size
          : expectedMultipartChunkSize(session, candidate.index);
    }

    if (candidates.length === limit) break;
    shardIndex++;
    afterIndex = -1;
  }

  landed.sort((left, right) => left - right);
  if (shardIndex >= session.pool_size) {
    return {
      landed,
      total: session.total_chunks,
      bytesUploaded,
      expiresAtMs: session.expires_at,
    };
  }
  const continuation = await signVFSMultipartStatusCursor(
    durableObject.envPublic,
    {
      uploadId: session.upload_id,
      userId,
      ns: scope.ns,
      tn: scope.tenant,
      sub: scope.sub,
      shardIndex,
      afterIndex,
    }
  );
  return {
    landed,
    total: session.total_chunks,
    bytesUploaded,
    expiresAtMs: session.expires_at,
    continuation,
  };
}

export async function vfsGetMultipartStatus(
  durableObject: UserDO,
  scope: VFSScope,
  uploadId: string,
  continuation?: string
): Promise<MultipartStatusPageResponse & { status: string }> {
  const userId = userIdFor(scope);
  const position = await decodeMultipartStatusPosition(
    durableObject,
    scope,
    userId,
    uploadId,
    continuation
  );
  const row = durableObject.sql
    .exec(
      `SELECT * FROM upload_sessions WHERE upload_id = ? AND user_id = ?`,
      uploadId,
      userId
    )
    .toArray()[0] as unknown as UploadSessionRow | undefined;
  if (!row) {
    throw new VFSError(
      "ENOENT",
      `getMultipartStatus: session not found: ${uploadId}`
    );
  }
  if (position.shardIndex > row.pool_size) {
    throw new VFSError("EINVAL", "getMultipartStatus: continuation is out of range");
  }
  return {
    ...(await readMultipartLandedPage(
      durableObject,
      scope,
      userId,
      row,
      position
    )),
    status: row.status,
  };
}

/**
 * Cap on repeated deterministic local corruption for one session. Transient
 * failures back off without a terminal cap. Once cleanup intents commit,
 * their remote work retries independently through the outbox.
 *
 * Explicitly aborting sessions are retried on the prompt maintenance cadence;
 * expired open/finalizing sessions are picked up when their deadline passes.
 */
export const MULTIPART_MAX_ABORT_ATTEMPTS = 5;
export const MULTIPART_SWEEP_SESSION_LIMIT = 4;
export const MULTIPART_FINALIZE_RECOVERY_SESSION_LIMIT = 4;
export const MULTIPART_TERMINAL_PRUNE_LIMIT = 64;
export const MULTIPART_SWEEP_MAX_PHASE_PAGES =
  MULTIPART_SWEEP_SESSION_LIMIT * MULTIPART_ABORT_PHASE_PAGES_PER_CALL;

function multipartAbortBackoffMs(attempts: number): number {
  return Math.min(1_000 * 2 ** Math.min(attempts - 1, 9), 10 * 60 * 1_000);
}

export function pruneTerminalMultipartSessions(
  durableObject: UserDO
): { pruned: number; remaining: boolean } {
  const cutoff = Date.now() - MULTIPART_TERMINAL_RETENTION_MS;
  const rows = durableObject.sql
    .exec(
      `SELECT upload_id FROM upload_sessions
        WHERE (status IN ('aborted', 'poisoned')
           OR (status = 'finalized' AND finalize_phase = 'done'))
          AND COALESCE(terminal_at, created_at) <= ?
        ORDER BY COALESCE(terminal_at, created_at), upload_id
        LIMIT ?`,
      cutoff,
      MULTIPART_TERMINAL_PRUNE_LIMIT
    )
    .toArray() as Array<{ upload_id: string }>;
  if (rows.length > 0) {
    transactionSync(durableObject, () => {
      for (const row of rows) {
        durableObject.sql.exec(
          "DELETE FROM upload_expected_chunks WHERE upload_id = ?",
          row.upload_id
        );
        durableObject.sql.exec(
          "DELETE FROM upload_verified_chunks WHERE upload_id = ?",
          row.upload_id
        );
        durableObject.sql.exec(
          "DELETE FROM upload_cleanup_routes WHERE upload_id = ?",
          row.upload_id
        );
        durableObject.sql.exec(
          "DELETE FROM upload_sessions WHERE upload_id = ?",
          row.upload_id
        );
      }
    });
  }
  const remaining =
    durableObject.sql
      .exec(
        `SELECT 1 FROM upload_sessions
          WHERE (status IN ('aborted', 'poisoned')
             OR (status = 'finalized' AND finalize_phase = 'done'))
            AND COALESCE(terminal_at, created_at) <= ?
          LIMIT 1`,
        cutoff
      )
      .toArray().length > 0;
  return { pruned: rows.length, remaining };
}

export async function resumeFinalizedMultipartSessions(
  durableObject: UserDO,
  scope: VFSScope
): Promise<{ resumed: number; remaining: boolean }> {
  const userId = userIdFor(scope);
  const sessions = durableObject.sql
    .exec(
      `SELECT upload_id FROM upload_sessions
        WHERE user_id = ? AND status = 'finalized'
          AND (finalize_phase IS NULL OR finalize_phase != 'done')
        ORDER BY created_at, upload_id
        LIMIT ?`,
      userId,
      MULTIPART_FINALIZE_RECOVERY_SESSION_LIMIT
    )
    .toArray() as Array<{ upload_id: string }>;

  for (const session of sessions) {
    try {
      await vfsFinalizeMultipartStep(durableObject, scope, session.upload_id);
    } catch (error) {
      logError(
        "multipart finalized-session recovery failed",
        {},
        error,
        {
          event: "multipart_finalize_recovery_failed",
          uploadId: session.upload_id,
        }
      );
    }
  }

  const remaining =
    durableObject.sql
      .exec(
        `SELECT 1 FROM upload_sessions
          WHERE user_id = ? AND status = 'finalized'
            AND (finalize_phase IS NULL OR finalize_phase != 'done')
          LIMIT 1`,
        userId
      )
      .toArray().length > 0;
  return { resumed: sessions.length, remaining };
}

/**
 * Alarm-driven sweep of unexpired aborting and expired open/finalizing
 * sessions. Called from UserDOCore's alarm() handler at scheduled intervals.
 * Idempotent and batch-bounded so one alarm performs at most 20 persisted
 * phase pages.
 *
 * For each selected session, advances the same bounded state machine as an
 * explicit abort.
 *
 * Failures back off without discarding persisted phase progress. Only a
 * repeatedly observed, deterministic local state corruption is poisoned;
 * remote failures remain recoverable. Once local intent creation commits,
 * shard cleanup is owned by the outbox.
 */
export async function sweepExpiredMultipartSessions(
  durableObject: UserDO,
  scopeForUser: (userId: string) => VFSScope
): Promise<{ swept: number; remaining: boolean }> {
  const now = Date.now();
  const stale = durableObject.sql
    .exec(
      `SELECT upload_id, user_id, attempts FROM upload_sessions
        WHERE abort_retry_at <= ?
          AND (status = 'aborting'
            OR (status IN ('open', 'finalizing') AND expires_at < ?))
        ORDER BY CASE WHEN status = 'aborting' THEN 0 ELSE 1 END, expires_at ASC
        LIMIT ?`,
      now,
      now,
      MULTIPART_SWEEP_SESSION_LIMIT
    )
    .toArray() as {
      upload_id: string;
      user_id: string;
      attempts: number;
    }[];

  for (const row of stale) {
    try {
      const scope = scopeForUser(row.user_id);
      await requestMultipartAbort(durableObject, scope, row.upload_id, true);
    } catch (err) {
      const nextAttempts = (row.attempts ?? 0) + 1;
      if (
        err instanceof MultipartLocalCorruptionError &&
        nextAttempts >= MULTIPART_MAX_ABORT_ATTEMPTS
      ) {
        durableObject.sql.exec(
          `UPDATE upload_sessions
              SET status = 'poisoned', attempts = ?, terminal_at = ?,
                  abort_retry_at = 0
            WHERE upload_id = ?`,
          nextAttempts,
          now,
          row.upload_id
        );
        logError(
          "multipart session poisoned after abort attempts",
          {},
          err,
          {
            event: "multipart_session_poisoned",
            uploadId: row.upload_id,
            attempts: nextAttempts,
          }
        );
      } else {
        durableObject.sql.exec(
          `UPDATE upload_sessions SET attempts = ?, abort_retry_at = ?
            WHERE upload_id = ?`,
          nextAttempts,
          now + multipartAbortBackoffMs(nextAttempts),
          row.upload_id
        );
      }
    }
  }

  const remaining =
    durableObject.sql
      .exec(
        `SELECT 1 FROM upload_sessions
          WHERE abort_retry_at <= ?
            AND (status = 'aborting'
              OR (status IN ('open', 'finalizing') AND expires_at < ?))
          LIMIT 1`,
        now,
        now
      )
      .toArray().length > 0;

  return { swept: stale.length, remaining };
}
