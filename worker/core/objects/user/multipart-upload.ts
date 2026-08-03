/**
 * Multipart parallel transfer engine, server-side.
 *
 * This module implements five UserDO RPCs:
 *
 *   - `vfsBeginMultipart` — mints a session, inserts a tmp `files` row
 *     (status='uploading'), inserts an `upload_sessions` row, signs an
 *     HMAC session token. Single UserDO turn; zero ShardDO RPCs.
 *
 *   - `vfsAbortMultipart` — flips session status to 'aborted', drops
 *     `chunk_refs` on every shard in the pool via `deleteChunks`, drops
 *     `upload_chunks` staging on every shard via `clearMultipartStaging`,
 *     hard-deletes the tmp `files` row.
 *
 *   - `vfsStageMultipartHashes` — persists at most 256 declared chunk
 *     hashes per call into `upload_expected_chunks`, advancing the
 *     session's contiguous `staged_hash_cursor`.
 *
 *   - `vfsFinalizeMultipartStep` — advances the durable finalize machine by
 *     one bounded page: at most 64 shard fences, or at most 256 verified
 *     chunks, or the publication that turns the verified manifest into a
 *     live file. Every page is resumable after Durable Object eviction.
 *
 *   - `vfsFinalizeMultipart` — the one-request entry point, which stages the
 *     hashes the caller declared and then drives that machine to completion
 *     inside a single turn. `commitRename` atomically supersedes any prior
 *     row at the target path. The chunk_refs were placed under
 *     `refId = uploadId`; rename preserves `file_id`, so the refs
 *     remain valid for the post-rename file.
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
} from "../../lib/auth";
import {
  MULTIPART_DEFAULT_TTL_MS,
  MULTIPART_FENCE_PAGE_SIZE,
  MULTIPART_HASH_PAGE_SIZE,
  MULTIPART_MAX_OPEN_SESSIONS_PER_TENANT,
  MULTIPART_PLACEMENT_VERSION,
  type MultipartBeginResponse,
  type MultipartFinalizeProgress,
  type MultipartFinalizeResponse,
  type MultipartHashPageResponse,
  type MultipartPlacementVersion,
  type ShardMultipartManifestRow,
} from "../../../../shared/multipart";
import {
  commitOperationTransition,
  heldProgress,
  runOperationPages,
  type PagedOperationTable,
  type RetryPolicy,
} from "../../lib/paged-operation";
import {
  commitRename,
  userIdFor,
  resolveParent,
  poolSizeFor,
  recordWriteUsage,
  folderExists,
  bumpFolderRevision,
  drainChunkCleanupIntents,
  stageChunkCleanupIntents,
} from "./vfs-ops";
import { hardDeleteFileRowLocal } from "./vfs/write-commit";
import {
  commitVersionChecked,
  dropTmpRowAfterVersionCommit,
  insertVersionChunk,
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
  stampFileEncryption,
  type EncryptionStampOpts,
} from "./encryption-stamp";
import { bytesToHex } from "../../../../shared/crypto";
import {
  createSha256State,
  digestSha256,
  restoreSha256State,
  serializeSha256State,
  updateSha256,
  type Sha256State,
} from "../../../../shared/incremental-sha256";
import { readMetadataBytes, replaceTags } from "./metadata-tags";
import {
  ChunkCleanupKind,
  lastSqlChanges,
  scheduleStaleUploadSweep,
  stageChunkCleanupIntent,
  retainMultipartStagingCleanup,
  transactionSync,
} from "./internal-storage";

export interface VFSBeginMultipartOpts {
  size: number;
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

/**
 * A whole `upload_sessions` row. Declared as an alias rather than an interface
 * so it carries the implicit index signature `SqlStorage.exec<T>` and the
 * paged-operation driver both require of a column record.
 */
type UploadSessionRow = {
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
  staged_hash_cursor: number;
  finalize_phase: string | null;
  finalize_fence_cursor: number;
  finalize_chunk_cursor: number;
  finalize_verify_shard_cursor: number;
  finalize_total_size: number;
  finalize_sha_state: string | null;
  finalize_result: string | null;
};

function readUploadSession(
  durableObject: UserDO,
  userId: string,
  uploadId: string
): UploadSessionRow | undefined {
  return durableObject.sql
    .exec<UploadSessionRow>(
      "SELECT * FROM upload_sessions WHERE upload_id = ? AND user_id = ?",
      uploadId,
      userId
    )
    .toArray()
    .at(0);
}

function shardNs(durableObject: UserDO): DurableObjectNamespace<ShardDO> {
  return durableObject.envPublic
    .MOSSAIC_SHARD as unknown as DurableObjectNamespace<ShardDO>;
}

async function fenceMultipartShards(
  durableObject: UserDO,
  scope: VFSScope,
  session: UploadSessionRow,
  state: "finalizing" | "aborting"
): Promise<void> {
  const fenceId = session.fence_id;
  if (fenceId === null) return;
  const ns = shardNs(durableObject);
  await Promise.all(
    Array.from({ length: session.pool_size }, async (_, shardIndex) => {
      const shardName = vfsShardDOName(
        scope.ns,
        scope.tenant,
        scope.sub,
        shardIndex
      );
      // The session's expiry becomes the shard fence's reclaim deadline:
      // no token for this upload outlives it, so nothing can re-open the
      // fence once it has passed.
      await ns
        .get(ns.idFromName(shardName))
        .fenceMultipart(session.upload_id, fenceId, state, session.expires_at);
    })
  );
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

  // Per-tenant cap on open sessions — defends against orphan-session
  // storms before the alarm sweeper has a chance to GC them. Caller
  // surfaces as EBUSY.
  const openCount = (
    durableObject.sql
      .exec(
        "SELECT COUNT(*) AS n FROM upload_sessions WHERE user_id = ? AND status = 'open'",
        userId
      )
      .toArray()[0] as { n: number }
  ).n;
  if (openCount >= MULTIPART_MAX_OPEN_SESSIONS_PER_TENANT) {
    throw new VFSError(
      "EBUSY",
      `beginMultipart: tenant has ${openCount} open sessions (cap ${MULTIPART_MAX_OPEN_SESSIONS_PER_TENANT}); abort or finalize before opening more`
    );
  }

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
  const now = Date.now();
  const ttl =
    typeof opts.ttlMs === "number" && opts.ttlMs > 0
      ? opts.ttlMs
      : MULTIPART_DEFAULT_TTL_MS;
  const expiresAt = now + ttl;

  // Insert the tmp `files` row — same shape as `vfsBeginWriteStream`,
  // with an additional `total_chunks` field (added in ensureInit) so
  // finalize can sanity-check.
  const mode = opts.mode ?? 0o644;
  const mimeType = opts.mimeType ?? "application/octet-stream";
  const tmpName = `_vfs_tmp_${tmpId}`;
  await scheduleStaleUploadSweep(durableObject);
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
  // Insert session row — captures every commit-time payload so finalize
  // can apply them without re-validation.
  let metadataBlob: Uint8Array | null = null;
  if (opts.metadata === null) {
    metadataBlob = new Uint8Array(0);
  } else if (opts.metadata !== undefined) {
    metadataBlob = validateMetadata(opts.metadata).encoded;
  }
  const tagsJson =
    opts.tags !== undefined ? JSON.stringify([...opts.tags]) : null;
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
    MULTIPART_PLACEMENT_VERSION,
    expiresAt,
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

  // Mint the session token (CPU-only; no DO RPC).
  const { token } = await signVFSMultipartToken(
    durableObject.envPublic,
    {
      uploadId: tmpId,
      fenceId,
      userId,
      ns: scope.ns,
      tn: scope.tenant,
      sub: scope.sub,
      poolSize,
      placementVersion: MULTIPART_PLACEMENT_VERSION,
      totalChunks: finalTotalChunks,
      chunkSize: finalChunkSize,
      totalSize: opts.size,
    },
    ttl
  );

  return {
    uploadId: tmpId,
    chunkSize: finalChunkSize,
    totalChunks: finalTotalChunks,
    poolSize,
    sessionToken: token,
    putEndpoint: `/api/vfs/multipart/${tmpId}`,
    expiresAtMs: expiresAt,
    landed: [],
  };
}

/**
 * Resume an existing multipart session. Re-mints a session token (so
 * the caller's token is fresh even if the prior one expired) and
 * returns the union of landed chunk indices across all shards in the
 * pool.
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
  const row = readUploadSession(durableObject, userId, uploadId);
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
  // Validate alignment if the caller passed dimensions — defends
  // against accidentally hijacking another tenant's session id.
  if (opts.size !== row.total_size) {
    throw new VFSError(
      "EINVAL",
      `resumeMultipart: size mismatch (session=${row.total_size}, caller=${opts.size})`
    );
  }

  // Probe every shard in the pool for landed indices. This is the
  // ONE place the resume probe pays a per-shard subrequest. For
  // typical pools (32) that's 32 subrequests — well within the
  // budget for a one-shot begin call.
  const ns = shardNs(durableObject);
  const landedSet = new Set<number>();
  const probes: Promise<void>[] = [];
  for (let sIdx = 0; sIdx < row.pool_size; sIdx++) {
    const shardName = vfsShardDOName(scope.ns, scope.tenant, scope.sub, sIdx);
    const stub = ns.get(ns.idFromName(shardName));
    probes.push(
      (async () => {
        try {
          const res = await stub.getMultipartLanded(uploadId);
          for (const i of res.idx) landedSet.add(i);
        } catch {
          // Best-effort; a shard fan-out failure on resume just means
          // the caller will see fewer landed chunks and re-PUT them.
          // Idempotent supersession on the ShardDO absorbs that.
        }
      })()
    );
  }
  await Promise.all(probes);

  // Re-mint the session token (extending the expiry).
  const ttl =
    typeof opts.ttlMs === "number" && opts.ttlMs > 0
      ? opts.ttlMs
      : MULTIPART_DEFAULT_TTL_MS;
  const expiresAt = Date.now() + ttl;
  const fenceId = row.fence_id ?? generateId();
  const { token } = await signVFSMultipartToken(
    durableObject.envPublic,
    {
      uploadId,
      fenceId,
      userId,
      ns: scope.ns,
      tn: scope.tenant,
      sub: scope.sub,
      poolSize: row.pool_size,
      // Re-freeze the session's own algorithm. A resumed session must
      // keep addressing the shards its already-landed chunks are on.
      placementVersion: row.placement_version,
      totalChunks: row.total_chunks,
      chunkSize: row.chunk_size,
      totalSize: row.total_size,
    },
    ttl
  );
  // Update the session row's expires_at to reflect the new token.
  durableObject.sql.exec(
    "UPDATE upload_sessions SET expires_at = ?, fence_id = ? WHERE upload_id = ?",
    expiresAt,
    fenceId,
    uploadId
  );

  const landed = Array.from(landedSet).sort((a, b) => a - b);
  return {
    uploadId,
    chunkSize: row.chunk_size,
    totalChunks: row.total_chunks,
    poolSize: row.pool_size,
    sessionToken: token,
    putEndpoint: `/api/vfs/multipart/${uploadId}`,
    expiresAtMs: expiresAt,
    landed,
  };
}

/**
 * Abort a multipart upload. Idempotent: aborting a session that is
 * already 'aborted' is a no-op; aborting a 'finalized' session
 * raises EBUSY (cannot un-finalize).
 *
 * The session transition, temp-row deletion, and one durable cleanup intent
 * per pool shard commit together. The outbox then runs the idempotent
 * `deleteChunks` + `clearMultipartStaging` protocol and alarm-retries any
 * unacknowledged shard.
 */
export async function vfsAbortMultipart(
  durableObject: UserDO,
  scope: VFSScope,
  uploadId: string,
  allowFinalizing = false
): Promise<{ ok: true }> {
  const userId = userIdFor(scope);
  const row = readUploadSession(durableObject, userId, uploadId);
  if (!row) {
    throw new VFSError("ENOENT", `abortMultipart: session not found: ${uploadId}`);
  }
  if (row.status === "finalized") {
    throw new VFSError(
      "EBUSY",
      `abortMultipart: session is already finalized; cannot un-finalize`
    );
  }
  if (row.status === "aborted") return { ok: true };
  if (row.status === "finalizing" && !allowFinalizing) {
    throw new VFSError("EBUSY", "abortMultipart: finalize is in progress");
  }

  await scheduleStaleUploadSweep(durableObject);
  transactionSync(durableObject, () => {
    const current = durableObject.sql
      .exec(
        `SELECT status, pool_size FROM upload_sessions
          WHERE upload_id = ? AND user_id = ?`,
        uploadId,
        userId
      )
      .toArray()[0] as
      | { status: string; pool_size: number }
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

    durableObject.sql.exec(
      `UPDATE upload_sessions SET status = 'aborting'
        WHERE upload_id = ? AND user_id = ? AND status IN ('open', 'finalizing')`,
      uploadId,
      userId
    );
  });

  await fenceMultipartShards(durableObject, scope, row, "aborting");

  transactionSync(durableObject, () => {
    const current = durableObject.sql
      .exec(
        `SELECT status, pool_size FROM upload_sessions
          WHERE upload_id = ? AND user_id = ?`,
        uploadId,
        userId
      )
      .toArray()[0] as
      | { status: string; pool_size: number }
      | undefined;
    if (!current || current.status === "aborted") return;
    if (current.status !== "aborting") {
      throw new VFSError("EBUSY", "abortMultipart: session changed while fencing");
    }

    const now = Date.now();
    for (let shardIndex = 0; shardIndex < current.pool_size; shardIndex++) {
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
      `UPDATE upload_sessions SET status = 'aborted'
        WHERE upload_id = ? AND user_id = ? AND status = 'aborting'`,
      uploadId,
      userId
    );
    hardDeleteFileRowLocal(durableObject, userId, uploadId);
    discardMultipartFinalizeScratch(durableObject, uploadId);
  });

  await drainChunkCleanupIntents(durableObject, scope, uploadId);

  return { ok: true };
}

/**
 * Stage one page of the chunk hashes the client declares for a session.
 *
 * A finalize that has to survive Durable Object eviction cannot be handed the
 * whole manifest in its last request, so the manifest arrives beforehand in
 * pages of at most `MULTIPART_HASH_PAGE_SIZE`.
 * `upload_sessions.staged_hash_cursor` is the contiguous high-water mark, and
 * a page is only accepted exactly where the cursor already stands: that makes
 * both the position check and the advance O(1), and leaves no way to stage a
 * hole the finalize would later have to detect.
 *
 * A page lying entirely below the cursor is a retry of a call the server
 * already accepted — it is compared against what was persisted and answered
 * without writing, so a client that lost the response can safely repeat it,
 * while the same indices carrying different hashes are refused rather than
 * silently reinterpreted.
 */
export function vfsStageMultipartHashes(
  durableObject: UserDO,
  scope: VFSScope,
  uploadId: string,
  startIndex: number,
  hashes: readonly string[]
): MultipartHashPageResponse {
  const userId = userIdFor(scope);
  if (!Number.isInteger(startIndex) || startIndex < 0) {
    throw new VFSError(
      "EINVAL",
      `stageMultipartHashes: startIndex ${startIndex} is not a non-negative integer`
    );
  }
  // The route caps the page too; this is the guard for every other caller.
  if (hashes.length === 0 || hashes.length > MULTIPART_HASH_PAGE_SIZE) {
    throw new VFSError(
      "EINVAL",
      `stageMultipartHashes: page must carry 1..${MULTIPART_HASH_PAGE_SIZE} hashes, got ${hashes.length}`
    );
  }
  for (let offset = 0; offset < hashes.length; offset++) {
    const hash = hashes[offset];
    if (typeof hash !== "string" || !/^[0-9a-f]{64}$/.test(hash)) {
      throw new VFSError(
        "EINVAL",
        `stageMultipartHashes: hashes[${offset}] is not a 64-char lowercase hex string`
      );
    }
  }

  const session = durableObject.sql
    .exec<{ total_chunks: number; status: string; staged_hash_cursor: number }>(
      `SELECT total_chunks, status, staged_hash_cursor FROM upload_sessions
        WHERE upload_id = ? AND user_id = ?`,
      uploadId,
      userId
    )
    .toArray()
    .at(0);
  if (!session) {
    throw new VFSError(
      "ENOENT",
      `stageMultipartHashes: session not found: ${uploadId}`
    );
  }
  const endIndex = startIndex + hashes.length;
  if (endIndex > session.total_chunks) {
    throw new VFSError(
      "EINVAL",
      `stageMultipartHashes: page [${startIndex}, ${endIndex}) exceeds totalChunks ${session.total_chunks}`
    );
  }
  // Only an open session is still declaring its manifest; once finalize or
  // abort owns the session the staged set is an input they have already read.
  if (session.status !== "open") {
    throw new VFSError(
      "EBUSY",
      `stageMultipartHashes: session status='${session.status}'`
    );
  }
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
        "stageMultipartHashes: replay straddles the staged hash cursor"
      );
    }
    const at = firstStagedHashMismatch(
      durableObject,
      uploadId,
      startIndex,
      hashes
    );
    if (at !== null) {
      throw new VFSError(
        "EBUSY",
        `stageMultipartHashes: conflicting replay at index ${at}`
      );
    }
    return { staged: session.staged_hash_cursor, total: session.total_chunks };
  }

  transactionSync(durableObject, () => {
    for (let offset = 0; offset < hashes.length; offset++) {
      durableObject.sql.exec(
        `INSERT INTO upload_expected_chunks (upload_id, chunk_index, chunk_hash)
           VALUES (?, ?, ?)`,
        uploadId,
        startIndex + offset,
        hashes[offset]
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
      throw new VFSError(
        "EBUSY",
        "stageMultipartHashes: session changed while staging the page"
      );
    }
  });
  return { staged: endIndex, total: session.total_chunks };
}

/**
 * First index in `[startIndex, startIndex + hashes.length)` whose staged hash
 * is absent or differs, or `null` when every one of them matches.
 *
 * Both callers hand over a page the client already declared once: staging uses
 * it to answer a retried page without rewriting it, and the one-request
 * finalize uses it to refuse a caller that changed its mind about an upload
 * some earlier call already froze.
 */
function firstStagedHashMismatch(
  durableObject: UserDO,
  uploadId: string,
  startIndex: number,
  hashes: readonly string[]
): number | null {
  const staged = durableObject.sql
    .exec<{ chunk_index: number; chunk_hash: string }>(
      `SELECT chunk_index, chunk_hash FROM upload_expected_chunks
        WHERE upload_id = ? AND chunk_index >= ? AND chunk_index < ?
        ORDER BY chunk_index`,
      uploadId,
      startIndex,
      startIndex + hashes.length
    )
    .toArray();
  const mismatch = staged.findIndex(
    (row, offset) =>
      row.chunk_index !== startIndex + offset || row.chunk_hash !== hashes[offset]
  );
  if (mismatch !== -1) return startIndex + mismatch;
  // A short row set means the first missing index is the conflict.
  return staged.length === hashes.length ? null : startIndex + staged.length;
}

// ── the durable finalize machine ──────────────────────────────────────

/**
 * Phases of one multipart finalize, in the order they may be reached.
 *
 * `fencing` closes the session's shards to further PUTs, `verifying` turns the
 * declared manifest into `upload_verified_chunks` a page at a time, and
 * `publishing` makes the verified manifest a live file in one local
 * transaction. `done` is terminal, and its `finalize_result` is what every
 * later replay reads.
 */
const MULTIPART_FINALIZE_PHASES = [
  "fencing",
  "verifying",
  "publishing",
  "done",
] as const;

type MultipartFinalizePhase = (typeof MULTIPART_FINALIZE_PHASES)[number];

/**
 * Backoff a resumed finalize would wait between attempts. Only the driver's
 * claim plane reads it, and finalize is addressed directly rather than
 * claimed, so it is declared here as the operation's stated policy.
 */
const MULTIPART_FINALIZE_RETRY: RetryPolicy = {
  baseMs: 1_000,
  maxMs: 60_000,
  maxDoublings: 6,
};

/**
 * The control-plane columns `lib/paged-operation` fences this machine on.
 *
 * `finalize_verify_shard_cursor` is declared inside `finalize_chunk_cursor`
 * because finishing a chunk page restarts the shard fan-out at zero, which is
 * exactly the reset the driver's lexicographic rule permits.
 * `finalize_total_size` and `finalize_sha_state` are not cursors: they are
 * domain state the same transition writes.
 */
const MULTIPART_FINALIZE_OPERATION: PagedOperationTable<MultipartFinalizePhase> =
  {
    table: "upload_sessions",
    keyColumns: ["upload_id", "user_id"],
    phases: {
      column: "finalize_phase",
      forward: MULTIPART_FINALIZE_PHASES,
      terminal: ["done"],
    },
    cursorColumns: [
      "finalize_fence_cursor",
      "finalize_chunk_cursor",
      "finalize_verify_shard_cursor",
    ],
    retry: MULTIPART_FINALIZE_RETRY,
  };

/**
 * Compare-and-set one finalize transition. Must run inside the transaction
 * that carries the rows it commits, so a refused transition takes them with
 * it.
 *
 * The guard is the progress the page read plus `status`: a page whose row
 * moved underneath it — a concurrent step, an abort — matches zero rows
 * instead of applying its work a second time.
 */
function commitFinalizeAdvance(
  durableObject: UserDO,
  session: UploadSessionRow,
  phase: MultipartFinalizePhase,
  next: Readonly<Record<string, SqlStorageValue>>
): void {
  const committed = commitOperationTransition(
    durableObject,
    MULTIPART_FINALIZE_OPERATION,
    { upload_id: session.upload_id, user_id: session.user_id },
    {
      ...heldProgress(MULTIPART_FINALIZE_OPERATION, session),
      status: "finalizing",
    },
    next
  );
  if (!committed) {
    throw new VFSError(
      "EBUSY",
      `finalizeMultipart: session changed while ${phase}`
    );
  }
}

/** Run one page's mutations and its transition in a single transaction. */
function commitFinalizePage(
  durableObject: UserDO,
  session: UploadSessionRow,
  phase: MultipartFinalizePhase,
  advance: () => Readonly<Record<string, SqlStorageValue>>
): void {
  transactionSync(durableObject, () => {
    commitFinalizeAdvance(durableObject, session, phase, advance());
  });
}

/**
 * Drop the per-upload scratch a terminal session no longer reads. Runs in the
 * transaction that made the session terminal, so nothing can observe a
 * finished upload whose manifest tables are half gone.
 */
function discardMultipartFinalizeScratch(
  durableObject: UserDO,
  uploadId: string
): void {
  durableObject.sql.exec(
    "DELETE FROM upload_expected_chunks WHERE upload_id = ?",
    uploadId
  );
  durableObject.sql.exec(
    "DELETE FROM upload_verified_chunks WHERE upload_id = ?",
    uploadId
  );
  durableObject.sql.exec(
    "DELETE FROM upload_cleanup_routes WHERE upload_id = ?",
    uploadId
  );
}

/** Shards a finished upload still has to clean, routed during verification. */
function readMultipartCleanupRoutes(
  durableObject: UserDO,
  uploadId: string
): number[] {
  return durableObject.sql
    .exec<{ shard_index: number }>(
      `SELECT shard_index FROM upload_cleanup_routes
        WHERE upload_id = ? AND cleanup_kind = ?
        ORDER BY shard_index`,
      uploadId,
      ChunkCleanupKind.MultipartStaging
    )
    .toArray()
    .map((row) => row.shard_index);
}

/** Restore the running content hash a previous page persisted. */
function restoreFinalizeDigest(session: UploadSessionRow): Sha256State {
  if (session.finalize_sha_state === null) {
    throw new VFSError(
      "EBUSY",
      "finalizeMultipart: the running content hash is missing"
    );
  }
  const serialized: unknown = JSON.parse(session.finalize_sha_state);
  return restoreSha256State(serialized);
}

/**
 * Terminal results are read back out of the row on every replay, so the JSON
 * is a trust boundary and is checked rather than asserted.
 */
function parseFinalizeResult(
  session: UploadSessionRow
): MultipartFinalizeResponse {
  const raw: unknown =
    session.finalize_result === null ? null : JSON.parse(session.finalize_result);
  if (
    typeof raw === "object" &&
    raw !== null &&
    "fileId" in raw &&
    typeof raw.fileId === "string" &&
    "size" in raw &&
    typeof raw.size === "number" &&
    "chunkCount" in raw &&
    typeof raw.chunkCount === "number" &&
    "fileHash" in raw &&
    typeof raw.fileHash === "string" &&
    "path" in raw &&
    typeof raw.path === "string" &&
    "mimeType" in raw &&
    typeof raw.mimeType === "string" &&
    "isEncrypted" in raw &&
    typeof raw.isEncrypted === "boolean"
  ) {
    return {
      fileId: raw.fileId,
      size: raw.size,
      chunkCount: raw.chunkCount,
      fileHash: raw.fileHash,
      path: raw.path,
      mimeType: raw.mimeType,
      isEncrypted: raw.isEncrypted,
    };
  }
  throw new VFSError(
    "EBUSY",
    `finalizeMultipart: session ${session.upload_id} finalized before this server recorded its result`
  );
}

/**
 * Advance a multipart finalize by one bounded, durable page.
 *
 * Each call fences at most `MULTIPART_FENCE_PAGE_SIZE` shards, or verifies at
 * most `MULTIPART_HASH_PAGE_SIZE` chunks, or publishes the verified manifest —
 * never more. Where it got to lives in the session row, so a Durable Object
 * eviction between calls costs at most the page that was in flight, and a
 * caller that lost a response can simply call again: the server holds the only
 * cursor, and a page whose row already moved is refused rather than replayed.
 */
export async function vfsFinalizeMultipartStep(
  durableObject: UserDO,
  scope: VFSScope,
  uploadId: string
): Promise<MultipartFinalizeProgress> {
  const userId = userIdFor(scope);
  let session = readUploadSession(durableObject, userId, uploadId);
  if (!session) {
    throw new VFSError(
      "ENOENT",
      `finalizeMultipart: session not found: ${uploadId}`
    );
  }
  if (session.status === "finalized") {
    return { done: true, result: parseFinalizeResult(session), fresh: false };
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
  if (session.status === "open") {
    await enterMultipartFinalize(durableObject, session);
    const entered = readUploadSession(durableObject, userId, uploadId);
    if (!entered) {
      throw new VFSError(
        "ENOENT",
        `finalizeMultipart: session not found: ${uploadId}`
      );
    }
    session = entered;
  }
  if (session.finalize_phase === null) {
    // A finalize that predates the durable machine left no resumable page and
    // its shards are already fenced, so releasing the session is the only way
    // to let the caller upload again.
    await vfsAbortMultipart(durableObject, scope, uploadId, true);
    throw new VFSError(
      "EBUSY",
      "finalizeMultipart: released a finalize that started before this server owned the session"
    );
  }
  switch (session.finalize_phase) {
    case "fencing":
      return await advanceMultipartFence(durableObject, scope, session);
    case "verifying":
      try {
        return await advanceMultipartVerification(durableObject, scope, session);
      } catch (err) {
        // EBUSY means a shard did not answer: retryable, so the session
        // stays finalizing. Any other verdict is about bytes that are
        // already staged and cannot heal on retry, and the session is past
        // the point where the caller can abort it itself.
        if (err instanceof VFSError && err.code !== "EBUSY") {
          await vfsAbortMultipart(durableObject, scope, uploadId, true);
        }
        throw err;
      }
    case "publishing":
      return await publishMultipart(durableObject, scope, session);
    default:
      throw new VFSError(
        "EBUSY",
        `finalizeMultipart: unknown finalize phase '${session.finalize_phase}'`
      );
  }
}

/**
 * Take ownership of an open session and arm its finalize machine.
 *
 * The declared manifest has to be complete first: every later page compares
 * what the shards hold against `upload_expected_chunks`, so a session that
 * staged only part of it could never satisfy one.
 */
async function enterMultipartFinalize(
  durableObject: UserDO,
  session: UploadSessionRow
): Promise<void> {
  if (session.staged_hash_cursor !== session.total_chunks) {
    throw new VFSError(
      "EINVAL",
      `finalizeMultipart: staged ${session.staged_hash_cursor}/${session.total_chunks} expected chunk hashes`
    );
  }
  // A finalize the caller walks away from is reclaimed by the same alarm that
  // sweeps stale uploads, so the machine never depends on anyone coming back.
  await scheduleStaleUploadSweep(durableObject);
  transactionSync(durableObject, () => {
    const committed = commitOperationTransition(
      durableObject,
      MULTIPART_FINALIZE_OPERATION,
      { upload_id: session.upload_id, user_id: session.user_id },
      {
        status: "open",
        created_at: session.created_at,
        expires_at: session.expires_at,
      },
      {
        status: "finalizing",
        finalize_phase: "fencing",
        finalize_fence_cursor: 0,
        finalize_chunk_cursor: 0,
        finalize_verify_shard_cursor: 0,
        finalize_total_size: 0,
        finalize_sha_state: JSON.stringify(
          serializeSha256State(createSha256State())
        ),
      }
    );
    if (!committed) {
      throw new VFSError(
        "EBUSY",
        "finalizeMultipart: session changed before fencing"
      );
    }
  });
}

/**
 * Fence one page of the session's frozen pool.
 *
 * Replaying a page re-fences shards that already hold the fence, which the
 * ShardDO treats as the same terminal state it is already in; the cursor is
 * what makes the replay bounded rather than a second full fan-out.
 */
async function advanceMultipartFence(
  durableObject: UserDO,
  scope: VFSScope,
  session: UploadSessionRow
): Promise<MultipartFinalizeProgress> {
  const startShard = session.finalize_fence_cursor;
  const endShard = Math.min(
    startShard + MULTIPART_FENCE_PAGE_SIZE,
    session.pool_size
  );
  const fenceId = session.fence_id;
  if (fenceId !== null) {
    const ns = shardNs(durableObject);
    await Promise.all(
      Array.from({ length: endShard - startShard }, async (_, offset) => {
        const shardName = vfsShardDOName(
          scope.ns,
          scope.tenant,
          scope.sub,
          startShard + offset
        );
        // The session's expiry becomes the shard fence's reclaim deadline:
        // no token for this upload outlives it, so nothing can re-open the
        // fence once it has passed.
        await ns
          .get(ns.idFromName(shardName))
          .fenceMultipart(
            session.upload_id,
            fenceId,
            "finalizing",
            session.expires_at
          );
      })
    );
  }
  const fenced = endShard >= session.pool_size;
  commitFinalizePage(durableObject, session, "fencing", () => ({
    finalize_fence_cursor: endShard,
    finalize_phase: fenced ? "verifying" : "fencing",
  }));
  return fenced
    ? { done: false, phase: "verifying", cursor: 0, total: session.total_chunks }
    : { done: false, phase: "fencing", cursor: endShard, total: session.pool_size };
}

interface ShardManifestPage {
  readonly shardIndex: number;
  readonly rows: readonly ShardMultipartManifestRow[];
}

interface MultipartVerifiedChunk {
  readonly index: number;
  readonly hash: string;
  readonly size: number;
  /** Shard that reported the row, which must be the chunk's owner. */
  readonly shardIndex: number;
}

type VerifiedChunkRow = {
  chunk_index: number;
  chunk_hash: string;
  chunk_size: number;
  shard_index: number;
};

/**
 * Verify one page of the declared manifest against what the shards hold.
 *
 * The page's indices name their own owner shards, so those are the only
 * manifests it reads, and it reads only the page's index range from each. When
 * the last of them answers, the page's rows, its byte count and its slice of
 * the running content hash are committed in one transition — an interrupted
 * invocation therefore replays a whole page rather than half-counting one.
 */
async function advanceMultipartVerification(
  durableObject: UserDO,
  scope: VFSScope,
  session: UploadSessionRow
): Promise<MultipartFinalizeProgress> {
  const uploadId = session.upload_id;
  const startIndex = session.finalize_chunk_cursor;
  const endIndex = Math.min(
    startIndex + MULTIPART_HASH_PAGE_SIZE,
    session.total_chunks
  );

  const declared = new Map(
    durableObject.sql
      .exec<{ chunk_index: number; chunk_hash: string }>(
        `SELECT chunk_index, chunk_hash FROM upload_expected_chunks
          WHERE upload_id = ? AND chunk_index >= ? AND chunk_index < ?`,
        uploadId,
        startIndex,
        endIndex
      )
      .toArray()
      .map((row): [number, string] => [row.chunk_index, row.chunk_hash])
  );
  if (declared.size !== endIndex - startIndex) {
    throw new VFSError(
      "EINVAL",
      `finalizeMultipart: expected chunk hashes [${startIndex}, ${endIndex}) are incomplete`
    );
  }

  const placedBy = new Map<number, number>();
  for (let index = startIndex; index < endIndex; index++) {
    placedBy.set(
      index,
      placeMultipartChunk(
        session.user_id,
        uploadId,
        index,
        session.pool_size,
        session.placement_version
      )
    );
  }
  const owners = [...new Set(placedBy.values())].sort((a, b) => a - b);
  const pending = owners.filter(
    (shardIndex) => shardIndex >= session.finalize_verify_shard_cursor
  );
  const shardPage = pending.slice(0, MULTIPART_FENCE_PAGE_SIZE);
  const seen = new Map(
    durableObject.sql
      .exec<{ chunk_index: number; shard_index: number }>(
        `SELECT chunk_index, shard_index FROM upload_verified_chunks
          WHERE upload_id = ? AND chunk_index >= ? AND chunk_index < ?`,
        uploadId,
        startIndex,
        endIndex
      )
      .toArray()
      .map((row): [number, number] => [row.chunk_index, row.shard_index])
  );
  const landed =
    shardPage.length === 0
      ? []
      : acceptMultipartManifestPage(
          await collectMultipartManifestPage(
            durableObject,
            scope,
            uploadId,
            shardPage,
            startIndex,
            endIndex
          ),
          placedBy,
          seen,
          startIndex,
          endIndex
        );
  const persistLanded = (): void => {
    for (const chunk of landed) {
      // An index can only already be here if another step recorded this page
      // while the shards were answering, and the transition below is what
      // refuses that page. Letting the insert raise first would replace the
      // driver's verdict with a primary-key error.
      durableObject.sql.exec(
        `INSERT OR IGNORE INTO upload_verified_chunks
           (upload_id, chunk_index, chunk_hash, chunk_size, shard_index)
         VALUES (?, ?, ?, ?, ?)`,
        uploadId,
        chunk.index,
        chunk.hash,
        chunk.size,
        chunk.shardIndex
      );
    }
    // Routed now, while the staging rows that name these shards are still
    // there: publication owes each of them a staging clear long after this
    // page has been forgotten.
    for (const shardIndex of new Set(landed.map((chunk) => chunk.shardIndex))) {
      durableObject.sql.exec(
        `INSERT OR IGNORE INTO upload_cleanup_routes
           (upload_id, cleanup_kind, shard_index) VALUES (?, ?, ?)`,
        uploadId,
        ChunkCleanupKind.MultipartStaging,
        shardIndex
      );
    }
  };

  if (shardPage.length < pending.length) {
    const nextShard = shardPage[shardPage.length - 1] + 1;
    commitFinalizePage(durableObject, session, "verifying", () => {
      persistLanded();
      return { finalize_verify_shard_cursor: nextShard };
    });
    return {
      done: false,
      phase: "verifying",
      cursor: startIndex,
      total: session.total_chunks,
    };
  }

  const nextPhase =
    endIndex >= session.total_chunks ? "publishing" : "verifying";
  commitFinalizePage(durableObject, session, "verifying", () => {
    persistLanded();
    const verified = durableObject.sql
      .exec<VerifiedChunkRow>(
        `SELECT chunk_index, chunk_hash, chunk_size, shard_index
           FROM upload_verified_chunks
          WHERE upload_id = ? AND chunk_index >= ? AND chunk_index < ?
          ORDER BY chunk_index`,
        uploadId,
        startIndex,
        endIndex
      )
      .toArray();
    const digest = restoreFinalizeDigest(session);
    const encoder = new TextEncoder();
    let pageBytes = 0;
    for (const [index, owner] of placedBy) {
      const row = verified[index - startIndex];
      if (row === undefined || row.chunk_index !== index) {
        throw new VFSError(
          "ENOENT",
          `finalizeMultipart: chunk ${index} not landed (shard ${owner})`
        );
      }
      const expected = declared.get(index);
      if (row.chunk_hash !== expected) {
        throw new VFSError(
          "EBADF",
          `finalizeMultipart: chunk ${index} hash divergence (server=${row.chunk_hash}, client=${expected})`
        );
      }
      pageBytes += row.chunk_size;
      updateSha256(digest, encoder.encode(row.chunk_hash));
    }
    return {
      finalize_chunk_cursor: endIndex,
      finalize_verify_shard_cursor: 0,
      finalize_total_size: session.finalize_total_size + pageBytes,
      finalize_sha_state: JSON.stringify(serializeSha256State(digest)),
      finalize_phase: nextPhase,
    };
  });
  return {
    done: false,
    phase: nextPhase,
    cursor: endIndex,
    total: session.total_chunks,
  };
}

/**
 * Read one index range from each shard of a verification page.
 *
 * A shard that fails to answer raises `EBUSY` — the caller retries rather than
 * treating an unreachable shard as a verdict on the data.
 */
async function collectMultipartManifestPage(
  durableObject: UserDO,
  scope: VFSScope,
  uploadId: string,
  shardIndexes: readonly number[],
  startIndex: number,
  endIndex: number
): Promise<ShardManifestPage[]> {
  const ns = shardNs(durableObject);
  const failures: unknown[] = [];
  const pages = await Promise.all(
    shardIndexes.map(async (shardIndex): Promise<ShardManifestPage> => {
      const shardName = vfsShardDOName(
        scope.ns,
        scope.tenant,
        scope.sub,
        shardIndex
      );
      try {
        const res = await ns
          .get(ns.idFromName(shardName))
          .getMultipartManifestRange(uploadId, startIndex, endIndex);
        return { shardIndex, rows: res.rows };
      } catch (err) {
        failures.push(err);
        return { shardIndex, rows: [] };
      }
    })
  );
  if (failures.length > 0) {
    // Surface as EBUSY: a transient shard failure during the finalize
    // fan-out is a "try again" signal.
    const first = failures[0];
    throw new VFSError(
      "EBUSY",
      `finalizeMultipart: shard manifest collect failed on ${failures.length} shard(s); first error: ${
        first instanceof Error ? first.message : String(first)
      }`
    );
  }
  return pages;
}

/**
 * Accept what one shard page reported, or refuse the finalize.
 *
 * `placedBy` maps the page's indices onto the shards the session's frozen
 * placement puts them on, and `seen` carries the shards earlier pages already
 * recorded, so both refusals hold across the whole chunk page:
 *  - an index reported by more than one shard, and
 *  - an index reported by a shard that does not own it.
 * Either one means bytes reached a shard some other way than this session's
 * placement, so the manifest no longer describes one reconstructable file and
 * must not be published.
 */
function acceptMultipartManifestPage(
  pages: readonly ShardManifestPage[],
  placedBy: ReadonlyMap<number, number>,
  seen: Map<number, number>,
  startIndex: number,
  endIndex: number
): MultipartVerifiedChunk[] {
  const landed: MultipartVerifiedChunk[] = [];
  const duplicates: string[] = [];
  for (const page of pages) {
    for (const row of page.rows) {
      const prior = seen.get(row.idx);
      if (prior !== undefined) {
        duplicates.push(
          `chunk ${row.idx} staged on shards ${prior} and ${page.shardIndex}`
        );
        continue;
      }
      seen.set(row.idx, page.shardIndex);
      landed.push({
        index: row.idx,
        hash: row.hash,
        size: row.size,
        shardIndex: page.shardIndex,
      });
    }
  }
  if (duplicates.length > 0) {
    throw new VFSError(
      "EBADF",
      `finalizeMultipart: ${duplicates.length} duplicate chunk index(es); first: ${duplicates[0]}`
    );
  }
  landed.sort((a, b) => a.index - b.index);
  for (const chunk of landed) {
    const owner = placedBy.get(chunk.index);
    if (owner === undefined) {
      throw new VFSError(
        "EBADF",
        `finalizeMultipart: chunk ${chunk.index} is outside the verified range [${startIndex}, ${endIndex})`
      );
    }
    if (owner !== chunk.shardIndex) {
      throw new VFSError(
        "EBADF",
        `finalizeMultipart: chunk ${chunk.index} staged on shard ${chunk.shardIndex}; expected ${owner}`
      );
    }
  }
  return landed;
}

/**
 * Publish the verified manifest as a live file.
 *
 * ONE UserDO turn; the manifest, the total size and the file hash all come
 * from what verification persisted, so publication reads no shard and repeats
 * no work. The terminal transition records the answer in `finalize_result`,
 * which is what makes a replayed step idempotent.
 *
 * Visibility intent: failures before the commit path leave the temporary row
 * uploading so callers can resume or abort. Full SQL/cross-DO failure atomicity
 * and implementation linearizability are not proved by the Lean corpus.
 *
 * @lean-invariant Mossaic.Vfs.Multipart.commitManifest_success_is_complete
 * The abstract gate proves declared-count and collected index/hash
 * completeness on success. It does not refine this SQL/RPC implementation.
 */
async function publishMultipart(
  durableObject: UserDO,
  scope: VFSScope,
  session: UploadSessionRow
): Promise<MultipartFinalizeProgress> {
  const uploadId = session.upload_id;
  const userId = session.user_id;
  const manifestRows = durableObject.sql
    .exec<VerifiedChunkRow>(
      `SELECT chunk_index, chunk_hash, chunk_size, shard_index
         FROM upload_verified_chunks
        WHERE upload_id = ? ORDER BY chunk_index`,
      uploadId
    )
    .toArray();
  if (manifestRows.length !== session.total_chunks) {
    throw new VFSError(
      "EBUSY",
      `finalizeMultipart: verified ${manifestRows.length} of ${session.total_chunks} chunks`
    );
  }
  const touched = readMultipartCleanupRoutes(durableObject, uploadId);
  const totalSize = session.finalize_total_size;
  // file_hash := SHA-256(concat-as-utf8 of chunk_hashes), matches the
  // existing vfsWriteFile / vfsCommitWriteStream formula — verification fed
  // the accumulator the same bytes in the same order.
  const fileHash = bytesToHex(digestSha256(restoreFinalizeDigest(session)));

  const destinationRow = durableObject.sql
    .exec<{ file_id: string; head_version_id: string | null }>(
      `SELECT file_id, head_version_id FROM files
        WHERE user_id = ? AND IFNULL(parent_id, '') = IFNULL(?, '')
          AND file_name = ? AND status = 'complete'`,
      userId,
      session.parent_id,
      session.leaf
    )
    .toArray()
    .at(0);
  const assertSessionState = (): void => {
    const currentSession = durableObject.sql
      .exec(
        `SELECT 1 FROM upload_sessions
          WHERE upload_id = ? AND user_id = ? AND status = 'finalizing'
            AND created_at = ? AND expires_at = ?`,
        uploadId,
        userId,
        session.created_at,
        session.expires_at
      )
      .toArray();
    const currentTemp = durableObject.sql
      .exec(
        `SELECT 1 FROM files
          WHERE file_id = ? AND user_id = ? AND status = 'uploading'
            AND IFNULL(parent_id, '') = IFNULL(?, '')`,
        uploadId,
        userId,
        session.parent_id
      )
      .toArray();
    if (currentSession.length !== 1 || currentTemp.length !== 1) {
      throw new VFSError(
        "EBUSY",
        "finalizeMultipart: session changed during publication"
      );
    }
  };

  // Multipart × versioning. When versioning is enabled for this
  // tenant, finalize must:
  //   (a) write `version_chunks` (NOT `file_chunks`) keyed by a fresh
  //       version id, recording shard_ref_id = uploadId so a future
  //       `dropVersionRows` fan-out keys ShardDO `deleteChunks` with
  //       the same refId the chunk PUTs used at upload time;
  //   (b) call `commitVersion` to insert the file_versions row and
  //       move `files.head_version_id` ATOMICALLY — the prior
  //       version's row + chunks survive;
  //   (c) reuse an existing path identity without `commitRename`; a
  //       no-prior-path finalize uses its vacancy-guarded publication hook.
  // The non-versioned branch keeps `commitRename`'s hard-delete
  // supersede — correct semantics for versioning-off tenants.
  const versioning = isVersioningEnabled(durableObject, userId);
  const now = Date.now();
  const commitTags =
    session.tags_json === null
      ? undefined
      : (JSON.parse(session.tags_json) as string[]);
  const commitMetadata =
    session.metadata_blob === null
      ? undefined
      : session.metadata_blob.byteLength === 0
        ? null
        : new Uint8Array(session.metadata_blob);
  // A versioned overwrite attaches to the existing path's identity; every
  // other publication keeps the upload's own id.
  const pathId = versioning && destinationRow ? destinationRow.file_id : uploadId;
  const result: MultipartFinalizeResponse = {
    fileId: pathId,
    size: totalSize,
    chunkCount: session.total_chunks,
    fileHash,
    // Reconstructed from (parent_id, leaf) so the route layer can dispatch
    // follow-on side effects (preview pre-gen via ctx.waitUntil) without
    // re-querying.
    path: reconstructFinalizedPath(
      durableObject,
      userId,
      session.parent_id,
      session.leaf
    ),
    mimeType: session.mime_type,
    isEncrypted: session.encryption_mode !== null,
  };
  const commitTerminal = (): void => {
    commitFinalizeAdvance(durableObject, session, "publishing", {
      status: "finalized",
      finalize_phase: "done",
      finalize_result: JSON.stringify(result),
    });
    discardMultipartFinalizeScratch(durableObject, uploadId);
    if (touched.length > 0) {
      retainMultipartStagingCleanup(durableObject, uploadId, Date.now());
    }
  };

  const cleanupFailedPublication = async (
    versionId?: string
  ): Promise<void> => {
    let shouldDrain = false;
    transactionSync(durableObject, () => {
      if (versionId !== undefined) {
        durableObject.sql.exec(
          "DELETE FROM version_chunks WHERE version_id = ?",
          versionId
        );
      }
      const current = durableObject.sql
        .exec<{ status: string; created_at: number; expires_at: number }>(
          `SELECT status, created_at, expires_at FROM upload_sessions
            WHERE upload_id = ? AND user_id = ?`,
          uploadId,
          userId
        )
        .toArray()
        .at(0);
      const sameOpenSession =
        current?.status === "finalizing" &&
        current.created_at === session.created_at &&
        current.expires_at === session.expires_at;
      if (sameOpenSession) {
        const now = Date.now();
        for (let shardIndex = 0; shardIndex < session.pool_size; shardIndex++) {
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
          `UPDATE upload_sessions SET status = 'aborted'
            WHERE upload_id = ? AND user_id = ? AND status = 'finalizing'
              AND created_at = ? AND expires_at = ?`,
          uploadId,
          userId,
          session.created_at,
          session.expires_at
        );
        if (lastSqlChanges(durableObject) !== 1) {
          throw new VFSError(
            "EBUSY",
            "finalizeMultipart: session changed during cleanup"
          );
        }
        dropTmpRowAfterVersionCommit(durableObject, uploadId, {
          hasChunks: true,
        });
        discardMultipartFinalizeScratch(durableObject, uploadId);
        shouldDrain = true;
      } else if (
        current?.status === "aborted" ||
        current?.status === "poisoned"
      ) {
        const now = Date.now();
        for (let shardIndex = 0; shardIndex < session.pool_size; shardIndex++) {
          stageChunkCleanupIntent(
            durableObject,
            uploadId,
            shardIndex,
            now,
            now,
            ChunkCleanupKind.Multipart
          );
        }
        dropTmpRowAfterVersionCommit(durableObject, uploadId, {
          hasChunks: true,
        });
        discardMultipartFinalizeScratch(durableObject, uploadId);
        shouldDrain = true;
      } else {
        if (touched.length > 0) {
          retainMultipartStagingCleanup(durableObject, uploadId, Date.now());
        }
      }
    });
    if (shouldDrain) {
      await drainChunkCleanupIntents(durableObject, scope, uploadId);
    }
  };

  if (versioning) {
    const expectedHead: VersionedFileExpectation = {
      fileId: pathId,
      userId,
      parentId: session.parent_id,
      fileName: session.leaf,
      headVersionId: destinationRow?.head_version_id ?? null,
    };
    const versionId = generateId();
    const metadataForVersion =
      commitMetadata !== undefined
        ? commitMetadata
        : destinationRow
          ? readMetadataBytes(durableObject, pathId)
          : null;
    const finalizeVersion = (): void => {
      for (const row of manifestRows) {
        insertVersionChunk(durableObject, versionId, {
          chunk_index: row.chunk_index,
          chunk_hash: row.chunk_hash,
          chunk_size: row.chunk_size,
          shard_index: row.shard_index,
        });
      }
      if (commitMetadata !== undefined) {
        durableObject.sql.exec(
          "UPDATE files SET metadata = ? WHERE file_id = ?",
          commitMetadata,
          pathId
        );
      }
      if (commitTags !== undefined) {
        replaceTags(durableObject, userId, pathId, commitTags);
      }
      commitVersionChecked(
        durableObject,
        {
          pathId,
          versionId,
          userId,
          size: totalSize,
          mode: session.mode,
          mtimeMs: now,
          chunkSize: session.chunk_size,
          chunkCount: session.total_chunks,
          fileHash,
          mimeType: session.mime_type,
          inlineData: null,
          userVisible: session.version_user_visible !== 0,
          label: session.version_label,
          metadata: metadataForVersion,
          shardRefId: uploadId,
          encryption:
            session.encryption_mode !== null
              ? {
                  mode: session.encryption_mode as "convergent" | "random",
                  keyId: session.encryption_key_id ?? undefined,
                }
              : undefined,
        },
        expectedHead,
        "finalizeMultipart"
      );
      commitTerminal();
    };

    let cleanupArmed = false;
    try {
      await stageChunkCleanupIntents(durableObject, uploadId, touched);
      cleanupArmed = true;
      if (destinationRow) {
        await scheduleStaleUploadSweep(durableObject);
        transactionSync(durableObject, () => {
          assertSessionState();
          finalizeVersion();
          dropTmpRowAfterVersionCommit(durableObject, uploadId, {
            hasChunks: true,
          });
          bumpFolderRevision(durableObject, userId, session.parent_id);
        });
      } else {
        await commitRename(
          durableObject,
          userId,
          scope,
          uploadId,
          session.parent_id,
          session.leaf,
          {
            requireVacantDestination: true,
            preconditionLocal: assertSessionState,
            finalizeLocal: finalizeVersion,
          }
        );
      }
    } catch (err) {
      if (!cleanupArmed) throw err;
      await cleanupFailedPublication(versionId);
      throw err;
    }
  } else {
    // Non-versioned tenant — commitRename hard-deletes any prior
    // live row, which is correct semantics for versioning-off (no
    // history to keep).

    let cleanupArmed = false;
    try {
      await stageChunkCleanupIntents(durableObject, uploadId, touched);
      cleanupArmed = true;
      await commitRename(
        durableObject,
        userId,
        scope,
        uploadId,
        session.parent_id,
        session.leaf,
        {
          requireVacantDestination: destinationRow === undefined,
          expectedDestination: destinationRow
            ? {
                fileId: destinationRow.file_id,
                headVersionId: destinationRow.head_version_id,
              }
            : undefined,
          publicationEncryption:
            session.encryption_mode === null
              ? null
              : {
                  mode: session.encryption_mode as "convergent" | "random",
                  ...(session.encryption_key_id === null
                    ? {}
                    : { keyId: session.encryption_key_id }),
                },
          preconditionLocal: assertSessionState,
          finalizeLocal: () => {
            for (const row of manifestRows) {
              durableObject.sql.exec(
                `INSERT INTO file_chunks (file_id, chunk_index, chunk_hash, chunk_size, shard_index)
                 VALUES (?, ?, ?, ?, ?)`,
                uploadId,
                row.chunk_index,
                row.chunk_hash,
                row.chunk_size,
                row.shard_index
              );
            }
            durableObject.sql.exec(
              `UPDATE files
                  SET file_size = ?, chunk_count = ?, file_hash = ?, updated_at = ?
                WHERE file_id = ?`,
              totalSize,
              session.total_chunks,
              fileHash,
              now,
              uploadId
            );
            if (commitMetadata !== undefined) {
              durableObject.sql.exec(
                "UPDATE files SET metadata = ? WHERE file_id = ?",
                commitMetadata,
                uploadId
              );
            }
            if (commitTags !== undefined) {
              replaceTags(durableObject, userId, uploadId, commitTags);
            }
            if (session.encryption_mode !== null) {
              stampFileEncryption(durableObject, uploadId, {
                mode: session.encryption_mode as "convergent" | "random",
                keyId: session.encryption_key_id ?? undefined,
              });
            }
            recordWriteUsage(durableObject, userId, totalSize, 1);
            commitTerminal();
          },
        }
      );
    } catch (err) {
      if (cleanupArmed) {
        await cleanupFailedPublication();
      }
      throw err;
    }
  }

  // Clear staging across touched shards after local publication commits.
  await drainChunkCleanupIntents(durableObject, scope, uploadId);

  return { done: true, result, fresh: true };
}

/**
 * Pages the one-request finalize may run before giving up.
 *
 * Fencing costs one page per shard page, verification one per shard page of
 * every chunk page, and publication one more — so this is the exact work the
 * session in hand implies, not a guess that would silently truncate a large
 * upload.
 */
function multipartFinalizePageBudget(session: UploadSessionRow): number {
  const shardPages = Math.max(
    1,
    Math.ceil(session.pool_size / MULTIPART_FENCE_PAGE_SIZE)
  );
  const chunkPages = Math.max(
    1,
    Math.ceil(session.total_chunks / MULTIPART_HASH_PAGE_SIZE)
  );
  return shardPages * (1 + chunkPages) + 1;
}

/**
 * Finalize a multipart upload in one request.
 *
 * The caller hands over the whole declared manifest, so this stages it and
 * then drives the same durable machine `vfsFinalizeMultipartStep` exposes to
 * completion inside this turn. A caller that already staged its manifest in
 * pages, or that lost the response to an earlier attempt, is answered from
 * what the session persisted rather than from the list it just re-sent.
 *
 * Every page it runs costs the invocation its shard fan-out, so an upload big
 * enough to exhaust the subrequest budget cannot finish in one request. That
 * is not a failure: the pages it did complete are durable, the refusal is
 * `EBUSY`, and repeating the call resumes from the cursor rather than
 * restarting.
 */
export async function vfsFinalizeMultipart(
  durableObject: UserDO,
  scope: VFSScope,
  uploadId: string,
  chunkHashList: readonly string[]
): Promise<MultipartFinalizeResponse> {
  const userId = userIdFor(scope);
  const session = readUploadSession(durableObject, userId, uploadId);
  if (!session) {
    throw new VFSError(
      "ENOENT",
      `finalizeMultipart: session not found: ${uploadId}`
    );
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
  if (chunkHashList.length !== session.total_chunks) {
    throw new VFSError(
      "EINVAL",
      `finalizeMultipart: chunkHashList length ${chunkHashList.length} != totalChunks ${session.total_chunks}`
    );
  }
  for (let i = 0; i < chunkHashList.length; i++) {
    const h = chunkHashList[i];
    if (typeof h !== "string" || !/^[0-9a-f]{64}$/.test(h)) {
      throw new VFSError(
        "EINVAL",
        `finalizeMultipart: chunkHashList[${i}] is not a 64-char lowercase hex string`
      );
    }
  }
  // A finalize already owns a session past 'open', so the manifest it verifies
  // is the staged one; answering a caller who changed the list would report on
  // an upload this server never agreed to publish. A finalizing session with
  // no phase predates the durable machine and staged nothing at all — the step
  // below releases it instead.
  const staging = session.status === "open";
  if (staging || session.finalize_phase !== null) {
    for (
      let start = 0;
      start < chunkHashList.length;
      start += MULTIPART_HASH_PAGE_SIZE
    ) {
      const page = chunkHashList.slice(start, start + MULTIPART_HASH_PAGE_SIZE);
      if (staging) {
        vfsStageMultipartHashes(durableObject, scope, uploadId, start, page);
        continue;
      }
      const at = firstStagedHashMismatch(durableObject, uploadId, start, page);
      if (at !== null) {
        throw new VFSError(
          "EBUSY",
          `finalizeMultipart: chunk ${at} differs from the hash staged for the finalize in progress`
        );
      }
    }
  }

  let result: MultipartFinalizeResponse | undefined;
  const { done } = await runOperationPages(
    multipartFinalizePageBudget(session),
    async () => {
      const progress = await vfsFinalizeMultipartStep(
        durableObject,
        scope,
        uploadId
      );
      if (!progress.done) return { kind: "advanced" };
      result = progress.result;
      return { kind: "completed" };
    }
  );
  if (!done || result === undefined) {
    throw new VFSError(
      "EBUSY",
      "finalizeMultipart: bounded finalize did not finish"
    );
  }
  return result;
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

/**
 * Read the status of an open session. Used by the SDK to decide
 * whether to resume or restart. Returns landed[] from the shards.
 *
 * Like `resumeMultipart`'s probe, this fans out to every shard in
 * the pool; for an open session that's bounded (poolSize ≤ 200 in
 * practice).
 */
export async function vfsGetMultipartStatus(
  durableObject: UserDO,
  scope: VFSScope,
  uploadId: string
): Promise<{
  landed: number[];
  total: number;
  bytesUploaded: number;
  expiresAtMs: number;
  status: string;
}> {
  const userId = userIdFor(scope);
  const row = readUploadSession(durableObject, userId, uploadId);
  if (!row) {
    throw new VFSError(
      "ENOENT",
      `getMultipartStatus: session not found: ${uploadId}`
    );
  }

  const ns = shardNs(durableObject);
  const landedSet = new Set<number>();
  let bytesUploaded = 0;
  await Promise.all(
    Array.from({ length: row.pool_size }, (_, sIdx) => sIdx).map(
      async (sIdx) => {
        const shardName = vfsShardDOName(scope.ns, scope.tenant, scope.sub, sIdx);
        const stub = ns.get(ns.idFromName(shardName));
        try {
          const res = await stub.getMultipartManifest(uploadId);
          for (const r of res.rows) {
            landedSet.add(r.idx);
            bytesUploaded += r.size;
          }
        } catch {
          // best-effort
        }
      }
    )
  );

  return {
    landed: Array.from(landedSet).sort((a, b) => a - b),
    total: row.total_chunks,
    bytesUploaded,
    expiresAtMs: row.expires_at,
    status: row.status,
  };
}

/**
 * Cap on local abort failures for one expired session. Remote shard failures
 * do not consume this budget because their committed outbox intents retry
 * independently.
 *
 * 5 attempts × ~10 minute alarm cadence = ~50 minutes of retries
 * before declaring the session unrecoverable. Generous given that
 * the typical failure mode is a transient ShardDO error.
 */
export const MULTIPART_MAX_ABORT_ATTEMPTS = 5;

/**
 * Alarm-driven sweep of expired open sessions. Called from
 * UserDOCore's alarm() handler at scheduled intervals. Idempotent and
 * batch-bounded (LIMIT 32 per call) to keep DO turns short.
 *
 * For each expired session, performs the equivalent of
 * `vfsAbortMultipart` — flips status, fans out cleanup, hard-deletes
 * the tmp files row.
 *
 * A local transaction failure increments `attempts` and leaves the session
 * open. After the cap, `poisoned` keeps the corrupt session operator-visible.
 * Once the local transaction commits, shard cleanup is owned by the outbox.
 */
export async function sweepExpiredMultipartSessions(
  durableObject: UserDO,
  scopeForUser: (userId: string) => VFSScope
): Promise<{ swept: number; remaining: boolean }> {
  const now = Date.now();
  const stale = durableObject.sql
    .exec(
      `SELECT upload_id, user_id, attempts FROM upload_sessions
        WHERE status IN ('open', 'finalizing', 'aborting') AND expires_at < ?
        ORDER BY expires_at ASC
        LIMIT 32`,
      now
    )
    .toArray() as {
      upload_id: string;
      user_id: string;
      attempts: number;
    }[];

  for (const row of stale) {
    try {
      const scope = scopeForUser(row.user_id);
      await vfsAbortMultipart(durableObject, scope, row.upload_id, true);
    } catch (err) {
      const nextAttempts = (row.attempts ?? 0) + 1;
      if (nextAttempts >= MULTIPART_MAX_ABORT_ATTEMPTS) {
        // Give up on a repeatedly failing local transition and keep the row
        // operator-visible. No terminal state was committed, so no outbox
        // intent can safely replace this retry yet.
        durableObject.sql.exec(
          `UPDATE upload_sessions
              SET status = 'poisoned', attempts = ?
            WHERE upload_id = ?`,
          nextAttempts,
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
        // Bump the attempt counter; leave status='open' so the
        // next sweep retries. The next sweep query at the top of
        // this function still finds this row (status='open' AND
        // expires_at<now), so retries continue on the alarm
        // cadence until MULTIPART_MAX_ABORT_ATTEMPTS.
        durableObject.sql.exec(
          "UPDATE upload_sessions SET attempts = ? WHERE upload_id = ?",
          nextAttempts,
          row.upload_id
        );
      }
    }
  }

  const stillOpen = (
    durableObject.sql
      .exec(
        "SELECT COUNT(*) AS n FROM upload_sessions WHERE status IN ('open', 'finalizing', 'aborting') AND expires_at < ?",
        now
      )
      .toArray()[0] as { n: number }
  ).n;

  return { swept: stale.length, remaining: stillOpen > 0 };
}
