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
 *     one bounded page: at most 64 shard fences, at most 256 verified chunks,
 *     at most 256 rows of a displaced manifest, or the constant-size
 *     publication that switches the path onto the new file. Every page is
 *     resumable after Durable Object eviction.
 *
 *   - `vfsFinalizeMultipart` — the one-request entry point, which stages the
 *     hashes the caller declared and then drives that machine as far as
 *     publication inside a single turn. The chunk_refs were placed under
 *     `refId = uploadId` and publication keeps the temporary row's `file_id`,
 *     so they remain valid for the published file. Past publication the
 *     caller owns a file, so this returns the recorded result and leaves the
 *     bounded cleaning still owed to the alarm.
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
  MULTIPART_PROTOCOL_VERSION,
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
  dropTmpRowAfterVersionCommit,
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
import type { EncryptionMode } from "../../../../shared/encryption-types";
import {
  base64ToBytes,
  bytesToBase64,
  bytesToHex,
} from "../../../../shared/crypto";
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
  scheduleAlarmAt,
  scheduleStaleUploadSweep,
  stageChunkCleanupIntent,
  transactionSync,
} from "./internal-storage";

export interface VFSBeginMultipartOpts {
  size: number;
  /**
   * Control plane the caller can drive. Absent means it only knows the
   * one-request finalize, which the server then has to keep inside the
   * bounds one invocation can carry.
   */
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
  finalize_old_manifest_cursor: number;
  finalize_old_cleanup_cursor: number;
  finalize_cleanup_cursor: number;
  finalize_total_size: number;
  finalize_sha_state: string | null;
  finalize_context: string | null;
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
  assertFinalizeFitsOneRequest(
    "beginMultipart",
    finalTotalChunks,
    poolSize,
    opts.protocolVersion
  );
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
    ...(opts.protocolVersion === MULTIPART_PROTOCOL_VERSION
      ? { protocolVersion: MULTIPART_PROTOCOL_VERSION }
      : {}),
  };
}

/**
 * Shard round-trips one one-request finalize may spend.
 *
 * Fencing and verification are the only phases that leave the object, and a
 * Durable Object invocation may issue on the order of a thousand subrequests.
 * Half of that is what a caller who only knows the one-request finalize may
 * commit to at begin; the rest stays for the cleanup drain publication
 * triggers. An upload past this ceiling is refused before a single chunk is
 * accepted rather than after every one of them has been.
 */
const MULTIPART_ONE_REQUEST_FINALIZE_MAX_FANOUT = 500;

/**
 * Bounded pages one one-request finalize may run before it asks the caller to
 * call again. Begin bounds the pages an upload of its own needs; this also
 * bounds the routing scan over a file the upload displaces, whose size the
 * upload itself says nothing about. Every page it did run is durable, so the
 * next call resumes from the cursor instead of restarting.
 */
const MULTIPART_ONE_REQUEST_FINALIZE_MAX_PAGES = 512;

/** Shard round-trips finalizing this shape costs, fencing through publication. */
function multipartFinalizeFanout(
  totalChunks: number,
  poolSize: number
): number {
  const chunkPages = Math.ceil(totalChunks / MULTIPART_HASH_PAGE_SIZE);
  // Fencing walks the whole pool once, and every chunk page walks the shards
  // its own indices are placed on — at most one page's worth of them.
  return (
    poolSize + chunkPages * Math.min(poolSize, MULTIPART_HASH_PAGE_SIZE)
  );
}

/**
 * Refuse work one request cannot spend its way through.
 *
 * Begin and resume call this so a caller that only knows the one-request
 * finalize is turned away before a single chunk is accepted; the one-request
 * finalize calls it so a session opened for the paged control plane cannot be
 * driven down this path either. A caller that declared the paged control plane
 * at begin is bounded by nothing here — it finalizes a page per request.
 */
function assertFinalizeFitsOneRequest(
  operation: string,
  totalChunks: number,
  poolSize: number,
  protocolVersion?: number
): void {
  if (protocolVersion === MULTIPART_PROTOCOL_VERSION) return;
  const fanout = multipartFinalizeFanout(totalChunks, poolSize);
  if (fanout <= MULTIPART_ONE_REQUEST_FINALIZE_MAX_FANOUT) return;
  throw new VFSError(
    "EINVAL",
    `${operation}: finalizing ${totalChunks} chunks across ${poolSize} shards costs ${fanout} shard round-trips (cap ${MULTIPART_ONE_REQUEST_FINALIZE_MAX_FANOUT}); declare protocolVersion ${MULTIPART_PROTOCOL_VERSION} and drive finalize in pages`
  );
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
  // The session's dimensions are frozen, so a caller that resumes it without
  // the paged control plane is held to the same ceiling a cold begin is.
  assertFinalizeFitsOneRequest(
    "resumeMultipart",
    row.total_chunks,
    row.pool_size,
    opts.protocolVersion
  );

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
    ...(opts.protocolVersion === MULTIPART_PROTOCOL_VERSION
      ? { protocolVersion: MULTIPART_PROTOCOL_VERSION }
      : {}),
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
    // The temporary row takes its own chunk rows with it; a candidate
    // version's are reachable only through the frozen context.
    hardDeleteFileRowLocal(durableObject, userId, uploadId);
    discardMultipartFinalizeScratch(durableObject, row);
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
 * `fencing` closes the session's shards to further PUTs. `verifying` turns the
 * declared manifest into `upload_verified_chunks` a page at a time and copies
 * each page straight into the destination manifest, so no phase ever holds the
 * whole manifest. `preparing` routes the shards a displaced file's chunks live
 * on. `publishing` is then a constant-size head switch: it materialises
 * nothing, and the routing it froze becomes executable cleanup in the same
 * transaction. What the switch left owed is paged off afterwards —
 * `cleaning_old_manifest` drops the displaced file's chunk rows,
 * `cleaning` drops this upload's scratch — and `done` is terminal, its
 * `finalize_result` what every later replay reads.
 */
const MULTIPART_FINALIZE_PHASES = [
  "fencing",
  "verifying",
  "preparing",
  "publishing",
  "cleaning_old_manifest",
  "cleaning",
  "done",
] as const;

type MultipartFinalizePhase = (typeof MULTIPART_FINALIZE_PHASES)[number];

/**
 * Backoff a resumed finalize would wait between attempts. Only the driver's
 * claim plane reads it, and finalize is addressed directly by `upload_id`
 * rather than claimed, so it is declared here as the operation's stated
 * policy and the alarm's fixed maintenance cadence is what actually paces
 * retries.
 *
 * The driver's poison policy is deliberately not consumed either. Before
 * publication a deterministic failure releases the session outright, which is
 * a stronger disposition than a retry cap; after publication the file exists
 * and the reaping it left owed is still owed however often it fails, which is
 * exactly the case `PoisonPolicy` says must never be abandoned.
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
 * exactly the reset the driver's lexicographic rule permits. The three that
 * follow belong to one phase each and only ever advance.
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
      "finalize_old_manifest_cursor",
      "finalize_old_cleanup_cursor",
      "finalize_cleanup_cursor",
    ],
    retry: MULTIPART_FINALIZE_RETRY,
  };

/**
 * Cursors that seek by a chunk index start one step below zero, so the first
 * page selects `> -1` and takes index zero.
 */
const MULTIPART_SEEK_CURSOR_START = -1;

/**
 * How long a published session may wait before the alarm picks up the
 * cleaning it still owes. Only a caller that walked away pays it: a caller
 * that keeps stepping finishes the cleaning itself.
 */
const MULTIPART_CLEANING_RESUME_DELAY_MS = 60_000;

/** Encryption a session was opened with, narrowed out of its text columns. */
interface MultipartEncryption {
  readonly mode: EncryptionMode;
  readonly keyId: string | null;
}

/**
 * Everything publication is allowed to decide, decided once and written to
 * `upload_sessions.finalize_context` before a single shard is fenced.
 *
 * Publication re-derives each of these from live state and refuses rather than
 * apply a decision the operation never made: a destination that appeared,
 * moved or was replaced, versioning switched underneath the upload, a rewritten
 * metadata blob or tag set, a changed encryption stamp. The serialized form is
 * also the compare-and-set token every page of the machine is fenced on, so a
 * context rewritten mid-flight invalidates the pages that read it.
 */
interface MultipartFinalizeContext {
  readonly schema: 1;
  /** Non-null exactly when the tenant had versioning on at freeze time. */
  readonly version: { readonly versionId: string } | null;
  /** Path identity the publication attaches to. */
  readonly pathId: string;
  readonly parentId: string | null;
  readonly leaf: string;
  /** The live row this publication displaces, or null for a vacant path. */
  readonly destination: {
    readonly fileId: string;
    readonly headVersionId: string | null;
  } | null;
  readonly encryption: MultipartEncryption | null;
  /** Absent means "inherit"; `{ base64: null }` means "clear". */
  readonly metadata: { readonly base64: string | null } | null;
  /** Absent means "inherit". */
  readonly tags: readonly string[] | null;
  readonly committedAt: number;
}

function readUploadSessionOrThrow(
  durableObject: UserDO,
  userId: string,
  uploadId: string,
  operation: string
): UploadSessionRow {
  const session = readUploadSession(durableObject, userId, uploadId);
  if (!session) {
    throw new VFSError("ENOENT", `${operation}: session not found: ${uploadId}`);
  }
  return session;
}

/** The session's encryption stamp, refusing a column this server never wrote. */
function sessionEncryption(
  session: UploadSessionRow
): MultipartEncryption | null {
  const mode = session.encryption_mode;
  if (mode === null) return null;
  if (mode !== "convergent" && mode !== "random") {
    throw new VFSError(
      "EBUSY",
      `finalizeMultipart: session ${session.upload_id} carries unknown encryption mode '${mode}'`
    );
  }
  return { mode, keyId: session.encryption_key_id };
}

function encryptionStamp(
  encryption: MultipartEncryption | null
): EncryptionStampOpts | undefined {
  if (encryption === null) return undefined;
  return encryption.keyId === null
    ? { mode: encryption.mode }
    : { mode: encryption.mode, keyId: encryption.keyId };
}

/** The metadata decision the session carries, in the context's shape. */
function sessionMetadata(
  session: UploadSessionRow
): { base64: string | null } | null {
  const blob = session.metadata_blob;
  if (blob === null) return null;
  return {
    base64:
      blob.byteLength === 0 ? null : bytesToBase64(new Uint8Array(blob)),
  };
}

/** The tag decision the session carries, in the context's shape. */
function sessionTags(session: UploadSessionRow): string[] | null {
  if (session.tags_json === null) return null;
  const raw: unknown = JSON.parse(session.tags_json);
  if (!Array.isArray(raw) || raw.some((tag) => typeof tag !== "string")) {
    throw new VFSError(
      "EBUSY",
      `finalizeMultipart: session ${session.upload_id} carries a malformed tag set`
    );
  }
  return raw.filter((tag): tag is string => typeof tag === "string");
}

/** The live row occupying the session's destination, if any. */
function readFinalizeDestination(
  durableObject: UserDO,
  userId: string,
  parentId: string | null,
  leaf: string
): { fileId: string; headVersionId: string | null } | null {
  const row = durableObject.sql
    .exec<{ file_id: string; head_version_id: string | null }>(
      `SELECT file_id, head_version_id FROM files
        WHERE user_id = ? AND IFNULL(parent_id, '') = IFNULL(?, '')
          AND file_name = ? AND status = 'complete'`,
      userId,
      parentId,
      leaf
    )
    .toArray()
    .at(0);
  return row === undefined
    ? null
    : { fileId: row.file_id, headVersionId: row.head_version_id };
}

/**
 * The displaced file whose local manifest publication leaves behind, or null.
 *
 * Only a non-versioned overwrite has one: a versioned overwrite keeps the
 * prior version's chunk rows, and a vacant destination displaces nothing.
 */
function displacedManifestOwner(
  context: MultipartFinalizeContext
): string | null {
  return context.version === null && context.destination !== null
    ? context.destination.fileId
    : null;
}

/**
 * Terminal results and frozen contexts are read back out of the row on every
 * replay, so their JSON is a trust boundary and is checked rather than
 * asserted.
 */
function parseFinalizeContext(
  session: UploadSessionRow
): MultipartFinalizeContext {
  const raw: unknown =
    session.finalize_context === null
      ? null
      : JSON.parse(session.finalize_context);
  const context = asFinalizeContext(raw);
  if (context === null) {
    throw new VFSError(
      "EBUSY",
      `finalizeMultipart: session ${session.upload_id} carries a frozen context this server cannot read`
    );
  }
  return context;
}

/** A decoded JSON object, or null when the value is anything else. */
function asRecord(value: unknown): Record<string, unknown> | null {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return null;
  }
  const record: Record<string, unknown> = { ...value };
  return record;
}

function isString(value: unknown): value is string {
  return typeof value === "string";
}

function asFinalizeContext(raw: unknown): MultipartFinalizeContext | null {
  const root = asRecord(raw);
  if (root === null || root.schema !== 1) return null;
  const { pathId, leaf, parentId, committedAt } = root;
  if (
    typeof pathId !== "string" ||
    typeof leaf !== "string" ||
    typeof committedAt !== "number" ||
    (parentId !== null && typeof parentId !== "string")
  ) {
    return null;
  }

  let version: { versionId: string } | null = null;
  if (root.version !== null) {
    const record = asRecord(root.version);
    if (record === null || typeof record.versionId !== "string") return null;
    version = { versionId: record.versionId };
  }

  let destination: MultipartFinalizeContext["destination"] = null;
  if (root.destination !== null) {
    const record = asRecord(root.destination);
    if (
      record === null ||
      typeof record.fileId !== "string" ||
      (record.headVersionId !== null && typeof record.headVersionId !== "string")
    ) {
      return null;
    }
    destination = {
      fileId: record.fileId,
      headVersionId: record.headVersionId,
    };
  }

  let encryption: MultipartEncryption | null = null;
  if (root.encryption !== null) {
    const record = asRecord(root.encryption);
    if (
      record === null ||
      (record.mode !== "convergent" && record.mode !== "random") ||
      (record.keyId !== null && typeof record.keyId !== "string")
    ) {
      return null;
    }
    encryption = { mode: record.mode, keyId: record.keyId };
  }

  let metadata: MultipartFinalizeContext["metadata"] = null;
  if (root.metadata !== null) {
    const record = asRecord(root.metadata);
    if (
      record === null ||
      (record.base64 !== null && typeof record.base64 !== "string")
    ) {
      return null;
    }
    metadata = { base64: record.base64 };
  }

  let tags: string[] | null = null;
  if (root.tags !== null) {
    if (!Array.isArray(root.tags)) return null;
    const declared: unknown[] = root.tags;
    if (!declared.every(isString)) return null;
    tags = declared;
  }

  return {
    schema: 1,
    version,
    pathId,
    parentId,
    leaf,
    destination,
    encryption,
    metadata,
    tags,
    committedAt,
  };
}

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
 * Compare-and-set one finalize transition. Must run inside the transaction
 * that carries the rows it commits, so a refused transition takes them with
 * it.
 *
 * The guard is the progress the page read, plus `status` and the frozen
 * context: a page whose row moved underneath it — a concurrent step, an abort,
 * a re-frozen decision — matches zero rows instead of applying its work a
 * second time.
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
      status: session.status,
      finalize_context: session.finalize_context,
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

/**
 * Run one page's transition and the mutations it commits in a single
 * transaction, transition first: a page whose row already moved is refused
 * before it writes anything, so the driver's verdict is what surfaces rather
 * than a constraint violation from rows a losing page tried to insert.
 */
function commitFinalizePage(
  durableObject: UserDO,
  session: UploadSessionRow,
  phase: MultipartFinalizePhase,
  next: Readonly<Record<string, SqlStorageValue>>,
  mutate?: () => void
): void {
  transactionSync(durableObject, () => {
    commitFinalizeAdvance(durableObject, session, phase, next);
    mutate?.();
  });
}

/**
 * Undo everything a finalize staged locally before it published.
 *
 * Called from the abort's terminal transaction. Besides the scratch, that
 * includes the destination manifest verification materialised a page at a time
 * so publication would not have to: the temporary row's chunks go with the row
 * itself, but a candidate version's are reachable only through the frozen
 * context, which an aborting session reads best-effort — a context this server
 * cannot read is one no verification page could have written against.
 *
 * A published finalize pages its scratch off instead, because by then it is as
 * large as the manifest.
 */
function discardMultipartFinalizeScratch(
  durableObject: UserDO,
  session: UploadSessionRow
): void {
  const uploadId = session.upload_id;
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
  const versionId = candidateVersionId(session);
  if (versionId !== null) {
    durableObject.sql.exec(
      "DELETE FROM version_chunks WHERE version_id = ?",
      versionId
    );
  }
}

/** Version id a finalize froze, if it froze one this server can still read. */
function candidateVersionId(session: UploadSessionRow): string | null {
  if (session.finalize_context === null) return null;
  let raw: unknown;
  try {
    raw = JSON.parse(session.finalize_context);
  } catch {
    return null;
  }
  return asFinalizeContext(raw)?.version?.versionId ?? null;
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
 * Advance a multipart finalize by one bounded, durable page.
 *
 * Each call fences at most `MULTIPART_FENCE_PAGE_SIZE` shards, verifies at
 * most `MULTIPART_HASH_PAGE_SIZE` chunks, routes or reaps at most that many
 * rows of a displaced manifest, or publishes — never more. Where it got to
 * lives in the session row, so a Durable Object eviction between calls costs
 * at most the page that was in flight, and a caller that lost a response can
 * simply call again: the server holds the only cursor, a page whose row
 * already moved is refused rather than replayed, and a session that already
 * published answers from what it recorded.
 */
export async function vfsFinalizeMultipartStep(
  durableObject: UserDO,
  scope: VFSScope,
  uploadId: string
): Promise<MultipartFinalizeProgress> {
  const userId = userIdFor(scope);
  let session = readUploadSessionOrThrow(
    durableObject,
    userId,
    uploadId,
    "finalizeMultipart"
  );
  if (session.status === "finalized") {
    return advanceMultipartCleaning(durableObject, session);
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
    await freezeMultipartFinalizeContext(durableObject, session);
    session = readUploadSessionOrThrow(
      durableObject,
      userId,
      uploadId,
      "finalizeMultipart"
    );
  }
  if (session.finalize_context === null) {
    // A finalize that predates the durable machine froze no decision and left
    // no resumable page, and its shards are already fenced, so releasing the
    // session is the only way to let the caller upload again.
    await vfsAbortMultipart(durableObject, scope, uploadId, true);
    throw new VFSError(
      "EBUSY",
      "finalizeMultipart: released a finalize that started before this server owned the session"
    );
  }
  const context = parseFinalizeContext(session);
  switch (session.finalize_phase) {
    case "fencing":
      return await advanceMultipartFence(durableObject, scope, session);
    case "verifying":
      try {
        return await advanceMultipartVerification(
          durableObject,
          scope,
          session,
          context
        );
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
    case "preparing":
      return advanceMultipartPreparation(durableObject, session, context);
    case "publishing":
      try {
        return await publishMultipart(durableObject, scope, session, context);
      } catch (err) {
        // Publication commits or it does not, and nothing it refuses on heals:
        // either the frozen decision no longer matches live state, or the
        // local write failed deterministically. Release the session so the
        // caller can upload again.
        await releaseUnpublishedMultipart(durableObject, scope, session);
        throw err;
      }
    default:
      throw new VFSError(
        "EBUSY",
        `finalizeMultipart: unknown finalize phase '${session.finalize_phase}'`
      );
  }
}

/**
 * Take ownership of an open session, freeze every decision publication will
 * later be held to, and arm the machine.
 *
 * The declared manifest has to be complete first: every later page compares
 * what the shards hold against `upload_expected_chunks`, so a session that
 * staged only part of it could never satisfy one.
 */
async function freezeMultipartFinalizeContext(
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
  const versionId = generateId();
  transactionSync(durableObject, () => {
    const userId = session.user_id;
    assertTemporaryRowPresent(durableObject, session);
    const encryption = sessionEncryption(session);
    enforceModeMonotonic(
      durableObject,
      userId,
      session.parent_id,
      session.leaf,
      encryptionStamp(encryption)
    );
    const versioning = isVersioningEnabled(durableObject, userId);
    const destination = readFinalizeDestination(
      durableObject,
      userId,
      session.parent_id,
      session.leaf
    );
    const context: MultipartFinalizeContext = {
      schema: 1,
      version: versioning ? { versionId } : null,
      // A versioned overwrite attaches to the existing path's identity; every
      // other publication keeps the upload's own id.
      pathId:
        versioning && destination !== null
          ? destination.fileId
          : session.upload_id,
      parentId: session.parent_id,
      leaf: session.leaf,
      destination,
      encryption,
      metadata: sessionMetadata(session),
      tags: sessionTags(session),
      committedAt: Date.now(),
    };
    const committed = commitOperationTransition(
      durableObject,
      MULTIPART_FINALIZE_OPERATION,
      { upload_id: session.upload_id, user_id: userId },
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
        finalize_old_manifest_cursor: MULTIPART_SEEK_CURSOR_START,
        finalize_old_cleanup_cursor: MULTIPART_SEEK_CURSOR_START,
        finalize_cleanup_cursor: 0,
        finalize_total_size: 0,
        finalize_sha_state: JSON.stringify(
          serializeSha256State(createSha256State())
        ),
        finalize_context: JSON.stringify(context),
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
 * Release a session whose publication did not commit, if it is still ours.
 *
 * A page that merely lost its fence must not release anything: the session
 * moved because another writer owns it — one that may already have published —
 * and aborting there would destroy a live file's session or mask that writer's
 * outcome with an unrelated error.
 */
async function releaseUnpublishedMultipart(
  durableObject: UserDO,
  scope: VFSScope,
  session: UploadSessionRow
): Promise<void> {
  const current = readUploadSession(
    durableObject,
    session.user_id,
    session.upload_id
  );
  if (
    current?.status !== "finalizing" ||
    current.finalize_context !== session.finalize_context
  ) {
    return;
  }
  await vfsAbortMultipart(durableObject, scope, session.upload_id, true);
}

/** The upload's temporary row, still where the session put it. */
function assertTemporaryRowPresent(
  durableObject: UserDO,
  session: UploadSessionRow
): void {
  const rows = durableObject.sql
    .exec(
      `SELECT 1 FROM files
        WHERE file_id = ? AND user_id = ? AND status = 'uploading'
          AND IFNULL(parent_id, '') = IFNULL(?, '')`,
      session.upload_id,
      session.user_id,
      session.parent_id
    )
    .toArray();
  if (rows.length !== 1) {
    throw new VFSError(
      "EBUSY",
      "finalizeMultipart: the upload's temporary file changed"
    );
  }
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
  commitFinalizePage(durableObject, session, "fencing", {
    finalize_fence_cursor: endShard,
    finalize_phase: fenced ? "verifying" : "fencing",
  });
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
 * Verify one page of the declared manifest against what the shards hold, and
 * copy it into the destination manifest.
 *
 * The page's indices name their own owner shards, so those are the only
 * manifests it reads, and it reads only the page's index range from each. When
 * the last of them answers, the page's rows, its byte count, its slice of the
 * running content hash and its slice of the destination manifest are committed
 * in one transition — an interrupted invocation therefore replays a whole page
 * rather than half-counting one, and publication inherits a manifest it never
 * has to build.
 */
async function advanceMultipartVerification(
  durableObject: UserDO,
  scope: VFSScope,
  session: UploadSessionRow,
  context: MultipartFinalizeContext
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
    commitFinalizePage(
      durableObject,
      session,
      "verifying",
      { finalize_verify_shard_cursor: nextShard },
      persistLanded
    );
    return {
      done: false,
      phase: "verifying",
      cursor: startIndex,
      total: session.total_chunks,
    };
  }

  const nextPhase =
    endIndex < session.total_chunks
      ? "verifying"
      : displacedManifestOwner(context) === null
        ? "publishing"
        : "preparing";
  transactionSync(durableObject, () => {
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
    commitFinalizeAdvance(durableObject, session, "verifying", {
      finalize_chunk_cursor: endIndex,
      finalize_verify_shard_cursor: 0,
      finalize_total_size: session.finalize_total_size + pageBytes,
      finalize_sha_state: JSON.stringify(serializeSha256State(digest)),
      finalize_phase: nextPhase,
    });
    materializeManifestPage(
      durableObject,
      context,
      uploadId,
      startIndex,
      endIndex
    );
  });
  return {
    done: false,
    phase: nextPhase,
    cursor: endIndex,
    total: session.total_chunks,
  };
}

/**
 * Copy one verified index range into the manifest publication will hand to
 * readers, set-based, so no page and no publication ever holds it in memory.
 *
 * A versioned publication builds the fresh version's manifest; every other one
 * builds the temporary row's, which `publishMultipart` renames into place
 * without touching a chunk row.
 */
function materializeManifestPage(
  durableObject: UserDO,
  context: MultipartFinalizeContext,
  uploadId: string,
  startIndex: number,
  endIndex: number
): void {
  const version = context.version;
  if (version !== null) {
    durableObject.sql.exec(
      `INSERT INTO version_chunks
         (version_id, chunk_index, chunk_hash, chunk_size, shard_index)
       SELECT ?, chunk_index, chunk_hash, chunk_size, shard_index
         FROM upload_verified_chunks
        WHERE upload_id = ? AND chunk_index >= ? AND chunk_index < ?`,
      version.versionId,
      uploadId,
      startIndex,
      endIndex
    );
    return;
  }
  durableObject.sql.exec(
    `INSERT INTO file_chunks
       (file_id, chunk_index, chunk_hash, chunk_size, shard_index)
     SELECT ?, chunk_index, chunk_hash, chunk_size, shard_index
       FROM upload_verified_chunks
      WHERE upload_id = ? AND chunk_index >= ? AND chunk_index < ?`,
    uploadId,
    uploadId,
    startIndex,
    endIndex
  );
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
 * Route one page of a displaced file's chunks onto the shards that hold them.
 *
 * A non-versioned overwrite orphans the prior file's bytes, and the shards
 * that own them can only be read off its manifest — which publication is about
 * to make unreachable. Recording the routing beforehand is what lets
 * publication owe the cleanup in constant size and lets the manifest itself be
 * reaped a page at a time afterwards. Verification skips this phase entirely
 * when the publication displaces nothing.
 */
function advanceMultipartPreparation(
  durableObject: UserDO,
  session: UploadSessionRow,
  context: MultipartFinalizeContext
): MultipartFinalizeProgress {
  const displaced = displacedManifestOwner(context);
  if (displaced === null) {
    throw new VFSError(
      "EBUSY",
      "finalizeMultipart: a displaced manifest routing has no displaced file"
    );
  }
  const cursor = session.finalize_old_manifest_cursor;
  const rows = durableObject.sql
    .exec<{ chunk_index: number; shard_index: number }>(
      `SELECT chunk_index, shard_index FROM file_chunks
        WHERE file_id = ? AND chunk_index > ?
        ORDER BY chunk_index LIMIT ?`,
      displaced,
      cursor,
      MULTIPART_HASH_PAGE_SIZE + 1
    )
    .toArray();
  const page = rows.slice(0, MULTIPART_HASH_PAGE_SIZE);
  const hasMore = rows.length > MULTIPART_HASH_PAGE_SIZE;
  const nextCursor = page.at(-1)?.chunk_index ?? cursor;
  commitFinalizePage(
    durableObject,
    session,
    "preparing",
    {
      finalize_old_manifest_cursor: nextCursor,
      finalize_phase: hasMore ? "preparing" : "publishing",
    },
    () => {
      for (const shardIndex of new Set(page.map((row) => row.shard_index))) {
        durableObject.sql.exec(
          `INSERT OR IGNORE INTO upload_cleanup_routes
             (upload_id, cleanup_kind, shard_index) VALUES (?, ?, ?)`,
          session.upload_id,
          ChunkCleanupKind.Chunks,
          shardIndex
        );
      }
    }
  );
  return hasMore
    ? displacedManifestProgress("preparing", nextCursor + 1)
    : {
        done: false,
        phase: "publishing",
        cursor: session.total_chunks,
        total: session.total_chunks,
      };
}

/**
 * Progress over a displaced file's manifest, whose length a page only learns
 * by reaching the end of it: `total` is what is known so far, which is one
 * past `cursor` for as long as another page remains.
 */
function displacedManifestProgress(
  phase: "preparing" | "cleaning",
  handled: number
): MultipartFinalizeProgress {
  return { done: false, phase, cursor: handled, total: handled + 1 };
}

/**
 * Publish the verified manifest as a live file, in constant size.
 *
 * Verification already wrote every chunk row and preparation already routed
 * every shard the switch orphans, so this reads no shard, materialises no
 * manifest, and issues a fixed number of statements over a fixed number of
 * rows however many chunks the upload has. What it does is decide: every
 * choice `finalize_context` froze is re-derived from live state and the
 * publication is refused rather than applied against a destination, a
 * versioning setting, a metadata blob, a tag set or an encryption stamp that
 * moved since the freeze.
 *
 * The head switch, the conversion of the frozen routing into executable
 * cleanup intents, and the terminal transition that records
 * `finalize_result` all commit together, so a caller can never observe a
 * published file whose cleanup nobody owes or whose result nobody recorded.
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
  session: UploadSessionRow,
  context: MultipartFinalizeContext
): Promise<MultipartFinalizeProgress> {
  const uploadId = session.upload_id;
  const userId = session.user_id;
  if (session.finalize_chunk_cursor !== session.total_chunks) {
    throw new VFSError(
      "EBUSY",
      `finalizeMultipart: verified ${session.finalize_chunk_cursor} of ${session.total_chunks} chunks`
    );
  }
  await scheduleStaleUploadSweep(durableObject);

  // file_hash := SHA-256(concat-as-utf8 of chunk_hashes), matches the
  // existing vfsWriteFile / vfsCommitWriteStream formula — verification fed
  // the accumulator the same bytes in the same order.
  const result: MultipartFinalizeResponse = {
    fileId: context.pathId,
    size: session.finalize_total_size,
    chunkCount: session.total_chunks,
    fileHash: bytesToHex(digestSha256(restoreFinalizeDigest(session))),
    // Reconstructed from (parent_id, leaf) so the route layer can dispatch
    // follow-on side effects (preview pre-gen via ctx.waitUntil) without
    // re-querying.
    path: reconstructFinalizedPath(
      durableObject,
      userId,
      context.parentId,
      context.leaf
    ),
    mimeType: session.mime_type,
    isEncrypted: context.encryption !== null,
  };
  const nextPhase = firstMultipartCleaningPhase(session, context);

  transactionSync(durableObject, () => {
    assertFrozenContextHolds(durableObject, session, context);
    const version = context.version;
    if (version === null) {
      publishMultipartOverwrite(durableObject, session, context, result);
    } else {
      publishMultipartVersion(durableObject, session, context, version, result);
    }
    bumpFolderRevision(durableObject, userId, context.parentId);
    commitFinalizeAdvance(durableObject, session, "publishing", {
      status: "finalized",
      finalize_phase: nextPhase,
      finalize_result: JSON.stringify(result),
      ...(nextPhase === "done" ? MULTIPART_TERMINAL_COMPACTION : {}),
    });
    stageRoutedMultipartCleanup(durableObject, session, context);
  });

  const displaced = displacedManifestOwner(context);
  await drainChunkCleanupIntents(
    durableObject,
    scope,
    displaced === null ? uploadId : [uploadId, displaced]
  );
  if (nextPhase === "done") return { done: true, result, fresh: true };
  // Nobody is obliged to come back for the cleaning the switch left owed.
  await scheduleAlarmAt(
    durableObject,
    Date.now() + MULTIPART_CLEANING_RESUME_DELAY_MS
  );
  return {
    done: false,
    phase: "cleaning",
    cursor: 0,
    total: session.total_chunks,
  };
}

/**
 * Refuse a publication whose frozen decision no longer describes live state.
 *
 * The session row is re-read here rather than trusted from the page that
 * entered publication: the phase, cursors and context are fenced by the
 * transition's compare-and-set, but the payload columns the decision was
 * derived from are not.
 */
function assertFrozenContextHolds(
  durableObject: UserDO,
  session: UploadSessionRow,
  context: MultipartFinalizeContext
): void {
  const userId = session.user_id;
  const current = readUploadSessionOrThrow(
    durableObject,
    userId,
    session.upload_id,
    "finalizeMultipart"
  );
  const refuse = (what: string): never => {
    throw new VFSError(
      "EBUSY",
      `finalizeMultipart: ${what} changed since the finalize froze`
    );
  };
  if (current.parent_id !== context.parentId || current.leaf !== context.leaf) {
    refuse("the destination path");
  }
  if (!sameFrozenValue(sessionEncryption(current), context.encryption)) {
    refuse("the encryption stamp");
  }
  if (!sameFrozenValue(sessionMetadata(current), context.metadata)) {
    refuse("the metadata");
  }
  if (!sameFrozenValue(sessionTags(current), context.tags)) {
    refuse("the tag set");
  }
  if (isVersioningEnabled(durableObject, userId) !== (context.version !== null)) {
    refuse("versioning");
  }
  const live = readFinalizeDestination(
    durableObject,
    userId,
    context.parentId,
    context.leaf
  );
  if (!sameFrozenValue(live, context.destination)) {
    refuse("the destination");
  }
  assertTemporaryRowPresent(durableObject, session);
  enforceModeMonotonic(
    durableObject,
    userId,
    context.parentId,
    context.leaf,
    encryptionStamp(context.encryption)
  );
}

/**
 * Structural equality for the values a frozen decision is compared against.
 * Both sides come out of the same builders, so their key order is fixed and a
 * serialized comparison is exact.
 */
function sameFrozenValue(left: unknown, right: unknown): boolean {
  return JSON.stringify(left) === JSON.stringify(right);
}

/**
 * Attach a fresh version to the path and move its head.
 *
 * Verification already wrote `version_chunks`; the prior version's rows
 * survive, which is what versioning is for, so nothing here is proportional to
 * either file.
 */
function publishMultipartVersion(
  durableObject: UserDO,
  session: UploadSessionRow,
  context: MultipartFinalizeContext,
  version: { readonly versionId: string },
  result: MultipartFinalizeResponse
): void {
  const userId = session.user_id;
  const pathId = context.pathId;
  const metadata = frozenMetadataBytes(context);
  if (context.destination === null) {
    // The temporary row is the path: name it and complete it in place.
    completeTemporaryRow(durableObject, session, context, result);
  }
  if (metadata !== undefined) {
    durableObject.sql.exec(
      "UPDATE files SET metadata = ? WHERE file_id = ?",
      metadata,
      pathId
    );
  }
  if (context.tags !== null) {
    replaceTags(durableObject, userId, pathId, context.tags);
  }
  const expectation: VersionedFileExpectation = {
    fileId: pathId,
    userId,
    parentId: context.parentId,
    fileName: context.leaf,
    headVersionId: context.destination?.headVersionId ?? null,
  };
  commitVersionChecked(
    durableObject,
    {
      pathId,
      versionId: version.versionId,
      userId,
      size: result.size,
      mode: session.mode,
      mtimeMs: context.committedAt,
      chunkSize: session.chunk_size,
      chunkCount: session.total_chunks,
      fileHash: result.fileHash,
      mimeType: session.mime_type,
      inlineData: null,
      userVisible: session.version_user_visible !== 0,
      label: session.version_label,
      metadata:
        metadata !== undefined
          ? metadata
          : context.destination === null
            ? null
            : readMetadataBytes(durableObject, pathId),
      shardRefId: session.upload_id,
      encryption: encryptionStamp(context.encryption),
    },
    expectation,
    "finalizeMultipart"
  );
  if (context.destination !== null) {
    // The bytes belong to the version now, so the temporary row is redundant.
    // Its chunk rows were never written: verification wrote `version_chunks`.
    dropTmpRowAfterVersionCommit(durableObject, session.upload_id, {
      hasChunks: false,
    });
  }
}

/**
 * Switch the path onto the temporary row, discarding any row it displaces.
 *
 * The displaced row's own manifest is deliberately left behind: it is as large
 * as the file it described, and `cleaning_old_manifest` reaps it in pages once
 * the switch is durable. Everything else the row owned — its tags, its stream
 * session, its byte accounting — is bounded and goes here.
 */
function publishMultipartOverwrite(
  durableObject: UserDO,
  session: UploadSessionRow,
  context: MultipartFinalizeContext,
  result: MultipartFinalizeResponse
): void {
  const userId = session.user_id;
  const uploadId = session.upload_id;
  const displaced = context.destination;
  if (displaced !== null) {
    // Metadata and tags are properties of the path, not of the file id, so an
    // overwrite that stated neither inherits both rather than dropping them.
    if (context.metadata === null) {
      durableObject.sql.exec(
        `UPDATE files SET metadata = (SELECT metadata FROM files WHERE file_id = ?)
          WHERE file_id = ? AND metadata IS NULL`,
        displaced.fileId,
        uploadId
      );
    }
    if (context.tags === null) {
      durableObject.sql.exec(
        `INSERT OR IGNORE INTO file_tags (path_id, tag, user_id, mtime_ms)
         SELECT ?, tag, user_id, ? FROM file_tags WHERE path_id = ?`,
        uploadId,
        context.committedAt,
        displaced.fileId
      );
    }
    const accounting = durableObject.sql
      .exec<{ file_size: number; inline_data: ArrayBuffer | null }>(
        "SELECT file_size, inline_data FROM files WHERE file_id = ?",
        displaced.fileId
      )
      .toArray()
      .at(0);
    durableObject.sql.exec(
      "DELETE FROM file_tags WHERE path_id = ?",
      displaced.fileId
    );
    durableObject.sql.exec(
      "DELETE FROM write_stream_sessions WHERE tmp_id = ?",
      displaced.fileId
    );
    durableObject.sql.exec(
      "DELETE FROM files WHERE file_id = ?",
      displaced.fileId
    );
    if (accounting !== undefined) {
      recordWriteUsage(
        durableObject,
        userId,
        -accounting.file_size,
        -1,
        accounting.inline_data === null ? 0 : -accounting.inline_data.byteLength
      );
    }
  }
  completeTemporaryRow(durableObject, session, context, result);
  const metadata = frozenMetadataBytes(context);
  if (metadata !== undefined) {
    durableObject.sql.exec(
      "UPDATE files SET metadata = ? WHERE file_id = ?",
      metadata,
      uploadId
    );
  }
  if (context.tags !== null) {
    replaceTags(durableObject, userId, uploadId, context.tags);
  }
  stampFileEncryption(
    durableObject,
    uploadId,
    encryptionStamp(context.encryption)
  );
  recordWriteUsage(durableObject, userId, result.size, 1);
}

/** Name the temporary row and complete it, or refuse if it moved. */
function completeTemporaryRow(
  durableObject: UserDO,
  session: UploadSessionRow,
  context: MultipartFinalizeContext,
  result: MultipartFinalizeResponse
): void {
  durableObject.sql.exec(
    `UPDATE files
        SET file_name = ?, status = 'complete', file_size = ?, chunk_count = ?,
            file_hash = ?, updated_at = ?
      WHERE file_id = ? AND user_id = ? AND status = 'uploading'
        AND IFNULL(parent_id, '') = IFNULL(?, '')`,
    context.leaf,
    result.size,
    result.chunkCount,
    result.fileHash,
    context.committedAt,
    session.upload_id,
    session.user_id,
    context.parentId
  );
  if (lastSqlChanges(durableObject) !== 1) {
    throw new VFSError(
      "EBUSY",
      "finalizeMultipart: the upload's temporary file changed"
    );
  }
}

/** Frozen metadata bytes, or `undefined` when the upload stated none. */
function frozenMetadataBytes(
  context: MultipartFinalizeContext
): Uint8Array | null | undefined {
  if (context.metadata === null) return undefined;
  return context.metadata.base64 === null
    ? null
    : base64ToBytes(context.metadata.base64);
}

/**
 * Turn the routing verification and preparation recorded into cleanup the
 * outbox will execute, one statement per reference.
 *
 * This is what makes the head switch and the obligation it creates the same
 * event: after the transaction, either the file is published and every shard
 * that owes bytes has a durable intent, or neither happened. An intent another
 * drain already claimed is fenced by `generation`, so re-arming it here costs
 * that drain its acknowledgement rather than the work.
 */
function stageRoutedMultipartCleanup(
  durableObject: UserDO,
  session: UploadSessionRow,
  context: MultipartFinalizeContext
): void {
  const now = Date.now();
  const stage = (cleanupKind: ChunkCleanupKind, refId: string): void => {
    durableObject.sql.exec(
      `INSERT INTO chunk_cleanup_intents
         (ref_id, shard_index, cleanup_kind, state, generation, provisional,
          created_at, updated_at, next_attempt_at, attempts, last_error)
       SELECT ?, shard_index, ?, 'pending', 0, 0, ?, ?, ?, 0, NULL
         FROM upload_cleanup_routes
        WHERE upload_id = ? AND cleanup_kind = ?
       ON CONFLICT(ref_id, shard_index) DO UPDATE SET
         state = 'pending',
         generation = chunk_cleanup_intents.generation + 1,
         provisional = 0,
         updated_at = excluded.updated_at,
         next_attempt_at = MIN(chunk_cleanup_intents.next_attempt_at,
                               excluded.next_attempt_at),
         attempts = 0,
         last_error = NULL`,
      refId,
      cleanupKind,
      now,
      now,
      now,
      session.upload_id,
      cleanupKind
    );
  };
  stage(ChunkCleanupKind.MultipartStaging, session.upload_id);
  const displaced = displacedManifestOwner(context);
  if (displaced !== null) stage(ChunkCleanupKind.Chunks, displaced);
  durableObject.sql.exec(
    "DELETE FROM upload_cleanup_routes WHERE upload_id = ?",
    session.upload_id
  );
}

/**
 * Columns a terminal session stops needing. Written by whichever transition
 * reaches `done`, because no transition may follow it.
 */
const MULTIPART_TERMINAL_COMPACTION: Readonly<
  Record<string, SqlStorageValue>
> = {
  metadata_blob: null,
  tags_json: null,
  finalize_context: null,
  finalize_sha_state: null,
};

/** The first cleaning a published session owes, or `done` when it owes none. */
function firstMultipartCleaningPhase(
  session: UploadSessionRow,
  context: MultipartFinalizeContext
): MultipartFinalizePhase {
  if (displacedManifestOwner(context) !== null) return "cleaning_old_manifest";
  return session.total_chunks > 0 ? "cleaning" : "done";
}

/**
 * Advance one bounded page of the cleaning a published session still owes, or
 * answer from what publication recorded once it owes none.
 *
 * Every page here runs after the head switch committed, so none of it can fail
 * the finalize: the worst a refused page costs is a later retry, and the
 * result the caller gets is the one publication persisted either way.
 */
function advanceMultipartCleaning(
  durableObject: UserDO,
  session: UploadSessionRow
): MultipartFinalizeProgress {
  const result = parseFinalizeResult(session);
  switch (session.finalize_phase) {
    case "cleaning_old_manifest":
      return reapDisplacedManifestPage(durableObject, session, result);
    case "cleaning":
      return reapFinalizeScratchPage(durableObject, session, result);
    // `done`, or a session finalized before this machine existed, which the
    // schema migration adopted as terminal.
    case "done":
    case null:
      return { done: true, result, fresh: false };
    default:
      // Publication is the only transition into a finalized status and it
      // always names one of the three above, so this is a corrupt row rather
      // than an unfinished one — say so instead of calling it done and
      // stranding the scratch.
      throw new VFSError(
        "EBUSY",
        `finalizeMultipart: published session is in phase '${session.finalize_phase}'`
      );
  }
}

function reapDisplacedManifestPage(
  durableObject: UserDO,
  session: UploadSessionRow,
  result: MultipartFinalizeResponse
): MultipartFinalizeProgress {
  const context = parseFinalizeContext(session);
  const displaced = displacedManifestOwner(context);
  if (displaced === null) {
    throw new VFSError(
      "EBUSY",
      "finalizeMultipart: a displaced manifest cleanup has no displaced file"
    );
  }
  const cursor = session.finalize_old_cleanup_cursor;
  const rows = durableObject.sql
    .exec<{ chunk_index: number }>(
      `SELECT chunk_index FROM file_chunks
        WHERE file_id = ? AND chunk_index > ?
        ORDER BY chunk_index LIMIT ?`,
      displaced,
      cursor,
      MULTIPART_HASH_PAGE_SIZE + 1
    )
    .toArray();
  const page = rows.slice(0, MULTIPART_HASH_PAGE_SIZE);
  const hasMore = rows.length > MULTIPART_HASH_PAGE_SIZE;
  const nextCursor = page.at(-1)?.chunk_index ?? cursor;
  const nextPhase = hasMore
    ? "cleaning_old_manifest"
    : session.total_chunks > 0
      ? "cleaning"
      : "done";
  commitFinalizePage(
    durableObject,
    session,
    "cleaning_old_manifest",
    {
      finalize_old_cleanup_cursor: nextCursor,
      finalize_phase: nextPhase,
      ...(nextPhase === "done" ? MULTIPART_TERMINAL_COMPACTION : {}),
    },
    () => {
      durableObject.sql.exec(
        `DELETE FROM file_chunks
          WHERE file_id = ? AND chunk_index > ? AND chunk_index <= ?`,
        displaced,
        cursor,
        nextCursor
      );
    }
  );
  return nextPhase === "done"
    ? { done: true, result, fresh: true }
    : displacedManifestProgress("cleaning", nextCursor + 1);
}

function reapFinalizeScratchPage(
  durableObject: UserDO,
  session: UploadSessionRow,
  result: MultipartFinalizeResponse
): MultipartFinalizeProgress {
  const startIndex = session.finalize_cleanup_cursor;
  const endIndex = Math.min(
    startIndex + MULTIPART_HASH_PAGE_SIZE,
    session.total_chunks
  );
  const finished = endIndex >= session.total_chunks;
  commitFinalizePage(
    durableObject,
    session,
    "cleaning",
    {
      finalize_cleanup_cursor: endIndex,
      finalize_phase: finished ? "done" : "cleaning",
      ...(finished ? MULTIPART_TERMINAL_COMPACTION : {}),
    },
    () => {
      durableObject.sql.exec(
        `DELETE FROM upload_expected_chunks
          WHERE upload_id = ? AND chunk_index >= ? AND chunk_index < ?`,
        session.upload_id,
        startIndex,
        endIndex
      );
      durableObject.sql.exec(
        `DELETE FROM upload_verified_chunks
          WHERE upload_id = ? AND chunk_index >= ? AND chunk_index < ?`,
        session.upload_id,
        startIndex,
        endIndex
      );
    }
  );
  return finished
    ? { done: true, result, fresh: true }
    : {
        done: false,
        phase: "cleaning",
        cursor: endIndex,
        total: session.total_chunks,
      };
}

/**
 * Finalize a multipart upload in one request.
 *
 * The caller hands over the whole declared manifest, so this stages it and
 * then drives the same durable machine `vfsFinalizeMultipartStep` exposes. A
 * caller that already staged its manifest in pages, or that lost the response
 * to an earlier attempt, is answered from what the session persisted rather
 * than from the list it just re-sent.
 *
 * Publication is the point of no return: past it the caller owns a published
 * file, so this returns the recorded result the moment the session is
 * finalized and leaves whatever cleaning is still owed to the alarm. Before
 * it, an upload too large to finish inside one invocation's shard budget was
 * already refused at begin, and a session that still runs out of pages is
 * refused with `EBUSY` — the pages it did complete are durable, so repeating
 * the call resumes from the cursor rather than restarting.
 */
export async function vfsFinalizeMultipart(
  durableObject: UserDO,
  scope: VFSScope,
  uploadId: string,
  chunkHashList: readonly string[]
): Promise<MultipartFinalizeResponse> {
  const userId = userIdFor(scope);
  const session = readUploadSessionOrThrow(
    durableObject,
    userId,
    uploadId,
    "finalizeMultipart"
  );
  if (session.status === "finalized") return parseFinalizeResult(session);
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
  assertFinalizeFitsOneRequest(
    "finalizeMultipart",
    session.total_chunks,
    session.pool_size
  );
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
  // no frozen context predates the durable machine and staged nothing at all —
  // the step below releases it instead.
  const staging = session.status === "open";
  if (staging || session.finalize_context !== null) {
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
  const publishedResult = (): MultipartFinalizeResponse | undefined => {
    const current = readUploadSession(durableObject, userId, uploadId);
    return current?.status === "finalized"
      ? parseFinalizeResult(current)
      : undefined;
  };
  try {
    await runOperationPages(
      MULTIPART_ONE_REQUEST_FINALIZE_MAX_PAGES,
      async () => {
        const progress = await vfsFinalizeMultipartStep(
          durableObject,
          scope,
          uploadId
        );
        if (progress.done) {
          result = progress.result;
          return { kind: "completed" };
        }
        result = publishedResult();
        return result === undefined
          ? { kind: "advanced" }
          : { kind: "completed" };
      }
    );
  } catch (err) {
    result = publishedResult();
    if (result === undefined) throw err;
  }
  if (result !== undefined) return result;
  throw new VFSError(
    "EBUSY",
    "finalizeMultipart: bounded finalize did not finish"
  );
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

/** Published sessions one alarm resumes, and pages it runs on each. */
export const MULTIPART_CLEANING_SESSION_LIMIT = 4;
export const MULTIPART_CLEANING_PAGES_PER_SESSION = 8;

/**
 * Alarm-driven cleaning of sessions that published but still owe the bounded
 * reaping the head switch left behind.
 *
 * Publication is terminal for the caller — it has its result and may never
 * call again — so nothing but the alarm is guaranteed to come back for the
 * displaced manifest and the upload's scratch. Bounded twice over: at most
 * `MULTIPART_CLEANING_SESSION_LIMIT` sessions, at most
 * `MULTIPART_CLEANING_PAGES_PER_SESSION` pages each. `remaining` keeps the
 * alarm cadence tight until every one of them reaches `done`.
 */
export async function resumeCleaningMultipartSessions(
  durableObject: UserDO,
  scopeForUser: (userId: string) => VFSScope
): Promise<{ resumed: number; remaining: boolean }> {
  const owed = durableObject.sql
    .exec<{ upload_id: string; user_id: string }>(
      `SELECT upload_id, user_id FROM upload_sessions
        WHERE status = 'finalized' AND finalize_phase IS NOT NULL
          AND finalize_phase != 'done'
        ORDER BY created_at, upload_id
        LIMIT ?`,
      MULTIPART_CLEANING_SESSION_LIMIT
    )
    .toArray();

  for (const row of owed) {
    try {
      await runOperationPages(
        MULTIPART_CLEANING_PAGES_PER_SESSION,
        async () => {
          const progress = await vfsFinalizeMultipartStep(
            durableObject,
            scopeForUser(row.user_id),
            row.upload_id
          );
          return progress.done ? { kind: "completed" } : { kind: "advanced" };
        }
      );
    } catch (err) {
      // The file is published either way; the reaping stays owed and the next
      // alarm retries it from the cursor.
      logError("multipart finalize cleaning failed", {}, err, {
        event: "multipart_cleaning_failed",
        uploadId: row.upload_id,
      });
    }
  }

  const remaining =
    durableObject.sql
      .exec(
        `SELECT 1 FROM upload_sessions
          WHERE status = 'finalized' AND finalize_phase IS NOT NULL
            AND finalize_phase != 'done'
          LIMIT 1`
      )
      .toArray().length > 0;
  return { resumed: owed.length, remaining };
}

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
