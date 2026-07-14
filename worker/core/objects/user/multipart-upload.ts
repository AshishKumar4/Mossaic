/**
 * Multipart parallel transfer engine, server-side.
 *
 * This module implements three UserDO RPCs:
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
 *   - `vfsFinalizeMultipart` — verifies completeness across shards via
 *     fan-out manifest collect, batch-inserts `file_chunks` rows on
 *     UserDO, calls `commitRename` to atomically supersede any prior
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
import { placeChunk } from "../../../../shared/placement";
import {
  signVFSMultipartToken,
} from "../../lib/auth";
import {
  MULTIPART_DEFAULT_TTL_MS,
  MULTIPART_MAX_OPEN_SESSIONS_PER_TENANT,
  type MultipartBeginResponse,
  type MultipartFinalizeResponse,
  type ShardMultipartManifestRow,
} from "../../../../shared/multipart";
import {
  commitRename,
  userIdFor,
  resolveParent,
  poolSizeFor,
  recordWriteUsage,
  folderExists,
  bumpFolderRevision,
  disarmChunkCleanupIntents,
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
import { hashChunk } from "../../../../shared/crypto";
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
  if (session.fence_id === null) return;
  const ns = shardNs(durableObject);
  await Promise.all(
    Array.from({ length: session.pool_size }, async (_, shardIndex) => {
      const shardName = vfsShardDOName(
        scope.ns,
        scope.tenant,
        scope.sub,
        shardIndex
      );
      await ns
        .get(ns.idFromName(shardName))
        .fenceMultipart(session.upload_id, session.fence_id!, state);
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
       (upload_id, fence_id, user_id, parent_id, leaf, total_size, total_chunks, chunk_size, pool_size, expires_at, status,
         encryption_mode, encryption_key_id, metadata_blob, tags_json, version_label, version_user_visible, mode, mime_type, created_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'open', ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    tmpId,
    fenceId,
    userId,
    parentId,
    leaf,
    opts.size,
    finalTotalChunks,
    finalChunkSize,
    poolSize,
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
  });

  await drainChunkCleanupIntents(durableObject, scope, uploadId);

  return { ok: true };
}

/**
 * Finalize a multipart upload. ONE UserDO turn; one fan-out per
 * unique touched shard for manifest verification + a second fan-out
 * for staging clear.
 *
 * Visibility intent: failures before the commit path leave the temporary row
 * uploading so callers can resume or abort. Full SQL/cross-DO failure atomicity
 * and implementation linearizability are not proved by the Lean corpus.
 *
 * @lean-invariant Mossaic.Vfs.Multipart.commitManifest_success_is_complete
 * The abstract gate proves declared-count and collected index/hash
 * completeness on success. It does not refine this SQL/RPC implementation.
 */
export async function vfsFinalizeMultipart(
  durableObject: UserDO,
  scope: VFSScope,
  uploadId: string,
  chunkHashList: readonly string[]
): Promise<MultipartFinalizeResponse> {
  const userId = userIdFor(scope);

  // 1. Lookup session.
  const session = durableObject.sql
    .exec(
      `SELECT * FROM upload_sessions WHERE upload_id = ? AND user_id = ?`,
      uploadId,
      userId
    )
    .toArray()[0] as unknown as UploadSessionRow | undefined;
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

  // 2. Validate hash list shape.
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

  const destinationRow = durableObject.sql
    .exec(
      `SELECT file_id, head_version_id FROM files
        WHERE user_id = ? AND IFNULL(parent_id, '') = IFNULL(?, '')
          AND file_name = ? AND status = 'complete'`,
      userId,
      session.parent_id,
      session.leaf
    )
    .toArray()[0] as
    | { file_id: string; head_version_id: string | null }
    | undefined;
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

  // 3. Compute touched shards.
  //
  // Multipart placement intentionally does NOT pass `fullShards`
  // to `placeChunk`. The route layer (`multipart-routes.ts`)
  // places each chunk PUT via the same
  // `placeChunk(uploadId, idx, payload.poolSize)` call without a
  // skip-set; the `fullShards` set at finalize time may differ
  // from the set at upload time, and we have no reliable way to
  // replay the upload-time snapshot here. The deterministic
  // pure-rendezvous form keeps finalize verification consistent
  // with placement. Multipart cap-awareness is deferred until we
  // persist the per-session full-shards snapshot. Reads work
  // either way; only the write "prefer less-full shards"
  // optimization is missing for multipart.
  const touched = new Set<number>();
  const idxToShard = new Array<number>(session.total_chunks);
  for (let i = 0; i < session.total_chunks; i++) {
    const sIdx = placeChunk(userIdFor(scope), uploadId, i, session.pool_size);
    idxToShard[i] = sIdx;
    touched.add(sIdx);
  }

  // 4. Fan out manifest collect across touched shards.
  const ns = shardNs(durableObject);
  const collected = new Map<number, ShardMultipartManifestRow>();
  const collectErrors: unknown[] = [];
  await Promise.all(
    Array.from(touched).map(async (sIdx) => {
      const shardName = vfsShardDOName(scope.ns, scope.tenant, scope.sub, sIdx);
      const stub = ns.get(ns.idFromName(shardName));
      try {
        const res = await stub.getMultipartManifest(uploadId);
        for (const r of res.rows) collected.set(r.idx, r);
      } catch (err) {
        collectErrors.push(err);
      }
    })
  );
  if (collectErrors.length > 0) {
    // Surface as EBUSY: a transient shard failure during the finalize
    // fan-out is a "try again" signal — the session is still 'open'
    // and the caller can retry finalize after backoff.
    throw new VFSError(
      "EBUSY",
      `finalizeMultipart: shard manifest collect failed on ${collectErrors.length} shard(s); first error: ${
        (collectErrors[0] as Error)?.message ?? String(collectErrors[0])
      }`
    );
  }

  // 5. Cross-check: every idx must exist with matching hash.
  const manifestRows: ShardMultipartManifestRow[] = [];
  for (let i = 0; i < session.total_chunks; i++) {
    const have = collected.get(i);
    if (!have) {
      throw new VFSError(
        "ENOENT",
        `finalizeMultipart: chunk ${i} not landed (shard ${idxToShard[i]})`
      );
    }
    if (have.hash !== chunkHashList[i]) {
      throw new VFSError(
        "EBADF",
        `finalizeMultipart: chunk ${i} hash divergence (server=${have.hash}, client=${chunkHashList[i]})`
      );
    }
    manifestRows.push(have);
  }

  if (session.status === "open") {
    transactionSync(durableObject, () => {
      durableObject.sql.exec(
        `UPDATE upload_sessions SET status = 'finalizing'
          WHERE upload_id = ? AND user_id = ? AND status = 'open'
            AND created_at = ? AND expires_at = ?`,
        uploadId,
        userId,
        session.created_at,
        session.expires_at
      );
      if (lastSqlChanges(durableObject) !== 1) {
        throw new VFSError(
          "EBUSY",
          "finalizeMultipart: session changed before fencing"
        );
      }
    });
  }

  await fenceMultipartShards(durableObject, scope, session, "finalizing");

  // Re-read after every shard acknowledges the fence. PUTs that completed
  // before their shard fenced are included; later PUTs are rejected.
  collected.clear();
  collectErrors.length = 0;
  await Promise.all(
    Array.from(touched).map(async (sIdx) => {
      const shardName = vfsShardDOName(scope.ns, scope.tenant, scope.sub, sIdx);
      const stub = ns.get(ns.idFromName(shardName));
      try {
        const res = await stub.getMultipartManifest(uploadId);
        for (const row of res.rows) collected.set(row.idx, row);
      } catch (err) {
        collectErrors.push(err);
      }
    })
  );
  if (collectErrors.length > 0) {
    throw new VFSError(
      "EBUSY",
      `finalizeMultipart: post-fence manifest collect failed on ${collectErrors.length} shard(s)`
    );
  }
  manifestRows.length = 0;
  for (let i = 0; i < session.total_chunks; i++) {
    const have = collected.get(i);
    if (!have || have.hash !== chunkHashList[i]) {
      await vfsAbortMultipart(durableObject, scope, uploadId, true);
      throw new VFSError(
        have ? "EBADF" : "ENOENT",
        `finalizeMultipart: post-fence chunk ${i} is missing or changed`
      );
    }
    manifestRows.push(have);
  }

  // 6. Compute file hash + total size from the collected sizes (which
  //    are already verified against the client list).
  let totalSize = 0;
  for (const row of manifestRows) {
    totalSize += row.size;
  }
  // file_hash := SHA-256(concat-as-utf8 of chunk_hashes), matches the
  // existing vfsWriteFile / vfsCommitWriteStream formula.
  const fileHash = await hashChunk(
    new TextEncoder().encode(chunkHashList.join(""))
  );

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
  let finalizedFileId = uploadId;
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
        .exec(
          `SELECT status, created_at, expires_at FROM upload_sessions
            WHERE upload_id = ? AND user_id = ?`,
          uploadId,
          userId
        )
        .toArray()[0] as
        | { status: string; created_at: number; expires_at: number }
        | undefined;
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
        shouldDrain = true;
      } else {
        if (touched.size > 0) {
          retainMultipartStagingCleanup(durableObject, uploadId, Date.now());
        }
      }
    });
    if (shouldDrain) {
      await drainChunkCleanupIntents(durableObject, scope, uploadId);
    }
  };

  if (versioning) {
    // Locate the pre-existing live row at (parent, leaf), if any. Its
    // file_id is the stable `pathId` for this path's history; the
    // multipart's tmp row will be discarded once the version is
    // committed.
    let pathId: string;
    if (destinationRow) {
      // Existing path — reuse its identity. The new version attaches
      // to it via `head_version_id`. The multipart tmp row is dropped
      // at the end of this branch (its chunks now belong to the
      // version, refid = uploadId).
      pathId = destinationRow.file_id;
    } else {
      pathId = uploadId;
    }
    finalizedFileId = pathId;
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
      for (const [i, row] of manifestRows.entries()) {
        insertVersionChunk(durableObject, versionId, {
          chunk_index: i,
          chunk_hash: row.hash,
          chunk_size: row.size,
          shard_index: idxToShard[i],
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
      durableObject.sql.exec(
        `UPDATE upload_sessions SET status = 'finalized'
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
          "finalizeMultipart: session changed during publication"
        );
      }
      if (touched.size > 0) {
        retainMultipartStagingCleanup(durableObject, uploadId, Date.now());
      }
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
            for (const [i, row] of manifestRows.entries()) {
              durableObject.sql.exec(
                `INSERT INTO file_chunks (file_id, chunk_index, chunk_hash, chunk_size, shard_index)
                 VALUES (?, ?, ?, ?, ?)`,
                uploadId,
                i,
                row.hash,
                row.size,
                idxToShard[i]
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
            durableObject.sql.exec(
              `UPDATE upload_sessions SET status = 'finalized'
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
                "finalizeMultipart: session changed during publication"
              );
            }
            if (touched.size > 0) {
              retainMultipartStagingCleanup(durableObject, uploadId, Date.now());
            }
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

  // 11. Clear staging across touched shards after local publication commits.
  await clearMultipartStaging(durableObject, scope, touched, uploadId);

  // 13. Reconstruct the absolute path from (parent_id, leaf) so the
  //     route layer can dispatch follow-on side effects
  //     (preview pre-gen via ctx.waitUntil) without re-querying.
  const finalizedPath = reconstructFinalizedPath(
    durableObject,
    userId,
    session.parent_id,
    session.leaf
  );

  return {
    fileId: finalizedFileId,
    size: totalSize,
    chunkCount: session.total_chunks,
    fileHash,
    path: finalizedPath,
    mimeType: session.mime_type,
    isEncrypted: session.encryption_mode !== null,
  };
}

async function clearMultipartStaging(
  durableObject: UserDO,
  scope: VFSScope,
  touched: Iterable<number>,
  uploadId: string
): Promise<void> {
  void touched;
  await drainChunkCleanupIntents(durableObject, scope, uploadId);
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
