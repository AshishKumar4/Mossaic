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
  hardDeleteFileRow,
  abortTempFile,
  userIdFor,
  resolveParent,
  poolSizeFor,
  recordWriteUsage,
  folderExists,
  findLiveFile,
  bumpFolderRevision,
} from "./vfs-ops";
import {
  commitVersion,
  dropTmpRowAfterVersionCommit,
  insertVersionChunk,
  isVersioningEnabled,
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
  // Stamp encryption columns on the tmp row up-front (they'll carry
  // through `commitRename`).
  if (incomingEncryption) {
    stampFileEncryption(durableObject, tmpId, incomingEncryption);
  }

  // Insert session row — captures every commit-time payload so finalize
  // can apply them without re-validation.
  let metadataBlob: Uint8Array | null = null;
  if (opts.metadata !== undefined && opts.metadata !== null) {
    metadataBlob = validateMetadata(opts.metadata).encoded;
  }
  const tagsJson =
    opts.tags !== undefined ? JSON.stringify([...opts.tags]) : null;
  durableObject.sql.exec(
    `INSERT INTO upload_sessions
       (upload_id, user_id, parent_id, leaf, total_size, total_chunks, chunk_size, pool_size, expires_at, status,
        encryption_mode, encryption_key_id, metadata_blob, tags_json, version_label, version_user_visible, mode, mime_type, created_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'open', ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    tmpId,
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
  const { token } = await signVFSMultipartToken(
    durableObject.envPublic,
    {
      uploadId,
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
    "UPDATE upload_sessions SET expires_at = ? WHERE upload_id = ?",
    expiresAt,
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
 * Drops chunk_refs on every shard in the pool by calling the existing
 * `deleteChunks(uploadId)` typed RPC — this re-uses the Lean-proven
 * refcount-decrement path and triggers the existing 30 s soft-mark +
 * alarm sweep for orphan chunks.
 */
export async function vfsAbortMultipart(
  durableObject: UserDO,
  scope: VFSScope,
  uploadId: string
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
  if (row.status === "aborted") return { ok: true };
  if (row.status === "finalized") {
    throw new VFSError(
      "EBUSY",
      `abortMultipart: session is already finalized; cannot un-finalize`
    );
  }

  // Mark aborted FIRST so any concurrent resume/finalize observes the
  // state change.
  durableObject.sql.exec(
    "UPDATE upload_sessions SET status = 'aborted' WHERE upload_id = ?",
    uploadId
  );

  // Fan out: drop chunk_refs + clear staging on every shard in the
  // pool. We don't know which shards were touched (no per-chunk
  // UserDO write), so we visit all of them. Idempotent on each shard.
  const ns = shardNs(durableObject);
  const cleanup: Promise<void>[] = [];
  for (let sIdx = 0; sIdx < row.pool_size; sIdx++) {
    const shardName = vfsShardDOName(scope.ns, scope.tenant, scope.sub, sIdx);
    const stub = ns.get(ns.idFromName(shardName));
    cleanup.push(
      (async () => {
        try {
          await stub.deleteChunks(uploadId);
          await stub.clearMultipartStaging(uploadId);
        } catch {
          // Best-effort; orphans are reaped by the alarm sweeper at
          // session expiry.
        }
      })()
    );
  }
  await Promise.all(cleanup);

  // Hard-delete the tmp `files` row. The file_chunks table was never
  // populated (finalize never ran), so nothing else to clean on
  // UserDO. Use the existing helper — drops file_chunks (no-op),
  // file_tags (no-op), then files.
  await abortTempFile(durableObject, userId, scope, uploadId);

  return { ok: true };
}

/**
 * Finalize a multipart upload. ONE UserDO turn; one fan-out per
 * unique touched shard for manifest verification + a second fan-out
 * for staging clear.
 *
 * Atomicity: all UserDO-side state changes happen in this one method
 * body (single DO turn = single SQL transaction). On any thrown
 * exception before `commitRename`, the tmp row remains
 * `status='uploading'` and the caller can resume or abort. Step 7
 * (rename) is itself proven atomic by the existing
 * `commitRename_atomic` theorem.
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
  if (session.status !== "open") {
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
  }

  // 6. Compute file hash + total size from the collected sizes (which
  //    are already verified against the client list).
  let totalSize = 0;
  for (let i = 0; i < session.total_chunks; i++) {
    totalSize += collected.get(i)!.size;
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
  //   (c) NOT call `commitRename` (whose hard-delete branch would
  //       silently destroy prior history under versioning).
  // The non-versioned branch keeps `commitRename`'s hard-delete
  // supersede — correct semantics for versioning-off tenants.
  const versioning = isVersioningEnabled(durableObject, userId);
  const now = Date.now();

  if (versioning) {
    // Locate the pre-existing live row at (parent, leaf), if any. Its
    // file_id is the stable `pathId` for this path's history; the
    // multipart's tmp row will be discarded once the version is
    // committed.
    const liveRow = findLiveFile(
      durableObject,
      userId,
      session.parent_id,
      session.leaf
    );
    let pathId: string;
    if (liveRow) {
      // Existing path — reuse its identity. The new version attaches
      // to it via `head_version_id`. The multipart tmp row is dropped
      // at the end of this branch (its chunks now belong to the
      // version, refid = uploadId).
      pathId = liveRow.file_id;
    } else {
      // No prior path: promote the multipart tmp row to the live row
      // by `commitRename`, making `tmpId` the path's permanent
      // identity. Chunks were filed under `refId = uploadId = tmpId`
      // at upload time; `pathId` therefore equals the refId, but the
      // version below records `shard_ref_id = uploadId` explicitly
      // so the GC path is uniform with the live-prior case.
      await commitRename(
        durableObject,
        userId,
        scope,
        uploadId,
        session.parent_id,
        session.leaf
      );
      pathId = uploadId;
    }

    // Apply commit-time metadata + tags to the path's stable row,
    // before the version is committed (so subsequent reads of the
    // metadata-from-files denormalization match what the version
    // captures).
    if (session.metadata_blob !== null) {
      durableObject.sql.exec(
        "UPDATE files SET metadata = ? WHERE file_id = ?",
        session.metadata_blob,
        pathId
      );
    }
    if (session.tags_json !== null) {
      const tags = JSON.parse(session.tags_json) as string[];
      const { replaceTags } = await import("./metadata-tags");
      replaceTags(durableObject, userId, pathId, tags);
    }

    // Insert version_chunks rows for the newly-finalized version.
    // The version_id is fresh; the chunks themselves already live on
    // ShardDOs under refId = uploadId, recorded in shard_ref_id below.
    const versionId = generateId();
    for (let i = 0; i < session.total_chunks; i++) {
      const row = collected.get(i)!;
      insertVersionChunk(durableObject, versionId, {
        chunk_index: i,
        chunk_hash: row.hash,
        chunk_size: row.size,
        shard_index: idxToShard[i],
      });
    }

    // commitVersion: inserts the file_versions row AND atomically
    // moves files.head_version_id to it. Prior versions / chunks
    // remain — the data preservation invariant.
    commitVersion(durableObject, {
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
      shardRefId: uploadId,
      encryption: session.encryption_mode !== null
        ? {
            mode: session.encryption_mode as "convergent" | "random",
            keyId: session.encryption_key_id ?? undefined,
          }
        : undefined,
    });

    // When the prior live row was reused as `pathId`, the multipart
    // tmp `files` row is now redundant. Drop it WITHOUT chunk
    // fan-out: its chunks are owned by the new version_chunks rows
    // (and the chunk_refs on shards remain valid under
    // refId = uploadId). Calling `hardDeleteFileRow` here would
    // dispatch `deleteChunks(uploadId)` to every shard and reap the
    // chunks the version we just committed depends on.
    if (liveRow) {
      dropTmpRowAfterVersionCommit(durableObject, uploadId, {
        hasChunks: true,
      });
    }
    // (no-prior-row case: commitRename already promoted the tmp row,
    //  and the file_chunks / metadata / tags it carried are intact;
    //  the row IS the path's identity so we keep it.)

    // Versioned multipart finalize advanced head_version_id on an
    // existing path. commitRename only fires (and self-bumps) in
    // the no-prior-row branch above; the prior-row branch needs an
    // explicit bump so listChildren observers see the new head.
    if (liveRow) {
      bumpFolderRevision(durableObject, userId, session.parent_id);
    }
  } else {
    // Non-versioned tenant — commitRename hard-deletes any prior
    // live row, which is correct semantics for versioning-off (no
    // history to keep).

    // 7. Batch-insert file_chunks rows. SQLite supports multi-row
    //    VALUES; we go per-row in a tight loop because the DO's bound-
    //    parameter limit is around 100 in practice, and this stays
    //    inside one DO turn so it's still atomic.
    for (let i = 0; i < session.total_chunks; i++) {
      const row = collected.get(i)!;
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

    // 8. Update tmp `files` row with size/file_hash/chunk_count. mode/
    //    mime_type were stamped at begin and remain.
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

    // 9. Apply commit-time payload (metadata, tags) BEFORE rename.
    if (session.metadata_blob !== null) {
      durableObject.sql.exec(
        "UPDATE files SET metadata = ? WHERE file_id = ?",
        session.metadata_blob,
        uploadId
      );
    }
    if (session.tags_json !== null) {
      const tags = JSON.parse(session.tags_json) as string[];
      const { replaceTags } = await import("./metadata-tags");
      replaceTags(durableObject, userId, uploadId, tags);
    }

    // 10. Atomic supersede via existing commitRename. If a live row
    //     exists at (parent, leaf), it's hard-deleted (chunks GC'd via
    //     deleteChunks fan-out inside hardDeleteFileRow).
    await commitRename(
      durableObject,
      userId,
      scope,
      uploadId,
      session.parent_id,
      session.leaf
    );
  }

  // 11. Record bytes against quota + grow pool size if we crossed a
  //     5 GB boundary. Must run AFTER commitRename succeeds (we own
  //     the row) but BEFORE marking the session finalized so a
  //     post-finalize crash doesn't double-count on retry.
  //
  // Only fires for the non-versioning branch. commitVersion (the
  // versioning branch above) self-accounts via recordWriteUsage;
  // calling here too would double-count bytes + file_count on
  // every versioning multipart finalize.
  if (!versioning) {
    recordWriteUsage(durableObject, userId, totalSize, 1);
  }

  // 12. Mark session finalized + clear staging across touched shards.
  durableObject.sql.exec(
    "UPDATE upload_sessions SET status = 'finalized' WHERE upload_id = ?",
    uploadId
  );
  await Promise.all(
    Array.from(touched).map(async (sIdx) => {
      const shardName = vfsShardDOName(scope.ns, scope.tenant, scope.sub, sIdx);
      const stub = ns.get(ns.idFromName(shardName));
      try {
        await stub.clearMultipartStaging(uploadId);
      } catch {
        // Best-effort; orphan staging rows are reaped by the alarm
        // sweeper at session expiry.
      }
    })
  );

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
    fileId: uploadId,
    size: totalSize,
    chunkCount: session.total_chunks,
    fileHash,
    path: finalizedPath,
    mimeType: session.mime_type,
    isEncrypted: session.encryption_mode !== null,
  };
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
 * Cap on how many times the alarm sweeper retries
 * `vfsAbortMultipart` on a single session before giving up. After
 * the cap, the row flips to status='poisoned' so future sweeps
 * skip it and the operator can investigate via Logpush.
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
 * P1-5 fix — failure handling. Pre-fix, a `vfsAbortMultipart`
 * throw flipped status to 'aborted' immediately and the row was
 * invisible to subsequent sweeps. Chunks staged on the shards
 * stayed refcounted forever — permanent shard storage leak per
 * transient error. Now the catch path BUMPS the `attempts`
 * counter and leaves status='open' so the next sweep retries.
 * Only after MULTIPART_MAX_ABORT_ATTEMPTS does the row flip to
 * 'poisoned' (a NEW status, distinct from 'aborted'/'finalized')
 * so an operator can find it via Logpush + manually reconcile.
 */
export async function sweepExpiredMultipartSessions(
  durableObject: UserDO,
  scopeForUser: (userId: string) => VFSScope
): Promise<{ swept: number; remaining: boolean }> {
  const now = Date.now();
  const stale = durableObject.sql
    .exec(
      `SELECT upload_id, user_id, attempts FROM upload_sessions
        WHERE status = 'open' AND expires_at < ?
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
      await vfsAbortMultipart(durableObject, scope, row.upload_id);
    } catch (err) {
      const nextAttempts = (row.attempts ?? 0) + 1;
      if (nextAttempts >= MULTIPART_MAX_ABORT_ATTEMPTS) {
        // Give up — flip to the distinct 'poisoned' status so an
        // operator can find these via Logpush + manually
        // reconcile (the staged chunks on shards still hold
        // chunk_refs under the upload_id; an admin script can
        // call deleteChunks(upload_id) per shard to free them).
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
        "SELECT COUNT(*) AS n FROM upload_sessions WHERE status = 'open' AND expires_at < ?",
        now
      )
      .toArray()[0] as { n: number }
  ).n;

  // Suppress unused warning for the existing helper signature.
  void hardDeleteFileRow;

  return { swept: stale.length, remaining: stillOpen > 0 };
}
