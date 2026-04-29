/**
 * Phase 17.6 — App-side multipart upload helpers.
 *
 * Mirrors `worker/core/objects/user/multipart-upload.ts` but adapted to
 * the legacy App schema (no `_vfs_tmp_*` rename pattern; legacy `files`
 * table without the canonical `parent_id` UNIQUE partial index;
 * placement via `legacyAppPlacement`).
 *
 * The App's UserDO subclass (`worker/app/objects/user/user-do.ts`)
 * exposes these as `appBeginMultipart` / `appAbortMultipart` /
 * `appFinalizeMultipart` / `appGetMultipartStatus` / `appOpenManifest`
 * typed RPCs. The App-pinned multipart route
 * (`worker/app/routes/multipart.ts`) calls them via the typed DO RPC
 * binding.
 *
 * **Score-template invariance** (Phase 17.5 §1.4): chunk placement uses
 * `legacyAppPlacement.placeChunk` which delegates to the shared
 * `placeChunk` rendezvous score keyed by `shard:${userId}:${idx}` —
 * identical to the legacy App route's per-chunk placement. New chunks
 * land on the same physical ShardDO instances as the legacy
 * single-chunk path.
 *
 * **Cross-class consistency.** `worker/core/objects/shard/shard-do.ts`
 * already provides `putChunkMultipart` + `getMultipartManifest` +
 * `getMultipartLanded` + `clearMultipartStaging` from Phase 16. The
 * legacy App ShardDOs (`shard:${userId}:${idx}`) instantiate the same
 * physical class so all four RPCs are present without modification.
 */

import type { UserDO } from "./user-do";
import type { ShardDO } from "@core/objects/shard/shard-do";
import { computeChunkSpec } from "@shared/chunking";
import { generateId } from "@core/lib/utils";
import { legacyAppPlacement } from "@shared/placement";
import {
  signVFSMultipartToken,
} from "@core/lib/auth";
import {
  MULTIPART_DEFAULT_TTL_MS,
  MULTIPART_MAX_OPEN_SESSIONS_PER_TENANT,
  type MultipartBeginResponse,
  type MultipartFinalizeResponse,
  type ShardMultipartManifestRow,
} from "@shared/multipart";
import { hashChunk } from "@shared/crypto";
import { recordChunk, completeFile, getFileManifest } from "./files";
import { updateUsage, checkQuota } from "./quota";

// VFSError isn't used in the App layer; surface failures as plain
// `Error` with a code-like prefix so the route handler maps them to
// HTTP statuses uniformly.
class AppMpError extends Error {
  constructor(
    public readonly code:
      | "ENOENT"
      | "EBUSY"
      | "EACCES"
      | "EINVAL"
      | "EBADF"
      | "EFBIG",
    message: string
  ) {
    super(`${code}: ${message}`);
  }
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

/** App-internal scope adapter — the legacy `userId` doubles as `tenant`. */
function appScope(userId: string): { ns: "default"; tenant: string } {
  return { ns: "default" as const, tenant: userId };
}

export interface AppBeginMultipartOpts {
  size: number;
  chunkSize?: number;
  mimeType?: string;
  parentId?: string | null;
  ttlMs?: number;
  resumeFrom?: string;
}

/**
 * Begin a multipart upload session against the legacy App schema.
 *
 * Differences from canonical:
 *  - Inserts directly into `files` with status='uploading' and the
 *    real fileName (no `_app_tmp_<uploadId>` tmp + rename — legacy
 *    schema has no UNIQUE index that requires the supersede pattern).
 *  - The `uploadId` IS the `file_id` (saves a column).
 *  - Pool size comes from `quota.pool_size` (legacy column) via
 *    `appCreateFile`-shaped logic.
 */
export async function appBeginMultipart(
  durableObject: UserDO,
  userId: string,
  path: string,
  opts: AppBeginMultipartOpts
): Promise<MultipartBeginResponse> {
  // ensureInit() handled by the RPC wrapper on UserDO

  if (!Number.isInteger(opts.size) || opts.size < 0) {
    throw new AppMpError(
      "EINVAL",
      `beginMultipart: size must be a non-negative integer (got ${opts.size})`
    );
  }

  // Per-tenant cap on open sessions.
  const open = (
    durableObject.sql
      .exec(
        "SELECT COUNT(*) AS n FROM upload_sessions WHERE user_id = ? AND status = 'open'",
        userId
      )
      .toArray()[0] as { n: number }
  ).n;
  if (open >= MULTIPART_MAX_OPEN_SESSIONS_PER_TENANT) {
    throw new AppMpError(
      "EBUSY",
      `beginMultipart: tenant has ${open} open sessions (cap ${MULTIPART_MAX_OPEN_SESSIONS_PER_TENANT})`
    );
  }

  // Resume branch.
  if (opts.resumeFrom !== undefined) {
    return await resumeAppMultipart(durableObject, userId, opts);
  }

  // Quota check upfront.
  if (!checkQuota(durableObject, userId, opts.size)) {
    throw new AppMpError("EFBIG", "beginMultipart: quota exceeded");
  }

  // Resolve (parentId, fileName) from path. Legacy App routes already
  // pass parentId explicitly; we accept it via opts. The `path` string
  // is informational (used for logging / future canonical migration);
  // for begin we use parentId+fileName.
  //
  // Path parsing: last `/`-separated segment is the fileName; we don't
  // walk folders here (the SPA already resolved the parentId before
  // calling).
  const fileName = path.split("/").filter(Boolean).pop() ?? path;
  if (fileName.length === 0) {
    throw new AppMpError(
      "EINVAL",
      `beginMultipart: cannot derive fileName from path '${path}'`
    );
  }
  const parentId = opts.parentId ?? null;

  const { chunkSize, chunkCount } = computeChunkSpec(opts.size);
  const finalChunkSize = opts.chunkSize ?? chunkSize;
  const finalChunkCount =
    finalChunkSize === chunkSize
      ? chunkCount
      : Math.ceil(opts.size / finalChunkSize);

  // Pool size from quota (legacy column).
  const poolRow = durableObject.sql
    .exec("SELECT pool_size FROM quota WHERE user_id = ?", userId)
    .toArray()[0] as { pool_size: number } | undefined;
  const poolSize = poolRow ? poolRow.pool_size : 32;

  const uploadId = generateId();
  const now = Date.now();
  const ttl = opts.ttlMs ?? MULTIPART_DEFAULT_TTL_MS;
  const expiresAt = now + ttl;
  const mimeType = opts.mimeType ?? "application/octet-stream";

  // Insert legacy `files` row in uploading state. The fileName is the
  // real one — legacy schema has no UNIQUE index; collisions are
  // accepted (App routes' DELETE filters by status).
  durableObject.sql.exec(
    `INSERT INTO files (file_id, user_id, parent_id, file_name, file_size, file_hash, mime_type, chunk_size, chunk_count, pool_size, status, created_at, updated_at)
     VALUES (?, ?, ?, ?, ?, '', ?, ?, ?, ?, 'uploading', ?, ?)`,
    uploadId,
    userId,
    parentId,
    fileName,
    opts.size,
    mimeType,
    finalChunkSize,
    finalChunkCount,
    poolSize,
    now,
    now
  );

  // Insert upload_sessions row mirroring canonical schema. The
  // `metadata_blob` / `tags_json` / `version_*` / `encryption_*`
  // columns stay NULL because legacy App data does not use them.
  durableObject.sql.exec(
    `INSERT INTO upload_sessions
       (upload_id, user_id, parent_id, leaf, total_size, total_chunks,
        chunk_size, pool_size, expires_at, status,
        mode, mime_type, created_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'open', ?, ?, ?)`,
    uploadId,
    userId,
    parentId,
    fileName,
    opts.size,
    finalChunkCount,
    finalChunkSize,
    poolSize,
    expiresAt,
    420, // 0o644 default mode for legacy files
    mimeType,
    now
  );

  // Sign the session token. Same `signVFSMultipartToken` as canonical;
  // the token's `(ns, tn)` claims are validated by the route handler
  // against the App JWT's userId.
  const { token } = await signVFSMultipartToken(
    durableObject.envPublic,
    {
      uploadId,
      ns: "default",
      tn: userId,
      poolSize,
      totalChunks: finalChunkCount,
      chunkSize: finalChunkSize,
      totalSize: opts.size,
    },
    ttl
  );

  return {
    uploadId,
    chunkSize: finalChunkSize,
    totalChunks: finalChunkCount,
    poolSize,
    sessionToken: token,
    putEndpoint: `/api/upload/multipart/${uploadId}`,
    expiresAtMs: expiresAt,
    landed: [],
  };
}

/** Resume an open multipart session — rehydrate landed[] from shards. */
async function resumeAppMultipart(
  durableObject: UserDO,
  userId: string,
  opts: AppBeginMultipartOpts
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
    throw new AppMpError(
      "ENOENT",
      `resumeMultipart: session not found: ${uploadId}`
    );
  }
  if (row.status !== "open") {
    throw new AppMpError(
      "EBUSY",
      `resumeMultipart: session status='${row.status}'; only 'open' is resumable`
    );
  }
  if (row.expires_at < Date.now()) {
    throw new AppMpError(
      "EBUSY",
      `resumeMultipart: session expired at ${row.expires_at}`
    );
  }
  if (opts.size !== row.total_size) {
    throw new AppMpError(
      "EINVAL",
      `resumeMultipart: size mismatch (session=${row.total_size}, caller=${opts.size})`
    );
  }

  // Probe every shard for landed indices.
  const ns = shardNs(durableObject);
  const scope = appScope(userId);
  const landedSet = new Set<number>();
  await Promise.all(
    Array.from({ length: row.pool_size }, (_, sIdx) => sIdx).map(
      async (sIdx) => {
        const shardName = legacyAppPlacement.shardDOName(scope, sIdx);
        const stub = ns.get(ns.idFromName(shardName));
        try {
          const res = await stub.getMultipartLanded(uploadId);
          for (const i of res.idx) landedSet.add(i);
        } catch {
          /* best-effort */
        }
      }
    )
  );

  const ttl = opts.ttlMs ?? MULTIPART_DEFAULT_TTL_MS;
  const expiresAt = Date.now() + ttl;
  const { token } = await signVFSMultipartToken(
    durableObject.envPublic,
    {
      uploadId,
      ns: "default",
      tn: userId,
      poolSize: row.pool_size,
      totalChunks: row.total_chunks,
      chunkSize: row.chunk_size,
      totalSize: row.total_size,
    },
    ttl
  );
  durableObject.sql.exec(
    "UPDATE upload_sessions SET expires_at = ? WHERE upload_id = ?",
    expiresAt,
    uploadId
  );
  return {
    uploadId,
    chunkSize: row.chunk_size,
    totalChunks: row.total_chunks,
    poolSize: row.pool_size,
    sessionToken: token,
    putEndpoint: `/api/upload/multipart/${uploadId}`,
    expiresAtMs: expiresAt,
    landed: Array.from(landedSet).sort((a, b) => a - b),
  };
}

/**
 * Abort a multipart session. Idempotent. Mirrors canonical
 * `vfsAbortMultipart`: marks aborted, fans out
 * `deleteChunks`+`clearMultipartStaging` across the pool, hard-deletes
 * the legacy `files` row.
 */
export async function appAbortMultipart(
  durableObject: UserDO,
  userId: string,
  uploadId: string
): Promise<{ ok: true }> {
  // ensureInit() handled by the RPC wrapper on UserDO
  const row = durableObject.sql
    .exec(
      "SELECT * FROM upload_sessions WHERE upload_id = ? AND user_id = ?",
      uploadId,
      userId
    )
    .toArray()[0] as unknown as UploadSessionRow | undefined;
  if (!row) {
    throw new AppMpError(
      "ENOENT",
      `abortMultipart: session not found: ${uploadId}`
    );
  }
  if (row.status === "aborted") return { ok: true };
  if (row.status === "finalized") {
    throw new AppMpError(
      "EBUSY",
      "abortMultipart: session already finalized; cannot un-finalize"
    );
  }
  durableObject.sql.exec(
    "UPDATE upload_sessions SET status = 'aborted' WHERE upload_id = ?",
    uploadId
  );
  const ns = shardNs(durableObject);
  const scope = appScope(userId);
  await Promise.all(
    Array.from({ length: row.pool_size }, (_, sIdx) => sIdx).map(
      async (sIdx) => {
        const shardName = legacyAppPlacement.shardDOName(scope, sIdx);
        const stub = ns.get(ns.idFromName(shardName));
        try {
          await stub.deleteChunks(uploadId);
          await stub.clearMultipartStaging(uploadId);
        } catch {
          /* best-effort */
        }
      }
    )
  );
  // Drop the tmp `files` row (no rename pattern to undo on App side).
  durableObject.sql.exec(
    "DELETE FROM files WHERE file_id = ? AND status = 'uploading'",
    uploadId
  );
  return { ok: true };
}

/**
 * Finalize a multipart upload. Verifies completeness across shards,
 * batch-inserts `file_chunks` rows (legacy schema), flips
 * status='complete', updates quota.
 */
export async function appFinalizeMultipart(
  durableObject: UserDO,
  userId: string,
  uploadId: string,
  chunkHashList: readonly string[]
): Promise<MultipartFinalizeResponse> {
  // ensureInit() handled by the RPC wrapper on UserDO
  const session = durableObject.sql
    .exec(
      "SELECT * FROM upload_sessions WHERE upload_id = ? AND user_id = ?",
      uploadId,
      userId
    )
    .toArray()[0] as unknown as UploadSessionRow | undefined;
  if (!session) {
    throw new AppMpError(
      "ENOENT",
      `finalizeMultipart: session not found: ${uploadId}`
    );
  }
  if (session.status !== "open") {
    throw new AppMpError(
      "EBUSY",
      `finalizeMultipart: session status='${session.status}'`
    );
  }
  if (session.expires_at < Date.now()) {
    throw new AppMpError(
      "EBUSY",
      `finalizeMultipart: session expired at ${session.expires_at}`
    );
  }
  if (chunkHashList.length !== session.total_chunks) {
    throw new AppMpError(
      "EINVAL",
      `finalizeMultipart: chunkHashList length ${chunkHashList.length} != totalChunks ${session.total_chunks}`
    );
  }
  for (let i = 0; i < chunkHashList.length; i++) {
    const h = chunkHashList[i];
    if (typeof h !== "string" || !/^[0-9a-f]{64}$/.test(h)) {
      throw new AppMpError(
        "EINVAL",
        `finalizeMultipart: chunkHashList[${i}] is not a 64-char lowercase hex string`
      );
    }
  }

  const scope = appScope(userId);
  const touched = new Set<number>();
  const idxToShard = new Array<number>(session.total_chunks);
  for (let i = 0; i < session.total_chunks; i++) {
    const sIdx = legacyAppPlacement.placeChunk(
      scope,
      uploadId,
      i,
      session.pool_size
    );
    idxToShard[i] = sIdx;
    touched.add(sIdx);
  }

  const ns = shardNs(durableObject);
  const collected = new Map<number, ShardMultipartManifestRow>();
  const collectErrors: unknown[] = [];
  await Promise.all(
    Array.from(touched).map(async (sIdx) => {
      const shardName = legacyAppPlacement.shardDOName(scope, sIdx);
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
    throw new AppMpError(
      "EBUSY",
      `finalizeMultipart: shard manifest collect failed on ${collectErrors.length} shard(s); first error: ${
        (collectErrors[0] as Error)?.message ?? String(collectErrors[0])
      }`
    );
  }

  for (let i = 0; i < session.total_chunks; i++) {
    const have = collected.get(i);
    if (!have) {
      throw new AppMpError(
        "ENOENT",
        `finalizeMultipart: chunk ${i} not landed (shard ${idxToShard[i]})`
      );
    }
    if (have.hash !== chunkHashList[i]) {
      throw new AppMpError(
        "EBADF",
        `finalizeMultipart: chunk ${i} hash divergence (server=${have.hash}, client=${chunkHashList[i]})`
      );
    }
  }

  // Sum sizes + compute file_hash (matches canonical formula).
  let totalSize = 0;
  for (let i = 0; i < session.total_chunks; i++) {
    totalSize += collected.get(i)!.size;
  }
  const fileHash = await hashChunk(
    new TextEncoder().encode(chunkHashList.join(""))
  );

  // Insert legacy `file_chunks` rows. The `recordChunk` helper handles
  // INSERT OR REPLACE — safe under retry.
  for (let i = 0; i < session.total_chunks; i++) {
    const row = collected.get(i)!;
    recordChunk(durableObject, uploadId, i, row.hash, row.size, idxToShard[i]);
  }

  // Flip status='complete' + stamp file_hash + bump quota. Legacy App
  // routes' `appCompleteFile` does this — we inline so we control the
  // ordering relative to the session-flip.
  completeFile(durableObject, uploadId, fileHash);
  updateUsage(durableObject, userId, totalSize, 1);

  // Mark session finalized + clear staging.
  durableObject.sql.exec(
    "UPDATE upload_sessions SET status = 'finalized' WHERE upload_id = ?",
    uploadId
  );
  await Promise.all(
    Array.from(touched).map(async (sIdx) => {
      const shardName = legacyAppPlacement.shardDOName(scope, sIdx);
      const stub = ns.get(ns.idFromName(shardName));
      try {
        await stub.clearMultipartStaging(uploadId);
      } catch {
        /* best-effort */
      }
    })
  );

  return {
    fileId: uploadId,
    size: totalSize,
    chunkCount: session.total_chunks,
    fileHash,
  };
}

/**
 * Probe the status of an open multipart session.
 */
export async function appGetMultipartStatus(
  durableObject: UserDO,
  userId: string,
  uploadId: string
): Promise<{
  landed: number[];
  total: number;
  bytesUploaded: number;
  expiresAtMs: number;
}> {
  // ensureInit() handled by the RPC wrapper on UserDO
  const row = durableObject.sql
    .exec(
      "SELECT * FROM upload_sessions WHERE upload_id = ? AND user_id = ?",
      uploadId,
      userId
    )
    .toArray()[0] as unknown as UploadSessionRow | undefined;
  if (!row) {
    throw new AppMpError(
      "ENOENT",
      `getMultipartStatus: session not found: ${uploadId}`
    );
  }

  const ns = shardNs(durableObject);
  const scope = appScope(userId);
  const landedSet = new Set<number>();
  let bytesUploaded = 0;
  await Promise.all(
    Array.from({ length: row.pool_size }, (_, sIdx) => sIdx).map(
      async (sIdx) => {
        const shardName = legacyAppPlacement.shardDOName(scope, sIdx);
        const stub = ns.get(ns.idFromName(shardName));
        try {
          const res = await stub.getMultipartManifest(uploadId);
          for (const r of res.rows) {
            landedSet.add(r.idx);
            bytesUploaded += r.size;
          }
        } catch {
          /* best-effort */
        }
      }
    )
  );
  return {
    landed: Array.from(landedSet).sort((a, b) => a - b),
    total: row.total_chunks,
    bytesUploaded,
    expiresAtMs: row.expires_at,
  };
}

/**
 * Open a manifest for a finalized App file. Wraps `getFileManifest`
 * (legacy SQL helper) into the Phase 16 `MultipartDownloadTokenResponse`
 * manifest shape so the SDK's `parallelDownload` can consume it
 * verbatim.
 */
export async function appOpenManifest(
  durableObject: UserDO,
  fileId: string
): Promise<{
  fileId: string;
  size: number;
  chunkSize: number;
  chunkCount: number;
  chunks: Array<{ index: number; hash: string; size: number }>;
  inlined: boolean;
  mimeType: string;
}> {
  // ensureInit() handled by the RPC wrapper on UserDO
  const m = getFileManifest(durableObject, fileId);
  if (!m) {
    throw new AppMpError("ENOENT", `openManifest: file not found: ${fileId}`);
  }
  return {
    fileId: m.fileId,
    size: m.fileSize,
    chunkSize: m.chunkSize,
    chunkCount: m.chunkCount,
    chunks: m.chunks.map((c) => ({
      index: c.index,
      hash: c.hash,
      size: c.size,
    })),
    inlined: m.inlineData !== null,
    mimeType: m.mimeType,
  };
}
