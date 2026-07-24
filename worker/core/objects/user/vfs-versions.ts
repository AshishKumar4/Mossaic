import type { UserDOCore as UserDO } from "./user-do-core";
import { enforceModeMonotonic } from "./encryption-stamp";
import type { ShardDO } from "../shard/shard-do";
import {
  VFSError,
  type DropVersionsStepResult,
  type VFSScope,
} from "../../../../shared/vfs-types";
import { generateId, vfsShardDOName } from "../../lib/utils";
import { placeChunk } from "../../../../shared/placement";
import {
  bumpFolderRevision,
  poolSizeFor,
  recordWriteUsage,
  userIdFor,
} from "./vfs/helpers";
import { insertAuditLog } from "./vfs/audit-log";
import { resolvePath } from "./path-walk";
import {
  lastSqlChanges,
  runWithConcurrencyBlocked,
  scheduleStaleUploadSweep,
  stageChunkCleanupIntent,
  transactionSync,
} from "./internal-storage";

/**
 * Audit H4 — placement function for the versioning path.
 *
 * The invariant — "two writes of the same content share the
 * same chunk row, refcount = (number of versions referencing it)" —
 * requires that an identical content hash always lands on the same
 * shard. The previous code computed `placeChunk(userId, hash, 0, poolSize)`
 * which is rendezvous-deterministic only as long as `poolSize` stays
 * constant. When pool_size grows (every 5 GB stored, per
 * `computePoolSize`), rendezvous hashing re-routes ~1/N of hashes to
 * a different shard, which breaks cross-version dedup silently:
 *
 *   write(P, content) at pool=32 → shard S1, refcount=1
 *   ... user accumulates >5GB, pool grows to 33 ...
 *   write(P, content) at pool=33 → shard S2 (rendezvous re-routed)
 *
 * S2 has no chunk H, so the cold-path INSERT runs and storage is
 * doubled. `chunks.ref_count` claims become false.
 *
 * Fix: BEFORE placing a hash via the rendezvous formula, look it up
 * in `version_chunks` for the current tenant. If a row exists, reuse
 * that shard_index — the chunk has a frozen home for as long as any
 * version references it. Only on first appearance of a hash do we
 * compute placement, and that placement becomes the canonical home
 * for all future versions until the chunk is fully reaped.
 *
 * Lookup cost: one indexed SQL probe per chunk hash. Negligible
 * (single-digit µs in SQLite per call) compared to the ShardDO RPC.
 *
 * Caveats:
 * - Two concurrent first-writes of the same hash may both miss the
 *   probe and pick different shards (different poolSizes if growth
 *   happened between them). DO single-thread serializes them, so the
 *   second write finds the first's row in version_chunks and reuses.
 *   The rare cross-DO race is bounded by the per-tenant DO model.
 * - If admin manually deletes a version_chunks row while the chunk
 *   row survives on a shard, a future write may compute a different
 *   placement and dedupe miss. Acceptable: admin operations are
 *   off-path.
 */
export function placeChunkForVersion(
  durableObject: UserDO,
  scope: VFSScope,
  hash: string,
  poolSize: number
): number {
  // First check: have we placed this hash before? Any existing
  // version_chunks row pins the shard. We don't need user_id in the
  // query because version_chunks doesn't carry it directly — but
  // version_chunks rows are per-DO so isolation is implicit (one
  // UserDO per tenant scope).
  const existing = durableObject.sql
    .exec(
      "SELECT shard_index FROM version_chunks WHERE chunk_hash = ? LIMIT 1",
      hash
    )
    .toArray()[0] as { shard_index: number } | undefined;
  if (existing !== undefined) {
    return existing.shard_index;
  }
  // First placement: rendezvous-hash as before. The result will be
  // recorded in version_chunks by the caller, freezing future
  // placements for this hash even if poolSize subsequently grows.
  // routes through the placement abstraction; the resulting
  // integer is identical to the legacy path because the rendezvous
  // score template is invariant across placements (§1.4).
  return placeChunk(userIdFor(scope), hash, 0, poolSize);
}

/**
 * file-level versioning (S3-style, opt-in).
 *
 * Per-tenant `quota.versioning_enabled` toggles whether writes
 * create historical version rows. When OFF, behavior is byte-equivalent
 * to (no version rows ever inserted, no head pointer used,
 * no readFile-by-version-id surface). When ON, the write path:
 *
 *   1. Resolves / creates a stable `files` row at (parent_id, name)
 *      — this is the `path_id` for the version's lifetime.
 *   2. Inserts a fresh `file_versions` row with a new ULID
 *      `version_id`. The row carries inline_data (if ≤16KB) OR
 *      points at version_chunks rows (chunked tier).
 *   3. Updates `files.head_version_id` to the new version_id.
 *
 * Reads with no version arg resolve via head_version_id; reads with
 * a `{ version: id }` arg resolve via direct lookup. unlink inserts
 * a tombstone version (deleted=1, no chunks).
 *
 * Refcount semantics: chunks are pushed to ShardDO with a synthetic
 * file_id of `${path_id}#${version_id}`. chunk_refs
 * (chunk_hash, file_id, chunk_index) PK becomes naturally
 * per-version — refcount per chunk hash equals "number of versions
 * still referencing it", and the alarm sweeper reclaims chunks
 * when the last reference drops. Identical content across versions
 * deduplicates by chunk_hash inside the ShardDO.
 *
 * IDs flow:
 *   path_id  = files.file_id (stable for a path's lifetime)
 *   version_id = generateId() per write
 *   shard ref = `${path_id}#${version_id}` (passed as fileId to ShardDO)
 */

export interface VersionRow {
  versionId: string;
  mtimeMs: number;
  size: number;
  mode: number;
  deleted: boolean;
  /** optional human-readable label. */
  label?: string | null;
  /**
   * true iff this version was created by an explicit
   * user-facing op (writeFile, restoreVersion, flush()). False for
   * Yjs opportunistic compactions and legacy rows that pre-date
   * the column.
   */
  userVisible?: boolean;
  /** snapshot of metadata at this version (when requested). */
  metadata?: Record<string, unknown> | null;
  /**
   * per-version encryption stamp. Undefined for plaintext
   * (default for legacy rows and explicit plaintext writes).
   */
  encryption?: { mode: "convergent" | "random"; keyId?: string };
}

/** True iff versioning is enabled for the tenant on this DO. */
export function isVersioningEnabled(
  durableObject: UserDO,
  userId: string
): boolean {
  // Lazy ensure quota row exists.
  durableObject.sql.exec(
    `INSERT OR IGNORE INTO quota (user_id, storage_used, storage_limit, file_count, pool_size)
     VALUES (?, 0, 107374182400, 0, 32)`,
    userId
  );
  const row = durableObject.sql
    .exec("SELECT versioning_enabled FROM quota WHERE user_id = ?", userId)
    .toArray()[0] as { versioning_enabled: number | null } | undefined;
  return !!row?.versioning_enabled;
}

/**
 * Cleanup helper for the "tmp row supersedes a live row" path. After
 * a versioned commit attaches a new version onto an EXISTING pathId
 * (`liveRow`/`liveDst`), the tmp row that originally hosted the
 * upload becomes redundant: its bytes already belong to the new
 * version under the appropriate `shard_ref_id`, and re-attaching its
 * `files`-row state would shadow the live path.
 *
 * Drop the tmp `files` row + its companion `file_chunks` /
 * `file_tags` rows WITHOUT calling `hardDeleteFileRow` — that helper
 * dispatches `deleteChunks(tmpId)` to every shard, which would reap
 * the chunks the just-committed version still references.
 *
 * Used by streams.ts (commitWriteStream), multipart-upload.ts
 * (vfsFinalizeMultipart), and copy-file.ts (copyVersioned). The
 * `hasChunks` flag distinguishes the chunked tier (delete file_chunks
 * too) from the inline tier (no chunks were ever inserted).
 */
export function dropTmpRowAfterVersionCommit(
  durableObject: UserDO,
  tmpId: string,
  opts: { hasChunks: boolean }
): void {
  if (opts.hasChunks) {
    durableObject.sql.exec(
      "DELETE FROM file_chunks WHERE file_id = ?",
      tmpId
    );
  }
  durableObject.sql.exec(
    "DELETE FROM file_tags WHERE path_id = ?",
    tmpId
  );
  durableObject.sql.exec("DELETE FROM files WHERE file_id = ?", tmpId);
}

/** Single chunk row written into a version's manifest. */
export interface VersionChunkRow {
  chunk_index: number;
  chunk_hash: string;
  chunk_size: number;
  shard_index: number;
}

/**
 * Insert a single `version_chunks` row. Mirrors the 5-column shape
 * used by every versioned-write callsite (streams commit, multipart
 * finalize, copy-file chunked, mutations renameOverwriteVersioned).
 *
 * Each callsite previously inlined the same 5-line INSERT against a
 * different chunk source (file_chunks for streams, the multipart
 * `collected` Map, version_chunks for copy/rename). Chunk sources
 * differ; the INSERT does not.
 *
 * Note: callers that interleave INSERTs with shard putChunk fan-out
 * (copy-file, mutations) call this PER CHUNK inside the inner loop;
 * callers that collect first then INSERT (streams, multipart) call
 * it in a tight loop after the source is materialised. Both patterns
 * are well-served by the row-at-a-time signature.
 */
export function insertVersionChunk(
  durableObject: UserDO,
  versionId: string,
  row: VersionChunkRow
): void {
  durableObject.sql.exec(
    `INSERT INTO version_chunks (version_id, chunk_index, chunk_hash, chunk_size, shard_index)
     VALUES (?, ?, ?, ?, ?)`,
    versionId,
    row.chunk_index,
    row.chunk_hash,
    row.chunk_size,
    row.shard_index
  );
}

/**
 * Operator helper: toggle versioning for a tenant. Idempotent;
 * affects only future writes (existing files / versions unchanged).
 */
export function setVersioningEnabled(
  durableObject: UserDO,
  userId: string,
  enabled: boolean
): void {
  durableObject.sql.exec(
    `INSERT OR IGNORE INTO quota (user_id, storage_used, storage_limit, file_count, pool_size)
     VALUES (?, 0, 107374182400, 0, 32)`,
    userId
  );
  durableObject.sql.exec(
    "UPDATE quota SET versioning_enabled = ? WHERE user_id = ?",
    enabled ? 1 : 0,
    userId
  );
}

/**
 * Compose the synthetic file_id sent to ShardDO so chunk_refs are
 * per-version. The "#" separator is invalid in our path-walk regex
 * for tenant components — and ShardDO doesn't validate it — so it's
 * safe as an internal shard-ref key without colliding with any
 * legitimate file_id.
 */
export function shardRefId(pathId: string, versionId: string): string {
  return `${pathId}#${versionId}`;
}

/**
 * Insert a new file_versions row. Caller is responsible for having
 * already pushed chunk_refs to ShardDOs and recorded version_chunks
 * rows BEFORE calling this. The atomic head-pointer flip happens here
 * — it's the commit point that makes the new version visible to
 * subsequent readers.
 *
 * For the inline tier, pass `inlineData` (Uint8Array); chunks are
 * empty. For chunked, pass `inlineData=null` and ensure
 * version_chunks rows have already been inserted.
 *
 * @lean-invariant Mossaic.Generated.UserDO.insertVersion_advances
 *   The list-based abstract model proves its post-insert maxMtime is at
 *   least the supplied mtime. SQL, clocks, and this function are not
 *   mechanically refined by that theorem.
 */
export interface CommitVersionArgs {
  pathId: string;
  versionId: string;
  userId: string;
  size: number;
  mode: number;
  mtimeMs: number;
  chunkSize: number;
  chunkCount: number;
  fileHash: string;
  mimeType: string;
  inlineData: Uint8Array | null;
  deleted?: boolean;
  userVisible?: boolean;
  label?: string | null;
  metadata?: Uint8Array | null;
  encryption?: { mode: "convergent" | "random"; keyId?: string };
  shardRefId?: string;
}

export interface VersionedFileExpectation {
  fileId: string;
  userId: string;
  parentId: string | null;
  fileName: string;
  headVersionId: string | null;
}

export function assertVersionedFileExpectation(
  durableObject: UserDO,
  expected: VersionedFileExpectation,
  operation: string
): void {
  const rows = durableObject.sql
    .exec(
      `SELECT 1 FROM files
        WHERE file_id = ? AND user_id = ? AND status = 'complete'
          AND IFNULL(parent_id, '') = IFNULL(?, '') AND file_name = ?
          AND head_version_id IS ?`,
      expected.fileId,
      expected.userId,
      expected.parentId,
      expected.fileName,
      expected.headVersionId
    )
    .toArray();
  if (rows.length !== 1) {
    throw new VFSError("EBUSY", `${operation}: file state changed during publication`);
  }
}

export function commitVersion(
  durableObject: UserDO,
  args: CommitVersionArgs
): void {
  commitVersionInternal(durableObject, args);
}

/** Checked publication for operations that crossed an await boundary. */
export function commitVersionChecked(
  durableObject: UserDO,
  args: CommitVersionArgs,
  expected: VersionedFileExpectation,
  operation: string
): void {
  if (args.pathId !== expected.fileId || args.userId !== expected.userId) {
    throw new VFSError("EINVAL", `${operation}: invalid version publication expectation`);
  }
  enforceModeMonotonic(
    durableObject,
    expected.userId,
    expected.parentId,
    expected.fileName,
    args.encryption
  );
  commitVersionInternal(durableObject, args, expected, operation);
}

function commitVersionInternal(
  durableObject: UserDO,
  args: CommitVersionArgs,
  expected?: VersionedFileExpectation,
  operation = "commitVersion"
): void {
  if (expected) {
    assertVersionedFileExpectation(durableObject, expected, operation);
  }
  // Single chokepoint for versioned accounting.
  //
  // Read the prior head's state BEFORE the INSERT so we can
  // compute (deltaBytes, deltaFiles, deltaInline) for one
  // recordWriteUsage call at the end. This consolidates the
  // accounting that previously had to live (or be missing) at
  // every commitVersion caller — 13 call sites across 6 files.
  //
  // Semantics (versioning-on file-count + storage):
  //   - file_count counts LIVE PATHS (paths with a non-tombstone
  //     head). Adding a new live version keeps the path live;
  //     adding a tombstone makes the path no longer-live.
  //   - storage_used counts BYTES of live-version content. Each
  //     non-tombstone version commit adds args.size; the older
  //     versions remain (and their bytes are already counted).
  //     dropVersionRows reaps the bytes on retention sweeps.
  //   - inline_bytes_used counts CUMULATIVE inline-tier bytes;
  //     non-tombstone inline commits add args.inlineData.byteLength.
  //
  // The yjs compaction path (yjs.ts:739) calls with
  // userVisible=true on user-flush events, so it accounts as a
  // normal inline write. Opportunistic compactions
  // (userVisible=false) skip commitVersion entirely.
  const headRowForAccounting = durableObject.sql
    .exec(
      `SELECT v.size AS prev_size, v.deleted AS prev_deleted
         FROM files f
         LEFT JOIN file_versions v
           ON v.path_id = f.file_id AND v.version_id = f.head_version_id
        WHERE f.file_id = ?`,
      args.pathId
    )
    .toArray()[0] as
    | { prev_size: number | null; prev_deleted: number | null }
    | undefined;
  const prevWasLive =
    !!headRowForAccounting &&
    headRowForAccounting.prev_size !== null &&
    headRowForAccounting.prev_deleted === 0;
  const nowIsLive = !args.deleted;

  // Preserve encryption stamp on tombstones.
  //
  // Without this, `vfsUnlink` (and `vfsRename` overwrite +
  // `vfsRemoveRecursive`) calling `commitVersion` with
  // `deleted: true` and no `args.encryption` would stamp both
  // the new tombstone row AND `files.encryption_mode/key_id`
  // NULL. If the user then `restoreVersion`'d a prior live
  // (encrypted) version, the restored head would carry the
  // tombstone's NULL stamp on `files` — readers would treat the
  // bytes as plaintext and serve garbage to the SDK, which would
  // skip its decrypt step and surface unintelligible bytes.
  //
  // Post-fix: when `args.deleted` is true AND the caller didn't
  // pass `args.encryption`, inherit the prior live version's
  // encryption stamp. The tombstone row records the stamp it
  // shadows so a later `restoreVersion` of THIS tombstone (which
  // is rejected anyway via vfs-versions.ts:805 "EINVAL — cannot
  // restore tombstone"), and a future `walkBack` recovery via
  // `adminReapTombstonedHeads`, both have a faithful trail.
  let encMode: "convergent" | "random" | null = args.encryption?.mode ?? null;
  let encKeyId: string | null = args.encryption?.keyId ?? null;
  if (args.deleted && args.encryption === undefined) {
    const prior = durableObject.sql
      .exec(
        `SELECT encryption_mode, encryption_key_id
           FROM file_versions
          WHERE path_id = ? AND deleted = 0
          ORDER BY mtime_ms DESC
          LIMIT 1`,
        args.pathId
      )
      .toArray()[0] as
      | {
          encryption_mode: string | null;
          encryption_key_id: string | null;
        }
      | undefined;
    if (prior) {
      // Cast back to the typed union — the SQL column is TEXT but
      // `commitVersion` only ever writes "convergent" | "random" | null
      // (validated upstream at the route + SDK layers).
      encMode = prior.encryption_mode as "convergent" | "random" | null;
      encKeyId = prior.encryption_key_id;
    }
  }
  durableObject.sql.exec(
    `INSERT INTO file_versions
       (path_id, version_id, user_id, size, mode, mtime_ms, deleted,
        inline_data, chunk_size, chunk_count, file_hash, mime_type,
        user_visible, label, metadata, encryption_mode, encryption_key_id,
        shard_ref_id)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    args.pathId,
    args.versionId,
    args.userId,
    args.size,
    args.mode,
    args.mtimeMs,
    args.deleted ? 1 : 0,
    args.inlineData,
    args.chunkSize,
    args.chunkCount,
    args.fileHash,
    args.mimeType,
    args.userVisible ? 1 : 0,
    args.label ?? null,
    args.metadata ?? null,
    encMode,
    encKeyId,
    args.shardRefId ?? null
  );
  // Update head pointer to the new version. Tombstones also become
  // the head — readers find them by mtime_ms and then check deleted.
  // also stamp `files.encryption_mode` + `files.encryption_key_id`
  // so non-versioned reads (`stat`, `readFile`) reflect the latest mode.
  if (expected) {
    durableObject.sql.exec(
      `UPDATE files
          SET head_version_id = ?,
              updated_at = ?,
              encryption_mode = ?,
              encryption_key_id = ?,
              version_generation = version_generation + 1
        WHERE file_id = ? AND user_id = ? AND status = 'complete'
          AND IFNULL(parent_id, '') = IFNULL(?, '') AND file_name = ?
          AND head_version_id IS ?`,
      args.versionId,
      args.mtimeMs,
      encMode,
      encKeyId,
      args.pathId,
      args.userId,
      expected.parentId,
      expected.fileName,
      expected.headVersionId
    );
  } else {
    durableObject.sql.exec(
      `UPDATE files
          SET head_version_id = ?,
              updated_at = ?,
              encryption_mode = ?,
              encryption_key_id = ?,
              version_generation = version_generation + 1
        WHERE file_id = ?`,
      args.versionId,
      args.mtimeMs,
      encMode,
      encKeyId,
      args.pathId
    );
  }
  if (expected && lastSqlChanges(durableObject) !== 1) {
    throw new VFSError("EBUSY", `${operation}: head changed during publication`);
  }

  // Single recordWriteUsage call covering all three counters.
  // Negative deltas are clamped at zero by the helper's
  // MAX(0, ...) clamp; pool growth is monotonic via
  // newPool > row.pool_size guard. See helpers.ts:421-453.
  //
  // deltaFiles transitions:
  //   prev=live  + now=live      → 0 (path stayed live)
  //   prev=live  + now=tombstone → -1 (path exited live)
  //   prev=dead  + now=live      → +1 (path entered live)
  //   prev=dead  + now=tombstone → 0 (still no live path)
  // ("dead" = no head OR tombstoned head OR no row.)
  //
  // deltaBytes / deltaInline are zero on tombstone inserts \u2014 the
  // older versions still occupy bytes; dropVersionRows is the
  // path that frees them.
  const deltaFiles =
    (nowIsLive ? 1 : 0) - (prevWasLive ? 1 : 0);
  const deltaBytes = nowIsLive ? args.size : 0;
  const deltaInline =
    nowIsLive && args.inlineData ? args.inlineData.byteLength : 0;
  if (deltaBytes !== 0 || deltaFiles !== 0 || deltaInline !== 0) {
    recordWriteUsage(
      durableObject,
      args.userId,
      deltaBytes,
      deltaFiles,
      deltaInline
    );
  }
}

/**
 * Resolve a file_versions row to read. With no `versionId`, returns
 * the newest non-deleted version (the head). With `versionId`, looks
 * up that exact row and returns it even if deleted — caller decides
 * what to do (S3 behavior: GET ?versionId=X on a tombstone returns
 * the tombstone metadata, not the bytes).
 *
 * Returns null if no matching version exists.
 */
export interface VersionContent {
  versionId: string;
  size: number;
  mode: number;
  mtimeMs: number;
  deleted: boolean;
  inlineData: ArrayBuffer | null;
  chunkSize: number;
  chunkCount: number;
  fileHash: string;
  mimeType: string;
  /**
   * per-version encryption stamp. NULL for plaintext (default
   * for legacy rows and for plaintext writes). When set, the SDK
   * decrypts the bytes (envelope-stream stored in `inline_data` or
   * across `version_chunks`) before returning them to the consumer.
   */
  encryption?: { mode: "convergent" | "random"; keyId?: string };
}

export function getVersion(
  durableObject: UserDO,
  pathId: string,
  versionId?: string
): VersionContent | null {
  const row = versionId
    ? durableObject.sql
        .exec(
          `SELECT version_id, size, mode, mtime_ms, deleted, inline_data,
                  chunk_size, chunk_count, file_hash, mime_type,
                  encryption_mode, encryption_key_id
             FROM file_versions
            WHERE path_id = ? AND version_id = ?`,
          pathId,
          versionId
        )
        .toArray()[0]
    : durableObject.sql
        .exec(
          `SELECT version_id, size, mode, mtime_ms, deleted, inline_data,
                  chunk_size, chunk_count, file_hash, mime_type,
                  encryption_mode, encryption_key_id
             FROM file_versions
            WHERE path_id = ? AND deleted = 0
            ORDER BY mtime_ms DESC
            LIMIT 1`,
          pathId
        )
        .toArray()[0];
  if (!row) return null;
  const r = row as Record<string, unknown>;
  const encMode = r.encryption_mode as string | null;
  const encKeyId = r.encryption_key_id as string | null;
  let encryption: { mode: "convergent" | "random"; keyId?: string } | undefined;
  if (encMode === "convergent" || encMode === "random") {
    encryption = { mode: encMode };
    if (encKeyId !== null) encryption.keyId = encKeyId;
  }
  return {
    versionId: r.version_id as string,
    size: r.size as number,
    mode: r.mode as number,
    mtimeMs: r.mtime_ms as number,
    deleted: (r.deleted as number) === 1,
    inlineData: (r.inline_data as ArrayBuffer | null) ?? null,
    chunkSize: r.chunk_size as number,
    chunkCount: r.chunk_count as number,
    fileHash: r.file_hash as string,
    mimeType: r.mime_type as string,
    ...(encryption !== undefined ? { encryption } : {}),
  };
}

/**
 * List versions newest-first. Backed by the
 * idx_file_versions_path_mtime index — a single B-tree range scan,
 * O(log N + limit).
 */
export function listVersions(
  durableObject: UserDO,
  pathId: string,
  opts: {
    limit?: number;
    /** filter to versions with `user_visible = 1`. */
    userVisibleOnly?: boolean;
    /** include the metadata snapshot per row. */
    includeMetadata?: boolean;
  } = {}
): VersionRow[] {
  const limit = opts.limit ?? 1000;
  const where: string[] = ["path_id = ?"];
  const args: (string | number)[] = [pathId];
  if (opts.userVisibleOnly) {
    where.push("user_visible = 1");
  }
  args.push(limit);
  const sql = `
    SELECT version_id, mtime_ms, size, mode, deleted,
           user_visible, label, metadata,
           encryption_mode, encryption_key_id
      FROM file_versions
     WHERE ${where.join(" AND ")}
     ORDER BY mtime_ms DESC
     LIMIT ?
  `;
  const rows = durableObject.sql
    .exec(sql, ...(args as [string, ...unknown[]]))
    .toArray() as {
    version_id: string;
    mtime_ms: number;
    size: number;
    mode: number;
    deleted: number;
    user_visible: number;
    label: string | null;
    metadata: ArrayBuffer | null;
    encryption_mode: string | null;
    encryption_key_id: string | null;
  }[];
  return rows.map((r) => {
    const out: VersionRow = {
      versionId: r.version_id,
      mtimeMs: r.mtime_ms,
      size: r.size,
      mode: r.mode,
      deleted: r.deleted === 1,
      userVisible: r.user_visible === 1,
      label: r.label,
    };
    // surface per-version encryption stamp.
    if (r.encryption_mode === "convergent" || r.encryption_mode === "random") {
      const enc: { mode: "convergent" | "random"; keyId?: string } = {
        mode: r.encryption_mode,
      };
      if (r.encryption_key_id !== null) enc.keyId = r.encryption_key_id;
      out.encryption = enc;
    }
    if (opts.includeMetadata) {
      if (r.metadata) {
        try {
          out.metadata = JSON.parse(
            new TextDecoder().decode(new Uint8Array(r.metadata))
          ) as Record<string, unknown>;
        } catch {
          out.metadata = null;
        }
      } else {
        out.metadata = null;
      }
    }
    return out;
  });
}

/**
 * set per-version flags. Idempotent. Throws EINVAL on
 *   - userVisible:false (the flag is monotonic; demoting is not
 *     supported because consumers may have built durable bookmarks
 *     against the version_id and silently flipping it would break
 *     them).
 *   - missing version row.
 *   - label > 128 chars (caller validates).
 */
export function markVersion(
  durableObject: UserDO,
  pathId: string,
  versionId: string,
  opts: { label?: string; userVisible?: boolean }
): void {
  // Existence check + current state.
  const row = durableObject.sql
    .exec(
      "SELECT user_visible FROM file_versions WHERE path_id = ? AND version_id = ?",
      pathId,
      versionId
    )
    .toArray()[0] as { user_visible: number } | undefined;
  if (!row) {
    throw new VFSError(
      "ENOENT",
      `markVersion: version ${versionId} not found at pathId ${pathId}`
    );
  }
  if (opts.userVisible === false) {
    throw new VFSError(
      "EINVAL",
      "markVersion: userVisible cannot be set to false (the bit is monotonic)"
    );
  }
  if (opts.label !== undefined) {
    durableObject.sql.exec(
      "UPDATE file_versions SET label = ? WHERE path_id = ? AND version_id = ?",
      opts.label,
      pathId,
      versionId
    );
  }
  if (opts.userVisible === true) {
    durableObject.sql.exec(
      `UPDATE file_versions SET user_visible = 1
        WHERE path_id = ? AND version_id = ? AND user_visible = 0`,
      pathId,
      versionId
    );
  }
}

/**
 * Resolve `path` → its current `path_id`. Used by listVersions /
 * readFile-by-version / restoreVersion / dropVersions. Returns null
 * if the path doesn't exist (no `files` row) — caller maps to ENOENT.
 *
 * NOTE: a path with only tombstone versions still has a `files` row
 * (with head_version_id pointing at the tombstone). Path resolution
 * uses path-walk which checks status != 'deleted' on the `files`
 * row, so tombstoned paths still resolve. listVersions surfaces the
 * tombstones; readFile-no-version returns ENOENT (because the head
 * is deleted and no live version exists earlier in history? — see
 * note: S3's behavior is ENOENT on a delete-marker head, which we
 * mirror).
 */
export function resolvePathId(
  durableObject: UserDO,
  userId: string,
  path: string
): string | null {
  let r;
  try {
    r = resolvePath(durableObject, userId, path);
  } catch {
    return null;
  }
  if (r.kind !== "file" && r.kind !== "symlink") return null;
  return r.leafId;
}

/** Atomically remove version metadata and queue its shard refs for cleanup. */
export async function dropVersionRows(
  durableObject: UserDO,
  scope: VFSScope,
  userId: string,
  pathId: string,
  versionIds: string[]
): Promise<number> {
  await scheduleStaleUploadSweep(durableObject);
  if (versionIds.length === 0) return 0;
  const { drainChunkCleanupIntents } = await import("./vfs-ops");
  const result = transactionSync(durableObject, () =>
    dropVersionRowsLocal(durableObject, userId, pathId, versionIds)
  );
  for (const refId of result.refIds) {
    await drainChunkCleanupIntents(durableObject, scope, refId);
  }
  return result.reaped;
}

export interface DropVersionRowsLocalResult {
  reaped: number;
  refIds: string[];
}

/** SQL-only version cleanup. The caller must arm maintenance first. */
export function dropVersionRowsLocal(
  durableObject: UserDO,
  userId: string,
  pathId: string,
  versionIds: readonly string[]
): DropVersionRowsLocalResult {
  const fileRow = durableObject.sql
    .exec(
      "SELECT head_version_id, status FROM files WHERE file_id = ? AND user_id = ?",
      pathId,
      userId
    )
    .toArray()[0] as
    | { head_version_id: string | null; status: string }
    | undefined;
  const headBefore = fileRow?.head_version_id
    ? (durableObject.sql
        .exec(
          "SELECT deleted FROM file_versions WHERE path_id = ? AND version_id = ?",
          pathId,
          fileRow.head_version_id
        )
        .toArray()[0] as { deleted: number } | undefined)
    : undefined;
  const wasLive = fileRow?.status === "complete" && headBefore?.deleted === 0;
  const refIds = new Set<string>();
  let bytesReaped = 0;
  let inlineBytesReaped = 0;
  let reaped = 0;

  for (const versionId of new Set(versionIds)) {
    const version = durableObject.sql
      .exec(
        `SELECT shard_ref_id, size, inline_data, deleted
           FROM file_versions
          WHERE path_id = ? AND version_id = ? AND user_id = ?`,
        pathId,
        versionId,
        userId
      )
      .toArray()[0] as
      | {
          shard_ref_id: string | null;
          size: number;
          inline_data: ArrayBuffer | null;
          deleted: number;
        }
      | undefined;
    if (!version) continue;

    const shardRows = durableObject.sql
      .exec(
        "SELECT DISTINCT shard_index FROM version_chunks WHERE version_id = ?",
        versionId
      )
      .toArray() as { shard_index: number }[];
    const refId = version.shard_ref_id ?? shardRefId(pathId, versionId);
    const now = Date.now();
    for (const { shard_index } of shardRows) {
      stageChunkCleanupIntent(durableObject, refId, shard_index, now);
      refIds.add(refId);
    }

    if (version.deleted === 0) {
      bytesReaped += version.size;
      inlineBytesReaped += version.inline_data?.byteLength ?? 0;
    }
    durableObject.sql.exec(
      "DELETE FROM version_chunks WHERE version_id = ?",
      versionId
    );
    durableObject.sql.exec(
      `DELETE FROM file_versions
        WHERE path_id = ? AND version_id = ? AND user_id = ?`,
      pathId,
      versionId,
      userId
    );
    if (lastSqlChanges(durableObject) !== 1) {
      throw new VFSError("EBUSY", `dropVersionRows: version ${versionId} changed`);
    }
    reaped++;
  }

  if (reaped === 0) return { reaped, refIds: [] };

  type RemainingHead = {
    version_id: string;
    mtime_ms: number;
    deleted: number;
    encryption_mode: string | null;
    encryption_key_id: string | null;
  };
  let headAfter: RemainingHead | undefined;
  if (fileRow?.head_version_id) {
    headAfter = durableObject.sql
      .exec(
        `SELECT version_id, mtime_ms, deleted, encryption_mode, encryption_key_id
           FROM file_versions WHERE path_id = ? AND version_id = ?`,
        pathId,
        fileRow.head_version_id
      )
      .toArray()[0] as RemainingHead | undefined;
  }
  headAfter ??= durableObject.sql
    .exec(
      `SELECT version_id, mtime_ms, deleted, encryption_mode, encryption_key_id
         FROM file_versions WHERE path_id = ?
        ORDER BY mtime_ms DESC, version_id DESC LIMIT 1`,
      pathId
    )
    .toArray()[0] as RemainingHead | undefined;

  if (fileRow && !headAfter) {
    const directShardRows = durableObject.sql
      .exec(
        "SELECT DISTINCT shard_index FROM file_chunks WHERE file_id = ?",
        pathId
      )
      .toArray() as { shard_index: number }[];
    const now = Date.now();
    for (const { shard_index } of directShardRows) {
      stageChunkCleanupIntent(durableObject, pathId, shard_index, now);
      refIds.add(pathId);
    }
    durableObject.sql.exec("DELETE FROM file_chunks WHERE file_id = ?", pathId);
  }

  const isLive = fileRow !== undefined && headAfter?.deleted === 0;
  const fileDelta = Number(isLive) - Number(wasLive);
  if (bytesReaped !== 0 || inlineBytesReaped !== 0 || fileDelta !== 0) {
    recordWriteUsage(
      durableObject,
      userId,
      -bytesReaped,
      fileDelta,
      -inlineBytesReaped
    );
  }

  if (fileRow && headAfter) {
    durableObject.sql.exec(
      `UPDATE files
          SET head_version_id = ?, updated_at = ?,
              encryption_mode = ?, encryption_key_id = ?
        WHERE file_id = ? AND user_id = ?`,
      headAfter.version_id,
      headAfter.mtime_ms,
      headAfter.encryption_mode,
      headAfter.encryption_key_id,
      pathId,
      userId
    );
    if (lastSqlChanges(durableObject) !== 1) {
      throw new VFSError("EBUSY", "dropVersionRows: failed to repair file head");
    }
  } else if (fileRow) {
    durableObject.sql.exec("DELETE FROM file_tags WHERE path_id = ?", pathId);
    durableObject.sql.exec(
      "DELETE FROM files WHERE file_id = ? AND user_id = ?",
      pathId,
      userId
    );
    if (lastSqlChanges(durableObject) !== 1) {
      throw new VFSError("EBUSY", "dropVersionRows: failed to drop empty path");
    }
  }

  return { reaped, refIds: [...refIds] };
}

/**
 * dropVersions retention policy:
 *   - olderThan: drop versions whose mtime_ms < cutoff (ms epoch)
 *   - keepLast: keep the N newest versions; drop the rest
 *   - exceptVersions: explicit allowlist that survives any other
 *     filter
 *   - all three may combine; each keep rule is additive.
 *
 * The CURRENT head version is never dropped — even if filters say
 * to. (S3 has the same invariant: you can't delete the current
 * version through a retention policy.)
 */
export const DROP_VERSIONS_BATCH_LIMIT = 128;
export const DROP_VERSIONS_MANIFEST_LIMIT = 200;
export const DROP_VERSIONS_CLEANUP_INTENT_LIMIT = 128;
export const DROP_VERSIONS_KEEP_LAST_MAX = 100_000;
export const DROP_VERSIONS_EXCEPT_MAX = 1_000;
const RETENTION_OPERATION_ID_MAX = 128;
const RETENTION_VERSION_ID_MAX = 128;
const RUNNING_RETENTION_TTL_MS = 24 * 60 * 60 * 1000;
const COMPLETED_RETENTION_TTL_MS = 7 * 24 * 60 * 60 * 1000;
const RETENTION_OPERATION_PRUNE_LIMIT = 32;
const RETENTION_RUNNING_MAX = 64;
const RETENTION_TOTAL_MAX = 128;
const RETENTION_ID_PATTERN = /^[A-Za-z0-9._:-]+$/;

export interface DropVersionsPolicy {
  olderThan?: number;
  keepLast?: number;
  exceptVersions?: string[];
}

interface RetentionOperationRow extends Record<string, SqlStorageValue> {
  operation_id: string;
  user_id: string;
  requested_path: string;
  path_id: string;
  policy_json: string;
  plan_generation: number;
  plan_head_version_id: string | null;
  cursor_mtime_ms: number | null;
  cursor_version_id: string | null;
  remaining_keep: number;
  dropped: number;
  kept: number;
  status: string;
  pending_version_id: string | null;
  pending_mtime_ms: number | null;
  pending_ref_id: string | null;
  pending_metadata_deleted: number;
  manifest_cursor: number | null;
}

interface RetentionVersionRow extends Record<string, SqlStorageValue> {
  version_id: string;
  mtime_ms: number;
}

interface RetentionKeepDecision {
  keep: boolean;
  consumesKeepSlot: boolean;
}

interface RetentionManifestRow extends Record<string, SqlStorageValue> {
  chunk_index: number;
  shard_index: number;
}

function validateRetentionId(value: unknown, label: string): string {
  if (
    typeof value !== "string" ||
    value.length === 0 ||
    value.length > RETENTION_VERSION_ID_MAX ||
    !RETENTION_ID_PATTERN.test(value)
  ) {
    throw new VFSError("EINVAL", `dropVersions: invalid ${label}`);
  }
  return value;
}

export function validateDropVersionsPolicy(policy: unknown): DropVersionsPolicy {
  if (policy === null || typeof policy !== "object" || Array.isArray(policy)) {
    throw new VFSError("EINVAL", "dropVersions: policy must be an object");
  }
  const input = policy as Record<string, unknown>;
  const normalized: DropVersionsPolicy = {};
  if (input.olderThan !== undefined) {
    if (typeof input.olderThan !== "number" || !Number.isFinite(input.olderThan)) {
      throw new VFSError("EINVAL", "dropVersions: olderThan must be finite");
    }
    normalized.olderThan = input.olderThan;
  }
  if (input.keepLast !== undefined) {
    if (
      typeof input.keepLast !== "number" ||
      !Number.isInteger(input.keepLast) ||
      input.keepLast < 0 ||
      input.keepLast > DROP_VERSIONS_KEEP_LAST_MAX
    ) {
      throw new VFSError(
        "EINVAL",
        `dropVersions: keepLast must be an integer from 0 to ${DROP_VERSIONS_KEEP_LAST_MAX}`
      );
    }
    normalized.keepLast = input.keepLast;
  }
  if (input.exceptVersions !== undefined) {
    if (!Array.isArray(input.exceptVersions)) {
      throw new VFSError("EINVAL", "dropVersions: exceptVersions must be an array");
    }
    if (input.exceptVersions.length > DROP_VERSIONS_EXCEPT_MAX) {
      throw new VFSError(
        "EINVAL",
        `dropVersions: exceptVersions exceeds ${DROP_VERSIONS_EXCEPT_MAX}`
      );
    }
    normalized.exceptVersions = input.exceptVersions.map((versionId) =>
      validateRetentionId(versionId, "exceptVersions id")
    );
  }
  return normalized;
}

export function assertLegacyDropVersionsBounded(
  durableObject: UserDO,
  userId: string,
  path: string,
  untrustedPolicy: DropVersionsPolicy
): void {
  const policy = validateDropVersionsPolicy(untrustedPolicy);
  if (durableObject.maintainVersionRetentionOrder()) {
    throw new VFSError(
      "EFBIG",
      "dropVersions: history requires the paged retention capability"
    );
  }
  const pathId = resolvePathId(durableObject, userId, path);
  if (!pathId) {
    throw new VFSError("ENOENT", `dropVersions: path not found: ${path}`);
  }
  const head = durableObject.sql
    .exec(
      "SELECT head_version_id FROM files WHERE file_id = ? AND user_id = ?",
      pathId,
      userId
    )
    .toArray()[0] as { head_version_id: string | null } | undefined;
  if (!head) {
    throw new VFSError("ENOENT", `dropVersions: path not found: ${path}`);
  }
  const versions = durableObject.sql
    .exec<RetentionVersionRow>(
      `SELECT version_id, mtime_ms FROM version_retention_order
        WHERE path_id = ?
        ORDER BY mtime_ms DESC, version_id DESC LIMIT ?`,
      pathId,
      DROP_VERSIONS_BATCH_LIMIT
    )
    .toArray();
  if (versions.length >= DROP_VERSIONS_BATCH_LIMIT) {
    throw new VFSError(
      "EFBIG",
      "dropVersions: history requires the paged retention capability"
    );
  }

  const keepSet = new Set(policy.exceptVersions ?? []);
  let remainingKeep = retentionRemainingKeep(policy, head.head_version_id);
  const droppedVersionIds: string[] = [];
  for (const version of versions) {
    const decision = retentionKeepDecision(
      policy,
      head.head_version_id,
      keepSet,
      version,
      remainingKeep
    );
    if (decision.keep) {
      if (decision.consumesKeepSlot) remainingKeep--;
    } else {
      droppedVersionIds.push(version.version_id);
    }
  }
  if (droppedVersionIds.length === 0) return;

  const manifestThreshold = Math.ceil(DROP_VERSIONS_MANIFEST_LIMIT / 2);
  const manifestOverflow = durableObject.sql
    .exec(
      `SELECT 1 FROM version_chunks
        WHERE version_id IN (
          SELECT CAST(value AS TEXT) FROM json_each(?)
        )
        LIMIT 1 OFFSET ?`,
      JSON.stringify(droppedVersionIds),
      manifestThreshold - 1
    )
    .toArray();
  if (manifestOverflow.length > 0) {
    throw new VFSError(
      "EFBIG",
      "dropVersions: manifests require the paged retention capability"
    );
  }
}

function retentionRemainingKeep(
  policy: DropVersionsPolicy,
  headVersionId: string | null
): number {
  return Math.max(0, (policy.keepLast ?? 0) - (headVersionId ? 1 : 0));
}

function retentionKeepDecision(
  policy: DropVersionsPolicy,
  headVersionId: string | null,
  keepSet: ReadonlySet<string>,
  version: RetentionVersionRow,
  remainingKeep: number
): RetentionKeepDecision {
  const explicitlyKept =
    version.version_id === headVersionId || keepSet.has(version.version_id);
  const consumesKeepSlot = !explicitlyKept && remainingKeep > 0;
  const keepForAge =
    policy.olderThan !== undefined && version.mtime_ms >= policy.olderThan;
  return {
    keep: explicitlyKept || consumesKeepSlot || keepForAge,
    consumesKeepSlot,
  };
}

function readRetentionOperation(
  durableObject: UserDO,
  operationId: string
): RetentionOperationRow | undefined {
  return durableObject.sql
    .exec<RetentionOperationRow>(
      `SELECT operation_id, user_id, requested_path, path_id, policy_json,
              plan_generation, plan_head_version_id, cursor_mtime_ms,
              cursor_version_id, remaining_keep, dropped, kept, status,
              pending_version_id, pending_mtime_ms, pending_ref_id,
              pending_metadata_deleted, manifest_cursor
         FROM version_retention_operations
        WHERE operation_id = ?`,
      operationId
    )
    .toArray()[0];
}

function persistRetentionOperation(
  durableObject: UserDO,
  operation: RetentionOperationRow,
  now: number
): void {
  durableObject.sql.exec(
    `UPDATE version_retention_operations
        SET plan_generation = ?, plan_head_version_id = ?,
            cursor_mtime_ms = ?, cursor_version_id = ?, remaining_keep = ?,
            dropped = ?, kept = ?, status = ?, pending_version_id = ?,
            pending_mtime_ms = ?, pending_ref_id = ?,
            pending_metadata_deleted = ?, manifest_cursor = ?, updated_at = ?
      WHERE operation_id = ?`,
    operation.plan_generation,
    operation.plan_head_version_id,
    operation.cursor_mtime_ms,
    operation.cursor_version_id,
    operation.remaining_keep,
    operation.dropped,
    operation.kept,
    operation.status,
    operation.pending_version_id,
    operation.pending_mtime_ms,
    operation.pending_ref_id,
    operation.pending_metadata_deleted,
    operation.manifest_cursor,
    now,
    operation.operation_id
  );
}

function maintainRetentionOperations(
  durableObject: UserDO,
  userId: string,
  now: number
): void {
  durableObject.sql.exec(
    `DELETE FROM version_retention_operations
      WHERE operation_id IN (
        SELECT operation_id FROM version_retention_operations
         WHERE user_id = ? AND status = 'done' AND updated_at < ?
         ORDER BY updated_at LIMIT ?
      )`,
    userId,
    now - COMPLETED_RETENTION_TTL_MS,
    RETENTION_OPERATION_PRUNE_LIMIT
  );
  durableObject.sql.exec(
    `UPDATE version_retention_operations SET status = 'expiring', updated_at = ?
      WHERE operation_id IN (
        SELECT operation_id FROM version_retention_operations
         WHERE user_id = ? AND status = 'running'
           AND pending_metadata_deleted = 0 AND updated_at < ?
         ORDER BY updated_at LIMIT ?
      )`,
    now,
    userId,
    now - RUNNING_RETENTION_TTL_MS,
    RETENTION_OPERATION_PRUNE_LIMIT
  );
  durableObject.sql.exec(
    `DELETE FROM version_retention_cleanup_routes
      WHERE rowid IN (
        SELECT r.rowid FROM version_retention_cleanup_routes r
        JOIN version_retention_operations o ON o.operation_id = r.operation_id
        WHERE o.user_id = ? AND o.status = 'expiring'
        ORDER BY o.updated_at, r.operation_id, r.version_id, r.shard_index
        LIMIT ?
      )`,
    userId,
    DROP_VERSIONS_MANIFEST_LIMIT
  );
  durableObject.sql.exec(
    `DELETE FROM version_retention_operations
      WHERE operation_id IN (
        SELECT o.operation_id FROM version_retention_operations o
         WHERE o.user_id = ? AND o.status = 'expiring'
           AND NOT EXISTS (
             SELECT 1 FROM version_retention_cleanup_routes r
              WHERE r.operation_id = o.operation_id
           )
         ORDER BY o.updated_at LIMIT ?
      )`,
    userId,
    RETENTION_OPERATION_PRUNE_LIMIT
  );
  durableObject.sql.exec(
    `DELETE FROM version_retention_operations
      WHERE operation_id IN (
        SELECT operation_id FROM version_retention_operations
         WHERE user_id = ? AND status = 'done'
         ORDER BY updated_at DESC
         LIMIT -1 OFFSET ?
      )
      AND operation_id IN (
        SELECT operation_id FROM version_retention_operations
         WHERE user_id = ? AND status = 'done'
         ORDER BY updated_at LIMIT ?
      )`,
    userId,
    RETENTION_TOTAL_MAX,
    userId,
    RETENTION_OPERATION_PRUNE_LIMIT
  );
}

function assertRetentionCapacity(durableObject: UserDO, userId: string): void {
  const counts = durableObject.sql
    .exec(
      `SELECT COUNT(*) AS total,
              SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END) AS running
         FROM version_retention_operations WHERE user_id = ?`,
      userId
    )
    .toArray()[0] as { total: number; running: number | null };
  if (counts.total >= RETENTION_TOTAL_MAX || (counts.running ?? 0) >= RETENTION_RUNNING_MAX) {
    throw new VFSError("EBUSY", "dropVersions: too many retained operations");
  }
}

function nextRetentionVersion(
  durableObject: UserDO,
  operation: RetentionOperationRow
): RetentionVersionRow | undefined {
  if (operation.cursor_mtime_ms === null) {
    return durableObject.sql
      .exec<RetentionVersionRow>(
        `SELECT version_id, mtime_ms FROM version_retention_order
          WHERE path_id = ?
          ORDER BY mtime_ms DESC, version_id DESC LIMIT 1`,
        operation.path_id
      )
      .toArray()[0];
  }
  if (operation.cursor_version_id === null) {
    throw new VFSError("EBUSY", "dropVersions: invalid retention cursor");
  }
  return durableObject.sql
    .exec<RetentionVersionRow>(
      `SELECT version_id, mtime_ms FROM version_retention_order
        WHERE path_id = ?
          AND (mtime_ms, version_id) < (?, ?)
        ORDER BY mtime_ms DESC, version_id DESC LIMIT 1`,
      operation.path_id,
      operation.cursor_mtime_ms,
      operation.cursor_version_id
    )
    .toArray()[0];
}

function dropRetentionVersionMetadata(
  durableObject: UserDO,
  operation: RetentionOperationRow
): "dropped" | "missing" | "head" {
  const version = durableObject.sql
    .exec(
      `SELECT v.size, v.deleted, LENGTH(v.inline_data) AS inline_bytes,
              f.head_version_id
         FROM file_versions v
         JOIN files f ON f.file_id = v.path_id AND f.user_id = v.user_id
        WHERE v.path_id = ? AND v.version_id = ? AND v.user_id = ?`,
      operation.path_id,
      operation.pending_version_id,
      operation.user_id
    )
    .toArray()[0] as
    | {
        size: number;
        deleted: number;
        inline_bytes: number | null;
        head_version_id: string | null;
      }
    | undefined;
  if (!version) return "missing";
  if (version.head_version_id === operation.pending_version_id) return "head";
  durableObject.sql.exec(
    `DELETE FROM file_versions
      WHERE path_id = ? AND version_id = ? AND user_id = ?`,
    operation.path_id,
    operation.pending_version_id,
    operation.user_id
  );
  if (lastSqlChanges(durableObject) !== 1) {
    throw new VFSError("EBUSY", "dropVersions: version changed during retention");
  }
  if (version.deleted === 0) {
    recordWriteUsage(
      durableObject,
      operation.user_id,
      -version.size,
      0,
      -(version.inline_bytes ?? 0)
    );
  }
  return "dropped";
}

/** Process one persistent, idempotent retention step. */
export async function dropVersions(
  durableObject: UserDO,
  scope: VFSScope,
  userId: string,
  path: string,
  untrustedPolicy: DropVersionsPolicy,
  untrustedOperationId: string
): Promise<DropVersionsStepResult> {
  const operationId = validateRetentionId(untrustedOperationId, "operation id");
  if (operationId.length > RETENTION_OPERATION_ID_MAX) {
    throw new VFSError("EINVAL", "dropVersions: invalid operation id");
  }
  const policy = validateDropVersionsPolicy(untrustedPolicy);
  const policyJson = JSON.stringify(policy);

  if (transactionSync(durableObject, () => durableObject.maintainVersionRetentionOrder())) {
    await scheduleStaleUploadSweep(durableObject);
    return { done: false };
  }

  await scheduleStaleUploadSweep(durableObject);
  const { drainChunkCleanupIntents } = await import("./vfs-ops");
  const result = transactionSync(durableObject, () => {
    const now = Date.now();
    let operation = readRetentionOperation(durableObject, operationId);
    if (!operation) {
      maintainRetentionOperations(durableObject, userId, now);
      assertRetentionCapacity(durableObject, userId);
      const pathId = resolvePathId(durableObject, userId, path);
      if (!pathId) {
        throw new VFSError("ENOENT", `dropVersions: path not found: ${path}`);
      }
      const file = durableObject.sql
        .exec(
          `SELECT head_version_id, version_generation
             FROM files WHERE file_id = ? AND user_id = ?`,
          pathId,
          userId
        )
        .toArray()[0] as
        | { head_version_id: string | null; version_generation: number }
        | undefined;
      if (!file) {
        throw new VFSError("ENOENT", `dropVersions: path not found: ${path}`);
      }
      durableObject.sql.exec(
        `INSERT INTO version_retention_operations
           (operation_id, user_id, requested_path, path_id, policy_json,
            plan_generation, plan_head_version_id, cursor_mtime_ms,
            cursor_version_id, remaining_keep, dropped, kept, status,
            pending_version_id, pending_mtime_ms, pending_ref_id,
            pending_metadata_deleted, manifest_cursor, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, 0, 0, 'running',
                 NULL, NULL, NULL, 0, NULL, ?, ?)`,
        operationId,
        userId,
        path,
        pathId,
        policyJson,
        file.version_generation,
        file.head_version_id,
        retentionRemainingKeep(policy, file.head_version_id),
        now,
        now
      );
      operation = readRetentionOperation(durableObject, operationId);
      if (!operation) {
        throw new VFSError("EBUSY", "dropVersions: operation was not persisted");
      }
    }

    if (
      operation.user_id !== userId ||
      operation.requested_path !== path ||
      operation.policy_json !== policyJson
    ) {
      throw new VFSError("EINVAL", "dropVersions: operation parameters changed");
    }
    if (operation.status === "done") {
      return {
        step: {
          done: true,
          dropped: operation.dropped,
          kept: operation.kept,
        } satisfies DropVersionsStepResult,
        refIds: [] as string[],
      };
    }
    if (operation.status !== "running") {
      throw new VFSError("EINVAL", "dropVersions: operation expired");
    }

    const file = durableObject.sql
      .exec(
        `SELECT head_version_id, version_generation
           FROM files WHERE file_id = ? AND user_id = ?`,
        operation.path_id,
        userId
      )
      .toArray()[0] as
      | { head_version_id: string | null; version_generation: number }
      | undefined;
    if (!file) {
      throw new VFSError("EBUSY", "dropVersions: path changed during retention");
    }

    let manifestBudget = DROP_VERSIONS_MANIFEST_LIMIT;
    let cleanupBudget = DROP_VERSIONS_CLEANUP_INTENT_LIMIT;
    let versionsProcessed = 0;
    const refIds = new Set<string>();
    const keepSet = new Set(policy.exceptVersions ?? []);

    while (true) {
      if (operation.pending_version_id !== null) {
        if (operation.pending_mtime_ms === null || operation.pending_ref_id === null) {
          throw new VFSError("EBUSY", "dropVersions: invalid pending version");
        }

        if (operation.pending_metadata_deleted === 0) {
          if (manifestBudget === 0) {
            persistRetentionOperation(durableObject, operation, now);
            return { step: { done: false } as const, refIds: [...refIds] };
          }
          const scanLimit = manifestBudget;
          const manifestRows = durableObject.sql
            .exec<RetentionManifestRow>(
              `SELECT chunk_index, shard_index FROM version_chunks
                WHERE version_id = ? AND chunk_index > ?
                ORDER BY chunk_index LIMIT ?`,
              operation.pending_version_id,
              operation.manifest_cursor ?? -1,
              scanLimit
            )
            .toArray();
          for (const row of manifestRows) {
            durableObject.sql.exec(
              `INSERT OR IGNORE INTO version_retention_cleanup_routes
                 (operation_id, version_id, ref_id, shard_index)
               VALUES (?, ?, ?, ?)`,
              operation.operation_id,
              operation.pending_version_id,
              operation.pending_ref_id,
              row.shard_index
            );
          }
          manifestBudget -= manifestRows.length;
          if (manifestRows.length > 0) {
            operation.manifest_cursor = manifestRows.at(-1)?.chunk_index ?? null;
          }
          if (manifestRows.length === scanLimit) {
            persistRetentionOperation(durableObject, operation, now);
            return { step: { done: false } as const, refIds: [...refIds] };
          }

          const metadataResult = dropRetentionVersionMetadata(
            durableObject,
            operation
          );
          if (metadataResult === "head") {
            operation.pending_metadata_deleted = 2;
          } else {
            operation.pending_metadata_deleted = 1;
            if (metadataResult === "dropped") operation.dropped++;
          }
          operation.manifest_cursor = null;
        }

        if (operation.pending_metadata_deleted === 1) {
          if (manifestBudget > 0) {
            const manifestRows = durableObject.sql
              .exec<RetentionManifestRow>(
                `SELECT chunk_index, shard_index FROM version_chunks
                  WHERE version_id = ? ORDER BY chunk_index LIMIT ?`,
                operation.pending_version_id,
                manifestBudget
              )
              .toArray();
            for (const row of manifestRows) {
              durableObject.sql.exec(
                `DELETE FROM version_chunks
                  WHERE version_id = ? AND chunk_index = ?`,
                operation.pending_version_id,
                row.chunk_index
              );
            }
            manifestBudget -= manifestRows.length;
            if (manifestRows.length > 0 && manifestBudget === 0) {
              persistRetentionOperation(durableObject, operation, now);
              return { step: { done: false } as const, refIds: [...refIds] };
            }
          }

          if (cleanupBudget > 0) {
            const routes = durableObject.sql
              .exec<RetentionManifestRow>(
                `SELECT 0 AS chunk_index, shard_index
                   FROM version_retention_cleanup_routes
                  WHERE operation_id = ? AND version_id = ?
                  ORDER BY shard_index LIMIT ?`,
                operation.operation_id,
                operation.pending_version_id,
                cleanupBudget
              )
              .toArray();
            for (const route of routes) {
              stageChunkCleanupIntent(
                durableObject,
                operation.pending_ref_id,
                route.shard_index,
                now
              );
              durableObject.sql.exec(
                `DELETE FROM version_retention_cleanup_routes
                  WHERE operation_id = ? AND version_id = ? AND shard_index = ?`,
                operation.operation_id,
                operation.pending_version_id,
                route.shard_index
              );
            }
            cleanupBudget -= routes.length;
            if (routes.length > 0) refIds.add(operation.pending_ref_id);
          }
        } else {
          const routes = durableObject.sql
            .exec<RetentionManifestRow>(
              `SELECT 0 AS chunk_index, shard_index
                 FROM version_retention_cleanup_routes
                WHERE operation_id = ? AND version_id = ?
                ORDER BY shard_index LIMIT ?`,
              operation.operation_id,
              operation.pending_version_id,
              cleanupBudget
            )
            .toArray();
          for (const route of routes) {
            durableObject.sql.exec(
              `DELETE FROM version_retention_cleanup_routes
                WHERE operation_id = ? AND version_id = ? AND shard_index = ?`,
              operation.operation_id,
              operation.pending_version_id,
              route.shard_index
            );
          }
          cleanupBudget -= routes.length;
        }

        const routesRemain = durableObject.sql
          .exec(
            `SELECT 1 FROM version_retention_cleanup_routes
              WHERE operation_id = ? AND version_id = ? LIMIT 1`,
            operation.operation_id,
            operation.pending_version_id
          )
          .toArray().length > 0;
        const manifestRemains = operation.pending_metadata_deleted === 1 &&
          durableObject.sql
            .exec(
              "SELECT 1 FROM version_chunks WHERE version_id = ? LIMIT 1",
              operation.pending_version_id
            )
            .toArray().length > 0;
        if (routesRemain || manifestRemains) {
          persistRetentionOperation(durableObject, operation, now);
          return { step: { done: false } as const, refIds: [...refIds] };
        }

        operation.cursor_mtime_ms = operation.pending_mtime_ms;
        operation.cursor_version_id = operation.pending_version_id;
        operation.pending_version_id = null;
        operation.pending_mtime_ms = null;
        operation.pending_ref_id = null;
        operation.pending_metadata_deleted = 0;
        operation.manifest_cursor = null;
        if (file.head_version_id !== operation.plan_head_version_id) {
          continue;
        }
      }

      if (
        operation.plan_generation !== file.version_generation ||
        operation.plan_head_version_id !== file.head_version_id
      ) {
        operation.plan_generation = file.version_generation;
        operation.plan_head_version_id = file.head_version_id;
        operation.cursor_mtime_ms = null;
        operation.cursor_version_id = null;
        operation.remaining_keep = retentionRemainingKeep(
          policy,
          file.head_version_id
        );
        operation.kept = 0;
      }

      if (versionsProcessed >= DROP_VERSIONS_BATCH_LIMIT) {
        persistRetentionOperation(durableObject, operation, now);
        return { step: { done: false } as const, refIds: [...refIds] };
      }
      const version = nextRetentionVersion(durableObject, operation);
      if (!version) {
        operation.status = "done";
        if (operation.dropped + operation.kept > 0) {
          insertAuditLog(durableObject, {
            op: "dropVersions",
            actor: userId,
            target: operation.path_id,
            payload: JSON.stringify({
              dropped: operation.dropped,
              kept: operation.kept,
              policy,
            }),
          });
        }
        persistRetentionOperation(durableObject, operation, now);
        return {
          step: {
            done: true,
            dropped: operation.dropped,
            kept: operation.kept,
          } as const,
          refIds: [...refIds],
        };
      }

      versionsProcessed++;
      const decision = retentionKeepDecision(
        policy,
        file.head_version_id,
        keepSet,
        version,
        operation.remaining_keep
      );
      if (decision.keep) {
        if (decision.consumesKeepSlot) operation.remaining_keep--;
        operation.kept++;
        operation.cursor_mtime_ms = version.mtime_ms;
        operation.cursor_version_id = version.version_id;
        continue;
      }

      const refRow = durableObject.sql
        .exec(
          "SELECT shard_ref_id FROM file_versions WHERE path_id = ? AND version_id = ?",
          operation.path_id,
          version.version_id
        )
        .toArray()[0] as { shard_ref_id: string | null } | undefined;
      if (!refRow) continue;
      operation.pending_version_id = version.version_id;
      operation.pending_mtime_ms = version.mtime_ms;
      operation.pending_ref_id =
        refRow.shard_ref_id ?? shardRefId(operation.path_id, version.version_id);
      operation.pending_metadata_deleted = 0;
      operation.manifest_cursor = null;
    }
  });

  if (result.refIds.length > 0) {
    await drainChunkCleanupIntents(durableObject, scope, result.refIds);
  }
  return result.step;
}

export async function resumeVersionRetentionOperations(
  durableObject: UserDO,
  scope: VFSScope
): Promise<boolean> {
  const userId = userIdFor(scope);
  const operation = transactionSync(durableObject, () => {
    maintainRetentionOperations(durableObject, userId, Date.now());
    return durableObject.sql
      .exec<{
        operation_id: string;
        requested_path: string;
        policy_json: string;
      } & Record<string, SqlStorageValue>>(
        `SELECT operation_id, requested_path, policy_json
           FROM version_retention_operations
          WHERE user_id = ? AND status = 'running'
          ORDER BY updated_at LIMIT 1`,
        userId
      )
      .toArray()[0];
  });
  if (operation) {
    const policy = validateDropVersionsPolicy(JSON.parse(operation.policy_json));
    await dropVersions(
      durableObject,
      scope,
      userId,
      operation.requested_path,
      policy,
      operation.operation_id
    );
  }
  return durableObject.sql
    .exec(
      `SELECT 1 FROM version_retention_operations
        WHERE user_id = ? AND status IN ('running', 'expiring') LIMIT 1`,
      userId
    )
    .toArray().length > 0;
}

/**
 * Restore a historical version by creating a NEW version row whose
 * content is the same as the source. The new row carries a fresh
 * version_id; chunk_refs are added to ShardDOs (or content is
 * inlined) so refcount math stays correct.
 *
 * S3 semantics: this is a copy, not a pointer. The old version is
 * unchanged and remains in the history list. The new version
 * becomes head.
 */
export async function restoreVersion(
  durableObject: UserDO,
  scope: VFSScope,
  userId: string,
  pathId: string,
  sourceVersionId: string
): Promise<{ versionId: string }> {
  const pathRow = durableObject.sql
    .exec(
      `SELECT parent_id, file_name, head_version_id, mode_yjs
         FROM files
        WHERE file_id = ? AND user_id = ? AND status = 'complete'`,
      pathId,
      userId
    )
    .toArray()[0] as
    | {
        parent_id: string | null;
        file_name: string;
        head_version_id: string | null;
        mode_yjs: number;
      }
    | undefined;
  if (!pathRow) {
    throw new VFSError("ENOENT", `restoreVersion: path ${pathId} not found`);
  }
  const expectedPath: VersionedFileExpectation = {
    fileId: pathId,
    userId,
    parentId: pathRow.parent_id,
    fileName: pathRow.file_name,
    headVersionId: pathRow.head_version_id,
  };
  const src = getVersion(durableObject, pathId, sourceVersionId);
  if (!src) {
    throw new VFSError("ENOENT", `version ${sourceVersionId} not found`);
  }
  if (src.deleted) {
    // Restoring a tombstone is meaningless (would create another
    // tombstone). Surface as EINVAL.
    throw new VFSError(
      "EINVAL",
      `cannot restore tombstone version ${sourceVersionId}`
    );
  }

  const newVersionId = generateId();
  const now = Date.now();

  // Restore can flip a tombstoned path back to live, or simply
  // advance head_version_id of an already-live path. Either case
  // warrants a folder-revision bump so listChildren observers
  // re-fetch. Read parent_id of the path's stable row.
  const parentId = pathRow.parent_id;

  const assertSourceVersion = (): void => {
    const sourceRows = durableObject.sql
      .exec(
        "SELECT 1 FROM file_versions WHERE path_id = ? AND version_id = ? AND user_id = ?",
        pathId,
        sourceVersionId,
        userId
      )
      .toArray();
    if (sourceRows.length !== 1) {
      throw new VFSError(
        "EBUSY",
        `restoreVersion: source version ${sourceVersionId} changed during publication`
      );
    }
  };

  if (pathRow.mode_yjs === 1) {
    if (src.encryption) {
      throw new VFSError("ENOTSUP", "restoreVersion: encrypted Yjs versions require client-side restore");
    }
    if (!src.inlineData) {
      throw new VFSError("ENOTSUP", "restoreVersion: Yjs version is not an inline snapshot");
    }
    const bytes = new Uint8Array(src.inlineData);
    const { hasYjsSnapshotMagic, writeYjsBytes } = await import("./yjs");
    if (!hasYjsSnapshotMagic(bytes)) {
      throw new VFSError("ENOTSUP", "restoreVersion: Yjs version is not a restorable snapshot");
    }
    return runWithConcurrencyBlocked(durableObject, async () => {
      assertVersionedFileExpectation(
        durableObject,
        expectedPath,
        "restoreVersion"
      );
      assertSourceVersion();
      await writeYjsBytes(
        durableObject,
        scope,
        userId,
        pathId,
        poolSizeFor(durableObject, userId),
        bytes
      );
      transactionSync(durableObject, () => {
        assertSourceVersion();
        commitVersionChecked(
          durableObject,
          {
            pathId,
            versionId: newVersionId,
            userId,
            size: src.size,
            mode: src.mode,
            mtimeMs: now,
            chunkSize: 0,
            chunkCount: 0,
            fileHash: src.fileHash,
            mimeType: src.mimeType,
            inlineData: bytes,
            encryption: src.encryption,
          },
          expectedPath,
          "restoreVersion"
        );
        bumpFolderRevision(durableObject, userId, parentId);
      });
      insertAuditLog(durableObject, {
        op: "restoreVersion",
        actor: userId,
        target: pathId,
        payload: JSON.stringify({
          sourceVersionId,
          newVersionId,
          tier: "inline",
        }),
      });
      return { versionId: newVersionId };
    });
  }

  if (src.inlineData) {
    // Inline restore: no shard work; just insert + flip head.
    const inlineData = new Uint8Array(src.inlineData);
    transactionSync(durableObject, () => {
      assertSourceVersion();
      commitVersionChecked(
        durableObject,
        {
          pathId,
          versionId: newVersionId,
          userId,
          size: src.size,
          mode: src.mode,
          mtimeMs: now,
          chunkSize: 0,
          chunkCount: 0,
          fileHash: src.fileHash,
          mimeType: src.mimeType,
          inlineData,
          encryption: src.encryption,
        },
        expectedPath,
        "restoreVersion"
      );
      bumpFolderRevision(durableObject, userId, parentId);
    });
    insertAuditLog(durableObject, {
      op: "restoreVersion",
      actor: userId,
      target: pathId,
      payload: JSON.stringify({
        sourceVersionId,
        newVersionId,
        tier: "inline",
      }),
    });
    return { versionId: newVersionId };
  }

  // Chunked restore: fan out chunk refs to ShardDOs under the new
  // synthetic file_id, then mirror version_chunks rows. We do NOT
  // re-upload chunk bytes — content-addressed dedup means the
  // existing chunks (still referenced by the source version) are
  // ALREADY on the right shards. We only add a new ref slot.
  const env = durableObject.envPublic;
  const shardNs = env.MOSSAIC_SHARD as unknown as DurableObjectNamespace<ShardDO>;
  const newRefId = shardRefId(pathId, newVersionId);
  const {
    disarmChunkCleanupIntents,
    drainChunkCleanupIntents,
    stageChunkCleanupIntents,
  } = await import("./vfs-ops");

  const chunks = durableObject.sql
    .exec(
      `SELECT chunk_index, chunk_hash, chunk_size, shard_index
         FROM version_chunks WHERE version_id = ?
        ORDER BY chunk_index`,
      sourceVersionId
    )
    .toArray() as {
    chunk_index: number;
    chunk_hash: string;
    chunk_size: number;
    shard_index: number;
  }[];

  // P1-1 fix — atomic ref restoration via `restoreChunkRef` RPC.
  //
  // Pre-fix this code did `chunksAlive` pre-flight + a per-chunk
  // `putChunk(empty)` loop. Between the two RPCs an unrelated
  // concurrent `dropVersions` could decrement chunk_refs on a
  // shared chunk to zero, soft-mark it, and let the alarm sweeper
  // reap it during the 30s grace window. The 0-byte cold-path
  // defense at writeChunkInternal blocked silent corruption but
  // the throw-mid-loop path leaked partial chunk_refs under
  // `newRefId` permanently.
  //
  // The new ShardDO RPC `restoreChunkRef` collapses (verify alive
  // + INSERT OR IGNORE chunk_refs + bump ref_count) into ONE atomic
  // DO turn. No await between the existence check and the ref
  // bump → no `dropVersions` can sweep the chunk between our check
  // and our increment. The audit C2 TOCTOU window is closed
  // structurally.
  //
  // Per-chunk RPCs run sequentially within a shard (matching the
  // original putChunk loop's ordering for predictable subrequest
  // count) but parallelize across shards via Promise.all to keep
  // wall-clock latency bounded.
  const byShard = new Map<
    number,
    { chunk_hash: string; chunk_index: number; chunk_size: number }[]
  >();
  for (const c of chunks) {
    const arr = byShard.get(c.shard_index) ?? [];
    arr.push({
      chunk_hash: c.chunk_hash,
      chunk_index: c.chunk_index,
      chunk_size: c.chunk_size,
    });
    byShard.set(c.shard_index, arr);
  }
  await stageChunkCleanupIntents(durableObject, newRefId, byShard.keys());
  const restoreResults = await Promise.allSettled(
    Array.from(byShard.entries()).map(async ([shardIndex, shardChunks]) => {
      const shardName = vfsShardDOName(
        scope.ns,
        scope.tenant,
        scope.sub,
        shardIndex
      );
      const stub = shardNs.get(shardNs.idFromName(shardName));
      for (const c of shardChunks) {
        try {
          await stub.restoreChunkRef(
            c.chunk_hash,
            newRefId,
            c.chunk_index,
            userId
          );
        } catch (err) {
          const msg = err instanceof Error ? err.message : String(err);
          if (msg.startsWith("ENOENT")) {
            throw new VFSError(
              "ENOENT",
              `restoreVersion: source chunk ${c.chunk_hash} swept on shard ${shardIndex}`
            );
          }
          throw err;
        }
      }
    })
  );
  const restoreFailure = restoreResults.find(
    (result): result is PromiseRejectedResult => result.status === "rejected"
  );
  if (restoreFailure) {
    await drainChunkCleanupIntents(durableObject, scope, newRefId);
    throw restoreFailure.reason;
  }

  try {
    transactionSync(durableObject, () => {
      assertSourceVersion();
      for (const chunk of chunks) {
        insertVersionChunk(durableObject, newVersionId, chunk);
      }
      commitVersionChecked(
        durableObject,
        {
          pathId,
          versionId: newVersionId,
          userId,
          size: src.size,
          mode: src.mode,
          mtimeMs: now,
          chunkSize: src.chunkSize,
          chunkCount: src.chunkCount,
          fileHash: src.fileHash,
          mimeType: src.mimeType,
          inlineData: null,
          encryption: src.encryption,
        },
        expectedPath,
        "restoreVersion"
      );
      bumpFolderRevision(durableObject, userId, parentId);
      disarmChunkCleanupIntents(durableObject, newRefId);
    });
  } catch (err) {
    durableObject.sql.exec(
      "DELETE FROM version_chunks WHERE version_id = ?",
      newVersionId
    );
    await drainChunkCleanupIntents(durableObject, scope, newRefId);
    throw err;
  }
  insertAuditLog(durableObject, {
    op: "restoreVersion",
    actor: userId,
    target: pathId,
    payload: JSON.stringify({
      sourceVersionId,
      newVersionId,
      tier: "chunked",
    }),
  });
  return { versionId: newVersionId };
}
