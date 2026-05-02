import type { UserDOCore as UserDO } from "./user-do-core";
import type { ShardDO } from "../shard/shard-do";
import { VFSError, type VFSScope } from "../../../../shared/vfs-types";
import { generateId, vfsShardDOName } from "../../lib/utils";
import { placeChunk } from "../../../../shared/placement";
import {
  bumpFolderRevision,
  recordWriteUsage,
  userIdFor,
} from "./vfs/helpers";
import { insertAuditLog } from "./vfs/audit-log";
import { resolvePath } from "./path-walk";

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
 *   Lean proves that under monotonic clock (Date.now() non-decreasing),
 *   inserting a new version advances `maxMtime` for the path. This is
 *   the algebraic core of versioning monotonicity. See
 *   `lean/Mossaic/Vfs/Versioning.lean :: insertVersion_max_ge`. The
 *   stretch goal proof is partial; the structural sub-property is
 *   unconditional.
 */
export function commitVersion(
  durableObject: UserDO,
  args: {
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
    /**
     * per-version flags. All optional; defaults preserve
     * the legacy default behaviour (NULL label, user_visible=0,
     * NULL metadata).
     *
     * - `userVisible`: when truthy, sets `file_versions.user_visible = 1`.
     *   Used by writeFile (default true), restoreVersion (true), and
     *   YjsRuntime.compact when called via flush() (true). Opportunistic
     *   YjsRuntime compactions pass false.
     * - `label`: optional ≤128-char human label; SDK validates.
     * - `metadata`: snapshot of `files.metadata` at commit time, as
     *   already-encoded bytes. NULL preserves the column's NULL.
     */
    userVisible?: boolean;
    label?: string | null;
    metadata?: Uint8Array | null;
    /**
     * per-version encryption stamp. Mirrors the column on
     * `files`. When set, the columns on both `file_versions` (this
     * row) and `files` (the head row) are updated. The `data` payload
     * has already been written; this is metadata only.
     */
    encryption?: { mode: "convergent" | "random"; keyId?: string };
    /**
     * Multipart × versioning. ShardDO chunk_refs file_id actually
     * used when chunks were written. Default `null`; the canonical
     * versioned write path uses the synthetic form
     * `${pathId}#${versionId}` (computed by `dropVersionRows` on
     * fan-out) and leaves this NULL. Multipart-finalize-under-
     * versioning sets it to `uploadId` because that's the refId
     * the chunk PUT route used during upload — see
     * `vfsFinalizeMultipart`'s versioned branch.
     */
    shardRefId?: string;
  }
): void {
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
  durableObject.sql.exec(
    `UPDATE files
        SET head_version_id = ?,
            updated_at = ?,
            encryption_mode = ?,
            encryption_key_id = ?
      WHERE file_id = ?`,
    args.versionId,
    args.mtimeMs,
    encMode,
    encKeyId,
    args.pathId
  );

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

/**
 * Decrement chunk_refs across a set of versions on every shard the
 * versions touched, then DELETE the file_versions + version_chunks
 * rows. The ShardDO alarm sweeper handles blob hard-delete after the
 * 30s grace per.
 *
 * Returns the count of versions reaped.
 */
export async function dropVersionRows(
  durableObject: UserDO,
  scope: VFSScope,
  userId: string,
  pathId: string,
  versionIds: string[]
): Promise<number> {
  if (versionIds.length === 0) return 0;
  const env = durableObject.envPublic;
  const shardNs = env.MOSSAIC_SHARD as unknown as DurableObjectNamespace<ShardDO>;

  // dropVersionRows owns ALL three counters' negative deltas for
  // versioning-on tenants. Without symmetric decrements,
  // storage_used and file_count would drift upward forever (only
  // inline_bytes_used was decremented historically). The
  // versioning-on write path increments via commitVersion;
  // dropVersionRows is the symmetric decrement. Bytes are
  // accumulated across non-tombstone versions; the file_count
  // delta fires when the `files` row gets dropped at the end
  // (path goes ENOENT) AND the path was previously live.
  let bytesReaped = 0;
  let inlineBytesReaped = 0;

  // Snapshot whether the path is currently LIVE (head is
  // non-tombstone) BEFORE we start dropping. If the `files` row
  // ends up dropped at the end (liveCount === 0), the path
  // exits the live-set and we owe a -1 file_count delta. If the
  // path was already tombstoned, file_count was already
  // decremented by the commitVersion(deleted=true) that produced
  // the tombstone \u2014 don't double-decrement.
  const wasLiveAtStart = (() => {
    const r = durableObject.sql
      .exec(
        `SELECT v.deleted AS d
           FROM files f
           LEFT JOIN file_versions v
             ON v.path_id = f.file_id AND v.version_id = f.head_version_id
          WHERE f.file_id = ?`,
        pathId
      )
      .toArray()[0] as { d: number | null } | undefined;
    return !!r && r.d === 0;
  })();

  let reaped = 0;
  for (const versionId of versionIds) {
    // Audit C2 structural guard: we MUST NOT decrement ShardDO refs
    // for a version whose file_versions row still exists. The
    // (UserDO-side metadata) → (ShardDO refs) order matters: if we
    // drop refs first and then crash before deleting the metadata,
    // a subsequent restoreVersion would resolve a stale manifest
    // pointing at chunks whose refcount may already be 0 / swept,
    // and the reuse path in restoreVersion would silently corrupt
    // data. Always delete metadata BEFORE the RPC fan-out.

    // Find unique shards this version's chunks live on.
    const shardRows = durableObject.sql
      .exec(
        "SELECT DISTINCT shard_index FROM version_chunks WHERE version_id = ?",
        versionId
      )
      .toArray() as { shard_index: number }[];

    // Read the per-version `shard_ref_id` BEFORE the delete so
    // multipart-finalized versions can fan out to the correct
    // chunk_refs key (uploadId, not the synthetic
    // `${pathId}#${versionId}`). Falls back to the synthetic form
    // when the column is NULL — matches every legacy and canonical
    // versioned-write row.
    //
    // Also read size, inline_data, and deleted so we can
    // accumulate the byte deltas for the post-loop
    // recordWriteUsage call. Tombstones (`deleted=1`)
    // contribute 0 (their bytes were never positive-counted by
    // commitVersion). Live (`deleted=0`) versions contribute
    // `+size` for storage_used and (if inline) `+inline_data.byteLength`
    // for inline_bytes_used.
    const refRow = durableObject.sql
      .exec(
        "SELECT shard_ref_id, size, inline_data, deleted FROM file_versions WHERE path_id = ? AND version_id = ?",
        pathId,
        versionId
      )
      .toArray()[0] as
      | {
          shard_ref_id: string | null;
          size: number;
          inline_data: ArrayBuffer | null;
          deleted: number;
        }
      | undefined;
    const explicitRefId = refRow?.shard_ref_id ?? null;
    if (refRow && refRow.deleted === 0) {
      bytesReaped += refRow.size;
      if (refRow.inline_data) {
        inlineBytesReaped += refRow.inline_data.byteLength;
      }
    }

    // Drop UserDO-side metadata first (mirrors hardDeleteFileRow).
    // version_chunks must go before file_versions because some
    // future GC paths key off file_versions presence.
    durableObject.sql.exec(
      "DELETE FROM version_chunks WHERE version_id = ?",
      versionId
    );
    durableObject.sql.exec(
      "DELETE FROM file_versions WHERE path_id = ? AND version_id = ?",
      pathId,
      versionId
    );

    // Sanity: re-read and confirm both metadata rows are gone before
    // we touch any ShardDO. Refusing to drop refs when metadata is
    // still present is the load-bearing structural invariant —
    // chunk_refs are reachable from file_versions/version_chunks; if
    // one side leaks, the other does too. This is belt-and-suspenders
    // (DO single-thread guarantees no concurrent INSERT can re-create
    // the rows we just deleted), but it pins the invariant in code.
    const stillPresent = durableObject.sql
      .exec(
        "SELECT 1 FROM file_versions WHERE path_id = ? AND version_id = ? LIMIT 1",
        pathId,
        versionId
      )
      .toArray();
    if (stillPresent.length > 0) {
      throw new VFSError(
        "EINVAL",
        `dropVersionRows: file_versions row for ${versionId} still present after delete; refusing to fan out chunk decrement`
      );
    }

    // Dispatch deleteChunks RPC per touched shard. Use the explicit
    // `shard_ref_id` stamped at write time when set (multipart
    // path); else fall back to the synthetic form used by the
    // canonical versioned-write path.
    const shardFileId = explicitRefId ?? shardRefId(pathId, versionId);
    for (const { shard_index } of shardRows) {
      const shardName = vfsShardDOName(scope.ns, scope.tenant, scope.sub, shard_index);
      const stub = shardNs.get(shardNs.idFromName(shardName));
      await stub.deleteChunks(shardFileId);
    }
    reaped++;
  }

  // Single recordWriteUsage call covering all three counters for
  // the whole batch. Without symmetric decrements,
  // storage_used + file_count would drift forever for versioning
  // tenants (only inline_bytes_used was decremented historically).
  // Symmetric with commitVersion.
  //
  // file_count delta: -1 IFF the path was LIVE at the start of
  // the drop AND the `files` row gets dropped below (liveCount === 0
  // \u2014 the path goes ENOENT). If the path was already tombstoned
  // when drop started, file_count was already decremented by the
  // commitVersion(deleted=true) that wrote the tombstone \u2014 we
  // must not double-decrement here.
  const liveCount = (
    durableObject.sql
      .exec(
        "SELECT COUNT(*) AS n FROM file_versions WHERE path_id = ?",
        pathId
      )
      .toArray()[0] as { n: number }
  ).n;
  const willDropFilesRow = liveCount === 0;
  const filesRowDelta = willDropFilesRow && wasLiveAtStart ? -1 : 0;

  if (
    bytesReaped > 0 ||
    inlineBytesReaped > 0 ||
    filesRowDelta !== 0
  ) {
    recordWriteUsage(
      durableObject,
      userId,
      -bytesReaped,
      filesRowDelta,
      -inlineBytesReaped
    );
  }

  // After dropping, if no live version remains AND no tombstone
  // either, also drop the empty `files` row so the path becomes
  // ENOENT cleanly.
  if (willDropFilesRow) {
    durableObject.sql.exec(
      "DELETE FROM files WHERE file_id = ? AND user_id = ?",
      pathId,
      userId
    );
  } else {
    // Reset the head pointer to the (still extant) newest LIVE
    // version.
    //
    // Tombstone-consistency. The query MUST filter on
    // `WHERE deleted = 0` (not just
    // `ORDER BY mtime_ms DESC LIMIT 1`). If retention dropped every
    // live version while a tombstone survived (e.g. operator
    // dropped older live versions newer than a retention cutoff
    // that also captured a tombstone in between), without the
    // filter the head would be repointed at a tombstone — making
    // subsequent stat() throw the systemic "head version is a
    // tombstone" error.
    //
    // We prefer the newest non-tombstoned version. Two outcomes:
    //   A. A live version exists → head moves to it; stat() works.
    //   B. Only tombstones survive → head goes to NULL; stat() falls
    //      through helpers.ts:225 to the non-versioned branch using
    //      the denormalized `files` columns (which were updated by
    //      the original write). Listings filter the row by the new
    //      tombstone consistency invariant when the most recent
    //      tombstone is the apparent head — `LEFT JOIN
    //      file_versions ON head_version_id` returns NULL columns
    //      and the row is INCLUDED. To keep listings semantically
    //      consistent we'd want to also drop the `files` row when
    //      only tombstones remain, but the user has explicitly kept
    //      these tombstone rows by NOT including them in the drop
    //      set — they want history preserved. Setting head to NULL
    //      preserves that history while making stat() succeed
    //      (returning the denormalized stat as a "no current
    //      version" placeholder).
    const headRow = durableObject.sql
      .exec(
        `SELECT version_id FROM file_versions
          WHERE path_id = ? AND deleted = 0
          ORDER BY mtime_ms DESC
          LIMIT 1`,
        pathId
      )
      .toArray()[0] as { version_id: string } | undefined;
    if (headRow) {
      durableObject.sql.exec(
        "UPDATE files SET head_version_id = ?, updated_at = ? WHERE file_id = ?",
        headRow.version_id,
        Date.now(),
        pathId
      );
    } else {
      // Only tombstones remain. Clear head_version_id so stat()
      // takes the non-versioned branch instead of throwing.
      durableObject.sql.exec(
        "UPDATE files SET head_version_id = NULL, updated_at = ? WHERE file_id = ?",
        Date.now(),
        pathId
      );
    }
  }

  return reaped;
}

/**
 * dropVersions retention policy:
 *   - olderThan: drop versions whose mtime_ms < cutoff (ms epoch)
 *   - keepLast: keep the N newest versions; drop the rest
 *   - exceptVersions: explicit allowlist that survives any other
 *     filter
 *   - all three may combine; the surviving set = intersection of the
 *     keep predicates.
 *
 * The CURRENT head version is never dropped — even if filters say
 * to. (S3 has the same invariant: you can't delete the current
 * version through a retention policy.)
 */
export async function dropVersions(
  durableObject: UserDO,
  scope: VFSScope,
  userId: string,
  pathId: string,
  policy: {
    olderThan?: number;
    keepLast?: number;
    exceptVersions?: string[];
  }
): Promise<{ dropped: number; kept: number }> {
  const all = listVersions(durableObject, pathId, { limit: 100_000 });
  if (all.length === 0) return { dropped: 0, kept: 0 };

  const headRow = durableObject.sql
    .exec(
      "SELECT head_version_id FROM files WHERE file_id = ?",
      pathId
    )
    .toArray()[0] as { head_version_id: string | null } | undefined;
  const headId = headRow?.head_version_id ?? null;

  const exceptSet = new Set(policy.exceptVersions ?? []);
  const keepLast = policy.keepLast ?? 0;
  const cutoff = policy.olderThan ?? 0;

  // Build the "keep" set: head + exceptVersions + newest keepLast.
  const keepSet = new Set<string>();
  if (headId) keepSet.add(headId);
  for (const id of exceptSet) keepSet.add(id);
  // Versions are newest-first in `all`; the first keepLast are kept.
  for (let i = 0; i < Math.min(keepLast, all.length); i++) {
    keepSet.add(all[i].versionId);
  }

  const drop: string[] = [];
  for (const v of all) {
    if (keepSet.has(v.versionId)) continue;
    if (cutoff > 0 && v.mtimeMs >= cutoff) continue;
    // If neither olderThan nor keepLast was specified, default
    // semantics are: drop everything not in exceptVersions and not
    // the head. (Caller-explicit "drop all but X" pattern.)
    drop.push(v.versionId);
  }

  const reaped = await dropVersionRows(
    durableObject,
    scope,
    userId,
    pathId,
    drop
  );
  insertAuditLog(durableObject, {
    op: "dropVersions",
    actor: userId,
    target: pathId,
    payload: JSON.stringify({
      dropped: reaped,
      kept: all.length - reaped,
      policy,
    }),
  });
  return { dropped: reaped, kept: all.length - reaped };
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
  const parentRow = durableObject.sql
    .exec("SELECT parent_id FROM files WHERE file_id = ?", pathId)
    .toArray()[0] as { parent_id: string | null } | undefined;
  const parentId = parentRow ? parentRow.parent_id : null;

  if (src.inlineData) {
    // Inline restore: no shard work; just insert + flip head.
    commitVersion(durableObject, {
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
      inlineData: new Uint8Array(src.inlineData),
      // restore preserves the source version's encryption mode.
      encryption: src.encryption,
    });
    bumpFolderRevision(durableObject, userId, parentId);
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
  // Audit C2 + P1-1: each shard's contribution restored atomically
  // per chunk via `restoreChunkRef`. A swept chunk surfaces as
  // `ENOENT` thrown from the RPC; we map to VFSError("ENOENT", ...)
  // matching the prior contract. Partial-failure cleanup: if shard
  // A succeeds but shard B throws, ref_count for A's chunks has
  // already been bumped under `newRefId`. Those refs are reachable
  // via the file_versions row we DON'T insert below (commitVersion
  // is gated on the loop completing). To make the partial state
  // observable for cleanup we'd need a compensating delete — for
  // v1, the alarm sweeper picks up these refs naturally on the
  // next tick because file_versions has no row for newVersionId
  // (so `dropVersionRows` doesn't see them) and the shard's
  // chunk_refs row will outlive the crash. Acceptable: the
  // refs are user_id-scoped + content-addressed so cross-tenant
  // exposure is impossible. A future fix can layer a UserDO-side
  // staging-row that the alarm reaps. Documented; tracked.
  await Promise.all(
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

  // Mirror version_chunks rows AFTER the atomic shard-side ref
  // bumps. If any shard throws above, this block is skipped and
  // the partial-state shard refs are flagged in the comment above.
  for (const c of chunks) {
    durableObject.sql.exec(
      `INSERT OR REPLACE INTO version_chunks
         (version_id, chunk_index, chunk_hash, chunk_size, shard_index)
       VALUES (?, ?, ?, ?, ?)`,
      newVersionId,
      c.chunk_index,
      c.chunk_hash,
      c.chunk_size,
      c.shard_index
    );
  }

  commitVersion(durableObject, {
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
    // restore preserves the source version's encryption mode.
    encryption: src.encryption,
  });
  bumpFolderRevision(durableObject, userId, parentId);
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
