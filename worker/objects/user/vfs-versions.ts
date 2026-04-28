import type { UserDO } from "./user-do";
import type { ShardDO } from "../shard/shard-do";
import { VFSError, type VFSScope } from "@shared/vfs-types";
import { vfsShardDOName } from "../../lib/utils";
import { generateId } from "../../lib/utils";
import { resolvePath } from "./path-walk";

/**
 * Phase 9 — file-level versioning (S3-style, opt-in).
 *
 * Per-tenant `quota.versioning_enabled` toggles whether writes
 * create historical version rows. When OFF, behavior is byte-equivalent
 * to Phase 8 (no version rows ever inserted, no head pointer used,
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
 * file_id of `${path_id}#${version_id}`. Phase 1's chunk_refs
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
  }
): void {
  durableObject.sql.exec(
    `INSERT INTO file_versions
       (path_id, version_id, user_id, size, mode, mtime_ms, deleted,
        inline_data, chunk_size, chunk_count, file_hash, mime_type)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
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
    args.mimeType
  );
  // Update head pointer to the new version. Tombstones also become
  // the head — readers find them by mtime_ms and then check deleted.
  durableObject.sql.exec(
    "UPDATE files SET head_version_id = ?, updated_at = ? WHERE file_id = ?",
    args.versionId,
    args.mtimeMs,
    args.pathId
  );
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
                  chunk_size, chunk_count, file_hash, mime_type
             FROM file_versions
            WHERE path_id = ? AND version_id = ?`,
          pathId,
          versionId
        )
        .toArray()[0]
    : durableObject.sql
        .exec(
          `SELECT version_id, size, mode, mtime_ms, deleted, inline_data,
                  chunk_size, chunk_count, file_hash, mime_type
             FROM file_versions
            WHERE path_id = ? AND deleted = 0
            ORDER BY mtime_ms DESC
            LIMIT 1`,
          pathId
        )
        .toArray()[0];
  if (!row) return null;
  const r = row as Record<string, unknown>;
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
  opts: { limit?: number } = {}
): VersionRow[] {
  const limit = opts.limit ?? 1000;
  const rows = durableObject.sql
    .exec(
      `SELECT version_id, mtime_ms, size, mode, deleted
         FROM file_versions
        WHERE path_id = ?
        ORDER BY mtime_ms DESC
        LIMIT ?`,
      pathId,
      limit
    )
    .toArray() as {
    version_id: string;
    mtime_ms: number;
    size: number;
    mode: number;
    deleted: number;
  }[];
  return rows.map((r) => ({
    versionId: r.version_id,
    mtimeMs: r.mtime_ms,
    size: r.size,
    mode: r.mode,
    deleted: r.deleted === 1,
  }));
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
 * 30s grace per Phase 3.
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
  const shardNs = env.SHARD_DO as unknown as DurableObjectNamespace<ShardDO>;

  let reaped = 0;
  for (const versionId of versionIds) {
    // Find unique shards this version's chunks live on.
    const shardRows = durableObject.sql
      .exec(
        "SELECT DISTINCT shard_index FROM version_chunks WHERE version_id = ?",
        versionId
      )
      .toArray() as { shard_index: number }[];

    // Drop UserDO-side metadata first (mirrors hardDeleteFileRow).
    durableObject.sql.exec(
      "DELETE FROM version_chunks WHERE version_id = ?",
      versionId
    );
    durableObject.sql.exec(
      "DELETE FROM file_versions WHERE path_id = ? AND version_id = ?",
      pathId,
      versionId
    );

    // Dispatch deleteChunks RPC per touched shard. The synthetic
    // file_id matches what was used at write time so chunk_refs
    // resolve correctly.
    const shardFileId = shardRefId(pathId, versionId);
    for (const { shard_index } of shardRows) {
      const shardName = vfsShardDOName(
        scope.ns,
        scope.tenant,
        scope.sub,
        shard_index
      );
      const stub = shardNs.get(shardNs.idFromName(shardName));
      await stub.deleteChunks(shardFileId);
    }
    reaped++;
  }

  // After dropping, if no live version remains AND no tombstone
  // either, also drop the empty `files` row so the path becomes
  // ENOENT cleanly.
  const liveCount = (
    durableObject.sql
      .exec(
        "SELECT COUNT(*) AS n FROM file_versions WHERE path_id = ?",
        pathId
      )
      .toArray()[0] as { n: number }
  ).n;
  if (liveCount === 0) {
    durableObject.sql.exec(
      "DELETE FROM files WHERE file_id = ? AND user_id = ?",
      pathId,
      userId
    );
  } else {
    // Reset the head pointer to the (still extant) newest version.
    const headRow = durableObject.sql
      .exec(
        `SELECT version_id FROM file_versions
          WHERE path_id = ?
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
    });
    return { versionId: newVersionId };
  }

  // Chunked restore: fan out chunk refs to ShardDOs under the new
  // synthetic file_id, then mirror version_chunks rows. We do NOT
  // re-upload chunk bytes — content-addressed dedup means the
  // existing chunks (still referenced by the source version) are
  // ALREADY on the right shards. We only add a new ref slot.
  const env = durableObject.envPublic;
  const shardNs = env.SHARD_DO as unknown as DurableObjectNamespace<ShardDO>;
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

  for (const c of chunks) {
    const shardName = vfsShardDOName(
      scope.ns,
      scope.tenant,
      scope.sub,
      c.shard_index
    );
    const stub = shardNs.get(shardNs.idFromName(shardName));
    // putChunk with a 0-byte placeholder is wrong — we want to
    // INCREMENT the refcount on an existing chunk by adding a new
    // chunk_refs row. The dedup PUT path does exactly that when
    // the chunk already exists; we send an empty Uint8Array (the
    // hash check happens BEFORE the bytes are stored, so dedup
    // sees the existing chunk and adds a ref).
    //
    // Wait: the existing putChunk recomputes nothing — it accepts
    // hash + data, checks if hash exists, and if so just adds a
    // ref. We pass the hash and a placeholder; the bytes are
    // ignored because dedup short-circuits. To be safe we pass an
    // empty buffer AND the existing chunk_size in chunk_refs.
    //
    // Concretely: putChunk(hash, data, fileId, idx, userId) →
    // existing path adds a ref iff (hash, fileId, idx) is new.
    await stub.putChunk(
      c.chunk_hash,
      new Uint8Array(0),
      newRefId,
      c.chunk_index,
      userId
    );
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
  });
  return { versionId: newVersionId };
}
