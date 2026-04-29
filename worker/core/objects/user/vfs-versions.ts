import type { UserDOCore as UserDO } from "./user-do-core";
import type { ShardDO } from "../shard/shard-do";
import { VFSError, type VFSScope } from "../../../../shared/vfs-types";
import { vfsShardDOName } from "../../lib/utils";
import { generateId } from "../../lib/utils";
import { resolvePath } from "./path-walk";
import { placeChunk } from "../../../../shared/placement";

/**
 * Audit H4 — placement function for the Phase 9 versioning path.
 *
 * The Phase 9 invariant — "two writes of the same content share the
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
  userId: string,
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
  return placeChunk(userId, hash, 0, poolSize);
}

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
  /** Phase 12: optional human-readable label. */
  label?: string | null;
  /**
   * Phase 12: true iff this version was created by an explicit
   * user-facing op (writeFile, restoreVersion, flush()). False for
   * Yjs opportunistic compactions and pre-Phase-12 rows.
   */
  userVisible?: boolean;
  /** Phase 12: snapshot of metadata at this version (when requested). */
  metadata?: Record<string, unknown> | null;
  /**
   * Phase 15: per-version encryption stamp. Undefined for plaintext
   * (default for pre-Phase-15 rows and explicit plaintext writes).
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
     * Phase 12: per-version flags. All optional; defaults preserve
     * Phase-9 behavior (NULL label, user_visible=0, NULL metadata).
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
     * Phase 15: per-version encryption stamp. Mirrors the column on
     * `files`. When set, the columns on both `file_versions` (this
     * row) and `files` (the head row) are updated. The `data` payload
     * has already been written; this is metadata only.
     */
    encryption?: { mode: "convergent" | "random"; keyId?: string };
  }
): void {
  const encMode = args.encryption?.mode ?? null;
  const encKeyId = args.encryption?.keyId ?? null;
  durableObject.sql.exec(
    `INSERT INTO file_versions
       (path_id, version_id, user_id, size, mode, mtime_ms, deleted,
        inline_data, chunk_size, chunk_count, file_hash, mime_type,
        user_visible, label, metadata, encryption_mode, encryption_key_id)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
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
    encKeyId
  );
  // Update head pointer to the new version. Tombstones also become
  // the head — readers find them by mtime_ms and then check deleted.
  // Phase 15: also stamp `files.encryption_mode` + `files.encryption_key_id`
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
   * Phase 15: per-version encryption stamp. NULL for plaintext (default
   * for pre-Phase-15 rows and for plaintext writes). When set, the SDK
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
    /** Phase 12: filter to versions with `user_visible = 1`. */
    userVisibleOnly?: boolean;
    /** Phase 12: include the metadata snapshot per row. */
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
    // Phase 15: surface per-version encryption stamp.
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
 * Phase 12: set per-version flags. Idempotent. Throws EINVAL on
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
  const shardNs = env.MOSSAIC_SHARD as unknown as DurableObjectNamespace<ShardDO>;

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
      // Phase 15: restore preserves the source version's encryption mode.
      encryption: src.encryption,
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

  // Audit C2: pre-flight liveness check on every shard the source
  // manifest touches. If ANY chunk has been swept (alarm GC'd it
  // because its last reference dropped to 0 and the grace window
  // elapsed), we MUST refuse the restore and surface ENOENT — the
  // alternative (proceed and let putChunk's cold-path INSERT silently
  // store an empty buffer under the original hash because we passed
  // `new Uint8Array(0)`) would corrupt every future read of the
  // restored version. Bytes return zero, manifests look intact, and
  // the corruption is silent.
  //
  // We group the source's chunk hashes by shard, then issue one
  // chunksAlive RPC per shard in parallel. The expected steady-state
  // is "every chunk is alive" — versioning's whole point is that
  // dropVersionRows hard-deletes the file_versions/version_chunks
  // rows in the same transaction as the deleteChunks RPC, so a
  // version_chunks row pointing at a swept chunk is a concurrency
  // bug or a partial-replay scenario.
  const byShard = new Map<number, string[]>();
  for (const c of chunks) {
    const arr = byShard.get(c.shard_index) ?? [];
    arr.push(c.chunk_hash);
    byShard.set(c.shard_index, arr);
  }
  await Promise.all(
    Array.from(byShard.entries()).map(async ([shardIndex, hashes]) => {
      const shardName = vfsShardDOName(
        scope.ns,
        scope.tenant,
        scope.sub,
        shardIndex
      );
      const stub = shardNs.get(shardNs.idFromName(shardName));
      const { alive } = await stub.chunksAlive(hashes);
      if (alive.length !== hashes.length) {
        const aliveSet = new Set(alive);
        const missing = hashes.filter((h) => !aliveSet.has(h));
        throw new VFSError(
          "ENOENT",
          `restoreVersion: source chunks swept on shard ${shardIndex}: ${missing.slice(0, 3).join(",")}${missing.length > 3 ? "..." : ""}`
        );
      }
    })
  );

  for (const c of chunks) {
    const shardName = vfsShardDOName(
      scope.ns,
      scope.tenant,
      scope.sub,
      c.shard_index
    );
    const stub = shardNs.get(shardNs.idFromName(shardName));
    // The chunksAlive pre-flight above guarantees the chunk row is
    // present and live on this shard, so putChunk's existence check
    // (`SELECT hash FROM chunks WHERE hash = ?`) hits the dedup
    // branch unconditionally and the empty `new Uint8Array(0)` is
    // discarded. The cold-path INSERT in writeChunkInternal is
    // unreachable here. If a race elapses between the pre-flight
    // and putChunk (e.g. another concurrent dropVersions hits the
    // alarm-grace boundary), the alarm sweeper's resurrection-aware
    // logic in shard-do.ts:355-362 un-marks any chunk that gained a
    // new ref — so even the worst-case race upgrades a swept chunk
    // back to alive without inserting empty bytes.
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
    // Phase 15: restore preserves the source version's encryption mode.
    encryption: src.encryption,
  });
  return { versionId: newVersionId };
}
