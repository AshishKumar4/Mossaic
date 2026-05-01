import type { UserDOCore as UserDO } from "../user-do-core";
import {
  VFSError,
  type ResolveResult,
  type VFSScope,
  type VFSStatRaw,
} from "../../../../../shared/vfs-types";
import { gidFromTenant, inoFromId, uidFromTenant } from "../../../../../shared/ino";
import { normalizePath, VFSPathError } from "../../../../../shared/vfs-paths";
import { resolvePath, resolvePathFollow } from "../path-walk";

/**
 * VFS foundation tier — helpers shared by every other split module.
 *
 * Seven helpers concentrated here so each per-concern module
 * (reads / write-commit / mutations / metadata / streams) imports
 * only from this file plus existing leaf siblings (path-walk,
 * vfs-versions, metadata-tags, encryption-stamp). Keeps the split
 * cycle-free.
 *
 * All functions are pure SQL on the UserDO's storage. No cross-DO
 * subrequests — those are deferred to the per-concern modules.
 */

/**
 * Resolve the SQL `user_id` for a given scope.
 *
 * The scope identifies the DO instance via vfsUserDOName, but we
 * *also* use a derived `user_id` for SQL filtering inside the DO so
 * a single DO instance can host multiple sub-tenants without leakage
 * if the binding layer ever consolidates them. The composed form
 * `${tenant}::${sub}` and the same-tenant-no-sub form `${tenant}`
 * are intentionally distinct.
 *
 * Each component is validated against the same character class as
 * vfsUserDOName / vfsShardDOName: `[A-Za-z0-9._-]{1,128}`. This makes
 * the "::" separator unambiguous (no component can contain ":"). Even
 * if vfs-ops were called directly bypassing the DO-name layer, the
 * SQL user_id space remains injection-free.
 */
const VFS_SCOPE_TOKEN = /^[A-Za-z0-9._-]{1,128}$/;

export function userIdFor(scope: VFSScope): string {
  if (!scope || typeof scope.tenant !== "string" || scope.tenant.length === 0) {
    throw new VFSError("EINVAL", "scope.tenant is required");
  }
  if (!VFS_SCOPE_TOKEN.test(scope.tenant)) {
    throw new VFSError("EINVAL", `scope.tenant invalid: ${JSON.stringify(scope.tenant)}`);
  }
  if (scope.sub !== undefined) {
    if (typeof scope.sub !== "string" || !VFS_SCOPE_TOKEN.test(scope.sub)) {
      throw new VFSError("EINVAL", `scope.sub invalid: ${JSON.stringify(scope.sub)}`);
    }
    return `${scope.tenant}::${scope.sub}`;
  }
  return scope.tenant;
}

/**
 * Narrowed `ResolveResult` for hits only — file/dir/symlink. The
 * "miss" variants (ENOENT/ENOTDIR/ELOOP) are converted to thrown
 * `VFSError`s by `resolveOrThrow`, so callers can rely on `leafId`
 * being present on the returned value.
 */
export type ResolvedHit = Extract<
  ResolveResult,
  { kind: "file" | "dir" | "symlink" }
>;

/**
 * Wrap synchronous resolution and convert path-error / ELOOP / ENOTDIR
 * into thrown VFSErrors. Returns ResolveResult ONLY for hits (file/dir/symlink);
 * misses throw. Return type is narrowed so callers get `leafId` without
 * having to re-discriminate on `kind`.
 */
export function resolveOrThrow(
  durableObject: UserDO,
  userId: string,
  path: string,
  follow: boolean
): ResolvedHit {
  let r: ResolveResult;
  try {
    r = follow
      ? resolvePathFollow(durableObject, userId, path)
      : resolvePath(durableObject, userId, path);
  } catch (err) {
    if (err instanceof VFSPathError) {
      throw new VFSError("EINVAL", err.message);
    }
    throw err;
  }
  if (r.kind === "ENOENT") {
    throw new VFSError("ENOENT", `no such file or directory: ${path}`);
  }
  if (r.kind === "ENOTDIR") {
    throw new VFSError("ENOTDIR", `not a directory: ${path}`);
  }
  if (r.kind === "ELOOP") {
    throw new VFSError("ELOOP", `too many symlinks: ${path}`);
  }
  return r;
}

/**
 * Fetch the row for an already-resolved leaf and turn it into a stat
 * object. Splits files vs folders by source table.
 */
export function statForResolved(
  durableObject: UserDO,
  userId: string,
  scope: VFSScope,
  r: ResolveResult & { leafId: string; kind: "file" | "dir" | "symlink" }
): VFSStatRaw {
  const uid = uidFromTenant(scope.tenant);
  const gid = gidFromTenant(scope.tenant);

  if (r.kind === "dir") {
    if (r.leafId === "") {
      // Synthetic root.
      return {
        type: "dir",
        mode: 0o755,
        size: 0,
        mtimeMs: 0,
        uid,
        gid,
        ino: inoFromId(`${userId}:/`),
      };
    }
    const row = durableObject.sql
      .exec(
        `SELECT folder_id, mode, updated_at FROM folders
          WHERE folder_id=? AND user_id=?`,
        r.leafId,
        userId
      )
      .toArray()[0] as
      | { folder_id: string; mode: number | null; updated_at: number }
      | undefined;
    if (!row) {
      throw new VFSError("ENOENT", "stat: folder vanished");
    }
    return {
      type: "dir",
      mode: row.mode ?? 0o755,
      size: 0,
      mtimeMs: row.updated_at,
      uid,
      gid,
      ino: inoFromId(row.folder_id),
    };
  }

  // file or symlink: same SQL row.
  const row = durableObject.sql
    .exec(
      `SELECT file_id, file_size, mode, mode_yjs, node_kind, symlink_target,
              inline_data, updated_at, encryption_mode, encryption_key_id
         FROM files
        WHERE file_id=? AND user_id=? AND status!='deleted'`,
      r.leafId,
      userId
    )
    .toArray()[0] as
    | {
        file_id: string;
        file_size: number;
        mode: number | null;
        mode_yjs: number;
        node_kind: string | null;
        symlink_target: string | null;
        inline_data: ArrayBuffer | null;
        updated_at: number;
        encryption_mode: string | null;
        encryption_key_id: string | null;
      }
    | undefined;
  if (!row) {
    throw new VFSError("ENOENT", "stat: file vanished");
  }
  // project encryption columns into the SDK-facing shape.
  // Defined inline so both file-return branches can call it; symlinks
  // ignore encryption (the target string is plaintext metadata).
  const projectEnc = ():
    | { mode: "convergent" | "random"; keyId?: string }
    | undefined => {
    if (
      row.encryption_mode !== "convergent" &&
      row.encryption_mode !== "random"
    )
      return undefined;
    const enc: { mode: "convergent" | "random"; keyId?: string } = {
      mode: row.encryption_mode,
    };
    if (row.encryption_key_id !== null) enc.keyId = row.encryption_key_id;
    return enc;
  };

  if (r.kind === "symlink") {
    // Symlink size = byteLength of the target string (POSIX convention).
    const targetLen = new TextEncoder().encode(row.symlink_target ?? "")
      .byteLength;
    return {
      type: "symlink",
      mode: row.mode ?? 0o777,
      size: targetLen,
      mtimeMs: row.updated_at,
      uid,
      gid,
      ino: inoFromId(row.file_id),
    };
  }
  // if a head_version_id exists, the truth-of-record is in
  // file_versions, not in `files`. We consult the head version row
  // for size/mode/mtime AND tombstone status. If the head is a
  // tombstone, the path appears ENOENT — same semantics as readFile.
  const headRow = durableObject.sql
    .exec(
      "SELECT head_version_id FROM files WHERE file_id=? AND user_id=?",
      row.file_id,
      userId
    )
    .toArray()[0] as { head_version_id: string | null } | undefined;
  if (headRow?.head_version_id) {
    const head = durableObject.sql
      .exec(
        `SELECT version_id, size, mode, mtime_ms, deleted, inline_data
           FROM file_versions WHERE path_id=? AND version_id=?`,
        row.file_id,
        headRow.head_version_id
      )
      .toArray()[0] as
      | {
          version_id: string;
          size: number;
          mode: number;
          mtime_ms: number;
          deleted: number;
          inline_data: ArrayBuffer | null;
        }
      | undefined;
    if (head) {
      if (head.deleted === 1) {
        throw new VFSError("ENOENT", "stat: head version is a tombstone");
      }
      const vsize = head.inline_data ? head.inline_data.byteLength : head.size;
      const out: VFSStatRaw = {
        type: "file",
        // surface the yjs-mode bit on stat.mode. The
        // mode_yjs flag lives on the `files` row (not the version
        // row) so it's invariant across versions of the same path.
        mode: head.mode | (row.mode_yjs === 1 ? 0o4000 : 0),
        size: vsize,
        mtimeMs: head.mtime_ms,
        uid,
        gid,
        ino: inoFromId(row.file_id),
      };
      // encryption stamp from the head row on `files`. The
      // versioned write path keeps `files.encryption_*` in sync with
      // the head version's columns (see commitVersion), so reading
      // from `files` here is correct.
      const enc = projectEnc();
      if (enc) out.encryption = enc;
      return out;
    }
  }

  // Regular file (path). If inlined, size still reflects
  // file_size (which equals inline_data byteLength by construction
  // in the write path; for legacy / non-inlined rows it's
  // the chunked total).
  const size = row.inline_data
    ? row.inline_data.byteLength
    : row.file_size;
  const out: VFSStatRaw = {
    type: "file",
    // surface the yjs-mode bit on stat.mode (0o4000).
    mode: (row.mode ?? 0o644) | (row.mode_yjs === 1 ? 0o4000 : 0),
    size,
    mtimeMs: row.updated_at,
    uid,
    gid,
    ino: inoFromId(row.file_id),
  };
  // encryption stamp.
  const enc = projectEnc();
  if (enc) out.encryption = enc;
  return out;
}

/**
 * Resolve a path to its (parentId, leafName) tuple — the location for a
 * new entry to be inserted. For a path of `/a/b/leaf`, returns
 * `(folder_id of /a/b, "leaf")`. The parent must exist and be a directory;
 * otherwise ENOENT/ENOTDIR.
 *
 * Root is special-cased: a path of `/leaf` returns `(null, "leaf")`.
 */
export function resolveParent(
  durableObject: UserDO,
  userId: string,
  path: string
): { parentId: string | null; leaf: string } {
  let segs: string[];
  try {
    segs = normalizePath(path);
  } catch (err) {
    if (err instanceof VFSPathError) {
      throw new VFSError("EINVAL", err.message);
    }
    throw err;
  }
  if (segs.length === 0) {
    throw new VFSError("EINVAL", "cannot operate on root path");
  }
  const leaf = segs[segs.length - 1];
  if (segs.length === 1) {
    return { parentId: null, leaf };
  }
  const parentPath = "/" + segs.slice(0, -1).join("/");
  const r = resolvePathFollow(durableObject, userId, parentPath);
  if (r.kind === "ENOENT") {
    throw new VFSError("ENOENT", `parent does not exist: ${parentPath}`);
  }
  if (r.kind === "ENOTDIR") {
    throw new VFSError("ENOTDIR", `parent is not a directory: ${parentPath}`);
  }
  if (r.kind === "ELOOP") {
    throw new VFSError("ELOOP", `too many symlinks in: ${parentPath}`);
  }
  if (r.kind !== "dir") {
    throw new VFSError(
      "ENOTDIR",
      `parent is not a directory: ${parentPath} (got ${r.kind})`
    );
  }
  return {
    parentId: r.leafId === "" ? null : r.leafId,
    leaf,
  };
}

/** Read the server-authoritative pool size from quota. Defaults to 32. */
export function poolSizeFor(durableObject: UserDO, userId: string): number {
  const row = durableObject.sql
    .exec("SELECT pool_size FROM quota WHERE user_id = ?", userId)
    .toArray()[0] as { pool_size: number } | undefined;
  return row ? row.pool_size : 32;
}

/**
 * Record bytes written / deleted against the canonical `quota` row,
 * recompute the dynamic shard pool size from the new total, and write
 * back a larger `pool_size` if `computePoolSize` says so. Pool size
 * never shrinks — rendezvous redistribution would orphan chunks
 * already pinned to higher shard indices.
 *
 * Called from every committed write/delete path:
 *  - commitInlineTier (post-commitRename)
 *  - commitChunkedTier (post-commitRename)
 *  - vfsFinalizeMultipart (post-commitRename)
 *  - hardDeleteFileRow (negative delta)
 *  - vfsWriteFileVersioned (post-commitVersion)
 *
 * The next placement decision (next write, on the SAME UserDO turn or
 * later) reads the updated pool_size via `poolSizeFor`. Existing
 * chunks stay on their original shards because reads use the
 * `file_chunks.shard_index` recorded at write time, not a live
 * recomputation. Pool growth N→N+1 reroutes ~1/(N+1) of NEW writes
 * to shard N; existing data is untouched.
 *
 * Idempotent quota-row creation: `INSERT OR IGNORE` then `UPDATE`.
 *
 * @lean-invariant Mossaic.Vfs.Quota.pool_size_monotone
 *   Lean proves `recordWriteUsage` never shrinks `pool_size`. See
 *   `lean/Mossaic/Vfs/Quota.lean :: pool_size_monotone`.
 * @lean-invariant Mossaic.Vfs.Quota.pool_growth_threshold
 *   Lean proves the post-update `pool_size` equals
 *   `max(prior, BASE_POOL + ⌊storage_used / BYTES_PER_SHARD⌋)`.
 *   See `lean/Mossaic/Vfs/Quota.lean :: pool_growth_threshold`.
 * @lean-invariant Mossaic.Vfs.Quota.pool_growth_at_5GB_boundary
 *   Lean proves crossing a 5 GB threshold grows the pool by exactly 1.
 *   See `lean/Mossaic/Vfs/Quota.lean :: pool_growth_at_5GB_boundary`.
 */
export function recordWriteUsage(
  durableObject: UserDO,
  userId: string,
  deltaBytes: number,
  deltaFiles: number,
  /**
   * Inline-tier byte accounting. Positive on inline-tier writes,
   * negative on inline-tier deletes. Defaults to 0 so legacy
   * callers (every site outside `commitInlineTier` and
   * `hardDeleteFileRow` for inline rows) keep their existing
   * behaviour. Used to gate the inline tier at `INLINE_TIER_CAP`
   * (1 GiB) per tenant — beyond which `vfsWriteFile` falls through
   * to the chunked tier even for ≤ INLINE_LIMIT inputs.
   */
  deltaInlineBytes: number = 0
): void {
  // Ensure a quota row exists. The schema is created at ensureInit
  // (`user-do-core.ts:172-179`) but a brand-new tenant's first
  // write may run before any explicit row insert.
  durableObject.sql.exec(
    `INSERT OR IGNORE INTO quota (user_id, storage_used, storage_limit, file_count, pool_size)
     VALUES (?, 0, 107374182400, 0, 32)`,
    userId
  );
  // Clamp at zero on negative deltas. `deltaInlineBytes` accepts
  // negative values from `hardDeleteFileRow` for inline-row
  // deletes. The clamp is defensive against any historical drift
  // (pool growth is monotonic by design so any over-count is
  // cosmetic and doesn't impact scaling).
  durableObject.sql.exec(
    `UPDATE quota
        SET storage_used = MAX(0, storage_used + ?),
            file_count = MAX(0, file_count + ?),
            inline_bytes_used = MAX(0, COALESCE(inline_bytes_used, 0) + ?)
      WHERE user_id = ?`,
    deltaBytes,
    deltaFiles,
    deltaInlineBytes,
    userId
  );
  // Recompute pool size from the post-update total. We import lazily
  // to keep this helper free of cross-module cycles when bundled.
  const row = durableObject.sql
    .exec(
      "SELECT storage_used, pool_size FROM quota WHERE user_id = ?",
      userId
    )
    .toArray()[0] as { storage_used: number; pool_size: number } | undefined;
  if (!row) return;
  // BASE_POOL=32, +1 per 5 GB stored. Inlined to avoid the
  // shared/placement import (the helper is on the hot write path).
  // Pool size is monotonic — Lean invariant. Negative byte deltas
  // do NOT shrink it; the `newPool > row.pool_size` guard below
  // is the load-bearing check. (Shrinking would orphan chunks
  // pinned to high shard indices via `file_chunks.shard_index`.)
  const BASE_POOL = 32;
  const BYTES_PER_SHARD = 5 * 1024 * 1024 * 1024;
  const newPool = BASE_POOL + Math.floor(Math.max(0, row.storage_used) / BYTES_PER_SHARD);
  if (newPool > row.pool_size) {
    durableObject.sql.exec(
      "UPDATE quota SET pool_size = ? WHERE user_id = ?",
      newPool,
      userId
    );
  }
}

/**
 * Find the live (non-deleted, non-uploading) file row at (parentId, leaf).
 * Used by the commit-rename phase to identify a row to supersede.
 */
export function findLiveFile(
  durableObject: UserDO,
  userId: string,
  parentId: string | null,
  leaf: string
): { file_id: string } | undefined {
  return durableObject.sql
    .exec(
      `SELECT file_id FROM files
        WHERE user_id=? AND IFNULL(parent_id,'')=IFNULL(?,'') AND file_name=?
          AND status='complete'`,
      userId,
      parentId,
      leaf
    )
    .toArray()[0] as { file_id: string } | undefined;
}

/** True iff the (parentId, name) slot is occupied by a live folder. */
export function folderExists(
  durableObject: UserDO,
  userId: string,
  parentId: string | null,
  name: string
): boolean {
  const r = durableObject.sql
    .exec(
      `SELECT folder_id FROM folders
        WHERE user_id=? AND IFNULL(parent_id,'')=IFNULL(?,'') AND name=?
        LIMIT 1`,
      userId,
      parentId,
      name
    )
    .toArray()[0] as { folder_id: string } | undefined;
  return r !== undefined;
}

/**
 * Bump the per-folder revision counter.
 *
 * Used by `vfsListChildren` consumers (Seal etc.) as a cheap ETag
 * for directory contents: when the returned revision is unchanged
 * between two reads, the contents are guaranteed identical.
 *
 * Bumped on every mutation that changes the direct-children set
 * for `folderId` OR the folder row's own (parent_id, name) slot.
 * `null` represents the synthetic root folder; we bump the
 * "user_root" sentinel row stored as a `folders` row with
 * folder_id='__root__' for that case (created lazily here so
 * legacy tenants don't need a one-shot migration).
 *
 * Mutations that bump (caller's responsibility — bumpFolderRevision
 * does NOT discover the parent itself):
 *   - vfsMkdir: bump parent of the newly-created folder.
 *   - vfsRmdir: bump the removed folder's parent.
 *   - vfsCreateFolder (legacy app path): same as vfsMkdir.
 *   - write-commit (vfsWriteFile / vfsWriteFileVersioned /
 *     stream commit / multipart finalize): bump the file's parent.
 *   - vfsUnlink / vfsPurge: bump the removed file's parent.
 *   - vfsRename: bump SRC parent + DST parent (two bumps; src
 *     parent loses, dst parent gains; both observers see the
 *     directory change).
 *   - vfsRemoveRecursive: bump every folder in the deleted tree
 *     (cheap — they're already collected for the SQL DELETE).
 *   - vfsArchive / vfsUnarchive: bump parent (visibility flips).
 *   - vfsRestoreVersion: when materialising a tombstoned head
 *     (path goes back from ENOENT to live), bump parent.
 *   - vfsCopyFile: bump dst parent (src is unchanged).
 *   - vfsSymlink: bump parent of the new symlink.
 *
 * Idempotent w.r.t. concurrent callers because the DO runs single-
 * threaded; the SQL UPDATE is atomic and `revision = revision + 1`
 * cannot lose updates inside one DO turn.
 *
 * Strict-monotonic guarantee: the returned revision is the new
 * value AFTER the bump. Two consecutive bumps produce two
 * consecutive integers; no skips, no duplicates.
 */
export function bumpFolderRevision(
  durableObject: UserDO,
  userId: string,
  folderId: string | null
): void {
  if (folderId === null) {
    // Root revision lives in `root_folder_revision` (dedicated table)
    // — NOT in `folders`. A synthetic root row inside `folders` would
    // leak into `vfsReaddir` (which scans `WHERE parent_id IS NULL`).
    durableObject.sql.exec(
      `INSERT INTO root_folder_revision (user_id, revision) VALUES (?, 1)
         ON CONFLICT(user_id) DO UPDATE SET revision = revision + 1`,
      userId
    );
    return;
  }
  durableObject.sql.exec(
    "UPDATE folders SET revision = revision + 1, updated_at = ? WHERE folder_id = ? AND user_id = ?",
    Date.now(),
    folderId,
    userId
  );
}

/**
 * Read the current revision counter for a folder.
 * Returns 0 for the root sentinel before its first bump (the row
 * may not yet exist) and for any folder_id that doesn't resolve.
 *
 * The `__root__` synthetic row pattern matches `bumpFolderRevision`.
 */
export function readFolderRevision(
  durableObject: UserDO,
  userId: string,
  folderId: string | null
): number {
  if (folderId === null) {
    const r = durableObject.sql
      .exec(
        "SELECT revision FROM root_folder_revision WHERE user_id = ?",
        userId
      )
      .toArray()[0] as { revision: number } | undefined;
    return r?.revision ?? 0;
  }
  const r = durableObject.sql
    .exec(
      "SELECT revision FROM folders WHERE folder_id = ? AND user_id = ?",
      folderId,
      userId
    )
    .toArray()[0] as { revision: number } | undefined;
  return r?.revision ?? 0;
}
