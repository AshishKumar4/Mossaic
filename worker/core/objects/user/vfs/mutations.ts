import type { UserDOCore as UserDO } from "../user-do-core";
import {
  VFSError,
  type VFSScope,
} from "../../../../../shared/vfs-types";
import { generateId } from "../../../lib/utils";
import { normalizePath, VFSPathError } from "../../../../../shared/vfs-paths";
import {
  commitVersion,
  isVersioningEnabled,
} from "../vfs-versions";
import {
  folderExists,
  poolSizeFor,
  resolveOrThrow,
  resolveParent,
  userIdFor,
} from "./helpers";
import { isYjsMode } from "./metadata";
import { hardDeleteFileRow } from "./write-commit";

/**
 * Filesystem namespace mutations.
 *
 * `vfsUnlink`, `vfsMkdir`, `vfsRmdir`, `vfsRename`, `vfsSymlink`,
 * `vfsRemoveRecursive` all touch the namespace (create/delete/rename
 * file or folder rows) without touching content beyond the
 * commit-protocol fan-out via `hardDeleteFileRow`. They share the
 * `resolveOrThrow → hardDeleteFileRow` (or `resolveParent → INSERT`)
 * patterns.
 */

// ── unlink ─────────────────────────────────────────────────────────────

/**
 * unlink — hard-delete a regular file or symlink. Throws EISDIR for
 * directories (callers should use rmdir / removeRecursive). Plan §8.4.
 *
 * when versioning is enabled, unlink writes a TOMBSTONE
 * version (deleted=1, no chunks) instead of hard-deleting. The
 * existing version rows + their chunks remain intact; the path
 * appears ENOENT to readFile but listVersions still surfaces history.
 * dropVersions / dropVersions(allow-dangling) is the explicit way
 * to permanently reap.
 */
export async function vfsUnlink(
  durableObject: UserDO,
  scope: VFSScope,
  path: string
): Promise<void> {
  const userId = userIdFor(scope);
  const r = resolveOrThrow(durableObject, userId, path, /*follow*/ false);
  if (r.kind === "dir") {
    throw new VFSError("EISDIR", `unlink: is a directory: ${path}`);
  }
  if (r.kind !== "file" && r.kind !== "symlink") {
    throw new VFSError("EINVAL", `unlink: not a regular file: ${path}`);
  }

  // versioning fork.
  if (isVersioningEnabled(durableObject, userId)) {
    const tombId = generateId();
    const now = Date.now();
    commitVersion(durableObject, {
      pathId: r.leafId,
      versionId: tombId,
      userId,
      size: 0,
      mode: 0,
      mtimeMs: now,
      chunkSize: 0,
      chunkCount: 0,
      fileHash: "",
      mimeType: "application/octet-stream",
      inlineData: null,
      deleted: true,
    });
    return;
  }

  // yjs-mode files have their content in yjs_oplog +
  // shard chunks under refs `${pathId}#yjs#${seq}`, NOT in
  // file_chunks. We must drop those refs (so chunk_refs / refcount
  // gives the alarm sweeper a chance to free shard storage) and
  // wipe the per-path oplog/meta rows BEFORE the files row goes,
  // so we still know `r.leafId`.
  if (r.kind === "file" && isYjsMode(durableObject, userId, r.leafId)) {
    const { purgeYjs } = await import("../yjs");
    await purgeYjs(durableObject, scope, r.leafId);
  }

  await hardDeleteFileRow(durableObject, userId, scope, r.leafId);
}

/**
 * Phase 25 — `vfs.purge(path)`.
 *
 * Permanently destroy a path and ALL its history. Three-tier
 * delete model the SDK exposes:
 *
 *   - `vfs.unlink(path)`  — POSIX-style. Versioning-on: writes a
 *     tombstone version, leaves history. Versioning-off: hard
 *     deletes the file row.
 *   - `vfs.purge(path)`   — destructive cleanup. Drops every version
 *     row + the files row + decrements ShardDO chunk refs for all
 *     versions' chunks. Independent of versioning state. Acts like
 *     a versioning-off `unlink` even when versioning is on.
 *   - `archive(path)`     — RESERVED for Phase 25.1; will hide a
 *     path from listings without destroying data. NOT implemented
 *     yet — TODO doc-only.
 *
 * Use `vfs.purge` for compliance-style "right to be forgotten" or
 * to clean up a specific tombstoned-head path. For sweeping a
 * tenant-wide tombstone backlog, use `adminReapTombstonedHeads`
 * (admin-tombstones.ts) which scales by SQL scan rather than
 * per-path SDK calls.
 *
 * Does NOT throw if the path doesn't exist (idempotent — the user's
 * intent "this path should not exist" is satisfied either way).
 * Throws EISDIR / EINVAL like `unlink`.
 */
export async function vfsPurge(
  durableObject: UserDO,
  scope: VFSScope,
  path: string
): Promise<void> {
  const userId = userIdFor(scope);
  // resolveOrThrow throws ENOENT — purge is idempotent so we
  // resolve manually and short-circuit on miss.
  const { resolvePath } = await import("../path-walk");
  const r = resolvePath(durableObject, userId, path);
  if (r.kind === "ENOENT") return;
  if (r.kind === "dir") {
    throw new VFSError("EISDIR", `purge: is a directory: ${path}`);
  }
  if (r.kind !== "file" && r.kind !== "symlink") {
    throw new VFSError("EINVAL", `purge: not a regular file: ${path}`);
  }

  // If versioning is on AND there are version rows, dropVersionRows
  // does the per-version chunk fanout. After that the files row
  // either auto-drops (when no versions remain — see
  // vfs-versions.ts:626-631) or is dropped by hardDeleteFileRow as
  // a belt-and-suspenders.
  const fileId = r.leafId;
  const versionRows = durableObject.sql
    .exec(
      "SELECT version_id FROM file_versions WHERE path_id = ?",
      fileId
    )
    .toArray() as { version_id: string }[];

  if (versionRows.length > 0) {
    const { dropVersionRows } = await import("../vfs-versions");
    await dropVersionRows(
      durableObject,
      scope,
      userId,
      fileId,
      versionRows.map((v) => v.version_id)
    );
  }

  // Yjs-mode rows have their bytes outside file_versions; same
  // path as vfsUnlink's non-versioning branch.
  if (r.kind === "file" && isYjsMode(durableObject, userId, fileId)) {
    const { purgeYjs } = await import("../yjs");
    await purgeYjs(durableObject, scope, fileId);
  }

  // Drop the files row + decrement file_chunks-driven refs (covers
  // non-versioning rows AND any orphan chunk_refs that survived
  // dropVersionRows). hardDeleteFileRow is itself idempotent w.r.t.
  // an already-deleted row.
  await hardDeleteFileRow(durableObject, userId, scope, fileId);
}

// ── mkdir / rmdir ──────────────────────────────────────────────────────

/**
 * mkdir — create a folder at `path`. EEXIST if anything occupies the
 * slot. With `recursive: true`, walks the path and creates missing
 * intermediates; idempotent on existing dirs.
 */
export function vfsMkdir(
  durableObject: UserDO,
  scope: VFSScope,
  path: string,
  opts: { recursive?: boolean; mode?: number } = {}
): void {
  const userId = userIdFor(scope);
  const mode = opts.mode ?? 0o755;
  const recursive = opts.recursive === true;

  let segs: string[];
  try {
    segs = normalizePath(path);
  } catch (err) {
    if (err instanceof VFSPathError)
      throw new VFSError("EINVAL", err.message);
    throw err;
  }
  if (segs.length === 0) {
    if (recursive) return; // mkdir -p / is a no-op
    throw new VFSError("EEXIST", "mkdir: root already exists");
  }

  let parentId: string | null = null;
  for (let i = 0; i < segs.length; i++) {
    const seg = segs[i];
    const isLeaf = i === segs.length - 1;
    // Check if a folder already exists at (parentId, seg).
    const existing = durableObject.sql
      .exec(
        `SELECT folder_id FROM folders
          WHERE user_id=? AND IFNULL(parent_id,'')=IFNULL(?,'') AND name=?`,
        userId,
        parentId,
        seg
      )
      .toArray()[0] as { folder_id: string } | undefined;
    if (existing) {
      if (isLeaf && !recursive) {
        throw new VFSError("EEXIST", `mkdir: already exists: ${path}`);
      }
      parentId = existing.folder_id;
      continue;
    }
    // No folder — but maybe a file occupies the name?
    const fileRow = durableObject.sql
      .exec(
        `SELECT file_id FROM files
          WHERE user_id=? AND IFNULL(parent_id,'')=IFNULL(?,'') AND file_name=? AND status!='deleted'`,
        userId,
        parentId,
        seg
      )
      .toArray()[0] as { file_id: string } | undefined;
    if (fileRow) {
      throw new VFSError(
        "EEXIST",
        `mkdir: a file occupies the path component: ${seg}`
      );
    }
    if (!isLeaf && !recursive) {
      throw new VFSError(
        "ENOENT",
        `mkdir: parent does not exist (use recursive): ${seg}`
      );
    }
    const folderId = generateId();
    const now = Date.now();
    durableObject.sql.exec(
      `INSERT INTO folders (folder_id, user_id, parent_id, name, created_at, updated_at, mode)
       VALUES (?, ?, ?, ?, ?, ?, ?)`,
      folderId,
      userId,
      parentId,
      seg,
      now,
      now,
      mode
    );
    parentId = folderId;
  }
}

/**
 * rmdir — remove an empty directory. ENOTDIR for files, ENOTEMPTY when
 * there are children, ENOENT when missing.
 */
export function vfsRmdir(
  durableObject: UserDO,
  scope: VFSScope,
  path: string
): void {
  const userId = userIdFor(scope);
  const r = resolveOrThrow(durableObject, userId, path, /*follow*/ false);
  if (r.kind !== "dir") {
    throw new VFSError("ENOTDIR", `rmdir: not a directory: ${path}`);
  }
  if (r.leafId === "") {
    // root
    throw new VFSError("EBUSY", "rmdir: cannot remove root");
  }
  // Empty check: any folder or live file with this folder as parent_id?
  // H2: throw ENOTEMPTY (was ENOTDIR) — README + SDK promise the
  // POSIX-aligned code. ENOTEMPTY now exists in the server-side
  // VFSErrorCode union (shared/vfs-types.ts).
  const childFolder = durableObject.sql
    .exec(
      "SELECT 1 FROM folders WHERE user_id=? AND parent_id=? LIMIT 1",
      userId,
      r.leafId
    )
    .toArray();
  if (childFolder.length > 0) {
    throw new VFSError("ENOTEMPTY", `rmdir: directory not empty: ${path}`);
  }
  const childFile = durableObject.sql
    .exec(
      "SELECT 1 FROM files WHERE user_id=? AND parent_id=? AND status!='deleted' LIMIT 1",
      userId,
      r.leafId
    )
    .toArray();
  if (childFile.length > 0) {
    throw new VFSError("ENOTEMPTY", `rmdir: directory not empty: ${path}`);
  }
  durableObject.sql.exec(
    "DELETE FROM folders WHERE folder_id = ?",
    r.leafId
  );
}

// ── rename ─────────────────────────────────────────────────────────────

/**
 * rename — atomic move/rename. POSIX semantics:
 *   - If src is a regular file/symlink and dst doesn't exist: simple
 *     UPDATE (parent_id, file_name).
 *   - If dst exists and is a regular file: replace (hard-delete dst's
 *     contents + chunks).
 *   - If dst exists and is a directory: EISDIR (refuse to overwrite a
 *     dir with a file).
 *   - If src is a directory: rename the folder row. dst must not exist
 *     OR must be an empty directory — we keep it simple and only allow
 *     "doesn't exist" for now.
 *   - Same-path rename (src === dst after normalization): no-op.
 *
 * Concurrency: single DO method ⇒ atomic. The unique partial index on
 * (parent_id, file_name) WHERE status != 'deleted' is the gate.
 */
export async function vfsRename(
  durableObject: UserDO,
  scope: VFSScope,
  src: string,
  dst: string
): Promise<void> {
  const userId = userIdFor(scope);
  const srcR = resolveOrThrow(durableObject, userId, src, /*follow*/ false);
  if (srcR.kind === "dir" && srcR.leafId === "") {
    throw new VFSError("EBUSY", "rename: cannot rename root");
  }

  const { parentId: dstParent, leaf: dstLeaf } = resolveParent(
    durableObject,
    userId,
    dst
  );

  // Check if dst is the same as src (no-op).
  if (srcR.kind === "file" || srcR.kind === "symlink") {
    const srcRow = durableObject.sql
      .exec(
        "SELECT parent_id, file_name FROM files WHERE file_id=?",
        srcR.leafId
      )
      .toArray()[0] as { parent_id: string | null; file_name: string };
    if (
      (srcRow.parent_id ?? null) === dstParent &&
      srcRow.file_name === dstLeaf
    ) {
      return; // same path
    }
  } else if (srcR.kind === "dir") {
    const srcFolder = durableObject.sql
      .exec("SELECT parent_id, name FROM folders WHERE folder_id=?", srcR.leafId)
      .toArray()[0] as { parent_id: string | null; name: string };
    if (
      (srcFolder.parent_id ?? null) === dstParent &&
      srcFolder.name === dstLeaf
    ) {
      return;
    }
  }

  // Look at what's at dst.
  const dstFolder = durableObject.sql
    .exec(
      `SELECT folder_id FROM folders
        WHERE user_id=? AND IFNULL(parent_id,'')=IFNULL(?,'') AND name=?`,
      userId,
      dstParent,
      dstLeaf
    )
    .toArray()[0] as { folder_id: string } | undefined;
  const dstFile = durableObject.sql
    .exec(
      `SELECT file_id FROM files
        WHERE user_id=? AND IFNULL(parent_id,'')=IFNULL(?,'') AND file_name=? AND status!='deleted'`,
      userId,
      dstParent,
      dstLeaf
    )
    .toArray()[0] as { file_id: string } | undefined;

  if (srcR.kind === "dir") {
    if (dstFolder || dstFile) {
      throw new VFSError(
        "EEXIST",
        `rename: destination exists and src is a directory: ${dst}`
      );
    }
    const now = Date.now();
    durableObject.sql.exec(
      "UPDATE folders SET parent_id=?, name=?, updated_at=? WHERE folder_id=? AND user_id=?",
      dstParent,
      dstLeaf,
      now,
      srcR.leafId,
      userId
    );
    return;
  }

  // src is file/symlink.
  if (dstFolder) {
    throw new VFSError(
      "EISDIR",
      `rename: destination is a directory: ${dst}`
    );
  }
  if (dstFile) {
    // Phase 27 — under versioning-on, replace-overwrite must NOT
    // hard-delete the displaced file's history. The semantics
    // mirror what `unlink` does on a versioned tenant: the
    // displaced row's chunks survive, accessible via
    // `listVersions` + `restoreVersion`. We tombstone the
    // displaced path instead of hard-deleting it.
    if (isVersioningEnabled(durableObject, userId)) {
      // Tombstone the displaced row via commitVersion(deleted=true)
      // — same shape as `vfsUnlink`'s versioning fork.
      const { commitVersion } = await import("../vfs-versions");
      const now = Date.now();
      const tombId = generateId();
      commitVersion(durableObject, {
        pathId: dstFile.file_id,
        versionId: tombId,
        userId,
        size: 0,
        mode: 0,
        mtimeMs: now,
        chunkSize: 0,
        chunkCount: 0,
        fileHash: "",
        mimeType: "application/octet-stream",
        inlineData: null,
        deleted: true,
      });
      // Free the unique-index slot by renaming the displaced row.
      // The path the displaced file_id appears at no longer
      // matches `(dstParent, dstLeaf)`; subsequent listings filter
      // it via the tombstone-head consistency check (Phase 25). A
      // suffix keeps it unique per path-tombstone-event.
      durableObject.sql.exec(
        "UPDATE files SET file_name = ?, updated_at = ? WHERE file_id = ?",
        `${dstFile.file_id}.tombstoned-${now}`,
        now,
        dstFile.file_id
      );
      // Now move src into the freed slot.
      durableObject.sql.exec(
        "UPDATE files SET parent_id=?, file_name=?, updated_at=? WHERE file_id=? AND user_id=?",
        dstParent,
        dstLeaf,
        now,
        srcR.leafId,
        userId
      );
      return;
    }

    // Versioning OFF — pre-Phase-27 behaviour: hard-delete the
    // displaced row's chunks via shard fan-out.
    const now = Date.now();
    durableObject.sql.exec(
      "UPDATE files SET status='deleted', deleted_at=?, updated_at=? WHERE file_id=?",
      now,
      now,
      dstFile.file_id
    );
    try {
      durableObject.sql.exec(
        "UPDATE files SET parent_id=?, file_name=?, updated_at=? WHERE file_id=? AND user_id=?",
        dstParent,
        dstLeaf,
        now,
        srcR.leafId,
        userId
      );
    } catch (err) {
      // Rollback: revert the supersede so the original dst row stays
      // live and the unique-index slot is reoccupied. Wrap in a
      // defensive try/catch — under DO single-threading the surrounding
      // sql.exec calls are synchronous, so the index slot can't be
      // re-claimed by anyone else between supersede and rollback. The
      // try/catch hardens against future code changes that introduce
      // an `await` between those statements.
      try {
        durableObject.sql.exec(
          "UPDATE files SET status='complete', deleted_at=NULL, updated_at=? WHERE file_id=?",
          now,
          dstFile.file_id
        );
      } catch {
        // Rollback failed — accept that the dst row is permanently
        // soft-deleted. Surface the original error so the caller
        // knows the rename did not happen.
      }
      throw new VFSError(
        "EBUSY",
        `rename: replace failed: ${(err as Error).message}`
      );
    }
    // Hard-delete the displaced file's contents.
    await hardDeleteFileRow(durableObject, userId, scope, dstFile.file_id);
    return;
  }
  // dst is empty: simple UPDATE.
  const now = Date.now();
  durableObject.sql.exec(
    "UPDATE files SET parent_id=?, file_name=?, updated_at=? WHERE file_id=? AND user_id=?",
    dstParent,
    dstLeaf,
    now,
    srcR.leafId,
    userId
  );
}

// ── symlink ────────────────────────────────────────────────────────────

/**
 * symlink — create a symlink at `linkPath` pointing to `target`. The
 * target is stored verbatim — it may be relative or absolute and is
 * resolved at read time via resolvePathFollow + resolveSymlinkTarget.
 *
 * EEXIST if linkPath is already occupied.
 */
export function vfsSymlink(
  durableObject: UserDO,
  scope: VFSScope,
  target: string,
  linkPath: string
): void {
  const userId = userIdFor(scope);
  if (typeof target !== "string" || target.length === 0) {
    throw new VFSError("EINVAL", "symlink: target must be a non-empty string");
  }
  const { parentId, leaf } = resolveParent(durableObject, userId, linkPath);
  // EEXIST checks: folder or live file at the slot.
  if (folderExists(durableObject, userId, parentId, leaf)) {
    throw new VFSError("EEXIST", `symlink: ${linkPath} exists (folder)`);
  }
  const liveFile = durableObject.sql
    .exec(
      `SELECT 1 FROM files
        WHERE user_id=? AND IFNULL(parent_id,'')=IFNULL(?,'') AND file_name=? AND status!='deleted'
        LIMIT 1`,
      userId,
      parentId,
      leaf
    )
    .toArray();
  if (liveFile.length > 0) {
    throw new VFSError("EEXIST", `symlink: ${linkPath} exists (file)`);
  }
  const id = generateId();
  const now = Date.now();
  durableObject.sql.exec(
    `INSERT INTO files (file_id, user_id, parent_id, file_name, file_size, file_hash, mime_type, chunk_size, chunk_count, pool_size, status, created_at, updated_at, mode, node_kind, symlink_target)
     VALUES (?, ?, ?, ?, ?, '', 'inode/symlink', 0, 0, ?, 'complete', ?, ?, 511, 'symlink', ?)`,
    id,
    userId,
    parentId,
    leaf,
    new TextEncoder().encode(target).byteLength,
    poolSizeFor(durableObject, userId),
    now,
    now,
    target
  );
}

// ── removeRecursive ────────────────────────────────────────────────────

/**
 * removeRecursive — paginated rm -rf. Cursored across multiple
 * invocations so an enormous tree doesn't blow the per-invocation
 * subrequest budget.
 *
 * Strategy: depth-first, leaves-first. Each call drains up to
 * BATCH_LIMIT files; when a directory is empty its row is dropped. If
 * any work remains, returns a cursor (currently always undefined since
 * we walk in order; the SDK loops until done).
 *
 * The path must resolve to a directory; for a single file the caller
 * uses unlink.
 */
export async function vfsRemoveRecursive(
  durableObject: UserDO,
  scope: VFSScope,
  path: string,
  cursor?: string
): Promise<{ done: boolean; cursor?: string }> {
  const BATCH_LIMIT = 200;
  const userId = userIdFor(scope);
  const rootR = resolveOrThrow(durableObject, userId, path, /*follow*/ false);
  if (rootR.kind !== "dir") {
    throw new VFSError("ENOTDIR", `removeRecursive: not a directory: ${path}`);
  }
  if (rootR.leafId === "") {
    throw new VFSError("EBUSY", "removeRecursive: cannot remove root");
  }

  // Drain up to BATCH_LIMIT files within this subtree by gathering all
  // descendant folder ids first, then deleting files row by row.
  // Note: cursor is currently unused — we keep the parameter for
  // forward-compat and to match the SDK loop shape.
  void cursor;

  // BFS to collect descendant folder ids.
  const allFolders: string[] = [rootR.leafId];
  const queue = [rootR.leafId];
  while (queue.length > 0) {
    const cur = queue.shift()!;
    const subs = durableObject.sql
      .exec(
        "SELECT folder_id FROM folders WHERE user_id=? AND parent_id=?",
        userId,
        cur
      )
      .toArray() as { folder_id: string }[];
    for (const s of subs) {
      allFolders.push(s.folder_id);
      queue.push(s.folder_id);
    }
  }

  // Files in any descendant folder. Process up to BATCH_LIMIT per call.
  const placeholders = allFolders.map(() => "?").join(",");
  const fileRows = durableObject.sql
    .exec(
      `SELECT file_id FROM files
        WHERE user_id=? AND parent_id IN (${placeholders}) AND status!='deleted'
        LIMIT ?`,
      userId,
      ...allFolders,
      BATCH_LIMIT
    )
    .toArray() as { file_id: string }[];

  // Phase 27 Fix 5 — under versioning ON, recursive remove must
  // tombstone each file instead of hard-deleting it. The audit
  // sub-agent found that `hardDeleteFileRow` on a versioning-on
  // tenant silently destroyed prior version history AND leaked
  // ShardDO chunk_refs (file_chunks is empty for versioned writes;
  // chunks live in version_chunks under
  // `${pathId}#${versionId}` or `uploadId` — neither reachable
  // from `hardDeleteFileRow`'s `file_chunks`-keyed fan-out). Each
  // tombstoned path's history remains in `file_versions`,
  // accessible via `listVersions` + `restoreVersion`. Operators
  // who want full destruction should use `vfsPurge(path)` or
  // `adminReapTombstonedHeads`.
  const versioning = isVersioningEnabled(durableObject, userId);
  for (const f of fileRows) {
    if (versioning) {
      const tombId = generateId();
      const now = Date.now();
      commitVersion(durableObject, {
        pathId: f.file_id,
        versionId: tombId,
        userId,
        size: 0,
        mode: 0,
        mtimeMs: now,
        chunkSize: 0,
        chunkCount: 0,
        fileHash: "",
        mimeType: "application/octet-stream",
        inlineData: null,
        deleted: true,
      });
      // Free the unique-index slot — same shape as `vfsRename`'s
      // versioning-overwrite branch. The path resolution surface
      // (Phase 25) filters tombstoned-head rows out of listings.
      durableObject.sql.exec(
        "UPDATE files SET file_name = ?, updated_at = ? WHERE file_id = ?",
        `${f.file_id}.tombstoned-${now}`,
        now,
        f.file_id
      );
    } else {
      await hardDeleteFileRow(durableObject, userId, scope, f.file_id);
    }
  }

  // If the batch was full, we have more work — caller should loop.
  if (fileRows.length >= BATCH_LIMIT) {
    return { done: false, cursor: "" };
  }

  // All files drained. Now drop empty folders bottom-up.
  for (let i = allFolders.length - 1; i >= 0; i--) {
    const fid = allFolders[i];
    durableObject.sql.exec(
      "DELETE FROM folders WHERE folder_id=? AND user_id=?",
      fid,
      userId
    );
  }
  return { done: true };
}
