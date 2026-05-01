import type { UserDOCore as UserDO } from "../user-do-core";
import type { ShardDO } from "../../shard/shard-do";
import {
  VFSError,
  type VFSScope,
} from "../../../../../shared/vfs-types";
import { generateId, vfsShardDOName } from "../../../lib/utils";
import { normalizePath, VFSPathError } from "../../../../../shared/vfs-paths";
import {
  commitVersion,
  dropVersionRows,
  isVersioningEnabled,
  shardRefId,
} from "../vfs-versions";
import {
  bumpFolderRevision,
  folderExists,
  poolSizeFor,
  resolveOrThrow,
  resolveParent,
  userIdFor,
} from "./helpers";
import { insertAuditLog } from "./audit-log";
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

  // Capture parent_id BEFORE mutation so we can bump
  // the parent folder's revision. Read once; the value doesn't
  // change inside this DO turn.
  const parentRow = durableObject.sql
    .exec(
      "SELECT parent_id FROM files WHERE file_id = ? AND user_id = ?",
      r.leafId,
      userId
    )
    .toArray()[0] as { parent_id: string | null } | undefined;
  const parentId: string | null = parentRow?.parent_id ?? null;

  // versioning fork.
  if (isVersioningEnabled(durableObject, userId)) {
    // Purge yjs DO state inside the versioning fork too. Without
    // this, the early return at the bottom of this block would
    // bypass the yjs purge below — a yjs-mode file unlinked under
    // versioning ON would have its head tombstoned (so
    // list/stat/exists report ENOENT) but the yjs runtime would
    // keep serving live bytes, the op-log would keep growing, and
    // active WebSocket clients would keep emitting awareness
    // updates against a path the user has logically "deleted".
    // Tombstone semantics are consistent for byte reads via the
    // head_deleted gate; this closes the same gap for the yjs
    // runtime + persisted oplog rows.
    //
    // We purge BEFORE writing the tombstone so a race with another
    // unlink that arrives between commitVersion and purgeYjs
    // can't observe a "tombstoned but still-live yjs document"
    // state. (DO single-thread serializes this anyway; ordering
    // pinned for clarity.)
    if (r.kind === "file" && isYjsMode(durableObject, userId, r.leafId)) {
      const { purgeYjs } = await import("../yjs");
      await purgeYjs(durableObject, scope, r.leafId);
    }
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
    insertAuditLog(durableObject, {
      op: "unlink",
      actor: userId,
      target: r.leafId,
      payload: JSON.stringify({ path, versioning: true, tombId, kind: r.kind }),
    });
    // Bump parent revision so listChildren consumers see
    // the path disappear from the directory listing. Versioned
    // unlink leaves the row alive but tombstones the head; the
    // directory's user-visible contents change.
    bumpFolderRevision(durableObject, userId, parentId);
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
  insertAuditLog(durableObject, {
    op: "unlink",
    actor: userId,
    target: r.leafId,
    payload: JSON.stringify({ path, versioning: false, kind: r.kind }),
  });
  // Bump parent revision after non-versioning hard delete.
  bumpFolderRevision(durableObject, userId, parentId);
}

/**
 * `vfs.purge(path)`.
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
 *   - `archive(path)`     — cosmetic-only hide; reversible via
 *     `unarchive`. See `archive.ts`.
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
  // Capture parent_id BEFORE mutation for revision bump.
  const purgeParentRow = durableObject.sql
    .exec(
      "SELECT parent_id FROM files WHERE file_id = ? AND user_id = ?",
      fileId,
      userId
    )
    .toArray()[0] as { parent_id: string | null } | undefined;
  const purgeParentId: string | null = purgeParentRow?.parent_id ?? null;
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
  insertAuditLog(durableObject, {
    op: "purge",
    actor: userId,
    target: fileId,
    payload: JSON.stringify({
      path,
      versionsReaped: versionRows.length,
      kind: r.kind,
    }),
  });
  // Bump parent revision; purge always removes from listing.
  bumpFolderRevision(durableObject, userId, purgeParentId);
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
    // Bump the PARENT's revision (not the new folder's
    // own revision; that's 0 by default and only bumps when its own
    // children change). The parent gained a child.
    bumpFolderRevision(durableObject, userId, parentId);
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
  // Capture parent_id BEFORE delete for the revision bump.
  const rmdirParentRow = durableObject.sql
    .exec(
      "SELECT parent_id FROM folders WHERE folder_id = ? AND user_id = ?",
      r.leafId,
      userId
    )
    .toArray()[0] as { parent_id: string | null } | undefined;
  const rmdirParentId: string | null = rmdirParentRow?.parent_id ?? null;
  durableObject.sql.exec(
    "DELETE FROM folders WHERE folder_id = ?",
    r.leafId
  );
  // Parent's children just lost one entry.
  bumpFolderRevision(durableObject, userId, rmdirParentId);
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

  // Capture src parent BEFORE any mutation so we can
  // bump both src + dst parents atomically after the rename. Both
  // bumps fire iff src.parent !== dst.parent (cross-folder move);
  // for in-folder rename only one bump fires.
  let srcParent: string | null = null;
  // Check if dst is the same as src (no-op).
  if (srcR.kind === "file" || srcR.kind === "symlink") {
    const srcRow = durableObject.sql
      .exec(
        "SELECT parent_id, file_name FROM files WHERE file_id=?",
        srcR.leafId
      )
      .toArray()[0] as { parent_id: string | null; file_name: string };
    srcParent = srcRow.parent_id ?? null;
    if (
      srcParent === dstParent &&
      srcRow.file_name === dstLeaf
    ) {
      return; // same path
    }
  } else if (srcR.kind === "dir") {
    const srcFolder = durableObject.sql
      .exec("SELECT parent_id, name FROM folders WHERE folder_id=?", srcR.leafId)
      .toArray()[0] as { parent_id: string | null; name: string };
    srcParent = srcFolder.parent_id ?? null;
    if (
      srcParent === dstParent &&
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
    insertAuditLog(durableObject, {
      op: "rename",
      actor: userId,
      target: srcR.leafId,
      payload: JSON.stringify({ src, dst, kind: "dir" }),
    });
    // Bump src + dst parents (de-duplicated when same).
    bumpRenameParents(durableObject, userId, srcParent, dstParent);
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
    // Rename overwrite under versioning ON PRESERVES the
    // destination path's history.
    //
    // The naive approach would tombstone the displaced file_id via
    // commitVersion(deleted=true) AND rename to
    // `<file_id>.tombstoned-<ts>` to free the unique index slot,
    // then move the src row into that slot. But path A's history
    // (rows under file_id `fA`) would become orphaned:
    //   - listVersions(A) resolves to the NEW occupant (formerly
    //     `fB`), returning its history not A's.
    //   - The synthetic name `<fA>.tombstoned-<ts>` is filtered
    //     from listFiles by the tombstone-head guard.
    //   - No public API takes a file_id, so fA's a1/a2/etc.
    //     versions become unreachable except via the
    //     `adminReapTombstonedHeads({mode:"walkBack"})` recovery
    //     primitive — operator-only.
    //
    // Correct approach: instead of moving the src row INTO A's
    // slot, IMPORT src's content as a NEW VERSION on A's history.
    // After this:
    //   - A's path_id (= fA) stays put. Path A still resolves to
    //     fA, preserving the row's archival/tag/encryption stamps.
    //   - fA's history grows: [a1, a2, …, b_imported_as_head].
    //     listVersions(A) returns the full chain.
    //   - readFile(A) returns the imported content (the new head).
    //   - Source row fB is reaped via dropVersionRows so its
    //     ShardDO chunk_refs decrement and its files-row drops.
    //     Source path B becomes ENOENT.
    //
    // S3-style + POSIX-rename merged: the destination path keeps
    // its identity AND grows its history; the source path
    // ceases to exist (matches POSIX rename's "src no longer
    // visible at src"). Inline + chunked tiers handled
    // structurally identical to copy-file.ts:copyVersioned.
    if (isVersioningEnabled(durableObject, userId)) {
      await renameOverwriteVersioned(
        durableObject,
        scope,
        userId,
        srcR.leafId,
        dstFile.file_id
      );
      // After renameOverwriteVersioned: fA's `files` row stays
      // put (unchanged parent_id, file_name, file_id), with a
      // fresh head version pointing at the migrated content.
      // fB is gone. Bump src + dst parents so directory listings
      // refresh; src parent in particular bumps because fB's row
      // disappeared from it.
      insertAuditLog(durableObject, {
        op: "rename",
        actor: userId,
        target: dstFile.file_id, // the path_id that now owns the content
        payload: JSON.stringify({
          src,
          dst,
          srcFileId: srcR.leafId, // reaped
          dstFileId: dstFile.file_id, // preserved
          versioning: true,
          historyPreserved: true,
        }),
      });
      bumpRenameParents(durableObject, userId, srcParent, dstParent);
      return;
    }

    // Versioning OFF: hard-delete the displaced row's chunks via
    // shard fan-out.
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
    insertAuditLog(durableObject, {
      op: "rename",
      actor: userId,
      target: srcR.leafId,
      payload: JSON.stringify({
        src,
        dst,
        replacedFileId: dstFile.file_id,
        versioning: false,
      }),
    });
    // Bump src + dst parents (de-dup'd when same).
    bumpRenameParents(durableObject, userId, srcParent, dstParent);
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
  insertAuditLog(durableObject, {
    op: "rename",
    actor: userId,
    target: srcR.leafId,
    payload: JSON.stringify({ src, dst }),
  });
  // Bump src + dst parents (de-dup'd when same — that's
  // the in-folder rename case where only one bump is needed).
  bumpRenameParents(durableObject, userId, srcParent, dstParent);
}

/**
 * Bump src + dst parent revisions for a rename, de-duplicated
 * when src and dst share the same parent (in-folder rename). Two
 * distinct parents → two strict-monotonic bumps; same parent →
 * one bump.
 *
 * Folder-root → represented as `null`; both-null also de-dups to
 * one bump on the dedicated `root_folder_revision` row.
 */
function bumpRenameParents(
  durableObject: UserDO,
  userId: string,
  srcParent: string | null,
  dstParent: string | null
): void {
  bumpFolderRevision(durableObject, userId, srcParent);
  if (srcParent !== dstParent) {
    bumpFolderRevision(durableObject, userId, dstParent);
  }
}

// ── rename overwrite under versioning ON ───────────────────────────────

/**
 * rename(srcPath → dstPath) overwriting an existing destination on
 * a versioning-ON tenant.
 *
 * Migrates the source row's HEAD content as a new version on the
 * destination's existing path_id, so dst's history is preserved:
 *
 *   Before:  /A.txt → fA = [a1, a2 (head)]
 *            /B.txt → fB = [b1 (head)]
 *
 *   After:   /A.txt → fA = [a1, a2, b_imported_as_v3 (head)]
 *            /B.txt → ENOENT (fB hard-dropped)
 *
 * Inline tier (src.inline_data IS NOT NULL): the inline bytes are
 * copied verbatim into the new file_versions row's `inline_data`
 * column. ZERO ShardDO interaction — same shape as
 * copyInline in copy-file.ts.
 *
 * Chunked tier (src head version has version_chunks rows): we
 * refbump each chunk under the destination's new
 * `${dstFileId}#${newVersionId}` shard ref, mirror the
 * version_chunks rows, then commitVersion on dstFileId. Refbump
 * happens BEFORE the source rows are dropped so a chunk shared
 * between src and dst keeps its refcount ≥ 1 across the swap.
 *
 * Source teardown: after the new version commits on dstFileId,
 * we call dropVersionRows on EVERY version under fB (the source
 * file_id). dropVersionRows handles ShardDO refcount fan-out per
 * version, including multipart-finalized versions whose
 * `shard_ref_id` was `uploadId` not `${pathId}#${versionId}`.
 *
 * The source `files` row drops automatically when
 * dropVersionRows reaps the last version (vfs-versions.ts:626-631).
 *
 * Failure modes:
 *  - Pre-flight chunksAlive check fails → throws ENOENT, no state
 *    changed (matches copy-file.ts).
 *  - putChunk refbump fails partway → version_chunks rows are
 *    reaped via best-effort DELETE; the new version_id was never
 *    committed so dst's history is untouched. Throws the original
 *    error to the caller.
 *  - dropVersionRows for source fails (rare — ShardDO transient):
 *    the new version on dstFileId is already committed, dst's
 *    history is correct. Source's `files` row + history rows are
 *    intentionally left intact for a future retry by
 *    adminReapTombstonedHeads / dropVersions / vfsPurge. We do
 *    NOT throw — the rename's user-visible contract ("dst now
 *    holds src's content") is satisfied; the leak is bounded.
 */
async function renameOverwriteVersioned(
  durableObject: UserDO,
  scope: VFSScope,
  userId: string,
  srcFileId: string,
  dstFileId: string
): Promise<void> {
  // Read src head metadata. We need the head version's content
  // and the src `files`-row metadata (mode, mime_type, etc.) for
  // commitVersion's args.
  const srcRow = durableObject.sql
    .exec(
      `SELECT file_id, file_size, file_hash, mime_type, mode,
              chunk_size, chunk_count, head_version_id, inline_data
         FROM files
        WHERE file_id = ? AND user_id = ?`,
      srcFileId,
      userId
    )
    .toArray()[0] as
    | {
        file_id: string;
        file_size: number;
        file_hash: string;
        mime_type: string;
        mode: number;
        chunk_size: number;
        chunk_count: number;
        head_version_id: string | null;
        inline_data: ArrayBuffer | null;
      }
    | undefined;
  if (!srcRow) {
    // Defensive: src vanished between resolveOrThrow and now. The
    // outer rename caller already guards via resolveOrThrow; this
    // branch is unreachable under DO single-thread but satisfies
    // the type system.
    throw new VFSError("ENOENT", `rename: source vanished: ${srcFileId}`);
  }

  // Resolve the head version row for the src. Versioning-ON
  // semantics: head_version_id IS NOT NULL and points at a
  // non-tombstone row. Versioning-OFF rows that reach this branch
  // (rare — caller guards on isVersioningEnabled) have
  // head_version_id NULL; we then synthesise one from the
  // legacy `files` columns + an in-place-promoted chunk manifest.
  let srcHead:
    | {
        versionId: string;
        size: number;
        mode: number;
        chunkSize: number;
        chunkCount: number;
        fileHash: string;
        mimeType: string;
        inlineData: ArrayBuffer | null;
      }
    | null = null;
  if (srcRow.head_version_id) {
    const v = durableObject.sql
      .exec(
        `SELECT version_id, size, mode, chunk_size, chunk_count,
                file_hash, mime_type, inline_data
           FROM file_versions
          WHERE path_id = ? AND version_id = ?`,
        srcFileId,
        srcRow.head_version_id
      )
      .toArray()[0] as
      | {
          version_id: string;
          size: number;
          mode: number;
          chunk_size: number;
          chunk_count: number;
          file_hash: string;
          mime_type: string;
          inline_data: ArrayBuffer | null;
        }
      | undefined;
    if (!v) {
      throw new VFSError(
        "ENOENT",
        `rename: src head version not found: ${srcFileId}#${srcRow.head_version_id}`
      );
    }
    srcHead = {
      versionId: v.version_id,
      size: v.size,
      mode: v.mode,
      chunkSize: v.chunk_size,
      chunkCount: v.chunk_count,
      fileHash: v.file_hash,
      mimeType: v.mime_type,
      inlineData: v.inline_data,
    };
  } else {
    // Legacy un-versioned src row on a versioning-ON tenant: rare
    // but possible (src predates versioning enablement). Use the
    // `files` columns directly. We don't synthesize a fresh head
    // version_id — we just use srcFileId as the manifest key for
    // the chunked tier (file_chunks rather than version_chunks).
    srcHead = {
      versionId: "", // sentinel — chunked branch uses file_chunks instead
      size: srcRow.file_size,
      mode: srcRow.mode,
      chunkSize: srcRow.chunk_size,
      chunkCount: srcRow.chunk_count,
      fileHash: srcRow.file_hash,
      mimeType: srcRow.mime_type,
      inlineData: srcRow.inline_data,
    };
  }

  const newVersionId = generateId();
  const now = Date.now();

  // ── Inline tier ──────────────────────────────────────────────────────
  if (srcHead.inlineData !== null) {
    commitVersion(durableObject, {
      pathId: dstFileId,
      versionId: newVersionId,
      userId,
      size: srcHead.size,
      mode: srcHead.mode,
      mtimeMs: now,
      chunkSize: 0,
      chunkCount: 0,
      fileHash: srcHead.fileHash,
      mimeType: srcHead.mimeType,
      inlineData: new Uint8Array(srcHead.inlineData),
      userVisible: true,
    });
    // Drop src history + files row (no shard fan-out — inline).
    await reapSourceVersionedRow(durableObject, scope, userId, srcFileId);
    return;
  }

  // ── Chunked tier ─────────────────────────────────────────────────────
  // Read src head version's chunks. If the src is a legacy
  // un-versioned row on a versioning-ON tenant (srcHead.versionId
  // === ""), pull from `file_chunks`; otherwise from `version_chunks`.
  const srcChunks = (
    srcHead.versionId === ""
      ? durableObject.sql.exec(
          `SELECT chunk_index, chunk_hash, chunk_size, shard_index
             FROM file_chunks WHERE file_id = ? ORDER BY chunk_index`,
          srcFileId
        )
      : durableObject.sql.exec(
          `SELECT chunk_index, chunk_hash, chunk_size, shard_index
             FROM version_chunks WHERE version_id = ? ORDER BY chunk_index`,
          srcHead.versionId
        )
  ).toArray() as {
    chunk_index: number;
    chunk_hash: string;
    chunk_size: number;
    shard_index: number;
  }[];

  if (srcChunks.length === 0) {
    // Empty file (size 0, no chunks). Just commit an empty version
    // — no shard interaction needed.
    commitVersion(durableObject, {
      pathId: dstFileId,
      versionId: newVersionId,
      userId,
      size: srcHead.size,
      mode: srcHead.mode,
      mtimeMs: now,
      chunkSize: srcHead.chunkSize,
      chunkCount: srcHead.chunkCount,
      fileHash: srcHead.fileHash,
      mimeType: srcHead.mimeType,
      inlineData: null,
      userVisible: true,
    });
    await reapSourceVersionedRow(durableObject, scope, userId, srcFileId);
    return;
  }

  // Pre-flight: verify every src chunk is alive on its shard
  // before we start mutating refs. A swept chunk would manifest
  // as a missing file post-rename; surface ENOENT pre-emptively.
  const env = durableObject.envPublic;
  const shardNs =
    env.MOSSAIC_SHARD as unknown as DurableObjectNamespace<ShardDO>;
  {
    const byShard = new Map<number, string[]>();
    for (const c of srcChunks) {
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
            `rename: source chunks swept on shard ${shardIndex}: ${missing
              .slice(0, 3)
              .join(",")}${missing.length > 3 ? "..." : ""}`
          );
        }
      })
    );
  }

  // Refbump each chunk under the destination's new shardRef and
  // mirror version_chunks rows under newVersionId. Same shape as
  // copy-file.ts:copyVersioned chunked branch.
  const newRefId = shardRefId(dstFileId, newVersionId);
  try {
    const byShard = new Map<number, typeof srcChunks>();
    for (const c of srcChunks) {
      const arr = byShard.get(c.shard_index) ?? [];
      arr.push(c);
      byShard.set(c.shard_index, arr);
    }
    await Promise.all(
      Array.from(byShard.entries()).map(async ([sIdx, chunks]) => {
        const shardName = vfsShardDOName(
          scope.ns,
          scope.tenant,
          scope.sub,
          sIdx
        );
        const stub = shardNs.get(shardNs.idFromName(shardName));
        for (const c of chunks) {
          // Empty buffer: chunk already exists on this shard
          // (chunksAlive pre-flight guarantees), so the dedup
          // branch in writeChunkInternal short-circuits and
          // INCREMENTS the refcount under newRefId. No bytes
          // travel on the wire.
          await stub.putChunk(
            c.chunk_hash,
            new Uint8Array(0),
            newRefId,
            c.chunk_index,
            userId
          );
          durableObject.sql.exec(
            `INSERT INTO version_chunks
               (version_id, chunk_index, chunk_hash, chunk_size, shard_index)
             VALUES (?, ?, ?, ?, ?)`,
            newVersionId,
            c.chunk_index,
            c.chunk_hash,
            c.chunk_size,
            c.shard_index
          );
        }
      })
    );
  } catch (err) {
    // Best-effort cleanup of any landed version_chunks rows. The
    // putChunk refbumps that succeeded leak chunk_refs under
    // newRefId — they'll be reclaimed by the shard's alarm sweep
    // when the user re-attempts and the failed shard recovers,
    // OR by adminReapOrphanRefs on the next sweep.
    durableObject.sql.exec(
      "DELETE FROM version_chunks WHERE version_id = ?",
      newVersionId
    );
    throw err;
  }

  commitVersion(durableObject, {
    pathId: dstFileId,
    versionId: newVersionId,
    userId,
    size: srcHead.size,
    mode: srcHead.mode,
    mtimeMs: now,
    chunkSize: srcHead.chunkSize,
    chunkCount: srcHead.chunkCount,
    fileHash: srcHead.fileHash,
    mimeType: srcHead.mimeType,
    inlineData: null,
    userVisible: true,
    shardRefId: newRefId,
  });
  await reapSourceVersionedRow(durableObject, scope, userId, srcFileId);
}

/**
 * Reap the source `files` row + every version it owns + decrement
 * ShardDO chunk_refs for every (shard_ref_id) the source's versions
 * accumulated. Called from `renameOverwriteVersioned` after the
 * destination's new version is committed.
 *
 * Versioning-on src: dropVersionRows over EVERY version_id under
 * srcFileId. dropVersionRows handles per-version shard fan-out and
 * drops the `files` row when no versions remain.
 *
 * Versioning-off src (legacy un-versioned row promoted onto a
 * versioning-on tenant): no version_chunks rows; the chunks live in
 * `file_chunks` and ShardDO chunk_refs are keyed by `srcFileId`
 * directly. Use `hardDeleteFileRow` which knows about that shape.
 */
async function reapSourceVersionedRow(
  durableObject: UserDO,
  scope: VFSScope,
  userId: string,
  srcFileId: string
): Promise<void> {
  const versions = durableObject.sql
    .exec(
      "SELECT version_id FROM file_versions WHERE path_id = ? AND user_id = ?",
      srcFileId,
      userId
    )
    .toArray() as { version_id: string }[];
  if (versions.length > 0) {
    await dropVersionRows(
      durableObject,
      scope,
      userId,
      srcFileId,
      versions.map((v) => v.version_id)
    );
    // dropVersionRows drops the files row once liveCount === 0.
    // Defensive: if a non-versioning legacy row ALSO had file_chunks
    // (a cross-mode edge case from a tenant that toggled versioning
    // mid-stream), reap them too.
    const stillHasFiles = durableObject.sql
      .exec(
        "SELECT 1 FROM files WHERE file_id = ? AND user_id = ?",
        srcFileId,
        userId
      )
      .toArray();
    if (stillHasFiles.length > 0) {
      await hardDeleteFileRow(durableObject, userId, scope, srcFileId);
    }
  } else {
    // No version rows — legacy un-versioned src row. Use the
    // canonical hardDeleteFileRow path which handles file_chunks
    // + ShardDO refs keyed by srcFileId.
    await hardDeleteFileRow(durableObject, userId, scope, srcFileId);
  }
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
  // Symlink creation adds a new entry to the parent's
  // child set; bump revision so listChildren observers refresh.
  bumpFolderRevision(durableObject, userId, parentId);
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
  // Subrequest budget, branched by versioning state.
  //
  // An unconditional BATCH_LIMIT = 200 plus per-file
  // `hardDeleteFileRow` fanning out up to `poolSize` subrequests
  // via `deleteChunks` (one RPC per touched shard) gives a worst
  // case 200 × 32 = 6400 subrequests per call — over the Workers
  // paid cap of 1000.
  //
  // Mitigation: the non-versioning path BATCHES the shard fan-out
  // into one `deleteManyChunks` RPC per touched shard (worst case
  // poolSize subrequests = 32) and uses BATCH_LIMIT = 30 because
  // `deleteManyChunks` over poolSize shards is the dominant cost;
  // raising would push toward the subrequest cap even with batched
  // fan-out (30 × 32 = 960 < 1000).
  //
  // The versioning branch tombstones in-place — NO ShardDO
  // fan-out, so subrequest cost is 0 per file. Capping at 30
  // would be a 6.6x slowdown on the path that doesn't need the
  // lower cap (a versioning-on rmrf of 150k files needs 750
  // invocations at 200 vs 5000 at 30). BATCH_LIMIT_VERSIONING =
  // 200 keeps SQL work (UPDATE files SET file_name=...,
  // commitVersion() tombstone insert) on a single DO turn — still
  // well below the 30s wall-clock bound.
  const userId = userIdFor(scope);
  const versioning = isVersioningEnabled(durableObject, userId);
  // Branched limits.
  const BATCH_LIMIT_VERSIONING = 200;
  const BATCH_LIMIT_NON_VERSIONING = 30;
  const BATCH_LIMIT = versioning
    ? BATCH_LIMIT_VERSIONING
    : BATCH_LIMIT_NON_VERSIONING;
  const rootR = resolveOrThrow(durableObject, userId, path, /*follow*/ false);
  if (rootR.kind !== "dir") {
    throw new VFSError("ENOTDIR", `removeRecursive: not a directory: ${path}`);
  }
  if (rootR.leafId === "") {
    throw new VFSError("EBUSY", "removeRecursive: cannot remove root");
  }

  // Capture the rmrf root's PARENT folder id so that when
  // the tree is fully drained we can bump the parent's revision
  // (the parent loses one child folder).
  const rmrfRootParentRow = durableObject.sql
    .exec(
      "SELECT parent_id FROM folders WHERE folder_id = ? AND user_id = ?",
      rootR.leafId,
      userId
    )
    .toArray()[0] as { parent_id: string | null } | undefined;
  const rmrfRootParentId: string | null = rmrfRootParentRow?.parent_id ?? null;

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
  // Also pull `parent_id` so we can bump the revision of
  // every folder that lost direct file children in this batch.
  const placeholders = allFolders.map(() => "?").join(",");
  const fileRows = durableObject.sql
    .exec(
      `SELECT file_id, parent_id FROM files
        WHERE user_id=? AND parent_id IN (${placeholders}) AND status!='deleted'
        LIMIT ?`,
      userId,
      ...allFolders,
      BATCH_LIMIT
    )
    .toArray() as { file_id: string; parent_id: string | null }[];

  // Collect distinct parent folders touched in THIS batch.
  // After the per-file mutations land below, we bump each one's
  // revision so rmrf-in-progress observers see the directory shrink
  // batch by batch.
  const touchedParents = new Set<string | null>();
  for (const f of fileRows) touchedParents.add(f.parent_id);

  // Under versioning ON, recursive remove must tombstone each
  // file instead of hard-deleting it. `hardDeleteFileRow` on a
  // versioning-on tenant would silently destroy prior version
  // history AND leak ShardDO chunk_refs (file_chunks is empty for
  // versioned writes; chunks live in version_chunks under
  // `${pathId}#${versionId}` or `uploadId` — neither reachable
  // from `hardDeleteFileRow`'s `file_chunks`-keyed fan-out). Each
  // tombstoned path's history remains in `file_versions`,
  // accessible via `listVersions` + `restoreVersion`. Operators
  // who want full destruction should use `vfsPurge(path)` or
  // `adminReapTombstonedHeads`.
  //
  // `versioning` already computed above for BATCH_LIMIT branching.
  if (versioning) {
    // Versioning branch — tombstone each file. No ShardDO
    // fan-out: chunks survive in version_chunks for restore.
    // BATCH_LIMIT is 200 here — SQL-only work, fan-out is zero,
    // cost is dominated by the per-file commitVersion + file_name
    // UPDATE (single-DO-turn).
    for (const f of fileRows) {
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
      // filters tombstoned-head rows out of listings.
      durableObject.sql.exec(
        "UPDATE files SET file_name = ?, updated_at = ? WHERE file_id = ?",
        `${f.file_id}.tombstoned-${now}`,
        now,
        f.file_id
      );
    }
  } else if (fileRows.length > 0) {
    // Batch the shard fan-out.
    //
    // 1. Read each file's accounting tuple BEFORE delete (needed
    //    for inline-bytes decrement).
    // 2. Collect (file_id, shard_index) pairs across all files in
    //    the batch.
    // 3. Drop UserDO-side metadata (file_chunks / file_tags /
    //    files) for ALL files at once via SQL IN (...) clauses.
    // 4. Group file_ids by shard_index; ONE deleteManyChunks RPC
    //    per shard. Worst case poolSize subrequests = 32.
    const fileIds = fileRows.map((r) => r.file_id);
    const placeholdersFiles = fileIds.map(() => "?").join(",");

    // Per-file accounting — quota-desync correction.
    // Read (status, file_size, inline_data) so a single negative
    // recordWriteUsage call covers storage_used, file_count, AND
    // inline_bytes_used. Without this, only inline bytes would be
    // decremented; storage_used / file_count would drift upward
    // every rmrf invocation.
    const accountingRows = durableObject.sql
      .exec(
        `SELECT file_id, status, file_size, inline_data FROM files
          WHERE file_id IN (${placeholdersFiles})`,
        ...fileIds
      )
      .toArray() as {
      file_id: string;
      status: string;
      file_size: number;
      inline_data: ArrayBuffer | null;
    }[];

    // Group (file_id, shard_index) pairs.
    const shardRows = durableObject.sql
      .exec(
        `SELECT DISTINCT shard_index, file_id FROM file_chunks
          WHERE file_id IN (${placeholdersFiles})`,
        ...fileIds
      )
      .toArray() as { shard_index: number; file_id: string }[];
    const byShard = new Map<number, string[]>();
    for (const r of shardRows) {
      const arr = byShard.get(r.shard_index) ?? [];
      arr.push(r.file_id);
      byShard.set(r.shard_index, arr);
    }

    // Drop metadata for ALL files in the batch in one transaction.
    durableObject.sql.exec(
      `DELETE FROM file_chunks WHERE file_id IN (${placeholdersFiles})`,
      ...fileIds
    );
    durableObject.sql.exec(
      `DELETE FROM file_tags WHERE path_id IN (${placeholdersFiles})`,
      ...fileIds
    );
    durableObject.sql.exec(
      `DELETE FROM files WHERE file_id IN (${placeholdersFiles})`,
      ...fileIds
    );

    // Quota-desync correction — single negative recordWriteUsage
    // covering storage_used, file_count, and inline_bytes_used for
    // the entire batch. Gate is `status !== 'uploading'` (mirrors
    // hardDeleteFileRow): tmp / multipart-abort rows were never
    // positive-counted, so they don't decrement. The rmrf SELECT
    // already filters `status != 'deleted'`, so accountingRows
    // contains only `complete` and `uploading` — the gate
    // effectively keeps complete-only here, but we use the same
    // predicate as hardDeleteFileRow for consistency / future-
    // proofing.
    let bytesDelta = 0;
    let filesDelta = 0;
    let inlineDelta = 0;
    for (const r of accountingRows) {
      if (r.status === "uploading") continue;
      bytesDelta -= r.file_size;
      filesDelta -= 1;
      if (r.inline_data) {
        inlineDelta -= r.inline_data.byteLength;
      }
    }
    if (bytesDelta !== 0 || filesDelta !== 0 || inlineDelta !== 0) {
      const { recordWriteUsage } = await import("./helpers");
      recordWriteUsage(
        durableObject,
        userId,
        bytesDelta,
        filesDelta,
        inlineDelta
      );
    }

    // Fan out: ONE deleteManyChunks per touched shard.
    const env = durableObject.envPublic;
    const shardNs = env.MOSSAIC_SHARD as unknown as DurableObjectNamespace<
      import("../../shard/shard-do").ShardDO
    >;
    const fanout: Promise<unknown>[] = [];
    for (const [sIdx, ids] of byShard) {
      const shardName = vfsShardDOName(scope.ns, scope.tenant, scope.sub, sIdx);
      const stub = shardNs.get(shardNs.idFromName(shardName));
      fanout.push(stub.deleteManyChunks(ids));
    }
    await Promise.all(fanout);
  }

  // Bump revision on every folder that lost direct
  // children in this batch. Each is a strict-monotonic +1; observers
  // see the listing shrink incrementally as rmrf progresses.
  for (const pid of touchedParents) {
    bumpFolderRevision(durableObject, userId, pid);
  }

  // If the batch was full, we have more work — caller should loop.
  if (fileRows.length >= BATCH_LIMIT) {
    insertAuditLog(durableObject, {
      op: "removeRecursive",
      actor: userId,
      target: rootR.leafId,
      payload: JSON.stringify({
        path,
        filesProcessed: fileRows.length,
        versioning,
        done: false,
      }),
    });
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
  // Final drain: bump the rmrf root's parent revision
  // because the parent just lost the entire subtree as a child.
  // Any inner folders that were drained are themselves gone now;
  // their `revision` rows die with them, which is fine — observers
  // walking the tree topologically will hit ENOENT on those paths.
  bumpFolderRevision(durableObject, userId, rmrfRootParentId);
  insertAuditLog(durableObject, {
    op: "removeRecursive",
    actor: userId,
    target: rootR.leafId,
    payload: JSON.stringify({
      path,
      filesProcessed: fileRows.length,
      foldersReaped: allFolders.length,
      versioning,
      done: true,
    }),
  });
  return { done: true };
}
