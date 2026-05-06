import type { UserDOCore as UserDO } from "./user-do-core";
import {
  normalizePath,
  resolveSymlinkTarget,
  VFSPathError,
} from "@shared/vfs-paths";
import { SYMLINK_MAX_HOPS, type ResolveResult } from "@shared/vfs-types";

/**
 * Path → row resolution, executed entirely inside one UserDO method
 * call. Walks each path segment via a SQL lookup against `folders`, then
 * checks `files` (which now also holds symlinks via node_kind='symlink')
 * and finally `folders` again for the leaf.
 *
 * Returned `ResolveResult.kind`:
 *   - "ENOENT": missing intermediate folder OR missing leaf
 *   - "ENOTDIR": an intermediate path component resolves to a file/symlink (POSIX semantics)
 *   - "file" / "dir" / "symlink": leaf type
 *
 * Symlinks at *intermediate* segments are NOT followed by `resolvePath`
 * itself — `resolvePathFollow()` handles that for stat-style ops. This
 * mirrors POSIX `lstat` vs `stat` semantics: `lstat` should not chase a
 * trailing symlink; `stat` should.
 *
 * The function is pure SQL — no env access, no awaits. Complexity is
 * O(D) SQL queries where D is the path depth, all in one DO invocation.
 */
export function resolvePath(
  durableObject: UserDO,
  userId: string,
  path: string
): ResolveResult {
  let segs: string[];
  try {
    segs = normalizePath(path);
  } catch (err) {
    if (err instanceof VFSPathError) {
      // Surface as ENOENT-with-context: callers convert to VFSError. We
      // could also throw EINVAL — but path normalization failures during
      // resolution are most usefully treated as "path doesn't exist".
      // Keeping the explicit shape so the user-do RPC layer can choose.
      throw err;
    }
    throw err;
  }

  // Root: special-case before any SQL.
  if (segs.length === 0) {
    return { kind: "dir", parentId: null, leafId: "" };
  }

  let parentId: string | null = null;
  for (let i = 0; i < segs.length - 1; i++) {
    const seg = segs[i];
    // Try folder first.
    const folderRow = durableObject.sql
      .exec(
        `SELECT folder_id FROM folders
          WHERE user_id=? AND IFNULL(parent_id,'')=IFNULL(?,'') AND name=?`,
        userId,
        parentId,
        seg
      )
      .toArray()[0] as { folder_id: string } | undefined;
    if (folderRow) {
      parentId = folderRow.folder_id;
      continue;
    }
    // Not a folder. Could be a file/symlink → ENOTDIR. Or missing → ENOENT.
    const fileRow = durableObject.sql
      .exec(
        `SELECT file_id FROM files
          WHERE user_id=? AND IFNULL(parent_id,'')=IFNULL(?,'') AND file_name=? AND status!='deleted'
          LIMIT 1`,
        userId,
        parentId,
        seg
      )
      .toArray()[0] as { file_id: string } | undefined;
    if (fileRow) {
      return { kind: "ENOTDIR", parentId };
    }
    return { kind: "ENOENT", parentId };
  }

  const leaf = segs[segs.length - 1];

  // Leaf as file or symlink. Use status!='deleted' to ignore tombstones.
  const fileRow = durableObject.sql
    .exec(
      `SELECT file_id, node_kind, symlink_target FROM files
        WHERE user_id=? AND IFNULL(parent_id,'')=IFNULL(?,'') AND file_name=? AND status!='deleted'
        LIMIT 1`,
      userId,
      parentId,
      leaf
    )
    .toArray()[0] as
    | {
        file_id: string;
        node_kind: "file" | "symlink" | null;
        symlink_target: string | null;
      }
    | undefined;
  if (fileRow) {
    if (fileRow.node_kind === "symlink") {
      return {
        kind: "symlink",
        parentId,
        leafId: fileRow.file_id,
        target: fileRow.symlink_target ?? "",
      };
    }
    return { kind: "file", parentId, leafId: fileRow.file_id };
  }

  const folderRow = durableObject.sql
    .exec(
      `SELECT folder_id FROM folders
        WHERE user_id=? AND IFNULL(parent_id,'')=IFNULL(?,'') AND name=?
        LIMIT 1`,
      userId,
      parentId,
      leaf
    )
    .toArray()[0] as { folder_id: string } | undefined;
  if (folderRow) {
    return { kind: "dir", parentId, leafId: folderRow.folder_id };
  }

  return { kind: "ENOENT", parentId };
}

/**
 * Like `resolvePath`, but follows trailing symlinks up to
 * SYMLINK_MAX_HOPS. Used by `vfsStat`, `vfsReadFile` (when a path points
 * at a symlink), etc. Returns "ELOOP" if the chain exceeds the cap.
 *
 * Cycle detection is bounded by the hop counter — we don't track
 * visited paths since the linear hop limit is cheaper and POSIX-faithful.
 */
export function resolvePathFollow(
  durableObject: UserDO,
  userId: string,
  path: string
): ResolveResult {
  let current = path;
  for (let hops = 0; hops < SYMLINK_MAX_HOPS; hops++) {
    const r = resolvePath(durableObject, userId, current);
    if (r.kind !== "symlink") return r;
    // Resolve the symlink target, possibly relative.
    try {
      current = resolveSymlinkTarget(current, r.target);
    } catch (err) {
      if (err instanceof VFSPathError) {
        return { kind: "ENOENT", parentId: r.parentId };
      }
      throw err;
    }
  }
  return { kind: "ELOOP", parentId: null };
}
