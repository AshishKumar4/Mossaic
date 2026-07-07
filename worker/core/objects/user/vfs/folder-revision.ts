import type { UserDOCore as UserDO } from "../user-do-core";
import { VFSError, type VFSScope } from "../../../../../shared/vfs-types";
import { readFolderRevision, resolveOrThrow, userIdFor } from "./helpers";

/**
 * Read the folder-revision counter for `path`.
 *
 * `/` → `root_folder_revision.revision` for this tenant.
 * `/foo` → `folders.revision` where (parent=null, name='foo').
 *
 * The counter is the folder-surface bust oracle.
 * Used by the Seal-side `CachingVFS` wrapper to key folder cache
 * entries by `(tenant, parentId, revision)`.
 */
export function vfsFolderRevision(
  durableObject: UserDO,
  scope: VFSScope,
  path: string,
): { revision: number } {
  const userId = userIdFor(scope);
  // Normalise to the folder itself, not what it points to.
  const r = resolveOrThrow(durableObject, userId, path, /*follow*/ false);
  if (r.kind !== "dir") {
    throw new VFSError("ENOTDIR", `folderRevision: not a folder: ${path}`);
  }
  const folderId = r.leafId === "" ? null : r.leafId;
  return { revision: readFolderRevision(durableObject, userId, folderId) };
}
