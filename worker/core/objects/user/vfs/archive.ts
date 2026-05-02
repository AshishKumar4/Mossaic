import type { UserDOCore as UserDO } from "../user-do-core";
import { VFSError, type VFSScope } from "../../../../../shared/vfs-types";
import { insertAuditLog } from "./audit-log";
import {
  bumpFolderRevision,
  resolveOrThrow,
  userIdFor,
  FILE_HEAD_JOIN,
  assertHeadNotTombstoned,
} from "./helpers";

/**
 * Refuse to mutate the archive bit on a tombstoned-head row.
 * Without this guard, `archive(path)` on a versioning-on tenant
 * where `unlink(path)` has tombstoned the head version would
 * silently flip `files.archived = 1` (the `files` row is still
 * alive — only the head version is `deleted=1`). Two consequences:
 *
 *   - the `listFiles({ includeArchived: true })` "Trash UI"
 *     surface excludes tombstoned heads, so the row becomes
 *     invisible to every listing, archived or not.
 *   - if a later `restoreVersion` / `writeFile` revives the path,
 *     the stale archive bit resurfaces — the resurrected file
 *     appears archived from day one with no user action.
 *
 * Match the `readFile` / `stat` semantics the doc-comment at the
 * top of this file claims: tombstoned-head → ENOENT. The check
 * adds a single SELECT on top of `resolveOrThrow`'s lookup; the
 * row was already in cache.
 */
function ensureNotTombstoned(
  durableObject: UserDO,
  userId: string,
  fileId: string,
  syscall: "archive" | "unarchive",
  path: string
): void {
  const row = durableObject.sql
    .exec(
      `SELECT f.head_version_id, fv.deleted AS head_deleted
         FROM files f
         ${FILE_HEAD_JOIN}
        WHERE f.file_id = ? AND f.user_id = ?`,
      fileId,
      userId
    )
    .toArray()[0] as
    | { head_version_id: string | null; head_deleted: number | null }
    | undefined;
  // Note: `archive` allows operating on a row whose `files` entry
  // exists but whose head_version is missing (un-versioned legacy
  // file pre-versioning enablement). Only error when the head row
  // EXISTS and is tombstoned. So we do an explicit check rather
  // than the generic `assertHeadNotTombstoned` which throws on
  // a missing `files` row.
  if (
    row !== undefined &&
    row.head_version_id !== null &&
    row.head_deleted === 1
  ) {
    throw new VFSError(
      "ENOENT",
      `${syscall}: head version is a tombstone for ${path}`
    );
  }
}

/**
 * `vfs.archive(path)` / `vfs.unarchive(path)`.
 *
 * The third tier of the delete API surface. Archive is a purely
 * cosmetic flag: it hides a path from the default `listFiles` /
 * `fileInfo` results so a tenant's "Trash" / "Hidden" UI can
 * implement a soft-delete + restore flow without touching version
 * rows or chunk refs.
 *
 * Contrast:
 *   - `unlink` (mutations.ts:46) — under versioning-on writes a
 *     tombstone version; the path becomes ENOENT to reads. Under
 *     versioning-off hard-deletes the row.
 *   - `purge`  (mutations.ts:122) — destructive; drops every
 *     version row + ShardDO chunk refs. Permanent.
 *   - `archive` (this file) — sets `files.archived = 1`. Reads
 *     (`stat`, `readFile`, `readPreview`, `createReadStream`,
 *     `openManifest`, `readChunk`, `listVersions`, `restoreVersion`)
 *     are UNCHANGED. Only the listing-side filters apply, gated by
 *     the `includeArchived` opt on `vfsListFiles` / `vfsFileInfo`.
 *
 * `unarchive` is the inverse and is idempotent. `archive` is also
 * idempotent — calling it twice is a no-op on the second call.
 *
 * Path resolution: `archive`/`unarchive` operate on a regular file
 * or symlink. Directories throw `EISDIR` (matching `unlink`'s
 * shape). A non-existent path throws `ENOENT`.
 *
 * The flag is stored on the `files` row (path identity is stable
 * across versioned writes — `commitVersion` does NOT mint a new
 * `files.file_id`), so archive state survives versioned overwrites
 * and `restoreVersion` cleanly: archived stays archived; unarchived
 * stays unarchived.
 */

/**
 * Mark a path as archived. Hides from default listings; does not
 * touch versions, chunks, or read surfaces.
 *
 * Idempotent — calling on an already-archived path returns without
 * error.
 *
 * @throws VFSError("ENOENT") — path does not exist.
 * @throws VFSError("EISDIR") — path is a directory.
 * @throws VFSError("EINVAL") — path is not a regular file or symlink.
 */
export function vfsArchive(
  durableObject: UserDO,
  scope: VFSScope,
  path: string
): void {
  const userId = userIdFor(scope);
  const r = resolveOrThrow(durableObject, userId, path, /*follow*/ false);
  if (r.kind === "dir") {
    throw new VFSError("EISDIR", `archive: is a directory: ${path}`);
  }
  if (r.kind !== "file" && r.kind !== "symlink") {
    throw new VFSError("EINVAL", `archive: not a regular file: ${path}`);
  }
  ensureNotTombstoned(durableObject, userId, r.leafId, "archive", path);
  durableObject.sql.exec(
    "UPDATE files SET archived = 1, updated_at = ? WHERE file_id = ? AND user_id = ?",
    Date.now(),
    r.leafId,
    userId
  );
  insertAuditLog(durableObject, {
    op: "archive",
    actor: userId,
    target: r.leafId,
    payload: JSON.stringify({ path, kind: r.kind }),
  });
  // Bump parent revision: default-listing visibility flipped from
  // "shown" to "hidden" so any listChildren consumer re-fetches.
  bumpParentRevisionForFile(durableObject, userId, r.leafId);
}

/**
 * Inverse of `vfsArchive` — clears `archived = 0`. Idempotent;
 * calling on an already-unarchived path returns without error.
 *
 * @throws VFSError("ENOENT") — path does not exist.
 * @throws VFSError("EISDIR") — path is a directory.
 * @throws VFSError("EINVAL") — path is not a regular file or symlink.
 */
export function vfsUnarchive(
  durableObject: UserDO,
  scope: VFSScope,
  path: string
): void {
  const userId = userIdFor(scope);
  const r = resolveOrThrow(durableObject, userId, path, /*follow*/ false);
  if (r.kind === "dir") {
    throw new VFSError("EISDIR", `unarchive: is a directory: ${path}`);
  }
  if (r.kind !== "file" && r.kind !== "symlink") {
    throw new VFSError("EINVAL", `unarchive: not a regular file: ${path}`);
  }
  ensureNotTombstoned(durableObject, userId, r.leafId, "unarchive", path);
  durableObject.sql.exec(
    "UPDATE files SET archived = 0, updated_at = ? WHERE file_id = ? AND user_id = ?",
    Date.now(),
    r.leafId,
    userId
  );
  insertAuditLog(durableObject, {
    op: "unarchive",
    actor: userId,
    target: r.leafId,
    payload: JSON.stringify({ path, kind: r.kind }),
  });
  // Bump parent revision: visibility flipped from "hidden" back
  // to "shown".
  bumpParentRevisionForFile(durableObject, userId, r.leafId);
}

/**
 * Read the file's parent_id and bump its folder revision.
 * Co-located with archive/unarchive so we don't pull mutations.ts
 * into archive.ts for one helper. The same SQL pattern is used by
 * vfsUnlink/vfsRename which read parent_id directly.
 */
function bumpParentRevisionForFile(
  durableObject: UserDO,
  userId: string,
  fileId: string
): void {
  const row = durableObject.sql
    .exec(
      "SELECT parent_id FROM files WHERE file_id = ? AND user_id = ?",
      fileId,
      userId
    )
    .toArray()[0] as { parent_id: string | null } | undefined;
  bumpFolderRevision(durableObject, userId, row?.parent_id ?? null);
}
