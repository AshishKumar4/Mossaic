/**
 * Cheap path → cache-key bust-token resolver.
 *
 * Routes that wrap a heavy read in `caches.default` (readPreview,
 * readChunk, openManifest) need a cache-key derived from the
 * tenant + the file's current state. The state must include
 * EVERY signal that would invalidate the cache:
 *
 *   - `fileId` (immutable) — picks out the right file row.
 *   - `headVersionId` — bumps on every commitVersion (versioning
 *     ON) or stays NULL (versioning OFF). Version-keyed cache.
 *   - `updatedAt` — covers metadata-only mutations that don't bump
 *     head_version (rename, mode change, archive flip, metadata
 *     edit). The gallery cache keys on this; we reuse for symmetry.
 *   - `encryptionMode` + `encryptionKeyId` — a key-rotation
 *     between writes invalidates rendered variants (re-encrypted
 *     bytes look different to readers).
 *
 * Returning all four fields in one SQL JOIN keeps the pre-flight
 * cost at ~1ms inside the UserDO. The route then builds the cache
 * key locally and proceeds.
 *
 * @lean-invariant Mossaic.Vfs.Cache.bust_state_changes_when_signal_changes
 * The abstract theorem says a genuinely changed returned signal changes
 * BustState. It does not prove SQL/TypeScript correspondence or write-path
 * completeness.
 */

import type { UserDOCore as UserDO } from "../user-do-core";
import {
  VFSError,
  type CacheResolveResult,
  type VFSScope,
} from "../../../../../shared/vfs-types";
import { resolvePath } from "../path-walk";
import { userIdFor } from "./helpers";

export type { CacheResolveResult };

/**
 * Resolve `path` to its cache-bust state. Throws ENOENT for
 * non-existent paths. Symlinks are followed; the returned state
 * is for the target (matches the semantics of vfsReadPreview /
 * vfsReadChunk / vfsOpenManifest, all of which follow).
 *
 * Single SQL JOIN: `files` + (LEFT JOIN) `file_versions` on the
 * head pointer. Parents are walked one row at a time by
 * `resolvePath`; that's the same cost any cached endpoint would
 * pay anyway.
 */
export function vfsResolveCacheKey(
  durableObject: UserDO,
  scope: VFSScope,
  path: string
): CacheResolveResult {
  const userId = userIdFor(scope);
  const r = resolvePath(durableObject, userId, path);
  if (r.kind === "ENOENT") {
    throw new VFSError("ENOENT", `no such file: ${path}`);
  }
  if (r.kind !== "file" && r.kind !== "symlink") {
    throw new VFSError("EISDIR", `not a regular file: ${path}`);
  }

  // For symlink: resolve target. For simplicity we follow ONLY
  // direct file targets here \u2014 the read RPCs do the same +
  // ELOOP check. The common case in cached endpoints
  // (readPreview / readChunk / openManifest) is direct files;
  // symlink-to-symlink chains are rare and the extra hop cost
  // matches what the read path itself pays.
  let leafId = r.leafId;
  if (r.kind === "symlink") {
    // Defer to resolvePathFollow shape via one more probe.
    const target = durableObject.sql
      .exec(
        "SELECT symlink_target FROM files WHERE file_id = ?",
        leafId
      )
      .toArray()[0] as { symlink_target: string | null } | undefined;
    if (target?.symlink_target) {
      const followed = resolvePath(durableObject, userId, target.symlink_target);
      if (followed.kind === "file" || followed.kind === "symlink") {
        leafId = followed.leafId;
      }
    }
  }

  const row = durableObject.sql
    .exec(
      `SELECT f.file_id,
              f.head_version_id,
              f.updated_at,
              f.encryption_mode,
              f.encryption_key_id
         FROM files f
        WHERE f.file_id = ?`,
      leafId
    )
    .toArray()[0] as
    | {
        file_id: string;
        head_version_id: string | null;
        updated_at: number;
        encryption_mode: string | null;
        encryption_key_id: string | null;
      }
    | undefined;
  if (!row) {
    throw new VFSError("ENOENT", `no such file: ${path}`);
  }

  return {
    fileId: row.file_id,
    headVersionId: row.head_version_id,
    updatedAt: row.updated_at,
    encryptionMode: row.encryption_mode,
    encryptionKeyId: row.encryption_key_id,
  };
}
