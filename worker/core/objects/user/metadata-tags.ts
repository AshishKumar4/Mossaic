/**
 * Phase 12: server-side metadata + tags primitives.
 *
 * These helpers run inside the UserDO single-thread, so every
 * mutation is atomic with respect to concurrent VFS RPCs on the
 * same path. They do NOT validate caps — callers should run
 * `validateMetadata` / `validateTags` from `@shared/metadata-validate`
 * BEFORE invoking these functions, so the EINVAL surfaces with the
 * correct error message before any SQL touches the row.
 */

import type { UserDOCore as UserDO } from "./user-do-core";
import { deepMerge } from "../../../../shared/metadata-merge";

/**
 * Read `files.metadata` for a path. Returns the parsed JSON object,
 * or null if the column is NULL.
 *
 * SQLite returns BLOB as ArrayBuffer over the SqlStorage API.
 */
export function readMetadata(
  durableObject: UserDO,
  pathId: string
): Record<string, unknown> | null {
  const row = durableObject.sql
    .exec("SELECT metadata FROM files WHERE file_id = ?", pathId)
    .toArray()[0] as { metadata: ArrayBuffer | null } | undefined;
  if (!row || !row.metadata) return null;
  try {
    const text = new TextDecoder().decode(new Uint8Array(row.metadata));
    return JSON.parse(text) as Record<string, unknown>;
  } catch {
    // A corrupt blob in the column is unrecoverable — surface as
    // null so callers fall back to a fresh metadata. Validation on
    // the write path prevents this from happening in the first
    // place; this branch is defensive against manual SQL repair.
    return null;
  }
}

/**
 * Read the encoded metadata blob (raw bytes) for a path. Returns
 * null if the column is NULL. Used by commitVersion to snapshot the
 * current metadata into `file_versions.metadata` without re-parsing.
 */
export function readMetadataBytes(
  durableObject: UserDO,
  pathId: string
): Uint8Array | null {
  const row = durableObject.sql
    .exec("SELECT metadata FROM files WHERE file_id = ?", pathId)
    .toArray()[0] as { metadata: ArrayBuffer | null } | undefined;
  if (!row || !row.metadata) return null;
  return new Uint8Array(row.metadata);
}

/**
 * Write the metadata blob for a path. `encoded === null` clears the
 * column. The caller must have already validated the encoded bytes
 * against METADATA_MAX_BYTES.
 */
export function writeMetadata(
  durableObject: UserDO,
  pathId: string,
  encoded: Uint8Array | null
): void {
  durableObject.sql.exec(
    "UPDATE files SET metadata = ?, updated_at = ? WHERE file_id = ?",
    encoded ?? null,
    Date.now(),
    pathId
  );
}

/**
 * Replace the entire tag set for a path. Atomic per DO single-thread.
 * Caller must have already validated the tags.
 */
export function replaceTags(
  durableObject: UserDO,
  userId: string,
  pathId: string,
  tags: readonly string[]
): void {
  const now = Date.now();
  durableObject.sql.exec("DELETE FROM file_tags WHERE path_id = ?", pathId);
  for (const tag of tags) {
    durableObject.sql.exec(
      `INSERT INTO file_tags (path_id, tag, user_id, mtime_ms)
       VALUES (?, ?, ?, ?)`,
      pathId,
      tag,
      userId,
      now
    );
  }
}

/**
 * Add tags to a path's tag set. Idempotent (existing tags are
 * silently skipped via INSERT OR IGNORE). Caller must validate.
 */
export function addTags(
  durableObject: UserDO,
  userId: string,
  pathId: string,
  tags: readonly string[]
): void {
  const now = Date.now();
  for (const tag of tags) {
    durableObject.sql.exec(
      `INSERT OR IGNORE INTO file_tags (path_id, tag, user_id, mtime_ms)
       VALUES (?, ?, ?, ?)`,
      pathId,
      tag,
      userId,
      now
    );
  }
}

/** Remove tags from a path's tag set. */
export function removeTags(
  durableObject: UserDO,
  pathId: string,
  tags: readonly string[]
): void {
  for (const tag of tags) {
    durableObject.sql.exec(
      "DELETE FROM file_tags WHERE path_id = ? AND tag = ?",
      pathId,
      tag
    );
  }
}

/** Get all tags for a path, sorted alphabetically. */
export function getTags(
  durableObject: UserDO,
  pathId: string
): string[] {
  const rows = durableObject.sql
    .exec("SELECT tag FROM file_tags WHERE path_id = ? ORDER BY tag", pathId)
    .toArray() as { tag: string }[];
  return rows.map((r) => r.tag);
}

/**
 * Bump `file_tags.mtime_ms` for every tag on a path. Used after a
 * writeFile / patchMetadata so list-by-tag results reflect the new
 * recency. Single SQL UPDATE; cheaper than DELETE+INSERT.
 */
export function bumpTagMtimes(
  durableObject: UserDO,
  pathId: string,
  mtimeMs: number
): void {
  durableObject.sql.exec(
    "UPDATE file_tags SET mtime_ms = ? WHERE path_id = ?",
    mtimeMs,
    pathId
  );
}

/**
 * Read the live (status=complete) `file_id` for a (parentId, leaf).
 * Returns null if no live row exists. Helper used by writeFile post
 * hooks that need the canonical pathId (the tmpId is gone after
 * commitRename).
 */
export function findCanonicalFileId(
  durableObject: UserDO,
  userId: string,
  parentId: string | null,
  leaf: string
): string | null {
  const row = durableObject.sql
    .exec(
      `SELECT file_id FROM files
        WHERE user_id=? AND IFNULL(parent_id,'')=IFNULL(?,'')
          AND file_name=? AND status='complete'`,
      userId,
      parentId,
      leaf
    )
    .toArray()[0] as { file_id: string } | undefined;
  return row?.file_id ?? null;
}

/**
 * Apply a deep-merge patch to the metadata blob for `pathId`.
 * Returns the merged result (so callers can re-emit it via
 * commitVersion when versioning is on, snapshotting the result).
 */
export function applyMetadataPatch(
  durableObject: UserDO,
  pathId: string,
  patch: Record<string, unknown>
): Record<string, unknown> {
  const existing = readMetadata(durableObject, pathId);
  return deepMerge(existing, patch);
}
