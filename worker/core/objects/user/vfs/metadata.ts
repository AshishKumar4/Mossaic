import type { UserDOCore as UserDO } from "../user-do-core";
import {
  VFSError,
  type VFSScope,
} from "../../../../../shared/vfs-types";
import {
  validateMetadata,
  validateTags,
} from "../../../../../shared/metadata-validate";
import {
  addTags as addTagsHelper,
  readMetadata,
  removeTags as removeTagsHelper,
  writeMetadata,
} from "../metadata-tags";
import { deepMerge } from "../../../../../shared/metadata-merge";
import { resolveOrThrow, userIdFor } from "./helpers";

/**
 * VFS metadata + mode + yjs-mode bit.
 *
 * Functions in this module modify file *attributes* without
 * touching content, the manifest, or the namespace. They share
 * `resolveOrThrow` + a pattern of single-column UPDATE.
 *
 * `isYjsMode` is consulted by `vfsReadFile` (`reads.ts`) and
 * `vfsUnlink` (`mutations.ts`) to fork into the yjs materialization
 * path — placing it here keeps the yjs-bit logic in one module.
 */

// ── chmod ──────────────────────────────────────────────────────────────

/** chmod — update mode on a file/symlink/dir. Only the low 12 bits matter. */
export function vfsChmod(
  durableObject: UserDO,
  scope: VFSScope,
  path: string,
  mode: number
): void {
  const userId = userIdFor(scope);
  if (!Number.isInteger(mode) || mode < 0 || mode > 0o7777) {
    throw new VFSError("EINVAL", `chmod: invalid mode: ${mode}`);
  }
  const r = resolveOrThrow(durableObject, userId, path, /*follow*/ false);
  const now = Date.now();
  if (r.kind === "dir") {
    if (r.leafId === "") {
      throw new VFSError("EINVAL", "chmod: cannot chmod root");
    }
    durableObject.sql.exec(
      "UPDATE folders SET mode=?, updated_at=? WHERE folder_id=? AND user_id=?",
      mode,
      now,
      r.leafId,
      userId
    );
  } else {
    durableObject.sql.exec(
      "UPDATE files SET mode=?, updated_at=? WHERE file_id=? AND user_id=?",
      mode,
      now,
      r.leafId,
      userId
    );
  }
}

/**
 * deep-merge a metadata patch into an existing path's
 * `files.metadata` blob, optionally adding/removing tags in the
 * same atomic DO invocation.
 *
 * Semantics:
 *   - patch === null: clear metadata (UPDATE files SET metadata=NULL).
 *   - patch === {...}: deep-merge with existing metadata; null leaves
 *     in the patch DELETE keys (tombstone). Validators in
 *     `@shared/metadata-validate` enforce caps on the MERGED result.
 *   - opts.addTags: idempotent INSERT OR IGNORE per tag.
 *   - opts.removeTags: DELETE WHERE tag IN (...).
 *
 * Atomic per DO single-thread. No shard work — pure UserDO SQL.
 *
 * Throws:
 *   - VFSError("EINVAL", ...) on cap violation (post-merge size,
 *     tag charset, etc.).
 *   - VFSError("ENOENT", ...) if the path doesn't resolve.
 *   - VFSError("EISDIR", ...) for directories — metadata is a
 *     file-only property in v3.
 */
export async function vfsPatchMetadata(
  durableObject: UserDO,
  scope: VFSScope,
  path: string,
  patch: Record<string, unknown> | null,
  opts: { addTags?: readonly string[]; removeTags?: readonly string[] } = {}
): Promise<void> {
  const userId = userIdFor(scope);
  const r = resolveOrThrow(durableObject, userId, path, /*follow*/ true);
  if (r.kind === "dir") {
    throw new VFSError(
      "EISDIR",
      `patchMetadata: target is a directory: ${path}`
    );
  }
  if (r.kind !== "file") {
    throw new VFSError(
      "EINVAL",
      `patchMetadata: not a regular file: ${path}`
    );
  }
  const pathId = r.leafId;
  if (opts.addTags !== undefined) validateTags(opts.addTags);
  if (opts.removeTags !== undefined) validateTags(opts.removeTags);

  if (patch === null) {
    writeMetadata(durableObject, pathId, null);
  } else if (patch !== undefined) {
    const merged = deepMerge(readMetadata(durableObject, pathId), patch);
    const { encoded } = validateMetadata(merged);
    writeMetadata(durableObject, pathId, encoded);
  }

  if (opts.addTags && opts.addTags.length > 0) {
    addTagsHelper(durableObject, userId, pathId, opts.addTags);
  }
  if (opts.removeTags && opts.removeTags.length > 0) {
    removeTagsHelper(durableObject, pathId, opts.removeTags);
  }
}

/**
 * toggle the per-file `mode_yjs` bit. Separate from
 * `vfsChmod(path, mode: number)` because the wire shape is a
 * boolean, not a POSIX mode number. Setting from 0 → 1 promotes
 * a regular file into yjs mode; the existing bytes (if any) are
 * read once and replayed into a fresh Y.Doc as the initial
 * "content" Y.Text. Toggling 1 → 0 is rejected with EINVAL —
 * downgrading would lose CRDT history; if you need that,
 * readFile + write a new non-yjs file at a different path.
 *
 * The promotion path runs in the caller (vfsWriteFileYjs branch
 * picks up the mode bit before any write) — this function ONLY
 * flips the column.
 */
export function vfsSetYjsMode(
  durableObject: UserDO,
  scope: VFSScope,
  path: string,
  enabled: boolean
): void {
  const userId = userIdFor(scope);
  const r = resolveOrThrow(durableObject, userId, path, /*follow*/ false);
  if (r.kind !== "file") {
    throw new VFSError(
      "EINVAL",
      `setYjsMode: not a regular file: ${path}`
    );
  }
  // Look at current state.
  const row = durableObject.sql
    .exec(
      "SELECT mode_yjs FROM files WHERE file_id=? AND user_id=?",
      r.leafId,
      userId
    )
    .toArray()[0] as { mode_yjs: number } | undefined;
  if (!row) throw new VFSError("ENOENT", `setYjsMode: file vanished`);
  const current = row.mode_yjs === 1;
  if (current === enabled) return; // idempotent
  if (current && !enabled) {
    throw new VFSError(
      "EINVAL",
      "setYjsMode: cannot demote yjs-mode to plain (would lose CRDT history)"
    );
  }
  // Promotion: flip the bit.
  durableObject.sql.exec(
    "UPDATE files SET mode_yjs=1, updated_at=? WHERE file_id=? AND user_id=?",
    Date.now(),
    r.leafId,
    userId
  );
}

/**
 * Read the mode_yjs bit for a path. Returns false for any
 * non-file or missing row. Used by the read/write branching in
 * vfsReadFile / vfsWriteFile to decide whether to route through
 * the YjsRuntime.
 */
export function isYjsMode(
  durableObject: UserDO,
  userId: string,
  pathId: string
): boolean {
  const row = durableObject.sql
    .exec(
      "SELECT mode_yjs FROM files WHERE file_id=? AND user_id=?",
      pathId,
      userId
    )
    .toArray()[0] as { mode_yjs: number } | undefined;
  return !!row && row.mode_yjs === 1;
}
