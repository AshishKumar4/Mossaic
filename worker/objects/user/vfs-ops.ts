import type { UserDO } from "./user-do";
import type { ShardDO } from "../shard/shard-do";
import {
  VFSError,
  type OpenManifestResult,
  type ResolveResult,
  type VFSScope,
  type VFSStatRaw,
} from "@shared/vfs-types";
import { INLINE_LIMIT, READFILE_MAX, WRITEFILE_MAX } from "@shared/inline";
import { gidFromTenant, inoFromId, uidFromTenant } from "@shared/ino";
import { normalizePath, VFSPathError } from "@shared/vfs-paths";
import { hashChunk } from "@shared/crypto";
import { computeChunkSpec } from "@shared/chunking";
import { placeChunk } from "@shared/placement";
import { generateId } from "../../lib/utils";
import { resolvePath, resolvePathFollow } from "./path-walk";

/**
 * Phase 2 read-side VFS operations.
 *
 * All functions are pure SQL on the UserDO's storage. They do NOT do any
 * cross-DO subrequests (those are deferred to Phase 4 streaming and to
 * the unlink/write sides). The intent is that read-side ops cost zero
 * UserDO subrequests and 0 or N ShardDO subrequests (only readFile of a
 * non-inlined file fans out).
 *
 * Multi-tenant scoping (Phase 4): for now we map `scope.tenant` directly
 * to `files.user_id`. Phase 4 will introduce `vfsUserDOName(ns, tenant,
 * sub)` and pass an opaque "user_id-equivalent" string here so the DO
 * itself can stay scope-agnostic. The shape of these signatures does not
 * change between phases.
 */

/**
 * Resolve the SQL `user_id` for a given scope.
 *
 * Phase 2: tenant *is* the user_id. The DO instance is already
 * tenant-scoped at the wrangler binding layer; we use scope.tenant for
 * the SQL filter so the DO can host multiple sub-tenants without
 * leakage. Phase 4 may compose sub into this or move scope to DO naming.
 */
function userIdFor(scope: VFSScope): string {
  if (!scope || typeof scope.tenant !== "string" || scope.tenant.length === 0) {
    throw new VFSError("EINVAL", "scope.tenant is required");
  }
  return scope.sub ? `${scope.tenant}::${scope.sub}` : scope.tenant;
}

/**
 * Narrowed `ResolveResult` for hits only — file/dir/symlink. The
 * "miss" variants (ENOENT/ENOTDIR/ELOOP) are converted to thrown
 * `VFSError`s by `resolveOrThrow`, so callers can rely on `leafId`
 * being present on the returned value.
 */
type ResolvedHit = Extract<
  ResolveResult,
  { kind: "file" | "dir" | "symlink" }
>;

/**
 * Wrap synchronous resolution and convert path-error / ELOOP / ENOTDIR
 * into thrown VFSErrors. Returns ResolveResult ONLY for hits (file/dir/symlink);
 * misses throw. Return type is narrowed so callers get `leafId` without
 * having to re-discriminate on `kind`.
 */
function resolveOrThrow(
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

// ── stat / lstat ───────────────────────────────────────────────────────

/**
 * Fetch the row for an already-resolved leaf and turn it into a stat
 * object. Splits files vs folders by source table.
 */
function statForResolved(
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
      `SELECT file_id, file_size, mode, node_kind, symlink_target, inline_data, updated_at
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
        node_kind: string | null;
        symlink_target: string | null;
        inline_data: ArrayBuffer | null;
        updated_at: number;
      }
    | undefined;
  if (!row) {
    throw new VFSError("ENOENT", "stat: file vanished");
  }
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
  // Regular file. If inlined, size still reflects file_size (which equals
  // inline_data byteLength by construction in the future Phase 3 write
  // path; for legacy / non-inlined rows it's the chunked total).
  const size = row.inline_data
    ? row.inline_data.byteLength
    : row.file_size;
  return {
    type: "file",
    mode: row.mode ?? 0o644,
    size,
    mtimeMs: row.updated_at,
    uid,
    gid,
    ino: inoFromId(row.file_id),
  };
}

/** stat() — follows trailing symlinks, throws ELOOP at SYMLINK_MAX_HOPS. */
export function vfsStat(
  durableObject: UserDO,
  scope: VFSScope,
  path: string
): VFSStatRaw {
  const userId = userIdFor(scope);
  const r = resolveOrThrow(durableObject, userId, path, /*follow*/ true);
  return statForResolved(
    durableObject,
    userId,
    scope,
    r as Extract<ResolveResult, { leafId: string }>
  );
}

/** lstat() — does NOT follow trailing symlinks. */
export function vfsLstat(
  durableObject: UserDO,
  scope: VFSScope,
  path: string
): VFSStatRaw {
  const userId = userIdFor(scope);
  const r = resolveOrThrow(durableObject, userId, path, /*follow*/ false);
  return statForResolved(
    durableObject,
    userId,
    scope,
    r as Extract<ResolveResult, { leafId: string }>
  );
}

// ── exists ─────────────────────────────────────────────────────────────

/** exists() — true when the path resolves to file/dir/symlink without throwing. */
export function vfsExists(
  durableObject: UserDO,
  scope: VFSScope,
  path: string
): boolean {
  const userId = userIdFor(scope);
  let r: ResolveResult;
  try {
    r = resolvePath(durableObject, userId, path);
  } catch {
    return false;
  }
  return r.kind === "file" || r.kind === "dir" || r.kind === "symlink";
}

// ── readlink ───────────────────────────────────────────────────────────

/** readlink() — returns the symlink target string. EINVAL if not a symlink. */
export function vfsReadlink(
  durableObject: UserDO,
  scope: VFSScope,
  path: string
): string {
  const userId = userIdFor(scope);
  const r = resolveOrThrow(durableObject, userId, path, /*follow*/ false);
  if (r.kind !== "symlink") {
    throw new VFSError("EINVAL", `readlink: not a symlink: ${path}`);
  }
  return r.target;
}

// ── readdir ────────────────────────────────────────────────────────────

/**
 * readdir() — lists entry names (no stat). Returns the union of folder
 * names and non-deleted file names under the given directory.
 *
 * Trailing-symlink follow: a path resolving to a symlink-to-dir is
 * followed (POSIX). Non-dir leaves throw ENOTDIR.
 */
export function vfsReaddir(
  durableObject: UserDO,
  scope: VFSScope,
  path: string
): string[] {
  const userId = userIdFor(scope);
  const r = resolveOrThrow(durableObject, userId, path, /*follow*/ true);
  if (r.kind !== "dir") {
    throw new VFSError("ENOTDIR", `readdir: not a directory: ${path}`);
  }

  const parentId = r.leafId === "" ? null : r.leafId;
  const folders = durableObject.sql
    .exec(
      `SELECT name FROM folders
        WHERE user_id=? AND IFNULL(parent_id,'')=IFNULL(?,'')
        ORDER BY name`,
      userId,
      parentId
    )
    .toArray() as { name: string }[];
  const files = durableObject.sql
    .exec(
      `SELECT file_name AS name FROM files
        WHERE user_id=? AND IFNULL(parent_id,'')=IFNULL(?,'') AND status!='deleted'
        ORDER BY file_name`,
      userId,
      parentId
    )
    .toArray() as { name: string }[];

  const seen = new Set<string>();
  const out: string[] = [];
  // Folders take precedence in the (rare/illegal) name collision case.
  for (const row of folders) {
    if (seen.has(row.name)) continue;
    seen.add(row.name);
    out.push(row.name);
  }
  for (const row of files) {
    if (seen.has(row.name)) continue;
    seen.add(row.name);
    out.push(row.name);
  }
  out.sort();
  return out;
}

// ── readManyStat ───────────────────────────────────────────────────────

/**
 * readManyStat(paths) — POSIX `lstat` for a batch of paths in one DO
 * invocation. The §7-of-study `git status` unblock: 10k paths in one
 * RPC, consumer pays 1 subrequest.
 *
 * Returns one entry per input path; a missing path becomes `null`
 * instead of throwing so a single ENOENT doesn't kill the batch.
 */
export function vfsReadManyStat(
  durableObject: UserDO,
  scope: VFSScope,
  paths: string[]
): (VFSStatRaw | null)[] {
  const userId = userIdFor(scope);
  const out: (VFSStatRaw | null)[] = [];
  for (const p of paths) {
    let r: ResolveResult;
    try {
      r = resolvePath(durableObject, userId, p);
    } catch {
      out.push(null);
      continue;
    }
    if (r.kind !== "file" && r.kind !== "dir" && r.kind !== "symlink") {
      out.push(null);
      continue;
    }
    out.push(
      statForResolved(
        durableObject,
        userId,
        scope,
        r as Extract<ResolveResult, { leafId: string }>
      )
    );
  }
  return out;
}

// ── readFile ───────────────────────────────────────────────────────────

/**
 * readFile() — bytes of a file. Three layers:
 *
 *   1. If the resolved row has `inline_data` set, return it directly (zero
 *      ShardDO subrequests; the §2.4(i) unlock for tiny git objects).
 *   2. Otherwise, walk `file_chunks` and fetch each chunk from the
 *      recorded ShardDO. For Phase 2 we use the legacy ShardDO name
 *      (`shard:${userId}:${idx}`); Phase 4 will rewire to the
 *      vfs:ns:tenant pattern.
 *   3. Cap at READFILE_MAX (100 MB) — beyond that, throw EFBIG and
 *      direct callers to createReadStream / openManifest+readChunk.
 *
 * EISDIR: path resolves to a directory.
 * Symlinks: followed (resolveOrThrow with follow=true).
 *
 * Each chunk fetch is one *internal* UserDO subrequest. The consumer
 * still pays exactly 1 subrequest for the entire readFile.
 */
export async function vfsReadFile(
  durableObject: UserDO,
  scope: VFSScope,
  path: string
): Promise<Uint8Array> {
  const userId = userIdFor(scope);
  const r = resolveOrThrow(durableObject, userId, path, /*follow*/ true);
  if (r.kind === "dir") {
    throw new VFSError("EISDIR", `readFile: is a directory: ${path}`);
  }
  if (r.kind !== "file") {
    // symlink would have been followed; if we got here it's something else
    throw new VFSError("EINVAL", `readFile: not a regular file: ${path}`);
  }

  const row = durableObject.sql
    .exec(
      `SELECT file_id, file_size, inline_data
         FROM files
        WHERE file_id=? AND user_id=? AND status!='deleted'`,
      r.leafId,
      userId
    )
    .toArray()[0] as
    | {
        file_id: string;
        file_size: number;
        inline_data: ArrayBuffer | null;
      }
    | undefined;
  if (!row) throw new VFSError("ENOENT", "readFile: file vanished");

  if (row.inline_data) {
    if (row.inline_data.byteLength > READFILE_MAX) {
      throw new VFSError("EFBIG", "readFile: inline blob exceeds READFILE_MAX");
    }
    return new Uint8Array(row.inline_data);
  }

  if (row.file_size > READFILE_MAX) {
    throw new VFSError(
      "EFBIG",
      `readFile: file_size ${row.file_size} > READFILE_MAX ${READFILE_MAX}; use createReadStream or openManifest+readChunk`
    );
  }

  // Chunked path. Get chunks ordered by index along with their shard.
  const chunkRows = durableObject.sql
    .exec(
      `SELECT chunk_index, chunk_hash, chunk_size, shard_index
         FROM file_chunks
        WHERE file_id=?
        ORDER BY chunk_index`,
      r.leafId
    )
    .toArray() as {
    chunk_index: number;
    chunk_hash: string;
    chunk_size: number;
    shard_index: number;
  }[];

  if (chunkRows.length === 0) {
    // Empty file (file_size === 0 is legal). Defensive: same as legacy.
    return new Uint8Array(0);
  }

  // Fetch + concatenate. Each shard fetch is an internal UserDO subrequest;
  // the consumer's invocation only paid 1 to enter this method.
  const env = durableObject.envPublic;
  const out = new Uint8Array(row.file_size);
  let written = 0;
  for (const c of chunkRows) {
    const shardName = `shard:${userId}:${c.shard_index}`;
    const stub = env.SHARD_DO.get(env.SHARD_DO.idFromName(shardName));
    const res = await stub.fetch(
      new Request(`http://internal/chunk/${c.chunk_hash}`)
    );
    if (!res.ok) {
      throw new VFSError(
        "ENOENT",
        `readFile: chunk ${c.chunk_index} (${c.chunk_hash}) missing on shard ${c.shard_index}`
      );
    }
    const buf = new Uint8Array(await res.arrayBuffer());
    out.set(buf, written);
    written += buf.byteLength;
  }
  // Trim if file_size disagrees with sum of chunks (defensive; should not happen).
  if (written !== row.file_size) {
    return out.slice(0, written);
  }
  return out;
}

// ── openManifest / readChunk ───────────────────────────────────────────

/**
 * openManifest() — caller-orchestrated escape hatch for files larger than
 * one Worker invocation can fan out to. Returns chunk hashes + indices +
 * sizes only — `shardIndex` is intentionally hidden as an internal
 * placement detail. The companion `vfsReadChunk(path, idx)` is what the
 * caller invokes per chunk, each in a separate consumer invocation.
 */
export function vfsOpenManifest(
  durableObject: UserDO,
  scope: VFSScope,
  path: string
): OpenManifestResult {
  const userId = userIdFor(scope);
  const r = resolveOrThrow(durableObject, userId, path, /*follow*/ true);
  if (r.kind !== "file") {
    throw new VFSError("EINVAL", `openManifest: not a regular file: ${path}`);
  }
  const row = durableObject.sql
    .exec(
      `SELECT file_id, file_size, chunk_size, chunk_count, inline_data
         FROM files
        WHERE file_id=? AND user_id=? AND status!='deleted'`,
      r.leafId,
      userId
    )
    .toArray()[0] as
    | {
        file_id: string;
        file_size: number;
        chunk_size: number;
        chunk_count: number;
        inline_data: ArrayBuffer | null;
      }
    | undefined;
  if (!row) throw new VFSError("ENOENT", "openManifest: file vanished");

  if (row.inline_data) {
    return {
      fileId: row.file_id,
      size: row.inline_data.byteLength,
      chunkSize: 0,
      chunkCount: 0,
      chunks: [],
      inlined: true,
    };
  }

  const chunkRows = durableObject.sql
    .exec(
      `SELECT chunk_index, chunk_hash, chunk_size FROM file_chunks
        WHERE file_id=? ORDER BY chunk_index`,
      r.leafId
    )
    .toArray() as {
    chunk_index: number;
    chunk_hash: string;
    chunk_size: number;
  }[];

  return {
    fileId: row.file_id,
    size: row.file_size,
    chunkSize: row.chunk_size,
    chunkCount: row.chunk_count,
    chunks: chunkRows.map((c) => ({
      index: c.chunk_index,
      hash: c.chunk_hash,
      size: c.chunk_size,
    })),
    inlined: false,
  };
}

/**
 * readChunk() — fetch one chunk by (path, chunkIndex). Used by callers
 * that drove `openManifest` and want to fan out chunk reads across
 * separate invocations. Returns a Uint8Array.
 */
export async function vfsReadChunk(
  durableObject: UserDO,
  scope: VFSScope,
  path: string,
  chunkIndex: number
): Promise<Uint8Array> {
  const userId = userIdFor(scope);
  const r = resolveOrThrow(durableObject, userId, path, /*follow*/ true);
  if (r.kind !== "file") {
    throw new VFSError("EINVAL", `readChunk: not a regular file: ${path}`);
  }
  const inlineRow = durableObject.sql
    .exec(
      `SELECT inline_data FROM files WHERE file_id=? AND user_id=? AND status!='deleted'`,
      r.leafId,
      userId
    )
    .toArray()[0] as { inline_data: ArrayBuffer | null } | undefined;
  if (!inlineRow) throw new VFSError("ENOENT", "readChunk: file vanished");
  if (inlineRow.inline_data) {
    if (chunkIndex !== 0) {
      throw new VFSError(
        "EINVAL",
        `readChunk: inlined file has no chunk index ${chunkIndex}`
      );
    }
    return new Uint8Array(inlineRow.inline_data);
  }
  const chunkRow = durableObject.sql
    .exec(
      `SELECT chunk_hash, chunk_size, shard_index FROM file_chunks
        WHERE file_id=? AND chunk_index=?`,
      r.leafId,
      chunkIndex
    )
    .toArray()[0] as
    | { chunk_hash: string; chunk_size: number; shard_index: number }
    | undefined;
  if (!chunkRow) {
    throw new VFSError(
      "ENOENT",
      `readChunk: no chunk at index ${chunkIndex}`
    );
  }
  const env = durableObject.envPublic;
  const shardName = `shard:${userId}:${chunkRow.shard_index}`;
  const stub = env.SHARD_DO.get(env.SHARD_DO.idFromName(shardName));
  const res = await stub.fetch(
    new Request(`http://internal/chunk/${chunkRow.chunk_hash}`)
  );
  if (!res.ok) {
    throw new VFSError(
      "ENOENT",
      `readChunk: chunk data missing on shard ${chunkRow.shard_index}`
    );
  }
  return new Uint8Array(await res.arrayBuffer());
}

// ───────────────────────────────────────────────────────────────────────
// Phase 3 — Write-side VFS operations.
//
// All writes go through one of three shapes:
//   1. Inline: file ≤ INLINE_LIMIT → single UPDATE on `files`, no shards.
//   2. Chunked: hash + place + putChunk RPC per chunk + recordChunk row +
//      single commit-rename UPDATE.
//   3. Folder/symlink/rename/chmod: pure SQL on the UserDO.
//
// Atomicity is delivered by:
//   - DO single-threaded fetch handler / RPC method ⇒ each method body is
//     its own transaction
//   - UNIQUE partial index on (user_id, parent_id, file_name)
//     WHERE status != 'deleted' ⇒ concurrent writers see each other; the
//     loser of a commit race fails INSERT/UPDATE and we surface EBUSY
//     after a bounded retry
//   - Temp-id-then-rename for writeFile ⇒ a partially-written tmp row
//     never shadows the live file_name; readFile of the path returns the
//     prior content until commit flips status='complete'
//
// GC: hard-delete files+file_chunks rows in the UserDO; queue chunk
// reference decrements on each touched ShardDO via the typed deleteChunks
// RPC. ShardDO's alarm sweeper performs the actual blob delete after the
// 30s grace window (Phase 1 + Phase 3 plumbing).
// ───────────────────────────────────────────────────────────────────────

/**
 * Resolve a path to its (parentId, leafName) tuple — the location for a
 * new entry to be inserted. For a path of `/a/b/leaf`, returns
 * `(folder_id of /a/b, "leaf")`. The parent must exist and be a directory;
 * otherwise ENOENT/ENOTDIR.
 *
 * Root is special-cased: a path of `/leaf` returns `(null, "leaf")`.
 */
function resolveParent(
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
function poolSizeFor(durableObject: UserDO, userId: string): number {
  const row = durableObject.sql
    .exec("SELECT pool_size FROM quota WHERE user_id = ?", userId)
    .toArray()[0] as { pool_size: number } | undefined;
  return row ? row.pool_size : 32;
}

/**
 * Find the live (non-deleted, non-uploading) file row at (parentId, leaf).
 * Used by the commit-rename phase to identify a row to supersede.
 */
function findLiveFile(
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
function folderExists(
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
 * Hard-delete a file row + its file_chunks, and dispatch a deleteChunks
 * RPC to each unique shard the file's chunks lived on. Does NOT touch
 * the inline_data (the row is being dropped wholesale). The caller is
 * responsible for any quota updates.
 *
 * Used by:
 *   - vfsUnlink (direct delete)
 *   - the supersede branch of commit-rename (overwrite)
 *   - vfsRename when the destination is occupied (replace semantics)
 *   - vfsRemoveRecursive for each touched file
 *
 * Subrequest cost: U fan-out RPCs to ShardDOs (one per unique shard).
 */
async function hardDeleteFileRow(
  durableObject: UserDO,
  userId: string,
  scope: VFSScope,
  fileId: string
): Promise<void> {
  // Group by shard before deleting the chunk_index rows.
  const shardRows = durableObject.sql
    .exec(
      "SELECT DISTINCT shard_index FROM file_chunks WHERE file_id = ?",
      fileId
    )
    .toArray() as { shard_index: number }[];

  // Drop UserDO-side metadata first. After this point, even if a
  // subsequent ShardDO RPC fails, the file is no longer reachable
  // through the VFS — the chunks become orphans, eventually swept by
  // the per-shard alarm if/when a re-write happens to push the same
  // (file_id, idx) ref again. (Realistically, since file_id is fresh
  // per write, those orphans need an explicit GC pass; we accept this
  // "best-effort delete" tradeoff for ordering simplicity. The failure
  // surface is a transient ShardDO error, retried by the worker
  // runtime.)
  durableObject.sql.exec("DELETE FROM file_chunks WHERE file_id = ?", fileId);
  durableObject.sql.exec("DELETE FROM files WHERE file_id = ?", fileId);

  // Then dispatch one deleteChunks RPC per touched shard.
  const env = durableObject.envPublic;
  // Env.SHARD_DO is the un-parameterized DurableObjectNamespace; cast to
  // the typed namespace so the .deleteChunks RPC method is visible.
  // Double cast (via `unknown`) because TS treats the un-parameterized
  // form as DurableObjectNamespace<undefined> which doesn't structurally
  // overlap with the typed form.
  // NOTE: `scope.sub` will be wired into the shard naming scheme in
  // Phase 4 (vfsShardDOName). For Phase 3 it's intentionally unused —
  // multi-tenant scoping happens at the DO instance level via tenant
  // → user_id mapping (see userIdFor).
  const shardNs = env.SHARD_DO as unknown as DurableObjectNamespace<ShardDO>;
  for (const { shard_index } of shardRows) {
    const shardName = `shard:${userId}:${shard_index}`;
    const stub = shardNs.get(shardNs.idFromName(shardName));
    // Use the typed RPC; don't await across all in parallel — keep
    // sequential so one bad shard doesn't fan out errors.
    await stub.deleteChunks(fileId);
  }
}

// ── writeFile ──────────────────────────────────────────────────────────

/**
 * writeFile — POSIX-style atomic file write.
 *
 *   1. Resolve parent → (parentId, leaf). Parent must exist and be a dir.
 *   2. If a folder already occupies (parentId, leaf) → EISDIR.
 *   3. Cap at WRITEFILE_MAX → EFBIG.
 *   4. Inline tier (≤ INLINE_LIMIT): single INSERT into files with
 *      inline_data populated, status='complete' from the get-go (no temp
 *      row needed — the inline write is itself atomic).
 *   5. Chunked tier:
 *      a. Insert tmp file row with status='uploading',
 *         file_name='_vfs_tmp_<id>'. (The leading underscore prefix
 *         keeps it out of the UNIQUE-on-non-deleted index for the real
 *         leaf name; uploading rows are not 'deleted' but they DO
 *         occupy the unique index — using a tmp name avoids that
 *         collision while we stream chunks.)
 *      b. Chunk + hash + placeChunk + putChunk RPC + recordChunk row.
 *      c. Commit: in one DO method body, find any live file at
 *         (parentId, leaf), supersede it (status='superseded'), then
 *         rename the tmp row to the real leaf with status='complete'.
 *      d. After the rename commits, hard-delete the superseded row +
 *         dispatch deleteChunks to its shards. (Decoupling the
 *         supersede flip from the chunk GC means the readable-state
 *         transition is itself instantaneous; the GC plays out
 *         asynchronously.)
 *      e. On any error before commit, abort: hard-delete the tmp row +
 *         its tmp chunks. Caller surface: the path either doesn't
 *         exist (no prior file) or still resolves to the prior
 *         contents (with a prior file).
 *
 * Concurrency: two parallel writeFiles to the same path serialize at the
 * UserDO. Both insert tmp rows successfully (different tmp names), both
 * stream chunks. The commits race: the second commit sees a row at
 * (parentId, leaf) that is *not* the tmp name they just inserted, and
 * supersedes it (which may be the first writer's just-committed result —
 * last-writer-wins, POSIX-correct).
 */
export async function vfsWriteFile(
  durableObject: UserDO,
  scope: VFSScope,
  path: string,
  data: Uint8Array,
  opts: { mode?: number; mimeType?: string } = {}
): Promise<void> {
  const userId = userIdFor(scope);
  const { parentId, leaf } = resolveParent(durableObject, userId, path);

  if (data.byteLength > WRITEFILE_MAX) {
    throw new VFSError(
      "EFBIG",
      `writeFile: ${data.byteLength} > WRITEFILE_MAX ${WRITEFILE_MAX}`
    );
  }
  if (folderExists(durableObject, userId, parentId, leaf)) {
    throw new VFSError("EISDIR", `writeFile: target is a directory: ${path}`);
  }

  const mode = opts.mode ?? 0o644;
  const mimeType = opts.mimeType ?? "application/octet-stream";
  const now = Date.now();

  // ── Inline tier ──
  if (data.byteLength <= INLINE_LIMIT) {
    // Two-phase commit pattern: insert with tmp name, then rename to
    // the real leaf so concurrent readers either see the prior file or
    // the new one — never a half-formed inline_data on the live name.
    // For inline, the "stream" is empty so we can do it in two SQL
    // statements with no async work in between.
    const tmpId = generateId();
    const tmpName = `_vfs_tmp_${tmpId}`;
    durableObject.sql.exec(
      `INSERT INTO files (file_id, user_id, parent_id, file_name, file_size, file_hash, mime_type, chunk_size, chunk_count, pool_size, status, created_at, updated_at, mode, node_kind, inline_data)
       VALUES (?, ?, ?, ?, ?, '', ?, 0, 0, ?, 'uploading', ?, ?, ?, 'file', ?)`,
      tmpId,
      userId,
      parentId,
      tmpName,
      data.byteLength,
      mimeType,
      poolSizeFor(durableObject, userId),
      now,
      now,
      mode,
      data
    );
    await commitRename(durableObject, userId, scope, tmpId, parentId, leaf);
    return;
  }

  // ── Chunked tier ──
  if (data.byteLength > READFILE_MAX) {
    // Cap: even though writeFile is one-shot, an oversized buffer would
    // be unreadable through readFile after commit. We could allow it
    // (callers could still use openManifest+readChunk), but EFBIG is
    // the safer default.
    throw new VFSError(
      "EFBIG",
      `writeFile: ${data.byteLength} bytes exceeds 100 MB readFile cap; use createWriteStream`
    );
  }

  const { chunkSize, chunkCount } = computeChunkSpec(data.byteLength);
  const tmpId = generateId();
  const tmpName = `_vfs_tmp_${tmpId}`;
  const poolSize = poolSizeFor(durableObject, userId);

  durableObject.sql.exec(
    `INSERT INTO files (file_id, user_id, parent_id, file_name, file_size, file_hash, mime_type, chunk_size, chunk_count, pool_size, status, created_at, updated_at, mode, node_kind)
     VALUES (?, ?, ?, ?, ?, '', ?, ?, ?, ?, 'uploading', ?, ?, ?, 'file')`,
    tmpId,
    userId,
    parentId,
    tmpName,
    data.byteLength,
    mimeType,
    chunkSize,
    chunkCount,
    poolSize,
    now,
    now,
    mode
  );

  // Chunk + place + putChunk per chunk. Any throw aborts via abortTempFile.
  const env = durableObject.envPublic;
  // Cast the un-parameterized namespace to the typed one so .putChunk RPC
  // resolves. (See hardDeleteFileRow for the same pattern on deleteChunks.)
  const shardNs = env.SHARD_DO as unknown as DurableObjectNamespace<ShardDO>;
  const fileHashParts: string[] = [];
  try {
    for (let i = 0; i < chunkCount; i++) {
      const start = i * chunkSize;
      const end = Math.min(start + chunkSize, data.byteLength);
      const slice = data.subarray(start, end);
      const hash = await hashChunk(slice);
      const sIdx = placeChunk(userId, tmpId, i, poolSize);
      const shardName = `shard:${userId}:${sIdx}`;
      const stub = shardNs.get(shardNs.idFromName(shardName));
      await stub.putChunk(hash, slice, tmpId, i, userId);
      durableObject.sql.exec(
        `INSERT OR REPLACE INTO file_chunks (file_id, chunk_index, chunk_hash, chunk_size, shard_index)
         VALUES (?, ?, ?, ?, ?)`,
        tmpId,
        i,
        hash,
        slice.byteLength,
        sIdx
      );
      fileHashParts.push(hash);
    }
  } catch (err) {
    // Abort: hard-delete the tmp row + dispatch deleteChunks for any
    // chunks already pushed.
    await abortTempFile(durableObject, userId, scope, tmpId);
    throw err;
  }

  const fileHash = await hashChunk(
    new TextEncoder().encode(fileHashParts.join(""))
  );
  durableObject.sql.exec(
    "UPDATE files SET file_hash = ? WHERE file_id = ?",
    fileHash,
    tmpId
  );

  await commitRename(durableObject, userId, scope, tmpId, parentId, leaf);
}

/**
 * Commit-rename: flip the tmp row to the real leaf, superseding any
 * existing live file at the destination. Runs entirely inside one DO
 * method body so the supersede + rename pair is atomic against
 * concurrent reads/writes.
 *
 * Algorithm:
 *   1. Find any live file at (parentId, leaf). If present, mark it
 *      'superseded' + set deleted_at — this frees the UNIQUE partial
 *      index slot without exposing a half-formed state.
 *   2. UPDATE the tmp row: rename to leaf, status='complete'. The unique
 *      partial index is satisfied because the prior live row, if any,
 *      is now status='superseded' (which equals 'deleted' for the
 *      WHERE clause `status != 'deleted'`).
 *
 * Wait — the WHERE clause is `status != 'deleted'`, not `status NOT IN
 * ('deleted', 'superseded')`. We need to use status='deleted' for the
 * supersede so the index sees the slot as free. We do that by setting
 * status='deleted' and deleted_at to mark it superseded (the
 * deleted_at marker distinguishes "soft-deleted by VFS" from "the
 * superseded by a writeFile commit" — but for the unique index it's
 * the same outcome).
 *
 * After the rename commits, kick off the GC: hardDeleteFileRow on the
 * superseded id (drops file_chunks + files row + ShardDO RPC). This
 * happens AFTER the readable-state transition so a reader between
 * commit and GC sees the new content.
 */
async function commitRename(
  durableObject: UserDO,
  userId: string,
  scope: VFSScope,
  tmpId: string,
  parentId: string | null,
  leaf: string
): Promise<void> {
  // Bounded retry: if the unique partial index throws (another commit
  // raced and got there first), supersede that newcomer too. Three
  // attempts is enough — a fourth concurrent writer in one DO instance
  // is exotic given DO single-threading.
  //
  // Track the supersede ids across iterations: if every rename UPDATE
  // throws and we exit via EBUSY, each iteration's already-soft-deleted
  // row needs its chunks GC-dispatched too. Without this, the EBUSY
  // path leaks superseded chunks (refcount stays >0 forever).
  const supersededIds: string[] = [];
  for (let attempt = 0; attempt < 3; attempt++) {
    const live = findLiveFile(durableObject, userId, parentId, leaf);
    if (live) {
      const now = Date.now();
      durableObject.sql.exec(
        "UPDATE files SET status='deleted', deleted_at=?, updated_at=? WHERE file_id=?",
        now,
        now,
        live.file_id
      );
      supersededIds.push(live.file_id);
    }
    try {
      const now = Date.now();
      durableObject.sql.exec(
        "UPDATE files SET file_name=?, status='complete', updated_at=? WHERE file_id=?",
        leaf,
        now,
        tmpId
      );
      // Hard-delete the superseded row + queue chunk GC. Drain ALL
      // supersede ids the loop accumulated (normally just one — `live`
      // for this iteration — but >1 if we retried).
      for (const id of supersededIds) {
        await hardDeleteFileRow(durableObject, userId, scope, id);
      }
      return;
    } catch (err) {
      // UNIQUE constraint violated: someone else committed concurrently.
      // Loop and try again.
      if (attempt === 2) {
        // Give up — hard-delete the tmp + every superseded ghost row's
        // chunks before surfacing EBUSY. Best-effort: swallow GC
        // errors so the EBUSY surfaces unimpeded.
        await abortTempFile(durableObject, userId, scope, tmpId);
        for (const id of supersededIds) {
          try {
            await hardDeleteFileRow(durableObject, userId, scope, id);
          } catch {
            // ignore — best-effort cleanup
          }
        }
        throw new VFSError(
          "EBUSY",
          `writeFile: failed to commit after 3 attempts: ${
            (err as Error).message
          }`
        );
      }
      // try again — supersede whoever got there first
    }
  }
}

/**
 * Abort a temp file write: hard-delete the tmp `files` row, drop any
 * already-recorded `file_chunks`, and queue chunk GC on each touched
 * shard. Idempotent: safe to call on a tmp_id that no longer exists.
 */
async function abortTempFile(
  durableObject: UserDO,
  userId: string,
  scope: VFSScope,
  tmpId: string
): Promise<void> {
  const exists = durableObject.sql
    .exec("SELECT 1 FROM files WHERE file_id = ?", tmpId)
    .toArray();
  if (exists.length === 0) return;
  await hardDeleteFileRow(durableObject, userId, scope, tmpId);
}

// ── unlink ─────────────────────────────────────────────────────────────

/**
 * unlink — hard-delete a regular file or symlink. Throws EISDIR for
 * directories (callers should use rmdir / removeRecursive). Plan §8.4.
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
  await hardDeleteFileRow(durableObject, userId, scope, r.leafId);
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
  const childFolder = durableObject.sql
    .exec(
      "SELECT 1 FROM folders WHERE user_id=? AND parent_id=? LIMIT 1",
      userId,
      r.leafId
    )
    .toArray();
  if (childFolder.length > 0) {
    throw new VFSError("ENOTDIR", `rmdir: directory not empty: ${path}`);
  }
  const childFile = durableObject.sql
    .exec(
      "SELECT 1 FROM files WHERE user_id=? AND parent_id=? AND status!='deleted' LIMIT 1",
      userId,
      r.leafId
    )
    .toArray();
  if (childFile.length > 0) {
    throw new VFSError("ENOTDIR", `rmdir: directory not empty: ${path}`);
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
    // Replace: free the unique-index slot first, then move src in.
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

  for (const f of fileRows) {
    await hardDeleteFileRow(durableObject, userId, scope, f.file_id);
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
