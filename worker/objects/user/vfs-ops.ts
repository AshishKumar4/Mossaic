import type { UserDO } from "./user-do";
import {
  VFSError,
  type OpenManifestResult,
  type ResolveResult,
  type VFSScope,
  type VFSStatRaw,
} from "@shared/vfs-types";
import { READFILE_MAX } from "@shared/inline";
import { gidFromTenant, inoFromId, uidFromTenant } from "@shared/ino";
import { VFSPathError } from "@shared/vfs-paths";
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
 * Wrap synchronous resolution and convert path-error / ELOOP / ENOTDIR
 * into thrown VFSErrors. Returns ResolveResult ONLY for hits (file/dir/symlink);
 * misses throw.
 */
function resolveOrThrow(
  durableObject: UserDO,
  userId: string,
  path: string,
  follow: boolean
): ResolveResult {
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
