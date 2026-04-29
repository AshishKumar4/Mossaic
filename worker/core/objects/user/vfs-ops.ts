import type { UserDOCore as UserDO } from "./user-do-core";
import type { ShardDO } from "../shard/shard-do";
import {
  VFSError,
  type OpenManifestResult,
  type ResolveResult,
  type VFSScope,
  type VFSStatRaw,
} from "../../../../shared/vfs-types";
import { INLINE_LIMIT, READFILE_MAX, WRITEFILE_MAX } from "../../../../shared/inline";
import { gidFromTenant, inoFromId, uidFromTenant } from "../../../../shared/ino";
import { normalizePath, VFSPathError } from "../../../../shared/vfs-paths";
import { hashChunk } from "../../../../shared/crypto";
import { computeChunkSpec } from "../../../../shared/chunking";
import { placeChunk } from "../../../../shared/placement";
import { generateId, vfsShardDOName } from "../../lib/utils";
import {
  commitVersion,
  isVersioningEnabled,
  placeChunkForVersion,
  shardRefId,
} from "./vfs-versions";
import { resolvePath, resolvePathFollow } from "./path-walk";
// yjs.ts is loaded via dynamic `await import("./yjs")` at
// each yjs-mode call site below (search for `await import("./yjs")`
// in this file). Static imports were pulling the yjs + y-protocols
// runtime into every non-collab consumer's bundle through tsup's
// chunking. The runtime path is only evaluated when a tenant has
// `mode_yjs = 1` on the file.
import {
  validateLabel,
  validateMetadata,
  validateTags,
} from "../../../../shared/metadata-validate";
import { replaceTags } from "./metadata-tags";

/**
 * Read-side VFS operations.
 *
 * All functions are pure SQL on the UserDO's storage. They do NOT do
 * cross-DO subrequests (those are deferred to streaming and the
 * unlink/write sides). The intent is that read-side ops cost zero
 * UserDO subrequests and 0 or N ShardDO subrequests (only readFile
 * of a non-inlined file fans out).
 *
 * Multi-tenant scoping: `scope.tenant` maps directly to
 * `files.user_id` for SQL filtering inside the DO. The
 * `vfsUserDOName(ns, tenant, sub)` helper produces the opaque
 * DO-instance name; the DO itself stays scope-agnostic.
 */

/**
 * Resolve the SQL `user_id` for a given scope.
 *
 * The scope identifies the DO instance via vfsUserDOName, but we
 * *also* use a derived `user_id` for SQL filtering inside the DO so
 * a single DO instance can host multiple sub-tenants without leakage
 * if the binding layer ever consolidates them. The composed form
 * `${tenant}::${sub}` and the same-tenant-no-sub form `${tenant}`
 * are intentionally distinct.
 *
 * Each component is validated against the same character class as
 * vfsUserDOName / vfsShardDOName: `[A-Za-z0-9._-]{1,128}`. This makes
 * the "::" separator unambiguous (no component can contain ":"). Even
 * if vfs-ops were called directly bypassing the DO-name layer, the
 * SQL user_id space remains injection-free.
 */
const VFS_SCOPE_TOKEN = /^[A-Za-z0-9._-]{1,128}$/;

function userIdFor(scope: VFSScope): string {
  if (!scope || typeof scope.tenant !== "string" || scope.tenant.length === 0) {
    throw new VFSError("EINVAL", "scope.tenant is required");
  }
  if (!VFS_SCOPE_TOKEN.test(scope.tenant)) {
    throw new VFSError("EINVAL", `scope.tenant invalid: ${JSON.stringify(scope.tenant)}`);
  }
  if (scope.sub !== undefined) {
    if (typeof scope.sub !== "string" || !VFS_SCOPE_TOKEN.test(scope.sub)) {
      throw new VFSError("EINVAL", `scope.sub invalid: ${JSON.stringify(scope.sub)}`);
    }
    return `${scope.tenant}::${scope.sub}`;
  }
  return scope.tenant;
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
      `SELECT file_id, file_size, mode, mode_yjs, node_kind, symlink_target,
              inline_data, updated_at, encryption_mode, encryption_key_id
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
        mode_yjs: number;
        node_kind: string | null;
        symlink_target: string | null;
        inline_data: ArrayBuffer | null;
        updated_at: number;
        encryption_mode: string | null;
        encryption_key_id: string | null;
      }
    | undefined;
  if (!row) {
    throw new VFSError("ENOENT", "stat: file vanished");
  }
  // project encryption columns into the SDK-facing shape.
  // Defined inline so both file-return branches can call it; symlinks
  // ignore encryption (the target string is plaintext metadata).
  const projectEnc = ():
    | { mode: "convergent" | "random"; keyId?: string }
    | undefined => {
    if (
      row.encryption_mode !== "convergent" &&
      row.encryption_mode !== "random"
    )
      return undefined;
    const enc: { mode: "convergent" | "random"; keyId?: string } = {
      mode: row.encryption_mode,
    };
    if (row.encryption_key_id !== null) enc.keyId = row.encryption_key_id;
    return enc;
  };

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
  // if a head_version_id exists, the truth-of-record is in
  // file_versions, not in `files`. We consult the head version row
  // for size/mode/mtime AND tombstone status. If the head is a
  // tombstone, the path appears ENOENT — same semantics as readFile.
  const headRow = durableObject.sql
    .exec(
      "SELECT head_version_id FROM files WHERE file_id=? AND user_id=?",
      row.file_id,
      userId
    )
    .toArray()[0] as { head_version_id: string | null } | undefined;
  if (headRow?.head_version_id) {
    const head = durableObject.sql
      .exec(
        `SELECT version_id, size, mode, mtime_ms, deleted, inline_data
           FROM file_versions WHERE path_id=? AND version_id=?`,
        row.file_id,
        headRow.head_version_id
      )
      .toArray()[0] as
      | {
          version_id: string;
          size: number;
          mode: number;
          mtime_ms: number;
          deleted: number;
          inline_data: ArrayBuffer | null;
        }
      | undefined;
    if (head) {
      if (head.deleted === 1) {
        throw new VFSError("ENOENT", "stat: head version is a tombstone");
      }
      const vsize = head.inline_data ? head.inline_data.byteLength : head.size;
      const out: VFSStatRaw = {
        type: "file",
        // surface the yjs-mode bit on stat.mode. The
        // mode_yjs flag lives on the `files` row (not the version
        // row) so it's invariant across versions of the same path.
        mode: head.mode | (row.mode_yjs === 1 ? 0o4000 : 0),
        size: vsize,
        mtimeMs: head.mtime_ms,
        uid,
        gid,
        ino: inoFromId(row.file_id),
      };
      // encryption stamp from the head row on `files`. The
      // versioned write path keeps `files.encryption_*` in sync with
      // the head version's columns (see commitVersion), so reading
      // from `files` here is correct.
      const enc = projectEnc();
      if (enc) out.encryption = enc;
      return out;
    }
  }

  // Regular file (path). If inlined, size still reflects
  // file_size (which equals inline_data byteLength by construction
  // in the write path; for legacy / non-inlined rows it's
  // the chunked total).
  const size = row.inline_data
    ? row.inline_data.byteLength
    : row.file_size;
  const out: VFSStatRaw = {
    type: "file",
    // surface the yjs-mode bit on stat.mode (0o4000).
    mode: (row.mode ?? 0o644) | (row.mode_yjs === 1 ? 0o4000 : 0),
    size,
    mtimeMs: row.updated_at,
    uid,
    gid,
    ino: inoFromId(row.file_id),
  };
  // encryption stamp.
  const enc = projectEnc();
  if (enc) out.encryption = enc;
  return out;
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
  if (r.kind === "dir" || r.kind === "symlink") return true;
  if (r.kind !== "file") return false;
  // when versioning is enabled and the head is a tombstone,
  // exists() returns false to match readFile/stat semantics.
  const headRow = durableObject.sql
    .exec(
      "SELECT head_version_id FROM files WHERE file_id=? AND user_id=?",
      r.leafId,
      userId
    )
    .toArray()[0] as { head_version_id: string | null } | undefined;
  if (headRow?.head_version_id) {
    const head = durableObject.sql
      .exec(
        "SELECT deleted FROM file_versions WHERE path_id=? AND version_id=?",
        r.leafId,
        headRow.head_version_id
      )
      .toArray()[0] as { deleted: number } | undefined;
    if (head && head.deleted === 1) return false;
  }
  return true;
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
 *      recorded ShardDO via `vfsShardDOName(scope.ns, tenant, sub, idx)`.
 *      The recorded `shard_index` is unchanged from what
 *      `placeChunk` returned at write time, so reads remain
 *      deterministic across pool growth.
 *   3. Cap at READFILE_MAX (default 100 MB; configurable) — beyond
 *      that, throw EFBIG and direct callers to createReadStream /
 *      openManifest+readChunk.
 *
 * EISDIR: path resolves to a directory.
 * Symlinks: followed (resolveOrThrow with follow=true).
 *
 * Each chunk fetch is one *internal* UserDO subrequest. The consumer
 * still pays exactly 1 subrequest for the entire readFile.
 */
/**
 * versioned read path.
 *
 * Resolution order:
 *   - explicit versionId → that exact row (ENOENT if missing,
 *     tombstone reads the metadata but throws ENOENT for the bytes)
 *   - default → head_version_id; if it points at a tombstone OR no
 *     head exists, ENOENT
 *
 * Inline tier short-circuits (zero shard subrequests). Chunked tier
 * walks version_chunks (NOT file_chunks) so the version's own
 * chunk manifest is the source of truth.
 */
async function readFileVersioned(
  durableObject: UserDO,
  scope: VFSScope,
  pathId: string,
  versionId: string | undefined
): Promise<Uint8Array> {
  // S3 semantics: when no versionId is passed, look at the literal
  // HEAD version (whatever the most-recent write said). If it's a
  // tombstone, ENOENT — even if older live versions still exist
  // in history. Callers asking for an older version explicitly
  // pass `versionId`.
  let row;
  if (versionId) {
    row = durableObject.sql
      .exec(
        `SELECT version_id, size, inline_data, chunk_size, chunk_count, deleted
           FROM file_versions
          WHERE path_id=? AND version_id=?`,
        pathId,
        versionId
      )
      .toArray()[0];
  } else {
    // Resolve the head pointer; if there isn't one (zero versions
    // ever) or it's a tombstone, ENOENT.
    const headRow = durableObject.sql
      .exec(
        "SELECT head_version_id FROM files WHERE file_id=?",
        pathId
      )
      .toArray()[0] as { head_version_id: string | null } | undefined;
    if (!headRow?.head_version_id) {
      throw new VFSError("ENOENT", "readFile: no head version");
    }
    row = durableObject.sql
      .exec(
        `SELECT version_id, size, inline_data, chunk_size, chunk_count, deleted
           FROM file_versions
          WHERE path_id=? AND version_id=?`,
        pathId,
        headRow.head_version_id
      )
      .toArray()[0];
  }
  if (!row) {
    throw new VFSError(
      "ENOENT",
      versionId
        ? `readFile: version ${versionId} not found`
        : "readFile: no live version (tombstoned or empty history)"
    );
  }
  const r = row as Record<string, unknown>;
  const isTombstone = (r.deleted as number) === 1;
  if (isTombstone) {
    throw new VFSError(
      "ENOENT",
      `readFile: version ${r.version_id as string} is a tombstone`
    );
  }
  const inline = (r.inline_data as ArrayBuffer | null) ?? null;
  const size = r.size as number;
  if (inline) {
    if (inline.byteLength > READFILE_MAX) {
      throw new VFSError(
        "EFBIG",
        "readFile: inline blob exceeds READFILE_MAX"
      );
    }
    return new Uint8Array(inline);
  }
  if (size > READFILE_MAX) {
    throw new VFSError(
      "EFBIG",
      `readFile: file_size ${size} > READFILE_MAX ${READFILE_MAX}; use createReadStream or openManifest+readChunk`
    );
  }
  const vid = r.version_id as string;
  const chunkRows = durableObject.sql
    .exec(
      `SELECT chunk_index, chunk_hash, chunk_size, shard_index
         FROM version_chunks WHERE version_id=? ORDER BY chunk_index`,
      vid
    )
    .toArray() as {
    chunk_index: number;
    chunk_hash: string;
    chunk_size: number;
    shard_index: number;
  }[];
  if (chunkRows.length === 0) return new Uint8Array(0);
  const env = durableObject.envPublic;
  const out = new Uint8Array(size);

  // H3: parallel chunk fetches with bounded concurrency (mirrors the
  // read path). Per-chunk destination offset is precomputed
  // from chunk_size so order doesn't depend on arrival.
  const offsets = new Array<number>(chunkRows.length);
  {
    let acc = 0;
    for (let i = 0; i < chunkRows.length; i++) {
      offsets[i] = acc;
      acc += chunkRows[i].chunk_size;
    }
  }
  const CONCURRENCY = 8;
  let next = 0;
  async function fetchOne(i: number): Promise<void> {
    const c = chunkRows[i];
    const shardName = vfsShardDOName(
      scope.ns,
      scope.tenant,
      scope.sub,
      c.shard_index
    );
    const stub = env.MOSSAIC_SHARD.get(env.MOSSAIC_SHARD.idFromName(shardName));
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
    out.set(buf, offsets[i]);
  }
  async function lane(): Promise<void> {
    while (true) {
      const i = next++;
      if (i >= chunkRows.length) return;
      await fetchOne(i);
    }
  }
  const lanes: Promise<void>[] = [];
  for (let w = 0; w < Math.min(CONCURRENCY, chunkRows.length); w++) {
    lanes.push(lane());
  }
  await Promise.all(lanes);

  const written =
    chunkRows.length > 0
      ? offsets[chunkRows.length - 1] + chunkRows[chunkRows.length - 1].chunk_size
      : 0;
  if (written !== size) return out.slice(0, written);
  return out;
}

export async function vfsReadFile(
  durableObject: UserDO,
  scope: VFSScope,
  path: string,
  opts: { versionId?: string } = {}
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

  // yjs-mode fork. If the file has mode_yjs=1 we MUST
  // materialize from the op log + checkpoint instead of file_chunks
  // / file_versions. Even if a head_version_id exists (compaction
  // snapshots ALSO emit Mossaic versions when versioning is on),
  // the live truth is the YjsRuntime materialized doc. Routing
  // through readYjsAsBytes ensures readFile reflects unflushed
  // ops still cached in the in-memory Y.Doc.
  if (isYjsMode(durableObject, userId, r.leafId)) {
    const { readYjsAsBytes } = await import("./yjs");
    return readYjsAsBytes(durableObject, scope, r.leafId);
  }

  // versioning fork. If the path has a head_version_id, OR
  // an explicit versionId was passed, route through file_versions.
  // Otherwise fall through to the file_chunks-based path
  // (preserves byte-equivalence for versioning-OFF tenants and for
  // legacy data written before versioning was ever enabled).
  const headRow = durableObject.sql
    .exec(
      "SELECT head_version_id FROM files WHERE file_id=? AND user_id=?",
      r.leafId,
      userId
    )
    .toArray()[0] as { head_version_id: string | null } | undefined;
  const useVersioned =
    opts.versionId !== undefined || (headRow?.head_version_id ?? null) !== null;
  if (useVersioned) {
    return readFileVersioned(durableObject, scope, r.leafId, opts.versionId);
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

  // H3: parallel chunk fetches with bounded concurrency.
  //
  // Previously: serial `for (...) await stub.fetch(...)` capped
  // throughput at 1 chunk per ~10–30 ms intra-DO RPC, i.e. ~33–100 MB/s
  // for 1 MB chunks. The feasibility study's 200–500 MB/s claim
  // (study §5.2) needs parallel issuance.
  //
  // Bound at 8 concurrent in-flight to stay well under the Workers
  // concurrent-subrequest limit (50 free, 1000 paid). 8 saturates
  // typical home/cloud bandwidth on read while leaving headroom for
  // any other RPCs the calling Worker has in flight. Order of `out.set`
  // is preserved by the destination offset, which is computed from
  // each chunk's known position in the manifest, not its arrival
  // order. Throw-on-first-error is preserved by Promise.all semantics.
  const CONCURRENCY = 8;
  let next = 0;
  // Each chunk's destination offset = sum of preceding chunks' sizes,
  // computed up-front. This decouples the parallel fetches from any
  // notion of arrival order — a chunk's slot is fixed by its index.
  const offsets = new Array<number>(chunkRows.length);
  {
    let acc = 0;
    for (let i = 0; i < chunkRows.length; i++) {
      offsets[i] = acc;
      acc += chunkRows[i].chunk_size;
    }
  }

  async function fetchOne(i: number): Promise<void> {
    const c = chunkRows[i];
    const shardName = vfsShardDOName(
      scope.ns,
      scope.tenant,
      scope.sub,
      c.shard_index
    );
    const stub = env.MOSSAIC_SHARD.get(env.MOSSAIC_SHARD.idFromName(shardName));
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
    out.set(buf, offsets[i]);
  }

  async function worker(): Promise<void> {
    while (true) {
      const i = next++;
      if (i >= chunkRows.length) return;
      await fetchOne(i);
    }
  }
  const workers: Promise<void>[] = [];
  for (let w = 0; w < Math.min(CONCURRENCY, chunkRows.length); w++) {
    workers.push(worker());
  }
  await Promise.all(workers);

  const written = offsets.length > 0
    ? offsets[offsets.length - 1] + chunkRows[offsets.length - 1].chunk_size
    : 0;
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
  const shardName = vfsShardDOName(
    scope.ns,
    scope.tenant,
    scope.sub,
    chunkRow.shard_index
  );
  const stub = env.MOSSAIC_SHARD.get(env.MOSSAIC_SHARD.idFromName(shardName));
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
// Write-side VFS operations.
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
// 30s grace window (+ plumbing).
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
/**
 * External re-export of `hardDeleteFileRow` for the H1 alarm sweeper.
 * The internal callers stay on the private name to keep the surface
 * minimal; the alarm in `user-do.ts` reaches this via dynamic import
 * (avoiding a module-init cycle on the type-side).
 */
export async function hardDeleteFileRowExternal(
  durableObject: UserDO,
  userId: string,
  scope: VFSScope,
  fileId: string
): Promise<void> {
  return hardDeleteFileRow(durableObject, userId, scope, fileId);
}

/**
 * external wrapper around `commitRename` for use by the
 * copyFile primitive in `copy-file.ts`. Same semantics as the
 * private function used internally by writeFile.
 */
export async function commitRenameExternal(
  durableObject: UserDO,
  userId: string,
  scope: VFSScope,
  tmpId: string,
  parentId: string | null,
  leaf: string
): Promise<void> {
  return commitRename(durableObject, userId, scope, tmpId, parentId, leaf);
}

/**
 * external wrapper around `abortTempFile` for copy-file's
 * fan-out failure path. Same semantics as the private wrapper.
 */
export async function abortTempFileExternal(
  durableObject: UserDO,
  userId: string,
  scope: VFSScope,
  tmpId: string
): Promise<void> {
  return abortTempFile(durableObject, userId, scope, tmpId);
}

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
  // drop any tags + version rows still referencing this
  // file_id. Tags are per-pathId; for non-versioning tenants, hard
  // delete also reaps the path identity, so the tags must go too.
  // For versioning-on tenants, hardDeleteFileRow is reachable only
  // when versioning is OFF (it's the non-versioned write supersede
  // path); the versioning path uses dropVersions for its own GC.
  durableObject.sql.exec("DELETE FROM file_tags WHERE path_id = ?", fileId);
  durableObject.sql.exec("DELETE FROM files WHERE file_id = ?", fileId);

  // Then dispatch one deleteChunks RPC per touched shard.
  const env = durableObject.envPublic;
  // Env.MOSSAIC_SHARD is the un-parameterized DurableObjectNamespace; cast to
  // the typed namespace so the .deleteChunks RPC method is visible.
  // Double cast (via `unknown`) because TS treats the un-parameterized
  // form as DurableObjectNamespace<undefined> which doesn't structurally
  // overlap with the typed form.
  const shardNs = env.MOSSAIC_SHARD as unknown as DurableObjectNamespace<ShardDO>;
  for (const { shard_index } of shardRows) {
    const shardName = vfsShardDOName(
      scope.ns,
      scope.tenant,
      scope.sub,
      shard_index
    );
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
/**
 * versioning-ON write path.
 *
 * Pure-function-ish (depends on durableObject + ShardDO env, but no
 * cross-method state). Invariant we want a future TSLean proof to
 * cover: after a successful return, the path has exactly one new
 * head version row whose chunk_refs match the new content. On any
 * thrown error before the commit, NO version row exists and
 * ShardDO chunk_refs for `${pathId}#${versionId}` are reaped.
 */
async function vfsWriteFileVersioned(
  durableObject: UserDO,
  scope: VFSScope,
  userId: string,
  parentId: string | null,
  leaf: string,
  data: Uint8Array,
  mode: number,
  mimeType: string,
  now: number,
  /**
   * Optional metadata + tags + version flags. Applied BEFORE
   * commitVersion so the snapshot captures them. Caller is responsible
   * for validation against the caps in `shared/metadata-caps.ts`.
   */
  meta: {
    metadataEncoded?: Uint8Array | null | undefined;
    tags?: readonly string[] | undefined;
    versionUserVisible?: boolean;
    versionLabel?: string;
    /** Encryption stamp for this version. */
    encryption?: { mode: "convergent" | "random"; keyId?: string };
  } = {}
): Promise<void> {
  // 1. Ensure a stable `files` row exists at (parent_id, leaf).
  //    Either reuse an existing one (path already has versions) or
  //    create a fresh stable identity row. The unique partial index
  //    on (user_id, parent_id, file_name) WHERE status != 'deleted'
  //    guarantees at most one live row per path.
  let pathId: string;
  const existing = durableObject.sql
    .exec(
      `SELECT file_id FROM files
        WHERE user_id=? AND IFNULL(parent_id,'')=IFNULL(?,'') AND file_name=? AND status='complete'`,
      userId,
      parentId,
      leaf
    )
    .toArray()[0] as { file_id: string } | undefined;
  if (existing) {
    pathId = existing.file_id;
  } else {
    pathId = generateId();
    durableObject.sql.exec(
      `INSERT INTO files (file_id, user_id, parent_id, file_name, file_size, file_hash, mime_type, chunk_size, chunk_count, pool_size, status, created_at, updated_at, mode, node_kind)
       VALUES (?, ?, ?, ?, 0, '', ?, 0, 0, ?, 'complete', ?, ?, ?, 'file')`,
      pathId,
      userId,
      parentId,
      leaf,
      mimeType,
      poolSizeFor(durableObject, userId),
      now,
      now,
      mode
    );
  }

  const versionId = generateId();

  // resolve metadata snapshot. Three sources, in order:
  //   1. caller passed `metadataEncoded === null` → CLEAR (NULL blob,
  //      both on the files row and the version snapshot).
  //   2. caller passed `metadataEncoded` Uint8Array → SET (writeMetadata
  //      below + version snapshot uses these bytes).
  //   3. caller passed nothing → KEEP existing files.metadata
  //      (read once, used for the version snapshot only).
  let metadataForVersion: Uint8Array | null = null;
  if (meta.metadataEncoded === null) {
    // Explicit clear: write NULL on files; snapshot is also NULL.
    durableObject.sql.exec(
      "UPDATE files SET metadata = NULL WHERE file_id = ?",
      pathId
    );
    metadataForVersion = null;
  } else if (meta.metadataEncoded !== undefined) {
    durableObject.sql.exec(
      "UPDATE files SET metadata = ? WHERE file_id = ?",
      meta.metadataEncoded,
      pathId
    );
    metadataForVersion = meta.metadataEncoded;
  } else {
    // Read existing for snapshot only.
    const row = durableObject.sql
      .exec("SELECT metadata FROM files WHERE file_id = ?", pathId)
      .toArray()[0] as { metadata: ArrayBuffer | null } | undefined;
    metadataForVersion = row?.metadata ? new Uint8Array(row.metadata) : null;
  }

  // 2a. Inline tier — no shards, no chunk_refs. Just insert the
  //     file_versions row and flip the head pointer.
  if (data.byteLength <= INLINE_LIMIT) {
    commitVersion(durableObject, {
      pathId,
      versionId,
      userId,
      size: data.byteLength,
      mode,
      mtimeMs: now,
      chunkSize: 0,
      chunkCount: 0,
      fileHash: "",
      mimeType,
      inlineData: data,
      userVisible: meta.versionUserVisible ?? true,
      label: meta.versionLabel,
      metadata: metadataForVersion,
      encryption: meta.encryption,
    });
    if (meta.tags !== undefined) {
      const { replaceTags } = await import("./metadata-tags");
      replaceTags(durableObject, userId, pathId, meta.tags);
    } else {
      // Bump tag mtimes so list-by-tag reflects this write's recency.
      const { bumpTagMtimes } = await import("./metadata-tags");
      bumpTagMtimes(durableObject, pathId, now);
    }
    return;
  }

  // 2b. Chunked tier — push chunks under the synthetic
  //     `${pathId}#${versionId}` ref key so refcount is per-version.
  //     On any throw before commit, reap the partial chunk_refs we
  //     inserted (no version row exists yet, so no metadata leak).
  const { chunkSize, chunkCount } = computeChunkSpec(data.byteLength);
  const poolSize = poolSizeFor(durableObject, userId);
  const env = durableObject.envPublic;
  const shardNs = env.MOSSAIC_SHARD as unknown as DurableObjectNamespace<ShardDO>;
  const refId = shardRefId(pathId, versionId);
  const touchedShards = new Set<number>();

  // H3: parallel chunk PUTs with bounded concurrency, mirroring the
  // path. Each lane processes one chunk at a time; chunks
  // are independent so up to CONCURRENCY can be in flight.
  const fileHashByIdx = new Array<string>(chunkCount);
  try {
    const CONCURRENCY = 8;
    let cursor = 0;
    async function uploadOne(i: number): Promise<void> {
      const start = i * chunkSize;
      const end = Math.min(start + chunkSize, data.byteLength);
      const slice = data.subarray(start, end);
      const hash = await hashChunk(slice);
      // invariant: place by CONTENT-HASH (not by
      // (fileId, chunkIndex)). Same hash → same shard, every time.
      // This is what makes cross-version dedup actually work — two
      // versions with identical content land on the same ShardDO,
      // hit the dedup branch, and share one chunk row with
      // refcount = (number of versions referencing it).
      // The / non-versioning path still uses (fileId, idx)
      // placement for spread across shards on a single file.
      // (H4 freezes poolSize at the chunk's first-write so pool
      //  growth never re-routes a hash to a different shard.)
      const sIdx = placeChunkForVersion(durableObject, userId, hash, poolSize);
      const shardName = vfsShardDOName(scope.ns, scope.tenant, scope.sub, sIdx);
      const stub = shardNs.get(shardNs.idFromName(shardName));
      await stub.putChunk(hash, slice, refId, i, userId);
      touchedShards.add(sIdx);
      durableObject.sql.exec(
        `INSERT OR REPLACE INTO version_chunks
           (version_id, chunk_index, chunk_hash, chunk_size, shard_index)
         VALUES (?, ?, ?, ?, ?)`,
        versionId,
        i,
        hash,
        slice.byteLength,
        sIdx
      );
      fileHashByIdx[i] = hash;
    }
    async function lane(): Promise<void> {
      while (true) {
        const i = cursor++;
        if (i >= chunkCount) return;
        await uploadOne(i);
      }
    }
    const lanes: Promise<void>[] = [];
    for (let w = 0; w < Math.min(CONCURRENCY, chunkCount); w++) {
      lanes.push(lane());
    }
    await Promise.all(lanes);
  } catch (err) {
    // Abort: reap the chunk_refs we already pushed under refId so
    // ShardDO refcounts decrement. version_chunks rows: drop them;
    // no file_versions row was inserted yet so there's nothing
    // pointing at them.
    durableObject.sql.exec(
      "DELETE FROM version_chunks WHERE version_id = ?",
      versionId
    );
    for (const sIdx of touchedShards) {
      const shardName = vfsShardDOName(scope.ns, scope.tenant, scope.sub, sIdx);
      const stub = shardNs.get(shardNs.idFromName(shardName));
      try {
        await stub.deleteChunks(refId);
      } catch {
        /* best-effort during abort */
      }
    }
    throw err;
  }

  const fileHash = await hashChunk(
    new TextEncoder().encode(fileHashByIdx.join(""))
  );
  commitVersion(durableObject, {
    pathId,
    versionId,
    userId,
    size: data.byteLength,
    mode,
    mtimeMs: now,
    chunkSize,
    chunkCount,
    fileHash,
    mimeType,
    inlineData: null,
    userVisible: meta.versionUserVisible ?? true,
    label: meta.versionLabel,
    metadata: metadataForVersion,
    encryption: meta.encryption,
  });
  if (meta.tags !== undefined) {
    const { replaceTags } = await import("./metadata-tags");
    replaceTags(durableObject, userId, pathId, meta.tags);
  } else {
    const { bumpTagMtimes } = await import("./metadata-tags");
    bumpTagMtimes(durableObject, pathId, now);
  }
}

/**
 * Extended writeFile options. All fields are optional and default
 * to behavior bit-identical to a plain `writeFile` call.
 *
 * - `metadata`: undefined → no change; null → CLEAR; object → SET.
 * - `tags`: undefined → no change; [] → drop all; [...] → REPLACE.
 * - `version.label`: optional ≤128-char human-readable label.
 * - `version.userVisible`: defaults to true for explicit writes.
 *   YjsRuntime opportunistic compactions pass false; explicit
 *   flush() passes true.
 */
export interface VFSWriteFileOpts {
  mode?: number;
  mimeType?: string;
  metadata?: Record<string, unknown> | null;
  tags?: readonly string[];
  version?: { label?: string; userVisible?: boolean };
  /**
   * opt-in end-to-end encryption.
   *
   * When set, the worker stamps `files.encryption_mode` and
   * `files.encryption_key_id` (and the corresponding `file_versions`
   * columns when versioning is on). Mode-history-monotonic: a write
   * that disagrees with the existing path's mode is rejected EBADF.
   *
   * The `data` payload is treated identically to plaintext bytes
   * regardless of this opt — the SDK has already produced an
   * envelope-stream by this point. The server NEVER decrypts.
   */
  encryption?: { mode: "convergent" | "random"; keyId?: string };
}

export async function vfsWriteFile(
  durableObject: UserDO,
  scope: VFSScope,
  path: string,
  data: Uint8Array,
  opts: VFSWriteFileOpts = {}
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

  // validate metadata + tags BEFORE any SQL touches the
  // row. Validators throw VFSError("EINVAL", ...) on cap violation.
  let metadataEncoded: Uint8Array | null | undefined;
  if (opts.metadata === null) {
    metadataEncoded = null; // explicit clear
  } else if (opts.metadata !== undefined) {
    const { validateMetadata } = await import("../../../../shared/metadata-validate");
    metadataEncoded = validateMetadata(opts.metadata).encoded;
  }
  if (opts.tags !== undefined) {
    const { validateTags } = await import("../../../../shared/metadata-validate");
    validateTags(opts.tags);
  }
  if (opts.version?.label !== undefined) {
    const { validateLabel } = await import("../../../../shared/metadata-validate");
    validateLabel(opts.version.label);
  }

  // validate encryption opts shape and enforce mode-history
  // monotonicity. Both checks throw VFSError before any SQL touches
  // the row, so a rejected write leaves the existing path untouched.
  if (opts.encryption) {
    const { validateEncryptionOpts, enforceModeMonotonic } = await import(
      "./encryption-stamp"
    );
    validateEncryptionOpts(opts.encryption);
    enforceModeMonotonic(durableObject, userId, parentId, leaf, opts.encryption);
  } else {
    // Plaintext write: still need to check we're not silently writing
    // plaintext to an encrypted path.
    const { enforceModeMonotonic } = await import("./encryption-stamp");
    enforceModeMonotonic(durableObject, userId, parentId, leaf, undefined);
  }

  const mode = opts.mode ?? 0o644;
  const mimeType = opts.mimeType ?? "application/octet-stream";
  const now = Date.now();

  // yjs-mode fork. If the target file already exists
  // and has mode_yjs=1, route the bytes through the YjsRuntime.
  // Semantics (Option A): the `data` Uint8Array becomes the new
  // value of the Y.Text("content") inside the doc, emitted as a
  // single CRDT update under origin "writeFile". Versioning fork
  // is bypassed here — yjs op log + periodic checkpoints ARE the
  // history; compaction (not writeFile) creates Mossaic version
  // rows when versioning is also enabled.
  //
  // Use IFNULL on parent_id to make the lookup work for files at
  // the root (parent_id is NULL there) — bare `=` against NULL
  // never matches in SQL. Status filter mirrors findLiveFile but
  // only excludes 'deleted'/'uploading' tombstones; 'complete'
  // matches.
  {
    const existing = durableObject.sql
      .exec(
        `SELECT file_id, mode_yjs FROM files
           WHERE user_id=? AND IFNULL(parent_id,'')=IFNULL(?,'')
             AND file_name=? AND status='complete'`,
        userId,
        parentId,
        leaf
      )
      .toArray()[0] as
      | { file_id: string; mode_yjs: number }
      | undefined;
    if (existing && existing.mode_yjs === 1) {
      const { writeYjsBytes } = await import("./yjs");
      await writeYjsBytes(
        durableObject,
        scope,
        userId,
        existing.file_id,
        poolSizeFor(durableObject, userId),
        data
      );
      // apply metadata/tags to the yjs-mode file. Version
      // opts are ignored on yjs files — the op log IS the history;
      // explicit checkpoints come from `flush()` ().
      // stamp encryption columns if opts.encryption is set.
      await applyPhase12SideEffects(
        durableObject,
        userId,
        existing.file_id,
        metadataEncoded,
        opts.tags,
        Date.now(),
        opts.encryption
      );
      return;
    }
  }

  // versioning fork. When the tenant has versioning ON,
  // every writeFile creates a new file_versions row referenced by
  // a per-version synthetic shard key, and the `files` row is just
  // the stable identity that holds the head pointer. When OFF,
  // behavior is byte-equivalent to (no version rows
  // touched, no head pointer used).
  if (isVersioningEnabled(durableObject, userId)) {
    return vfsWriteFileVersioned(
      durableObject,
      scope,
      userId,
      parentId,
      leaf,
      data,
      mode,
      mimeType,
      now,
      {
        metadataEncoded,
        tags: opts.tags,
        versionUserVisible: opts.version?.userVisible ?? true,
        versionLabel: opts.version?.label,
        encryption: opts.encryption,
      }
    );
  }

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
    // H1: schedule the stale-upload sweep so this row is reclaimed
    // even if commitRename never runs (DO crash mid-method).
    await durableObject.scheduleStaleUploadSweep();
    await commitRename(durableObject, userId, scope, tmpId, parentId, leaf);
    // post-commit side effects. The canonical pathId after
    // commitRename is `tmpId` (the row was renamed in-place; the
    // file_id stayed the same — see commitRename UPDATE). Apply
    // metadata + tags now.
    await applyPhase12SideEffects(
      durableObject,
      userId,
      tmpId,
      metadataEncoded,
      opts.tags,
      now,
      opts.encryption
    );
    return;
  }

  // ── Chunked tier ──
  // EFBIG is already enforced above against WRITEFILE_MAX (100 MB).
  // The previous redundant READFILE_MAX gate has been folded; both caps
  // are equal so a writeFile that succeeds is always readable via
  // readFile. For larger workloads use createWriteStream (memory-bounded
  // streaming) or, on the read side, openManifest + readChunk.
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
  // H1: schedule stale-upload sweep so a crash mid-streaming reclaims
  // this row + its chunk_refs.
  await durableObject.scheduleStaleUploadSweep();

  // Chunk + place + putChunk per chunk. Any throw aborts via abortTempFile.
  //
  // H3: parallel chunk PUTs with bounded concurrency. The previous
  // serial loop capped throughput at one ShardDO RPC per chunk (~10–30
  // ms each); 100 chunks took 1–3 s. The hash + put + record triple
  // is parallelisable because each chunk is independent (no
  // cross-chunk shared state), and the per-chunk file_chunks INSERT
  // is sync SQL inside the DO single-thread so SQL ordering is
  // preserved without coordination.
  //
  // Concurrency cap = 8 (same rationale as the read path: stays well
  // inside the Workers concurrent-subrequest limit and saturates
  // typical bandwidth).
  const env = durableObject.envPublic;
  // Cast the un-parameterized namespace to the typed one so .putChunk RPC
  // resolves. (See hardDeleteFileRow for the same pattern on deleteChunks.)
  const shardNs = env.MOSSAIC_SHARD as unknown as DurableObjectNamespace<ShardDO>;
  const fileHashParts = new Array<string>(chunkCount);
  try {
    const CONCURRENCY = 8;
    let cursor = 0;
    async function uploadOne(i: number): Promise<void> {
      const start = i * chunkSize;
      const end = Math.min(start + chunkSize, data.byteLength);
      const slice = data.subarray(start, end);
      const hash = await hashChunk(slice);
      const sIdx = placeChunk(userId, tmpId, i, poolSize);
      const shardName = vfsShardDOName(scope.ns, scope.tenant, scope.sub, sIdx);
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
      fileHashParts[i] = hash;
    }
    async function lane(): Promise<void> {
      while (true) {
        const i = cursor++;
        if (i >= chunkCount) return;
        await uploadOne(i);
      }
    }
    const lanes: Promise<void>[] = [];
    for (let w = 0; w < Math.min(CONCURRENCY, chunkCount); w++) {
      lanes.push(lane());
    }
    await Promise.all(lanes);
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
  // post-commit side effects on the chunked-tier write.
  // stamp encryption columns when present.
  await applyPhase12SideEffects(
    durableObject,
    userId,
    tmpId,
    metadataEncoded,
    opts.tags,
    now,
    opts.encryption
  );
}

/**
 * apply metadata + tag side-effects to a freshly-committed
 * file. Called from the inline / chunked / yjs branches of
 * `vfsWriteFile` AFTER the canonical files row exists. Pure SQL,
 * no shard work — the caller has already done that.
 *
 * - metadataEncoded === undefined: no change.
 * - metadataEncoded === null: clear (UPDATE files SET metadata=NULL).
 * - metadataEncoded === bytes: set.
 * - tags === undefined: bump existing tag mtimes only (so list-by-tag
 *   reflects the new write recency).
 * - tags === []: drop all tags.
 * - tags === [...]: replace the entire tag set.
 *
 * The versioned write path bakes these into commitVersion and is
 * NOT routed through here.
 */
async function applyPhase12SideEffects(
  durableObject: UserDO,
  userId: string,
  pathId: string,
  metadataEncoded: Uint8Array | null | undefined,
  tags: readonly string[] | undefined,
  mtimeMs: number,
  /**
   * optional encryption stamp. Mode-history-monotonicity
   * was already enforced at the top of `vfsWriteFile`; this just
   * applies the column UPDATE to the freshly-committed row.
   *
   * - undefined → no change to existing encryption columns. Note
   *   that for the chunked/inline write paths the freshly-inserted
   *   row already has NULL columns (defaults), so undefined here is
   *   correct for plaintext writes.
   * - { mode, keyId? } → stamp the columns.
   */
  encryption?: { mode: "convergent" | "random"; keyId?: string }
): Promise<void> {
  if (metadataEncoded !== undefined) {
    durableObject.sql.exec(
      "UPDATE files SET metadata = ?, updated_at = ? WHERE file_id = ?",
      metadataEncoded,
      mtimeMs,
      pathId
    );
  }
  if (tags !== undefined) {
    const { replaceTags } = await import("./metadata-tags");
    replaceTags(durableObject, userId, pathId, tags);
  } else {
    const { bumpTagMtimes } = await import("./metadata-tags");
    bumpTagMtimes(durableObject, pathId, mtimeMs);
  }
  if (encryption !== undefined) {
    const { stampFileEncryption } = await import("./encryption-stamp");
    stampFileEncryption(durableObject, pathId, encryption);
  }
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
      // carry forward metadata + tags from the youngest
      // superseded row IFF the new tmp row hasn't already had them
      // set explicitly. This makes tags + metadata behave like
      // `mode` — properties of the path, not bound to the file_id.
      // Without this, a `writeFile(path, bytes)` call (no opts) on
      // a path that previously had tags would silently lose them
      // because the new tmp row's file_id is fresh.
      //
      // The youngest superseded row is the LAST entry in
      // supersededIds (the loop pushes in iteration order; under
      // contention the last attempt's `live` is freshest).
      if (supersededIds.length > 0) {
        const fromId = supersededIds[supersededIds.length - 1];
        // Copy metadata only if tmp doesn't already have one. The
        // writeFile happy-path applies metadata AFTER commitRename,
        // so the tmp row's metadata is NULL here unless an explicit
        // versioned-write (which routes through vfsWriteFileVersioned
        // and bypasses commitRename) populated it.
        const tmpMeta = durableObject.sql
          .exec("SELECT metadata FROM files WHERE file_id=?", tmpId)
          .toArray()[0] as { metadata: ArrayBuffer | null } | undefined;
        if (!tmpMeta || tmpMeta.metadata === null) {
          durableObject.sql.exec(
            `UPDATE files SET metadata = (SELECT metadata FROM files WHERE file_id=?)
              WHERE file_id=?`,
            fromId,
            tmpId
          );
        }
        // Copy tags from the superseded row to the tmp row — only
        // if the tmp row has no tags yet (i.e. the writer didn't
        // explicitly pass tags=[] or tags=[...]).
        const tmpTagCount = (
          durableObject.sql
            .exec(
              "SELECT COUNT(*) AS n FROM file_tags WHERE path_id=?",
              tmpId
            )
            .toArray()[0] as { n: number }
        ).n;
        if (tmpTagCount === 0) {
          durableObject.sql.exec(
            `INSERT OR IGNORE INTO file_tags (path_id, tag, user_id, mtime_ms)
             SELECT ?, tag, user_id, ? FROM file_tags WHERE path_id=?`,
            tmpId,
            now,
            fromId
          );
        }
      }
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
    const { purgeYjs } = await import("./yjs");
    await purgeYjs(durableObject, scope, r.leafId);
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
  const {
    validateMetadata,
    validateTags,
  } = await import("../../../../shared/metadata-validate");
  const { deepMerge } = await import("../../../../shared/metadata-merge");
  const {
    addTags: addTagsHelper,
    removeTags: removeTagsHelper,
    readMetadata,
    writeMetadata,
  } = await import("./metadata-tags");

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

// ───────────────────────────────────────────────────────────────────────
// Streams + low-level escape hatch.
//
// Two shapes ship together because they cover different consumer needs:
//
//   A. ReadableStream / WritableStream returned over Workers RPC. These
//      are the easy-path "just give me a stream" surface for consumers
//      that happen to be Workers themselves. Stream chunks flow over the
//      binding without buffering the whole file in either side. Backed
//      by Workers' RPC streaming support (compat-date 2024-04-03+).
//
//   B. Handle-based stream primitives (vfsBeginWriteStream / appendWrite
//      / commitWriteStream / abortWriteStream; vfsOpenReadStream /
//      pullReadStream / closeReadStream). These work from non-Worker
//      consumers (browsers, third-party clouds calling the HTTP fallback
//     from) and are the spine that the Worker-side stream
//      wrappers reuse internally. They also let callers resume a stream
//      across separate consumer invocations — important when a single
//      invocation can't fan out enough chunk fetches to read a 10 GB
//      file in one go.
//
// Both shapes share state stored in `files` rows (uploading-status tmp
// rows for writes; manifest+file_id for reads). No additional table —
// the read handle is just a (file_id, scope) pair the caller must pass
// back.
// ───────────────────────────────────────────────────────────────────────

// ── Read stream ────────────────────────────────────────────────────────

/** Opaque write handle returned by vfsBeginWriteStream. */
export interface VFSWriteHandle {
  /** tmp file_id; the same id we'll rename to the real leaf at commit */
  tmpId: string;
  /** parent folder id at commit time */
  parentId: string | null;
  /** target leaf name at commit time */
  leaf: string;
  /** server-authoritative chunk size for this write */
  chunkSize: number;
  /** server-authoritative pool size for placement */
  poolSize: number;
  /**
   * metadata/tags/version snapshot captured at begin-time and
   * applied at commit-time. Internal-only; not surfaced to SDK consumers
   * (the SDK's `WriteHandle` interface is a structural subset that omits
   * this field). Validated at begin so the caller fails fast — commit
   * just SETs.
   */
  commitOpts?: {
    metadataEncoded?: Uint8Array | null;
    tags?: readonly string[];
    versionLabel?: string;
    versionUserVisible?: boolean;
  };
}

/** Opaque read handle returned by vfsOpenReadStream. */
export interface VFSReadHandle {
  fileId: string;
  /** total file size, in bytes */
  size: number;
  /** number of chunks (0 for inlined files) */
  chunkCount: number;
  /** true iff content lives in inline_data; chunkCount == 0 in that case */
  inlined: boolean;
}

/**
 * Open a read handle. Returns a handle the caller pumps via
 * vfsPullReadStream(handle, chunkIndex). The handle is stateless on
 * the server (it's just a fileId + metadata snapshot) so the caller
 * can resume across invocations or fan out parallel pulls.
 */
export function vfsOpenReadStream(
  durableObject: UserDO,
  scope: VFSScope,
  path: string
): VFSReadHandle {
  const userId = userIdFor(scope);
  const r = resolveOrThrow(durableObject, userId, path, /*follow*/ true);
  if (r.kind !== "file") {
    throw new VFSError(
      "EINVAL",
      `openReadStream: not a regular file: ${path}`
    );
  }
  const row = durableObject.sql
    .exec(
      `SELECT file_id, file_size, chunk_count, inline_data
         FROM files
        WHERE file_id=? AND user_id=? AND status='complete'`,
      r.leafId,
      userId
    )
    .toArray()[0] as
    | {
        file_id: string;
        file_size: number;
        chunk_count: number;
        inline_data: ArrayBuffer | null;
      }
    | undefined;
  if (!row) throw new VFSError("ENOENT", "openReadStream: file vanished");
  return {
    fileId: row.file_id,
    size: row.file_size,
    chunkCount: row.inline_data ? 0 : row.chunk_count,
    inlined: !!row.inline_data,
  };
}

/**
 * Pull one chunk from an open read handle. For inlined files
 * (chunkIndex must be 0), returns the inline blob. For chunked files,
 * fetches the chunk from its recorded ShardDO.
 *
 * Range support: callers can pass start/end (in bytes within this
 * chunk) to get a slice. The default is the full chunk.
 *
 * Note: this is the same machinery as vfsReadChunk but typed against
 * a handle for clarity and to make the streaming code paths
 * symmetric with writes.
 */
export async function vfsPullReadStream(
  durableObject: UserDO,
  scope: VFSScope,
  handle: VFSReadHandle,
  chunkIndex: number,
  range?: { start?: number; end?: number }
): Promise<Uint8Array> {
  const userId = userIdFor(scope);
  if (handle.inlined) {
    if (chunkIndex !== 0) {
      throw new VFSError(
        "EINVAL",
        `pullReadStream: inlined file has no chunk index ${chunkIndex}`
      );
    }
    const row = durableObject.sql
      .exec(
        "SELECT inline_data FROM files WHERE file_id=? AND user_id=? AND status='complete'",
        handle.fileId,
        userId
      )
      .toArray()[0] as { inline_data: ArrayBuffer | null } | undefined;
    if (!row || !row.inline_data) {
      throw new VFSError("ENOENT", "pullReadStream: file vanished");
    }
    const buf = new Uint8Array(row.inline_data);
    return range ? sliceWithRange(buf, range) : buf;
  }

  if (
    !Number.isInteger(chunkIndex) ||
    chunkIndex < 0 ||
    chunkIndex >= handle.chunkCount
  ) {
    throw new VFSError(
      "EINVAL",
      `pullReadStream: chunkIndex ${chunkIndex} out of range [0, ${handle.chunkCount})`
    );
  }

  const chunkRow = durableObject.sql
    .exec(
      `SELECT chunk_hash, chunk_size, shard_index FROM file_chunks
        WHERE file_id=? AND chunk_index=?`,
      handle.fileId,
      chunkIndex
    )
    .toArray()[0] as
    | { chunk_hash: string; chunk_size: number; shard_index: number }
    | undefined;
  if (!chunkRow) {
    throw new VFSError(
      "ENOENT",
      `pullReadStream: no chunk at index ${chunkIndex}`
    );
  }
  const env = durableObject.envPublic;
  const shardName = vfsShardDOName(
    scope.ns,
    scope.tenant,
    scope.sub,
    chunkRow.shard_index
  );
  const stub = env.MOSSAIC_SHARD.get(env.MOSSAIC_SHARD.idFromName(shardName));
  const res = await stub.fetch(
    new Request(`http://internal/chunk/${chunkRow.chunk_hash}`)
  );
  if (!res.ok) {
    throw new VFSError(
      "ENOENT",
      `pullReadStream: chunk data missing on shard ${chunkRow.shard_index}`
    );
  }
  const buf = new Uint8Array(await res.arrayBuffer());
  return range ? sliceWithRange(buf, range) : buf;
}

function sliceWithRange(
  buf: Uint8Array,
  range: { start?: number; end?: number }
): Uint8Array {
  const start = range.start ?? 0;
  const end = range.end ?? buf.byteLength;
  if (start < 0 || end > buf.byteLength || start > end) {
    throw new VFSError(
      "EINVAL",
      `range invalid: [${start}, ${end}) for chunk of size ${buf.byteLength}`
    );
  }
  return buf.subarray(start, end);
}

/**
 * Worker-side createReadStream: return a ReadableStream that pulls
 * chunk-by-chunk via vfsPullReadStream. Memory stays bounded to one
 * chunk regardless of file size. Backpressure is honored via the
 * pull controller — each chunk is fetched only when the consumer
 * dequeues the previous one.
 *
 * Range support: optional byte-range over the file; chunks are
 * sliced as needed at the start/end boundaries.
 */
export async function vfsCreateReadStream(
  durableObject: UserDO,
  scope: VFSScope,
  path: string,
  range?: { start?: number; end?: number }
): Promise<ReadableStream<Uint8Array>> {
  const handle = vfsOpenReadStream(durableObject, scope, path);
  const fileSize = handle.size;
  const chunkSize = await getChunkSizeForHandle(durableObject, handle);

  const start = clampOffset(range?.start ?? 0, fileSize);
  const end = clampOffset(range?.end ?? fileSize, fileSize);
  if (end < start) {
    throw new VFSError("EINVAL", `range end < start: [${start}, ${end})`);
  }
  if (handle.inlined) {
    return new ReadableStream<Uint8Array>({
      pull: async (ctrl) => {
        const all = await vfsPullReadStream(durableObject, scope, handle, 0);
        ctrl.enqueue(all.subarray(start, end));
        ctrl.close();
      },
    });
  }

  // Compute first/last chunk indices that intersect the range.
  const firstIdx = Math.floor(start / chunkSize);
  const lastIdx = end === start ? firstIdx - 1 : Math.floor((end - 1) / chunkSize);

  let cur = firstIdx;
  return new ReadableStream<Uint8Array>({
    pull: async (ctrl) => {
      if (cur > lastIdx) {
        ctrl.close();
        return;
      }
      const chunkStartOffset = cur * chunkSize;
      const chunkEndOffset = Math.min(chunkStartOffset + chunkSize, fileSize);
      const sliceStart = Math.max(0, start - chunkStartOffset);
      const sliceEnd = Math.min(
        chunkEndOffset - chunkStartOffset,
        end - chunkStartOffset
      );
      const buf = await vfsPullReadStream(durableObject, scope, handle, cur, {
        start: sliceStart,
        end: sliceEnd,
      });
      ctrl.enqueue(buf);
      cur++;
    },
  });
}

function clampOffset(n: number, max: number): number {
  if (!Number.isFinite(n) || !Number.isInteger(n))
    throw new VFSError("EINVAL", `range value not an integer: ${n}`);
  if (n < 0) throw new VFSError("EINVAL", `range value negative: ${n}`);
  return Math.min(n, max);
}

async function getChunkSizeForHandle(
  durableObject: UserDO,
  handle: VFSReadHandle
): Promise<number> {
  if (handle.inlined) return handle.size;
  const row = durableObject.sql
    .exec("SELECT chunk_size FROM files WHERE file_id=?", handle.fileId)
    .toArray()[0] as { chunk_size: number } | undefined;
  if (!row) throw new VFSError("ENOENT", "createReadStream: file vanished");
  return row.chunk_size;
}

// ── Write stream (handle-based + WritableStream wrapper) ───────────────

/**
 * Begin a write stream. Inserts a tmp file row (status='uploading',
 * file_name='_vfs_tmp_<id>') and returns an opaque handle the caller
 * pumps via vfsAppendWriteStream. Server-authoritative chunkSize and
 * poolSize travel in the handle so the caller cannot influence
 * placement.
 *
 * Concurrency: two parallel begins for the same target path are fine —
 * each gets a distinct tmpId. They race only at commit (the rename),
 * where the unique partial index serializes them.
 */
export function vfsBeginWriteStream(
  durableObject: UserDO,
  scope: VFSScope,
  path: string,
  opts: VFSWriteFileOpts = {}
): VFSWriteHandle {
  const userId = userIdFor(scope);
  const { parentId, leaf } = resolveParent(durableObject, userId, path);
  if (folderExists(durableObject, userId, parentId, leaf)) {
    throw new VFSError(
      "EISDIR",
      `beginWriteStream: target is a directory: ${path}`
    );
  }

  // validate metadata + tags + version label up front so the
  // caller fails fast rather than late-at-commit. Validated payload is
  // stashed on the handle and re-applied at commit.
  let metadataEncoded: Uint8Array | null | undefined;
  if (opts.metadata === null) {
    metadataEncoded = null;
  } else if (opts.metadata !== undefined) {
    metadataEncoded = validateMetadata(opts.metadata).encoded;
  }
  if (opts.tags !== undefined) {
    validateTags(opts.tags);
  }
  if (opts.version?.label !== undefined) {
    validateLabel(opts.version.label);
  }

  const mode = opts.mode ?? 0o644;
  const mimeType = opts.mimeType ?? "application/octet-stream";
  const tmpId = generateId();
  const tmpName = `_vfs_tmp_${tmpId}`;
  const poolSize = poolSizeFor(durableObject, userId);
  const now = Date.now();

  // Pick an initial chunkSize. Streaming writes don't know the final
  // file size upfront, so we use the largest adaptive size (MAX_BLOB_SIZE)
  // when we can't predict — this maximizes throughput per chunk while
  // staying inside the SQLite blob ceiling.
  // NOTE: streaming writes never go through the inline tier — the
  // caller can use writeFile() for small payloads.
  const { chunkSize: defaultChunkSize } = computeChunkSpec(
    1024 * 1024 * 1024
  ); // 1 GB hint → 2 MB chunks
  durableObject.sql.exec(
    `INSERT INTO files (file_id, user_id, parent_id, file_name, file_size, file_hash, mime_type, chunk_size, chunk_count, pool_size, status, created_at, updated_at, mode, node_kind)
     VALUES (?, ?, ?, ?, 0, '', ?, ?, 0, ?, 'uploading', ?, ?, ?, 'file')`,
    tmpId,
    userId,
    parentId,
    tmpName,
    mimeType,
    defaultChunkSize,
    poolSize,
    now,
    now,
    mode
  );

  // Stash the validated commit-time payload on the handle. Cheap to
  // re-pass through the caller; the SDK's structural `WriteHandle` does
  // NOT expose this field so consumers cannot tamper with it.
  const hasCommitOpts =
    metadataEncoded !== undefined ||
    opts.tags !== undefined ||
    opts.version !== undefined;
  return {
    tmpId,
    parentId,
    leaf,
    chunkSize: defaultChunkSize,
    poolSize,
    ...(hasCommitOpts
      ? {
          commitOpts: {
            metadataEncoded,
            tags: opts.tags,
            versionLabel: opts.version?.label,
            versionUserVisible: opts.version?.userVisible,
          },
        }
      : {}),
  };
}

/**
 * Append a single chunk to an open write handle. The chunk is hashed,
 * placed via rendezvous hashing, and PUT to the appropriate ShardDO.
 *
 * `chunkIndex` must be the next sequential index — out-of-order
 * writes are rejected (EINVAL) since the read path assumes contiguous
 * chunks ordered by chunk_index.
 *
 * Returns the bytes written so far (cumulative file size). Useful for
 * caller-side EFBIG enforcement against the consumer's quota.
 */
export async function vfsAppendWriteStream(
  durableObject: UserDO,
  scope: VFSScope,
  handle: VFSWriteHandle,
  chunkIndex: number,
  data: Uint8Array
): Promise<{ bytesWritten: number }> {
  const userId = userIdFor(scope);
  // Verify handle still refers to an uploading row owned by this user.
  const row = durableObject.sql
    .exec(
      `SELECT file_size, chunk_count, status FROM files WHERE file_id=? AND user_id=?`,
      handle.tmpId,
      userId
    )
    .toArray()[0] as
    | { file_size: number; chunk_count: number; status: string }
    | undefined;
  if (!row) {
    throw new VFSError("ENOENT", "appendWriteStream: handle not found");
  }
  if (row.status !== "uploading") {
    throw new VFSError(
      "EINVAL",
      `appendWriteStream: handle not in uploading state (status=${row.status})`
    );
  }
  if (chunkIndex !== row.chunk_count) {
    throw new VFSError(
      "EINVAL",
      `appendWriteStream: out-of-order chunkIndex ${chunkIndex}, expected ${row.chunk_count}`
    );
  }
  if (row.file_size + data.byteLength > WRITEFILE_MAX) {
    throw new VFSError(
      "EFBIG",
      `appendWriteStream: cumulative ${row.file_size + data.byteLength} > WRITEFILE_MAX ${WRITEFILE_MAX}`
    );
  }
  if (data.byteLength === 0) {
    // Zero-byte append is a no-op; don't create a chunk_refs row.
    return { bytesWritten: row.file_size };
  }

  const hash = await hashChunk(data);
  const sIdx = placeChunk(userId, handle.tmpId, chunkIndex, handle.poolSize);
  const env = durableObject.envPublic;
  const shardNs = env.MOSSAIC_SHARD as unknown as DurableObjectNamespace<ShardDO>;
  const shardName = vfsShardDOName(scope.ns, scope.tenant, scope.sub, sIdx);
  const stub = shardNs.get(shardNs.idFromName(shardName));
  await stub.putChunk(hash, data, handle.tmpId, chunkIndex, userId);

  durableObject.sql.exec(
    `INSERT OR REPLACE INTO file_chunks (file_id, chunk_index, chunk_hash, chunk_size, shard_index)
     VALUES (?, ?, ?, ?, ?)`,
    handle.tmpId,
    chunkIndex,
    hash,
    data.byteLength,
    sIdx
  );
  const newSize = row.file_size + data.byteLength;
  const newCount = row.chunk_count + 1;
  durableObject.sql.exec(
    "UPDATE files SET file_size=?, chunk_count=?, updated_at=? WHERE file_id=?",
    newSize,
    newCount,
    Date.now(),
    handle.tmpId
  );
  return { bytesWritten: newSize };
}

/**
 * Commit a write stream: hash the recorded chunk hashes into a file
 * hash, then commit-rename the tmp row onto the target leaf via the
 * same supersede protocol as vfsWriteFile. The displaced row (if any)
 * is hard-deleted and its chunks are queued for GC.
 *
 * when the handle carries `commitOpts` (metadata/tags/version
 * captured at begin-time, validated then), apply them to the tmp row
 * BEFORE commitRename. metadata is written to `files.metadata`; tags
 * are recorded via `replaceTags` (which uses path_id == tmpId, and
 * commitRename carries them forward by virtue of the rename being a
 * file-name-only update — file_id stays stable). The `version` opts
 * are wired only when versioning is enabled for the tenant; when
 * disabled they are silently dropped (matches `writeFile`).
 */
export async function vfsCommitWriteStream(
  durableObject: UserDO,
  scope: VFSScope,
  handle: VFSWriteHandle
): Promise<void> {
  const userId = userIdFor(scope);
  const row = durableObject.sql
    .exec(
      "SELECT status FROM files WHERE file_id=? AND user_id=?",
      handle.tmpId,
      userId
    )
    .toArray()[0] as { status: string } | undefined;
  if (!row) {
    throw new VFSError("ENOENT", "commitWriteStream: handle not found");
  }
  if (row.status !== "uploading") {
    throw new VFSError(
      "EINVAL",
      `commitWriteStream: not in uploading state (status=${row.status})`
    );
  }
  const chunkHashes = durableObject.sql
    .exec(
      "SELECT chunk_hash FROM file_chunks WHERE file_id=? ORDER BY chunk_index",
      handle.tmpId
    )
    .toArray() as { chunk_hash: string }[];
  const fileHash = await hashChunk(
    new TextEncoder().encode(chunkHashes.map((c) => c.chunk_hash).join(""))
  );
  durableObject.sql.exec(
    "UPDATE files SET file_hash=? WHERE file_id=?",
    fileHash,
    handle.tmpId
  );

  // apply metadata + tags BEFORE commitRename. The opts
  // were validated at begin-time so we can write directly.
  const co = handle.commitOpts;
  if (co) {
    if (co.metadataEncoded === null) {
      durableObject.sql.exec(
        "UPDATE files SET metadata = NULL WHERE file_id = ?",
        handle.tmpId
      );
    } else if (co.metadataEncoded !== undefined) {
      durableObject.sql.exec(
        "UPDATE files SET metadata = ? WHERE file_id = ?",
        co.metadataEncoded,
        handle.tmpId
      );
    }
    if (co.tags !== undefined) {
      replaceTags(durableObject, userId, handle.tmpId, co.tags);
    }
  }

  await commitRename(
    durableObject,
    userId,
    scope,
    handle.tmpId,
    handle.parentId,
    handle.leaf
  );

  // version row creation. Streaming writes don't go through
  // commitVersion (the versioned write path is content-addressed by
  // hash; streaming uses tmp-file-id refs). For tenants with versioning
  // enabled, we add a post-commit version row capturing the file_hash
  // + size + label + visibility flag, mirroring writeFile's contract.
  // The chunks themselves are already placed under the path_id (the
  // post-rename file_id == tmpId) which provides the dedup boundary.
  if (co?.versionLabel !== undefined || co?.versionUserVisible !== undefined) {
    if (isVersioningEnabled(durableObject, userId)) {
      const final = durableObject.sql
        .exec(
          "SELECT file_id, file_size, chunk_size, chunk_count, mime_type, mode FROM files WHERE file_id=?",
          handle.tmpId
        )
        .toArray()[0] as
        | {
            file_id: string;
            file_size: number;
            chunk_size: number;
            chunk_count: number;
            mime_type: string;
            mode: number;
          }
        | undefined;
      if (final) {
        const versionId = generateId();
        commitVersion(durableObject, {
          pathId: final.file_id,
          versionId,
          userId,
          size: final.file_size,
          mode: final.mode,
          mtimeMs: Date.now(),
          chunkSize: final.chunk_size,
          chunkCount: final.chunk_count,
          fileHash,
          mimeType: final.mime_type,
          inlineData: null,
          userVisible: co.versionUserVisible ?? true,
          label: co.versionLabel,
          metadata: co.metadataEncoded ?? null,
        });
      }
    }
  }
}

/**
 * Abort a write stream: hard-delete the tmp row + queue chunk GC for
 * any chunks already pushed. Idempotent — safe to call after commit
 * (it's a no-op since the tmp row no longer exists).
 */
export async function vfsAbortWriteStream(
  durableObject: UserDO,
  scope: VFSScope,
  handle: VFSWriteHandle
): Promise<void> {
  const userId = userIdFor(scope);
  await abortTempFile(durableObject, userId, scope, handle.tmpId);
}

/**
 * Worker-side createWriteStream: return a WritableStream backed by the
 * handle-based primitives. The internal buffer is split into chunks
 * of `handle.chunkSize` bytes; each full chunk dispatches an append
 * RPC. close() drains the residual buffer + commits; abort() aborts.
 *
 * Returns a wrapper holding `{ stream, handle }` so callers that want
 * to surface the handle (e.g. for resumability) can grab it.
 */
export async function vfsCreateWriteStream(
  durableObject: UserDO,
  scope: VFSScope,
  path: string,
  opts: VFSWriteFileOpts = {}
): Promise<{ stream: WritableStream<Uint8Array>; handle: VFSWriteHandle }> {
  const handle = vfsBeginWriteStream(durableObject, scope, path, opts);
  let buffer = new Uint8Array(0);
  let chunkIndex = 0;
  let aborted = false;

  const flush = async (final: boolean) => {
    while (
      !aborted &&
      (buffer.byteLength >= handle.chunkSize ||
        (final && buffer.byteLength > 0))
    ) {
      const take = Math.min(handle.chunkSize, buffer.byteLength);
      const slice = buffer.subarray(0, take);
      const copy = new Uint8Array(slice); // detach from rolling buffer
      buffer = buffer.subarray(take);
      await vfsAppendWriteStream(
        durableObject,
        scope,
        handle,
        chunkIndex,
        copy
      );
      chunkIndex++;
    }
  };

  const stream = new WritableStream<Uint8Array>({
    write: async (chunk) => {
      if (aborted) {
        throw new VFSError("EINVAL", "createWriteStream: stream aborted");
      }
      if (!(chunk instanceof Uint8Array)) {
        chunk = new Uint8Array(chunk);
      }
      // Concat into buffer.
      if (buffer.byteLength === 0) {
        buffer = chunk.slice();
      } else {
        const merged = new Uint8Array(buffer.byteLength + chunk.byteLength);
        merged.set(buffer, 0);
        merged.set(chunk, buffer.byteLength);
        buffer = merged;
      }
      await flush(false);
    },
    close: async () => {
      if (aborted) return;
      await flush(true);
      await vfsCommitWriteStream(durableObject, scope, handle);
    },
    abort: async () => {
      aborted = true;
      await vfsAbortWriteStream(durableObject, scope, handle);
    },
  });

  return { stream, handle };
}

// ── helper exports for the multipart-upload module ──────────
//
// The multipart upload code in `multipart-upload.ts` shares the same
// path-resolution + supersede protocol as the streaming-write path
// (`commitRename`, `hardDeleteFileRow`). Rather than duplicate the
// logic, we expose a tiny set of named exports the multipart module
// imports.  Naming follows the existing `*External` pattern from
// (commitRenameExternal etc).

/**
 * external accessor for `userIdFor`. Same validation, same
 * `${tenant}::${sub}` composition.
 */
export function userIdForExternal(scope: VFSScope): string {
  return userIdFor(scope);
}

/**
 * external accessor for `resolveParent`. Returns
 * `(parentId, leaf)` tuple for a path's would-be parent.
 */
export function resolveParentExternal(
  durableObject: UserDO,
  userId: string,
  path: string
): { parentId: string | null; leaf: string } {
  return resolveParent(durableObject, userId, path);
}

/** external accessor for `poolSizeFor`. */
export function poolSizeForExternal(
  durableObject: UserDO,
  userId: string
): number {
  return poolSizeFor(durableObject, userId);
}

/** external accessor for `folderExists`. */
export function folderExistsExternal(
  durableObject: UserDO,
  userId: string,
  parentId: string | null,
  name: string
): boolean {
  return folderExists(durableObject, userId, parentId, name);
}

/** external accessor for `findLiveFile`. */
export function findLiveFileExternal(
  durableObject: UserDO,
  userId: string,
  parentId: string | null,
  leaf: string
): { file_id: string } | undefined {
  return findLiveFile(durableObject, userId, parentId, leaf);
}
