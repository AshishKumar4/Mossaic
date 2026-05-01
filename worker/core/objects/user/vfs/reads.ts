import type { UserDOCore as UserDO } from "../user-do-core";
import type { ShardDO } from "../../shard/shard-do";
import {
  VFSError,
  type OpenManifestResult,
  type ResolveResult,
  type VFSScope,
  type VFSStatRaw,
} from "../../../../../shared/vfs-types";
import { READFILE_MAX } from "../../../../../shared/inline";
import { vfsShardDOName } from "../../../lib/utils";
import { resolvePath } from "../path-walk";
import { resolveOrThrow, statForResolved, userIdFor } from "./helpers";
import { isYjsMode } from "./metadata";

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

// ── stat / lstat ───────────────────────────────────────────────────────

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
    // Phase 25 — tombstone-consistency. The per-path catch was
    // previously scoped to `resolvePath` only; a versioned-tombstone
    // ENOENT from `statForResolved` (helpers.ts:245) propagated and
    // killed the whole batch. We now degrade ANY ENOENT (resolution
    // failure OR tombstoned head) to `null` for that single entry,
    // preserving the documented "missing path becomes null" contract
    // at the JSDoc above.
    try {
      out.push(
        statForResolved(
          durableObject,
          userId,
          scope,
          r as Extract<ResolveResult, { leafId: string }>
        )
      );
    } catch (err) {
      if (err instanceof VFSError && err.code === "ENOENT") {
        out.push(null);
        continue;
      }
      throw err;
    }
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

  // Phase 39 B2/B3 — per-shard batched RPC fan-out (mirrors the
  // Phase-8 read path above). Group chunk-row indices by shard,
  // issue ONE `getChunksBatch(...)` RPC per shard in parallel.
  const offsets = new Array<number>(chunkRows.length);
  {
    let acc = 0;
    for (let i = 0; i < chunkRows.length; i++) {
      offsets[i] = acc;
      acc += chunkRows[i].chunk_size;
    }
  }
  const byShard = new Map<number, number[]>();
  for (let i = 0; i < chunkRows.length; i++) {
    const sIdx = chunkRows[i].shard_index;
    let arr = byShard.get(sIdx);
    if (arr === undefined) {
      arr = [];
      byShard.set(sIdx, arr);
    }
    arr.push(i);
  }
  const shardNs = env.MOSSAIC_SHARD as unknown as DurableObjectNamespace<ShardDO>;
  await Promise.all(
    Array.from(byShard, async ([sIdx, indices]) => {
      const shardName = vfsShardDOName(scope.ns, scope.tenant, scope.sub, sIdx);
      const stub = shardNs.get(shardNs.idFromName(shardName));
      const hashes = indices.map((i) => chunkRows[i].chunk_hash);
      const { bytes } = await stub.getChunksBatch(hashes);
      for (let k = 0; k < indices.length; k++) {
        const i = indices[k];
        const buf = bytes[k];
        if (buf === null) {
          throw new VFSError(
            "ENOENT",
            `readFile: chunk ${chunkRows[i].chunk_index} (${chunkRows[i].chunk_hash}) missing on shard ${sIdx}`
          );
        }
        out.set(buf, offsets[i]);
      }
    })
  );

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

  // Phase 25 Fix 12 — head-tombstone check BEFORE the yjs
  // short-circuit. Pre-fix, a yjs-mode file whose head version was
  // tombstoned (`unlink` under versioning-on) returned live yjs
  // bytes here while `vfsStat` / `vfsExists` / `vfsListFiles` all
  // reported the path as gone. Result: list/stat say "no", readFile
  // says "yes" — a direct cross-surface contradiction. Now an
  // explicit head-tombstone shortcuts to ENOENT regardless of mode.
  // (An explicit `opts.versionId` for a non-tombstoned historical
  // version still works through `readFileVersioned` below.)
  const headRow = durableObject.sql
    .exec(
      `SELECT f.head_version_id, fv.deleted AS head_deleted
         FROM files f
         LEFT JOIN file_versions fv
           ON fv.path_id = f.file_id AND fv.version_id = f.head_version_id
        WHERE f.file_id=? AND f.user_id=?`,
      r.leafId,
      userId
    )
    .toArray()[0] as
    | { head_version_id: string | null; head_deleted: number | null }
    | undefined;
  if (
    headRow !== undefined &&
    headRow.head_version_id !== null &&
    headRow.head_deleted === 1 &&
    opts.versionId === undefined
  ) {
    throw new VFSError(
      "ENOENT",
      "readFile: head version is a tombstone"
    );
  }

  // yjs-mode fork. If the file has mode_yjs=1 we MUST
  // materialize from the op log + checkpoint instead of file_chunks
  // / file_versions. Even if a head_version_id exists (compaction
  // snapshots ALSO emit Mossaic versions when versioning is on),
  // the live truth is the YjsRuntime materialized doc. Routing
  // through readYjsAsBytes ensures readFile reflects unflushed
  // ops still cached in the in-memory Y.Doc.
  if (isYjsMode(durableObject, userId, r.leafId)) {
    const { readYjsAsBytes } = await import("../yjs");
    return readYjsAsBytes(durableObject, scope, r.leafId);
  }

  // versioning fork. If the path has a head_version_id, OR
  // an explicit versionId was passed, route through file_versions.
  // Otherwise fall through to the file_chunks-based path
  // (preserves byte-equivalence for versioning-OFF tenants and for
  // legacy data written before versioning was ever enabled).
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

  // Phase 39 B2/B3 — per-shard batched RPC fan-out.
  //
  // Previous architecture (H3, 8-way concurrent `stub.fetch` per
  // chunk) issued ONE RPC per chunk. For a 100-chunk file landing
  // across 32 shards, that was 100 round trips at ~10–30 ms each.
  //
  // New architecture: group chunks by shard_index, issue ONE
  // `getChunksBatch(hashesOnThatShard)` typed RPC per shard, all
  // dispatched in parallel without intermediate awaits (CF Workers
  // RPC promise pipelining). Round trips drop to O(touched shards),
  // typically ≤ 32 for a tenant's full pool. For a 100-chunk file
  // spread evenly: ~32 RPCs vs 100 — a 3× cut. For a 1000-chunk
  // multipart upload: ~32 RPCs vs 1000 — 30× cut.
  //
  // Order is preserved by precomputed per-chunk offsets — a chunk's
  // destination slot is fixed by its `chunk_index`, not by RPC
  // arrival order. Throw-on-first-error is preserved by Promise.all
  // semantics.
  const offsets = new Array<number>(chunkRows.length);
  {
    let acc = 0;
    for (let i = 0; i < chunkRows.length; i++) {
      offsets[i] = acc;
      acc += chunkRows[i].chunk_size;
    }
  }
  // Group chunk-row indices by shard so each shard receives ONE
  // RPC with the subset of hashes it owns.
  const byShard = new Map<number, number[]>();
  for (let i = 0; i < chunkRows.length; i++) {
    const sIdx = chunkRows[i].shard_index;
    let arr = byShard.get(sIdx);
    if (arr === undefined) {
      arr = [];
      byShard.set(sIdx, arr);
    }
    arr.push(i);
  }
  // Cast the un-parameterized binding to the typed namespace so the
  // typed `.getChunksBatch(...)` RPC method is visible (matches the
  // existing pattern at hardDeleteFileRow et al.).
  const shardNs = env.MOSSAIC_SHARD as unknown as DurableObjectNamespace<ShardDO>;
  // Issue all per-shard RPCs in parallel. Each `.getChunksBatch(...)`
  // returns a Promise immediately (RPC pipelining); we only block
  // when we await the per-shard handler that drains the result into
  // the output buffer.
  await Promise.all(
    Array.from(byShard, async ([sIdx, indices]) => {
      const shardName = vfsShardDOName(scope.ns, scope.tenant, scope.sub, sIdx);
      const stub = shardNs.get(shardNs.idFromName(shardName));
      const hashes = indices.map((i) => chunkRows[i].chunk_hash);
      const { bytes } = await stub.getChunksBatch(hashes);
      for (let k = 0; k < indices.length; k++) {
        const i = indices[k];
        const buf = bytes[k];
        if (buf === null) {
          throw new VFSError(
            "ENOENT",
            `readFile: chunk ${chunkRows[i].chunk_index} (${chunkRows[i].chunk_hash}) missing on shard ${sIdx}`
          );
        }
        out.set(buf, offsets[i]);
      }
    })
  );

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
export async function vfsOpenManifest(
  durableObject: UserDO,
  scope: VFSScope,
  path: string
): Promise<OpenManifestResult> {
  const userId = userIdFor(scope);
  const r = resolveOrThrow(durableObject, userId, path, /*follow*/ true);
  if (r.kind !== "file") {
    throw new VFSError("EINVAL", `openManifest: not a regular file: ${path}`);
  }
  // Phase 25 Fix 10 — tombstone-consistency at the chunked-read
  // boundary. The download-token path (`/api/vfs/download-token` →
  // multipart-routes.ts) drives this RPC; without the head check
  // the SDK could obtain a manifest pointing at legacy chunks for
  // a path whose head has been tombstoned, then issue chunk GETs
  // and stream stale bytes for an "unlinked" file. Same ENOENT
  // semantics as `vfsStat` and `vfsReadFile`.
  //
  // Phase 27.5 — also pull `mode_yjs` so we can short-circuit yjs
  // files. Yjs content lives in `yjs_oplog` + `yjs_checkpoints`,
  // NOT in `file_chunks` / `version_chunks`. Pre-fix, openManifest
  // returned a stale legacy manifest (often size=0, chunks=[]) for
  // yjs files; the SDK then issued chunk GETs and got nothing
  // (or worse: a stale pre-yjs-toggle chunk).
  const row = durableObject.sql
    .exec(
      `SELECT f.file_id, f.file_size, f.chunk_size, f.chunk_count,
              f.inline_data, f.mode_yjs, f.head_version_id,
              fv.deleted AS head_deleted,
              fv.size AS head_size, fv.inline_data AS head_inline,
              fv.chunk_size AS head_chunk_size,
              fv.chunk_count AS head_chunk_count
         FROM files f
         LEFT JOIN file_versions fv
           ON fv.path_id = f.file_id AND fv.version_id = f.head_version_id
        WHERE f.file_id=? AND f.user_id=? AND f.status!='deleted'`,
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
        mode_yjs: number;
        head_version_id: string | null;
        head_deleted: number | null;
        head_size: number | null;
        head_inline: ArrayBuffer | null;
        head_chunk_size: number | null;
        head_chunk_count: number | null;
      }
    | undefined;
  if (!row) throw new VFSError("ENOENT", "openManifest: file vanished");
  if (row.head_version_id !== null && row.head_deleted === 1) {
    throw new VFSError(
      "ENOENT",
      `openManifest: head version is a tombstone for ${path}`
    );
  }

  // Phase 27.5 — yjs-mode short-circuit. Materialize the live Y.Doc
  // once and report it as inlined. Caller (download-token route)
  // sees `inlined: true, chunks: []` and serves the bytes out of
  // band via `vfsReadChunk(path, 0)` (which has the same yjs
  // short-circuit). This matches `vfsReadFile`'s behavior at
  // reads.ts:468.
  if (row.mode_yjs === 1) {
    const { readYjsAsBytes } = await import("../yjs");
    const bytes = await readYjsAsBytes(durableObject, scope, r.leafId);
    return {
      fileId: row.file_id,
      size: bytes.byteLength,
      chunkSize: 0,
      chunkCount: 0,
      chunks: [],
      inlined: true,
    };
  }

  // Versioned tenant path: read manifest from the head version's
  // `version_chunks`, NOT the legacy `file_chunks`. The legacy
  // columns on `files` aren't kept in sync after a versioned write
  // (see `commitVersion` in vfs-versions.ts which writes to
  // `file_versions` + `version_chunks` only).
  if (row.head_version_id !== null) {
    if (row.head_inline) {
      return {
        fileId: row.file_id,
        size: row.head_inline.byteLength,
        chunkSize: 0,
        chunkCount: 0,
        chunks: [],
        inlined: true,
      };
    }
    const verChunkRows = durableObject.sql
      .exec(
        `SELECT chunk_index, chunk_hash, chunk_size FROM version_chunks
          WHERE version_id=? ORDER BY chunk_index`,
        row.head_version_id
      )
      .toArray() as {
      chunk_index: number;
      chunk_hash: string;
      chunk_size: number;
    }[];
    return {
      fileId: row.file_id,
      size: row.head_size ?? 0,
      chunkSize: row.head_chunk_size ?? 0,
      chunkCount: row.head_chunk_count ?? 0,
      chunks: verChunkRows.map((c) => ({
        index: c.chunk_index,
        hash: c.chunk_hash,
        size: c.chunk_size,
      })),
      inlined: false,
    };
  }

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
  // Phase 25 Fix 10 — tombstone gate + versioned-byte-source.
  // Without this, a download-token chunk fetch for a tombstoned-head
  // path would silently stream legacy `file_chunks` bytes ("unlinked"
  // data exposure). For non-tombstoned versioned tenants we also
  // route through `version_chunks` so chunks reflect the head
  // version, matching `readFileVersioned`.
  //
  // Phase 27.5 — also pull `mode_yjs` so yjs files materialize from
  // the live Y.Doc, matching `vfsReadFile` and `vfsOpenManifest`.
  const inlineRow = durableObject.sql
    .exec(
      `SELECT f.inline_data, f.mode_yjs, f.head_version_id,
              fv.deleted AS head_deleted, fv.inline_data AS head_inline
         FROM files f
         LEFT JOIN file_versions fv
           ON fv.path_id = f.file_id AND fv.version_id = f.head_version_id
        WHERE f.file_id=? AND f.user_id=? AND f.status!='deleted'`,
      r.leafId,
      userId
    )
    .toArray()[0] as
    | {
        inline_data: ArrayBuffer | null;
        mode_yjs: number;
        head_version_id: string | null;
        head_deleted: number | null;
        head_inline: ArrayBuffer | null;
      }
    | undefined;
  if (!inlineRow) throw new VFSError("ENOENT", "readChunk: file vanished");
  if (inlineRow.head_version_id !== null && inlineRow.head_deleted === 1) {
    throw new VFSError(
      "ENOENT",
      `readChunk: head version is a tombstone for ${path}`
    );
  }
  // Phase 27.5 — yjs-mode: materialize and serve as a single chunk.
  // Caller's manifest reported `inlined: true, chunks: []`; only
  // chunkIndex===0 is valid and returns the full materialized bytes.
  if (inlineRow.mode_yjs === 1) {
    if (chunkIndex !== 0) {
      throw new VFSError(
        "EINVAL",
        `readChunk: yjs file has no chunk index ${chunkIndex}`
      );
    }
    const { readYjsAsBytes } = await import("../yjs");
    return readYjsAsBytes(durableObject, scope, r.leafId);
  }
  // Versioned tenant: use head version's inline / chunks.
  if (inlineRow.head_version_id !== null) {
    if (inlineRow.head_inline) {
      if (chunkIndex !== 0) {
        throw new VFSError(
          "EINVAL",
          `readChunk: inlined file has no chunk index ${chunkIndex}`
        );
      }
      return new Uint8Array(inlineRow.head_inline);
    }
    const verChunkRow = durableObject.sql
      .exec(
        `SELECT chunk_hash, chunk_size, shard_index FROM version_chunks
          WHERE version_id=? AND chunk_index=?`,
        inlineRow.head_version_id,
        chunkIndex
      )
      .toArray()[0] as
      | { chunk_hash: string; chunk_size: number; shard_index: number }
      | undefined;
    if (!verChunkRow) {
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
      verChunkRow.shard_index
    );
    // Phase 39 B1 — typed `getChunkBytes` RPC instead of HTTP-style
    // `stub.fetch`. One IPC hop, no Response/arrayBuffer marshalling.
    const shardNs = env.MOSSAIC_SHARD as unknown as DurableObjectNamespace<ShardDO>;
    const stub = shardNs.get(shardNs.idFromName(shardName));
    const buf = await stub.getChunkBytes(verChunkRow.chunk_hash);
    if (buf === null) {
      throw new VFSError(
        "ENOENT",
        `readChunk: chunk data missing on shard ${verChunkRow.shard_index}`
      );
    }
    return buf;
  }
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
  const shardName = vfsShardDOName(scope.ns, scope.tenant, scope.sub, chunkRow.shard_index);
  // Phase 39 B1 — typed `getChunkBytes` RPC.
  const shardNs = env.MOSSAIC_SHARD as unknown as DurableObjectNamespace<ShardDO>;
  const stub = shardNs.get(shardNs.idFromName(shardName));
  const buf = await stub.getChunkBytes(chunkRow.chunk_hash);
  if (buf === null) {
    throw new VFSError(
      "ENOENT",
      `readChunk: chunk data missing on shard ${chunkRow.shard_index}`
    );
  }
  return buf;
}
