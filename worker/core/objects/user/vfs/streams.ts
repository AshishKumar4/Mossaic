import type { UserDOCore as UserDO } from "../user-do-core";
import type { ShardDO } from "../../shard/shard-do";
import {
  VFSError,
  type VFSScope,
} from "../../../../../shared/vfs-types";
import { WRITEFILE_MAX } from "../../../../../shared/inline";
import { hashChunk } from "../../../../../shared/crypto";
import { computeChunkSpec } from "../../../../../shared/chunking";
import { generateId, vfsShardDOName } from "../../../lib/utils";
import { placeChunk } from "../../../../../shared/placement";
import {
  commitVersion,
  isVersioningEnabled,
} from "../vfs-versions";
import {
  validateLabel,
  validateMetadata,
  validateTags,
} from "../../../../../shared/metadata-validate";
import { replaceTags } from "../metadata-tags";
import {
  folderExists,
  poolSizeFor,
  resolveOrThrow,
  resolveParent,
  userIdFor,
  findLiveFile,
} from "./helpers";
import {
  abortTempFile,
  commitRename,
  type VFSWriteFileOpts,
} from "./write-commit";

/**
 * Streams + low-level escape hatch.
 *
 * Two shapes ship together because they cover different consumer needs:
 *
 *   A. ReadableStream / WritableStream returned over Workers RPC. These
 *      are the easy-path "just give me a stream" surface for consumers
 *      that happen to be Workers themselves. Stream chunks flow over the
 *      binding without buffering the whole file in either side. Backed
 *      by Workers' RPC streaming support (compat-date 2024-04-03+).
 *
 *   B. Handle-based stream primitives (vfsBeginWriteStream / appendWrite
 *      / commitWriteStream / abortWriteStream; vfsOpenReadStream /
 *      pullReadStream / closeReadStream). These work from non-Worker
 *      consumers (browsers, third-party clouds calling the HTTP fallback
 *     from) and are the spine that the Worker-side stream
 *      wrappers reuse internally. They also let callers resume a stream
 *      across separate consumer invocations — important when a single
 *      invocation can't fan out enough chunk fetches to read a 10 GB
 *      file in one go.
 *
 * Both shapes share state stored in `files` rows (uploading-status tmp
 * rows for writes; manifest+file_id for reads). No additional table —
 * the read handle is just a (file_id, scope) pair the caller must pass
 * back.
 */

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
  /**
   * Phase 25 — when set, `vfsPullReadStream` resolves chunks via
   * `version_chunks` keyed by this versionId (and inline bytes from
   * the head version row). Captured at open-time so a concurrent
   * write doesn't move the head out from under an in-flight stream.
   * Undefined for non-versioned tenants (legacy `file_chunks` /
   * `files.inline_data` path).
   */
  versionId?: string;
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
  // Phase 25 Fix 11 — tombstone gate + versioned-byte-source.
  // Without this, any caller of `vfsCreateReadStream` (HTTP fallback
  // download, SDK `createReadStream`) on a tombstoned-head path
  // streams legacy `file_chunks` bytes for an "unlinked" file. For
  // non-tombstoned versioned tenants the handle's chunkCount/size
  // come from the head version row (which `vfsPullReadStream` then
  // resolves via `version_chunks`), matching `readFileVersioned`.
  const row = durableObject.sql
    .exec(
      `SELECT f.file_id, f.file_size, f.chunk_count, f.inline_data,
              f.head_version_id, fv.deleted AS head_deleted,
              fv.size AS head_size, fv.chunk_count AS head_chunk_count,
              fv.inline_data AS head_inline
         FROM files f
         LEFT JOIN file_versions fv
           ON fv.path_id = f.file_id AND fv.version_id = f.head_version_id
        WHERE f.file_id=? AND f.user_id=? AND f.status='complete'`,
      r.leafId,
      userId
    )
    .toArray()[0] as
    | {
        file_id: string;
        file_size: number;
        chunk_count: number;
        inline_data: ArrayBuffer | null;
        head_version_id: string | null;
        head_deleted: number | null;
        head_size: number | null;
        head_chunk_count: number | null;
        head_inline: ArrayBuffer | null;
      }
    | undefined;
  if (!row) throw new VFSError("ENOENT", "openReadStream: file vanished");
  if (row.head_version_id !== null && row.head_deleted === 1) {
    throw new VFSError(
      "ENOENT",
      `openReadStream: head version is a tombstone for ${path}`
    );
  }
  // Versioned tenant: source size + chunkCount from the head version,
  // and pin `versionId` on the handle so `vfsPullReadStream` reads
  // from `version_chunks` deterministically.
  if (row.head_version_id !== null) {
    return {
      fileId: row.file_id,
      size: row.head_size ?? 0,
      chunkCount: row.head_inline ? 0 : (row.head_chunk_count ?? 0),
      inlined: !!row.head_inline,
      versionId: row.head_version_id,
    };
  }
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
    // Phase 25 Fix 11 — read inline bytes from the head version row
    // when the handle was opened for a versioned tenant; falls back
    // to legacy `files.inline_data` for non-versioned.
    if (handle.versionId !== undefined) {
      const vrow = durableObject.sql
        .exec(
          "SELECT inline_data FROM file_versions WHERE path_id=? AND version_id=?",
          handle.fileId,
          handle.versionId
        )
        .toArray()[0] as { inline_data: ArrayBuffer | null } | undefined;
      if (!vrow || !vrow.inline_data) {
        throw new VFSError("ENOENT", "pullReadStream: version inline missing");
      }
      const buf = new Uint8Array(vrow.inline_data);
      return range ? sliceWithRange(buf, range) : buf;
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

  // Versioned tenant: chunks come from `version_chunks` keyed by
  // the handle's pinned `versionId`. Falls back to legacy
  // `file_chunks` for non-versioned.
  const chunkRow =
    handle.versionId !== undefined
      ? (durableObject.sql
          .exec(
            `SELECT chunk_hash, chunk_size, shard_index FROM version_chunks
              WHERE version_id=? AND chunk_index=?`,
            handle.versionId,
            chunkIndex
          )
          .toArray()[0] as
          | { chunk_hash: string; chunk_size: number; shard_index: number }
          | undefined)
      : (durableObject.sql
          .exec(
            `SELECT chunk_hash, chunk_size, shard_index FROM file_chunks
              WHERE file_id=? AND chunk_index=?`,
            handle.fileId,
            chunkIndex
          )
          .toArray()[0] as
          | { chunk_hash: string; chunk_size: number; shard_index: number }
          | undefined);
  if (!chunkRow) {
    throw new VFSError(
      "ENOENT",
      `pullReadStream: no chunk at index ${chunkIndex}`
    );
  }
  const env = durableObject.envPublic;
  const shardName = vfsShardDOName(scope.ns, scope.tenant, scope.sub, chunkRow.shard_index);
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
  const sIdx = placeChunk(userIdFor(scope), handle.tmpId, chunkIndex, handle.poolSize);
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

  // Phase 27 Fix 6 — commit-write-stream × versioning.
  // Pre-fix this called `commitRename` unconditionally, which under
  // versioning ON hard-deleted any prior live row at the target —
  // destroying its history (same bug class as multipart finalize).
  // Post-fix: under versioning ON, route through `commitVersion`
  // analogously to multipart-versioned-finalize. The chunks already
  // live on ShardDOs under refId=tmpId; we mirror them into
  // version_chunks and stamp shard_ref_id=tmpId so the future
  // dropVersionRows fan-out finds them.
  const versioning = isVersioningEnabled(durableObject, userId);

  if (versioning) {
    // Find existing path identity, if any.
    const liveRow = findLiveFile(
      durableObject,
      userId,
      handle.parentId,
      handle.leaf
    );
    let pathId: string;
    if (liveRow) {
      pathId = liveRow.file_id;
    } else {
      // No prior path — promote the tmp row to live via commitRename
      // (it has no supersede target to hard-delete in this branch).
      await commitRename(
        durableObject,
        userId,
        scope,
        handle.tmpId,
        handle.parentId,
        handle.leaf
      );
      pathId = handle.tmpId;
    }

    // Snapshot the tmp row's final file_chunks into version_chunks
    // for a fresh versionId. The chunks themselves remain on
    // ShardDOs under refId=tmpId; shard_ref_id below preserves
    // that key for future GC.
    const tmpChunks = durableObject.sql
      .exec(
        "SELECT chunk_index, chunk_hash, chunk_size, shard_index FROM file_chunks WHERE file_id=? ORDER BY chunk_index",
        handle.tmpId
      )
      .toArray() as {
      chunk_index: number;
      chunk_hash: string;
      chunk_size: number;
      shard_index: number;
    }[];
    const final = durableObject.sql
      .exec(
        "SELECT file_size, chunk_size, chunk_count, mime_type, mode FROM files WHERE file_id=?",
        handle.tmpId
      )
      .toArray()[0] as
      | {
          file_size: number;
          chunk_size: number;
          chunk_count: number;
          mime_type: string;
          mode: number;
        }
      | undefined;
    if (!final) {
      throw new VFSError(
        "ENOENT",
        "commitWriteStream: tmp row vanished mid-commit"
      );
    }
    const versionId = generateId();
    for (const c of tmpChunks) {
      durableObject.sql.exec(
        `INSERT INTO version_chunks
           (version_id, chunk_index, chunk_hash, chunk_size, shard_index)
         VALUES (?, ?, ?, ?, ?)`,
        versionId,
        c.chunk_index,
        c.chunk_hash,
        c.chunk_size,
        c.shard_index
      );
    }
    commitVersion(durableObject, {
      pathId,
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
      userVisible: co?.versionUserVisible ?? true,
      label: co?.versionLabel ?? null,
      metadata: co?.metadataEncoded ?? null,
      shardRefId: handle.tmpId,
    });

    // When the prior live row was reused as pathId, drop the now-
    // redundant tmp files row WITHOUT chunk fan-out (chunks belong
    // to the new version under refId=tmpId).
    if (liveRow) {
      durableObject.sql.exec(
        "DELETE FROM file_chunks WHERE file_id = ?",
        handle.tmpId
      );
      durableObject.sql.exec(
        "DELETE FROM file_tags WHERE path_id = ?",
        handle.tmpId
      );
      durableObject.sql.exec(
        "DELETE FROM files WHERE file_id = ?",
        handle.tmpId
      );
    }
    return;
  }

  // Versioning OFF — pre-Phase-27 behaviour preserved.
  await commitRename(
    durableObject,
    userId,
    scope,
    handle.tmpId,
    handle.parentId,
    handle.leaf
  );
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
