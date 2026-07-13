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
import { placeChunk, POOL_FULL } from "../../../../../shared/placement";
import { loadFullShards } from "../shard-capacity";
import {
  commitVersionChecked,
  dropTmpRowAfterVersionCommit,
  insertVersionChunk,
  isVersioningEnabled,
  type VersionedFileExpectation,
} from "../vfs-versions";
import {
  validateLabel,
  validateMetadata,
  validateTags,
} from "../../../../../shared/metadata-validate";
import {
  bumpTagMtimes,
  readMetadataBytes,
  replaceTags,
} from "../metadata-tags";
import {
  enforceModeMonotonic,
  stampFileEncryption,
  validateEncryptionOpts,
  type EncryptionStampOpts,
} from "../encryption-stamp";
import {
  bumpFolderRevision,
  folderExists,
  poolSizeFor,
  resolveOrThrow,
  resolveParent,
  userIdFor,
  FILE_HEAD_JOIN,
  assertHeadNotTombstoned,
  recordWriteUsage,
} from "./helpers";
import {
  abortTempFile,
  commitRename,
  disarmChunkCleanupIntents,
  stageChunkCleanupIntents,
  type VFSWriteFileOpts,
} from "./write-commit";
import {
  scheduleStaleUploadSweep,
  transactionSync,
} from "../internal-storage";

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
  /** Present only on handles created before server-owned stream sessions. */
  commitOpts?: WriteStreamCommitOptions;
}

interface WriteStreamCommitOptions {
  metadataEncoded?: Uint8Array | null;
  tags?: readonly string[];
  versionLabel?: string;
  versionUserVisible?: boolean;
  encryption?: EncryptionStampOpts;
}

interface WriteStreamSessionRow extends Record<string, SqlStorageValue> {
  parent_id: string | null;
  leaf: string;
  chunk_size: number;
  pool_size: number;
  metadata_present: number;
  metadata_blob: ArrayBuffer | null;
  tags_json: string | null;
  version_label: string | null;
  version_user_visible: number | null;
  encryption_mode: string | null;
  encryption_key_id: string | null;
  status: string;
  inflight_index: number | null;
  inflight_hash: string | null;
  inflight_at: number | null;
  expires_at: number;
}

function readWriteStreamSession(
  durableObject: UserDO,
  userId: string,
  handle: VFSWriteHandle
): WriteStreamSessionRow {
  const row = durableObject.sql
    .exec(
      `SELECT parent_id, leaf, chunk_size, pool_size, metadata_present,
              metadata_blob, tags_json, version_label, version_user_visible,
              encryption_mode, encryption_key_id, status, inflight_index,
              inflight_hash, inflight_at, expires_at
         FROM write_stream_sessions
        WHERE tmp_id = ? AND user_id = ?`,
      handle.tmpId,
      userId
    )
    .toArray()[0] as WriteStreamSessionRow | undefined;
  if (row) return row;

  // Compatibility for handles that were opened before the server-owned
  // session table shipped. Only rows older than the schema marker qualify;
  // all caller-provided fields are revalidated and immediately persisted.
  const legacy = durableObject.sql
    .exec(
      `SELECT f.parent_id, f.chunk_size, f.pool_size, f.created_at,
              m.applied_at AS enabled_at
         FROM files f
         JOIN meta_schema m ON m.name = 'write_stream_sessions_enabled'
        WHERE f.file_id = ? AND f.user_id = ? AND f.status = 'uploading'
          AND f.file_name = ?`,
      handle.tmpId,
      userId,
      `_vfs_tmp_${handle.tmpId}`
    )
    .toArray()[0] as
    | {
        parent_id: string | null;
        chunk_size: number;
        pool_size: number;
        created_at: number;
        enabled_at: number;
      }
    | undefined;
  if (!legacy || legacy.created_at >= legacy.enabled_at) {
    throw new VFSError("ENOENT", "write stream session not found");
  }
  if (
    handle.parentId !== legacy.parent_id ||
    handle.chunkSize !== legacy.chunk_size ||
    handle.poolSize !== legacy.pool_size
  ) {
    throw new VFSError("EINVAL", "legacy write stream handle was modified");
  }
  const opts = handle.commitOpts;
  if (opts?.metadataEncoded !== undefined && opts.metadataEncoded !== null) {
    validateMetadata(JSON.parse(new TextDecoder().decode(opts.metadataEncoded)));
  }
  if (opts?.tags !== undefined) validateTags(opts.tags);
  if (opts?.versionLabel !== undefined) validateLabel(opts.versionLabel);
  validateEncryptionOpts(opts?.encryption);
  durableObject.sql.exec(
    `INSERT INTO write_stream_sessions
       (tmp_id, user_id, parent_id, leaf, chunk_size, pool_size,
        metadata_present, metadata_blob, tags_json, version_label,
         version_user_visible, encryption_mode, encryption_key_id,
         status, inflight_index, inflight_hash, inflight_at, expires_at, created_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'open', NULL, NULL, NULL, ?, ?)`,
    handle.tmpId,
    userId,
    handle.parentId,
    handle.leaf,
    handle.chunkSize,
    handle.poolSize,
    opts?.metadataEncoded === undefined ? 0 : 1,
    opts?.metadataEncoded ?? null,
    opts?.tags === undefined ? null : JSON.stringify([...opts.tags]),
    opts?.versionLabel ?? null,
    opts?.versionUserVisible === undefined
      ? null
      : opts.versionUserVisible
        ? 1
        : 0,
    opts?.encryption?.mode ?? null,
    opts?.encryption?.keyId ?? null,
    legacy.created_at + 60 * 60 * 1000,
    legacy.created_at
  );
  return readWriteStreamSession(durableObject, userId, {
    ...handle,
    commitOpts: undefined,
  });
}

function commitOptionsFromSession(
  row: WriteStreamSessionRow
): WriteStreamCommitOptions | undefined {
  const tags = row.tags_json === null
    ? undefined
    : (JSON.parse(row.tags_json) as string[]);
  const metadataEncoded = row.metadata_present === 0
    ? undefined
    : row.metadata_blob === null
      ? null
      : new Uint8Array(row.metadata_blob);
  const encryption = row.encryption_mode === null
    ? undefined
    : {
        mode: row.encryption_mode as "convergent" | "random",
        ...(row.encryption_key_id === null ? {} : { keyId: row.encryption_key_id }),
      };
  if (
    metadataEncoded === undefined &&
    tags === undefined &&
    row.version_label === null &&
    row.version_user_visible === null &&
    encryption === undefined
  ) {
    return undefined;
  }
  return {
    metadataEncoded,
    tags,
    versionLabel: row.version_label ?? undefined,
    versionUserVisible:
      row.version_user_visible === null
        ? undefined
        : row.version_user_visible !== 0,
    encryption,
  };
}

function assertHandleMatchesSession(
  handle: VFSWriteHandle,
  session: WriteStreamSessionRow
): void {
  if (
    handle.parentId !== session.parent_id ||
    handle.leaf !== session.leaf ||
    handle.chunkSize !== session.chunk_size ||
    handle.poolSize !== session.pool_size
  ) {
    throw new VFSError("EINVAL", "write stream handle was modified");
  }
}

/** Opaque read handle returned by vfsOpenReadStream. */
export interface VFSReadHandle {
  fileId: string;
  /** total file size, in bytes */
  size: number;
  /** number of chunks (0 for inlined files) */
  chunkCount: number;
  /**
   * server-authoritative chunkSize (bytes per chunk except possibly the
   * last). Pinned at open-time so `vfsCreateReadStream`'s range math
   * never has to re-query a stale `files.chunk_size` (which is 0 on
   * versioned tenants — readPreview would otherwise hit a NaN
   * div-by-zero). For inlined / yjs / empty files this is 0; the
   * stream layer above must guard div-by-zero before using it.
   */
  chunkSize: number;
  /** true iff content lives in inline_data; chunkCount == 0 in that case */
  inlined: boolean;
  /**
   * When set, `vfsPullReadStream` resolves chunks via
   * `version_chunks` keyed by this versionId (and inline bytes from
   * the head version row). Captured at open-time so a concurrent
   * write doesn't move the head out from under an in-flight stream.
   * Undefined for non-versioned tenants (legacy `file_chunks` /
   * `files.inline_data` path).
   */
  versionId?: string;
  /**
   * Pre-materialized bytes for yjs-mode files. Yjs content lives
   * in `yjs_oplog` + `yjs_checkpoints`, NOT in
   * `file_chunks` / `version_chunks`. The stream surface materializes
   * the live `Y.Doc` once at open-time and stashes the bytes here so
   * `vfsPullReadStream` can serve them as if they were inlined. When
   * present, `inlined === true` and `chunkCount === 0`.
   */
  inlineBytes?: Uint8Array;
}

/**
 * Open a read handle. Returns a handle the caller pumps via
 * vfsPullReadStream(handle, chunkIndex). The handle is stateless on
 * the server (it's just a fileId + metadata snapshot) so the caller
 * can resume across invocations or fan out parallel pulls.
 */
export async function vfsOpenReadStream(
  durableObject: UserDO,
  scope: VFSScope,
  path: string
): Promise<VFSReadHandle> {
  const userId = userIdFor(scope);
  const r = resolveOrThrow(durableObject, userId, path, /*follow*/ true);
  if (r.kind !== "file") {
    throw new VFSError(
      "EINVAL",
      `openReadStream: not a regular file: ${path}`
    );
  }
  // Tombstone gate + versioned-byte-source. Without this, any
  // caller of `vfsCreateReadStream` (HTTP fallback download, SDK
  // `createReadStream`) on a tombstoned-head path would stream
  // legacy `file_chunks` bytes for an "unlinked" file. For
  // non-tombstoned versioned tenants the handle's chunkCount/size
  // come from the head version row (which `vfsPullReadStream`
  // then resolves via `version_chunks`), matching
  // `readFileVersioned`.
  //
  // Also pull `mode_yjs`, `chunk_size` (legacy), and the head
  // version's `chunk_size` so the handle carries an authoritative
  // chunkSize. A naive `getChunkSizeForHandle` querying
  // `files.chunk_size` would be STALE (0) for versioned tenants —
  // producing `Math.floor(0/0) === NaN` in `vfsCreateReadStream`.
  // Pinning chunkSize at open-time eliminates that race.
  const rowRaw = durableObject.sql
    .exec(
      `SELECT f.file_id, f.file_size, f.chunk_size, f.chunk_count,
              f.inline_data, f.mode_yjs, f.head_version_id,
              fv.deleted AS head_deleted,
              fv.size AS head_size, fv.chunk_size AS head_chunk_size,
              fv.chunk_count AS head_chunk_count,
              fv.inline_data AS head_inline
         FROM files f
         ${FILE_HEAD_JOIN}
        WHERE f.file_id=? AND f.user_id=? AND f.status='complete'`,
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
        head_chunk_size: number | null;
        head_chunk_count: number | null;
        head_inline: ArrayBuffer | null;
      }
    | undefined;
  const row = assertHeadNotTombstoned(rowRaw, "openReadStream", path);

  // Yjs-mode short-circuit. Yjs files persist as an op-log +
  // checkpoint pair (`yjs_oplog` / `yjs_checkpoints`), NOT as
  // `file_chunks` / `version_chunks`. Materialize the live Y.Doc
  // once at open-time and serve it as a single inlined buffer. This
  // matches `vfsReadFile`'s yjs short-circuit at reads.ts:468 and
  // means `createReadStream` / `openManifest` / `readChunk` /
  // `readPreview` all see consistent yjs bytes.
  if (row.mode_yjs === 1) {
    const { readYjsAsBytes } = await import("../yjs");
    const bytes = await readYjsAsBytes(durableObject, scope, r.leafId);
    return {
      fileId: row.file_id,
      size: bytes.byteLength,
      chunkCount: 0,
      chunkSize: 0,
      inlined: true,
      inlineBytes: bytes,
    };
  }

  // Versioned tenant: source size + chunkCount + chunkSize from the
  // head version, and pin `versionId` on the handle so
  // `vfsPullReadStream` reads from `version_chunks` deterministically.
  if (row.head_version_id !== null) {
    return {
      fileId: row.file_id,
      size: row.head_size ?? 0,
      chunkCount: row.head_inline ? 0 : (row.head_chunk_count ?? 0),
      chunkSize: row.head_inline ? 0 : (row.head_chunk_size ?? 0),
      inlined: !!row.head_inline,
      versionId: row.head_version_id,
    };
  }
  return {
    fileId: row.file_id,
    size: row.file_size,
    chunkCount: row.inline_data ? 0 : row.chunk_count,
    chunkSize: row.inline_data ? 0 : row.chunk_size,
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
    // Yjs-mode handle carries pre-materialized bytes captured at
    // open-time. Serve directly without touching SQL.
    if (handle.inlineBytes !== undefined) {
      return range
        ? sliceWithRange(handle.inlineBytes, range)
        : handle.inlineBytes;
    }
    // Read inline bytes from the head version row when the
    // handle was opened for a versioned tenant; falls back to
    // legacy `files.inline_data` for non-versioned.
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
  // Typed `getChunkBytes` RPC. One IPC hop instead of the
  // two-await `stub.fetch(...).arrayBuffer()` pair.
  const shardNs = env.MOSSAIC_SHARD as unknown as DurableObjectNamespace<ShardDO>;
  const stub = shardNs.get(shardNs.idFromName(shardName));
  const buf = await stub.getChunkBytes(chunkRow.chunk_hash);
  if (buf === null) {
    throw new VFSError(
      "ENOENT",
      `pullReadStream: chunk data missing on shard ${chunkRow.shard_index}`
    );
  }
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
  const handle = await vfsOpenReadStream(durableObject, scope, path);
  const fileSize = handle.size;

  const start = clampOffset(range?.start ?? 0, fileSize);
  const end = clampOffset(range?.end ?? fileSize, fileSize);
  if (end < start) {
    throw new VFSError("EINVAL", `range end < start: [${start}, ${end})`);
  }

  // Empty-file fast path. A 0-byte file (legal: empty
  // writeFile, or the post-resurrection placeholder) MUST emit zero
  // chunks and close cleanly. Without this guard the chunked branch
  // below computes `Math.floor(start / 0) === NaN` and the stream
  // blocks forever or pulls a phantom chunk. Covers both
  //   - 0-byte inlined  (fileSize=0, inlined=true, inlineBytes empty)
  //   - 0-byte chunked  (fileSize=0, chunkCount=0, chunkSize=0)
  if (fileSize === 0 || end === start) {
    return new ReadableStream<Uint8Array>({
      pull: (ctrl) => {
        ctrl.close();
      },
    });
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

  // chunkSize is pinned on the handle at open-time (NOT re-queried
  // via `getChunkSizeForHandle` from the `files` row, which is
  // STALE for versioned tenants — root cause of a production
  // readPreview NaN bug). For non-empty chunked files
  // chunkSize MUST be >0; defend in depth so a corrupt schema row
  // surfaces as EINVAL rather than NaN-propagation.
  const chunkSize = handle.chunkSize;
  if (!Number.isFinite(chunkSize) || chunkSize <= 0) {
    throw new VFSError(
      "EINVAL",
      `createReadStream: invalid chunkSize ${chunkSize} for non-empty file ${path}`
    );
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

// `getChunkSizeForHandle` is intentionally absent. A naive
// re-query of `files.chunk_size` per createReadStream call is
// stale (0) on versioned tenants — `Math.floor(0/0) === NaN` was
// the production readPreview crash. Use `handle.chunkSize`,
// pinned at `vfsOpenReadStream` time from
// `version_chunks.chunk_size` (versioned) or `files.chunk_size`
// (legacy). Yjs / inlined / empty files carry chunkSize=0 and are
// routed away from chunk math entirely by the empty-file fast
// path above.

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
  validateEncryptionOpts(opts.encryption);
  enforceModeMonotonic(
    durableObject,
    userId,
    parentId,
    leaf,
    opts.encryption
  );

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
  transactionSync(durableObject, () => {
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
    durableObject.sql.exec(
      `INSERT INTO write_stream_sessions
         (tmp_id, user_id, parent_id, leaf, chunk_size, pool_size,
          metadata_present, metadata_blob, tags_json, version_label,
           version_user_visible, encryption_mode, encryption_key_id,
           status, inflight_index, inflight_hash, inflight_at, expires_at, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'open', NULL, NULL, NULL, ?, ?)`,
      tmpId,
      userId,
      parentId,
      leaf,
      defaultChunkSize,
      poolSize,
      metadataEncoded === undefined ? 0 : 1,
      metadataEncoded ?? null,
      opts.tags === undefined ? null : JSON.stringify([...opts.tags]),
      opts.version?.label ?? null,
      opts.version?.userVisible === undefined
        ? null
        : opts.version.userVisible
          ? 1
          : 0,
      opts.encryption?.mode ?? null,
      opts.encryption?.keyId ?? null,
      now + 60 * 60 * 1000,
      now
    );
  });
  return {
    tmpId,
    parentId,
    leaf,
    chunkSize: defaultChunkSize,
    poolSize,
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
  const session = readWriteStreamSession(durableObject, userId, handle);
  assertHandleMatchesSession(handle, session);
  // Verify handle still refers to an uploading row owned by this user.
  //
  // Server-authoritative pool size. The chunk PUT below MUST NOT
  // trust `handle.poolSize` directly. Handles are returned to the
  // SDK consumer at `vfsBeginWriteStream` and round-tripped on
  // every `vfsAppendWriteStream` call; a malicious or buggy
  // client could supply a tampered value (e.g. spoofed pool=64 to
  // spread chunks onto un-allocated shards, or pool=1 to
  // concentrate on a single shard). The tmp `files` row carries
  // the server's snapshotted `pool_size` from the begin-time
  // `vfsBeginWriteStream` call — read it from the row and use
  // that, not the handle's claim.
  const row = durableObject.sql
    .exec(
      `SELECT file_size, chunk_count, status, pool_size FROM files WHERE file_id=? AND user_id=?`,
      handle.tmpId,
      userId
    )
    .toArray()[0] as
    | {
        file_size: number;
        chunk_count: number;
        status: string;
        pool_size: number;
      }
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
  const reservedRow = transactionSync(durableObject, () => {
    const currentSession = readWriteStreamSession(durableObject, userId, handle);
    assertHandleMatchesSession(handle, currentSession);
    if (
      currentSession.status !== "open" ||
      (currentSession.inflight_index !== null &&
        (currentSession.inflight_index !== chunkIndex ||
          currentSession.inflight_hash !== hash))
    ) {
      throw new VFSError("EBUSY", "appendWriteStream: another append is in flight");
    }
    const current = durableObject.sql
      .exec(
        `SELECT file_size, chunk_count, status, pool_size
           FROM files WHERE file_id=? AND user_id=?`,
        handle.tmpId,
        userId
      )
      .toArray()[0] as
      | {
          file_size: number;
          chunk_count: number;
          status: string;
          pool_size: number;
        }
      | undefined;
    if (!current || current.status !== "uploading") {
      throw new VFSError("ENOENT", "appendWriteStream: stream is not open");
    }
    if (current.chunk_count !== chunkIndex) {
      throw new VFSError(
        "EINVAL",
        `appendWriteStream: out-of-order chunkIndex ${chunkIndex}, expected ${current.chunk_count}`
      );
    }
    durableObject.sql.exec(
      `UPDATE write_stream_sessions
          SET inflight_index = ?, inflight_hash = ?, inflight_at = ?
        WHERE tmp_id = ? AND user_id = ?
          AND status = 'open'
          AND (inflight_index IS NULL OR
               (inflight_index = ? AND inflight_hash = ?))`,
      chunkIndex,
      hash,
      Date.now(),
      handle.tmpId,
      userId,
      chunkIndex,
      hash
    );
    return current;
  });
  // Use server-recorded pool_size (not handle's claim). Honour
  // the skip-full-shard cache so a streaming append never sends
  // bytes to a near-cap shard. Pool growth on all-full is handled
  // in `vfsWriteFile`'s chunked tier; for the streaming path,
  // hitting POOL_FULL surfaces as an EBUSY (the SDK's append loop
  // will retry the chunk after
  // the next alarm refreshes the cache, by which point pool
  // growth in another write may have added headroom).
  const fullShards = loadFullShards(durableObject);
  const sIdx = placeChunk(
    userIdFor(scope),
    handle.tmpId,
    chunkIndex,
    reservedRow.pool_size,
    fullShards
  );
  if (sIdx === POOL_FULL) {
    throw new VFSError(
      "EBUSY",
      "appendWriteStream: every shard at soft cap; pool growth required"
    );
  }
  const env = durableObject.envPublic;
  const shardNs = env.MOSSAIC_SHARD as unknown as DurableObjectNamespace<ShardDO>;
  const shardName = vfsShardDOName(scope.ns, scope.tenant, scope.sub, sIdx);
  const stub = shardNs.get(shardNs.idFromName(shardName));
  await stageChunkCleanupIntents(durableObject, handle.tmpId, [sIdx]);
  await stub.putChunk(hash, data, handle.tmpId, chunkIndex, userId);

  const newSize = transactionSync(durableObject, () => {
    const currentSession = readWriteStreamSession(durableObject, userId, handle);
    if (
      currentSession.status !== "open" ||
      currentSession.inflight_index !== chunkIndex ||
      currentSession.inflight_hash !== hash
    ) {
      throw new VFSError("EBUSY", "appendWriteStream: append reservation changed");
    }
    const current = durableObject.sql
      .exec(
        `SELECT file_size, chunk_count, status FROM files
          WHERE file_id = ? AND user_id = ?`,
        handle.tmpId,
        userId
      )
      .toArray()[0] as
      | { file_size: number; chunk_count: number; status: string }
      | undefined;
    if (!current || current.status !== "uploading" || current.chunk_count !== chunkIndex) {
      throw new VFSError("EBUSY", "appendWriteStream: stream changed during append");
    }
    durableObject.sql.exec(
      `INSERT OR REPLACE INTO file_chunks
         (file_id, chunk_index, chunk_hash, chunk_size, shard_index)
       VALUES (?, ?, ?, ?, ?)`,
      handle.tmpId,
      chunkIndex,
      hash,
      data.byteLength,
      sIdx
    );
    const size = current.file_size + data.byteLength;
    durableObject.sql.exec(
      "UPDATE files SET file_size=?, chunk_count=?, updated_at=? WHERE file_id=?",
      size,
      current.chunk_count + 1,
      Date.now(),
      handle.tmpId
    );
    durableObject.sql.exec(
      `UPDATE write_stream_sessions
          SET inflight_index = NULL, inflight_hash = NULL, inflight_at = NULL,
              expires_at = ?
        WHERE tmp_id = ? AND user_id = ? AND inflight_index = ?`,
      Date.now() + 60 * 60 * 1000,
      handle.tmpId,
      userId,
      chunkIndex
    );
    return size;
  });
  return { bytesWritten: newSize };
}

/**
 * Commit a write stream: hash the recorded chunk hashes into a file
 * hash, then commit-rename the tmp row onto the target leaf via the
 * same supersede protocol as vfsWriteFile. The displaced row (if any)
 * is hard-deleted and its chunks are queued for GC.
 *
 * Commit options are captured and validated at begin-time, then applied in
 * the same local publication transaction as the rename or version head.
 */
export async function vfsCommitWriteStream(
  durableObject: UserDO,
  scope: VFSScope,
  handle: VFSWriteHandle
): Promise<void> {
  const userId = userIdFor(scope);
  const session = readWriteStreamSession(durableObject, userId, handle);
  assertHandleMatchesSession(handle, session);
  if (session.status !== "open" || session.inflight_index !== null) {
    throw new VFSError("EBUSY", "commitWriteStream: stream is not idle");
  }
  const row = durableObject.sql
    .exec(
      `SELECT status, parent_id, file_size, chunk_size, chunk_count,
              mime_type, mode
         FROM files WHERE file_id = ? AND user_id = ?`,
      handle.tmpId,
      userId
    )
    .toArray()[0] as
    | {
        status: string;
        parent_id: string | null;
        file_size: number;
        chunk_size: number;
        chunk_count: number;
        mime_type: string;
        mode: number;
      }
    | undefined;
  if (!row) {
    throw new VFSError("ENOENT", "commitWriteStream: handle not found");
  }
  if (row.status !== "uploading") {
    throw new VFSError(
      "EINVAL",
      `commitWriteStream: not in uploading state (status=${row.status})`
    );
  }
  if (row.parent_id !== session.parent_id) {
    throw new VFSError("EINVAL", "commitWriteStream: session parent changed");
  }
  const destinationRow = durableObject.sql
    .exec(
      `SELECT file_id, head_version_id FROM files
        WHERE user_id = ? AND IFNULL(parent_id, '') = IFNULL(?, '')
          AND file_name = ? AND status = 'complete'`,
      userId,
      session.parent_id,
      session.leaf
    )
    .toArray()[0] as
    | { file_id: string; head_version_id: string | null }
    | undefined;
  const co = commitOptionsFromSession(session);
  if (co?.metadataEncoded !== undefined && co.metadataEncoded !== null) {
    validateMetadata(JSON.parse(new TextDecoder().decode(co.metadataEncoded)));
  }
  if (co?.tags !== undefined) validateTags(co.tags);
  if (co?.versionLabel !== undefined) validateLabel(co.versionLabel);
  validateEncryptionOpts(co?.encryption);

  transactionSync(durableObject, () => {
    const current = readWriteStreamSession(durableObject, userId, handle);
    if (current.status !== "open" || current.inflight_index !== null) {
      throw new VFSError("EBUSY", "commitWriteStream: stream is not idle");
    }
    durableObject.sql.exec(
      `UPDATE write_stream_sessions SET status = 'committing'
        WHERE tmp_id = ? AND user_id = ? AND status = 'open'
          AND inflight_index IS NULL`,
      handle.tmpId,
      userId
    );
  });
  const assertStreamState = (): void => {
    const rows = durableObject.sql
      .exec(
        `SELECT 1 FROM files
          WHERE file_id = ? AND user_id = ? AND status = 'uploading'
            AND IFNULL(parent_id, '') = IFNULL(?, '')
            AND file_size = ? AND chunk_count = ?`,
        handle.tmpId,
        userId,
        session.parent_id,
        row.file_size,
        row.chunk_count
      )
      .toArray();
    if (rows.length !== 1) {
      throw new VFSError(
        "EBUSY",
        "commitWriteStream: stream changed during publication"
      );
    }
    const current = readWriteStreamSession(durableObject, userId, handle);
    if (current.status !== "committing" || current.inflight_index !== null) {
      throw new VFSError(
        "EBUSY",
        "commitWriteStream: session changed during publication"
      );
    }
  };
  const chunkHashes = durableObject.sql
    .exec(
      "SELECT chunk_hash FROM file_chunks WHERE file_id=? ORDER BY chunk_index",
      handle.tmpId
    )
    .toArray() as { chunk_hash: string }[];
  const fileHash = await hashChunk(
    new TextEncoder().encode(chunkHashes.map((c) => c.chunk_hash).join(""))
  );

  enforceModeMonotonic(
    durableObject,
    userId,
    session.parent_id,
    session.leaf,
    co?.encryption
  );

  // Commit-write-stream × versioning. A naive call to
  // `commitRename` here would, under versioning ON, hard-delete
  // any prior live row at the target — destroying its history
  // (same bug class as multipart finalize). Instead, under
  // versioning ON, route through `commitVersion` analogously to
  // multipart-versioned-finalize. The chunks already live on
  // ShardDOs under refId=tmpId; we mirror them into version_chunks
  // and stamp shard_ref_id=tmpId so the future dropVersionRows
  // fan-out finds them.
  const versioning = isVersioningEnabled(durableObject, userId);

  if (versioning) {
    const pathId = destinationRow?.file_id ?? handle.tmpId;

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
    const versionId = generateId();
    const committedAt = Date.now();
    const metadataForVersion =
      co?.metadataEncoded !== undefined
        ? co.metadataEncoded
        : destinationRow
          ? readMetadataBytes(durableObject, pathId)
          : null;
    const expectedHead: VersionedFileExpectation = {
      fileId: pathId,
      userId,
      parentId: session.parent_id,
      fileName: session.leaf,
      headVersionId: destinationRow?.head_version_id ?? null,
    };
    const finalizeVersion = (): void => {
      for (const chunk of tmpChunks) {
        insertVersionChunk(durableObject, versionId, chunk);
      }
      applyStreamCommitSideEffects(
        durableObject,
        userId,
        pathId,
        co,
        committedAt,
        false
      );
      commitVersionChecked(
        durableObject,
        {
          pathId,
          versionId,
          userId,
          size: row.file_size,
          mode: row.mode,
          mtimeMs: committedAt,
          chunkSize: row.chunk_size,
          chunkCount: row.chunk_count,
          fileHash,
          mimeType: row.mime_type,
          inlineData: null,
          userVisible: co?.versionUserVisible ?? true,
          label: co?.versionLabel ?? null,
          metadata: metadataForVersion,
          encryption: co?.encryption,
          shardRefId: handle.tmpId,
        },
        expectedHead,
        "commitWriteStream"
      );
      if (row.chunk_count > 0) {
        disarmChunkCleanupIntents(durableObject, handle.tmpId);
      }
    };

    try {
      if (destinationRow) {
        await scheduleStaleUploadSweep(durableObject);
        transactionSync(durableObject, () => {
          assertStreamState();
          finalizeVersion();
          dropTmpRowAfterVersionCommit(durableObject, handle.tmpId, {
            hasChunks: true,
          });
          durableObject.sql.exec(
            "DELETE FROM write_stream_sessions WHERE tmp_id = ?",
            handle.tmpId
          );
          bumpFolderRevision(durableObject, userId, session.parent_id);
        });
      } else {
        await commitRename(
          durableObject,
          userId,
          scope,
          handle.tmpId,
          session.parent_id,
          session.leaf,
          {
            requireVacantDestination: true,
            preconditionLocal: assertStreamState,
            publicationEncryption: co?.encryption ?? null,
            finalizeLocal: finalizeVersion,
          }
        );
      }
    } catch (err) {
      await abortTempFile(durableObject, userId, scope, handle.tmpId);
      throw err;
    }
    return;
  }

  // Versioning OFF: side effects and positive accounting publish with the
  // rename. The versioning branch accounts through commitVersion.
  await commitRename(
    durableObject,
    userId,
    scope,
    handle.tmpId,
        session.parent_id,
        session.leaf,
    {
      requireVacantDestination: destinationRow === undefined,
      expectedDestination: destinationRow
        ? {
            fileId: destinationRow.file_id,
            headVersionId: destinationRow.head_version_id,
          }
        : undefined,
      preconditionLocal: assertStreamState,
      publicationEncryption: co?.encryption ?? null,
      finalizeLocal: () => {
        durableObject.sql.exec(
          "UPDATE files SET file_hash = ? WHERE file_id = ?",
          fileHash,
          handle.tmpId
        );
        applyStreamCommitSideEffects(
          durableObject,
          userId,
          handle.tmpId,
          co,
          Date.now(),
          false
        );
        const sizeRow = durableObject.sql
          .exec(
            "SELECT file_size FROM files WHERE file_id = ?",
            handle.tmpId
          )
          .toArray()[0] as { file_size: number } | undefined;
        if (sizeRow) {
          recordWriteUsage(durableObject, userId, sizeRow.file_size, 1);
        }
        if (row.chunk_count > 0) {
          disarmChunkCleanupIntents(durableObject, handle.tmpId);
        }
        durableObject.sql.exec(
          "DELETE FROM write_stream_sessions WHERE tmp_id = ?",
          handle.tmpId
        );
      },
    }
  );
}

function applyStreamCommitSideEffects(
  durableObject: UserDO,
  userId: string,
  pathId: string,
  opts: WriteStreamCommitOptions | undefined,
  mtimeMs: number,
  bumpEncryptionRevision: boolean
): void {
  if (opts?.metadataEncoded !== undefined) {
    durableObject.sql.exec(
      "UPDATE files SET metadata = ?, updated_at = ? WHERE file_id = ?",
      opts.metadataEncoded,
      mtimeMs,
      pathId
    );
  }
  if (opts?.tags !== undefined) {
    replaceTags(durableObject, userId, pathId, opts.tags);
  } else {
    bumpTagMtimes(durableObject, pathId, mtimeMs);
  }
  if (opts?.encryption !== undefined) {
    stampFileEncryption(
      durableObject,
      pathId,
      opts.encryption,
      bumpEncryptionRevision ? userId : undefined
    );
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
  const session = durableObject.sql
    .exec(
      `SELECT parent_id, leaf, chunk_size, pool_size, metadata_present,
              metadata_blob, tags_json, version_label, version_user_visible,
              encryption_mode, encryption_key_id, status, inflight_index,
              inflight_hash, inflight_at, expires_at
         FROM write_stream_sessions WHERE tmp_id = ? AND user_id = ?`,
      handle.tmpId,
      userId
    )
    .toArray()[0] as WriteStreamSessionRow | undefined;
  if (session) {
    assertHandleMatchesSession(handle, session);
    if (session.status !== "open" || session.inflight_index !== null) {
      throw new VFSError("EBUSY", "abortWriteStream: stream is not idle");
    }
  }
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
