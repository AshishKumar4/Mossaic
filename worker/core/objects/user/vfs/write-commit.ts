import type { UserDOCore as UserDO } from "../user-do-core";
import type { ShardDO } from "../../shard/shard-do";
import {
  VFSError,
  type VFSScope,
} from "../../../../../shared/vfs-types";
import { INLINE_LIMIT, WRITEFILE_MAX } from "../../../../../shared/inline";
import { hashChunk } from "../../../../../shared/crypto";
import { computeChunkSpec } from "../../../../../shared/chunking";
// placement is resolved via `getPlacement(scope)` (already
// imported above); no direct `placeChunk` import needed.
import { generateId } from "../../../lib/utils";
import { getPlacement } from "../../../lib/placement-resolver";
import {
  commitVersion,
  isVersioningEnabled,
  placeChunkForVersion,
  shardRefId,
} from "../vfs-versions";
import {
  validateLabel,
  validateMetadata,
  validateTags,
} from "../../../../../shared/metadata-validate";
import { bumpTagMtimes, replaceTags } from "../metadata-tags";
import {
  findLiveFile,
  folderExists,
  poolSizeFor,
  resolveParent,
  userIdFor,
} from "./helpers";

/**
 * Top-level write + commit protocol.
 *
 * `vfsWriteFile` is the user-facing entry point for atomic file
 * writes; `commitRename`, `abortTempFile`, and `hardDeleteFileRow`
 * constitute the atomic-write protocol modeled by
 * `Mossaic.Vfs.AtomicWrite` in Lean. They are co-located here
 * because `vfsWriteFile` is the protocol's defining caller; other
 * callers (write-streams, mutations, multipart-upload, copy-file)
 * are derivative.
 *
 * All writes go through one of three shapes:
 *   1. Inline: file ≤ INLINE_LIMIT → single UPDATE on `files`, no shards.
 *   2. Chunked: hash + place + putChunk RPC per chunk + recordChunk row +
 *      single commit-rename UPDATE.
 *   3. Folder/symlink/rename/chmod: pure SQL on the UserDO (in sibling
 *      modules).
 *
 * Atomicity is delivered by:
 *   - DO single-threaded fetch handler / RPC method ⇒ each method body is
 *     its own transaction
 *   - UNIQUE partial index on (user_id, parent_id, file_name)
 *     WHERE status != 'deleted' ⇒ concurrent writers see each other; the
 *     loser of a commit race fails INSERT/UPDATE and we surface EBUSY
 *     after a bounded retry
 *   - Temp-id-then-rename for writeFile ⇒ a partially-written tmp row
 *     never shadows the live file_name; readFile of the path returns the
 *     prior content until commit flips status='complete'
 *
 * GC: hard-delete files+file_chunks rows in the UserDO; queue chunk
 * reference decrements on each touched ShardDO via the typed deleteChunks
 * RPC. ShardDO's alarm sweeper performs the actual blob delete after the
 * 30s grace window (+ plumbing).
 */

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
export async function hardDeleteFileRow(
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
    const shardName = getPlacement(scope).shardDOName(scope, shard_index);
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
      replaceTags(durableObject, userId, pathId, meta.tags);
    } else {
      // Bump tag mtimes so list-by-tag reflects this write's recency.
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
      const sIdx = placeChunkForVersion(durableObject, scope, hash, poolSize);
      const shardName = getPlacement(scope).shardDOName(scope, sIdx);
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
      const shardName = getPlacement(scope).shardDOName(scope, sIdx);
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
    replaceTags(durableObject, userId, pathId, meta.tags);
  } else {
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
    metadataEncoded = validateMetadata(opts.metadata).encoded;
  }
  if (opts.tags !== undefined) {
    validateTags(opts.tags);
  }
  if (opts.version?.label !== undefined) {
    validateLabel(opts.version.label);
  }

  // validate encryption opts shape and enforce mode-history
  // monotonicity. Both checks throw VFSError before any SQL touches
  // the row, so a rejected write leaves the existing path untouched.
  if (opts.encryption) {
    const { validateEncryptionOpts, enforceModeMonotonic } = await import(
      "../encryption-stamp"
    );
    validateEncryptionOpts(opts.encryption);
    enforceModeMonotonic(durableObject, userId, parentId, leaf, opts.encryption);
  } else {
    // Plaintext write: still need to check we're not silently writing
    // plaintext to an encrypted path.
    const { enforceModeMonotonic } = await import("../encryption-stamp");
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
      const { writeYjsBytes } = await import("../yjs");
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
      const sIdx = getPlacement(scope).placeChunk(scope, tmpId, i, poolSize);
      const shardName = getPlacement(scope).shardDOName(scope, sIdx);
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
    replaceTags(durableObject, userId, pathId, tags);
  } else {
    bumpTagMtimes(durableObject, pathId, mtimeMs);
  }
  if (encryption !== undefined) {
    const { stampFileEncryption } = await import("../encryption-stamp");
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
export async function commitRename(
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
export async function abortTempFile(
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
