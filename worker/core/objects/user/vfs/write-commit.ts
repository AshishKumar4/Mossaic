import type { UserDOCore as UserDO } from "../user-do-core";
import type { ShardDO } from "../../shard/shard-do";
import {
  VFSError,
  type VFSScope,
} from "../../../../../shared/vfs-types";
import {
  INLINE_LIMIT,
  INLINE_TIER_CAP,
  WRITEFILE_MAX,
} from "../../../../../shared/inline";
import { hashChunk } from "../../../../../shared/crypto";
import { computeChunkSpec } from "../../../../../shared/chunking";
import { generateId, vfsShardDOName } from "../../../lib/utils";
import { placeChunk, POOL_FULL } from "../../../../../shared/placement";
import { loadFullShards } from "../shard-capacity";
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
  recordWriteUsage,
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
  // Phase 32 Fix 5 — inline-tier accounting.
  //
  // Read `(status, inline_data)` BEFORE the delete cascade so we
  // can decrement `quota.inline_bytes_used` after the row goes
  // away. Inline-tier graceful migration depends on this counter
  // being accurate (the cap fires the moment a tenant crosses
  // INLINE_TIER_CAP, and not before). Plain `storage_used` /
  // `file_count` decrement is deferred to Phase 32.6 — pool growth
  // is monotonic by design (Lean invariant), so the cosmetic
  // inflation of `storage_used` doesn't impact scaling
  // correctness. Inline-bytes accounting MUST be balanced because
  // the inline tier cap is small (1 GiB) and the rounding error
  // would dominate (each overwrite cycle would inflate by file_size).
  //
  // The `status='uploading'` branch (tmp-row reaper sweeps;
  // multipart-abort) was never accounted as a positive delta in
  // `commitInlineTier`, so we do NOT decrement it. The
  // `'complete'` and `'deleted'` (post-supersede) statuses both
  // decrement \u2014 see Phase 32.5 BUG #1 fix at the gate below.
  const accountingRow = durableObject.sql
    .exec(
      "SELECT status, inline_data FROM files WHERE file_id = ?",
      fileId
    )
    .toArray()[0] as
    | { status: string; inline_data: ArrayBuffer | null }
    | undefined;

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

  // Phase 32 Fix 5 — decrement inline-tier counter on inline-row
  // deletes. Plain storage_used / file_count are NOT decremented
  // here (Phase 32.6 follow-up); they're cosmetic and unrelated
  // to the inline-tier cap.
  //
  // Phase 32.5 BUG #1 fix — gate is `status !== 'uploading'`, NOT
  // `status === 'complete'`. The two callers that drive the
  // overwrite/rename flow (`commitRename` write-commit.ts:1112,
  // `vfsRename` mutations.ts:498-499) flip status to `'deleted'`
  // BEFORE invoking hardDeleteFileRow on the displaced row. Under
  // the previous `status === 'complete'` gate the inline-bytes
  // decrement was skipped on every overwrite — `inline_bytes_used`
  // monotonically inflated by `file_size` per overwrite cycle, so
  // INLINE_TIER_CAP fired earlier than 1 GiB.
  //
  // What the gate must exclude is the `'uploading'` status \u2014 tmp
  // rows reaped by the stale-upload sweeper / multipart-abort were
  // never positive-counted by `commitInlineTier` (chunked writes
  // always start as `'uploading'` and the inline tier never enters
  // an `'uploading'` state for the file_id it commits). The
  // post-supersede `'deleted'` and the post-commit `'complete'`
  // statuses are both legitimately positive-counted, so both
  // decrement.
  //
  // Symmetry guarantee: every code path that flows positive bytes
  // into `inline_bytes_used` via `commitInlineTier` (write-commit.ts:704)
  // OR via the direct UPDATE in `vfsWriteFileVersioned` inline branch
  // (write-commit.ts:364-370) writes `inline_data IS NOT NULL` AND
  // commits the row to `'complete'` first. From that point the only
  // way the row can disappear is through hardDeleteFileRow (this
  // function) \u2014 either directly (rmrf, unlink) or via a supersede
  // (commitRename, vfsRename) that flips `'complete' \u2192 'deleted'`
  // immediately before the call. Both pre-flip statuses now
  // decrement.
  if (
    accountingRow &&
    accountingRow.status !== "uploading" &&
    accountingRow.inline_data
  ) {
    recordWriteUsage(
      durableObject,
      userId,
      0,
      0,
      -accountingRow.inline_data.byteLength
    );
  }

  // Then dispatch one deleteChunks RPC per touched shard.
  const env = durableObject.envPublic;
  // Env.MOSSAIC_SHARD is the un-parameterized DurableObjectNamespace; cast to
  // the typed namespace so the .deleteChunks RPC method is visible.
  // Double cast (via `unknown`) because TS treats the un-parameterized
  // form as DurableObjectNamespace<undefined> which doesn't structurally
  // overlap with the typed form.
  const shardNs = env.MOSSAIC_SHARD as unknown as DurableObjectNamespace<ShardDO>;
  for (const { shard_index } of shardRows) {
    const shardName = vfsShardDOName(scope.ns, scope.tenant, scope.sub, shard_index);
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
  //
  // Phase 32 Fix 5 — same graceful-migration cap as the
  // non-versioned path. Versioned tenants doing many tiny writes
  // accumulate `file_versions.inline_data` BLOBs on the UserDO
  // and hit the same SQLite ceiling. The cap also applies here.
  if (data.byteLength <= INLINE_LIMIT) {
    const inlineUsed = (
      durableObject.sql
        .exec(
          "SELECT COALESCE(inline_bytes_used, 0) AS used FROM quota WHERE user_id = ?",
          userId
        )
        .toArray()[0] as { used: number } | undefined
    )?.used ?? 0;
    if (inlineUsed + data.byteLength <= INLINE_TIER_CAP) {
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
      // Bump the inline counter for this versioned write — the
      // standard write-commit path's `commitInlineTier` does the
      // same via `recordWriteUsage(..., data.byteLength)`. Here
      // we tick it directly because the versioned commit doesn't
      // route through `recordWriteUsage` for byte accounting (it
      // uses `commitVersion` for the row, see Phase 27 design).
      // Sub-quota cosmetic for this phase; full versioned-bytes
      // accounting is Phase 32.5.
      durableObject.sql.exec(
        `UPDATE quota
            SET inline_bytes_used = COALESCE(inline_bytes_used, 0) + ?
          WHERE user_id = ?`,
        data.byteLength,
        userId
      );
      if (meta.tags !== undefined) {
        replaceTags(durableObject, userId, pathId, meta.tags);
      } else {
        // Bump tag mtimes so list-by-tag reflects this write's recency.
        bumpTagMtimes(durableObject, pathId, now);
      }
      return;
    }
    // Spill to chunked tier; same first-crossing warning shape.
    if (inlineUsed < INLINE_TIER_CAP) {
      console.warn(
        JSON.stringify({
          event: "inline_tier_cap_first_crossing",
          tenant: scope.tenant,
          ns: scope.ns,
          sub: scope.sub,
          versioned: true,
          inlineBytesUsed: inlineUsed,
          capBytes: INLINE_TIER_CAP,
          incomingByteLength: data.byteLength,
        })
      );
    }
    // fall through to chunked-tier branch below
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

/**
 * Resolved + validated write-commit input. Produced by
 * {@link prepareWriteCommit} and consumed by both
 * {@link commitInlineTier} and {@link commitChunkedTier}.
 *
 * All input validation has already happened by the time a `WriteCommitPlan`
 * exists — so `executeWriteCommit` can focus on storage-layer ordering
 * (insert tmp row → push chunks → commitRename → post-commit side
 * effects) without re-validating.
 */
export interface WriteCommitPlan {
  userId: string;
  parentId: string | null;
  leaf: string;
  mode: number;
  mimeType: string;
  /** Pre-validated, encoded metadata blob (or undefined for "no change"). */
  metadataEncoded: Uint8Array | null | undefined;
  /** Pre-validated tag set (undefined → unchanged). */
  tags: readonly string[] | undefined;
  /** Optional encryption stamp (mode-history monotonicity already enforced). */
  encryption: { mode: "convergent" | "random"; keyId?: string } | undefined;
  /** Wall-clock millisecond timestamp captured before any SQL touches the row. */
  now: number;
}

/**
 * Validate caller-supplied write opts and resolve the canonical target.
 *
 * Throws VFSError BEFORE any SQL touches the row:
 * - `EFBIG` if `data` exceeds WRITEFILE_MAX.
 * - `EISDIR` if the target path is a directory.
 * - `EINVAL` from metadata/tags/version validators on cap violation.
 * - `EBADF` from `enforceModeMonotonic` when the encryption mode
 *   disagrees with the existing path's history.
 *
 * Returns a {@link WriteCommitPlan} that the tier-specific commit
 * helpers consume. Idempotent: calling twice with the same args
 * resolves the same plan (modulo `now`).
 */
async function prepareWriteCommit(
  durableObject: UserDO,
  scope: VFSScope,
  path: string,
  byteLength: number,
  opts: VFSWriteFileOpts
): Promise<WriteCommitPlan> {
  const userId = userIdFor(scope);
  const { parentId, leaf } = resolveParent(durableObject, userId, path);

  if (byteLength > WRITEFILE_MAX) {
    throw new VFSError(
      "EFBIG",
      `writeFile: ${byteLength} > WRITEFILE_MAX ${WRITEFILE_MAX}`
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

  return {
    userId,
    parentId,
    leaf,
    mode: opts.mode ?? 0o644,
    mimeType: opts.mimeType ?? "application/octet-stream",
    metadataEncoded,
    tags: opts.tags,
    encryption: opts.encryption,
    now: Date.now(),
  };
}

/**
 * Execute the inline-tier commit: insert a tmp `files` row carrying the
 * full payload in `inline_data`, atomically rename it to the target
 * leaf via {@link commitRename}, then apply post-commit side effects
 * (metadata, tags, encryption stamp).
 *
 * Two-phase commit pattern: insert with tmp name first so concurrent
 * readers either see the prior file or the new one — never a
 * half-formed inline_data row at the live name.
 */
async function commitInlineTier(
  durableObject: UserDO,
  scope: VFSScope,
  plan: WriteCommitPlan,
  data: Uint8Array
): Promise<void> {
  const tmpId = generateId();
  const tmpName = `_vfs_tmp_${tmpId}`;
  durableObject.sql.exec(
    `INSERT INTO files (file_id, user_id, parent_id, file_name, file_size, file_hash, mime_type, chunk_size, chunk_count, pool_size, status, created_at, updated_at, mode, node_kind, inline_data)
     VALUES (?, ?, ?, ?, ?, '', ?, 0, 0, ?, 'uploading', ?, ?, ?, 'file', ?)`,
    tmpId,
    plan.userId,
    plan.parentId,
    tmpName,
    data.byteLength,
    plan.mimeType,
    poolSizeFor(durableObject, plan.userId),
    plan.now,
    plan.now,
    plan.mode,
    data
  );
  // H1: schedule the stale-upload sweep so this row is reclaimed
  // even if commitRename never runs (DO crash mid-method).
  await durableObject.scheduleStaleUploadSweep();
  await commitRename(
    durableObject,
    plan.userId,
    scope,
    tmpId,
    plan.parentId,
    plan.leaf
  );
  await applyPhase12SideEffects(
    durableObject,
    plan.userId,
    tmpId,
    plan.metadataEncoded,
    plan.tags,
    plan.now,
    plan.encryption
  );
  // Record bytes against quota + grow pool size if we crossed a 5 GB
  // boundary. Inline tier is always small (≤16 KB) so growth here is
  // accounting-only — but it MUST run because the tenant's first
  // write might be inline + push file_count from 0 → 1.
  //
  // Phase 32 Fix 5 — also bump `quota.inline_bytes_used` so the
  // inline-tier cap can fire on subsequent writes. The chunked
  // tier passes 0 for this delta (default).
  recordWriteUsage(
    durableObject,
    plan.userId,
    data.byteLength,
    1,
    data.byteLength
  );
}

/**
 * Execute the chunked-tier commit: chunk `data`, fan out PUTs to
 * ShardDOs with bounded concurrency (8 lanes), record `file_chunks`,
 * stamp `file_hash`, atomically rename, apply post-commit side
 * effects.
 *
 * H3: parallel chunk PUTs. Concurrency cap = 8 (same rationale as the
 * read path: stays well inside the Workers concurrent-subrequest limit
 * and saturates typical bandwidth). Per-chunk file_chunks INSERT is
 * sync SQL inside the DO single-thread so SQL ordering is preserved
 * without coordination.
 *
 * On any throw mid-stream, {@link abortTempFile} reclaims the tmp row
 * + already-pushed chunks so we never leak storage on a failed write.
 */
async function commitChunkedTier(
  durableObject: UserDO,
  scope: VFSScope,
  plan: WriteCommitPlan,
  data: Uint8Array
): Promise<void> {
  const { chunkSize, chunkCount } = computeChunkSpec(data.byteLength);
  const tmpId = generateId();
  const tmpName = `_vfs_tmp_${tmpId}`;
  let poolSize = poolSizeFor(durableObject, plan.userId);

  // Phase 32 Fix 4 \u2014 load the skip-set once per write batch.
  // Cold cache (empty Set) = byte-equivalent to pre-Phase-32
  // placement (deterministic top-1).
  let fullShards = loadFullShards(durableObject);

  // If every shard in the pool is full, force a pool-size bump
  // BEFORE the tmp row insert so the row records the post-growth
  // pool. We trigger growth by a 5 GiB \"phantom\" delta: it
  // doesn't change `storage_used`'s ground truth (we pass 0 for
  // bytes) but it forces the pool-size recompute. The simpler
  // alternative \u2014 directly UPDATE quota.pool_size += 1 \u2014
  // bypasses Lean's monotonicity invariant proof; using
  // recordWriteUsage keeps the proof trivially valid because
  // writes only ever grow the pool.
  if (fullShards.size >= poolSize) {
    // Bump the pool. We add `BYTES_PER_SHARD` to storage_used
    // virtually, then immediately consume the headroom \u2014 but
    // since recordWriteUsage caps the recomputation to
    // `BASE_POOL + floor(storage_used / BYTES_PER_SHARD)`, this
    // grows pool_size by at most 1. After the bump, the new
    // shard is non-full (it's empty) and placement succeeds.
    durableObject.sql.exec(
      `UPDATE quota
          SET pool_size = pool_size + 1
        WHERE user_id = ?`,
      plan.userId
    );
    poolSize = poolSize + 1;
    // Re-read \u2014 the new shard is not in the cache so it's
    // implicitly non-full.
    fullShards = loadFullShards(durableObject);
    console.warn(
      JSON.stringify({
        event: "pool_growth_forced_by_full_shards",
        tenant: scope.tenant,
        ns: scope.ns,
        sub: scope.sub,
        newPoolSize: poolSize,
        fullShardCount: fullShards.size,
      })
    );
  }

  durableObject.sql.exec(
    `INSERT INTO files (file_id, user_id, parent_id, file_name, file_size, file_hash, mime_type, chunk_size, chunk_count, pool_size, status, created_at, updated_at, mode, node_kind)
     VALUES (?, ?, ?, ?, ?, '', ?, ?, ?, ?, 'uploading', ?, ?, ?, 'file')`,
    tmpId,
    plan.userId,
    plan.parentId,
    tmpName,
    data.byteLength,
    plan.mimeType,
    chunkSize,
    chunkCount,
    poolSize,
    plan.now,
    plan.now,
    plan.mode
  );
  await durableObject.scheduleStaleUploadSweep();

  const env = durableObject.envPublic;
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
      const sIdx = placeChunk(userIdFor(scope), tmpId, i, poolSize, fullShards);
      if (sIdx === POOL_FULL) {
        // The pool-grow above already handled the all-full case;
        // hitting POOL_FULL here means the cache changed under us
        // (concurrent alarm refresh marked another shard full
        // mid-upload). Surface as EBUSY so the SDK retries.
        throw new VFSError(
          "EBUSY",
          "writeFile: every shard at soft cap; pool growth required"
        );
      }
      const shardName = vfsShardDOName(scope.ns, scope.tenant, scope.sub, sIdx);
      const stub = shardNs.get(shardNs.idFromName(shardName));
      await stub.putChunk(hash, slice, tmpId, i, plan.userId);
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
    await abortTempFile(durableObject, plan.userId, scope, tmpId);
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

  await commitRename(
    durableObject,
    plan.userId,
    scope,
    tmpId,
    plan.parentId,
    plan.leaf
  );
  await applyPhase12SideEffects(
    durableObject,
    plan.userId,
    tmpId,
    plan.metadataEncoded,
    plan.tags,
    plan.now,
    plan.encryption
  );
  // Record bytes against quota + grow pool size on 5 GB boundary
  // crossings. The next write (or the next chunk PUT in a brand-new
  // multipart session — chunk PUTs in the SAME session use the
  // token-frozen poolSize by design) will see the larger pool.
  recordWriteUsage(durableObject, plan.userId, data.byteLength, 1);
}

export async function vfsWriteFile(
  durableObject: UserDO,
  scope: VFSScope,
  path: string,
  data: Uint8Array,
  opts: VFSWriteFileOpts = {}
): Promise<void> {
  const plan = await prepareWriteCommit(
    durableObject,
    scope,
    path,
    data.byteLength,
    opts
  );

  // yjs-mode fork. If the target file already exists with mode_yjs=1,
  // route the bytes through YjsRuntime: the data becomes the new value
  // of Y.Text("content") under origin "writeFile". Versioning fork is
  // bypassed — the yjs op log IS the history; explicit checkpoints
  // come from compaction (not from writeFile).
  const yjsRow = durableObject.sql
    .exec(
      `SELECT file_id, mode_yjs FROM files
         WHERE user_id=? AND IFNULL(parent_id,'')=IFNULL(?,'')
           AND file_name=? AND status='complete'`,
      plan.userId,
      plan.parentId,
      plan.leaf
    )
    .toArray()[0] as { file_id: string; mode_yjs: number } | undefined;
  if (yjsRow && yjsRow.mode_yjs === 1) {
    const { writeYjsBytes } = await import("../yjs");
    await writeYjsBytes(
      durableObject,
      scope,
      plan.userId,
      yjsRow.file_id,
      poolSizeFor(durableObject, plan.userId),
      data
    );
    await applyPhase12SideEffects(
      durableObject,
      plan.userId,
      yjsRow.file_id,
      plan.metadataEncoded,
      plan.tags,
      Date.now(),
      plan.encryption
    );
    return;
  }

  // Versioning fork. With versioning ON, every writeFile creates a
  // new file_versions row + per-version synthetic shard key; the
  // `files` row is just the stable identity holding the head pointer.
  if (isVersioningEnabled(durableObject, plan.userId)) {
    return vfsWriteFileVersioned(
      durableObject,
      scope,
      plan.userId,
      plan.parentId,
      plan.leaf,
      data,
      plan.mode,
      plan.mimeType,
      plan.now,
      {
        metadataEncoded: plan.metadataEncoded,
        tags: plan.tags,
        versionUserVisible: opts.version?.userVisible ?? true,
        versionLabel: opts.version?.label,
        encryption: plan.encryption,
      }
    );
  }

  // Tier dispatch. The inline tier embeds bytes in `files.inline_data`
  // (≤ INLINE_LIMIT); the chunked tier fans out to ShardDOs via
  // bounded-concurrency PUTs.
  //
  // Phase 32 Fix 5 — graceful migration. A tenant approaching the
  // INLINE_TIER_CAP (1 GiB cumulative inline bytes) spills NEW
  // tiny writes to the chunked tier instead of further loading
  // the UserDO's SQLite. Pre-existing inline rows are read
  // identically by `vfsReadFile` (it checks `inline_data IS NOT
  // NULL` first); the cap is a write-side gate, not a read-side
  // migration. `quota.inline_bytes_used` is maintained by
  // `recordWriteUsage`'s `deltaInlineBytes` parameter; cold
  // tenants (`COALESCE(NULL, 0)`) start at 0 and inline freely.
  if (data.byteLength <= INLINE_LIMIT) {
    const inlineUsed = (
      durableObject.sql
        .exec(
          "SELECT COALESCE(inline_bytes_used, 0) AS used FROM quota WHERE user_id = ?",
          plan.userId
        )
        .toArray()[0] as { used: number } | undefined
    )?.used ?? 0;
    if (inlineUsed + data.byteLength <= INLINE_TIER_CAP) {
      return commitInlineTier(durableObject, scope, plan, data);
    }
    // Spill to chunked tier; the per-write structured warning lets
    // operators see when a tenant first crosses the cap.
    if (inlineUsed < INLINE_TIER_CAP) {
      console.warn(
        JSON.stringify({
          event: "inline_tier_cap_first_crossing",
          tenant: scope.tenant,
          ns: scope.ns,
          sub: scope.sub,
          inlineBytesUsed: inlineUsed,
          capBytes: INLINE_TIER_CAP,
          incomingByteLength: data.byteLength,
        })
      );
    }
  }
  return commitChunkedTier(durableObject, scope, plan, data);
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
