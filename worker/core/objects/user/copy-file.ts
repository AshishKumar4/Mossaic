/**
 * server-side copyFile primitive.
 *
 * Three-tier impl mirrors writeFile:
 *   - Inline-tier: src has `inline_data`. Bytes-only copy. ZERO
 *     shard work.
 *   - Versioned (versioning ON for the tenant): create a fresh
 *     `files` row at dest, then a new `file_versions` row whose
 *     `version_chunks` rows mirror the src head version's chunks.
 *     Refs are bumped via `putChunk(empty)` per chunk; the dedup
 *     branch in `writeChunkInternal` does the right thing —
 *     INSERT OR IGNORE chunk_refs + ref_count++.
 *   - Chunked (versioning OFF): same as versioned but writes to
 *     `file_chunks` under a new file_id.
 *   - yjs-mode src: materialize via `readYjsAsBytes`, then route
 *     through `vfsWriteFile`. Documented bytes-snapshot semantics
 *     — dest does NOT share src's CRDT op log.
 *
 * Refcount invariant (Lean §7.2): for every unique chunk_hash in
 * src's manifest, dest gets +1 ref. Source's refs are unchanged.
 *
 * Atomicity: dest slot is claimed via the same commitRename
 * protocol as writeFile. If the chunk fan-out fails halfway, the
 * stale-upload sweeper reaps the tmp + dispatches deleteChunks to
 * unwind any landed ref bumps.
 */

import type { UserDOCore as UserDO } from "./user-do-core";
import type { ShardDO } from "../shard/shard-do";
import { VFSError, type VFSScope } from "../../../../shared/vfs-types";
import { generateId, vfsShardDOName } from "../../lib/utils";
import {
  commitVersion,
  dropTmpRowAfterVersionCommit,
  insertVersionChunk,
  isVersioningEnabled,
  shardRefId,
} from "./vfs-versions";
import { findLiveFile } from "./vfs/helpers";

interface CopyOpts {
  metadataEncoded?: Uint8Array | null | undefined;
  tags?: readonly string[] | undefined;
  versionUserVisible?: boolean;
  versionLabel?: string;
  overwrite: boolean;
}

export async function vfsCopyFile(
  durableObject: UserDO,
  scope: VFSScope,
  src: string,
  dest: string,
  opts: {
    metadata?: Record<string, unknown> | null;
    tags?: readonly string[];
    version?: { label?: string; userVisible?: boolean };
    overwrite?: boolean;
  } = {}
): Promise<void> {
  if (src === dest) {
    throw new VFSError("EINVAL", "copyFile: src and dest are identical");
  }

  // Local imports to keep the module's import graph cheap on
  // tenants that never call copyFile.
  const opsMod = await import("./vfs-ops");
  const pathMod = await import("./path-walk");
  const yjsMod = await import("./yjs");
  const tagsMod = await import("./metadata-tags");

  const userId = ((): string => {
    if (scope.sub !== undefined) return `${scope.tenant}::${scope.sub}`;
    return scope.tenant;
  })();

  // Resolve src — follow symlinks; reject directories.
  const srcRes = pathMod.resolvePathFollow(durableObject, userId, src);
  if (srcRes.kind === "ENOENT") {
    throw new VFSError("ENOENT", `copyFile: source not found: ${src}`);
  }
  if (srcRes.kind === "ENOTDIR" || srcRes.kind === "ELOOP") {
    throw new VFSError(
      srcRes.kind,
      `copyFile: cannot resolve source: ${src}`
    );
  }
  if (srcRes.kind === "dir") {
    throw new VFSError("EISDIR", `copyFile: source is a directory: ${src}`);
  }
  if (srcRes.kind !== "file") {
    throw new VFSError(
      "EINVAL",
      `copyFile: source is not a regular file: ${src}`
    );
  }

  // cap pre-validation (mirrors writeFile).
  const validateMod = await import("../../../../shared/metadata-validate");
  let metadataEncoded: Uint8Array | null | undefined;
  if (opts.metadata === null) {
    metadataEncoded = null;
  } else if (opts.metadata !== undefined) {
    metadataEncoded = validateMod.validateMetadata(opts.metadata).encoded;
  }
  if (opts.tags !== undefined) validateMod.validateTags(opts.tags);
  if (opts.version?.label !== undefined) {
    validateMod.validateLabel(opts.version.label);
  }

  const overwrite = opts.overwrite !== false;

  // Yjs-mode fork: materialize src bytes once, then fall through to
  // a regular writeFile against dest. This is documented as a
  // bytes-snapshot copy — dest does NOT share src's CRDT op log.
  if (opsMod.isYjsMode(durableObject, userId, srcRes.leafId)) {
    const bytes = await yjsMod.readYjsAsBytes(
      durableObject,
      scope,
      srcRes.leafId
    );
    // Pass through the explicit user opts, but inherit src's
    // metadata + tags if not overridden (to match the non-yjs
    // copy semantics below).
    const inheritMeta =
      metadataEncoded === undefined
        ? tagsMod.readMetadataBytes(durableObject, srcRes.leafId)
        : metadataEncoded;
    const inheritTags =
      opts.tags === undefined
        ? tagsMod.getTags(durableObject, srcRes.leafId)
        : opts.tags;
    const inheritedMetaObj =
      inheritMeta === null
        ? null
        : inheritMeta === undefined
          ? undefined
          : (JSON.parse(new TextDecoder().decode(inheritMeta)) as Record<
              string,
              unknown
            >);
    await opsMod.vfsWriteFile(durableObject, scope, dest, bytes, {
      mode: 0o644,
      mimeType: "application/octet-stream",
      metadata: inheritedMetaObj,
      tags: inheritTags,
      version: opts.version,
    });
    return;
  }

  // Read src manifest. Three branches based on storage tier.
  const srcRow = durableObject.sql
    .exec(
      `SELECT file_id, file_size, file_hash, mime_type, mode,
              chunk_size, chunk_count, head_version_id, inline_data
         FROM files
        WHERE file_id = ? AND user_id = ? AND status = 'complete'`,
      srcRes.leafId,
      userId
    )
    .toArray()[0] as
    | {
        file_id: string;
        file_size: number;
        file_hash: string;
        mime_type: string;
        mode: number;
        chunk_size: number;
        chunk_count: number;
        head_version_id: string | null;
        inline_data: ArrayBuffer | null;
      }
    | undefined;
  if (!srcRow) {
    throw new VFSError("ENOENT", `copyFile: source vanished: ${src}`);
  }

  // Resolve dest's parent — same shape as writeFile's path resolution.
  const destParent = await resolveDestParent(durableObject, userId, dest);

  // EEXIST guard — only if overwrite=false.
  if (!overwrite) {
    const live = durableObject.sql
      .exec(
        `SELECT file_id FROM files
          WHERE user_id=? AND IFNULL(parent_id,'')=IFNULL(?,'')
            AND file_name=? AND status='complete'`,
        userId,
        destParent.parentId,
        destParent.leaf
      )
      .toArray()[0] as { file_id: string } | undefined;
    if (live) {
      throw new VFSError(
        "EEXIST",
        `copyFile: dest exists and overwrite=false: ${dest}`
      );
    }
  }

  // Inheritance defaults: src metadata + tags ride along when the
  // caller didn't override.
  const finalMetadata: Uint8Array | null | undefined =
    metadataEncoded === undefined
      ? tagsMod.readMetadataBytes(durableObject, srcRes.leafId)
      : metadataEncoded;
  const finalTags: readonly string[] =
    opts.tags === undefined
      ? tagsMod.getTags(durableObject, srcRes.leafId)
      : opts.tags;

  const innerOpts: CopyOpts = {
    metadataEncoded: finalMetadata,
    tags: finalTags,
    versionUserVisible: opts.version?.userVisible ?? true,
    versionLabel: opts.version?.label,
    overwrite,
  };

  // Inline tier: src.inline_data IS NOT NULL.
  if (srcRow.inline_data) {
    return copyInline(
      durableObject,
      scope,
      userId,
      destParent.parentId,
      destParent.leaf,
      srcRow,
      innerOpts
    );
  }

  // Versioned tier: src has head_version_id.
  if (srcRow.head_version_id || isVersioningEnabled(durableObject, userId)) {
    return copyVersioned(
      durableObject,
      scope,
      userId,
      destParent.parentId,
      destParent.leaf,
      srcRow,
      innerOpts
    );
  }

  // Chunked tier (versioning OFF): mirror file_chunks rows.
  return copyChunked(
    durableObject,
    scope,
    userId,
    destParent.parentId,
    destParent.leaf,
    srcRow,
    innerOpts
  );
}

async function resolveDestParent(
  durableObject: UserDO,
  userId: string,
  dest: string
): Promise<{ parentId: string | null; leaf: string }> {
  const pathMod = await import("./path-walk");
  const pathsMod = await import("../../../../shared/vfs-paths");
  let segs: string[];
  try {
    segs = pathsMod.normalizePath(dest);
  } catch (err) {
    if (err instanceof pathsMod.VFSPathError) {
      throw new VFSError("EINVAL", err.message);
    }
    throw err;
  }
  if (segs.length === 0) {
    throw new VFSError("EINVAL", "copyFile: dest cannot be root");
  }
  const leaf = segs[segs.length - 1];
  if (segs.length === 1) {
    return { parentId: null, leaf };
  }
  const parentPath = "/" + segs.slice(0, -1).join("/");
  const r = pathMod.resolvePathFollow(durableObject, userId, parentPath);
  if (r.kind === "ENOENT") {
    throw new VFSError(
      "ENOENT",
      `copyFile: dest parent does not exist: ${parentPath}`
    );
  }
  if (r.kind !== "dir") {
    throw new VFSError(
      "ENOTDIR",
      `copyFile: dest parent is not a directory: ${parentPath}`
    );
  }
  return { parentId: r.leafId === "" ? null : r.leafId, leaf };
}

async function copyInline(
  durableObject: UserDO,
  scope: VFSScope,
  userId: string,
  parentId: string | null,
  leaf: string,
  srcRow: {
    file_id: string;
    file_size: number;
    file_hash: string;
    mime_type: string;
    mode: number;
    inline_data: ArrayBuffer | null;
  },
  opts: CopyOpts
): Promise<void> {
  const opsMod = await import("./vfs-ops");
  const tmpId = generateId();
  const tmpName = `_vfs_tmp_${tmpId}`;
  const now = Date.now();
  const inlineData = srcRow.inline_data
    ? new Uint8Array(srcRow.inline_data)
    : new Uint8Array(0);

  durableObject.sql.exec(
    `INSERT INTO files (file_id, user_id, parent_id, file_name, file_size,
        file_hash, mime_type, chunk_size, chunk_count, pool_size, status,
        created_at, updated_at, mode, node_kind, inline_data)
     VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0, ?, 'uploading', ?, ?, ?, 'file', ?)`,
    tmpId,
    userId,
    parentId,
    tmpName,
    srcRow.file_size,
    srcRow.file_hash,
    srcRow.mime_type,
    poolSizeFor(durableObject, userId),
    now,
    now,
    srcRow.mode,
    inlineData
  );
  await durableObject.scheduleStaleUploadSweep();

  // Under versioning ON, copy must NOT hard-delete the
  // destination's prior history. The non-versioned source carries
  // no `head_version_id`, but the destination tenant may have
  // versioning enabled and the dest path may already be a versioned
  // file. Route through commitVersion when versioning is ON; the
  // bytes are inline so we attach them directly to the version row.
  const versioning = isVersioningEnabled(durableObject, userId);
  if (versioning) {
    const liveDst = findLiveFile(durableObject, userId, parentId, leaf);
    let pathId: string;
    if (liveDst) {
      pathId = liveDst.file_id;
    } else {
      await opsMod.commitRename(
        durableObject,
        userId,
        scope,
        tmpId,
        parentId,
        leaf
      );
      pathId = tmpId;
    }
    const versionId = generateId();
    commitVersion(durableObject, {
      pathId,
      versionId,
      userId,
      size: srcRow.file_size,
      mode: srcRow.mode,
      mtimeMs: now,
      chunkSize: 0,
      chunkCount: 0,
      fileHash: srcRow.file_hash,
      mimeType: srcRow.mime_type,
      inlineData,
      userVisible: opts.versionUserVisible ?? true,
      label: opts.versionLabel,
      metadata: opts.metadataEncoded ?? null,
    });
    if (liveDst) {
      // Drop the tmp row WITHOUT chunk fan-out (no chunks were
      // written to shards for the inline tier).
      durableObject.sql.exec(
        "DELETE FROM file_tags WHERE path_id = ?",
        tmpId
      );
      durableObject.sql.exec(
        "DELETE FROM files WHERE file_id = ?",
        tmpId
      );
    }
    // Apply commit-time side effects (metadata + tags) to the
    // path-stable identity.
    await applyCopySideEffects(
      durableObject,
      userId,
      pathId,
      opts,
      now,
      srcRow.file_id
    );
    return;
  }

  // Versioning OFF — commitRename hard-deletes any prior live row.
  await opsMod.commitRename(
    durableObject,
    userId,
    scope,
    tmpId,
    parentId,
    leaf
  );
  await applyCopySideEffects(durableObject, userId, tmpId, opts, now, srcRow.file_id);
  // Record bytes against quota for the non-versioning copy path.
  // Without this, copyFile would silently understate storage_used
  // / file_count for non-versioning tenants. Inline tier so also
  // bump inline_bytes_used.
  const { recordWriteUsage } = await import("./vfs/helpers");
  recordWriteUsage(
    durableObject,
    userId,
    srcRow.file_size,
    1,
    srcRow.file_size
  );
}

async function copyChunked(
  durableObject: UserDO,
  scope: VFSScope,
  userId: string,
  parentId: string | null,
  leaf: string,
  srcRow: {
    file_id: string;
    file_size: number;
    file_hash: string;
    mime_type: string;
    mode: number;
    chunk_size: number;
    chunk_count: number;
  },
  opts: CopyOpts
): Promise<void> {
  const opsMod = await import("./vfs-ops");
  const tmpId = generateId();
  const tmpName = `_vfs_tmp_${tmpId}`;
  const now = Date.now();
  const poolSize = poolSizeFor(durableObject, userId);

  durableObject.sql.exec(
    `INSERT INTO files (file_id, user_id, parent_id, file_name, file_size,
        file_hash, mime_type, chunk_size, chunk_count, pool_size, status,
        created_at, updated_at, mode, node_kind)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'uploading', ?, ?, ?, 'file')`,
    tmpId,
    userId,
    parentId,
    tmpName,
    srcRow.file_size,
    srcRow.file_hash,
    srcRow.mime_type,
    srcRow.chunk_size,
    srcRow.chunk_count,
    poolSize,
    now,
    now,
    srcRow.mode
  );
  await durableObject.scheduleStaleUploadSweep();

  // Read src manifest from file_chunks.
  const srcChunks = durableObject.sql
    .exec(
      `SELECT chunk_index, chunk_hash, chunk_size, shard_index
         FROM file_chunks WHERE file_id = ? ORDER BY chunk_index`,
      srcRow.file_id
    )
    .toArray() as {
    chunk_index: number;
    chunk_hash: string;
    chunk_size: number;
    shard_index: number;
  }[];

  const env = durableObject.envPublic;
  const shardNs = env.MOSSAIC_SHARD as unknown as DurableObjectNamespace<ShardDO>;

  try {
    await preflightChunksAlive(shardNs, scope, srcChunks);

    // Fan-out: putChunk(empty) per chunk under tmpId. Serial within
    // a shard (PK collision avoidance for repeated hashes at
    // different indices); parallel across shards.
    const byShard = new Map<number, typeof srcChunks>();
    for (const c of srcChunks) {
      const arr = byShard.get(c.shard_index) ?? [];
      arr.push(c);
      byShard.set(c.shard_index, arr);
    }
    await Promise.all(
      Array.from(byShard.entries()).map(async ([sIdx, chunks]) => {
        const shardName = vfsShardDOName(scope.ns, scope.tenant, scope.sub, sIdx);
        const stub = shardNs.get(shardNs.idFromName(shardName));
        for (const c of chunks) {
          await stub.putChunk(
            c.chunk_hash,
            new Uint8Array(0),
            tmpId,
            c.chunk_index,
            userId
          );
          durableObject.sql.exec(
            `INSERT INTO file_chunks (file_id, chunk_index, chunk_hash, chunk_size, shard_index)
             VALUES (?, ?, ?, ?, ?)`,
            tmpId,
            c.chunk_index,
            c.chunk_hash,
            c.chunk_size,
            c.shard_index
          );
        }
      })
    );
  } catch (err) {
    await opsMod.abortTempFile(durableObject, userId, scope, tmpId);
    throw err;
  }

  await opsMod.commitRename(
    durableObject,
    userId,
    scope,
    tmpId,
    parentId,
    leaf
  );
  await applyCopySideEffects(durableObject, userId, tmpId, opts, now, srcRow.file_id);
  // Record bytes against quota for the non-versioning chunked
  // copy. Without this, copyFile would silently understate
  // storage_used / file_count for non-versioning chunked copies,
  // so multi-GB tenants would see their pool stay at 32 forever.
  const { recordWriteUsage } = await import("./vfs/helpers");
  recordWriteUsage(durableObject, userId, srcRow.file_size, 1);
}

async function copyVersioned(
  durableObject: UserDO,
  scope: VFSScope,
  userId: string,
  parentId: string | null,
  leaf: string,
  srcRow: {
    file_id: string;
    file_size: number;
    file_hash: string;
    mime_type: string;
    mode: number;
    chunk_size: number;
    chunk_count: number;
    head_version_id: string | null;
    inline_data: ArrayBuffer | null;
  },
  opts: CopyOpts
): Promise<void> {
  const opsMod = await import("./vfs-ops");
  const versionsMod = await import("./vfs-versions");
  const tmpId = generateId();
  const tmpName = `_vfs_tmp_${tmpId}`;
  const now = Date.now();

  durableObject.sql.exec(
    `INSERT INTO files (file_id, user_id, parent_id, file_name, file_size,
        file_hash, mime_type, chunk_size, chunk_count, pool_size, status,
        created_at, updated_at, mode, node_kind)
     VALUES (?, ?, ?, ?, 0, '', ?, 0, 0, ?, 'uploading', ?, ?, ?, 'file')`,
    tmpId,
    userId,
    parentId,
    tmpName,
    srcRow.mime_type,
    poolSizeFor(durableObject, userId),
    now,
    now,
    srcRow.mode
  );
  await durableObject.scheduleStaleUploadSweep();

  // Read src head version + its chunks.
  const srcHead = versionsMod.getVersion(
    durableObject,
    srcRow.file_id,
    srcRow.head_version_id ?? undefined
  );
  if (!srcHead) {
    // Versioning ON but no head — treat as new path with no source content.
    throw new VFSError(
      "ENOENT",
      `copyFile: source has no head version`
    );
  }
  if (srcHead.deleted) {
    throw new VFSError(
      "ENOENT",
      `copyFile: source head is a tombstone`
    );
  }

  const newVersionId = generateId();

  // When destination already has a versioned live row, attach the
  // new version to its `pathId` instead of `tmpId` so prior
  // history survives the copy.
  const liveDst = findLiveFile(durableObject, userId, parentId, leaf);
  let pathId: string;
  if (liveDst) {
    pathId = liveDst.file_id;
  } else {
    // No prior path — promote the tmp row.
    await opsMod.commitRename(
      durableObject,
      userId,
      scope,
      tmpId,
      parentId,
      leaf
    );
    pathId = tmpId;
  }

  if (srcHead.inlineData) {
    // Inline-tier version: just snapshot the bytes.
    commitVersion(durableObject, {
      pathId,
      versionId: newVersionId,
      userId,
      size: srcHead.size,
      mode: srcHead.mode,
      mtimeMs: now,
      chunkSize: 0,
      chunkCount: 0,
      fileHash: srcHead.fileHash,
      mimeType: srcHead.mimeType,
      inlineData: new Uint8Array(srcHead.inlineData),
      userVisible: opts.versionUserVisible ?? true,
      label: opts.versionLabel,
      metadata: opts.metadataEncoded ?? null,
      // copy preserves the source's encryption mode + keyId.
      // The bytes are envelope-stream verbatim; the dest must report
      // the same mode so SDK readFile knows to decrypt.
      encryption: srcHead.encryption,
    });
    if (liveDst) {
      // Drop the redundant tmp row WITHOUT chunk fan-out (inline
      // bytes live in the file_versions row, not on shards).
      dropTmpRowAfterVersionCommit(durableObject, tmpId, {
        hasChunks: false,
      });
    }
    await applyCopySideEffects(durableObject, userId, pathId, opts, now, srcRow.file_id);
    return;
  }

  // Chunked-tier version: refbump per chunk, mirror version_chunks
  // under newVersionId / newPathId (= tmpId).
  const srcChunks = durableObject.sql
    .exec(
      `SELECT chunk_index, chunk_hash, chunk_size, shard_index
         FROM version_chunks WHERE version_id = ? ORDER BY chunk_index`,
      srcHead.versionId
    )
    .toArray() as {
    chunk_index: number;
    chunk_hash: string;
    chunk_size: number;
    shard_index: number;
  }[];

  const env = durableObject.envPublic;
  const shardNs = env.MOSSAIC_SHARD as unknown as DurableObjectNamespace<ShardDO>;
  const newRefId = shardRefId(tmpId, newVersionId);

  try {
    await preflightChunksAlive(shardNs, scope, srcChunks);
    const byShard = new Map<number, typeof srcChunks>();
    for (const c of srcChunks) {
      const arr = byShard.get(c.shard_index) ?? [];
      arr.push(c);
      byShard.set(c.shard_index, arr);
    }
    await Promise.all(
      Array.from(byShard.entries()).map(async ([sIdx, chunks]) => {
        const shardName = vfsShardDOName(scope.ns, scope.tenant, scope.sub, sIdx);
        const stub = shardNs.get(shardNs.idFromName(shardName));
        for (const c of chunks) {
          await stub.putChunk(
            c.chunk_hash,
            new Uint8Array(0),
            newRefId,
            c.chunk_index,
            userId
          );
          insertVersionChunk(durableObject, newVersionId, c);
        }
      })
    );
  } catch (err) {
    // Best-effort cleanup of any landed refs.
    durableObject.sql.exec(
      "DELETE FROM version_chunks WHERE version_id = ?",
      newVersionId
    );
    await opsMod.abortTempFile(durableObject, userId, scope, tmpId);
    throw err;
  }

  // Chunked copy commits the new version onto the destination's
  // stable pathId (resolved above). chunk_refs were
  // filed under `newRefId = shardRefId(tmpId, newVersionId)`; we
  // stamp that as `shard_ref_id` so a future `dropVersionRows`
  // fan-out keys ShardDO `deleteChunks` correctly when the version
  // is reaped.
  commitVersion(durableObject, {
    pathId,
    versionId: newVersionId,
    userId,
    size: srcHead.size,
    mode: srcHead.mode,
    mtimeMs: now,
    chunkSize: srcHead.chunkSize,
    chunkCount: srcHead.chunkCount,
    fileHash: srcHead.fileHash,
    mimeType: srcHead.mimeType,
    inlineData: null,
    userVisible: opts.versionUserVisible ?? true,
    label: opts.versionLabel,
    metadata: opts.metadataEncoded ?? null,
    encryption: srcHead.encryption,
    shardRefId: newRefId,
  });
  if (liveDst) {
    // Drop the redundant tmp row + its file_chunks (which the new
    // version doesn't reference — chunks are owned via
    // version_chunks under newVersionId, refed on shards via
    // newRefId). Skip the shard fan-out: chunks belong to the new
    // version.
    dropTmpRowAfterVersionCommit(durableObject, tmpId, {
      hasChunks: true,
    });
  }
  await applyCopySideEffects(durableObject, userId, pathId, opts, now, srcRow.file_id);
}

async function preflightChunksAlive(
  shardNs: DurableObjectNamespace<ShardDO>,
  scope: VFSScope,
  chunks: { chunk_hash: string; shard_index: number }[]
): Promise<void> {
  const byShard = new Map<number, string[]>();
  for (const c of chunks) {
    const arr = byShard.get(c.shard_index) ?? [];
    arr.push(c.chunk_hash);
    byShard.set(c.shard_index, arr);
  }
  await Promise.all(
    Array.from(byShard.entries()).map(async ([shardIndex, hashes]) => {
      const shardName = vfsShardDOName(scope.ns, scope.tenant, scope.sub, shardIndex);
      const stub = shardNs.get(shardNs.idFromName(shardName));
      const { alive } = await stub.chunksAlive(hashes);
      if (alive.length !== hashes.length) {
        const aliveSet = new Set(alive);
        const missing = hashes.filter((h) => !aliveSet.has(h));
        throw new VFSError(
          "ENOENT",
          `copyFile: source chunks swept on shard ${shardIndex}: ${missing
            .slice(0, 3)
            .join(",")}${missing.length > 3 ? "..." : ""}`
        );
      }
    })
  );
}

async function applyCopySideEffects(
  durableObject: UserDO,
  userId: string,
  pathId: string,
  opts: CopyOpts,
  mtimeMs: number,
  /**
   * source `files.file_id`. When set, the destination
   * inherits the source's `(encryption_mode, encryption_key_id)`
   * columns via {@link copyEncryptionStamp}. The bytes were already
   * copied opaquely (envelopes are byte-identical between src and
   * dst); this just makes the dest's stat surface report the right
   * mode so the SDK knows to decrypt on read.
   */
  srcFileId?: string
): Promise<void> {
  const tagsMod = await import("./metadata-tags");
  if (opts.metadataEncoded !== undefined) {
    tagsMod.writeMetadata(durableObject, pathId, opts.metadataEncoded);
  }
  if (opts.tags !== undefined) {
    tagsMod.replaceTags(durableObject, userId, pathId, opts.tags);
  } else {
    tagsMod.bumpTagMtimes(durableObject, pathId, mtimeMs);
  }
  if (srcFileId !== undefined) {
    const { copyEncryptionStamp } = await import("./encryption-stamp");
    copyEncryptionStamp(durableObject, srcFileId, pathId);
  }
}

function poolSizeFor(durableObject: UserDO, userId: string): number {
  const row = durableObject.sql
    .exec("SELECT pool_size FROM quota WHERE user_id = ?", userId)
    .toArray()[0] as { pool_size: number } | undefined;
  return row ? row.pool_size : 32;
}
