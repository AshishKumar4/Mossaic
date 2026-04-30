/**
 * `vfs.readPreview()` server implementation.
 *
 * Resolves a path → file row → renderer-dispatch → variant cache.
 * On hit, streams the variant bytes from the ShardDO. On miss,
 * runs the renderer, persists the variant, and returns the bytes
 * inline. Idempotent: concurrent misses for the same (file,
 * variant) reconverge via the composite PK + content-addressed
 * chunk_hash.
 *
 * Encryption boundary: encrypted files throw `ENOTSUP`. Server
 * cannot decrypt; client-side rendering is the path forward
 * (out of scope for the server-side pipeline).
 */

import type { UserDOCore as UserDO } from "../user-do-core";
import {
  VFSError,
  type VFSScope,
} from "../../../../../shared/vfs-types";
import type {
  ReadPreviewOpts,
  ReadPreviewResult,
  Variant,
} from "../../../../../shared/preview-types";
import { resolvePath } from "../path-walk";
import { userIdFor } from "./helpers";
import { getPlacement } from "../../../lib/placement-resolver";
import {
  encodeVariantKey,
  findVariantRow,
  renderAndStoreVariant,
} from "../preview-variants";
import { defaultRegistry } from "../../../lib/preview-pipeline";

/**
 * Materialize a preview variant for the file at `path`.
 *
 * Variant storage is keyed by `(file_id, variant_kind, renderer_kind)`.
 * The renderer kind is determined by the file's MIME — callers don't
 * need to know it. On a cold cache, this function blocks on the
 * renderer + a single ShardDO `putChunk`; on a warm cache it does
 * one row lookup + one ShardDO fetch.
 *
 * @param scope The VFS scope (tenant/sub/ns).
 * @param path  POSIX-style file path. Must resolve to a regular file.
 * @param opts  Variant request (standard or custom; format hint).
 *
 * @throws VFSError("ENOENT") — path does not resolve.
 * @throws VFSError("EISDIR") — path is a directory.
 * @throws VFSError("ENOTSUP") — file is encrypted (server cannot
 *   render ciphertext).
 */
export async function vfsReadPreview(
  durableObject: UserDO,
  scope: VFSScope,
  path: string,
  opts: ReadPreviewOpts
): Promise<ReadPreviewResult> {
  const userId = userIdFor(scope);
  const r = resolvePath(durableObject, userId, path);
  if (r.kind === "ENOENT") {
    throw new VFSError("ENOENT", `readPreview: no such file: ${path}`);
  }
  if (r.kind === "dir") {
    throw new VFSError("EISDIR", `readPreview: is a directory: ${path}`);
  }
  if (r.kind !== "file") {
    throw new VFSError(
      "EINVAL",
      `readPreview: not a regular file: ${path}`
    );
  }

  const fileId = r.leafId;

  // Pull the file's metadata in one query. encryption_mode gates
  // the entire pipeline; mime_type drives renderer dispatch;
  // file_name + file_size feed the renderer's RenderInput.
  const fileRow = durableObject.sql
    .exec(
      `SELECT file_name, file_size, mime_type, encryption_mode
         FROM files
        WHERE file_id = ? AND user_id = ? AND status != 'deleted'`,
      fileId,
      userId
    )
    .toArray()[0] as
    | {
        file_name: string;
        file_size: number;
        mime_type: string | null;
        encryption_mode: string | null;
      }
    | undefined;
  if (!fileRow) {
    throw new VFSError(
      "ENOENT",
      `readPreview: file row missing for ${path}`
    );
  }
  if (fileRow.encryption_mode !== null) {
    throw new VFSError(
      "ENOTSUP",
      "readPreview: encrypted files require client-side rendering"
    );
  }

  const mimeType = fileRow.mime_type ?? "application/octet-stream";
  const fileName = fileRow.file_name;
  const fileSize = fileRow.file_size;

  // Renderer dispatch (read-only) tells us which `renderer_kind`
  // to look up in `file_variants`. The fallback chain in
  // renderAndStoreVariant may swap to icon-card on
  // EMOSSAIC_UNAVAILABLE; the lookup tolerates that because we
  // also accept icon-card rows when the primary kind misses.
  const registry = defaultRegistry();
  const primaryRenderer = registry.dispatchByMime(mimeType);
  const variantKind: Variant = opts.variant ?? "thumb";
  const variantKey = encodeVariantKey(variantKind);

  // Try primary renderer kind first; fall back to icon-card row
  // (the universal fallback the writer would have stored under
  // EMOSSAIC_UNAVAILABLE conditions). Track which kind we hit so a
  // stale-chunk recovery can target the correct row.
  let row = findVariantRow(
    durableObject,
    fileId,
    variantKey,
    primaryRenderer.kind
  );
  let rowRendererKind = primaryRenderer.kind;
  if (row === null) {
    const fallback = findVariantRow(
      durableObject,
      fileId,
      variantKey,
      "icon-card"
    );
    if (fallback !== null) {
      row = fallback;
      rowRendererKind = "icon-card";
    }
  }

  if (row !== null) {
    const env = durableObject.envPublic;
    const shardName = getPlacement(scope).shardDOName(
      scope,
      row.shardIndex
    );
    const stub = env.MOSSAIC_SHARD.get(
      env.MOSSAIC_SHARD.idFromName(shardName)
    );
    const res = await stub.fetch(
      new Request(`http://internal/chunk/${row.chunkHash}`)
    );
    if (res.ok) {
      const bytes = new Uint8Array(await res.arrayBuffer());
      return {
        bytes,
        mimeType: row.mimeType,
        width: row.width,
        height: row.height,
        sourceMimeType: mimeType,
        rendererKind: rowRendererKind,
        fromVariantTable: true,
      };
    }
    // Dangling row — chunk was reaped or never landed. Drop the
    // stale mapping so renderAndStoreVariant's INSERT OR IGNORE
    // can write a fresh row with the new chunk_hash.
    durableObject.sql.exec(
      `DELETE FROM file_variants
        WHERE file_id = ? AND variant_kind = ? AND renderer_kind = ?`,
      fileId,
      variantKey,
      rowRendererKind
    );
  }

  // Cache miss (or dangling-row recovery) → render + persist.
  const out = await renderAndStoreVariant(
    durableObject,
    scope,
    fileId,
    path,
    mimeType,
    fileName,
    fileSize,
    variantKind
  );
  // Resolve the renderer_kind that was actually persisted (could
  // be the icon-card fallback if the primary hit
  // EMOSSAIC_UNAVAILABLE inside renderAndStoreVariant).
  const persistedRow = findVariantRow(
    durableObject,
    fileId,
    variantKey,
    primaryRenderer.kind
  );
  const persistedKind =
    persistedRow !== null ? primaryRenderer.kind : "icon-card";

  return {
    bytes: out.bytes,
    mimeType: out.row.mimeType,
    width: out.row.width,
    height: out.row.height,
    sourceMimeType: mimeType,
    rendererKind: persistedKind,
    fromVariantTable: false,
  };
}
