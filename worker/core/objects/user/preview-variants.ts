/**
 * Preview variant resolution.
 *
 * Variant rows are content-addressed: `chunk_hash` is the SHA-256
 * of the rendered bytes; the actual bytes live in the standard
 * `chunks` table on a ShardDO, refcounted via `chunk_refs` against
 * a synthetic `${fileId}#variant#${variantKind}` ref ID. Dedup
 * across users is automatic — two users uploading the same image
 * share variant chunks.
 *
 * Encryption boundary: callers MUST gate on `encryption_mode IS
 * NULL` before invoking the renderer. Encrypted previews require
 * client-side rendering (out of scope for v1 server-side).
 *
 * On-demand: this module is invoked by the `vfs.readPreview` RPC
 * on cache miss. The route layer wraps the result with HTTP cache
 * headers (`public, max-age=31536000, immutable`) keyed by the
 * variant chunk hash (strong content-addressed ETag).
 */

import type { UserDOCore } from "./user-do-core";
import type { ShardDO } from "../shard/shard-do";
import type { VFSScope } from "../../../../shared/vfs-types";
import type {
  Variant,
  RenderResult,
} from "../../../../shared/preview-types";
import { hashChunk } from "../../../../shared/crypto";
import { vfsCreateReadStream } from "./vfs/streams";
import { getPlacement } from "../../lib/placement-resolver";
import {
  defaultRegistry,
  RenderError,
} from "../../lib/preview-pipeline";

/**
 * Encode a `Variant` to a stable string suitable for SQL row keys
 * and refIds. Standard variants stringify to their label; custom
 * variants encode dims + fit so identical custom requests across
 * users dedupe.
 */
export function encodeVariantKey(v: Variant): string {
  if (typeof v === "string") return v;
  const h = v.height ?? v.width;
  const fit = v.fit ?? "cover";
  return `custom:w${v.width}h${h}${fit}`;
}

/**
 * Row shape returned by `findVariantRow`. Mirrors the
 * `file_variants` schema with camelCase field names.
 */
export type VariantRow = {
  chunkHash: string;
  shardIndex: number;
  mimeType: string;
  width: number;
  height: number;
  byteSize: number;
};

/**
 * Look up an existing variant row by its composite key. Returns
 * null if absent — the caller should render on-demand and call
 * `insertVariantRow`.
 */
export function findVariantRow(
  durableObject: UserDOCore,
  fileId: string,
  variantKind: string,
  rendererKind: string
): VariantRow | null {
  const row = durableObject.sql
    .exec(
      `SELECT chunk_hash, shard_index, mime_type, width, height, byte_size
         FROM file_variants
        WHERE file_id = ? AND variant_kind = ? AND renderer_kind = ?`,
      fileId,
      variantKind,
      rendererKind
    )
    .toArray()[0] as
    | {
        chunk_hash: string;
        shard_index: number;
        mime_type: string;
        width: number;
        height: number;
        byte_size: number;
      }
    | undefined;
  if (!row) return null;
  return {
    chunkHash: row.chunk_hash,
    shardIndex: row.shard_index,
    mimeType: row.mime_type,
    width: row.width,
    height: row.height,
    byteSize: row.byte_size,
  };
}

/**
 * Render a single variant for a file and persist the result:
 *
 *   1. Open a read stream over the original file.
 *   2. Dispatch the registered renderer for the file's MIME.
 *   3. Hash the rendered bytes.
 *   4. Place + write the bytes to a ShardDO under the synthetic
 *      `${fileId}#variant#${variantKind}` ref ID.
 *   5. Insert the `file_variants` row (composite PK guards races).
 *
 * Returns the row + bytes so the caller can stream the response
 * without a follow-up shard read.
 *
 * @param fileId  Committed `files.file_id`.
 * @param path    Canonical path used by `vfsCreateReadStream`.
 * @param mimeType File's MIME type (drives renderer dispatch).
 * @param fileName Display name (icon-card label, code filename).
 * @param fileSize Original byte size (icon-card label, headers).
 * @param variant  Standard or custom variant request.
 */
export async function renderAndStoreVariant(
  durableObject: UserDOCore,
  scope: VFSScope,
  fileId: string,
  path: string,
  mimeType: string,
  fileName: string,
  fileSize: number,
  variant: Variant
): Promise<{ row: VariantRow; bytes: Uint8Array; result: RenderResult }> {
  const registry = defaultRegistry();
  const renderer = registry.dispatchByMime(mimeType);

  const stream = await vfsCreateReadStream(durableObject, scope, path);
  let result: RenderResult;
  try {
    result = await renderer.render(
      { bytes: stream, mimeType, fileName, fileSize },
      durableObject.envPublic,
      {
        variant,
        // Standard variants negotiate format from the renderer's
        // native output. Image renderer emits image/webp; SVG-only
        // renderers (icon-card, code, waveform, video-poster) emit
        // image/svg+xml. The route layer surfaces this via the
        // Content-Type header on the response.
        format:
          renderer.kind === "image" ? "image/webp" : "image/svg+xml",
      }
    );
  } catch (err: unknown) {
    // EMOSSAIC_UNAVAILABLE → renderer missing a binding (e.g. no
    // IMAGES). Fall back to icon-card so previews degrade
    // gracefully rather than 500ing.
    if (err instanceof RenderError && err.code === "EMOSSAIC_UNAVAILABLE") {
      const fallback = registry.dispatchByKind("icon-card");
      if (fallback === null) {
        // The default registry always includes icon-card. If
        // someone built a custom registry without it AND a
        // primary renderer needed a missing binding, surface the
        // original error rather than masking it.
        throw err;
      }
      const fallbackStream = await vfsCreateReadStream(
        durableObject,
        scope,
        path
      );
      result = await fallback.render(
        { bytes: fallbackStream, mimeType, fileName, fileSize },
        durableObject.envPublic,
        { variant, format: "image/svg+xml" }
      );
    } else {
      throw err;
    }
  }

  const variantBytes = result.bytes;
  const variantHash = await hashChunk(variantBytes);

  // Pool size is frozen on the original file; reuse it so the
  // variant chunk lands in the same pool footprint and rendezvous
  // hashing is deterministic for re-derivation.
  const poolRow = durableObject.sql
    .exec("SELECT pool_size FROM files WHERE file_id = ?", fileId)
    .toArray()[0] as { pool_size: number } | undefined;
  const poolSize = poolRow?.pool_size ?? 32;

  // Encode the variant as a string for refId / row keying.
  // Standard variants use their bare label (`thumb`/`medium`/
  // `lightbox`); custom variants encode their dimensions so two
  // requests for `{w:512,h:512,fit:"cover"}` share storage.
  const variantKey = encodeVariantKey(variant);
  const refId = `${fileId}#variant#${variantKey}`;
  const sIdx = getPlacement(scope).placeChunk(scope, refId, 0, poolSize);
  const shardName = getPlacement(scope).shardDOName(scope, sIdx);
  const env = durableObject.envPublic;
  const shardNs = env.MOSSAIC_SHARD as unknown as DurableObjectNamespace<ShardDO>;
  const shardStub = shardNs.get(shardNs.idFromName(shardName));

  const userId = scope.sub
    ? `${scope.tenant}::${scope.sub}`
    : scope.tenant;
  await shardStub.putChunk(variantHash, variantBytes, refId, 0, userId);

  // Resolve the actual stored renderer kind (could be the
  // fallback if the primary failed).
  const rendererKind =
    result.mimeType === "image/webp" ? "image" : renderer.kind;

  durableObject.sql.exec(
    `INSERT OR IGNORE INTO file_variants
       (file_id, variant_kind, renderer_kind, chunk_hash, shard_index,
        mime_type, width, height, byte_size, created_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    fileId,
    variantKey,
    rendererKind,
    variantHash,
    sIdx,
    result.mimeType,
    result.width,
    result.height,
    variantBytes.byteLength,
    Date.now()
  );

  return {
    row: {
      chunkHash: variantHash,
      shardIndex: sIdx,
      mimeType: result.mimeType,
      width: result.width,
      height: result.height,
      byteSize: variantBytes.byteLength,
    },
    bytes: variantBytes,
    result,
  };
}
