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
import { vfsShardDOName } from "../../lib/utils";
import { placeChunk } from "../../../../shared/placement";
import { userIdFor } from "./vfs/helpers";
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
  const sIdx = placeChunk(userIdFor(scope), refId, 0, poolSize);
  const shardName = vfsShardDOName(scope.ns, scope.tenant, scope.sub, sIdx);
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

/**
 * Standard variants pre-generated at upload-finalize time. The
 * gallery's typical access pattern hits `thumb` first, then
 * `lightbox` on click; pre-rendering both removes the cold-render
 * latency on the user's first interaction.
 */
const PRE_GEN_STANDARD_VARIANTS = ["thumb", "medium", "lightbox"] as const;

/**
 * Pre-generate standard preview variants for a freshly-written file,
 * intended to run inside `c.executionCtx.waitUntil(...)`. Best-effort:
 * each variant renders inside its own try/catch and a failure for one
 * variant never bubbles up — the caller's request must not 500
 * because the renderer didn't have a binding or hit an EFBIG.
 *
 * Skips non-renderable inputs:
 *  - empty files (`fileSize === 0`)
 *  - encrypted files (server can't decrypt; preview must be client-side)
 *
 * Idempotent: `renderAndStoreVariant` is content-addressed so re-runs
 * with the same input produce the same chunk hash; the `file_variants`
 * row already exists check inside the routing layer (`findVariantRow`)
 * is the cache hit. We don't gate here — let the renderer dispatch
 * decide whether to skip via mime.
 */
export async function preGenerateStandardVariants(
  durableObject: UserDOCore,
  scope: VFSScope,
  args: {
    fileId: string;
    path: string;
    mimeType: string;
    fileName: string;
    fileSize: number;
    isEncrypted: boolean;
  }
): Promise<void> {
  if (args.fileSize === 0 || args.isEncrypted) return;

  for (const variant of PRE_GEN_STANDARD_VARIANTS) {
    try {
      // `renderAndStoreVariant` is idempotent: chunk bytes are
      // content-addressed (variantHash = SHA-256 of rendered bytes)
      // and the `file_variants` insert is `OR IGNORE` keyed by
      // (file_id, variant_kind, renderer_kind). Re-running produces
      // no observable change beyond a wasted render — acceptable on
      // the rare double-finalize path; on the hot path each variant
      // is fresh.
      //
      // Phase 23 audit Claim 7: gate through `withRenderSlot` so
      // concurrent finalize bursts don't fan out >MAX_CONCURRENT_RENDERS
      // simultaneous Cloudflare Images binding calls — that path will
      // 429 under sustained load and the silent-drop in this catch
      // would manifest as user-visible "no preview" on first gallery
      // click. The slot bound is module-global per worker isolate.
      await withRenderSlot(() =>
        renderAndStoreVariant(
          durableObject,
          scope,
          args.fileId,
          args.path,
          args.mimeType,
          args.fileName,
          args.fileSize,
          variant
        )
      );
    } catch (err) {
      // Best-effort. Log to console; never throw out of waitUntil.
      console.warn(
        `preGenerateStandardVariants: ${variant} failed for ${args.path}: ${
          err instanceof Error ? err.message : String(err)
        }`
      );
    }
  }
}

/**
 * Concurrency bound for in-flight preview renders within a single
 * worker isolate (Phase 23 audit Claim 7).
 *
 * Cloudflare Images binding (and the JS-only renderer code paths) all
 * compete for the same per-isolate CPU + bound-binding budget. Without
 * a bound, a bulk-import of 200 photos finalizing on background
 * `ctx.waitUntil` slots would spawn 200 concurrent `renderAndStoreVariant`
 * promises × 3 variants = 600 in-flight renders; Cloudflare Images
 * starts returning 429, the renderer throws, and the catch in
 * `preGenerateStandardVariants` silently drops the variant. The user
 * lands on a gallery with a sea of broken thumbnails and the only
 * remediation is on-demand re-render (cold-path latency +800ms each).
 *
 * 6 was picked from the audit recommendation (4-8): an isolate
 * comfortably handles 6 concurrent IMAGES binding calls, which
 * corresponds to roughly 2 fully-rendering files at a time (3 variants
 * each). For very small files / fast renders the bound just gates the
 * theoretical max — average concurrency stays much lower.
 *
 * Implementation: a Promise-chain semaphore. We don't pull in a
 * dependency; the `slots` array tracks in-flight Promises and a new
 * caller awaits the earliest-completing slot when at capacity. Promises
 * are GC'd after settling so the array doesn't grow.
 */
const MAX_CONCURRENT_RENDERS = 6;
let inFlight = 0;
const waiters: Array<() => void> = [];

async function withRenderSlot<T>(fn: () => Promise<T>): Promise<T> {
  if (inFlight >= MAX_CONCURRENT_RENDERS) {
    await new Promise<void>((resolve) => waiters.push(resolve));
  }
  inFlight++;
  try {
    return await fn();
  } finally {
    inFlight--;
    const next = waiters.shift();
    if (next !== undefined) next();
  }
}

/**
 * Test-only: read the current concurrency state. Exported for the
 * `tests/integration/preview-concurrency.test.ts` regression suite.
 */
export function _renderSlotStateForTests(): {
  inFlight: number;
  waiting: number;
  max: number;
} {
  return {
    inFlight,
    waiting: waiters.length,
    max: MAX_CONCURRENT_RENDERS,
  };
}
