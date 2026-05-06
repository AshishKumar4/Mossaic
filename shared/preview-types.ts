/**
 * Universal preview pipeline — wire shapes shared by worker, SDK, and
 * SPA. Types live here so each consumer imports from a single source
 * of truth (cleanliness rule 8: no duplicate type definitions).
 */

/**
 * Standard variant labels. Resolve to fixed dimensions at the
 * registry layer; consumers may also pass an explicit
 * `{width, height?, fit?}` for arbitrary on-demand sizing.
 *
 *  - `thumb`    — gallery-grid sized; ~256×256 longest-edge cover.
 *  - `medium`   — feed-card sized; ~768 longest-edge contain.
 *  - `lightbox` — full-screen sized; ~1920 longest-edge contain.
 */
export type StandardVariant = "thumb" | "medium" | "lightbox";

/** Image-fit semantics; mirrors the Cloudflare Images binding's `fit`. */
export type FitMode = "cover" | "contain" | "scale-down";

/** Custom-dimension variant. */
export interface CustomVariant {
  width: number;
  height?: number;
  fit?: FitMode;
}

export type Variant = StandardVariant | CustomVariant;

/** Output formats supported across renderers. */
export type PreviewFormat =
  | "image/png"
  | "image/webp"
  | "image/avif"
  | "image/svg+xml";

export interface ReadPreviewOpts {
  variant?: Variant;
  format?: PreviewFormat;
  /**
   * Force a specific renderer by kind. Default: dispatched by
   * `mimeType` via the registry. The registry returns ENOTSUP if
   * the forced renderer's `canRender(mimeType)` is false.
   */
  renderer?: string;
}

export interface ReadPreviewResult {
  /** Output bytes. */
  bytes: Uint8Array;
  /** Output MIME type. */
  mimeType: string;
  /** Output width in pixels. */
  width: number;
  /** Output height in pixels. */
  height: number;
  /** The original file's MIME type. */
  sourceMimeType: string;
  /** Identifier of the renderer that produced this output. */
  rendererKind: string;
  /**
   * `true` when served from a pre-generated `file_variants` row;
   * `false` when produced on-demand. Useful for tooling /
   * observability — never affects rendered bytes.
   */
  fromVariantTable: boolean;
}

/** Input passed to every renderer. */
export interface RenderInput {
  /** Streaming source of original bytes. */
  bytes: ReadableStream<Uint8Array>;
  /** Original file's MIME type. */
  mimeType: string;
  /** Original file's name (extension drives icon-card fallback). */
  fileName: string;
  /** Original file's size in bytes (drives icon-card label). */
  fileSize: number;
}

/** Resolved render options after standard-variant expansion. */
export interface RenderOpts {
  variant: Variant;
  format: PreviewFormat;
}

/** Concrete render output. Mirrors the `ReadPreviewResult` data plane. */
export interface RenderResult {
  bytes: Uint8Array;
  mimeType: string;
  width: number;
  height: number;
}

/**
 * Per-row shape of `file_variants`. Mirrors the DDL in
 * `worker/core/objects/user/user-do-core.ts:ensureInit`. SDK consumers
 * never read this directly; tests do.
 */
export interface FileVariant {
  fileId: string;
  variantKind: string;
  rendererKind: string;
  chunkHash: string;
  shardIndex: number;
  mimeType: string;
  width: number;
  height: number;
  byteSize: number;
  createdAt: number;
}

/** Standard-variant → pixel-dimension map. Single source of truth. */
export const STANDARD_VARIANT_DIMS: Record<
  StandardVariant,
  { width: number; height: number; fit: FitMode }
> = {
  thumb: { width: 256, height: 256, fit: "cover" },
  medium: { width: 768, height: 768, fit: "contain" },
  lightbox: { width: 1920, height: 1920, fit: "contain" },
};

/**
 * Result of `vfs.previewInfo(path, opts)` and
 * `vfs.previewInfoMany(paths, opts)`. Carries the signed URL the
 * browser can fetch directly + metadata an SPA needs to render
 * the IMG element (mimeType, width, height) and to perform
 * conditional revalidation (etag).
 *
 *   - `token` HMAC-signed by the server. Browser embeds in URL.
 *   - `url` full path component starting `/api/vfs/preview-variant/<token>`.
 *     Caller prepends its origin.
 *   - `etag` weak ETag wrapping the contentHash. Send via
 *     `If-None-Match` for 304 revalidation.
 *   - `mimeType`, `width`, `height` mirror `ReadPreviewResult`.
 *   - `rendererKind` is the renderer that actually produced the
 *     bytes (server-side dispatched).
 *   - `versionId` is the head_version_id at mint time
 *     (null on legacy / versioning-OFF tenants).
 *   - `cacheControl` mirrors the route's response header so the
 *     SPA can pre-set browser cache expectations.
 *   - `contentHash` SHA-256 hex of the rendered bytes; the cache
 *     key on the route side.
 *   - `expiresAtMs` token expiry (ms epoch). The cached bytes
 *     live for the year-long max-age regardless.
 */
export interface PreviewInfo {
  token: string;
  url: string;
  etag: string;
  mimeType: string;
  width: number;
  height: number;
  rendererKind: string;
  versionId: string | null;
  cacheControl: string;
  contentHash: string;
  expiresAtMs: number;
}

/**
 * Per-path result for `vfs.previewInfoMany`. Mirrors the batched
 * manifest shape (`{ ok: true, ... } | { ok: false, code, message }`)
 * so a single missing file in a 256-path batch surfaces as a
 * per-entry failure rather than 4xx for the whole batch.
 */
export type PreviewInfoBatchEntry =
  | { path: string; ok: true; info: PreviewInfo }
  | { path: string; ok: false; code: string; message: string };

/**
 * Options for `vfs.previewUrl` / `vfs.previewInfo` /
 * `vfs.previewInfoMany`. Same shape as `ReadPreviewOpts` plus a
 * `ttlMs` knob for the signed token's lifetime.
 *
 * Default token TTL is 24 hours (`PREVIEW_TOKEN_DEFAULT_TTL_MS`
 * on the server). Operators or callers needing long-lived
 * embeds can pass up to 30 days; the server clamps to that
 * maximum.
 */
export interface PreviewUrlOpts extends ReadPreviewOpts {
  /** Token TTL in milliseconds. Server clamps to [60s, 30d]. */
  ttlMs?: number;
}
