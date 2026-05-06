/**
 * Workers Cache helper for read-heavy endpoints.
 *
 * Wraps `caches.default` for surfaces where:
 *   - The response is bytes-with-stable-content-type
 *     (image / chunk / preview / manifest JSON).
 *   - Authentication has already been verified by the caller.
 *   - The cache key encodes EVERY signal that would invalidate
 *     the response. No active purge calls; structural bust via
 *     key-versioning instead.
 *
 * Lives in `worker/core/lib/` so the core route layer
 * (vfs-preview, vfs-readChunk, vfs-openManifest) can use it
 * alongside the App-mode gallery surfaces. The cache-key shape
 * supports `extraKeyParts[]` so a surface can fold in
 * headVersionId + encryption fingerprint + variant descriptor
 * without bloating the type with one field per surface.
 *
 * Auth safety — four invariants:
 *   1. `cache.match` runs AFTER caller-supplied auth verification.
 *      A cached response never serves an unauthenticated
 *      request.
 *   2. Cache key includes `namespace` (`<userId>` for private,
 *      `<shareToken>` for public). Cross-tenant collision
 *      requires breaking SHA-256 / forging a share token.
 *   3. Bust tokens (`updatedAt`, `headVersionId`,
 *      `encryptionFingerprint`) advance on every write that
 *      changes the response bytes — stale-after-write is
 *      structurally impossible.
 *   4. Cached responses keep their original Cache-Control;
 *      intermediaries respect the shorter TTL of either
 *      the response or their own policy.
 *
 * @lean-invariant Mossaic.Vfs.Cache.bust_token_completeness —
 * the cache key must include every column any write path can
 * mutate AND that affects the response. See
 * `local/cache-staleness-audit.md` for the per-surface proof.
 */

/**
 * Surface-tag union. Each tag carves out a separate cache
 * namespace under `https://<tag>.mossaic.local/...` so two
 * surfaces with overlapping (fileId, updatedAt) pairs never
 * collide.
 *
 *   - gthumb / gimg — gallery thumbnail / image.
 *   - simg — shared album image.
 *   - preview — vfs.readPreview rendered variant.
 *   - chunk — vfs.readChunk + chunk-download endpoint.
 *   - manifest — vfs.openManifest JSON.
 */
export type EdgeCacheSurfaceTag =
  | "gthumb"
  | "gimg"
  | "simg"
  | "preview"
  | "chunk"
  | "manifest";

export interface EdgeCacheOpts {
  /** Distinguishes cache namespaces across endpoint kinds. */
  surfaceTag: EdgeCacheSurfaceTag;
  /**
   * Per-tenant or per-share namespace component.
   * `<userId>` for private; `<shareToken>` for public.
   */
  namespace: string;
  /**
   * Stable identifier for the resource being cached. For file-
   * keyed surfaces this is `file_id`. For chunk-keyed surfaces
   * (cross-tenant content-addressed dedup) this is `chunk_hash`.
   */
  fileId: string;
  /**
   * Bust token: `files.updated_at` in milliseconds. Pass `0`
   * for content-addressed surfaces (chunk by hash) where the
   * key is structurally immutable already.
   */
  updatedAt: number;
  /**
   * Free-form additional key components. Folded
   * into the URL path after `(namespace, fileId, updatedAt)`.
   * Use cases: headVersionId, encryptionFingerprint,
   * variantKind, rendererKind, chunkIndex, format. Each part
   * MUST be URL-safe (no `/`); callers should hash arbitrary
   * caller-supplied bytes before passing.
   */
  extraKeyParts?: readonly string[];
  /** Cache-Control header to attach to the FRESH response. */
  cacheControl: string;
  /**
   * Wraps `executionCtx.waitUntil` so the cache PUT runs in the
   * background and the request completes immediately. Pass
   * `c.executionCtx.waitUntil.bind(c.executionCtx)` from the
   * route handler.
   */
  waitUntil: (promise: Promise<unknown>) => void;
}

/**
 * Build the cache `Request` key. Exposed so tests can assert
 * key shape without firing real cache lookups.
 *
 * Shape:
 *   https://<surfaceTag>.mossaic.local/<namespace>/<fileId>/<updatedAt>[/<...extraKeyParts>]
 *
 * Determinism: the same `EdgeCacheOpts` always yields the same
 * key. Order of `extraKeyParts` is preserved in the URL so
 * `[v, e]` is a different key from `[e, v]` \u2014 callers must
 * pick one ordering and stick with it.
 */
export function edgeCacheKey(opts: EdgeCacheOpts): Request {
  const base =
    `https://${opts.surfaceTag}.mossaic.local/` +
    `${opts.namespace}/${opts.fileId}/${opts.updatedAt}`;
  const tail =
    opts.extraKeyParts && opts.extraKeyParts.length > 0
      ? "/" + opts.extraKeyParts.join("/")
      : "";
  return new Request(base + tail, { method: "GET" });
}

/**
 * Look up the cached response. Returns null on miss.
 */
export async function edgeCacheLookup(
  opts: EdgeCacheOpts
): Promise<Response | null> {
  const cache = (caches as unknown as { default: Cache }).default;
  const hit = await cache.match(edgeCacheKey(opts));
  return hit ?? null;
}

/**
 * Stash a fresh response in the cache. The clone is essential \u2014
 * the original Response body can only be consumed once and must
 * be returned to the caller.
 *
 * Only cacheable when the response status is 200. Cloudflare
 * Workers Cache silently rejects non-200; spelling it out keeps
 * the contract obvious.
 */
export function edgeCachePut(
  opts: EdgeCacheOpts,
  response: Response
): void {
  if (response.status !== 200) return;
  const cache = (caches as unknown as { default: Cache }).default;
  opts.waitUntil(cache.put(edgeCacheKey(opts), response.clone()));
}

/**
 * Convenience wrapper: lookup-or-fall-through-and-put.
 *
 * The fresh-builder is invoked only on cache miss. The fresh
 * response MUST attach Content-Type + Cache-Control headers;
 * the helper doesn't second-guess them.
 */
export async function edgeCacheServe(
  opts: EdgeCacheOpts,
  buildFresh: () => Promise<Response>
): Promise<Response> {
  const cached = await edgeCacheLookup(opts);
  if (cached) return cached;
  const fresh = await buildFresh();
  edgeCachePut(opts, fresh);
  return fresh;
}

/**
 * Short hex digest of a string. Useful for folding arbitrary
 * opaque inputs (e.g. encryption stamps, variant descriptors
 * with `/` characters) into a URL-safe cache-key part.
 * Synchronous + deterministic; not cryptographic (the cache key
 * isn't a security boundary, just a uniqueness one — collisions
 * would only confuse caches, not bypass auth).
 *
 * Uses a small fnv1a-style hash over UTF-8 codepoints; 8 hex
 * chars (32 bits) is enough to disambiguate per-tenant
 * encryption stamps. Cross-tenant collisions are blocked
 * structurally by the `namespace` key part.
 */
export function edgeCacheKeyPart(input: string): string {
  let h = 0x811c9dc5;
  for (let i = 0; i < input.length; i++) {
    h ^= input.charCodeAt(i);
    h = (h + ((h << 1) + (h << 4) + (h << 7) + (h << 8) + (h << 24))) >>> 0;
  }
  return h.toString(16).padStart(8, "0");
}
