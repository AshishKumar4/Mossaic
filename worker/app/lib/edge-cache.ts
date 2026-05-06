/**
 * Phase 36 \u2014 Workers Cache helper for read-heavy endpoints.
 *
 * Wraps `caches.default` for surfaces where:
 *   - The response is bytes-with-stable-content-type (image / chunk / preview).
 *   - Authentication has already been verified by the caller.
 *   - The cache key includes a per-tenant namespace AND a version
 *     token (file's `updated_at` timestamp) so any write
 *     invalidates without explicit purge calls.
 *
 * The pattern mirrors the existing precedent at
 * `worker/core/routes/multipart-routes.ts:514-654`
 * (caches.default for /api/vfs/chunk/:fileId/:idx). Phase 36
 * propagates that one-off into a deliberate convention with
 * three call-sites \u2014 gallery thumbnail, gallery image, shared
 * album image.
 *
 * Auth safety:
 *   - Cache key includes either `<userId>` (private) or
 *     `<shareToken>` (public). No cross-tenant key collision is
 *     possible without breaking SHA-256 (in the chunk-hash
 *     precedent) or share-token signing (here).
 *   - `cache.match` runs AFTER caller-supplied auth verification
 *     so a cached response never serves an unauthenticated
 *     request.
 *
 * Cache-key shape:
 *   https://<surfaceTag>.mossaic.local/<namespace>/<fileId>/<updatedAt>
 *
 *   - surfaceTag distinguishes endpoints that return different
 *     mime types or sizes for the same fileId
 *     (e.g. `gthumb` vs `gimg` vs `simg`).
 *   - namespace is `<userId>` for private surfaces and
 *     `<shareToken>` (or its hash) for public surfaces.
 *   - updatedAt is `files.updated_at` (millisecond epoch);
 *     bumped by every commit / unlink / rename / metadata
 *     change. Cache-busts on writes structurally.
 */

export interface EdgeCacheOpts {
  /** Distinguishes cache namespaces across endpoint kinds. */
  surfaceTag: "gthumb" | "gimg" | "simg" | "preview";
  /** Per-tenant or per-share namespace component. */
  namespace: string;
  /** File identifier (the immutable `file_id` from `files`). */
  fileId: string;
  /** Bust token: `files.updated_at` in milliseconds. */
  updatedAt: number;
  /** Cache-Control header to attach to the FRESH response. */
  cacheControl: string;
  /**
   * Wraps `executionCtx.waitUntil` so the cache PUT runs in the
   * background and the request completes immediately. Pass
   * `c.executionCtx` from the route handler.
   */
  waitUntil: (promise: Promise<unknown>) => void;
}

/**
 * Build the cache `Request` key. Exposed so tests can assert
 * key shape without firing real cache lookups.
 */
export function edgeCacheKey(opts: EdgeCacheOpts): Request {
  return new Request(
    `https://${opts.surfaceTag}.mossaic.local/${opts.namespace}/${opts.fileId}/${opts.updatedAt}`,
    { method: "GET" }
  );
}

/**
 * Look up the cached response. Returns null on miss.
 *
 * Caller pattern:
 *   const cached = await edgeCacheLookup(opts);
 *   if (cached) return cached;
 *   const fresh = await origin();
 *   const response = buildResponse(fresh, opts.cacheControl);
 *   edgeCachePut(opts, response);
 *   return response;
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
 * be returned to the caller. Cache.put receives the clone in a
 * waitUntil so the request returns immediately.
 *
 * Phase 36 \u2014 only cacheable when the response status is 200.
 * Cloudflare Workers Cache silently rejects non-200; spelling it
 * out keeps the contract obvious to readers.
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
 *   const response = await edgeCacheServe(opts, async () => {
 *     const bytes = await origin.fetch(...);
 *     return new Response(bytes, { headers: { ... } });
 *   });
 *
 * The fresh-builder is invoked only on cache miss. The returned
 * Response is the one to send back to the client (cached or fresh).
 *
 * The fresh-builder MUST attach Content-Type + Cache-Control
 * headers; this helper doesn't second-guess them. Cache-Control
 * is what Cloudflare's edge layer reads; without it the
 * response defaults to no-store.
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
