/**
 * Phase 17.6 — SPA HTTP transfer client.
 *
 * The SPA upload/download hooks (`use-upload.ts`, `use-download.ts`)
 * collapse onto `@mossaic/sdk` 's `parallelUpload` /
 * `parallelDownload`. They do so against an `HttpVFS` instance
 * configured to:
 *
 *  1. Send multipart calls to `/api/upload/multipart/*` (App-pinned
 *     bridge route — Phase 17.6 §4).
 *  2. Send cacheable per-chunk GETs to `/api/download/chunk/...`
 *     (App's existing legacy chunk download path — preserves
 *     production data integrity).
 *  3. Authenticate via the App's existing JWT (`api.getToken()`),
 *     not a VFS Bearer token. The App's `authMiddleware` validates
 *     this token at every multipart route.
 *
 * The client is cached per session — `getTransferClient()` returns
 * the same instance until `resetTransferClient()` is called (e.g.
 * on logout / token refresh).
 */

import { createMossaicHttpClient, type HttpVFS } from "@mossaic/sdk/http";
import { api } from "./api";

let client: HttpVFS | null = null;

/**
 * Get the cached SDK HTTP client, lazily constructed against the
 * current session's JWT. Returns the same instance until
 * {@link resetTransferClient} is called.
 *
 * Throws if no token is set on the `api` singleton (caller must be
 * authenticated). Components that render before login should gate
 * the call behind the auth context.
 */
export function getTransferClient(): HttpVFS {
  if (!client) {
    const token = api.getToken();
    if (!token) {
      throw new Error(
        "getTransferClient: no API token set. Ensure the user is authenticated " +
          "before initiating a transfer."
      );
    }
    client = createMossaicHttpClient({
      url: window.location.origin,
      apiKey: token,
      multipartBaseOverride: "/api/upload/multipart",
      chunkFetchBaseOverride: "/api/download",
    });
  }
  return client;
}

/**
 * Drop the cached client. Call on logout, token refresh, or any
 * point where the JWT becomes stale. The next
 * {@link getTransferClient} call rebuilds against the current
 * `api.getToken()`.
 */
export function resetTransferClient(): void {
  client = null;
}
