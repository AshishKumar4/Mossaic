/**
 * SPA HTTP transfer client.
 *
 * The SPA upload/download hooks (`use-upload.ts`, `use-download.ts`)
 * collapse onto `@mossaic/sdk` 's `parallelUpload` /
 * `parallelDownload`. The HttpVFS instance speaks to the canonical
 * `/api/vfs/multipart/*` and `/api/vfs/chunk/*` routes — the same
 * surface every SDK consumer (CLI, third-party Workers) addresses.
 *
 * Authentication uses the auth-bridge: a short-TTL VFS Bearer token
 * minted via `POST /api/auth/vfs-token` (gated by the App session
 * JWT). The token's `tn` claim is pinned server-side to the
 * authenticated user's tenant, so cross-tenant impersonation is
 * impossible without forging the session JWT.
 *
 * The client is cached per session — `getTransferClient()` returns
 * the same instance until `resetTransferClient()` is called (e.g.
 * on logout / token refresh). The underlying VFS token is cached
 * inside `api.getVfsToken()` and refreshed automatically on
 * near-expiry.
 */

import { createMossaicHttpClient, type HttpVFS } from "@mossaic/sdk/http";
import { api } from "./api";

let client: HttpVFS | null = null;

/**
 * Get the cached SDK HTTP client, lazily constructed against a
 * freshly-minted VFS Bearer token. Returns the same instance until
 * {@link resetTransferClient} is called.
 *
 * Async because the auth-bridge requires a network round-trip to
 * mint the VFS token. Both call sites (`use-upload`, `use-download`)
 * are inside async functions so this is transparent.
 *
 * Throws if no App session JWT is set (caller must be authenticated)
 * or the auth-bridge endpoint returns an error (e.g. 503 when
 * JWT_SECRET is unset on the worker).
 */
export async function getTransferClient(): Promise<HttpVFS> {
  if (!client) {
    const vfsToken = await api.getVfsToken();
    client = createMossaicHttpClient({
      url: window.location.origin,
      apiKey: vfsToken,
    });
  }
  return client;
}

/**
 * Drop the cached client. Call on logout, token refresh, or any
 * point where the VFS token becomes stale. The next
 * {@link getTransferClient} call re-mints.
 */
export function resetTransferClient(): void {
  client = null;
  api.clearVfsToken();
}
