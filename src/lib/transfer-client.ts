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
 * The HttpVFS instance is cached per session and resolves its Bearer
 * token via the {@link ApiKeyProvider} callback on every request —
 * so a long-lived client transparently rotates through multiple
 * VFS tokens (15-min TTL each) without recreating itself.
 * `resetTransferClient()` drops both the cached client and the
 * underlying token cache (e.g. on logout).
 */

import { createMossaicHttpClient, type HttpVFS } from "@mossaic/sdk/http";
import { api } from "./api";

let client: HttpVFS | null = null;

/**
 * Get the cached SDK HTTP client. Construction is synchronous — the
 * Bearer token is fetched lazily, per-request, via
 * {@link api.getVfsToken} which itself caches with a 60-second
 * near-expiry refresh window.
 *
 * Throws if no App session JWT is set (caller must be authenticated).
 * Server-side 503s (e.g. JWT_SECRET unset) surface from the first SDK
 * call as a typed `MossaicUnavailableError`.
 */
export function getTransferClient(): HttpVFS {
  if (!client) {
    client = createMossaicHttpClient({
      url: window.location.origin,
      apiKey: () => api.getVfsToken(),
    });
  }
  return client;
}

/**
 * Drop the cached client. Call on logout or any point where the VFS
 * token becomes invalid. The next {@link getTransferClient} call
 * rebuilds and the next SDK request re-mints the token via the
 * provider callback.
 */
export function resetTransferClient(): void {
  client = null;
  api.clearVfsToken();
}
