/**
 * Signed preview-variant tokens.
 *
 * Without these, thumbnail grids hammer the Worker
 * bytes-through-RPC even on cache hit because every grid item
 * would issue an authenticated `POST /readPreview` whose response
 * body is the variant bytes. The browser cannot directly cache
 * that response cross-page, and a CDN edge tier sees opaque
 * Authorization-bearing requests it must serialize through the
 * Worker.
 *
 * Signed-URL pattern: the Worker mints a per-variant token whose
 * payload encodes (tenantId, fileId, headVersionId, variantKind,
 * rendererKind, format, contentHash, exp). The browser fetches
 * `GET /api/vfs/preview-variant/<token>` directly. Bytes are
 * content-addressed by `contentHash` so the response is
 * `Cache-Control: public, max-age=31536000, immutable` and the
 * CDN edge happily caches across all clients (no Vary).
 *
 * Why a separate token shape (not reusing VFSDownloadToken):
 *
 *   1. **Different payload.** The chunk-download token binds
 *      `(fileId, ns, tn, sub)`; preview-variant tokens additionally
 *      carry `headVersionId`, `variantKind`, `rendererKind`,
 *      `format`, and `contentHash`. Adding those to
 *      VFSDownloadToken would broaden its scope and risk
 *      cross-purpose forgery (a CDN-cached preview-variant token
 *      should NOT be replayable as a raw chunk-download token).
 *
 *   2. **Different cache shape.** Chunk-download is keyed by
 *      `(fileId, idx)`; preview-variant is keyed by `contentHash`
 *      (the hash IS the cache key). RFC 8725 \xa72.8 scope-binding:
 *      each token shape carries a distinct `scope` claim so
 *      verification rejects cross-purpose replay.
 *
 *   3. **Different lifecycle.** Chunk-download tokens have a
 *      ~1h TTL (interactive download). Preview-variant tokens
 *      can live ~24h \u2014 the browser caches the IMG src for the
 *      year-long Cache-Control regardless of token TTL, but the
 *      token only needs to survive the initial fetch + a
 *      reasonable replay window so bookmarked URLs stay valid
 *      across SPA reloads.
 *
 * Multi-secret rotation aware (mirrors `verifyVFSDownloadToken` /
 * `verifyShareToken`). Pre-minted preview URLs survive a
 * `JWT_SECRET` rotation window via `verifyAgainstSecrets`.
 *
 * @lean-invariant Mossaic.Vfs.PreviewToken.scope_binding \u2014 the
 *   scope claim `vfs-pv` rejects any verifyVFSDownloadToken /
 *   verifyVFSToken / verifyShareToken / verifyMultipartToken
 *   forgery attempt that swaps payload between token shapes.
 */

import { jwtVerify } from "jose";
import type { EnvCore as Env } from "../../../shared/types";
import { VFSConfigError, signScopedJwt } from "./auth";

/**
 * Scope sentinel embedded in the JWT payload. Verifiers reject
 * any token whose `scope !== VFS_PREVIEW_SCOPE`. RFC 8725 \xa72.8
 * scope-binding pattern so a `vfs` / `vfs-mp` / `vfs-dl` /
 * `vfs-share` token replayed at the preview-variant route is
 * rejected.
 */
export const VFS_PREVIEW_SCOPE = "vfs-pv" as const;

/**
 * Default TTL for preview tokens \u2014 24 hours. Long enough that an
 * SPA reload re-mints transparently from cached path metadata; the
 * browser's IMG cache already serves the bytes for up to a year
 * once the token survived its first fetch.
 */
export const PREVIEW_TOKEN_DEFAULT_TTL_MS = 24 * 60 * 60 * 1000;

/** Minimum acceptable TTL: 60s. Below this is an operator mistake. */
export const PREVIEW_TOKEN_MIN_TTL_MS = 60 * 1000;

/** Maximum TTL: 30 days. Long-lived preview links beyond this should mint anew. */
export const PREVIEW_TOKEN_MAX_TTL_MS = 30 * 24 * 60 * 60 * 1000;

/**
 * Wire shape of the parsed preview-variant token.
 *
 * Every field is REQUIRED for verification; a missing field
 * causes `verifyPreviewToken` to return null. The completeness is
 * load-bearing: an attacker who substitutes any one of these
 * fields (e.g. swap fileId to a different tenant's file) would
 * present a token with a different signature \u2014 verification
 * fails. The route handler still re-validates `tenantId` matches
 * the resolved scope before serving bytes, so even a perfectly-
 * forged-but-implausible token is rejected at the data layer.
 */
export interface PreviewTokenPayload {
  scope: typeof VFS_PREVIEW_SCOPE;
  /** `<ns>::<tenant>[::<sub>]` — drives DO routing post-verify. */
  tenantId: string;
  /** Stable file identity; bound to a specific row. */
  fileId: string;
  /**
   * head_version_id at mint time, OR null for legacy /
   * versioning-OFF tenants. Mismatched mid-lifetime versions
   * mint new tokens \u2014 the SPA calls `vfs.previewUrl(path)` per
   * grid item per session.
   */
  headVersionId: string | null;
  /**
   * Standard ("thumb"/"medium"/"lightbox") or custom
   * (`c-WxH-fit`) variant key. Same shape as
   * `edge-cache.ts::variantKeyPart`.
   */
  variantKind: string;
  /** Renderer that produced the variant ("image", "code", etc.). */
  rendererKind: string;
  /** Output format ("auto"/"webp"/"jpeg"/...). Free-form. */
  format: string;
  /**
   * SHA-256 hex of the rendered bytes. The cache key on
   * `caches.default` is `https://preview-variant.mossaic.local/<contentHash>`
   * so a stable hash means structural cache hits across all
   * clients. Hash mismatch (e.g. a re-render produced different
   * bytes) reaches the route layer and triggers a re-mint
   * via the standard mint flow.
   */
  contentHash: string;
  /** Issued-at (seconds since epoch). */
  iat: number;
  /** Expiry (seconds since epoch). */
  exp: number;
}

function getSecret(env: Env): Uint8Array {
  const secret = env.JWT_SECRET;
  if (typeof secret !== "string" || secret.length === 0) {
    throw new VFSConfigError(
      "JWT_SECRET is not configured. Set it via `wrangler secret put JWT_SECRET` " +
        "before deploying. Refusing to sign or verify tokens with a missing/empty secret."
    );
  }
  return new TextEncoder().encode(secret);
}

function getPreviousSecretMaybe(env: Env): Uint8Array | null {
  const prev = env.JWT_SECRET_PREVIOUS;
  if (typeof prev !== "string" || prev.length === 0) return null;
  return new TextEncoder().encode(prev);
}

/**
 * Verify against the current secret first, falling through to
 * `JWT_SECRET_PREVIOUS` when set. Mirrors the helper used by
 * `verifyVFSDownloadToken` / `verifyShareToken`. Returns the
 * jose-verified payload or null on any failure.
 */
async function verifyAgainstSecrets(
  env: Env,
  token: string
): Promise<{ payload: import("jose").JWTPayload } | null> {
  const current = getSecret(env);
  try {
    return await jwtVerify(token, current);
  } catch {
    const previous = getPreviousSecretMaybe(env);
    if (previous === null) return null;
    try {
      return await jwtVerify(token, previous);
    } catch {
      return null;
    }
  }
}

/**
 * Sign a preview-variant token. Called by the auth-gated mint
 * RPC `vfsMintPreviewToken` which is the single source of truth
 * for who can mint a token for which file.
 *
 * The mint RPC is responsible for:
 *   - verifying the caller's auth scope grants access to the
 *     path that resolves to `fileId`,
 *   - resolving the current `headVersionId` from `files`,
 *   - looking up the pre-rendered variant in `file_variants`
 *     to obtain `contentHash` (or rendering on demand if no
 *     row exists),
 *   - calling this helper with the verified payload.
 *
 * TTL clamped to [PREVIEW_TOKEN_MIN_TTL_MS, PREVIEW_TOKEN_MAX_TTL_MS].
 */
export async function signPreviewToken(
  env: Env,
  payload: Omit<PreviewTokenPayload, "scope" | "iat" | "exp">,
  ttlMs: number = PREVIEW_TOKEN_DEFAULT_TTL_MS
): Promise<{ token: string; expiresAtMs: number }> {
  const secret = getSecret(env);
  if (typeof payload.tenantId !== "string" || payload.tenantId.length === 0) {
    throw new Error("signPreviewToken: tenantId required");
  }
  if (typeof payload.fileId !== "string" || payload.fileId.length === 0) {
    throw new Error("signPreviewToken: fileId required");
  }
  if (
    payload.headVersionId !== null &&
    (typeof payload.headVersionId !== "string" ||
      payload.headVersionId.length === 0)
  ) {
    throw new Error(
      "signPreviewToken: headVersionId must be string or null"
    );
  }
  if (
    typeof payload.variantKind !== "string" ||
    payload.variantKind.length === 0
  ) {
    throw new Error("signPreviewToken: variantKind required");
  }
  if (
    typeof payload.rendererKind !== "string" ||
    payload.rendererKind.length === 0
  ) {
    throw new Error("signPreviewToken: rendererKind required");
  }
  if (typeof payload.format !== "string" || payload.format.length === 0) {
    throw new Error("signPreviewToken: format required");
  }
  // Loose hex check on contentHash \u2014 the verifier replays the same
  // shape so a mint-time error here surfaces immediately.
  if (
    typeof payload.contentHash !== "string" ||
    !/^[0-9a-f]{64}$/.test(payload.contentHash)
  ) {
    throw new Error(
      "signPreviewToken: contentHash must be 64-char lowercase hex"
    );
  }
  const ttl = Math.min(
    Math.max(ttlMs, PREVIEW_TOKEN_MIN_TTL_MS),
    PREVIEW_TOKEN_MAX_TTL_MS
  );
  const expiresAtMs = Date.now() + ttl;
  const claims: Record<string, unknown> = {
    scope: VFS_PREVIEW_SCOPE,
    tenantId: payload.tenantId,
    fileId: payload.fileId,
    headVersionId: payload.headVersionId,
    variantKind: payload.variantKind,
    rendererKind: payload.rendererKind,
    format: payload.format,
    contentHash: payload.contentHash,
  };
  const token = await signScopedJwt(secret, claims, expiresAtMs);
  return { token, expiresAtMs };
}

/**
 * Verify a preview-variant token. Returns the parsed payload or
 * `null` on any failure (bad signature against either current or
 * previous secret, expired, missing/invalid claims, wrong scope).
 *
 * Multi-secret aware: tokens minted under the OLD `JWT_SECRET`
 * stay valid through a rotation window \u2014 see
 * `docs/operations.md` \xa76.10.
 *
 * The `scope === VFS_PREVIEW_SCOPE` check is load-bearing: a
 * `vfs` / `vfs-mp` / `vfs-dl` / `vfs-share` token replayed at
 * the preview-variant route is rejected.
 */
export async function verifyPreviewToken(
  env: Env,
  token: string
): Promise<PreviewTokenPayload | null> {
  const result = await verifyAgainstSecrets(env, token);
  if (result === null) return null;
  try {
    const { payload } = result;
    if (payload.scope !== VFS_PREVIEW_SCOPE) return null;
    if (typeof payload.tenantId !== "string" || payload.tenantId.length === 0)
      return null;
    if (typeof payload.fileId !== "string" || payload.fileId.length === 0)
      return null;
    // headVersionId may be null (legacy / versioning-OFF) but
    // never undefined or non-string-non-null.
    let headVersionId: string | null;
    if (payload.headVersionId === null) {
      headVersionId = null;
    } else if (
      typeof payload.headVersionId === "string" &&
      payload.headVersionId.length > 0
    ) {
      headVersionId = payload.headVersionId;
    } else {
      return null;
    }
    if (
      typeof payload.variantKind !== "string" ||
      payload.variantKind.length === 0
    )
      return null;
    if (
      typeof payload.rendererKind !== "string" ||
      payload.rendererKind.length === 0
    )
      return null;
    if (typeof payload.format !== "string" || payload.format.length === 0)
      return null;
    if (
      typeof payload.contentHash !== "string" ||
      !/^[0-9a-f]{64}$/.test(payload.contentHash)
    )
      return null;
    const iat = typeof payload.iat === "number" ? payload.iat : 0;
    const exp = typeof payload.exp === "number" ? payload.exp : 0;
    return {
      scope: VFS_PREVIEW_SCOPE,
      tenantId: payload.tenantId,
      fileId: payload.fileId,
      headVersionId,
      variantKind: payload.variantKind,
      rendererKind: payload.rendererKind,
      format: payload.format,
      contentHash: payload.contentHash,
      iat,
      exp,
    };
  } catch {
    return null;
  }
}
