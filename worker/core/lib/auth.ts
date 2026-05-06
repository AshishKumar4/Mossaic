import { SignJWT, jwtVerify } from "jose";
import type { Context, MiddlewareHandler } from "hono";
import { JWT_EXPIRATION_MS } from "../../../shared/constants";
import type { EnvCore as Env } from "../../../shared/types";

/**
 * Thrown at request-time when `JWT_SECRET` is missing or empty.
 *
 * The class is exported so callers (route handlers, middleware, the SDK)
 * can `instanceof`-discriminate this from a generic `Error` and surface
 * a 503 / configuration-error response rather than a 500.
 *
 * Deliberately NOT thrown at module load — Workers evaluate modules
 * eagerly during deploy, and a load-time throw would brick the entire
 * Worker (including the legacy `/api/upload`/`/api/download` paths that
 * don't need JWT) on any tenant whose secret rollout was incomplete.
 * Request-time evaluation localises the failure to the auth surface.
 */
export class VFSConfigError extends Error {
  readonly code = "VFS_CONFIG_ERROR" as const;
  constructor(message: string) {
    super(message);
    this.name = "VFSConfigError";
  }
}

/**
 * Resolve the JWT signing secret from env. Throws `VFSConfigError`
 * when the variable is missing/empty. There is intentionally NO dev
 * fallback string — a hard-coded fallback in source enables anyone
 * who reads the public repo to mint cross-tenant VFS tokens against
 * any deployment that forgot to run `wrangler secret put JWT_SECRET`.
 *
 * Tests must inject `JWT_SECRET` via the test runner's vars (see
 * `tests/wrangler.test.jsonc`).
 */
function getSecret(env: Env): Uint8Array {
  const secret = env.JWT_SECRET;
  if (typeof secret !== "string" || secret.length === 0) {
    throw new VFSConfigError(
      "JWT_SECRET is not configured. Set it via `wrangler secret put JWT_SECRET` " +
        "before deploying. Refusing to sign or verify tokens with a missing/empty secret.",
    );
  }
  return new TextEncoder().encode(secret);
}

/**
 * Resolve the optional rotation-window previous secret. Returns null
 * when `JWT_SECRET_PREVIOUS` is unset or empty (the steady-state).
 *
 * During a graceful rotation, the operator deploys with both env vars
 * set: `JWT_SECRET` = NEW value, `JWT_SECRET_PREVIOUS` = OLD value.
 * `verifyJWT` / `verifyVFSToken` accept tokens signed with EITHER
 * secret; signing always uses the NEW one. After every issued token's
 * TTL has elapsed (~30 d / 15 m), the operator unsets
 * `JWT_SECRET_PREVIOUS` and the rotation is complete with zero
 * dropped sessions. See OPERATIONS.md §6.10.
 */
function getPreviousSecretMaybe(env: Env): Uint8Array | null {
  const prev = env.JWT_SECRET_PREVIOUS;
  if (typeof prev !== "string" || prev.length === 0) return null;
  return new TextEncoder().encode(prev);
}

/**
 * Verify a token against the current secret first, falling through to
 * `JWT_SECRET_PREVIOUS` when set and the current verification rejects.
 *
 * Returns the verified jose payload on success, or `null` on any
 * failure (bad signature against both secrets, expired, malformed).
 * Throws `VFSConfigError` (propagated from `getSecret`) when the
 * primary secret is unset.
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
 * Resolve the secret used to HMAC listFiles cursors. Same source as
 * `JWT_SECRET` (we deliberately reuse the one Workers secret rather
 * than introducing a second). Throws `VFSConfigError` on
 * missing/empty — there is intentionally NO dev fallback string.
 *
 * This mirrors the C1 audit fix on `getSecret`. The cursor codec at
 * `worker/core/lib/cursor.ts` accepts a string secret (so it can be
 * unit-tested with an explicit value); the only production caller —
 * `vfsListFiles` — must route through this helper so a deploy
 * without `JWT_SECRET` cannot silently fall back to a public string
 * that any reader of this open-source repo could replay.
 *
 * Returns the raw string (the cursor codec hashes it itself via
 * `crypto.subtle.importKey`).
 */
export function getCursorSecret(env: { JWT_SECRET?: string }): string {
  const secret = env.JWT_SECRET;
  if (typeof secret !== "string" || secret.length === 0) {
    throw new VFSConfigError(
      "JWT_SECRET is not configured. Set it via `wrangler secret put JWT_SECRET` " +
        "before deploying. Refusing to sign or verify listFiles cursors with a " +
        "missing/empty secret.",
    );
  }
  return secret;
}

/**
 * Sign a JWT with userId and email claims.
 */
export async function signJWT(
  env: Env,
  payload: { userId: string; email: string }
): Promise<string> {
  const secret = getSecret(env);
  return new SignJWT({ sub: payload.userId, email: payload.email })
    .setProtectedHeader({ alg: "HS256" })
    .setIssuedAt()
    // jose's `setExpirationTime` interprets numeric input as
    // seconds-since-epoch (per the JWT `exp` claim spec). The previous
    // call passed milliseconds, producing tokens with `exp` ~year 57000
    // — i.e. never-expiring sessions. Convert to seconds.
    .setExpirationTime(Math.floor((Date.now() + JWT_EXPIRATION_MS) / 1000))
    .sign(secret);
}

/**
 * Verify a JWT and return the payload.
 */
export async function verifyJWT(
  env: Env,
  token: string
): Promise<{ userId: string; email: string } | null> {
  // Multi-secret aware. `verifyAgainstSecrets` resolves the primary
  // secret OUTSIDE its inner try/catch so a missing JWT_SECRET still
  // surfaces as VFSConfigError (503) instead of a silent 401.
  const result = await verifyAgainstSecrets(env, token);
  if (result === null) return null;
  const { payload } = result;
  if (!payload.sub || !payload.email) return null;
  return { userId: payload.sub, email: payload.email as string };
}

/**
 * Auth middleware for Hono. Extracts JWT from Authorization header
 * and sets userId/email on the context.
 */
export function authMiddleware(): MiddlewareHandler<{
  Bindings: Env;
  Variables: { userId: string; email: string };
}> {
  return async (c, next) => {
    const authHeader = c.req.header("Authorization");
    if (!authHeader?.startsWith("Bearer ")) {
      return c.json({ error: "Unauthorized" }, 401);
    }

    const token = authHeader.slice(7);
    let payload: { userId: string; email: string } | null;
    try {
      payload = await verifyJWT(c.env, token);
    } catch (err) {
      // JWT_SECRET unset → 503 service-misconfigured, not 401.
      if (err instanceof VFSConfigError) {
        return c.json({ error: err.message }, 503);
      }
      throw err;
    }
    if (!payload) {
      return c.json({ error: "Invalid or expired token" }, 401);
    }

    c.set("userId", payload.userId);
    c.set("email", payload.email);
    await next();
  };
}

// ── VFS token surface (sdk-impl-plan §6.4) ─────────────────────────────
//
// The VFS uses a *separate* token shape from the legacy app's
// {sub, email} login JWTs. VFS tokens carry the multi-tenant scope
// (ns, tenant, optional sub) and a literal `scope: "vfs"` claim that
// the verifier asserts. This keeps legacy-issued tokens from accessing
// VFS data and vice versa, even though both flow through the same
// JWT_SECRET.
//
// The legacy `verifyJWT` returns null for VFS tokens because they don't
// carry `email`; that's the natural boundary. The new `verifyVFSToken`
// requires `scope === "vfs"` so a hand-crafted token without that claim
// is rejected.

/** Wire shape of the parsed VFS token. */
export interface VFSTokenPayload {
  /** namespace */
  ns: string;
  /** tenant id */
  tn: string;
  /** optional sub-tenant id */
  sub?: string;
  /** scope sentinel — must equal "vfs". */
  scope: "vfs";
}

/**
 * Sign a VFS-scoped JWT. Operator-side helper — the SDK calls this with
 * an API key (separately validated) before handing the token to a
 * downstream consumer.
 */
export async function signVFSToken(
  env: Env,
  payload: { ns: string; tenant: string; sub?: string },
  ttlMs: number = JWT_EXPIRATION_MS
): Promise<string> {
  const secret = getSecret(env);
  const claims: Record<string, unknown> = {
    scope: "vfs",
    ns: payload.ns,
    tn: payload.tenant,
  };
  if (payload.sub !== undefined) claims.sub = payload.sub;
  return new SignJWT(claims)
    .setProtectedHeader({ alg: "HS256" })
    .setIssuedAt()
    // Seconds-since-epoch per JWT `exp` claim spec.
    .setExpirationTime(Math.floor((Date.now() + ttlMs) / 1000))
    .sign(secret);
}

/**
 * Verify a VFS-scoped JWT. Returns the parsed payload or null on any
 * failure (bad signature, expired, missing claims, wrong scope).
 *
 * The `scope === "vfs"` check is the load-bearing guard: a legacy login
 * token would not carry that claim and is rejected here.
 */
export async function verifyVFSToken(
  env: Env,
  token: string
): Promise<VFSTokenPayload | null> {
  // Multi-secret aware. `verifyAgainstSecrets` raises VFSConfigError
  // when the primary secret is unset (→ 503 at the route); on
  // signature mismatch against the current secret it falls through
  // to JWT_SECRET_PREVIOUS when set.
  const result = await verifyAgainstSecrets(env, token);
  if (result === null) return null;
  const { payload } = result;
  if (payload.scope !== "vfs") return null;
  if (typeof payload.ns !== "string" || payload.ns.length === 0) return null;
  if (typeof payload.tn !== "string" || payload.tn.length === 0) return null;
  const sub =
    typeof payload.sub === "string" && payload.sub.length > 0
      ? payload.sub
      : undefined;
  return { ns: payload.ns, tn: payload.tn, sub, scope: "vfs" };
}

// ── multipart session + download tokens ──────────────────────
//
// Two new HMAC token shapes layered onto the same `JWT_SECRET` via
// scope-discrimination claims:
//
//   - `scope: "vfs-mp"` — multipart upload session token. Mints at
//     `beginMultipart`, validates per-chunk PUTs without a UserDO RPC.
//     Carries enough state (`poolSize`, `totalChunks`, `chunkSize`,
//     `totalSize`) for the chunk PUT route to compute placement and
//     enforce caps without consulting the DO.
//
//   - `scope: "vfs-dl"` — cacheable-chunk download token. Mints at
//     `mintDownloadToken`, validates per-chunk GETs against the
//     browser-direct cacheable endpoint. Short-lived (1h default).
//
// Same JWT_SECRET signs all three (`vfs`, `vfs-mp`, `vfs-dl`); each
// verify function rejects tokens lacking its sentinel — RFC 8725 §2.8
// scope-binding pattern. Cross-purpose forgery is impossible without
// the secret.

import {
  VFS_MP_SCOPE,
  VFS_DL_SCOPE,
  MULTIPART_DEFAULT_TTL_MS,
  MULTIPART_MAX_TTL_MS,
  DOWNLOAD_TOKEN_DEFAULT_TTL_MS,
  type MultipartSessionTokenPayload,
  type DownloadTokenPayload,
} from "../../../shared/multipart";

/**
 * Sign a multipart session token. Called once per `beginMultipart` —
 * the resulting token is presented on every subsequent chunk PUT to
 * authorise it without a UserDO round-trip.
 *
 * `poolSize` is the snapshotted-at-begin pool size; freezing it in
 * the token guarantees `placeChunk(uid, uploadId, idx, poolSize)`
 * stays stable across the session even if the tenant's pool grows
 * between begin and finalize.
 */
export async function signVFSMultipartToken(
  env: Env,
  payload: Omit<MultipartSessionTokenPayload, "scope" | "iat" | "exp">,
  ttlMs: number = MULTIPART_DEFAULT_TTL_MS
): Promise<{ token: string; expiresAtMs: number }> {
  const secret = getSecret(env);
  const ttl = Math.min(Math.max(ttlMs, 60_000), MULTIPART_MAX_TTL_MS);
  const expiresAtMs = Date.now() + ttl;
  const claims: Record<string, unknown> = {
    scope: VFS_MP_SCOPE,
    uploadId: payload.uploadId,
    ns: payload.ns,
    tn: payload.tn,
    poolSize: payload.poolSize,
    totalChunks: payload.totalChunks,
    chunkSize: payload.chunkSize,
    totalSize: payload.totalSize,
  };
  if (payload.sub !== undefined) claims.sub = payload.sub;
  const token = await new SignJWT(claims)
    .setProtectedHeader({ alg: "HS256" })
    .setIssuedAt()
    .setExpirationTime(Math.floor(expiresAtMs / 1000))
    .sign(secret);
  return { token, expiresAtMs };
}

/**
 * Verify a multipart session token. Returns parsed payload or null
 * on any failure. CPU-only; no DO round-trip.
 *
 * The `scope === "vfs-mp"` sentinel rejects `vfs` and `vfs-dl`
 * tokens — even though they share JWT_SECRET, they cannot be replayed
 * across surfaces.
 */
export async function verifyVFSMultipartToken(
  env: Env,
  token: string
): Promise<MultipartSessionTokenPayload | null> {
  // Multi-secret aware (Phase 23 Fix 2). Tokens minted under the OLD
  // JWT_SECRET stay valid through a rotation window so in-flight
  // multipart sessions don't get killed when an operator rotates the
  // signing key — see `docs/operations.md` §6.10.
  const result = await verifyAgainstSecrets(env, token);
  if (result === null) return null;
  try {
    const { payload } = result;
    if (payload.scope !== VFS_MP_SCOPE) return null;
    if (typeof payload.uploadId !== "string" || payload.uploadId.length === 0)
      return null;
    if (typeof payload.ns !== "string" || payload.ns.length === 0) return null;
    if (typeof payload.tn !== "string" || payload.tn.length === 0) return null;
    if (
      typeof payload.poolSize !== "number" ||
      !Number.isInteger(payload.poolSize) ||
      payload.poolSize < 1
    )
      return null;
    if (
      typeof payload.totalChunks !== "number" ||
      !Number.isInteger(payload.totalChunks) ||
      payload.totalChunks < 0
    )
      return null;
    if (
      typeof payload.chunkSize !== "number" ||
      !Number.isInteger(payload.chunkSize) ||
      payload.chunkSize < 0
    )
      return null;
    if (
      typeof payload.totalSize !== "number" ||
      !Number.isInteger(payload.totalSize) ||
      payload.totalSize < 0
    )
      return null;
    const sub =
      typeof payload.sub === "string" && payload.sub.length > 0
        ? payload.sub
        : undefined;
    const iat = typeof payload.iat === "number" ? payload.iat : 0;
    const exp = typeof payload.exp === "number" ? payload.exp : 0;
    return {
      scope: VFS_MP_SCOPE,
      uploadId: payload.uploadId,
      ns: payload.ns,
      tn: payload.tn,
      sub,
      poolSize: payload.poolSize,
      totalChunks: payload.totalChunks,
      chunkSize: payload.chunkSize,
      totalSize: payload.totalSize,
      iat,
      exp,
    };
  } catch {
    return null;
  }
}

/**
 * Sign a download token. Tied to a specific `fileId` (not `path`) so
 * it doesn't accidentally grant access if the path's content changes
 * via a subsequent write — `fileId` is the immutable head's identity.
 */
export async function signVFSDownloadToken(
  env: Env,
  payload: Omit<DownloadTokenPayload, "scope" | "iat" | "exp">,
  ttlMs: number = DOWNLOAD_TOKEN_DEFAULT_TTL_MS
): Promise<{ token: string; expiresAtMs: number }> {
  const secret = getSecret(env);
  const ttl = Math.min(Math.max(ttlMs, 60_000), MULTIPART_MAX_TTL_MS);
  const expiresAtMs = Date.now() + ttl;
  const claims: Record<string, unknown> = {
    scope: VFS_DL_SCOPE,
    fileId: payload.fileId,
    ns: payload.ns,
    tn: payload.tn,
  };
  if (payload.sub !== undefined) claims.sub = payload.sub;
  const token = await new SignJWT(claims)
    .setProtectedHeader({ alg: "HS256" })
    .setIssuedAt()
    .setExpirationTime(Math.floor(expiresAtMs / 1000))
    .sign(secret);
  return { token, expiresAtMs };
}

/**
 * Verify a download token.
 *
 * Multi-secret aware (Phase 23 Fix 2). Pre-minted download URLs
 * (e.g. shareable thumbnail links handed to a CDN) stay valid
 * through a JWT_SECRET rotation window — see `docs/operations.md`
 * §6.10.
 */
export async function verifyVFSDownloadToken(
  env: Env,
  token: string
): Promise<DownloadTokenPayload | null> {
  const result = await verifyAgainstSecrets(env, token);
  if (result === null) return null;
  try {
    const { payload } = result;
    if (payload.scope !== VFS_DL_SCOPE) return null;
    if (typeof payload.fileId !== "string" || payload.fileId.length === 0)
      return null;
    if (typeof payload.ns !== "string" || payload.ns.length === 0) return null;
    if (typeof payload.tn !== "string" || payload.tn.length === 0) return null;
    const sub =
      typeof payload.sub === "string" && payload.sub.length > 0
        ? payload.sub
        : undefined;
    const iat = typeof payload.iat === "number" ? payload.iat : 0;
    const exp = typeof payload.exp === "number" ? payload.exp : 0;
    return {
      scope: VFS_DL_SCOPE,
      fileId: payload.fileId,
      ns: payload.ns,
      tn: payload.tn,
      sub,
      iat,
      exp,
    };
  } catch {
    return null;
  }
}
