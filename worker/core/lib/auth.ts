import { SignJWT, jwtVerify } from "jose";
import type { Context, MiddlewareHandler } from "hono";
import { JWT_EXPIRATION_MS } from "../../../shared/constants";
import type { Env } from "../../../shared/types";

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
    .setExpirationTime(Date.now() + JWT_EXPIRATION_MS)
    .sign(secret);
}

/**
 * Verify a JWT and return the payload.
 */
export async function verifyJWT(
  env: Env,
  token: string
): Promise<{ userId: string; email: string } | null> {
  // Resolve the secret OUTSIDE the try/catch so a missing JWT_SECRET
  // surfaces as VFSConfigError (503) instead of being silently swallowed
  // as "invalid token" (which would map to 401 and leak no signal that
  // the deploy is mis-configured).
  const secret = getSecret(env);
  try {
    const { payload } = await jwtVerify(token, secret);
    if (!payload.sub || !payload.email) return null;
    return { userId: payload.sub, email: payload.email as string };
  } catch {
    return null;
  }
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
    .setExpirationTime(Date.now() + ttlMs)
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
  // Same rationale as verifyJWT: resolve the secret OUTSIDE the catch
  // so a missing JWT_SECRET surfaces as VFSConfigError up to the route
  // and turns into a 503, not a silent 401 "invalid token".
  const secret = getSecret(env);
  try {
    const { payload } = await jwtVerify(token, secret);
    if (payload.scope !== "vfs") return null;
    if (typeof payload.ns !== "string" || payload.ns.length === 0) return null;
    if (typeof payload.tn !== "string" || payload.tn.length === 0) return null;
    const sub =
      typeof payload.sub === "string" && payload.sub.length > 0
        ? payload.sub
        : undefined;
    return { ns: payload.ns, tn: payload.tn, sub, scope: "vfs" };
  } catch {
    return null;
  }
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
  const secret = getSecret(env);
  try {
    const { payload } = await jwtVerify(token, secret);
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

/** Verify a download token. */
export async function verifyVFSDownloadToken(
  env: Env,
  token: string
): Promise<DownloadTokenPayload | null> {
  const secret = getSecret(env);
  try {
    const { payload } = await jwtVerify(token, secret);
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
