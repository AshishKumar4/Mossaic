import { SignJWT, jwtVerify } from "jose";
import type { Context, MiddlewareHandler } from "hono";
import { JWT_EXPIRATION_MS } from "@shared/constants";
import type { Env } from "@shared/types";

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
