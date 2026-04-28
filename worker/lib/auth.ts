import { SignJWT, jwtVerify } from "jose";
import type { Context, MiddlewareHandler } from "hono";
import { JWT_EXPIRATION_MS } from "@shared/constants";
import type { Env } from "@shared/types";

const DEFAULT_SECRET = "mossaic-dev-secret-change-in-production";

function getSecret(env: Env): Uint8Array {
  const secret = env.JWT_SECRET || DEFAULT_SECRET;
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
  try {
    const secret = getSecret(env);
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
    const payload = await verifyJWT(c.env, token);
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
  try {
    const secret = getSecret(env);
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
