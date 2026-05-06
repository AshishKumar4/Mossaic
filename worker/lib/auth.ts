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
