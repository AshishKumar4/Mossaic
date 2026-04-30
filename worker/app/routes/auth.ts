import { Hono, type Context } from "hono";
import type { EnvApp as Env } from "@shared/types";
import { authMiddleware, signJWT, signVFSToken, VFSConfigError } from "@core/lib/auth";
import { userStubByName } from "../lib/user-stub";

const auth = new Hono<{
  Bindings: Env;
  Variables: { userId: string; email: string };
}>();

type AuthCtx = Context<{
  Bindings: Env;
  Variables: { userId: string; email: string };
}>;

/**
 * Short-TTL VFS token issued by the auth-bridge. 15 minutes balances
 * SPA UX (one mint covers a typical browsing session) against blast
 * radius (token compromise window).
 */
const VFS_TOKEN_TTL_MS = 15 * 60 * 1000;

/**
 * Mint a JWT, mapping a missing-secret VFSConfigError to a clean
 * 503 instead of a generic 500.
 */
async function mintJWT(
  c: AuthCtx,
  result: { userId: string; email: string }
): Promise<Response | string> {
  try {
    return await signJWT(c.env, result);
  } catch (err) {
    if (err instanceof VFSConfigError) {
      return c.json({ error: err.message }, 503);
    }
    throw err;
  }
}

/**
 * POST /api/auth/signup
 * Create a new user account.
 *
 * replaced direct DO `stub.fetch("http://internal/signup")`
 * with a typed RPC call to `UserDO.appHandleSignup`.
 */
auth.post("/signup", async (c) => {
  const { email, password } = await c.req.json<{
    email: string;
    password: string;
  }>();

  if (!email || !password) {
    return c.json({ error: "Email and password are required" }, 400);
  }

  if (password.length < 8) {
    return c.json({ error: "Password must be at least 8 characters" }, 400);
  }

  // Route to a UserDO named by email (consistent routing — the email
  // namespace is App-specific because the SDK's `vfs:*` form is keyed
  // by tenant, not by an account-discovery email).
  const stub = userStubByName(c.env, `auth:${email}`);

  let result: { userId: string; email: string };
  try {
    result = await stub.appHandleSignup(email, password);
  } catch (err) {
    const message = err instanceof Error ? err.message : "Signup failed";
    return c.json({ error: message }, 400);
  }

  const tokenOrResp = await mintJWT(c, result);
  if (typeof tokenOrResp !== "string") return tokenOrResp;

  return c.json({
    token: tokenOrResp,
    userId: result.userId,
    email: result.email,
  });
});

/**
 * POST /api/auth/login
 * Authenticate and get a JWT.
 *
 * replaced direct DO fetch with `UserDO.appHandleLogin`.
 */
auth.post("/login", async (c) => {
  const { email, password } = await c.req.json<{
    email: string;
    password: string;
  }>();

  if (!email || !password) {
    return c.json({ error: "Email and password are required" }, 400);
  }

  const stub = userStubByName(c.env, `auth:${email}`);

  let result: { userId: string; email: string };
  try {
    result = await stub.appHandleLogin(email, password);
  } catch (err) {
    const message = err instanceof Error ? err.message : "Invalid credentials";
    return c.json({ error: message }, 401);
  }

  const tokenOrResp = await mintJWT(c, result);
  if (typeof tokenOrResp !== "string") return tokenOrResp;

  return c.json({
    token: tokenOrResp,
    userId: result.userId,
    email: result.email,
  });
});

/**
 * POST /api/auth/vfs-token
 * Auth-bridge endpoint. Exchanges the App session JWT for a
 * short-TTL VFS Bearer token bound to the authenticated user's
 * tenant. The SPA calls this at session start (and on near-expiry
 * refresh) and uses the returned token as the `Authorization:
 * Bearer ...` header for canonical `/api/vfs/*` routes.
 *
 * Tenant binding: the minted token's `tn` claim is pinned to the
 * `userId` extracted from the validated session JWT. Callers
 * cannot specify an arbitrary tenant — cross-tenant impersonation
 * is impossible without forging the session JWT (which requires
 * the same `JWT_SECRET`).
 *
 * TTL: 15 minutes. Short enough to limit the blast radius of a
 * compromised token; long enough that an active SPA session
 * doesn't ping this endpoint constantly. Refresh is the SPA's
 * responsibility (`api.getVfsToken()` caches with a 60s safety
 * margin and rebuilds on 401).
 */
auth.post("/vfs-token", authMiddleware(), async (c) => {
  const userId = c.get("userId");
  const expiresAtMs = Date.now() + VFS_TOKEN_TTL_MS;
  let token: string;
  try {
    token = await signVFSToken(
      c.env,
      { ns: "default", tenant: userId },
      VFS_TOKEN_TTL_MS
    );
  } catch (err) {
    if (err instanceof VFSConfigError) {
      return c.json({ error: err.message }, 503);
    }
    throw err;
  }
  return c.json({ token, expiresAtMs });
});

export default auth;
