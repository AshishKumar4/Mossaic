import { Hono, type Context } from "hono";
import type { EnvApp as Env } from "@shared/types";
import { signJWT, VFSConfigError } from "@core/lib/auth";
import { userStubByName } from "../lib/user-stub";

const auth = new Hono<{ Bindings: Env }>();

type AuthCtx = Context<{ Bindings: Env }>;

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

export default auth;
