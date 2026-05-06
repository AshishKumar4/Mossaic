import { Hono, type Context } from "hono";
import type { EnvApp as Env } from "@shared/types";
import { signJWT, VFSConfigError } from "@core/lib/auth";

const auth = new Hono<{ Bindings: Env }>();

type AuthCtx = Context<{ Bindings: Env }>;

/**
 * Mint a JWT, mapping a missing-secret VFSConfigError to a clean
 * 503 instead of a generic 500. Auth signup/login pre-existed but
 * inherited the same JWT_SECRET; they now share the same fail-mode.
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

  // Route to a UserDO named by email (consistent routing)
  const doId = c.env.MOSSAIC_USER.idFromName(`auth:${email}`);
  const stub = c.env.MOSSAIC_USER.get(doId);

  const res = await stub.fetch(
    new Request("http://internal/signup", {
      method: "POST",
      body: JSON.stringify({ email, password }),
    })
  );

  if (!res.ok) {
    const err = (await res.json()) as { error: string };
    return c.json({ error: err.error }, res.status as 400);
  }

  const result = (await res.json()) as { userId: string; email: string };
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
 */
auth.post("/login", async (c) => {
  const { email, password } = await c.req.json<{
    email: string;
    password: string;
  }>();

  if (!email || !password) {
    return c.json({ error: "Email and password are required" }, 400);
  }

  const doId = c.env.MOSSAIC_USER.idFromName(`auth:${email}`);
  const stub = c.env.MOSSAIC_USER.get(doId);

  const res = await stub.fetch(
    new Request("http://internal/login", {
      method: "POST",
      body: JSON.stringify({ email, password }),
    })
  );

  if (!res.ok) {
    const err = (await res.json()) as { error: string };
    return c.json({ error: err.error }, res.status as 401);
  }

  const result = (await res.json()) as { userId: string; email: string };
  const tokenOrResp = await mintJWT(c, result);
  if (typeof tokenOrResp !== "string") return tokenOrResp;

  return c.json({
    token: tokenOrResp,
    userId: result.userId,
    email: result.email,
  });
});

export default auth;
