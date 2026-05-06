import { Hono } from "hono";
import type { Env } from "@shared/types";
import { signJWT } from "../lib/auth";

const auth = new Hono<{ Bindings: Env }>();

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
  const doId = c.env.USER_DO.idFromName(`auth:${email}`);
  const stub = c.env.USER_DO.get(doId);

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
  const token = await signJWT(c.env, result);

  return c.json({
    token,
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

  const doId = c.env.USER_DO.idFromName(`auth:${email}`);
  const stub = c.env.USER_DO.get(doId);

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
  const token = await signJWT(c.env, result);

  return c.json({
    token,
    userId: result.userId,
    email: result.email,
  });
});

export default auth;
