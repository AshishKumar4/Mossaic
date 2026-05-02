/**
 * Shared integration-test fixtures.
 *
 * Hoisted from the inline `signup()` / `signupAndLogin()` /
 * `mintVfsToken()` helpers that previously lived in 5+ test files.
 * Each test now imports from here; signup shape is a single
 * `{ userId, jwt }` (not the older `{ userId, token }` mix —
 * call sites that needed `token` aliased to `jwt` already).
 *
 * NEVER imports from production code other than `@shared/*` types.
 * NEVER touches DOs directly — uses the `SELF.fetch` HTTP surface
 * exclusively, exercising the real request pipeline end-to-end.
 *
 * Vitest's worker-pool picks `tests/integration/_helpers.ts` up
 * automatically because it lives in the `tests/integration/`
 * glob. The leading underscore keeps it from being matched as a
 * test file by `*.test.ts`.
 */

import { SELF } from "cloudflare:test";
import { expect } from "vitest";

/** Default password for fixture signup. Tests rarely care about this value. */
export const TEST_PASSWORD = "test-password-123";

/**
 * Signup a fresh tenant by email and return its `userId` + session
 * JWT. Asserts the signup HTTP call returned 200; throws otherwise.
 *
 * Server returns `{ userId, email, token }`; we surface `token`
 * as `jwt` for consistency with the rest of the test surface.
 */
export async function signup(
  email: string,
  password: string = TEST_PASSWORD
): Promise<{ userId: string; jwt: string }> {
  const res = await SELF.fetch("https://test/api/auth/signup", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ email, password }),
  });
  expect(res.status).toBe(200);
  const body = (await res.json()) as { userId: string; token: string };
  return { userId: body.userId, jwt: body.token };
}

/**
 * Mint a VFS Bearer token off an existing session JWT. Asserts 200;
 * returns the token string. Used by tests that exercise the
 * `/api/vfs/...` HTTP surface (or the `/api/files/by-path` proxy).
 */
export async function mintVfsToken(sessionJwt: string): Promise<string> {
  const res = await SELF.fetch("https://test/api/auth/vfs-token", {
    method: "POST",
    headers: { Authorization: `Bearer ${sessionJwt}` },
  });
  expect(res.status).toBe(200);
  const { token } = (await res.json()) as { token: string };
  return token;
}

/**
 * Convenience: signup + immediately mint a VFS Bearer.
 * Returns `{ userId, jwt, vfsToken }` — the most common test setup
 * for HTTP-surface VFS exercises.
 */
export async function setupTenant(
  email: string,
  password: string = TEST_PASSWORD
): Promise<{ userId: string; jwt: string; vfsToken: string }> {
  const { userId, jwt } = await signup(email, password);
  const vfsToken = await mintVfsToken(jwt);
  return { userId, jwt, vfsToken };
}
