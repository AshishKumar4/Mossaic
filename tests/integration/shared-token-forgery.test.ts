import { describe, it, expect, beforeAll } from "vitest";
import { SELF, env } from "cloudflare:test";
import { SignJWT } from "jose";

/**
 * P0-1 fix — share-token forgery resistance.
 *
 * Pre-fix `worker/app/routes/shared.ts:23,68` did
 * `JSON.parse(atob(token))` — anyone who knew or guessed any
 * userId could forge a token granting access to that user's
 * files. The fix moves token minting to a server-side
 * auth-gated endpoint (`POST /api/auth/share-token`) that
 * HMAC-signs with `JWT_SECRET`; the public read routes verify
 * via `verifyShareToken`.
 *
 * Tests pin:
 *   F1 — pre-fix shape (`btoa(JSON.stringify({...}))`) is REJECTED with 403.
 *   F2 — token signed with the wrong secret → 403.
 *   F3 — token with `scope: "vfs"` (cross-purpose) → 403.
 *   F4 — expired token → 403.
 *   F5 — well-formed mint → photos endpoint returns 200; image
 *        endpoint refuses fileIds NOT in the token.
 */

interface TestEnv {
  MOSSAIC_USER: DurableObjectNamespace;
  JWT_SECRET?: string;
}
const TEST_ENV = env as unknown as TestEnv;

async function signup(
  email: string
): Promise<{ userId: string; jwt: string }> {
  const res = await SELF.fetch("https://test/api/auth/signup", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ email, password: "test-password-123" }),
  });
  expect(res.status).toBe(200);
  const body = (await res.json()) as { userId: string; token: string };
  return { userId: body.userId, jwt: body.token };
}

async function mintShareToken(
  sessionJWT: string,
  fileIds: string[],
  albumName: string
): Promise<{ status: number; body: { token?: string; error?: string } }> {
  const res = await SELF.fetch("https://test/api/auth/share-token", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${sessionJWT}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fileIds, albumName }),
  });
  return {
    status: res.status,
    body: (await res.json()) as { token?: string; error?: string },
  };
}

let aliceUserId: string;
let aliceJWT: string;

beforeAll(async () => {
  const a = await signup("share-forgery-alice@test.example");
  aliceUserId = a.userId;
  aliceJWT = a.jwt;
});

describe("share-token forgery rejected (P0-1)", () => {
  it("F1 — pre-fix shape (unsigned base64 JSON) → 403", async () => {
    // Reconstruct what the pre-fix SPA used to mint.
    const preFixToken = btoa(
      JSON.stringify({
        userId: aliceUserId,
        fileIds: ["any-file-id-the-attacker-claims"],
        albumName: "forged",
      })
    );
    const photosRes = await SELF.fetch(
      `https://test/api/shared/${preFixToken}/photos`
    );
    expect(photosRes.status).toBe(403);
    const photosBody = (await photosRes.json()) as { error: string };
    expect(photosBody.error).toMatch(/invalid|expired|share token/i);

    const imageRes = await SELF.fetch(
      `https://test/api/shared/${preFixToken}/image/any-file-id-the-attacker-claims`
    );
    expect(imageRes.status).toBe(403);
  });

  it("F2 — token signed with the wrong secret → 403", async () => {
    // A token signed with a different secret is structurally a
    // valid JWT but `verifyAgainstSecrets` rejects it.
    const wrongSecret = new TextEncoder().encode("not-the-real-secret");
    const tok = await new SignJWT({
      scope: "vfs-share",
      userId: aliceUserId,
      fileIds: ["x"],
      albumName: "x",
      jti: "deadbeef",
    })
      .setProtectedHeader({ alg: "HS256" })
      .setIssuedAt()
      .setExpirationTime(Math.floor(Date.now() / 1000) + 3600)
      .sign(wrongSecret);
    const res = await SELF.fetch(`https://test/api/shared/${tok}/photos`);
    expect(res.status).toBe(403);
  });

  it("F3 — token with `scope: \"vfs\"` (cross-purpose replay) → 403", async () => {
    // Construct a `vfs`-scoped token signed with the REAL secret;
    // verifyShareToken's scope check must reject it.
    const realSecret = new TextEncoder().encode(
      TEST_ENV.JWT_SECRET ?? "test-secret-for-vitest-only"
    );
    const tok = await new SignJWT({
      scope: "vfs",
      ns: "default",
      tn: aliceUserId,
    })
      .setProtectedHeader({ alg: "HS256" })
      .setIssuedAt()
      .setExpirationTime(Math.floor(Date.now() / 1000) + 3600)
      .sign(realSecret);
    const res = await SELF.fetch(`https://test/api/shared/${tok}/photos`);
    expect(res.status).toBe(403);
  });

  it("F4 — expired token → 403", async () => {
    const realSecret = new TextEncoder().encode(
      TEST_ENV.JWT_SECRET ?? "test-secret-for-vitest-only"
    );
    const tok = await new SignJWT({
      scope: "vfs-share",
      userId: aliceUserId,
      fileIds: ["x"],
      albumName: "x",
      jti: "deadbeef",
    })
      .setProtectedHeader({ alg: "HS256" })
      .setIssuedAt(1) // way in the past
      .setExpirationTime(2) // expired in 1970
      .sign(realSecret);
    const res = await SELF.fetch(`https://test/api/shared/${tok}/photos`);
    expect(res.status).toBe(403);
  });

  it("F5 — server-minted token works; cross-fileId requests refused", async () => {
    // Mint a real token via the auth-gated endpoint.
    const mint = await mintShareToken(
      aliceJWT,
      ["fid-1", "fid-2"],
      "alice's album"
    );
    expect(mint.status).toBe(200);
    expect(typeof mint.body.token).toBe("string");
    const token = mint.body.token!;

    // /photos endpoint should return 200 (album is empty since
    // the fileIds don't resolve to real files, but the token
    // verifies cleanly).
    const photos = await SELF.fetch(
      `https://test/api/shared/${token}/photos`
    );
    expect(photos.status).toBe(200);

    // Image fetch for a fileId NOT in the token's allowlist → 403.
    // The token says ["fid-1","fid-2"]; we request "fid-NOT-IN-TOKEN".
    const wrongImage = await SELF.fetch(
      `https://test/api/shared/${token}/image/fid-NOT-IN-TOKEN`
    );
    expect(wrongImage.status).toBe(403);

    // Image fetch for a fileId IN the allowlist → 404 (file
    // doesn't exist in the tenant; auth passed). Distinguishes
    // auth gate from existence check.
    const inListImage = await SELF.fetch(
      `https://test/api/shared/${token}/image/fid-1`
    );
    expect(inListImage.status).toBe(404);
  });

  it("F6 — share-token mint endpoint requires session auth (401 unauth)", async () => {
    const res = await SELF.fetch("https://test/api/auth/share-token", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ fileIds: ["x"], albumName: "x" }),
    });
    expect(res.status).toBe(401);
  });
});
