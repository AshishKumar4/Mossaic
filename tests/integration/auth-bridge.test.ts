import { describe, it, expect, beforeAll } from "vitest";
import { SELF, env } from "cloudflare:test";

/**
 * Auth-bridge tests.
 *
 * `POST /api/auth/vfs-token` exchanges an App session JWT for a
 * short-TTL VFS Bearer token bound to the authenticated user's
 * tenant. The SPA uses this to drive canonical /api/vfs/* with the
 * same trust boundary as the App's /api/auth, /api/files, etc.
 *
 * Cases:
 *   T-Auth1 — valid session JWT mints a token whose payload
 *             carries `{ ns:"default", tn:userId, scope:"vfs" }`.
 *   T-Auth2 — missing/empty Authorization header → 401.
 *   T-Auth3 — invalid (signature mismatch) Authorization → 401.
 *   T-Auth4 — TTL ≤ 15 min from issuance.
 *   T-Auth5 — minted token authenticates a canonical
 *             /api/vfs/exists call for the same tenant.
 *   T-Auth6 — cross-tenant: userA's token presented to canonical
 *             /api/vfs/* with userB's tenant fails — but since the
 *             canonical surface derives the tenant from the token
 *             itself, the failure mode is "userA's token only ever
 *             addresses userA's UserDO; userB's data is structurally
 *             unreachable". We assert this by reading a file
 *             written under userA's tenant comes back to A's token
 *             and is invisible to a different-tenant token.
 */

import { verifyVFSToken, signJWT } from "@core/lib/auth";

interface E {
  MOSSAIC_USER: DurableObjectNamespace;
  JWT_SECRET?: string;
}
const TEST_ENV = env as unknown as E;

let aliceUserId: string;
let aliceSessionJWT: string;
let bobUserId: string;
let bobSessionJWT: string;

import { signup as signupAndLogin } from "./_helpers";

async function bridgeMint(jwt: string): Promise<Response> {
  return SELF.fetch("https://test/api/auth/vfs-token", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${jwt}`,
      "Content-Type": "application/json",
    },
  });
}

beforeAll(async () => {
  // Two distinct accounts seeded once for cross-tenant tests.
  const a = await signupAndLogin("alice-bridge@test.example");
  aliceUserId = a.userId;
  aliceSessionJWT = a.jwt;
  const b = await signupAndLogin("bob-bridge@test.example");
  bobUserId = b.userId;
  bobSessionJWT = b.jwt;
  expect(aliceUserId).not.toBe(bobUserId);
});

describe("auth-bridge — POST /api/auth/vfs-token", () => {
  it("T-Auth1 — valid session JWT mints a token bound to the authenticated tenant", async () => {
    const t0 = Date.now();
    const res = await bridgeMint(aliceSessionJWT);
    expect(res.status).toBe(200);
    const body = (await res.json()) as {
      token: string;
      expiresAtMs: number;
    };
    expect(typeof body.token).toBe("string");
    expect(body.token.length).toBeGreaterThan(0);
    expect(typeof body.expiresAtMs).toBe("number");
    expect(body.expiresAtMs).toBeGreaterThan(t0);

    const payload = await verifyVFSToken(TEST_ENV as never, body.token);
    expect(payload).not.toBeNull();
    expect(payload!.scope).toBe("vfs");
    expect(payload!.ns).toBe("default");
    expect(payload!.tn).toBe(aliceUserId);
    // sub is unset for App-bridge tokens (no sub-tenants in the App).
    expect(payload!.sub).toBeUndefined();
  });

  it("T-Auth2 — missing Authorization header → 401", async () => {
    const res = await SELF.fetch("https://test/api/auth/vfs-token", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
    });
    expect(res.status).toBe(401);
  });

  it("T-Auth3 — tampered signature → 401", async () => {
    // Take a valid session JWT, flip a byte in the signature.
    const parts = aliceSessionJWT.split(".");
    expect(parts.length).toBe(3);
    const sig = parts[2]!;
    // Flip the first character of the signature segment (base64url).
    const mutated =
      sig[0] === "A" ? "B" + sig.slice(1) : "A" + sig.slice(1);
    const tampered = `${parts[0]}.${parts[1]}.${mutated}`;
    const res = await SELF.fetch("https://test/api/auth/vfs-token", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${tampered}`,
        "Content-Type": "application/json",
      },
    });
    expect(res.status).toBe(401);
  });

  it("T-Auth4 — TTL ≤ 15 minutes from issuance", async () => {
    const t0 = Date.now();
    const res = await bridgeMint(aliceSessionJWT);
    expect(res.status).toBe(200);
    const body = (await res.json()) as { expiresAtMs: number };
    const ttlMs = body.expiresAtMs - t0;
    // 15 min = 900_000 ms. Allow 5 s of clock-drift slack on either side.
    expect(ttlMs).toBeGreaterThan(15 * 60 * 1000 - 5_000);
    expect(ttlMs).toBeLessThanOrEqual(15 * 60 * 1000 + 5_000);
  });

  it("T-Auth5 — minted token authenticates canonical /api/vfs/exists", async () => {
    const mintRes = await bridgeMint(aliceSessionJWT);
    const { token } = (await mintRes.json()) as { token: string };

    // /api/vfs/exists — alice's tenant root. Canonical route accepts
    // the bridge-minted token. Root always exists on a freshly-
    // initialised UserDO (`folders` table seeded with the root row).
    const res = await SELF.fetch("https://test/api/vfs/exists", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ path: "/" }),
    });
    expect(res.status).toBe(200);
    const body = (await res.json()) as { exists: boolean };
    expect(typeof body.exists).toBe("boolean");
    expect(body.exists).toBe(true);
  });

  it("T-Auth6 — cross-tenant: bob's bridge token cannot reach alice's data", async () => {
    // Alice writes via her bridge token; bob's bridge token addresses
    // a DIFFERENT canonical UserDO and sees an empty filesystem.
    const aliceMint = await bridgeMint(aliceSessionJWT);
    const aliceTok = ((await aliceMint.json()) as { token: string }).token;
    const bobMint = await bridgeMint(bobSessionJWT);
    const bobTok = ((await bobMint.json()) as { token: string }).token;

    // Alice writes /secret.txt
    const writeRes = await SELF.fetch("https://test/api/vfs/writeFile", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${aliceTok}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        path: "/secret.txt",
        encoding: "utf8",
        data: "alice-only",
      }),
    });
    expect(writeRes.status).toBe(200);

    // Alice can read it.
    const aliceReads = await SELF.fetch("https://test/api/vfs/exists", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${aliceTok}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ path: "/secret.txt" }),
    });
    const aliceBody = (await aliceReads.json()) as { exists: boolean };
    expect(aliceBody.exists).toBe(true);

    // Bob's token addresses bob's UserDO — alice's file does NOT
    // exist there. This is structural tenant isolation
    // (Tenant.lean cross_tenant_user_isolation) — bob cannot
    // forge alice's tenant claim because the bridge mint pins
    // `tn` to the validated session JWT's userId.
    const bobReads = await SELF.fetch("https://test/api/vfs/exists", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${bobTok}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ path: "/secret.txt" }),
    });
    const bobBody = (await bobReads.json()) as { exists: boolean };
    expect(bobBody.exists).toBe(false);
  });
});

describe("auth-bridge — token claims discriminator", () => {
  it("rejects an App session JWT used directly on canonical /api/vfs/*", async () => {
    // The App session JWT has scope=undefined (it's a {sub, email}
    // token from signJWT). The canonical /api/vfs/* verifier
    // requires `scope === "vfs"`. So presenting the session JWT
    // as a canonical Bearer must fail.
    const res = await SELF.fetch("https://test/api/vfs/exists", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${aliceSessionJWT}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ path: "/" }),
    });
    expect(res.status).toBe(401);
  });

  it("a hand-crafted JWT with no scope claim is rejected by canonical", async () => {
    // signJWT creates {sub, email} tokens (App session shape). They
    // must not authenticate canonical /api/vfs/* routes.
    const fakeSessionJwt = await signJWT(TEST_ENV as never, {
      userId: aliceUserId,
      email: "alice-bridge@test.example",
    });
    const res = await SELF.fetch("https://test/api/vfs/exists", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${fakeSessionJwt}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ path: "/" }),
    });
    expect(res.status).toBe(401);
  });
});
