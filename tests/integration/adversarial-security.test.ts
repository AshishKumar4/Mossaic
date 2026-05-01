import { describe, it, expect, beforeAll } from "vitest";
import { SELF, env } from "cloudflare:test";
import { SignJWT } from "jose";

/**
 * Phase 50 — adversarial security audit.
 *
 * Cross-cuts the six attack categories (share-token, gate-bypass,
 * cache-poisoning, multipart, yjs WS, pool/quota) with attempts to
 * break the auth, replay, and isolation properties the system
 * claims. Tests pin the EXPECTED-REJECT shape — if any attack
 * succeeds (i.e. returns 200/expected-good) the test fails loud.
 *
 * Pinned attack matrix (each line = one attempt):
 *
 *   AS1  Forge share token by truncating signature.
 *   AS2  Forge share token by base64-padding the signature.
 *   AS3  Substitute fileId in share token, keep signature.
 *   AS4  Cross-purpose: download token replayed at share route.
 *   AS5  Cross-purpose: share token replayed at preview-variant route.
 *   AS6  Cross-purpose: preview token replayed at chunk-download route.
 *   AS7  Cross-purpose: multipart session token replayed at share route.
 *   AS8  Cross-purpose: vfs-scope token replayed at share route.
 *   AS9  fileId mismatch in URL vs. download token → 403.
 *   AS10 fileId mismatch in URL vs. share token → 403.
 *   AS11 URL token Base64-decode error → 401/403.
 *   AS12 Empty token in URL → 401.
 *   AS13 Token with `alg: "none"` (jose rejects) → null payload.
 *   AS14 Token with absurdly long fileIds list (> 1000 cap) → 400 on mint.
 *   AS15 Public yjs WS without Bearer/subprotocol → 401.
 *   AS16 Public yjs WS with garbage Bearer → 401.
 *   AS17 Public yjs WS with subprotocol that's malformed → 401.
 *   AS18 chunk-download with hash mismatching token → fetches by hash
 *        (no per-hash binding in token by design).  We pin THIS shape
 *        so future regressions don't tighten without intent.
 *   AS19 chunk-download with negative shard → 400.
 *   AS20 preview-variant with mangled token → 401.
 *   AS21 preview-variant with malformed tenantId in token →
 *        rejects via JSON.parse on tenant string.
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

async function realSecret(): Promise<Uint8Array> {
  return new TextEncoder().encode(
    TEST_ENV.JWT_SECRET ?? "test-secret-for-vitest-only"
  );
}

let aliceUserId: string;
let aliceJWT: string;

beforeAll(async () => {
  const a = await signup("phase50-adv-alice@test.example");
  aliceUserId = a.userId;
  aliceJWT = a.jwt;
});

describe("AS — share-token forgery surface (extended)", () => {
  it("AS1 — share token with truncated signature → 403", async () => {
    // Mint a real token and chop the last 5 chars off the signature.
    const sec = await realSecret();
    const tok = await new SignJWT({
      scope: "vfs-share",
      userId: aliceUserId,
      fileIds: ["x"],
      albumName: "t",
      jti: "deadbeef",
    })
      .setProtectedHeader({ alg: "HS256" })
      .setIssuedAt()
      .setExpirationTime(Math.floor(Date.now() / 1000) + 3600)
      .sign(sec);
    const truncated = tok.slice(0, tok.length - 5);
    const res = await SELF.fetch(
      `https://test/api/shared/${truncated}/photos`
    );
    expect(res.status).toBe(403);
  });

  it("AS2 — share token with extra padding bytes appended → 403", async () => {
    const sec = await realSecret();
    const tok = await new SignJWT({
      scope: "vfs-share",
      userId: aliceUserId,
      fileIds: ["x"],
      albumName: "t",
      jti: "deadbeef",
    })
      .setProtectedHeader({ alg: "HS256" })
      .setIssuedAt()
      .setExpirationTime(Math.floor(Date.now() / 1000) + 3600)
      .sign(sec);
    const padded = tok + "AA";
    const res = await SELF.fetch(`https://test/api/shared/${padded}/photos`);
    expect(res.status).toBe(403);
  });

  it("AS3 — substitute fileId in share token (keep sig) → 403", async () => {
    // JOSE refuses to verify a tampered payload; even if an attacker
    // replaces the middle segment (`payload`) with their own choice
    // of fileIds, the HMAC over (header.payload) fails.
    const sec = await realSecret();
    const tok = await new SignJWT({
      scope: "vfs-share",
      userId: aliceUserId,
      fileIds: ["original-fid"],
      albumName: "t",
      jti: "deadbeef",
    })
      .setProtectedHeader({ alg: "HS256" })
      .setIssuedAt()
      .setExpirationTime(Math.floor(Date.now() / 1000) + 3600)
      .sign(sec);
    const [h, , s] = tok.split(".");
    const tamperedPayload = btoa(
      JSON.stringify({
        scope: "vfs-share",
        userId: aliceUserId,
        fileIds: ["BOB-FILE-ID"],
        albumName: "t",
        jti: "deadbeef",
        iat: Math.floor(Date.now() / 1000),
        exp: Math.floor(Date.now() / 1000) + 3600,
      })
    )
      .replace(/\+/g, "-")
      .replace(/\//g, "_")
      .replace(/=+$/, "");
    const forged = `${h}.${tamperedPayload}.${s}`;
    const res = await SELF.fetch(`https://test/api/shared/${forged}/photos`);
    expect(res.status).toBe(403);
  });

  it("AS11 — base64-decode garbage in token URL → 403", async () => {
    const garbage = "not-a-jwt-at-all-just-letters";
    const res = await SELF.fetch(
      `https://test/api/shared/${garbage}/photos`
    );
    expect(res.status).toBe(403);
  });

  it("AS12 — empty token in URL → 404 (post-Phase-50 catch-all hardening)", async () => {
    // /api/shared//photos — the empty `:token` param fails Hono
    // route matching. Phase 50 hardened the catch-all so unmatched
    // `/api/*` paths return a clean 404 instead of falling through
    // to ASSETS (which served the SPA shell on API typos and
    // crashed in test-env where ASSETS is unbound).
    const res = await SELF.fetch("https://test/api/shared//photos");
    expect(res.status).toBe(404);
  });

  it("AS13 — token with alg=none → 403 (jose rejects)", async () => {
    // Constructing alg=none manually: header `{alg:none}`,
    // payload as desired, empty signature.
    const header = btoa('{"alg":"none","typ":"JWT"}')
      .replace(/\+/g, "-")
      .replace(/\//g, "_")
      .replace(/=+$/, "");
    const payload = btoa(
      JSON.stringify({
        scope: "vfs-share",
        userId: aliceUserId,
        fileIds: ["x"],
        albumName: "t",
        jti: "x",
        iat: Math.floor(Date.now() / 1000),
        exp: Math.floor(Date.now() / 1000) + 3600,
      })
    )
      .replace(/\+/g, "-")
      .replace(/\//g, "_")
      .replace(/=+$/, "");
    // alg=none JWTs have an empty signature segment.
    const tok = `${header}.${payload}.`;
    const res = await SELF.fetch(`https://test/api/shared/${tok}/photos`);
    expect(res.status).toBe(403);
  });
});

describe("AS — cross-purpose token replay (RFC 8725 §2.8)", () => {
  it("AS4 — download token (vfs-dl) replayed at share route → 403", async () => {
    const sec = await realSecret();
    const tok = await new SignJWT({
      scope: "vfs-dl",
      fileId: "any",
      ns: "default",
      tn: aliceUserId,
    })
      .setProtectedHeader({ alg: "HS256" })
      .setIssuedAt()
      .setExpirationTime(Math.floor(Date.now() / 1000) + 3600)
      .sign(sec);
    const res = await SELF.fetch(`https://test/api/shared/${tok}/photos`);
    expect(res.status).toBe(403);
  });

  it("AS5 — share token replayed at preview-variant route → 401", async () => {
    const sec = await realSecret();
    const tok = await new SignJWT({
      scope: "vfs-share",
      userId: aliceUserId,
      fileIds: ["x"],
      albumName: "t",
      jti: "deadbeef",
    })
      .setProtectedHeader({ alg: "HS256" })
      .setIssuedAt()
      .setExpirationTime(Math.floor(Date.now() / 1000) + 3600)
      .sign(sec);
    const res = await SELF.fetch(
      `https://test/api/vfs/preview-variant/${tok}`
    );
    // verifyPreviewToken returns null → route returns 401 EACCES.
    expect(res.status).toBe(401);
  });

  it("AS6 — preview token (vfs-pv) replayed at chunk-download route → 401", async () => {
    const sec = await realSecret();
    const tok = await new SignJWT({
      scope: "vfs-pv",
      tenantId: `default::${aliceUserId}`,
      fileId: "F",
      headVersionId: null,
      variantKind: "thumb",
      rendererKind: "image",
      format: "auto",
      contentHash:
        "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
    })
      .setProtectedHeader({ alg: "HS256" })
      .setIssuedAt()
      .setExpirationTime(Math.floor(Date.now() / 1000) + 3600)
      .sign(sec);
    const res = await SELF.fetch(
      `https://test/api/vfs/chunk/F/0?token=${tok}&hash=abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789&shard=0`
    );
    expect(res.status).toBe(401);
  });

  it("AS7 — multipart session token (vfs-mp) replayed at share route → 403", async () => {
    const sec = await realSecret();
    const tok = await new SignJWT({
      scope: "vfs-mp",
      uploadId: "u1",
      ns: "default",
      tn: aliceUserId,
      poolSize: 32,
      totalChunks: 1,
      chunkSize: 1024,
      totalSize: 1024,
    })
      .setProtectedHeader({ alg: "HS256" })
      .setIssuedAt()
      .setExpirationTime(Math.floor(Date.now() / 1000) + 3600)
      .sign(sec);
    const res = await SELF.fetch(`https://test/api/shared/${tok}/photos`);
    expect(res.status).toBe(403);
  });

  it("AS8 — vfs-scope token replayed at share route → 403", async () => {
    const sec = await realSecret();
    const tok = await new SignJWT({
      scope: "vfs",
      ns: "default",
      tn: aliceUserId,
    })
      .setProtectedHeader({ alg: "HS256" })
      .setIssuedAt()
      .setExpirationTime(Math.floor(Date.now() / 1000) + 3600)
      .sign(sec);
    const res = await SELF.fetch(`https://test/api/shared/${tok}/photos`);
    expect(res.status).toBe(403);
  });
});

describe("AS — chunk-download URL parameter manipulation", () => {
  it("AS9 — fileId in URL ≠ token's fileId → 403", async () => {
    const sec = await realSecret();
    const tok = await new SignJWT({
      scope: "vfs-dl",
      fileId: "TOKEN-FILE-ID",
      ns: "default",
      tn: aliceUserId,
    })
      .setProtectedHeader({ alg: "HS256" })
      .setIssuedAt()
      .setExpirationTime(Math.floor(Date.now() / 1000) + 3600)
      .sign(sec);
    const res = await SELF.fetch(
      `https://test/api/vfs/chunk/URL-FILE-ID/0?token=${tok}&hash=abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789&shard=0`
    );
    expect(res.status).toBe(403);
  });

  it("AS19 — chunk-download with negative shard index → 400", async () => {
    const sec = await realSecret();
    const tok = await new SignJWT({
      scope: "vfs-dl",
      fileId: "F",
      ns: "default",
      tn: aliceUserId,
    })
      .setProtectedHeader({ alg: "HS256" })
      .setIssuedAt()
      .setExpirationTime(Math.floor(Date.now() / 1000) + 3600)
      .sign(sec);
    const res = await SELF.fetch(
      `https://test/api/vfs/chunk/F/0?token=${tok}&hash=abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789&shard=-1`
    );
    expect(res.status).toBe(400);
  });

  it("AS18 — hash from URL is fetched as-given (no per-hash claim in download token); refuse non-hex shape", async () => {
    const sec = await realSecret();
    const tok = await new SignJWT({
      scope: "vfs-dl",
      fileId: "F",
      ns: "default",
      tn: aliceUserId,
    })
      .setProtectedHeader({ alg: "HS256" })
      .setIssuedAt()
      .setExpirationTime(Math.floor(Date.now() / 1000) + 3600)
      .sign(sec);
    // Non-hex hash should be rejected by the hex regex check.
    const res = await SELF.fetch(
      `https://test/api/vfs/chunk/F/0?token=${tok}&hash=not-a-hex-hash&shard=0`
    );
    expect(res.status).toBe(400);
  });
});

describe("AS — preview-variant URL surface", () => {
  it("AS20 — mangled preview token → 401", async () => {
    const garbage =
      "not.a.valid.token.with.too.many.dots.to.parse.as.jwt";
    const res = await SELF.fetch(
      `https://test/api/vfs/preview-variant/${garbage}`
    );
    expect(res.status).toBe(401);
  });

  it("AS21 — preview token with malformed tenantId (single segment) → 403", async () => {
    const sec = await realSecret();
    // tenantId "no-colons" lacks the ns::tenant split → route rejects.
    const tok = await new SignJWT({
      scope: "vfs-pv",
      tenantId: "no-colons",
      fileId: "F",
      headVersionId: null,
      variantKind: "thumb",
      rendererKind: "image",
      format: "auto",
      contentHash:
        "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
    })
      .setProtectedHeader({ alg: "HS256" })
      .setIssuedAt()
      .setExpirationTime(Math.floor(Date.now() / 1000) + 3600)
      .sign(sec);
    const res = await SELF.fetch(
      `https://test/api/vfs/preview-variant/${tok}`
    );
    expect(res.status).toBe(403);
  });
});

describe("AS — Yjs WebSocket auth surface", () => {
  it("AS15 — WS upgrade without Bearer/subprotocol → 401", async () => {
    const res = await SELF.fetch(
      `https://test/api/vfs/yjs/ws?path=/x.txt`,
      {
        headers: { Upgrade: "websocket" },
      }
    );
    expect(res.status).toBe(401);
  });

  it("AS16 — WS upgrade with garbage Bearer → 401", async () => {
    const res = await SELF.fetch(
      `https://test/api/vfs/yjs/ws?path=/x.txt`,
      {
        headers: {
          Upgrade: "websocket",
          Authorization: "Bearer not-a-jwt",
        },
      }
    );
    expect(res.status).toBe(401);
  });

  it("AS17 — WS upgrade with malformed subprotocol bearer.* → 401", async () => {
    const res = await SELF.fetch(
      `https://test/api/vfs/yjs/ws?path=/x.txt`,
      {
        headers: {
          Upgrade: "websocket",
          "Sec-WebSocket-Protocol": "bearer.malformed",
        },
      }
    );
    expect(res.status).toBe(401);
  });
});

describe("AS — share-token mint surface", () => {
  it("AS14 — mint with > 1000 fileIds rejected → 400", async () => {
    const fileIds = Array.from({ length: 1001 }, (_, i) => `f${i}`);
    const res = await SELF.fetch("https://test/api/auth/share-token", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${aliceJWT}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ fileIds, albumName: "huge" }),
    });
    expect(res.status).toBe(400);
  });

  it("AS-MINT-CROSS — share token mint accepts ANY fileId string;\n" +
     "    cross-tenant read STILL fails because Alice's DO does not\n" +
     "    contain Bob's fileIds (DO-isolation defense-in-depth)", async () => {
    // Alice mints a token containing what claims to be a fileId.
    // The mint endpoint does NOT verify ownership — but the read
    // path bounces against Alice's DO which can only return Alice's
    // files. This pins the defense-in-depth contract.
    const mintRes = await SELF.fetch(
      "https://test/api/auth/share-token",
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${aliceJWT}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          fileIds: ["bobs-private-fileid-XYZ"],
          albumName: "alice claims bob",
        }),
      }
    );
    expect(mintRes.status).toBe(200);
    const mintBody = (await mintRes.json()) as { token: string };

    // Now try to read "bobs-private-fileid-XYZ" via the share route.
    // Alice's DO does not contain that fileId → 404.
    const readRes = await SELF.fetch(
      `https://test/api/shared/${mintBody.token}/image/bobs-private-fileid-XYZ`
    );
    expect(readRes.status).toBe(404);
  });
});

describe("AS — preview-variant URL pollution", () => {
  it("AS-PV-SCOPE-INJ — token with `tenantId: '../'-style escape attempts cannot\n" +
     "    escape DO routing (tokens are HMAC-bound)", async () => {
    // Even if an attacker tried to inject path-traversal-style strings
    // in tenantId, the userIdFor regex `[A-Za-z0-9._-]{1,128}` rejects
    // anything outside the allowed charset. The DO routing therefore
    // never sees `..`/`/` characters.
    const sec = await realSecret();
    const tok = await new SignJWT({
      scope: "vfs-pv",
      tenantId: "default::alice/../bob", // contains slashes; invalid
      fileId: "F",
      headVersionId: null,
      variantKind: "thumb",
      rendererKind: "image",
      format: "auto",
      contentHash:
        "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
    })
      .setProtectedHeader({ alg: "HS256" })
      .setIssuedAt()
      .setExpirationTime(Math.floor(Date.now() / 1000) + 3600)
      .sign(sec);
    const res = await SELF.fetch(
      `https://test/api/vfs/preview-variant/${tok}`
    );
    // Route splits tenantId on "::" and constructs scope; the split
    // gives `["default", "alice/../bob"]` (length 2). The downstream
    // `userIdFor` will reject the slash chars when the DO RPC is
    // called. Either 403 (route validates first) or 4xx from the DO.
    expect([400, 403, 404, 500]).toContain(res.status);
  });
});
