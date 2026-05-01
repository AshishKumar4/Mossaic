import { Hono, type Context } from "hono";
import type { EnvApp as Env } from "@shared/types";
import {
  authMiddleware,
  signJWT,
  signShareToken,
  signVFSToken,
  VFSConfigError,
} from "@core/lib/auth";
import { ctxFromHono, logError, logInfo } from "@core/lib/logger";
import { userStub, userStubByName } from "../lib/user-stub";

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

  // Initialize the canonical (`vfs:default:<userId>`) UserDO with a
  // default quota row so subsequent `appGetQuota` / `appGetUserStats`
  // calls return non-zero defaults instead of the all-zeroes fallback.
  // The auth row stays on the `auth:<email>` DO; the quota row is
  // duplicated to the data-side DO.
  await userStub(c.env, result.userId).appInitTenant(result.userId);

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

/**
 * POST /api/auth/share-token
 *
 * Mint an HMAC-signed album-share token for the authenticated user.
 * Body: `{ fileIds: string[], albumName: string }`. The userId is
 * lifted from the verified session JWT — clients cannot share files
 * they do not own.
 *
 * P0-1 fix: the SPA used to mint share tokens client-side as
 * `btoa(JSON.stringify({...}))` which was unsigned and forgeable.
 * The server now mints HMAC-signed JWTs so `/api/shared/:token/*`
 * can verify the share is genuine.
 *
 * Returns the signed token + expiresAtMs so the SPA can render
 * a TTL-aware UI.
 */
auth.post("/share-token", authMiddleware(), async (c) => {
  const userId = c.get("userId");
  let body: { fileIds?: unknown; albumName?: unknown };
  try {
    body = await c.req.json<{ fileIds: unknown; albumName: unknown }>();
  } catch {
    return c.json({ error: "Body must be JSON" }, 400);
  }
  const albumName =
    typeof body.albumName === "string" ? body.albumName : "";
  if (
    !Array.isArray(body.fileIds) ||
    body.fileIds.length === 0 ||
    !body.fileIds.every((id) => typeof id === "string" && id.length > 0)
  ) {
    return c.json(
      { error: "fileIds must be a non-empty array of strings" },
      400
    );
  }
  // Cap the fileIds list to keep the token bounded — share tokens
  // are URL-embedded, and an unbounded list here would let a single
  // mint produce a multi-MB URL. 1000 photos per album is generous;
  // larger collections should split into multiple albums.
  const MAX_SHARE_FILE_IDS = 1000;
  if (body.fileIds.length > MAX_SHARE_FILE_IDS) {
    return c.json(
      {
        error: `fileIds must be ≤ ${MAX_SHARE_FILE_IDS} entries (got ${body.fileIds.length})`,
      },
      400
    );
  }
  try {
    const { token, expiresAtMs, jti } = await signShareToken(c.env, {
      userId,
      fileIds: body.fileIds as string[],
      albumName,
    });
    // Audit-log the mint. Persist on the owner's UserDO so an
    // operator querying "who minted share tokens?" gets the full
    // trail per-tenant. Best-effort: a failure here doesn't block
    // the mint (the token is already signed).
    try {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      await (userStub(c.env, userId) as any).appAuditShareLinkMint(userId, {
        jti,
        expiresAtMs,
        fileCount: body.fileIds.length,
        albumName,
      });
    } catch (auditErr) {
      logError(
        "share-token mint audit-log failed",
        ctxFromHono(c),
        auditErr,
        { event: "share_token_audit_failed" }
      );
    }
    logInfo(
      "share-token minted",
      ctxFromHono(c),
      {
        event: "share_token_minted",
        jti,
        fileCount: body.fileIds.length,
        expiresAtMs,
      }
    );
    return c.json({ token, expiresAtMs, jti });
  } catch (err) {
    if (err instanceof VFSConfigError) {
      return c.json({ error: err.message }, 503);
    }
    if (err instanceof Error) {
      return c.json({ error: err.message }, 400);
    }
    throw err;
  }
});

/**
 * DELETE /api/auth/account
 *
 * Self-service account deletion. Drops every byte the authenticated
 * user has stored across UserDO + ShardDOs, then removes the auth
 * row keyed by email so the credentials no longer exist.
 *
 * Two-DO purge — sequenced so a partial failure leaves the most
 * benign state behind:
 *   1. Wipe the data DO first (`vfs:default:<userId>`). This
 *      hard-deletes every file row through the canonical
 *      `hardDeleteFileRow` path, which dispatches `deleteChunks` RPCs
 *      to each touched ShardDO. ShardDO chunk_refs are dropped, the
 *      alarm sweeper picks up orphans within 30s. Quota row is
 *      zeroed (not dropped — the row is part of the schema invariant
 *      and a fresh signup would re-INSERT IGNORE it anyway).
 *   2. Wipe the auth DO (`auth:<email>`). Removes the password hash;
 *      a subsequent login attempt now 401s.
 *
 * Idempotent. Re-issuing DELETE on an already-deleted account is a
 * no-op (both DOs return 0 rows affected).
 *
 * NO confirmation flow — the auth-gated JWT is the confirmation. The
 * SPA (when one is wired up) can add a "type your email to confirm"
 * UI step; the API itself is one-call. This is consistent with the
 * existing absence of any other confirm-step on auth routes.
 *
 * Returns a summary so callers can verify the purge took effect.
 */
auth.delete("/account", authMiddleware(), async (c) => {
  const userId = c.get("userId");
  const email = c.get("email");

  // Wipe data first. Best-effort — a failure here still allows the
  // auth-row wipe to proceed so the credentials become unusable.
  let dataReport: {
    filesRemoved: number;
    foldersRemoved: number;
    versionsRemoved: number;
    chunksRemovedFromShards: number;
  } = {
    filesRemoved: 0,
    foldersRemoved: 0,
    versionsRemoved: 0,
    chunksRemovedFromShards: 0,
  };
  try {
    dataReport = await userStub(c.env, userId).appWipeAccountData(userId);
  } catch (err) {
    logError(
      "account-delete: appWipeAccountData failed",
      ctxFromHono(c),
      err,
      { event: "account_delete_data_wipe_failed", userId }
    );
  }

  // Wipe auth row.
  let authRemoved = false;
  try {
    const r = await userStubByName(c.env, `auth:${email}`).appWipeAuthRow(
      email
    );
    authRemoved = r.removed;
  } catch (err) {
    logError(
      "account-delete: appWipeAuthRow failed",
      ctxFromHono(c),
      err,
      { event: "account_delete_auth_wipe_failed", email }
    );
  }

  // Audit-log the account-delete on the tenant's UserDO.
  // Best-effort: failure here doesn't change the response. The
  // DataDO audit row is the user-facing event; the per-table audits
  // (adminWipeAccountData) provide a granular byte/file count.
  try {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    await (userStub(c.env, userId) as any).appAuditAccountDelete(userId, {
      email,
      filesRemoved: dataReport.filesRemoved,
      authRemoved,
    });
  } catch (err) {
    logError(
      "account-delete: audit-log emission failed",
      ctxFromHono(c),
      err,
      { event: "account_delete_audit_failed", userId }
    );
  }

  logInfo(
    "account-delete completed",
    ctxFromHono(c),
    {
      event: "account_delete_completed",
      userId,
      filesRemoved: dataReport.filesRemoved,
      authRemoved,
    }
  );

  return c.json({
    ok: true,
    userId,
    email,
    data: dataReport,
    authRowRemoved: authRemoved,
  });
});

export default auth;
