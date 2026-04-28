import { Hono } from "hono";
import type { Env } from "@shared/types";
import type { MiddlewareHandler } from "hono";
import { verifyVFSToken, VFSConfigError } from "../lib/auth";
import { vfsUserDOName } from "../lib/utils";
import type { UserDOCore } from "../objects/user/user-do-core";
import type { VFSScope } from "@shared/vfs-types";

/**
 * HTTP fallback for non-Worker consumers (browsers, Node servers,
 * third-party clouds). Accepts a Bearer VFS token (scope === "vfs"
 * from sdk/src/auth.ts:issueVFSToken) and forwards method args to
 * the per-tenant UserDO via typed RPC.
 *
 * Security model: the token IS the scope. Operators issue tokens
 * with `signVFSToken(env, { ns, tenant, sub? })`. The HTTP route
 * extracts (ns, tn, sub) from the verified token and uses those —
 * the request body never controls scope routing. This makes
 * cross-tenant escape impossible via header/body manipulation.
 *
 * Method shape: every method is POST with a JSON body of the form
 *   { path, ...opts }
 * (no `scope` field — that's derived from the token).
 *
 * Returns:
 *   - readFile / readChunk: 200 application/octet-stream
 *   - readdir / stat / lstat / openManifest / readManyStat / exists / readlink: 200 application/json
 *   - writeFile / writeChunk: 200 application/json with { ok: true }
 *   - unlink / mkdir / rmdir / rename / chmod / symlink: 200 application/json with { ok: true }
 *   - errors: matching status code + { code, message } JSON body
 *
 * Errors map to HTTP status codes mirroring Node fs convention:
 *   ENOENT  → 404
 *   EEXIST  → 409
 *   EISDIR  → 409
 *   ENOTDIR → 409
 *   EBUSY   → 409
 *   ENOTEMPTY → 409
 *   EFBIG   → 413
 *   ELOOP   → 508 (Loop Detected)
 *   EACCES  → 403
 *   EROFS   → 403
 *   EINVAL  → 400
 *   default → 500
 *
 * The SDK's createMossaicHttpClient inverts this mapping back to
 * VFSFsError subclasses so the consumer surface is identical to
 * the binding client.
 */

const vfs = new Hono<{
  Bindings: Env;
  Variables: { scope: VFSScope };
}>();

/**
 * Auth middleware: extract Bearer token from Authorization, verify
 * via verifyVFSToken, populate c.var.scope with the validated scope.
 */
const vfsAuth = (): MiddlewareHandler<{
  Bindings: Env;
  Variables: { scope: VFSScope };
}> => async (c, next) => {
  const auth = c.req.header("Authorization");
  if (!auth?.startsWith("Bearer ")) {
    return c.json({ code: "EACCES", message: "Bearer token required" }, 401);
  }
  const token = auth.slice(7);
  let payload;
  try {
    payload = await verifyVFSToken(c.env, token);
  } catch (err) {
    // VFSConfigError = JWT_SECRET missing on the deploy. 503 surfaces
    // a clear "service mis-configured" rather than masking as 401.
    if (err instanceof VFSConfigError) {
      return c.json(
        { code: "EMOSSAIC_UNAVAILABLE", message: err.message },
        503
      );
    }
    throw err;
  }
  if (!payload) {
    return c.json(
      { code: "EACCES", message: "Invalid or expired VFS token" },
      401
    );
  }
  c.set("scope", { ns: payload.ns, tenant: payload.tn, sub: payload.sub });
  await next();
};

vfs.use("*", vfsAuth());

/** Resolve the typed UserDO stub for the verified scope. */
function userStub(c: { env: Env; var: { scope: VFSScope } }): UserDOCore {
  const scope = c.var.scope;
  const name = vfsUserDOName(scope.ns, scope.tenant, scope.sub);
  const id = c.env.USER_DO.idFromName(name);
  return c.env.USER_DO.get(id) as unknown as UserDOCore;
}

/** Map a thrown error to (status, body) for JSON responses. */
function errToResponse(err: unknown): { status: number; body: { code: string; message: string } } {
  const e = err as { code?: unknown; message?: unknown };
  const rawMsg = typeof e?.message === "string" ? e.message : String(err);
  // Extract code via the same scan-all-tokens approach as
  // sdk/src/errors.ts mapServerError. Server-side VFSError throws
  // with message format "CODE: rest" but workerd's RPC wire
  // serialisation prepends the class name; scan tokens for the
  // first known code.
  const KNOWN: Record<string, number> = {
    ENOENT: 404,
    EEXIST: 409,
    EISDIR: 409,
    ENOTDIR: 409,
    EBUSY: 409,
    ENOTEMPTY: 409,
    EFBIG: 413,
    ELOOP: 508,
    EACCES: 403,
    EROFS: 403,
    EINVAL: 400,
    EMOSSAIC_UNAVAILABLE: 503,
  };
  const explicitCode = typeof e?.code === "string" ? (e.code as string) : undefined;
  let code: string | undefined =
    explicitCode && explicitCode in KNOWN ? explicitCode : undefined;
  if (!code) {
    const tokens = rawMsg.match(/[A-Z_]{3,}/g) ?? [];
    for (const tok of tokens) {
      if (tok in KNOWN) {
        code = tok;
        break;
      }
    }
  }
  const status = code ? KNOWN[code] : 500;
  return {
    status,
    body: { code: code ?? "EINTERNAL", message: rawMsg },
  };
}

/**
 * Validate a path string from the request body. The DO-side userIdFor
 * + path resolution already validate, so all we need here is type
 * coercion: body.path must be a string. Bad input → 400 EINVAL.
 */
function expectPath(body: unknown): string {
  const b = body as { path?: unknown };
  if (typeof b?.path !== "string") {
    throw Object.assign(new Error("EINVAL: body.path must be a string"), {
      code: "EINVAL",
    });
  }
  return b.path;
}

// ── Reads ──────────────────────────────────────────────────────────────

vfs.post("/readFile", async (c) => {
  try {
    const body = await c.req.json<{ path: string; encoding?: "utf8" }>();
    const path = expectPath(body);
    const buf = await userStub(c).vfsReadFile(c.var.scope, path);
    if (body.encoding === "utf8") {
      return c.json({ data: new TextDecoder().decode(buf) });
    }
    return new Response(buf, {
      status: 200,
      headers: { "Content-Type": "application/octet-stream" },
    });
  } catch (err) {
    const r = errToResponse(err);
    return c.json(r.body, r.status as 400);
  }
});

vfs.post("/readdir", async (c) => {
  try {
    const body = await c.req.json<{ path: string }>();
    const path = expectPath(body);
    const entries = await userStub(c).vfsReaddir(c.var.scope, path);
    return c.json({ entries });
  } catch (err) {
    const r = errToResponse(err);
    return c.json(r.body, r.status as 400);
  }
});

vfs.post("/stat", async (c) => {
  try {
    const body = await c.req.json<{ path: string }>();
    const path = expectPath(body);
    const stat = await userStub(c).vfsStat(c.var.scope, path);
    return c.json({ stat });
  } catch (err) {
    const r = errToResponse(err);
    return c.json(r.body, r.status as 400);
  }
});

vfs.post("/lstat", async (c) => {
  try {
    const body = await c.req.json<{ path: string }>();
    const path = expectPath(body);
    const stat = await userStub(c).vfsLstat(c.var.scope, path);
    return c.json({ stat });
  } catch (err) {
    const r = errToResponse(err);
    return c.json(r.body, r.status as 400);
  }
});

vfs.post("/exists", async (c) => {
  try {
    const body = await c.req.json<{ path: string }>();
    const path = expectPath(body);
    const exists = await userStub(c).vfsExists(c.var.scope, path);
    return c.json({ exists });
  } catch (err) {
    const r = errToResponse(err);
    return c.json(r.body, r.status as 400);
  }
});

vfs.post("/readlink", async (c) => {
  try {
    const body = await c.req.json<{ path: string }>();
    const path = expectPath(body);
    const target = await userStub(c).vfsReadlink(c.var.scope, path);
    return c.json({ target });
  } catch (err) {
    const r = errToResponse(err);
    return c.json(r.body, r.status as 400);
  }
});

vfs.post("/readManyStat", async (c) => {
  try {
    const body = await c.req.json<{ paths: string[] }>();
    if (!Array.isArray(body.paths)) {
      return c.json(
        { code: "EINVAL", message: "body.paths must be a string[]" },
        400
      );
    }
    const stats = await userStub(c).vfsReadManyStat(c.var.scope, body.paths);
    return c.json({ stats });
  } catch (err) {
    const r = errToResponse(err);
    return c.json(r.body, r.status as 400);
  }
});

// ── Writes ────────────────────────────────────────────────────────────

vfs.post("/writeFile", async (c) => {
  try {
    // Two body shapes:
    //   - application/json: { path, encoding: "utf8", data: <string>, opts? }
    //   - application/octet-stream + ?path=...: raw bytes; path comes
    //     from the query string. This avoids base64-bloating large
    //     payloads through JSON.
    const ct = c.req.header("Content-Type") ?? "";
    if (ct.includes("application/octet-stream")) {
      const path = c.req.query("path");
      if (typeof path !== "string" || path.length === 0) {
        return c.json(
          { code: "EINVAL", message: "?path=... required for octet-stream" },
          400
        );
      }
      const data = new Uint8Array(await c.req.arrayBuffer());
      await userStub(c).vfsWriteFile(c.var.scope, path, data);
      return c.json({ ok: true });
    }
    const body = await c.req.json<{
      path: string;
      data: string;
      encoding?: "utf8";
      mode?: number;
      mimeType?: string;
    }>();
    const path = expectPath(body);
    if (typeof body.data !== "string") {
      return c.json(
        { code: "EINVAL", message: "body.data must be a string (use octet-stream for bytes)" },
        400
      );
    }
    const data = new TextEncoder().encode(body.data);
    await userStub(c).vfsWriteFile(c.var.scope, path, data, {
      mode: body.mode,
      mimeType: body.mimeType,
    });
    return c.json({ ok: true });
  } catch (err) {
    const r = errToResponse(err);
    return c.json(r.body, r.status as 400);
  }
});

vfs.post("/unlink", async (c) => {
  try {
    const body = await c.req.json<{ path: string }>();
    const path = expectPath(body);
    await userStub(c).vfsUnlink(c.var.scope, path);
    return c.json({ ok: true });
  } catch (err) {
    const r = errToResponse(err);
    return c.json(r.body, r.status as 400);
  }
});

vfs.post("/mkdir", async (c) => {
  try {
    const body = await c.req.json<{
      path: string;
      recursive?: boolean;
      mode?: number;
    }>();
    const path = expectPath(body);
    await userStub(c).vfsMkdir(c.var.scope, path, {
      recursive: body.recursive,
      mode: body.mode,
    });
    return c.json({ ok: true });
  } catch (err) {
    const r = errToResponse(err);
    return c.json(r.body, r.status as 400);
  }
});

vfs.post("/rmdir", async (c) => {
  try {
    const body = await c.req.json<{ path: string }>();
    const path = expectPath(body);
    await userStub(c).vfsRmdir(c.var.scope, path);
    return c.json({ ok: true });
  } catch (err) {
    const r = errToResponse(err);
    return c.json(r.body, r.status as 400);
  }
});

vfs.post("/rename", async (c) => {
  try {
    const body = await c.req.json<{ src: string; dst: string }>();
    if (typeof body.src !== "string" || typeof body.dst !== "string") {
      return c.json(
        { code: "EINVAL", message: "body.src and body.dst must be strings" },
        400
      );
    }
    await userStub(c).vfsRename(c.var.scope, body.src, body.dst);
    return c.json({ ok: true });
  } catch (err) {
    const r = errToResponse(err);
    return c.json(r.body, r.status as 400);
  }
});

vfs.post("/chmod", async (c) => {
  try {
    const body = await c.req.json<{ path: string; mode: number }>();
    const path = expectPath(body);
    if (typeof body.mode !== "number") {
      return c.json(
        { code: "EINVAL", message: "body.mode must be a number" },
        400
      );
    }
    await userStub(c).vfsChmod(c.var.scope, path, body.mode);
    return c.json({ ok: true });
  } catch (err) {
    const r = errToResponse(err);
    return c.json(r.body, r.status as 400);
  }
});

vfs.post("/symlink", async (c) => {
  try {
    const body = await c.req.json<{ target: string; path: string }>();
    if (typeof body.target !== "string" || typeof body.path !== "string") {
      return c.json(
        { code: "EINVAL", message: "body.target and body.path must be strings" },
        400
      );
    }
    await userStub(c).vfsSymlink(c.var.scope, body.target, body.path);
    return c.json({ ok: true });
  } catch (err) {
    const r = errToResponse(err);
    return c.json(r.body, r.status as 400);
  }
});

vfs.post("/removeRecursive", async (c) => {
  try {
    const body = await c.req.json<{ path: string; cursor?: string }>();
    const path = expectPath(body);
    const r = await userStub(c).vfsRemoveRecursive(
      c.var.scope,
      path,
      body.cursor
    );
    return c.json(r);
  } catch (err) {
    const r = errToResponse(err);
    return c.json(r.body, r.status as 400);
  }
});

// ── Low-level escape hatch ─────────────────────────────────────────────

vfs.post("/openManifest", async (c) => {
  try {
    const body = await c.req.json<{ path: string }>();
    const path = expectPath(body);
    const m = await userStub(c).vfsOpenManifest(c.var.scope, path);
    return c.json({ manifest: m });
  } catch (err) {
    const r = errToResponse(err);
    return c.json(r.body, r.status as 400);
  }
});

vfs.post("/readChunk", async (c) => {
  try {
    const body = await c.req.json<{ path: string; chunkIndex: number }>();
    const path = expectPath(body);
    if (typeof body.chunkIndex !== "number") {
      return c.json(
        { code: "EINVAL", message: "body.chunkIndex must be a number" },
        400
      );
    }
    const buf = await userStub(c).vfsReadChunk(
      c.var.scope,
      path,
      body.chunkIndex
    );
    return new Response(buf, {
      status: 200,
      headers: { "Content-Type": "application/octet-stream" },
    });
  } catch (err) {
    const r = errToResponse(err);
    return c.json(r.body, r.status as 400);
  }
});

// ── Phase 9: versioning ────────────────────────────────────────────────

vfs.post("/listVersions", async (c) => {
  try {
    const body = await c.req.json<{ path: string; limit?: number }>();
    const path = expectPath(body);
    const rows = await userStub(c).vfsListVersions(c.var.scope, path, {
      limit: body.limit,
    });
    // Map server VersionRow → public VersionInfo (id field).
    const versions = rows.map((r) => ({
      id: r.versionId,
      mtimeMs: r.mtimeMs,
      size: r.size,
      mode: r.mode,
      deleted: r.deleted,
    }));
    return c.json({ versions });
  } catch (err) {
    const r = errToResponse(err);
    return c.json(r.body, r.status as 400);
  }
});

vfs.post("/restoreVersion", async (c) => {
  try {
    const body = await c.req.json<{
      path: string;
      sourceVersionId: string;
    }>();
    const path = expectPath(body);
    if (typeof body.sourceVersionId !== "string") {
      return c.json(
        {
          code: "EINVAL",
          message: "body.sourceVersionId must be a string",
        },
        400
      );
    }
    const r = await userStub(c).vfsRestoreVersion(
      c.var.scope,
      path,
      body.sourceVersionId
    );
    return c.json({ id: r.versionId });
  } catch (err) {
    const r = errToResponse(err);
    return c.json(r.body, r.status as 400);
  }
});

vfs.post("/dropVersions", async (c) => {
  try {
    const body = await c.req.json<{
      path: string;
      policy: {
        olderThan?: number;
        keepLast?: number;
        exceptVersions?: string[];
      };
    }>();
    const path = expectPath(body);
    const r = await userStub(c).vfsDropVersions(
      c.var.scope,
      path,
      body.policy ?? {}
    );
    return c.json(r);
  } catch (err) {
    const r = errToResponse(err);
    return c.json(r.body, r.status as 400);
  }
});

// Catch-all 404 for unknown /api/vfs/<method> paths. Without this,
// the parent app's wildcard route would forward to ASSETS (or
// wherever) and clients would see weird 500s instead of a clean 404.
vfs.all("*", (c) =>
  c.json({ code: "ENOENT", message: "Unknown VFS method" }, 404)
);

export default vfs;
