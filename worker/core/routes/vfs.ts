import { Hono } from "hono";
import type { EnvCore as Env } from "../../../shared/types";
import type { MiddlewareHandler } from "hono";
import { verifyVFSToken, VFSConfigError } from "../lib/auth";
import { vfsUserDOName } from "../lib/utils";
import type { UserDOCore } from "../objects/user/user-do-core";
import type { VFSScope } from "../../../shared/vfs-types";

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
 *
 * Exported so sibling route modules (e.g. `vfs-preview.ts`) reuse
 * the same contract without duplicating the implementation.
 */
export const vfsAuth = (): MiddlewareHandler<{
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
export function userStub(c: { env: Env; var: { scope: VFSScope } }): UserDOCore {
  const scope = c.var.scope;
  const name = vfsUserDOName(scope.ns, scope.tenant, scope.sub);
  const id = c.env.MOSSAIC_USER.idFromName(name);
  return c.env.MOSSAIC_USER.get(id) as unknown as UserDOCore;
}

/**
 * Map a thrown error to (status, body) for JSON responses.
 *
 * Exported so sibling route modules reuse the same status+code map.
 */
export function errToResponse(err: unknown): { status: number; body: { code: string; message: string } } {
  // VFSConfigError = JWT_SECRET missing on the deploy. Surface as 503
  // service-misconfigured (mirrors the auth-middleware path). Caught
  // here as defense-in-depth: post-auth handlers like listFiles can
  // also throw VFSConfigError if the cursor secret resolves to empty
  // (B-1 fix in worker/core/objects/user/list-files.ts).
  if (err instanceof VFSConfigError) {
    return {
      status: 503,
      body: { code: "EMOSSAIC_UNAVAILABLE", message: err.message },
    };
  }
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
    EAGAIN: 429,
    // encryption error surface.
    // EBADF: writeFile attempted to mix encryption modes within a
    //   single path's history, OR write plaintext to an encrypted
    //   path. 409 (Conflict) matches the EISDIR / EEXIST family.
    // ENOTSUP: chmod-style encryption toggle in v15. 501 (Not
    //   Implemented) — the operation is well-formed but unsupported.
    EBADF: 409,
    ENOTSUP: 501,
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
export function expectPath(body: unknown): string {
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
    const body = await c.req.json<{
      path: string;
      encoding?: "utf8";
      // optional historical-version selector. Pairs with
      // CLI `cat --version <id>`.
      versionId?: string;
    }>();
    const path = expectPath(body);
    // surface encryption metadata via response header so the
    // HTTP-fallback consumer knows whether to decrypt the bytes. The
    // server NEVER decrypts; it just reports the per-file
    // encryption_mode + encryption_key_id from `stat`.
    const stat = await userStub(c).vfsStat(c.var.scope, path);
    const buf = await userStub(c).vfsReadFile(
      c.var.scope,
      path,
      body.versionId ? { versionId: body.versionId } : undefined,
    );
    if (body.encoding === "utf8") {
      // utf8 mode is incompatible with encryption — the bytes returned
      // are the envelope, not text. We refuse with EINVAL rather than
      // silently mojibake the consumer's screen.
      if (stat.encryption !== undefined) {
        return c.json(
          {
            code: "EINVAL",
            message:
              "readFile: cannot encoding=utf8 on an encrypted file (use binary)",
          },
          400
        );
      }
      return c.json({ data: new TextDecoder().decode(buf) });
    }
    const headers: Record<string, string> = {
      "Content-Type": "application/octet-stream",
    };
    if (stat.encryption !== undefined) {
      headers["X-Mossaic-Encryption"] = JSON.stringify(stat.encryption);
    }
    return new Response(buf, {
      status: 200,
      headers,
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
    // Three body shapes:
    //   - application/json: { path, encoding: "utf8", data: <string>,
    //                         mode?, mimeType?, metadata?, tags?, version? }
    //   - application/octet-stream + ?path=...: raw bytes; path comes
    //     from the query string. No metadata/tags/version on this path
    //     (use multipart for those).
    //   - multipart/form-data + ?path=...: two parts —
    //         "bytes" (Blob) — required, the file content,
    //         "meta"  (text) — JSON string with
    //                          { mode?, mimeType?, metadata?, tags?, version? }.
    //     This is the parity path with the binding-mode `writeFile`.
    const ct = c.req.header("Content-Type") ?? "";
    // parse `X-Mossaic-Encryption` header, applies to all
    // body-shape branches (octet-stream / multipart / json). The
    // header value is JSON `{ mode, keyId? }`.
    const encryptionHeader = c.req.header("X-Mossaic-Encryption");
    let httpEncryption:
      | { mode: "convergent" | "random"; keyId?: string }
      | undefined;
    if (encryptionHeader !== undefined && encryptionHeader.length > 0) {
      try {
        const parsed = JSON.parse(encryptionHeader);
        if (
          parsed &&
          typeof parsed === "object" &&
          (parsed.mode === "convergent" || parsed.mode === "random")
        ) {
          httpEncryption = { mode: parsed.mode };
          if (typeof parsed.keyId === "string") {
            httpEncryption.keyId = parsed.keyId;
          }
        } else {
          return c.json(
            {
              code: "EINVAL",
              message:
                "X-Mossaic-Encryption: invalid mode (must be 'convergent' or 'random')",
            },
            400
          );
        }
      } catch {
        return c.json(
          {
            code: "EINVAL",
            message:
              "X-Mossaic-Encryption: header value is not valid JSON",
          },
          400
        );
      }
    }
    if (ct.includes("multipart/form-data")) {
      const path = c.req.query("path");
      if (typeof path !== "string" || path.length === 0) {
        return c.json(
          { code: "EINVAL", message: "?path=... required for multipart" },
          400
        );
      }
      const form = await c.req.formData();
      const bytesPart = form.get("bytes") as unknown;
      if (
        bytesPart === null ||
        typeof bytesPart === "string" ||
        typeof (bytesPart as { arrayBuffer?: unknown }).arrayBuffer !==
          "function"
      ) {
        return c.json(
          { code: "EINVAL", message: "multipart: 'bytes' part required (Blob)" },
          400
        );
      }
      const data = new Uint8Array(
        await (bytesPart as { arrayBuffer(): Promise<ArrayBuffer> }).arrayBuffer()
      );
      const metaRaw = form.get("meta");
      let opts: {
        mode?: number;
        mimeType?: string;
        metadata?: Record<string, unknown> | null;
        tags?: readonly string[];
        version?: { label?: string; userVisible?: boolean };
        encryption?: { mode: "convergent" | "random"; keyId?: string };
      } = {};
      if (typeof metaRaw === "string" && metaRaw.length > 0) {
        try {
          opts = JSON.parse(metaRaw);
        } catch {
          return c.json(
            { code: "EINVAL", message: "multipart: 'meta' part is not valid JSON" },
            400
          );
        }
      }
      // Header takes precedence over `meta.encryption` (defense
      // against accidentally forgetting to include encryption opts in
      // the meta JSON body). Both can be set; the header value wins.
      if (httpEncryption !== undefined) opts.encryption = httpEncryption;
      await userStub(c).vfsWriteFile(c.var.scope, path, data, opts);
      return c.json({ ok: true });
    }
    if (ct.includes("application/octet-stream")) {
      const path = c.req.query("path");
      if (typeof path !== "string" || path.length === 0) {
        return c.json(
          { code: "EINVAL", message: "?path=... required for octet-stream" },
          400
        );
      }
      const data = new Uint8Array(await c.req.arrayBuffer());
      await userStub(c).vfsWriteFile(
        c.var.scope,
        path,
        data,
        httpEncryption !== undefined ? { encryption: httpEncryption } : undefined
      );
      return c.json({ ok: true });
    }
    const body = await c.req.json<{
      path: string;
      data: string;
      encoding?: "utf8";
      mode?: number;
      mimeType?: string;
      metadata?: Record<string, unknown> | null;
      tags?: readonly string[];
      version?: { label?: string; userVisible?: boolean };
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
      metadata: body.metadata,
      tags: body.tags,
      version: body.version,
      // header > body precedence (no body field for JSON path
      // — JSON writeFile is for plaintext text only; encryption uses
      // the header).
      ...(httpEncryption !== undefined
        ? { encryption: httpEncryption }
        : {}),
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

vfs.post("/purge", async (c) => {
  try {
    const body = await c.req.json<{ path: string }>();
    const path = expectPath(body);
    await userStub(c).vfsPurge(c.var.scope, path);
    return c.json({ ok: true });
  } catch (err) {
    const r = errToResponse(err);
    return c.json(r.body, r.status as 400);
  }
});

// Phase 29 — archive / unarchive endpoints. Cosmetic-only hide;
// reads remain unaffected. Both are idempotent — calling on an
// already-(un)archived path is a no-op.
vfs.post("/archive", async (c) => {
  try {
    const body = await c.req.json<{ path: string }>();
    const path = expectPath(body);
    await userStub(c).vfsArchive(c.var.scope, path);
    return c.json({ ok: true });
  } catch (err) {
    const r = errToResponse(err);
    return c.json(r.body, r.status as 400);
  }
});

vfs.post("/unarchive", async (c) => {
  try {
    const body = await c.req.json<{ path: string }>();
    const path = expectPath(body);
    await userStub(c).vfsUnarchive(c.var.scope, path);
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

// setYjsMode HTTP endpoint. Required by the @mossaic/cli
// `yjs init` command. The DO method is binding-only by default; we
// surface it here so external clients (Node CLI, scripts) can promote
// a file to yjs-mode without holding a DO stub. Demoting back to plain
// (`enabled: false`) is rejected at the DO layer with EINVAL — losing
// CRDT history is never silent.
vfs.post("/setYjsMode", async (c) => {
  try {
    const body = await c.req.json<{ path: string; enabled: boolean }>();
    const path = expectPath(body);
    if (typeof body.enabled !== "boolean") {
      return c.json(
        { code: "EINVAL", message: "body.enabled must be a boolean" },
        400
      );
    }
    await userStub(c).vfsSetYjsMode(c.var.scope, path, body.enabled);
    return c.json({ ok: true });
  } catch (err) {
    const r = errToResponse(err);
    return c.json(r.body, r.status as 400);
  }
});

// Phase 38 — Yjs snapshot read.
//
// Returns `Y.encodeStateAsUpdate(doc)` bytes as `application/octet-stream`
// so SDK consumers can decode the FULL Y.Doc and use any named shared
// types (Y.XmlFragment, Y.Map, Y.Array — Tiptap/ProseMirror,
// Notion-style block editors). Pairs with the SDK's
// `vfs.readYjsSnapshot(path)` and the Worker RPC `vfsReadYjsSnapshot`.
//
// EINVAL for non-yjs paths, EACCES for encrypted yjs files (server
// cannot materialise — clients must use `openYDoc` and decrypt op log
// locally for those tenants).
vfs.post("/readYjsSnapshot", async (c) => {
  try {
    const body = await c.req.json<{ path: string }>();
    const path = expectPath(body);
    const bytes = await userStub(c).vfsReadYjsSnapshot(c.var.scope, path);
    return new Response(bytes, {
      status: 200,
      headers: { "Content-Type": "application/octet-stream" },
    });
  } catch (err) {
    const r = errToResponse(err);
    return c.json(r.body, r.status as 400);
  }
});

// admin: enable/disable per-tenant versioning. Operator-
// class RPC; Bearer-gated like the rest of /api/vfs/*. The userId
// argument is derived from the verified scope (tenant + optional sub),
// not from the request body — cross-tenant manipulation is impossible.
vfs.post("/admin/setVersioning", async (c) => {
  try {
    const body = await c.req.json<{ enabled: boolean }>();
    if (typeof body.enabled !== "boolean") {
      return c.json(
        { code: "EINVAL", message: "body.enabled must be a boolean" },
        400
      );
    }
    const userId = c.var.scope.sub
      ? `${c.var.scope.tenant}::${c.var.scope.sub}`
      : c.var.scope.tenant;
    const r = await (
      userStub(c) as unknown as {
        adminSetVersioning(uid: string, on: boolean): Promise<{ enabled: boolean }>;
      }
    ).adminSetVersioning(userId, body.enabled);
    return c.json(r);
  } catch (err) {
    const r = errToResponse(err);
    return c.json(r.body, r.status as 400);
  }
});

// flushYjs HTTP endpoint. Triggers a Yjs compaction
// whose checkpoint emits a USER-VISIBLE Mossaic version row (when
// versioning is enabled). Symmetric with the binding-mode
// `YDocHandle.flush({ label })` surface.
vfs.post("/flushYjs", async (c) => {
  try {
    const body = await c.req.json<{ path: string; label?: string }>();
    const path = expectPath(body);
    const r = await userStub(c).vfsFlushYjs(c.var.scope, path, {
      label: body.label,
    });
    return c.json(r);
  } catch (err) {
    const r = errToResponse(err);
    return c.json(r.body, r.status as 400);
  }
});

// patchMetadata HTTP endpoint already exists above (PATCH
// /metadata + POST /patchMetadata). Kept here as a comment for grep
// continuity.

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

// ── versioning ────────────────────────────────────────────────

vfs.post("/listVersions", async (c) => {
  try {
    const body = await c.req.json<{
      path: string;
      limit?: number;
      userVisibleOnly?: boolean;
      includeMetadata?: boolean;
    }>();
    const path = expectPath(body);
    const rows = await userStub(c).vfsListVersions(c.var.scope, path, {
      limit: body.limit,
      userVisibleOnly: body.userVisibleOnly,
      includeMetadata: body.includeMetadata,
    });
    // Map server VersionRow → public VersionInfo. surfaces
    // label + userVisible; propagates them through the
    // HTTP fallback so external consumers (CLI, etc.) see the same
    // shape as binding-mode callers.
    const versions = rows.map((r) => ({
      id: r.versionId,
      mtimeMs: r.mtimeMs,
      size: r.size,
      mode: r.mode,
      deleted: r.deleted,
      label: r.label,
      userVisible: r.userVisible,
      metadata: body.includeMetadata ? r.metadata ?? null : undefined,
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

// ── copyFile, metadata, listFiles, version-mark ──────────────

const copyFileHandler = async (
  c: import("hono").Context<{ Bindings: Env; Variables: { scope: VFSScope } }>
) => {
  try {
    const body = await c.req.json<{
      src: string;
      dest: string;
      opts?: {
        metadata?: Record<string, unknown> | null;
        tags?: readonly string[];
        version?: { label?: string; userVisible?: boolean };
        overwrite?: boolean;
      };
    }>();
    if (typeof body.src !== "string" || typeof body.dest !== "string") {
      return c.json(
        { code: "EINVAL", message: "body.src and body.dest must be strings" },
        400
      );
    }
    await userStub(c).vfsCopyFile(c.var.scope, body.src, body.dest, body.opts);
    return c.json({ ok: true });
  } catch (err) {
    const r = errToResponse(err);
    return c.json(r.body, r.status as 400);
  }
};
vfs.post("/copy", copyFileHandler);
vfs.post("/copyFile", copyFileHandler);

// PATCH is the RESTful verb for partial-update; we ALSO accept POST
// so the HttpVFS client (which uses a single POST helper) can call
// the same handler.
const patchMetadataHandler = async (
  c: import("hono").Context<{ Bindings: Env; Variables: { scope: VFSScope } }>
) => {
  try {
    const body = await c.req.json<{
      path: string;
      patch: Record<string, unknown> | null;
      opts?: { addTags?: readonly string[]; removeTags?: readonly string[] };
    }>();
    const path = expectPath(body);
    await userStub(c).vfsPatchMetadata(c.var.scope, path, body.patch, body.opts);
    return c.json({ ok: true });
  } catch (err) {
    const r = errToResponse(err);
    return c.json(r.body, r.status as 400);
  }
};
vfs.patch("/metadata", patchMetadataHandler);
vfs.post("/patchMetadata", patchMetadataHandler);

const listFilesHandler = async (
  c: import("hono").Context<{ Bindings: Env; Variables: { scope: VFSScope } }>
) => {
  try {
    // Use POST for /list so callers can pass complex `metadata`
    // filter objects without URL-encoding gymnastics. The HTTP
    // route's JSON body mirrors the typed RPC surface 1:1.
    const body = await c.req.json<{
      prefix?: string;
      tags?: readonly string[];
      metadata?: Record<string, unknown>;
      limit?: number;
      cursor?: string;
      orderBy?: "mtime" | "name" | "size";
      direction?: "asc" | "desc";
      includeStat?: boolean;
      includeMetadata?: boolean;
      includeTombstones?: boolean;
    }>();
    const r = await userStub(c).vfsListFiles(c.var.scope, body);
    return c.json(r);
  } catch (err) {
    const r = errToResponse(err);
    return c.json(r.body, r.status as 400);
  }
};
vfs.post("/list", listFilesHandler);
vfs.post("/listFiles", listFilesHandler);

vfs.post("/fileInfo", async (c) => {
  try {
    const body = await c.req.json<{
      path: string;
      includeStat?: boolean;
      includeMetadata?: boolean;
      includeTombstones?: boolean;
    }>();
    const path = expectPath(body);
    const item = await userStub(c).vfsFileInfo(c.var.scope, path, {
      includeStat: body.includeStat,
      includeMetadata: body.includeMetadata,
      includeTombstones: body.includeTombstones,
    });
    return c.json({ item });
  } catch (err) {
    const r = errToResponse(err);
    return c.json(r.body, r.status as 400);
  }
});

const markVersionHandler = async (
  c: import("hono").Context<{ Bindings: Env; Variables: { scope: VFSScope } }>
) => {
  try {
    const body = await c.req.json<{
      path: string;
      versionId: string;
      label?: string;
      userVisible?: boolean;
    }>();
    const path = expectPath(body);
    if (typeof body.versionId !== "string") {
      return c.json(
        { code: "EINVAL", message: "body.versionId must be a string" },
        400
      );
    }
    await userStub(c).vfsMarkVersion(c.var.scope, path, body.versionId, {
      label: body.label,
      userVisible: body.userVisible,
    });
    return c.json({ ok: true });
  } catch (err) {
    const r = errToResponse(err);
    return c.json(r.body, r.status as 400);
  }
};
vfs.put("/version-mark", markVersionHandler);
vfs.post("/markVersion", markVersionHandler);

// Catch-all 404 for unknown /api/vfs/<method> paths. Without this,
// the parent app's wildcard route would forward to ASSETS (or
// wherever) and clients would see weird 500s instead of a clean 404.
vfs.all("*", (c) =>
  c.json({ code: "ENOENT", message: "Unknown VFS method" }, 404)
);

export default vfs;
