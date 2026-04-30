/**
 * Multipart parallel transfer engine, HTTP routes.
 *
 * Mounted at `/api/vfs/multipart/*` from `worker/core/index.ts`.
 *
 * Endpoint inventory:
 *   POST   /api/vfs/multipart/begin              → mint session + token
 *   POST   /api/vfs/multipart/finalize           → atomic commit
 *   POST   /api/vfs/multipart/abort              → drop session
 *   GET    /api/vfs/multipart/:uploadId/status   → landed[] for resume
 *   PUT    /api/vfs/multipart/:uploadId/chunk/:idx
 *                                                → per-chunk PUT (NO UserDO RPC)
 *   POST   /api/vfs/multipart/download-token     → cacheable-chunk dl token
 *   GET    /api/vfs/chunk/:fileId/:idx           → cacheable per-chunk GET
 *
 * The chunk PUT path is the load-bearing surface: it validates the
 * session token via HMAC (CPU-only, no DO touch), computes placement
 * from the token's frozen poolSize, and dispatches a SINGLE
 * ShardDO.putChunkMultipart RPC. Every other route does its work via
 * a typed UserDO RPC (begin/finalize/abort/status/dl-token) plus
 * per-shard fan-out where finalize/abort need it.
 */

import { Hono } from "hono";
import type { MiddlewareHandler } from "hono";
import type { EnvCore as Env } from "../../../shared/types";
import type { VFSScope } from "../../../shared/vfs-types";
import type { UserDOCore } from "../objects/user/user-do-core";
import type { ShardDO } from "../objects/shard/shard-do";
import { vfsUserDOName, vfsShardDOName } from "../lib/utils";
import {
  verifyVFSToken,
  verifyVFSMultipartToken,
  verifyVFSDownloadToken,
  signVFSDownloadToken,
  VFSConfigError,
} from "../lib/auth";
import {
  MULTIPART_MAX_CHUNK_BYTES,
  type MultipartBeginRequest,
  type MultipartFinalizeRequest,
  type MultipartAbortRequest,
  type MultipartPutChunkResponse,
  type MultipartStatusResponse,
  type DownloadTokenRequest,
  type DownloadTokenResponse,
} from "../../../shared/multipart";
import { hashChunk } from "../../../shared/crypto";
import { placeChunk } from "../../../shared/placement";
import { userIdFor } from "../objects/user/vfs/helpers";

// ── Shared auth middleware (Bearer VFS token) ──────────────────────────

const vfsBearer = (): MiddlewareHandler<{
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

function userStub(c: {
  env: Env;
  var: { scope: VFSScope };
}): UserDOCore {
  const scope = c.var.scope;
  const name = vfsUserDOName(scope.ns, scope.tenant, scope.sub);
  const id = c.env.MOSSAIC_USER.idFromName(name);
  return c.env.MOSSAIC_USER.get(id) as unknown as UserDOCore;
}

function shardStub(
  env: Env,
  scope: VFSScope,
  shardIndex: number
) {
  const ns =
    env.MOSSAIC_SHARD as unknown as DurableObjectNamespace<ShardDO>;
  const name = vfsShardDOName(scope.ns, scope.tenant, scope.sub, shardIndex);
  return ns.get(ns.idFromName(name));
}

/** Map a server-thrown error to a JSON response. Mirrors vfs.ts:errToResponse. */
function errToResponse(err: unknown): {
  status: number;
  body: { code: string; message: string };
} {
  if (err instanceof VFSConfigError) {
    return {
      status: 503,
      body: { code: "EMOSSAIC_UNAVAILABLE", message: err.message },
    };
  }
  const e = err as { code?: unknown; message?: unknown };
  const rawMsg = typeof e?.message === "string" ? e.message : String(err);
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
    EBADF: 409,
    ENOTSUP: 501,
    EMOSSAIC_UNAVAILABLE: 503,
  };
  const explicit = typeof e?.code === "string" ? (e.code as string) : undefined;
  let code: string | undefined =
    explicit && explicit in KNOWN ? explicit : undefined;
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

// ── Multipart router ───────────────────────────────────────────────────

const mp = new Hono<{
  Bindings: Env;
  Variables: { scope: VFSScope };
}>();

mp.use("*", vfsBearer());

// POST /begin
mp.post("/begin", async (c) => {
  try {
    const body = await c.req.json<MultipartBeginRequest>();
    if (typeof body.path !== "string" || body.path.length === 0) {
      return c.json(
        { code: "EINVAL", message: "body.path must be a non-empty string" },
        400
      );
    }
    if (typeof body.size !== "number") {
      return c.json(
        { code: "EINVAL", message: "body.size must be a number" },
        400
      );
    }
    const r = await userStub(c).vfsBeginMultipart(c.var.scope, body.path, {
      size: body.size,
      chunkSize: body.chunkSize,
      mode: body.mode,
      mimeType: body.mimeType,
      metadata: body.metadata,
      tags: body.tags,
      version: body.version,
      encryption: body.encryption,
      resumeFrom: body.resumeFrom,
      ttlMs: body.ttlMs,
    });
    return c.json(r);
  } catch (err) {
    const r = errToResponse(err);
    return c.json(r.body, r.status as 400);
  }
});

// POST /finalize
mp.post("/finalize", async (c) => {
  try {
    const body = await c.req.json<MultipartFinalizeRequest>();
    if (typeof body.uploadId !== "string" || body.uploadId.length === 0) {
      return c.json(
        { code: "EINVAL", message: "body.uploadId must be a non-empty string" },
        400
      );
    }
    if (!Array.isArray(body.chunkHashList)) {
      return c.json(
        { code: "EINVAL", message: "body.chunkHashList must be a string[]" },
        400
      );
    }
    const stub = userStub(c);
    const r = await stub.vfsFinalizeMultipart(
      c.var.scope,
      body.uploadId,
      body.chunkHashList
    );
    // Pre-generate standard preview variants in the background so
    // the user's first gallery click hits a warm cache. Scoped to
    // image MIME types: that's the gallery-thumbnail use case where
    // pre-gen actually saves user-visible latency. Other renderers
    // (code-svg, waveform, icon-card) are cheap enough to run
    // on-demand from `vfsReadPreview`. Best-effort: the routine
    // catches per-variant failures internally.
    if (r.size > 0 && !r.isEncrypted && r.mimeType.startsWith("image/")) {
      c.executionCtx.waitUntil(
        stub.adminPreGenerateStandardVariants(c.var.scope, {
          fileId: r.fileId,
          path: r.path,
          mimeType: r.mimeType,
          fileName: r.path.split("/").pop() ?? r.fileId,
          fileSize: r.size,
          isEncrypted: r.isEncrypted,
        })
      );
    }
    return c.json(r);
  } catch (err) {
    const r = errToResponse(err);
    return c.json(r.body, r.status as 400);
  }
});

// POST /abort
mp.post("/abort", async (c) => {
  try {
    const body = await c.req.json<MultipartAbortRequest>();
    if (typeof body.uploadId !== "string" || body.uploadId.length === 0) {
      return c.json(
        { code: "EINVAL", message: "body.uploadId must be a non-empty string" },
        400
      );
    }
    const r = await userStub(c).vfsAbortMultipart(c.var.scope, body.uploadId);
    return c.json(r);
  } catch (err) {
    const r = errToResponse(err);
    return c.json(r.body, r.status as 400);
  }
});

// GET /:uploadId/status
mp.get("/:uploadId/status", async (c) => {
  try {
    const uploadId = c.req.param("uploadId");
    if (typeof uploadId !== "string" || uploadId.length === 0) {
      return c.json(
        { code: "EINVAL", message: "uploadId required" },
        400
      );
    }
    const r = await userStub(c).vfsGetMultipartStatus(c.var.scope, uploadId);
    const out: MultipartStatusResponse = {
      landed: r.landed,
      total: r.total,
      bytesUploaded: r.bytesUploaded,
      expiresAtMs: r.expiresAtMs,
    };
    return c.json(out);
  } catch (err) {
    const r = errToResponse(err);
    return c.json(r.body, r.status as 400);
  }
});

// PUT /:uploadId/chunk/:idx — the load-bearing path.
//
// Validates the session token statelessly (HMAC verify, no DO RPC),
// hashes the body, places by frozen poolSize, dispatches ONE
// ShardDO.putChunkMultipart RPC. UserDO is never touched.
mp.put("/:uploadId/chunk/:idx", async (c) => {
  try {
    const uploadId = c.req.param("uploadId");
    const idxStr = c.req.param("idx");
    const idx = Number.parseInt(idxStr, 10);
    if (!Number.isInteger(idx) || idx < 0) {
      return c.json(
        { code: "EINVAL", message: `idx must be a non-negative integer (got '${idxStr}')` },
        400
      );
    }
    const sessionTokenHeader = c.req.header("X-Session-Token");
    if (typeof sessionTokenHeader !== "string" || sessionTokenHeader.length === 0) {
      return c.json(
        { code: "EACCES", message: "X-Session-Token header required" },
        401
      );
    }
    let payload;
    try {
      payload = await verifyVFSMultipartToken(c.env, sessionTokenHeader);
    } catch (err) {
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
        { code: "EACCES", message: "Invalid or expired session token" },
        401
      );
    }
    // Cross-bind: the token's tenant/ns/sub must match the Bearer.
    if (
      payload.ns !== c.var.scope.ns ||
      payload.tn !== c.var.scope.tenant ||
      (payload.sub ?? undefined) !== (c.var.scope.sub ?? undefined)
    ) {
      return c.json(
        { code: "EACCES", message: "session token scope does not match bearer scope" },
        403
      );
    }
    if (payload.uploadId !== uploadId) {
      return c.json(
        { code: "EACCES", message: "session token does not match uploadId in URL" },
        403
      );
    }
    // Token's `exp` is in seconds (jose convention). Convert + check.
    if (payload.exp * 1000 < Date.now()) {
      return c.json(
        { code: "EACCES", message: "session token expired" },
        401
      );
    }
    if (idx >= payload.totalChunks) {
      return c.json(
        {
          code: "EINVAL",
          message: `idx ${idx} out of range [0, ${payload.totalChunks})`,
        },
        400
      );
    }

    const ct = c.req.header("Content-Type") ?? "";
    if (!ct.startsWith("application/octet-stream")) {
      return c.json(
        {
          code: "EINVAL",
          message: "Content-Type must be application/octet-stream",
        },
        400
      );
    }
    // Defensive size cap before reading the body so a malicious caller
    // can't blow the heap with a 10 GiB request.
    const lenHeader = c.req.header("Content-Length");
    if (lenHeader !== undefined) {
      const len = Number.parseInt(lenHeader, 10);
      if (Number.isFinite(len) && len > MULTIPART_MAX_CHUNK_BYTES) {
        return c.json(
          {
            code: "EFBIG",
            message: `chunk Content-Length ${len} exceeds cap ${MULTIPART_MAX_CHUNK_BYTES}`,
          },
          413
        );
      }
    }

    const ab = await c.req.arrayBuffer();
    const bytes = new Uint8Array(ab);
    if (bytes.byteLength > MULTIPART_MAX_CHUNK_BYTES) {
      return c.json(
        {
          code: "EFBIG",
          message: `chunk byteLength ${bytes.byteLength} exceeds cap ${MULTIPART_MAX_CHUNK_BYTES}`,
        },
        413
      );
    }
    if (bytes.byteLength === 0 && idx !== payload.totalChunks - 1) {
      // Allow zero-byte for an empty file (totalChunks=0 path won't
      // reach here because idx>=totalChunks check above) or for an
      // explicit trailing zero chunk; reject otherwise as garbled.
      // For totalChunks > 0 we reject all zero-length chunks defensively.
      return c.json(
        { code: "EINVAL", message: "zero-length chunk rejected" },
        400
      );
    }

    const hash = await hashChunk(bytes);
    const userId = userIdFor(c.var.scope);
    const sIdx = placeChunk(userId, uploadId, idx, payload.poolSize);
    const stub = shardStub(c.env, c.var.scope, sIdx);
    let putResult;
    try {
      putResult = await stub.putChunkMultipart(
        hash,
        bytes,
        uploadId,
        idx,
        userId
      );
    } catch (err) {
      // Network / shard transient → 503; map shard `EINVAL` (cold-path
      // empty buffer audit) → 400.
      const msg = (err as Error)?.message ?? String(err);
      if (msg.startsWith("EINVAL")) {
        return c.json({ code: "EINVAL", message: msg }, 400);
      }
      return c.json(
        { code: "EMOSSAIC_UNAVAILABLE", message: msg },
        503
      );
    }
    const out: MultipartPutChunkResponse = {
      ok: true,
      hash,
      idx,
      bytesAccepted: bytes.byteLength,
      status: putResult.status,
    };
    return c.json(out);
  } catch (err) {
    const r = errToResponse(err);
    return c.json(r.body, r.status as 400);
  }
});

// POST /download-token
mp.post("/download-token", async (c) => {
  try {
    const body = await c.req.json<DownloadTokenRequest>();
    if (typeof body.path !== "string" || body.path.length === 0) {
      return c.json(
        { code: "EINVAL", message: "body.path must be a non-empty string" },
        400
      );
    }
    // Resolve manifest via the existing typed RPC. Single UserDO RPC.
    const manifest = await userStub(c).vfsOpenManifest(c.var.scope, body.path);
    const { token, expiresAtMs } = await signVFSDownloadToken(
      c.env,
      {
        fileId: manifest.fileId,
        ns: c.var.scope.ns,
        tn: c.var.scope.tenant,
        sub: c.var.scope.sub,
      },
      body.ttlMs
    );
    const out: DownloadTokenResponse = {
      token,
      expiresAtMs,
      manifest: {
        fileId: manifest.fileId,
        size: manifest.size,
        chunkSize: manifest.chunkSize,
        chunkCount: manifest.chunkCount,
        chunks: manifest.chunks.map((ch) => ({
          index: ch.index,
          hash: ch.hash,
          size: ch.size,
        })),
        inlined: manifest.inlined,
      },
    };
    return c.json(out);
  } catch (err) {
    const r = errToResponse(err);
    return c.json(r.body, r.status as 400);
  }
});

mp.all("*", (c) =>
  c.json({ code: "ENOENT", message: "Unknown multipart endpoint" }, 404)
);

export default mp;

// ── Cacheable chunk download endpoint ──────────────────────────────────
//
// Mounted at /api/vfs/chunk/:fileId/:idx as a separate Hono app so
// the path doesn't conflict with the multipart `:uploadId/chunk/:idx`
// PUT. Validates a download token (signed via signVFSDownloadToken),
// resolves the chunk's hash + shard via the existing
// `vfsReadChunk`-shaped UserDO RPC, then streams the chunk bytes.
//
// Cache hit path: lookup `caches.default` keyed on (fileId, idx) —
// chunks are content-addressed and immutable per fileId, so a
// 1-year `Cache-Control: immutable` is safe.

export const chunkDownload = new Hono<{ Bindings: Env }>();

chunkDownload.get("/chunk/:fileId/:idx", async (c) => {
  try {
    const fileId = c.req.param("fileId");
    const idxStr = c.req.param("idx");
    const idx = Number.parseInt(idxStr, 10);
    if (!Number.isInteger(idx) || idx < 0) {
      return c.json({ code: "EINVAL", message: "idx must be a non-negative integer" }, 400);
    }
    const tokenStr =
      c.req.query("token") ??
      (c.req.header("Authorization")?.startsWith("Bearer ")
        ? c.req.header("Authorization")!.slice(7)
        : undefined);
    if (typeof tokenStr !== "string" || tokenStr.length === 0) {
      return c.json(
        { code: "EACCES", message: "?token=… or Bearer required" },
        401
      );
    }
    let payload;
    try {
      payload = await verifyVFSDownloadToken(c.env, tokenStr);
    } catch (err) {
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
        { code: "EACCES", message: "invalid or expired download token" },
        401
      );
    }
    if (payload.fileId !== fileId) {
      return c.json(
        { code: "EACCES", message: "download token does not match fileId" },
        403
      );
    }
    if (payload.exp * 1000 < Date.now()) {
      return c.json({ code: "EACCES", message: "download token expired" }, 401);
    }

    const cacheKey = new Request(
      `https://chunks.mossaic.local/${fileId}/${idx}`,
      { method: "GET" }
    );
    const cache = (caches as unknown as { default: Cache }).default;
    const cached = await cache.match(cacheKey);
    if (cached) {
      return cached;
    }

    // Resolve chunk via UserDO. The vfsReadChunk RPC validates scope
    // and returns the bytes — not ideal because it streams the bytes
    // through the UserDO too, but it's the simplest correct path
    // for v1. A future optimisation can split lookup-and-stream
    // (manifest cache + shard direct).
    const scope: VFSScope = {
      ns: payload.ns,
      tenant: payload.tn,
      sub: payload.sub,
    };
    const stub = (c.env.MOSSAIC_USER.get(
      c.env.MOSSAIC_USER.idFromName(
        vfsUserDOName(scope.ns, scope.tenant, scope.sub)
      )
    ) as unknown) as UserDOCore;

    // Look up the manifest to find (hash, sIdx) without paying
    // the full chunk read through the UserDO. We need a lookup-by-
    // fileId; reuse `vfsOpenManifest` by resolving fileId → path is
    // not possible (fileId is the immutable identity, path is
    // mutable). Instead we ship a small lookup via vfsReadChunk
    // routed through a path-by-fileId — for v1, take the simpler
    // route: fall back to scanning the path that owns this fileId.
    // BUT: the typed UserDO surface doesn't expose a path-by-fileId
    // lookup. For v1, return a 501 if the lookup is unavailable;
    // the SDK uses /download-token which already returns the
    // manifest, so the SDK doesn't need this endpoint to do its own
    // lookup. The endpoint is therefore most useful when paired with
    // a manifest the caller already has — we accept ?hash=… and
    // ?shard=… as authoritative routing hints, defended by the
    // download token (which ties caller to fileId).
    const hashHint = c.req.query("hash");
    const shardHint = c.req.query("shard");
    if (
      typeof hashHint === "string" &&
      /^[0-9a-f]{64}$/.test(hashHint) &&
      typeof shardHint === "string"
    ) {
      const sIdx = Number.parseInt(shardHint, 10);
      if (!Number.isInteger(sIdx) || sIdx < 0) {
        return c.json(
          { code: "EINVAL", message: "shard must be a non-negative integer" },
          400
        );
      }
      const ssub = shardStub(c.env, scope, sIdx);
      const res = await ssub.fetch(
        new Request(`http://internal/chunk/${hashHint}`)
      );
      if (!res.ok) {
        return c.json(
          { code: "ENOENT", message: `chunk not found on shard ${sIdx}` },
          404
        );
      }
      const buf = await res.arrayBuffer();
      const response = new Response(buf, {
        status: 200,
        headers: {
          "Content-Type": "application/octet-stream",
          "Content-Length": String(buf.byteLength),
          "Cache-Control": "public, max-age=31536000, immutable",
          ETag: `"${hashHint}"`,
          "X-Mossaic-Hash": hashHint,
        },
      });
      // Cache asynchronously; respond immediately.
      c.executionCtx.waitUntil(cache.put(cacheKey, response.clone()));
      return response;
    }

    // Without hints we have to walk the manifest. Use vfsReadChunk
    // — the path lookup happens server-side. Pay one UserDO RPC.
    // This requires the path; we don't have it. Instead, walk via
    // the manifest fan-out: but we don't have path either. The
    // contract here is: callers MUST supply ?hash= and ?shard= for
    // the no-DO-touch path, and SHOULD use /download-token to obtain
    // them along with the auth token. Without hints, we fail clearly.
    void stub; // avoid unused
    return c.json(
      {
        code: "EINVAL",
        message:
          "?hash= and ?shard= required (obtain via /api/vfs/multipart/download-token which returns the manifest)",
      },
      400
    );
  } catch (err) {
    const r = errToResponse(err);
    return c.json(r.body, r.status as 400);
  }
});
