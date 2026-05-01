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
// Re-use the canonical errToResponse from ./vfs so EAGAIN → 429
// (rate-limit), EBADF / ENOTSUP, and any future codes stay in
// lockstep across both route surfaces. A local re-implementation
// here would drift and collapse EAGAIN to 500.
import { errToResponse } from "./vfs";
import { parseRange, rangeResponse, rangeNotSatisfiableResponse } from "../lib/http-range";
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
import { edgeCacheLookup, edgeCachePut } from "../lib/edge-cache";

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

// errToResponse is imported from ./vfs (canonical). A local
// re-implementation would drift — e.g. lack EAGAIN → 429, so a
// per-tenant rate-limit hit would return 500 instead of "retry-
// with-backoff". The vfs.ts version is the single source of truth
// for HTTP status mapping; both routers must share it.

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
    // Multipart placement is intentionally pure-rendezvous (no
    // `fullShards` skip-set). The per-chunk PUT here and
    // `vfsFinalizeMultipart`'s touched-shard fan-out
    // (multipart-upload.ts:573-587) MUST use the SAME placement
    // decision; the `fullShards` set at finalize time may differ
    // from the set at upload time, and the server has no reliable
    // way to replay the upload-time snapshot. The signed
    // `payload.poolSize` is server-authoritative (HMAC at begin) so
    // adversarial clients can't tamper. Multipart cap-awareness is
    // deferred until we persist a per-session full-shards snapshot.
    // Reads work either way; only the write "prefer less-full
    // shards" optimization is missing here.
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
// resolves the chunk's hash + shard via caller-supplied hints, then
// streams the chunk bytes.
//
// Uses the shared `edgeCache*` helpers (same as readPreview /
// readChunk / openManifest) with surfaceTag="chunk" for a single
// source-of-truth cache convention. Cache key shape:
//   https://chunk.mossaic.local/<userId>/<fileId>/0/d/<chunkHash>/i<idx>
//
// `updatedAt = 0` because the (fileId, hash, idx) triple is
// structurally immutable: download tokens are bound to a specific
// file_id; under versioning-ON each historical version has its
// own file_id (multipart finalize stamps `shard_ref_id =
// uploadId`), so writes never alter what's already in the
// cache for a given fileId.

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
        { code: "EACCES", message: "?token=\u2026 or Bearer required" },
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

    // Caller MUST supply ?hash= and ?shard= for the no-DO-touch
    // path. The contract is: callers obtain them via
    // /api/vfs/multipart/download-token, which signs the token AND
    // returns the manifest. Without hints we have no way to look
    // up the chunk without a UserDO RPC and the cache loses its
    // value.
    const hashHint = c.req.query("hash");
    const shardHint = c.req.query("shard");
    if (
      typeof hashHint !== "string" ||
      !/^[0-9a-f]{64}$/.test(hashHint) ||
      typeof shardHint !== "string"
    ) {
      void stub;
      return c.json(
        {
          code: "EINVAL",
          message:
            "?hash= and ?shard= required (obtain via /api/vfs/multipart/download-token which returns the manifest)",
        },
        400
      );
    }
    const sIdx = Number.parseInt(shardHint, 10);
    if (!Number.isInteger(sIdx) || sIdx < 0) {
      return c.json(
        { code: "EINVAL", message: "shard must be a non-negative integer" },
        400
      );
    }

    // Unified cache key. Per-tenant namespace; chunk hash + idx as
    // bust-keyed extras. The (fileId, hash, idx) triple is
    // immutable for a download-token-scoped fileId.
    const namespace = payload.sub
      ? `${payload.tn}::${payload.sub}`
      : payload.tn;
    const cacheOpts = {
      surfaceTag: "chunk" as const,
      namespace,
      fileId,
      updatedAt: 0,
      extraKeyParts: [`d`, hashHint, `i${idx}`],
      cacheControl: "public, max-age=31536000, immutable",
      waitUntil: c.executionCtx.waitUntil.bind(c.executionCtx),
    };
    // Honour Range when present. Per-chunk Range support lets
    // browsers do byte-precise scrubbing within a chunk. The
    // cache stores the FULL 200; Range responses are served from
    // origin (or from the cached buffer below) and never cached
    // themselves.
    const rangeHeader = c.req.header("Range") ?? null;

    const cached = await edgeCacheLookup(cacheOpts);
    if (cached) {
      if (rangeHeader === null) return cached;
      // Slice the cached body to the requested range. Avoids a
      // ShardDO round-trip on Range hits over an already-cached
      // chunk (the common case for video playback).
      const cachedBuf = new Uint8Array(await cached.arrayBuffer());
      const total = cachedBuf.byteLength;
      const parsed = parseRange(rangeHeader, total);
      if (parsed === "unsatisfiable") {
        return rangeNotSatisfiableResponse(total, "application/octet-stream");
      }
      if (parsed !== null) {
        return rangeResponse(cachedBuf, parsed, total, {
          "Content-Type": "application/octet-stream",
          "Cache-Control": "public, max-age=31536000, immutable",
          Vary: "Authorization",
          ETag: `"${hashHint}"`,
          "X-Mossaic-Hash": hashHint,
        });
      }
      return cached;
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

    // Always build + cache the full 200; emit 206 for Range
    // requests separately. The cache entry must be the full body
    // so a future Range hit can slice it without a ShardDO RPC.
    const fullResponse = new Response(buf, {
      status: 200,
      headers: {
        "Content-Type": "application/octet-stream",
        "Content-Length": String(buf.byteLength),
        "Cache-Control": "public, max-age=31536000, immutable",
        // Vary on Authorization. The download token sits in the
        // Authorization header; without Vary an intermediary CDN
        // could replay the bytes to a request bearing a different
        // (or no) token. The cache key already includes the
        // token's tenant namespace; Vary is the wire assertion
        // that downstream caches honour it.
        Vary: "Authorization",
        ETag: `"${hashHint}"`,
        "X-Mossaic-Hash": hashHint,
        "Accept-Ranges": "bytes",
      },
    });
    edgeCachePut(cacheOpts, fullResponse);

    if (rangeHeader !== null) {
      const total = buf.byteLength;
      const parsed = parseRange(rangeHeader, total);
      if (parsed === "unsatisfiable") {
        return rangeNotSatisfiableResponse(total, "application/octet-stream");
      }
      if (parsed !== null) {
        return rangeResponse(new Uint8Array(buf), parsed, total, {
          "Content-Type": "application/octet-stream",
          "Cache-Control": "public, max-age=31536000, immutable",
          Vary: "Authorization",
          ETag: `"${hashHint}"`,
          "X-Mossaic-Hash": hashHint,
        });
      }
    }
    return fullResponse;
  } catch (err) {
    const r = errToResponse(err);
    return c.json(r.body, r.status as 400);
  }
});
