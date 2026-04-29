/**
 * App-pinned multipart upload route.
 *
 * Mounted at `/api/upload/multipart/*` from `worker/app/index.ts`.
 *
 * **Architecture β.** Mirrors `worker/core/routes/multipart-routes.ts`
 * line-for-line with three substitutions:
 *  1. Auth: App JWT (`authMiddleware`) instead of VFS Bearer token.
 *     `c.get("userId")` doubles as the tenant identity for placement.
 *  2. UserDO addressing: legacy `userDOName(userId)` instead of
 *     canonical `vfsUserDOName(ns, tenant, sub)`.
 *  3. Placement: explicit `legacyAppPlacement.{shardDOName,placeChunk}`
 *     instead of `getPlacement(scope)`.
 *
 * The chunk PUT path stays the load-bearing surface: stateless HMAC
 * verify (no UserDO RPC), one ShardDO.putChunkMultipart RPC. The
 * ShardDO physical class is shared with canonical (`upload_chunks`
 * staging table), so no shard-class change.
 *
 * **Score-template invariance** (§1.4): chunk placement
 * uses the SAME rendezvous score key as the legacy single-chunk path
 * (`shard:${userId}:${idx}` prefix). New chunks land on the same
 * physical ShardDO instances as existing photo-library data. Verified
 * by the migration-safety integration test.
 *
 * Endpoint inventory (mirrors canonical):
 *   POST   /api/upload/multipart/begin              → mint session + token
 *   POST   /api/upload/multipart/finalize           → atomic commit
 *   POST   /api/upload/multipart/abort              → drop session
 *   GET    /api/upload/multipart/:uploadId/status   → landed[] for resume
 *   PUT    /api/upload/multipart/:uploadId/chunk/:idx
 *                                                   → per-chunk PUT
 *   POST   /api/upload/multipart/download-token     → cacheable-chunk dl token
 */

import { Hono } from "hono";
import type { EnvApp as Env } from "@shared/types";
import { authMiddleware } from "@core/lib/auth";
import { legacyAppPlacement } from "@shared/placement";
import { userDOName } from "@core/lib/utils";
import type { UserDO } from "../objects/user/user-do";
import type { ShardDO } from "@core/objects/shard/shard-do";
import {
  verifyVFSMultipartToken,
  verifyVFSDownloadToken,
  signVFSDownloadToken,
  VFSConfigError,
} from "@core/lib/auth";
import {
  MULTIPART_MAX_CHUNK_BYTES,
  type MultipartBeginRequest,
  type MultipartFinalizeRequest,
  type MultipartAbortRequest,
  type MultipartPutChunkResponse,
  type MultipartStatusResponse,
  type DownloadTokenRequest,
  type DownloadTokenResponse,
} from "@shared/multipart";
import { hashChunk } from "@shared/crypto";

// ── Helpers ────────────────────────────────────────────────────────────

/** Resolve the App's UserDO instance for a given userId. */
function userStub(c: { env: Env; var: { userId: string } }): UserDO {
  const id = c.env.MOSSAIC_USER.idFromName(userDOName(c.var.userId));
  return c.env.MOSSAIC_USER.get(id) as unknown as UserDO;
}

/** Resolve a legacy ShardDO instance for `(userId, shardIdx)`. */
function shardStub(env: Env, userId: string, shardIndex: number) {
  const ns = env.MOSSAIC_SHARD as unknown as DurableObjectNamespace<ShardDO>;
  const name = legacyAppPlacement.shardDOName(
    { ns: "default", tenant: userId },
    shardIndex
  );
  return ns.get(ns.idFromName(name));
}

/** Map server-thrown errors → JSON HTTP response. Mirrors canonical. */
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
  Variables: { userId: string; email: string };
}>();

mp.use("*", authMiddleware());

/** POST /begin — mint a session + token. */
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
    const userId = c.get("userId");
    const r = await userStub(c).appBeginMultipart(userId, body.path, {
      size: body.size,
      chunkSize: body.chunkSize,
      mimeType: body.mimeType,
      // SPA passes parentId via the metadata field for
      // back-compat with the canonical wire shape.
      parentId:
        body.metadata && typeof body.metadata === "object"
          ? ((body.metadata as { parentId?: string | null }).parentId ?? null)
          : null,
      ttlMs: body.ttlMs,
      resumeFrom: body.resumeFrom,
    });
    return c.json(r);
  } catch (err) {
    const r = errToResponse(err);
    return c.json(r.body, r.status as 400);
  }
});

/** POST /finalize — verify + commit. */
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
    const userId = c.get("userId");
    const r = await userStub(c).appFinalizeMultipart(
      userId,
      body.uploadId,
      body.chunkHashList
    );
    return c.json(r);
  } catch (err) {
    const r = errToResponse(err);
    return c.json(r.body, r.status as 400);
  }
});

/** POST /abort — idempotent cleanup. */
mp.post("/abort", async (c) => {
  try {
    const body = await c.req.json<MultipartAbortRequest>();
    if (typeof body.uploadId !== "string" || body.uploadId.length === 0) {
      return c.json(
        { code: "EINVAL", message: "body.uploadId must be a non-empty string" },
        400
      );
    }
    const userId = c.get("userId");
    const r = await userStub(c).appAbortMultipart(userId, body.uploadId);
    return c.json(r);
  } catch (err) {
    const r = errToResponse(err);
    return c.json(r.body, r.status as 400);
  }
});

/** GET /:uploadId/status — landed[] / total / bytesUploaded / expiresAtMs. */
mp.get("/:uploadId/status", async (c) => {
  try {
    const uploadId = c.req.param("uploadId");
    if (typeof uploadId !== "string" || uploadId.length === 0) {
      return c.json({ code: "EINVAL", message: "uploadId required" }, 400);
    }
    const userId = c.get("userId");
    const r = await userStub(c).appGetMultipartStatus(userId, uploadId);
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

/**
 * PUT /:uploadId/chunk/:idx — load-bearing path.
 *
 * Stateless HMAC session-token verify (no UserDO RPC). Hashes the
 * body, dispatches ONE ShardDO.putChunkMultipart RPC against the
 * legacy `shard:${userId}:${idx}` instance.
 */
mp.put("/:uploadId/chunk/:idx", async (c) => {
  try {
    const uploadId = c.req.param("uploadId");
    const idxStr = c.req.param("idx");
    const idx = Number.parseInt(idxStr, 10);
    if (!Number.isInteger(idx) || idx < 0) {
      return c.json(
        {
          code: "EINVAL",
          message: `idx must be a non-negative integer (got '${idxStr}')`,
        },
        400
      );
    }
    const sessionTokenHeader = c.req.header("X-Session-Token");
    if (
      typeof sessionTokenHeader !== "string" ||
      sessionTokenHeader.length === 0
    ) {
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
    const userId = c.get("userId");
    // Cross-bind: token's `tn` MUST match App JWT's userId. Token's
    // `ns` MUST be "default" (App scope).
    if (payload.tn !== userId || payload.ns !== "default") {
      return c.json(
        {
          code: "EACCES",
          message:
            "session token scope does not match App JWT (tn/ns mismatch)",
        },
        403
      );
    }
    if (payload.uploadId !== uploadId) {
      return c.json(
        {
          code: "EACCES",
          message: "session token does not match uploadId in URL",
        },
        403
      );
    }
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
      return c.json(
        { code: "EINVAL", message: "zero-length chunk rejected" },
        400
      );
    }

    const hash = await hashChunk(bytes);
    // Explicit legacyAppPlacement.placeChunk dispatch — keeps new
    // chunks landing on the same physical instances as legacy data.
    const sIdx = legacyAppPlacement.placeChunk(
      { ns: "default", tenant: userId },
      uploadId,
      idx,
      payload.poolSize
    );
    const stub = shardStub(c.env, userId, sIdx);
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

/**
 * POST /download-token — mint a download token + manifest.
 *
 * Resolves the file's manifest via `appOpenManifest` (legacy schema),
 * signs a `vfs-dl` token, returns both. The SDK's `parallelDownload`
 * then drives parallel chunk fetches against `/api/download/chunk/...`
 * (the App's existing chunk download route, which the SPA's
 * `chunkFetchBaseOverride` already targets).
 */
mp.post("/download-token", async (c) => {
  try {
    const body = await c.req.json<DownloadTokenRequest>();
    if (typeof body.path !== "string" || body.path.length === 0) {
      return c.json(
        { code: "EINVAL", message: "body.path must be a non-empty string" },
        400
      );
    }
    // The SPA passes `path` as the file's `fileId` (legacy App
    // identifies files by id, not path). Accept either: if the
    // string looks like a fileId (alnum + ULID-ish), use it directly;
    // otherwise fall back to a manifest lookup by path (unsupported
    // in legacy v1; SPA uses fileId).
    const fileId = body.path.startsWith("/")
      ? body.path.slice(1) // tolerate leading slash
      : body.path;
    const userId = c.get("userId");
    const manifest = await userStub(c).appOpenManifest(fileId);
    const { token, expiresAtMs } = await signVFSDownloadToken(
      c.env,
      {
        fileId: manifest.fileId,
        ns: "default",
        tn: userId,
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
        chunks: manifest.chunks,
        inlined: manifest.inlined,
        // surface mimeType for SPA Blob construction.
        mimeType: manifest.mimeType,
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

// `verifyVFSDownloadToken` is imported but unused at the route layer
// because the App's `/api/download/chunk/:fileId/:chunkIndex` route
// uses App JWT auth, NOT a download token. The token is emitted for
// future cacheable-chunk endpoints; v1 uses Bearer only.
void verifyVFSDownloadToken;

export default mp;
