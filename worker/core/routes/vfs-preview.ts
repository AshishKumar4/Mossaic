/**
 * Preview + batched-manifest HTTP routes. Mounted under `/api/vfs`
 * (same prefix as the rest of the VFS HTTP fallback). Bearer-auth
 * gated via the same `vfsAuth()` middleware exported from `vfs.ts`.
 *
 * - POST /readPreview → variant bytes + content-addressed cache
 *   headers. The renderer dispatch is server-side; the caller
 *   only chooses `(path, variant, format)`.
 *
 * - POST /manifests → batched openManifest. Galleries fetching N
 *   thumbnails would otherwise pay N round-trips for manifests +
 *   N for chunks. Batching the manifest leg cuts that to one.
 *
 * Cache strategy: variant bytes are content-addressed (SHA-256
 * over the rendered output). Responses set:
 *   Cache-Control: public, max-age=31536000, immutable
 *   ETag: W/"<chunk_hash>"
 * The weak ETag form lets intermediaries dedup variants whose
 * payload may differ in trailing bytes (SVG whitespace) but whose
 * semantic content is identical. Clients sending
 * If-None-Match get 304 with no body.
 */

import { Hono } from "hono";
import type { EnvCore as Env } from "../../../shared/types";
import type { VFSScope, OpenManifestResult } from "../../../shared/vfs-types";
import type { ReadPreviewOpts, Variant } from "../../../shared/preview-types";
import { vfsAuth, userStub, errToResponse, expectPath } from "./vfs";
import {
  edgeCacheKeyPart,
  edgeCacheLookup,
  edgeCachePut,
} from "../lib/edge-cache";
import { userIdFor } from "../objects/user/vfs/helpers";
import { verifyPreviewToken } from "../lib/preview-token";
import { VFSConfigError } from "../lib/auth";
import { vfsUserDOName } from "../lib/utils";
import type { UserDOCore } from "../objects/user/user-do-core";

const preview = new Hono<{
  Bindings: Env;
  Variables: { scope: VFSScope };
}>();

preview.use("*", vfsAuth());

/**
 * Validate a `Variant` from JSON. Returns the variant or throws
 * EINVAL. Accepts:
 *   - "thumb" | "medium" | "lightbox"  (string)
 *   - { width, height?, fit? }         (object, w required)
 */
function expectVariant(v: unknown): Variant {
  if (typeof v === "string") {
    if (v === "thumb" || v === "medium" || v === "lightbox") return v;
    throw Object.assign(
      new Error(`EINVAL: unknown standard variant "${v}"`),
      { code: "EINVAL" }
    );
  }
  if (typeof v === "object" && v !== null) {
    const o = v as { width?: unknown; height?: unknown; fit?: unknown };
    if (typeof o.width !== "number" || o.width <= 0) {
      throw Object.assign(
        new Error("EINVAL: variant.width must be a positive number"),
        { code: "EINVAL" }
      );
    }
    const out: Variant = { width: o.width };
    if (typeof o.height === "number" && o.height > 0) out.height = o.height;
    if (o.fit === "cover" || o.fit === "contain" || o.fit === "scale-down") {
      out.fit = o.fit;
    }
    return out;
  }
  throw Object.assign(new Error("EINVAL: variant must be a string or object"), {
    code: "EINVAL",
  });
}

// ── readPreview ────────────────────────────────────────────────────────

preview.post("/readPreview", async (c) => {
  try {
    const body = await c.req.json<{
      path: string;
      variant?: unknown;
      format?: ReadPreviewOpts["format"];
      renderer?: string;
    }>();
    const path = expectPath(body);
    const variant =
      body.variant === undefined ? "thumb" : expectVariant(body.variant);

    // Workers Cache for variant bytes.
    //
    // Pre-flight `vfsResolveCacheKey` returns the cache-bust
    // state in one cheap SQL JOIN: (fileId, headVersionId,
    // updatedAt, encryption stamp). The cache key folds in the
    // variant + format + renderer descriptors so different
    // variants of the same file land on different keys, and
    // every write that bumps headVersionId / updatedAt /
    // encryption stamp lands on a fresh key (old key expires).
    //
    // Cache.match runs AFTER vfsAuth() (above) so a cached
    // response never serves an unauthenticated request.
    //
    // Encrypted files surface ENOTSUP from the underlying RPC
    // (preview.ts:124) — we don't pre-cache them. The cache key
    // includes the encryption fingerprint so a tenant who
    // toggles encryption between writes lands on a different key.
    const stub = userStub(c);
    const ck = await stub.vfsResolveCacheKey(c.var.scope, path);

    const variantKeyPart =
      typeof variant === "string"
        ? variant
        : `c-${variant.width}x${variant.height ?? 0}-${variant.fit ?? "any"}`;
    const formatPart = body.format ?? "auto";
    const rendererPart = body.renderer ?? "auto";
    const encPart = edgeCacheKeyPart(
      `${ck.encryptionMode ?? ""}|${ck.encryptionKeyId ?? ""}`
    );
    // Use headVersionId when set (versioning ON / multipart
    // committed); fall back to updatedAt for non-versioned
    // tenants. Either way the part advances on every write.
    const versionPart = ck.headVersionId ?? `t${ck.updatedAt}`;

    const cacheOpts = {
      surfaceTag: "preview" as const,
      namespace: userIdFor(c.var.scope),
      fileId: ck.fileId,
      updatedAt: ck.updatedAt,
      extraKeyParts: [
        versionPart,
        variantKeyPart,
        formatPart,
        rendererPart,
        encPart,
      ],
      cacheControl: "public, max-age=31536000, immutable",
      waitUntil: c.executionCtx.waitUntil.bind(c.executionCtx),
    };

    const cached = await edgeCacheLookup(cacheOpts);
    if (cached) {
      // Conditional response: honour If-None-Match against the
      // cached ETag too. Streamlines the 304 path on warm hits.
      const ifNoneMatch = c.req.header("If-None-Match");
      const cachedEtag = cached.headers.get("ETag");
      if (ifNoneMatch !== null && cachedEtag !== null && ifNoneMatch === cachedEtag) {
        return new Response(null, {
          status: 304,
          headers: {
            ETag: cachedEtag,
            "Cache-Control": "public, max-age=31536000, immutable",
            // Even the warm-cache-hit 304 must carry Vary so a
            // downstream CDN cannot replay this 304 to a request
            // bearing a different Authorization token. See the 200
            // path below for rationale.
            Vary: "Authorization",
          },
        });
      }
      return cached;
    }

    const result = await stub.vfsReadPreview(c.var.scope, path, {
      variant,
      format: body.format,
      renderer: body.renderer,
    });

    // Conditional response: If-None-Match → 304.
    // ETag is the SHA-256 of the rendered bytes (weak form lets
    // intermediaries dedup variants whose payload differs in
    // trailing whitespace but whose content is identical).
    const digest = await crypto.subtle.digest("SHA-256", result.bytes);
    const etag =
      'W/"' +
      Array.from(new Uint8Array(digest))
        .map((b) => b.toString(16).padStart(2, "0"))
        .join("") +
      '"';
    const ifNoneMatch = c.req.header("If-None-Match");
    if (ifNoneMatch !== undefined && ifNoneMatch === etag) {
      return new Response(null, {
        status: 304,
        headers: {
          ETag: etag,
          "Cache-Control": "public, max-age=31536000, immutable",
          // See the 200 path below for rationale. The 304 also
          // flows through intermediary caches; without Vary, a CDN
          // could replay this response to a request bearing a
          // different Authorization token.
          Vary: "Authorization",
        },
      });
    }

    const fresh = new Response(result.bytes, {
      status: 200,
      headers: {
        "Content-Type": result.mimeType,
        "Content-Length": String(result.bytes.byteLength),
        ETag: etag,
        // Variants are content-addressed — the bytes for a given
        // ETag never change. Year-long immutable cache is safe;
        // on re-render the chunk_hash changes and the ETag
        // changes with it, busting any intermediary cache.
        "Cache-Control": "public, max-age=31536000, immutable",
        // Vary on Authorization so any intermediary CDN keys
        // cached entries by Bearer token and never serves a
        // tenant-A preview to a tenant-B request whose URL
        // collides. The Workers Cache key already includes a
        // per-user namespace; Vary is the wire assertion that
        // downstream caches honour the same axis.
        Vary: "Authorization",
        "X-Mossaic-Renderer": result.rendererKind,
        "X-Mossaic-Variant-Cache": result.fromVariantTable
          ? "hit"
          : "miss",
        "X-Mossaic-Source-Mime": result.sourceMimeType,
        "X-Mossaic-Width": String(result.width),
        "X-Mossaic-Height": String(result.height),
      },
    });
    edgeCachePut(cacheOpts, fresh);
    return fresh;
  } catch (err) {
    const r = errToResponse(err);
    return c.json(r.body, r.status as 400);
  }
});

// ── previewInfo / previewInfoMany ─────────────────────────────────────
//
// HTTP fallback for the SDK's `previewUrl` / `previewInfo` /
// `previewInfoMany` methods. Both endpoints invoke the auth-gated
// mint RPC, which signs HMAC tokens that the SPA then loads
// directly via the public `/api/vfs/preview-variant/:token` route.
//
// The mint RPC is the trust boundary: the auth check (vfsAuth
// middleware above) decides whether the caller can mint a token
// for the requested path. Once minted, the token grants
// CDN-cacheable access to the variant bytes \u2014 no per-request
// auth handshake on the public route.

preview.post("/previewInfo", async (c) => {
  try {
    const body = await c.req.json<{
      path: string;
      variant?: unknown;
      format?: ReadPreviewOpts["format"];
      renderer?: string;
      ttlMs?: number;
    }>();
    const path = expectPath(body);
    const variant =
      body.variant === undefined ? "thumb" : expectVariant(body.variant);
    const ttlMs =
      typeof body.ttlMs === "number" && body.ttlMs > 0 ? body.ttlMs : undefined;
    const info = await userStub(c).vfsMintPreviewToken(c.var.scope, path, {
      variant,
      format: body.format,
      renderer: body.renderer,
      ttlMs,
    });
    return c.json(info);
  } catch (err) {
    const r = errToResponse(err);
    return c.json(r.body, r.status as 400);
  }
});

preview.post("/previewInfoMany", async (c) => {
  try {
    const body = await c.req.json<{
      paths: unknown;
      variant?: unknown;
      format?: ReadPreviewOpts["format"];
      renderer?: string;
      ttlMs?: number;
    }>();
    if (!Array.isArray(body.paths)) {
      throw Object.assign(
        new Error("EINVAL: body.paths must be an array"),
        { code: "EINVAL" }
      );
    }
    if (body.paths.length === 0) return c.json({ results: [] });
    if (body.paths.length > 256) {
      throw Object.assign(
        new Error("EINVAL: max 256 paths per request"),
        { code: "EINVAL" }
      );
    }
    for (const p of body.paths) {
      if (typeof p !== "string") {
        throw Object.assign(
          new Error("EINVAL: every path must be a string"),
          { code: "EINVAL" }
        );
      }
    }
    const variant =
      body.variant === undefined ? "thumb" : expectVariant(body.variant);
    const ttlMs =
      typeof body.ttlMs === "number" && body.ttlMs > 0 ? body.ttlMs : undefined;
    const results = await userStub(c).vfsPreviewInfoMany(
      c.var.scope,
      body.paths as string[],
      {
        variant,
        format: body.format,
        renderer: body.renderer,
        ttlMs,
      }
    );
    return c.json({ results });
  } catch (err) {
    const r = errToResponse(err);
    return c.json(r.body, r.status as 400);
  }
});

// ── batched manifests ─────────────────────────────────────────────────

preview.post("/manifests", async (c) => {
  try {
    const body = await c.req.json<{ paths: unknown }>();
    if (!Array.isArray(body.paths)) {
      throw Object.assign(
        new Error("EINVAL: body.paths must be an array"),
        { code: "EINVAL" }
      );
    }
    const paths = body.paths;
    if (paths.length === 0) return c.json({ manifests: [] });
    if (paths.length > 256) {
      throw Object.assign(
        new Error("EINVAL: max 256 paths per request"),
        { code: "EINVAL" }
      );
    }
    for (const p of paths) {
      if (typeof p !== "string") {
        throw Object.assign(
          new Error("EINVAL: every path must be a string"),
          { code: "EINVAL" }
        );
      }
    }

    // Single DO invocation — vfsOpenManifest is per-path; we
    // serialize the lookups inside one stub call to amortize the
    // network hop. The DO is single-threaded so concurrent
    // promises wouldn't gain anything; the loop is the right shape.
    const stub = userStub(c);
    const results: ({ ok: true; manifest: OpenManifestResult } | {
      ok: false;
      code: string;
      message: string;
    })[] = [];
    for (const p of paths as string[]) {
      try {
        const m = await stub.vfsOpenManifest(c.var.scope, p);
        results.push({ ok: true, manifest: m });
      } catch (perPathErr) {
        const r = errToResponse(perPathErr);
        results.push({
          ok: false,
          code: r.body.code,
          message: r.body.message,
        });
      }
    }
    return c.json({ manifests: results });
  } catch (err) {
    const r = errToResponse(err);
    return c.json(r.body, r.status as 400);
  }
});

// ── public preview-variant route ─────────────────────────────────────
//
// `GET /api/vfs/preview-variant/:token` is a SEPARATE Hono app
// because the auth model differs from the rest of `vfs-preview`:
// the route has NO `vfsAuth()` middleware. The HMAC-signed token
// is the auth signal \u2014 a client who holds a valid token has
// already proved (at mint time, via the auth-gated
// `vfsMintPreviewToken` RPC) that they can read the path. The
// resulting URL is CDN-cacheable: bytes are content-addressed by
// the contentHash claim in the token, so the same URL serves
// identical bytes to any client until the variant is re-rendered
// (which produces a different contentHash and a new mint).
//
// Mirror the pattern at `multipart-routes.ts:514+` (chunk-download)
// where a token-gated public route lives in a separate Hono app
// that is mounted under the same prefix as the auth-gated routes.
//
// Cache key:
//   https://preview-variant.mossaic.local/<contentHash>
// Content-addressed; no per-tenant namespace because identical
// rendered bytes are byte-equivalent across tenants. Cross-tenant
// collision requires breaking SHA-256 (the hash IS the cache key).

export const previewVariant = new Hono<{ Bindings: Env }>();

previewVariant.get("/preview-variant/:token", async (c) => {
  try {
    const tokenStr = c.req.param("token");
    if (typeof tokenStr !== "string" || tokenStr.length === 0) {
      return c.json({ code: "EACCES", message: "token required" }, 401);
    }

    let payload;
    try {
      payload = await verifyPreviewToken(c.env, tokenStr);
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
        { code: "EACCES", message: "invalid or expired preview token" },
        401
      );
    }
    if (payload.exp * 1000 < Date.now()) {
      return c.json(
        { code: "EACCES", message: "preview token expired" },
        401
      );
    }

    // Cache key is the contentHash itself. Workers Cache key
    // shape preserves the helper's
    // `https://<surfaceTag>.mossaic.local/<namespace>/<fileId>/<updatedAt>/<extras>`
    // convention; for content-addressed keys we set updatedAt=0
    // and namespace="cas" (content-addressed storage) so
    // cross-tenant requests with the same contentHash share the
    // entry. Auth gate (HMAC verify) runs BEFORE this lookup, so
    // a cached response cannot serve an unauthenticated request.
    const cacheOpts = {
      surfaceTag: "preview" as const,
      namespace: "cas",
      fileId: payload.contentHash,
      updatedAt: 0,
      extraKeyParts: [
        // Optional width/height query hints fold into the cache key
        // when present. Pre-rendered standard variants don't pay
        // attention to ?w/?h \u2014 the variantKind in the token already
        // pins dimensions \u2014 but custom variants can use them when
        // the route ever forwards the request to a render-on-demand
        // path. Keeping them in the key makes the future change
        // structurally cache-safe.
        c.req.query("w") ? `w${c.req.query("w")}` : "wauto",
        c.req.query("h") ? `h${c.req.query("h")}` : "hauto",
      ],
      cacheControl: "public, max-age=31536000, immutable",
      waitUntil: c.executionCtx.waitUntil.bind(c.executionCtx),
    };

    // If-None-Match short-circuit \u2014 the ETag is the contentHash
    // wrapped in W/"...". Browsers re-issue with this on subsequent
    // page loads; 304 with no body is much faster than the cache
    // hit's full bytes. Honor it first.
    const expectedEtag = `W/"${payload.contentHash}"`;
    const ifNoneMatch = c.req.header("If-None-Match");
    if (ifNoneMatch === expectedEtag) {
      return new Response(null, {
        status: 304,
        headers: {
          ETag: expectedEtag,
          "Cache-Control": "public, max-age=31536000, immutable",
        },
      });
    }

    const cached = await edgeCacheLookup(cacheOpts);
    if (cached) return cached;

    // Cache miss \u2014 derive scope from the token's tenantId, look
    // up the variant row in the user's DO, fetch bytes from the
    // appropriate ShardDO. The DO RPC re-verifies the variant row
    // matches the token's contentHash; mismatch returns null and
    // we surface 410 Gone (token references content that no
    // longer exists; client should re-mint).
    const tenantParts = payload.tenantId.split("::");
    if (tenantParts.length < 2 || tenantParts.length > 3) {
      return c.json(
        { code: "EACCES", message: "preview token tenantId malformed" },
        403
      );
    }
    const scope: VFSScope = {
      ns: tenantParts[0],
      tenant: tenantParts[1],
      sub: tenantParts.length === 3 ? tenantParts[2] : undefined,
    };
    const stub = (c.env.MOSSAIC_USER.get(
      c.env.MOSSAIC_USER.idFromName(
        vfsUserDOName(scope.ns, scope.tenant, scope.sub)
      )
    ) as unknown) as UserDOCore;

    const result = await stub.vfsReadVariantByHash(
      scope,
      payload.fileId,
      payload.variantKind,
      payload.rendererKind,
      payload.headVersionId,
      payload.contentHash
    );
    if (result === null) {
      // Either the variant row was reaped, or its chunk_hash no
      // longer matches (re-render). 410 Gone signals the client
      // should re-mint; 404 would imply "this path never had a
      // preview", which is misleading.
      return c.json(
        {
          code: "ENOENT",
          message: "preview content no longer available; re-mint token",
        },
        410
      );
    }

    const fresh = new Response(result.bytes, {
      status: 200,
      headers: {
        "Content-Type": result.mimeType,
        "Content-Length": String(result.bytes.byteLength),
        ETag: expectedEtag,
        // Year-long immutable cache. Bytes are content-addressed
        // by contentHash; re-renders produce a different hash
        // (different URL).
        "Cache-Control": "public, max-age=31536000, immutable",
        // NO Vary: Authorization. The token IS in the URL path,
        // not a header; the URL itself is the cache key. Adding
        // Vary would force CDN re-fetch on every Authorization
        // value variation \u2014 the opposite of what we want.
        "X-Mossaic-Renderer": payload.rendererKind,
        "X-Mossaic-Variant": payload.variantKind,
        "X-Mossaic-Width": String(result.width),
        "X-Mossaic-Height": String(result.height),
      },
    });
    edgeCachePut(cacheOpts, fresh);
    return fresh;
  } catch (err) {
    const r = errToResponse(err);
    return c.json(r.body, r.status as 400);
  }
});

export default preview;
