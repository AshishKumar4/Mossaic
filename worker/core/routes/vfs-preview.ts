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

    const result = await userStub(c).vfsReadPreview(c.var.scope, path, {
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
        },
      });
    }

    return new Response(result.bytes, {
      status: 200,
      headers: {
        "Content-Type": result.mimeType,
        "Content-Length": String(result.bytes.byteLength),
        ETag: etag,
        // Variants are content-addressed — the bytes for a given
        // ETag never change. Year-long immutable cache is safe; on
        // re-render the chunk_hash changes and the ETag changes
        // with it, busting any intermediary cache.
        "Cache-Control": "public, max-age=31536000, immutable",
        "X-Mossaic-Renderer": result.rendererKind,
        "X-Mossaic-Variant-Cache": result.fromVariantTable
          ? "hit"
          : "miss",
        "X-Mossaic-Source-Mime": result.sourceMimeType,
        "X-Mossaic-Width": String(result.width),
        "X-Mossaic-Height": String(result.height),
      },
    });
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

export default preview;
