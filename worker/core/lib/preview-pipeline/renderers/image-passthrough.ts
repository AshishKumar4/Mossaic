/**
 * Image-passthrough renderer — Phase 39 A3.
 *
 * Returns the source bytes verbatim with their original MIME type
 * and the variant's nominal display dimensions. Used as the
 * `EMOSSAIC_UNAVAILABLE` fallback for `image/*` sources when the
 * Cloudflare Images binding is absent (test miniflare, service-mode
 * deployments without IMAGES). Strictly better than icon-card for
 * image MIMEs because the consumer at least gets a usable preview
 * — the browser scales it down on display.
 *
 * Non-image MIMEs (video, document, etc.) keep the icon-card
 * fallback because returning their original bytes (e.g. a 200 MB
 * MP4 in lieu of a 256-px thumbnail) would dwarf the Worker's
 * response budget and is never what the caller wanted from
 * "give me a thumbnail".
 *
 * Determinism: the bytes are content-addressed by the source file,
 * so passthrough's output hash is byte-identical for byte-identical
 * inputs. That keeps `file_variants.chunk_hash` content-deterministic
 * (the load-bearing property for variant dedup).
 *
 * Width/height are reported as the variant's nominal request
 * dimensions, NOT the actual decoded image size — we don't decode
 * here. Consumers that care about the true intrinsic dimensions
 * should ask the IMAGES binding once it's wired.
 */

import type { Renderer } from "../types";
import type {
  RenderInput,
  RenderOpts,
  RenderResult,
  StandardVariant,
} from "../../../../../shared/preview-types";
import { STANDARD_VARIANT_DIMS } from "../../../../../shared/preview-types";

/** Resolve a variant to nominal display dimensions for the result row. */
function resolveDims(opts: RenderOpts): { width: number; height: number } {
  if (typeof opts.variant === "string") {
    const std = STANDARD_VARIANT_DIMS[opts.variant as StandardVariant];
    return { width: std.width, height: std.height };
  }
  return {
    width: opts.variant.width,
    height: opts.variant.height ?? opts.variant.width,
  };
}

export const imagePassthroughRenderer: Renderer = {
  kind: "image-passthrough",

  /**
   * Strictly image MIMEs. The caller (`renderAndStoreVariant`'s
   * EMOSSAIC_UNAVAILABLE branch) already gates on the source MIME
   * before reaching us, but `canRender` keeps the registry
   * contract honest in case the renderer is ever invoked through
   * `dispatchByMime` directly (which it isn't today — passthrough
   * is dispatchByKind-only).
   */
  canRender(mimeType) {
    return mimeType.startsWith("image/");
  },

  async render(
    input: RenderInput,
    _env,
    opts: RenderOpts
  ): Promise<RenderResult> {
    // Drain the source stream into one buffer. Passthrough doesn't
    // transform — we're just collecting bytes that travel back as
    // the variant's body. Memory bound: bounded by READFILE_MAX
    // (server-side cap, default 100 MB per Phase 39 H7) because
    // this path is only reachable from preview RPCs that route
    // through the same chunked read pipeline.
    const reader = input.bytes.getReader();
    const parts: Uint8Array[] = [];
    let total = 0;
    try {
      while (true) {
        const r = await reader.read();
        if (r.done) break;
        parts.push(r.value);
        total += r.value.byteLength;
      }
    } finally {
      reader.releaseLock();
    }

    // Concatenate. One alloc, one copy — cheaper than incremental
    // grow loops at the sizes we expect here (tenant-uploaded
    // images, typically a few MB).
    const out = new Uint8Array(total);
    let offset = 0;
    for (const part of parts) {
      out.set(part, offset);
      offset += part.byteLength;
    }

    const { width, height } = resolveDims(opts);
    return {
      bytes: out,
      mimeType: input.mimeType,
      width,
      height,
    };
  },
};
