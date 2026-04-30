/**
 * Image renderer — uses the Cloudflare Images binding for resize +
 * format conversion. Handles `image/*` MIMEs (JPEG, PNG, WebP, AVIF,
 * HEIC, etc.).
 *
 * The renderer is content-deterministic: the Images binding's transform
 * is a pure function of `(input bytes, transform opts)`. EXIF and
 * other timestamp metadata are stripped by the binding's encoder.
 *
 * Bindings absent: when `env.IMAGES` is undefined (test miniflare,
 * service-mode without the binding), `canRender` still returns true
 * for image MIMEs but `render` throws `RenderError("EMOSSAIC_UNAVAILABLE")`.
 * The route handler maps that to 503 — callers who need a resilient
 * fallback should call again with `{renderer: "icon-card"}`.
 */

import type { Renderer } from "../types";
import { RenderError } from "../types";
import type {
  RenderInput,
  RenderOpts,
  RenderResult,
  StandardVariant,
  PreviewFormat,
  FitMode,
} from "../../../../../shared/preview-types";
import { STANDARD_VARIANT_DIMS } from "../../../../../shared/preview-types";

/** Cloudflare Images supports SVG output via no transform; we don't. */
type ImagesOutputFormat =
  | "image/jpeg"
  | "image/png"
  | "image/webp"
  | "image/avif";

/** Map our PreviewFormat to the binding's accepted formats. */
function toImagesFormat(format: PreviewFormat): ImagesOutputFormat {
  // SVG is for icon-card / waveform / code only; force webp here so
  // a caller that asks the image renderer for SVG still gets a
  // raster (we don't vectorise photos).
  if (format === "image/svg+xml") return "image/webp";
  return format;
}

/** Resolve a variant to (width, height, fit). */
function resolveDims(opts: RenderOpts): {
  width: number;
  height: number;
  fit: FitMode;
} {
  if (typeof opts.variant === "string") {
    const std = STANDARD_VARIANT_DIMS[opts.variant as StandardVariant];
    return { width: std.width, height: std.height, fit: std.fit };
  }
  return {
    width: opts.variant.width,
    height: opts.variant.height ?? opts.variant.width,
    fit: opts.variant.fit ?? "scale-down",
  };
}

export const imageRenderer: Renderer = {
  kind: "image-resize",

  canRender(mimeType) {
    return mimeType.startsWith("image/");
  },

  async render(
    input: RenderInput,
    env,
    opts: RenderOpts
  ): Promise<RenderResult> {
    if (!env.IMAGES) {
      throw new RenderError(
        "image-resize",
        "EMOSSAIC_UNAVAILABLE",
        "IMAGES binding not configured; falling back requires explicit renderer override"
      );
    }
    const { width, height, fit } = resolveDims(opts);
    const outFormat = toImagesFormat(opts.format);

    // Run the Images pipeline. Errors thrown by the binding (corrupt
    // input, unsupported sub-format, transform failure) propagate as
    // `RenderError("EINVAL")` so the route maps to 400.
    let pipeline;
    try {
      pipeline = env.IMAGES.input(input.bytes).transform({
        width,
        height,
        fit,
        format: outFormat,
      });
    } catch (err) {
      throw new RenderError(
        "image-resize",
        "EINVAL",
        `IMAGES.transform setup failed: ${err instanceof Error ? err.message : String(err)}`
      );
    }
    let result;
    try {
      result = await pipeline.output({ format: outFormat });
    } catch (err) {
      throw new RenderError(
        "image-resize",
        "EINVAL",
        `IMAGES output failed: ${err instanceof Error ? err.message : String(err)}`
      );
    }
    const response = result.response();
    const ab = await response.arrayBuffer();
    const bytes = new Uint8Array(ab);
    return {
      bytes,
      mimeType: outFormat,
      width,
      height,
    };
  },
};
