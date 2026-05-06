/**
 * Video poster-frame renderer.
 *
 * `canRender` matches `video/*` MIMEs and the renderer's body
 * delegates to the icon-card renderer for the actual bytes. The
 * `kind` is still `"video-poster"` so the registry surface and
 * tests can reason about a dedicated entry — when Browser Run
 * support lands (snapshot a `<video>` element via headless
 * Chromium), only this renderer's `render` body changes; the
 * registry contract stays stable.
 *
 * This deliberately does NOT register `iconCardRenderer` directly:
 * keeping a separate `kind` lets observability distinguish "video
 * file with no real poster yet" from "unknown MIME → icon-card".
 *
 * Determinism: delegates to `iconCardRenderer.render` which is
 * deterministic in `(fileName, fileSize, variant)`.
 */

import type { Renderer } from "../types";
import type {
  RenderInput,
  RenderOpts,
  RenderResult,
} from "../../../../../shared/preview-types";
import { iconCardRenderer } from "./icon-card";

export const videoPosterRenderer: Renderer = {
  kind: "video-poster",

  canRender(mimeType) {
    return mimeType.startsWith("video/");
  },

  async render(
    input: RenderInput,
    env,
    opts: RenderOpts
  ): Promise<RenderResult> {
    // Delegate to icon-card. The BROWSER (Browser Run) binding
    // was removed in Phase 39 — when a real video-poster pipeline
    // is wired (likely via a different binding name), this body
    // will run a headless snapshot and return the resulting
    // PNG/WebP bytes. Until then, video MIMEs render as the
    // generic icon-card stub.
    void env;
    return iconCardRenderer.render(input, env, opts);
  },
};
