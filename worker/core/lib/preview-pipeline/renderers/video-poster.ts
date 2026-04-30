/**
 * Video poster-frame renderer.
 *
 * Phase 20 ships a stub: `canRender` matches `video/*` MIMEs and
 * delegates to the icon-card renderer for the actual bytes. The
 * `kind` is still `"video-poster"` so the registry surface and
 * tests can reason about a dedicated entry — once Phase 20.1 wires
 * the Browser Run binding (snapshot a `<video>` element via
 * headless Chromium), the renderer's `render` body changes; the
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
    // Phase 20: delegate to icon-card. Browser Run wiring lands in
    // Phase 20.1 — at that point this body queues a headless
    // `<video>` snapshot via `env.BROWSER` and returns the resulting
    // PNG/WebP bytes.
    void env;
    return iconCardRenderer.render(input, env, opts);
  },
};
