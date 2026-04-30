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
    // Delegate to icon-card. When Browser Run support is wired,
    // this body will queue a headless `<video>` snapshot via
    // `env.BROWSER` and return the resulting PNG/WebP bytes.
    void env;
    return iconCardRenderer.render(input, env, opts);
  },
};
