/**
 * Preview pipeline — default registry assembly.
 *
 * Order matters: the registry walks renderers in registration order
 * and the first whose `canRender(mime)` returns true wins. Specialised
 * renderers register first; the always-fallback `iconCardRenderer`
 * registers last.
 */

import { RendererRegistry } from "./registry";
import { imageRenderer } from "./renderers/image";
import { codeRenderer } from "./renderers/code";
import { waveformRenderer } from "./renderers/waveform";
import { videoPosterRenderer } from "./renderers/video-poster";
import { iconCardRenderer } from "./renderers/icon-card";

/**
 * Build the default registry. Returns a fresh instance per call so
 * tests can mutate without leaking state across cases. Production
 * callers (`vfsReadPreview`, `vfsFinalizeMultipart`) cache a single
 * instance per UserDO at the module scope where they import this.
 */
export function buildDefaultRegistry(): RendererRegistry {
  const r = new RendererRegistry();
  r.register(imageRenderer);
  r.register(codeRenderer);
  r.register(waveformRenderer);
  r.register(videoPosterRenderer);
  r.register(iconCardRenderer);
  return r;
}

export { RendererRegistry } from "./registry";
export type { Renderer } from "./types";
export { RenderError } from "./types";
export { imageRenderer } from "./renderers/image";
export { codeRenderer } from "./renderers/code";
export { waveformRenderer } from "./renderers/waveform";
export { videoPosterRenderer } from "./renderers/video-poster";
export { iconCardRenderer } from "./renderers/icon-card";
