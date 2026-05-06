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
 * callers should prefer {@link defaultRegistry} which caches one
 * shared instance per worker process.
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

/**
 * Singleton registry shared by every server-side caller in a
 * single worker process. Built lazily on first access; the
 * registry is stateless after construction so concurrent reads
 * are safe.
 */
let cachedRegistry: RendererRegistry | null = null;
export function defaultRegistry(): RendererRegistry {
  if (cachedRegistry === null) {
    cachedRegistry = buildDefaultRegistry();
  }
  return cachedRegistry;
}

export { RendererRegistry } from "./registry";
export type { Renderer } from "./types";
export { RenderError } from "./types";
export { imageRenderer } from "./renderers/image";
export { codeRenderer } from "./renderers/code";
export { waveformRenderer } from "./renderers/waveform";
export { videoPosterRenderer } from "./renderers/video-poster";
export { iconCardRenderer } from "./renderers/icon-card";
