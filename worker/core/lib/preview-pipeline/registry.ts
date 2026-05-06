/**
 * Renderer registry. Holds an ordered list of renderers and dispatches
 * by MIME type or renderer-kind. The list is order-sensitive: earlier
 * renderers win when multiple `canRender(mime)` predicates match. The
 * always-fallback `iconCardRenderer` is registered LAST so it catches
 * MIMEs no specialised renderer accepted.
 *
 * Design notes:
 *  - The registry is a plain class, not a singleton at module scope —
 *    consumers (tests, custom-renderer SDK callers) construct their
 *    own instances. The default registry is built in `./index.ts` via
 *    `buildDefaultRegistry()`.
 *  - `dispatchByKind(kind)` returns the renderer with that exact
 *    `kind` string OR `null`; the route handler maps null →
 *    `400 EINVAL`. `dispatchByMime(mime)` always returns a renderer
 *    because the icon-card fallback accepts everything.
 */

import type { Renderer } from "./types";
import { RenderError } from "./types";

export class RendererRegistry {
  private readonly renderers: Renderer[] = [];

  /**
   * Append a renderer to the registry. Last-registered renderers are
   * checked LAST during MIME dispatch — register specialised
   * renderers first, fallbacks last.
   *
   * Throws `RenderError("EINVAL")` on duplicate `kind`.
   */
  register(renderer: Renderer): void {
    if (this.renderers.some((r) => r.kind === renderer.kind)) {
      throw new RenderError(
        renderer.kind,
        "EINVAL",
        `duplicate renderer kind '${renderer.kind}' in registry`
      );
    }
    this.renderers.push(renderer);
  }

  /** First renderer whose `canRender(mime)` returns true. */
  dispatchByMime(mimeType: string): Renderer {
    for (const r of this.renderers) {
      if (r.canRender(mimeType)) return r;
    }
    // Unreachable in practice because the icon-card fallback's
    // `canRender` is unconditionally true. Surface as an internal
    // error so a missing-fallback misconfiguration is loud.
    throw new RenderError(
      "registry",
      "EINTERNAL",
      `no renderer matched mime '${mimeType}' (fallback missing)`
    );
  }

  /** Renderer with the exact `kind`, or `null` if absent. */
  dispatchByKind(kind: string): Renderer | null {
    return this.renderers.find((r) => r.kind === kind) ?? null;
  }

  /** Read-only snapshot of registered kinds, in registration order. */
  list(): readonly string[] {
    return this.renderers.map((r) => r.kind);
  }
}
