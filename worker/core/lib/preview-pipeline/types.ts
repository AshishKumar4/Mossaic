/**
 * Renderer typeclass — server-side. Concrete implementations live in
 * `./renderers/*.ts`. The `Renderer` contract is intentionally minimal:
 * each renderer self-describes the MIME types it handles and produces
 * `RenderResult` bytes for a given `(input, opts)`.
 *
 * Determinism: every renderer MUST be content-deterministic — the same
 * input bytes + opts produce byte-identical output (modulo opaque
 * timestamp metadata stripped by the binding). This is the load-bearing
 * property for content-addressed dedup; the registry hashes the result
 * and the SDK's variant-table reads rely on stable content hashes.
 */

import type { EnvCore } from "../../../../shared/types";
import type {
  RenderInput,
  RenderOpts,
  RenderResult,
} from "../../../../shared/preview-types";

export interface Renderer {
  /**
   * Stable identifier — also written into `file_variants.renderer_kind`.
   * Examples: `"image-resize"`, `"code-svg"`, `"waveform-svg"`,
   * `"video-poster"`, `"icon-card"`. Required globally unique across
   * the registry.
   */
  readonly kind: string;

  /**
   * Self-dispatch predicate. The registry walks renderers in
   * registration order; the first whose `canRender(mime)` returns
   * `true` is selected.
   */
  canRender(mimeType: string): boolean;

  /**
   * Produce preview bytes. Renderers throw `RenderError` on
   * structural failure (corrupt input, unsupported sub-format).
   * Renderers MUST NOT swallow upstream errors silently.
   */
  render(
    input: RenderInput,
    env: EnvCore,
    opts: RenderOpts
  ): Promise<RenderResult>;
}

/**
 * Typed error class thrown by renderers. The route layer maps these
 * to HTTP status codes; the registry preserves the renderer-kind
 * context so observability is preserved.
 */
export class RenderError extends Error {
  constructor(
    public readonly rendererKind: string,
    public readonly code:
      | "EINVAL"
      | "ENOTSUP"
      | "EINTERNAL"
      | "EMOSSAIC_UNAVAILABLE",
    message: string
  ) {
    super(`${code}: [${rendererKind}] ${message}`);
    this.name = "RenderError";
  }
}
