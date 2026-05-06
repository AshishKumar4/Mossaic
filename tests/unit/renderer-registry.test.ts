import { describe, it, expect } from "vitest";
import {
  RendererRegistry,
  RenderError,
  imageRenderer,
  codeRenderer,
  iconCardRenderer,
  buildDefaultRegistry,
} from "@core/lib/preview-pipeline";
import type { Renderer } from "@core/lib/preview-pipeline";
import type {
  RenderInput,
  RenderOpts,
  RenderResult,
} from "@shared/preview-types";

/**
 * Registry contract.
 *
 *   R1.  dispatchByMime walks renderers in registration order and
 *        returns the first canRender match.
 *   R2.  The default registry's icon-card fallback catches MIMEs no
 *        specialised renderer accepts.
 *   R3.  Custom renderer registration order is respected (last
 *        non-fallback renderer wins for its MIME).
 *   R4.  Forced dispatchByKind returns the exact renderer or null.
 *   R5.  Duplicate kind on register throws RenderError("EINVAL").
 *   R6.  Empty registry's dispatchByMime throws (loud, not silent).
 */

const fakeStream = (): ReadableStream<Uint8Array> =>
  new ReadableStream<Uint8Array>({
    start(c) {
      c.enqueue(new Uint8Array([1, 2, 3]));
      c.close();
    },
  });

const fakeInput = (mimeType: string, fileName = "x"): RenderInput => ({
  bytes: fakeStream(),
  mimeType,
  fileName,
  fileSize: 3,
});

describe("RendererRegistry", () => {
  it("R1 — dispatchByMime matches in registration order", () => {
    const reg = new RendererRegistry();
    reg.register(imageRenderer);
    reg.register(codeRenderer);
    reg.register(iconCardRenderer);
    expect(reg.dispatchByMime("image/jpeg").kind).toBe("image-resize");
    expect(reg.dispatchByMime("text/plain").kind).toBe("code-svg");
    expect(reg.dispatchByMime("application/x-binary").kind).toBe("icon-card");
  });

  it("R2 — default registry's icon-card fallback catches any MIME", () => {
    const reg = buildDefaultRegistry();
    expect(reg.dispatchByMime("application/x-quux").kind).toBe("icon-card");
    expect(reg.dispatchByMime("audio/mpeg").kind).toBe("waveform-svg");
    expect(reg.dispatchByMime("video/mp4").kind).toBe("video-poster");
    expect(reg.dispatchByMime("image/heif").kind).toBe("image-resize");
  });

  it("R3 — custom renderer beats fallback when registered before icon-card", async () => {
    const customRenderer: Renderer = {
      kind: "custom-test",
      canRender: (m) => m === "application/x-mossaic",
      render: async (
        _input: RenderInput,
        _env,
        _opts: RenderOpts
      ): Promise<RenderResult> => ({
        bytes: new Uint8Array([0xaa]),
        mimeType: "image/png",
        width: 1,
        height: 1,
      }),
    };
    const reg = new RendererRegistry();
    reg.register(customRenderer);
    reg.register(iconCardRenderer);
    expect(reg.dispatchByMime("application/x-mossaic").kind).toBe(
      "custom-test"
    );
    // Other MIMEs still fall to icon-card.
    expect(reg.dispatchByMime("application/octet-stream").kind).toBe(
      "icon-card"
    );
    // Custom renderer is invoked successfully.
    const env = {} as never;
    const result = await customRenderer.render(
      fakeInput("application/x-mossaic"),
      env,
      { variant: "thumb", format: "image/png" }
    );
    expect(result.bytes[0]).toBe(0xaa);
  });

  it("R4 — dispatchByKind returns exact renderer or null", () => {
    const reg = buildDefaultRegistry();
    expect(reg.dispatchByKind("image-resize")?.kind).toBe("image-resize");
    expect(reg.dispatchByKind("icon-card")?.kind).toBe("icon-card");
    expect(reg.dispatchByKind("does-not-exist")).toBeNull();
  });

  it("R5 — duplicate kind on register throws RenderError EINVAL", () => {
    const reg = new RendererRegistry();
    reg.register(iconCardRenderer);
    let caught: unknown;
    try {
      reg.register(iconCardRenderer);
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(RenderError);
    expect((caught as RenderError).code).toBe("EINVAL");
  });

  it("R6 — empty registry's dispatchByMime throws RenderError EINTERNAL (loud)", () => {
    const reg = new RendererRegistry();
    let caught: unknown;
    try {
      reg.dispatchByMime("application/x-anything");
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(RenderError);
    expect((caught as RenderError).code).toBe("EINTERNAL");
  });

  it("list() returns kinds in registration order", () => {
    const reg = buildDefaultRegistry();
    expect(reg.list()).toEqual([
      "image-resize",
      "code-svg",
      "waveform-svg",
      "video-poster",
      "icon-card",
    ]);
  });
});
