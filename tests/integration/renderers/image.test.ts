import { describe, it, expect } from "vitest";
import { imageRenderer, RenderError } from "@core/lib/preview-pipeline";
import type { RenderInput } from "@shared/preview-types";
import type { ImagesBinding } from "@shared/types";

/**
 * Image renderer.
 *
 *   I1.  canRender accepts image/*.
 *   I2.  IMAGES binding absent → throws RenderError("EMOSSAIC_UNAVAILABLE").
 *   I3.  IMAGES binding present + working → returns the binding's bytes.
 *   I4.  IMAGES binding throws → renderer wraps as RenderError("EINVAL")
 *        — no silent failures.
 *
 * The miniflare runtime does NOT bind `env.IMAGES`, so I3+I4 inject a
 * stub binding shaped per the production interface. The renderer
 * doesn't care whether the binding is real or stubbed; it calls the
 * documented surface.
 */

function fakeImageInput(): RenderInput {
  return {
    bytes: new ReadableStream<Uint8Array>({
      start(c) {
        c.enqueue(new Uint8Array([0xff, 0xd8, 0xff, 0xe0]));
        c.close();
      },
    }),
    mimeType: "image/jpeg",
    fileName: "photo.jpg",
    fileSize: 4,
  };
}

function stubBinding(opts: { fail?: "transform" | "output" | null } = {}): ImagesBinding {
  return {
    input(stream) {
      // Read + discard so the stream is not leaked.
      void stream;
      return {
        transform(_t) {
          if (opts.fail === "transform") {
            throw new Error("transform-failed");
          }
          return {
            async output(o) {
              if (opts.fail === "output") {
                throw new Error("output-failed");
              }
              const payload = new Uint8Array([0xab, 0xcd, 0xef]);
              return {
                response: () => new Response(payload, {
                  headers: { "Content-Type": o.format },
                }),
                contentType: () => o.format,
              };
            },
          };
        },
      };
    },
  };
}

describe("imageRenderer", () => {
  it("I1 — canRender accepts image/*", () => {
    expect(imageRenderer.canRender("image/jpeg")).toBe(true);
    expect(imageRenderer.canRender("image/png")).toBe(true);
    expect(imageRenderer.canRender("image/heic")).toBe(true);
    expect(imageRenderer.canRender("text/plain")).toBe(false);
  });

  it("I2 — IMAGES binding absent → RenderError EMOSSAIC_UNAVAILABLE", async () => {
    const env = {} as never;
    let caught: unknown;
    try {
      await imageRenderer.render(fakeImageInput(), env, {
        variant: "thumb",
        format: "image/webp",
      });
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(RenderError);
    expect((caught as RenderError).code).toBe("EMOSSAIC_UNAVAILABLE");
  });

  it("I3 — IMAGES binding present + working → returns binding bytes", async () => {
    const env = {
      MOSSAIC_USER: {} as never,
      MOSSAIC_SHARD: {} as never,
      IMAGES: stubBinding(),
    };
    const r = await imageRenderer.render(fakeImageInput(), env, {
      variant: "thumb",
      format: "image/webp",
    });
    expect(r.mimeType).toBe("image/webp");
    expect(r.bytes).toEqual(new Uint8Array([0xab, 0xcd, 0xef]));
    expect(r.width).toBe(256);
    expect(r.height).toBe(256);
  });

  it("I4 — IMAGES binding transform throws → RenderError EINVAL (no silent fail)", async () => {
    const env = {
      MOSSAIC_USER: {} as never,
      MOSSAIC_SHARD: {} as never,
      IMAGES: stubBinding({ fail: "transform" }),
    };
    let caught: unknown;
    try {
      await imageRenderer.render(fakeImageInput(), env, {
        variant: "thumb",
        format: "image/webp",
      });
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(RenderError);
    expect((caught as RenderError).code).toBe("EINVAL");
  });

  it("I4b — IMAGES output throws → RenderError EINVAL", async () => {
    const env = {
      MOSSAIC_USER: {} as never,
      MOSSAIC_SHARD: {} as never,
      IMAGES: stubBinding({ fail: "output" }),
    };
    let caught: unknown;
    try {
      await imageRenderer.render(fakeImageInput(), env, {
        variant: "medium",
        format: "image/avif",
      });
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(RenderError);
    expect((caught as RenderError).code).toBe("EINVAL");
  });
});
