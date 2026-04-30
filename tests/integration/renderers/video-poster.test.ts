import { describe, it, expect } from "vitest";
import {
  videoPosterRenderer,
  iconCardRenderer,
} from "@core/lib/preview-pipeline";
import type { RenderInput } from "@shared/preview-types";

/**
 * Video-poster renderer (Phase 20 stub: delegates to icon-card).
 *
 *   V1.  canRender accepts video/*.
 *   V2.  Output is the same bytes as iconCardRenderer for the same
 *        input (Phase 20 stub) — confirming stable degradation. Phase
 *        20.1 swaps the body in via `env.BROWSER`; this test will
 *        change there.
 */

function fakeVideoInput(): RenderInput {
  return {
    bytes: new ReadableStream<Uint8Array>({
      start(c) {
        c.enqueue(new Uint8Array([0, 0, 0, 0x18, 0x66, 0x74, 0x79, 0x70]));
        c.close();
      },
    }),
    mimeType: "video/mp4",
    fileName: "movie.mp4",
    fileSize: 8,
  };
}

describe("videoPosterRenderer (Phase 20 stub)", () => {
  it("V1 — canRender accepts video/*", () => {
    expect(videoPosterRenderer.canRender("video/mp4")).toBe(true);
    expect(videoPosterRenderer.canRender("video/webm")).toBe(true);
    expect(videoPosterRenderer.canRender("video/quicktime")).toBe(true);
    expect(videoPosterRenderer.canRender("audio/mpeg")).toBe(false);
    expect(videoPosterRenderer.canRender("image/png")).toBe(false);
  });

  it("V2 — Phase 20 stub returns icon-card output", async () => {
    const env = {} as never;
    const fromVideo = await videoPosterRenderer.render(
      fakeVideoInput(),
      env,
      { variant: "thumb", format: "image/svg+xml" }
    );
    const fromIcon = await iconCardRenderer.render(fakeVideoInput(), env, {
      variant: "thumb",
      format: "image/svg+xml",
    });
    expect(fromVideo.bytes).toEqual(fromIcon.bytes);
    expect(fromVideo.mimeType).toBe("image/svg+xml");
  });
});
