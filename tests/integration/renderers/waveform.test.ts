import { describe, it, expect } from "vitest";
import { waveformRenderer } from "@core/lib/preview-pipeline";
import type { RenderInput } from "@shared/preview-types";

/**
 * Waveform renderer.
 *
 *   W1.  canRender accepts audio/*.
 *   W2.  Same input bytes produce byte-identical SVG (deterministic).
 *   W3.  Different input bytes produce different SVGs (peak buffer
 *        responds to input).
 */

function fakeAudioInput(bytes: Uint8Array): RenderInput {
  return {
    bytes: new ReadableStream<Uint8Array>({
      start(c) {
        c.enqueue(bytes);
        c.close();
      },
    }),
    mimeType: "audio/mpeg",
    fileName: "song.mp3",
    fileSize: bytes.byteLength,
  };
}

describe("waveformRenderer", () => {
  it("W1 — canRender accepts audio/*", () => {
    expect(waveformRenderer.canRender("audio/mpeg")).toBe(true);
    expect(waveformRenderer.canRender("audio/wav")).toBe(true);
    expect(waveformRenderer.canRender("audio/ogg")).toBe(true);
    expect(waveformRenderer.canRender("video/mp4")).toBe(false);
    expect(waveformRenderer.canRender("image/png")).toBe(false);
  });

  it("W2 — same input → byte-identical output (deterministic)", async () => {
    const env = {} as never;
    const samples = new Uint8Array(2048);
    for (let i = 0; i < samples.byteLength; i++) {
      samples[i] = (Math.sin(i / 8) * 96 + 128) & 0xff;
    }
    const a = await waveformRenderer.render(fakeAudioInput(samples), env, {
      variant: "thumb",
      format: "image/svg+xml",
    });
    const b = await waveformRenderer.render(fakeAudioInput(samples), env, {
      variant: "thumb",
      format: "image/svg+xml",
    });
    expect(a.bytes).toEqual(b.bytes);
    const text = new TextDecoder().decode(a.bytes);
    expect(text.startsWith("<svg")).toBe(true);
    // Should contain peak bars.
    expect(text).toContain('<rect');
  });

  it("W3 — different input → different SVG silhouette", async () => {
    const env = {} as never;
    const a = new Uint8Array(2048).fill(128); // flat → minimal peaks
    const b = new Uint8Array(2048);
    for (let i = 0; i < b.byteLength; i++) b[i] = (i & 1) === 0 ? 0 : 255; // square wave → big peaks
    const ra = await waveformRenderer.render(fakeAudioInput(a), {} as never, {
      variant: "thumb",
      format: "image/svg+xml",
    });
    const rb = await waveformRenderer.render(fakeAudioInput(b), {} as never, {
      variant: "thumb",
      format: "image/svg+xml",
    });
    void env;
    expect(ra.bytes).not.toEqual(rb.bytes);
  });
});
