import { describe, it, expect } from "vitest";
import { iconCardRenderer } from "@core/lib/preview-pipeline";
import type { RenderInput } from "@shared/preview-types";

/**
 * Icon-card renderer.
 *
 *   IC1.  Produces deterministic SVG bytes for the same (filename,
 *         size, variant) — load-bearing for content-addressed dedup.
 *   IC2.  Output dimensions match the requested variant.
 *   IC3.  Output is well-formed SVG (starts with <svg, ends with </svg>).
 *   IC4.  XML-escapes filenames containing reserved characters.
 *   IC5.  Drains the input stream so upstream callers don't leak
 *         the read lock.
 */

const fakeInput = (
  fileName: string,
  fileSize: number,
  bytes = new Uint8Array([1, 2, 3])
): RenderInput => ({
  bytes: new ReadableStream<Uint8Array>({
    start(c) {
      c.enqueue(bytes);
      c.close();
    },
  }),
  mimeType: "application/octet-stream",
  fileName,
  fileSize,
});

describe("iconCardRenderer", () => {
  it("IC1 — same input produces byte-identical output (deterministic)", async () => {
    const env = {} as never;
    const a = await iconCardRenderer.render(fakeInput("test.bin", 1024), env, {
      variant: "thumb",
      format: "image/svg+xml",
    });
    const b = await iconCardRenderer.render(fakeInput("test.bin", 1024), env, {
      variant: "thumb",
      format: "image/svg+xml",
    });
    expect(a.bytes).toEqual(b.bytes);
  });

  it("IC2 — output dimensions match the resolved variant", async () => {
    const env = {} as never;
    const r1 = await iconCardRenderer.render(fakeInput("x.bin", 1), env, {
      variant: "thumb",
      format: "image/svg+xml",
    });
    expect(r1.width).toBe(256);
    expect(r1.height).toBe(256);
    const r2 = await iconCardRenderer.render(fakeInput("x.bin", 1), env, {
      variant: { width: 400, height: 200 },
      format: "image/svg+xml",
    });
    expect(r2.width).toBe(400);
    expect(r2.height).toBe(200);
  });

  it("IC3 — output is well-formed SVG", async () => {
    const env = {} as never;
    const r = await iconCardRenderer.render(fakeInput("y.txt", 100), env, {
      variant: "medium",
      format: "image/svg+xml",
    });
    const text = new TextDecoder().decode(r.bytes);
    expect(text.startsWith("<svg")).toBe(true);
    expect(text.endsWith("</svg>")).toBe(true);
    expect(r.mimeType).toBe("image/svg+xml");
  });

  it("IC4 — XML-escapes reserved characters in the filename", async () => {
    const env = {} as never;
    const r = await iconCardRenderer.render(
      fakeInput('a&b"<x>.txt', 0),
      env,
      { variant: "thumb", format: "image/svg+xml" }
    );
    const text = new TextDecoder().decode(r.bytes);
    expect(text).not.toContain("<x>");
    expect(text).toContain("&lt;x&gt;");
    expect(text).toContain("&quot;");
    expect(text).toContain("&amp;");
  });

  it("IC5 — drains the input stream (no leaked read lock on subsequent reader.read)", async () => {
    const env = {} as never;
    const stream = new ReadableStream<Uint8Array>({
      start(c) {
        c.enqueue(new Uint8Array([1]));
        c.enqueue(new Uint8Array([2]));
        c.close();
      },
    });
    await iconCardRenderer.render(
      {
        bytes: stream,
        mimeType: "application/octet-stream",
        fileName: "drain.bin",
        fileSize: 2,
      },
      env,
      { variant: "thumb", format: "image/svg+xml" }
    );
    // After render, the stream's read lock must be released — locked
    // === true would prevent any subsequent `getReader()`.
    expect(stream.locked).toBe(false);
  });
});
