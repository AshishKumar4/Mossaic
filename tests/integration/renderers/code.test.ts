import { describe, it, expect } from "vitest";
import { codeRenderer } from "@core/lib/preview-pipeline";
import type { RenderInput } from "@shared/preview-types";

/**
 * Code renderer.
 *
 *   C1.  canRender accepts text/* + common code MIMEs.
 *   C2.  Renders a deterministic SVG containing the input's text.
 *   C3.  Highlights keywords + strings + comments via tspan colour.
 *   C4.  Truncates input beyond 1024 bytes (memory bound).
 */

const inputFromText = (
  text: string,
  mimeType = "text/plain",
  fileName = "src.ts"
): RenderInput => ({
  bytes: new ReadableStream<Uint8Array>({
    start(c) {
      c.enqueue(new TextEncoder().encode(text));
      c.close();
    },
  }),
  mimeType,
  fileName,
  fileSize: new TextEncoder().encode(text).byteLength,
});

describe("codeRenderer", () => {
  it("C1 — canRender accepts text/* and common code MIMEs", () => {
    expect(codeRenderer.canRender("text/plain")).toBe(true);
    expect(codeRenderer.canRender("text/markdown")).toBe(true);
    expect(codeRenderer.canRender("application/json")).toBe(true);
    expect(codeRenderer.canRender("application/typescript")).toBe(true);
    expect(codeRenderer.canRender("image/jpeg")).toBe(false);
    expect(codeRenderer.canRender("application/pdf")).toBe(false);
  });

  it("C2 — output is deterministic SVG containing some of the input text", async () => {
    const env = {} as never;
    const sample = `function hello() {\n  return "world";\n}`;
    const a = await codeRenderer.render(inputFromText(sample), env, {
      variant: "thumb",
      format: "image/svg+xml",
    });
    const b = await codeRenderer.render(inputFromText(sample), env, {
      variant: "thumb",
      format: "image/svg+xml",
    });
    expect(a.bytes).toEqual(b.bytes);
    const text = new TextDecoder().decode(a.bytes);
    expect(text.startsWith("<svg")).toBe(true);
    // The file content survives into the SVG (escaped).
    expect(text).toContain("hello");
    expect(text).toContain("world");
  });

  it("C3 — highlights keywords, strings, and comments with tspan colour", async () => {
    const env = {} as never;
    const sample = `// a comment\nconst x = "hi";`;
    const r = await codeRenderer.render(inputFromText(sample), env, {
      variant: "medium",
      format: "image/svg+xml",
    });
    const text = new TextDecoder().decode(r.bytes);
    // Comment colour (#6b7280).
    expect(text).toContain('fill="#6b7280"');
    // String colour (#86efac).
    expect(text).toContain('fill="#86efac"');
    // Keyword colour (#a78bfa) for `const`.
    expect(text).toContain('fill="#a78bfa"');
  });

  it("C4 — truncates input beyond 1024 bytes (memory bound)", async () => {
    const env = {} as never;
    // Stream emits 4 KB; only first 1 KB should appear in SVG.
    const longText =
      "A".repeat(512) + "\n" + "B".repeat(512) + "\n" + "C".repeat(2048);
    const r = await codeRenderer.render(inputFromText(longText), env, {
      variant: "thumb",
      format: "image/svg+xml",
    });
    const text = new TextDecoder().decode(r.bytes);
    // The first run of As must be present...
    expect(text).toContain("AAAA");
    // ...the long C-run beyond byte 1024 must NOT appear.
    expect(text.includes("CCCCCCCCCCCC")).toBe(false);
  });
});
