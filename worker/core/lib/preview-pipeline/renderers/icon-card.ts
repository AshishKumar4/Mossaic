/**
 * Icon-card renderer — universal fallback. Produces a deterministic
 * SVG containing the file's extension badge, name (truncated), and
 * size label. Pure compute; no binding required.
 *
 * Always-fallback semantics: `canRender(mime)` is unconditionally
 * `true`. The registry registers this renderer LAST so specialised
 * renderers win for MIMEs they accept.
 *
 * Determinism: output bytes are a deterministic function of
 * `(fileName, fileSize, variant)`. The renderer ignores `input.bytes`
 * — it doesn't read the file body — so encrypted files can also
 * receive icon-card previews without violating the encryption
 * boundary (they don't reveal plaintext).
 */

import type { Renderer } from "../types";
import type {
  RenderInput,
  RenderOpts,
  RenderResult,
  StandardVariant,
} from "../../../../../shared/preview-types";
import { STANDARD_VARIANT_DIMS } from "../../../../../shared/preview-types";

/**
 * Resolve a variant to concrete pixel dimensions. Standard variants
 * use the shared dimensions table; custom-dimension variants pass
 * through. Defaults to thumb dimensions for safety.
 */
function resolveDims(opts: RenderOpts): {
  width: number;
  height: number;
} {
  if (typeof opts.variant === "string") {
    const std = STANDARD_VARIANT_DIMS[opts.variant as StandardVariant];
    return { width: std.width, height: std.height };
  }
  return {
    width: opts.variant.width,
    height: opts.variant.height ?? opts.variant.width,
  };
}

/** Format a byte count as a human label ("1.2 MB"). Stable; no locale. */
function formatSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 * 1024 * 1024)
    return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(1)} GB`;
}

/** Extract the extension from a filename (uppercase, max 6 chars). */
function extractExtension(fileName: string): string {
  const dot = fileName.lastIndexOf(".");
  if (dot < 0 || dot === fileName.length - 1) return "FILE";
  return fileName.slice(dot + 1).toUpperCase().slice(0, 6);
}

/** XML-escape a string for safe inclusion in SVG text nodes. */
function svgEscape(s: string): string {
  return s
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&apos;");
}

/** Truncate a filename to fit within the card's text region. */
function truncateName(name: string, maxChars: number): string {
  if (name.length <= maxChars) return name;
  return name.slice(0, maxChars - 1) + "…";
}

export const iconCardRenderer: Renderer = {
  kind: "icon-card",

  canRender() {
    return true;
  },

  async render(
    input: RenderInput,
    _env,
    opts: RenderOpts
  ): Promise<RenderResult> {
    // Drain the input stream so the upstream caller's stream is
    // released cleanly. We don't use the bytes — icon-card is
    // metadata-only — but leaving the stream un-consumed leaks the
    // underlying read lock in workerd.
    const reader = input.bytes.getReader();
    try {
      while (true) {
        const r = await reader.read();
        if (r.done) break;
      }
    } finally {
      reader.releaseLock();
    }

    const { width, height } = resolveDims(opts);
    const ext = extractExtension(input.fileName);
    const sizeLabel = formatSize(input.fileSize);
    // Tune name max-chars to width: ~16 chars per 256 px, clamped to
    // [8, 40] so very small / very large icons stay legible.
    const maxNameChars = Math.max(8, Math.min(40, Math.floor(width / 16)));
    const displayName = svgEscape(truncateName(input.fileName, maxNameChars));
    const displayExt = svgEscape(ext);
    const displaySize = svgEscape(sizeLabel);

    // Hand-built SVG. Deterministic per (width, height, fileName,
    // fileSize) — same inputs ⇒ byte-identical output.
    const svg = [
      `<svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}" viewBox="0 0 ${width} ${height}">`,
      `<rect width="${width}" height="${height}" fill="#1f2937"/>`,
      `<rect x="${width * 0.2}" y="${height * 0.15}" width="${width * 0.6}" height="${height * 0.55}" fill="#374151" rx="${Math.max(4, width * 0.02)}"/>`,
      `<text x="${width / 2}" y="${height * 0.45}" font-family="ui-monospace,Menlo,monospace" font-size="${Math.max(12, Math.floor(width * 0.12))}" fill="#9ca3af" text-anchor="middle" font-weight="700">${displayExt}</text>`,
      `<text x="${width / 2}" y="${height * 0.83}" font-family="ui-sans-serif,system-ui,sans-serif" font-size="${Math.max(10, Math.floor(width * 0.06))}" fill="#e5e7eb" text-anchor="middle">${displayName}</text>`,
      `<text x="${width / 2}" y="${height * 0.93}" font-family="ui-sans-serif,system-ui,sans-serif" font-size="${Math.max(8, Math.floor(width * 0.045))}" fill="#9ca3af" text-anchor="middle">${displaySize}</text>`,
      `</svg>`,
    ].join("");

    return {
      bytes: new TextEncoder().encode(svg),
      mimeType: "image/svg+xml",
      width,
      height,
    };
  },
};
