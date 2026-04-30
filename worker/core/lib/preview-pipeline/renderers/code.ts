/**
 * Code renderer — renders the first ~1 KB of a `text/*` (or
 * common-source-code-mime) input as a deterministic SVG with
 * monospace text and lightweight syntax cues (comments / strings /
 * keywords colour-coded by simple regex). Pure compute, no binding.
 *
 * Determinism: output is a deterministic function of
 * `(input prefix bytes, fileName extension, variant dims)`.
 */

import type { Renderer } from "../types";
import type {
  RenderInput,
  RenderOpts,
  RenderResult,
  StandardVariant,
} from "../../../../../shared/preview-types";
import { STANDARD_VARIANT_DIMS } from "../../../../../shared/preview-types";

/** MIMEs the renderer accepts (text/* PLUS common code MIMEs). */
const TEXT_MIME_PREFIXES = ["text/", "application/json", "application/xml"];
const CODE_MIME_EXACTS = new Set([
  "application/javascript",
  "application/typescript",
  "application/x-yaml",
  "application/x-sh",
  "application/x-toml",
]);

/** Simple keyword set; intentionally small and language-agnostic. */
const KEYWORDS = new Set([
  "function", "return", "const", "let", "var", "if", "else", "for", "while",
  "import", "export", "from", "default", "class", "interface", "type",
  "extends", "implements", "public", "private", "protected", "static",
  "async", "await", "try", "catch", "finally", "throw", "new", "this",
  "true", "false", "null", "undefined",
  "def", "fn", "func", "fun", "package", "namespace", "module", "use",
]);

const MAX_BYTES = 1024;
const MAX_LINES = 28;
const LINE_HEIGHT_PX = 14;

function svgEscape(s: string): string {
  return s
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&apos;");
}

/**
 * Tokenise a single line into runs by category. Order-of-application
 * matters: comment > string > keyword > identifier.
 */
function tokenise(line: string): string {
  // Single-line comment (// or # — covers JS, TS, Python, shell, YAML).
  const commentMatch = /(\/\/.*|#.*)/.exec(line);
  if (commentMatch) {
    const before = line.slice(0, commentMatch.index);
    const comment = commentMatch[0]!;
    return tokenise(before) + `<tspan fill="#6b7280">${svgEscape(comment)}</tspan>`;
  }
  // String literals (single OR double quoted, no escape handling — best-effort).
  const stringMatch = /(["'`])((?:(?!\1).)*)\1/.exec(line);
  if (stringMatch) {
    const before = line.slice(0, stringMatch.index);
    const str = stringMatch[0]!;
    const after = line.slice(stringMatch.index + str.length);
    return (
      tokenise(before) +
      `<tspan fill="#86efac">${svgEscape(str)}</tspan>` +
      tokenise(after)
    );
  }
  // Keyword highlight — split on word boundaries, colour matches.
  const out: string[] = [];
  const re = /[A-Za-z_$][A-Za-z0-9_$]*|[^A-Za-z_$]+/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(line)) !== null) {
    const tok = m[0]!;
    if (KEYWORDS.has(tok)) {
      out.push(`<tspan fill="#a78bfa">${svgEscape(tok)}</tspan>`);
    } else {
      out.push(svgEscape(tok));
    }
  }
  return out.join("");
}

function resolveDims(opts: RenderOpts): { width: number; height: number } {
  if (typeof opts.variant === "string") {
    const std = STANDARD_VARIANT_DIMS[opts.variant as StandardVariant];
    return { width: std.width, height: std.height };
  }
  return {
    width: opts.variant.width,
    height: opts.variant.height ?? opts.variant.width,
  };
}

export const codeRenderer: Renderer = {
  kind: "code-svg",

  canRender(mimeType) {
    if (CODE_MIME_EXACTS.has(mimeType)) return true;
    return TEXT_MIME_PREFIXES.some((p) => mimeType.startsWith(p));
  },

  async render(
    input: RenderInput,
    _env,
    opts: RenderOpts
  ): Promise<RenderResult> {
    // Read up to MAX_BYTES from the stream. We deliberately do NOT
    // drain the rest — the upstream caller's stream cancel handler
    // releases the read lock when our reader is dropped.
    const reader = input.bytes.getReader();
    const parts: Uint8Array[] = [];
    let total = 0;
    try {
      while (total < MAX_BYTES) {
        const r = await reader.read();
        if (r.done) break;
        parts.push(r.value);
        total += r.value.byteLength;
      }
    } finally {
      reader.releaseLock();
    }
    let merged = new Uint8Array(total);
    let offset = 0;
    for (const p of parts) {
      merged.set(p, offset);
      offset += p.byteLength;
    }
    if (merged.byteLength > MAX_BYTES) {
      merged = merged.subarray(0, MAX_BYTES);
    }
    const text = new TextDecoder("utf-8", { fatal: false, ignoreBOM: false }).decode(merged);
    const lines = text.split(/\r?\n/).slice(0, MAX_LINES);

    const { width, height } = resolveDims(opts);
    const fontSize = Math.max(9, Math.min(13, Math.floor(width / 60)));
    const padX = 12;
    const padY = 14;
    const lineHeight = LINE_HEIGHT_PX;

    const tspans = lines.map((line, idx) => {
      const y = padY + (idx + 1) * lineHeight;
      return `<text x="${padX}" y="${y}" font-family="ui-monospace,Menlo,monospace" font-size="${fontSize}" fill="#e5e7eb" xml:space="preserve">${tokenise(line)}</text>`;
    });

    const svg = [
      `<svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}" viewBox="0 0 ${width} ${height}">`,
      `<rect width="${width}" height="${height}" fill="#0b1020"/>`,
      ...tspans,
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
