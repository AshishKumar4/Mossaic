/**
 * HTTP Range request parsing + 206 Partial Content response shaping.
 *
 * Browsers issue Range requests for `<video>` / `<audio>` seek/scrub
 * — without 206 support, every seek refetches the whole file (Chrome
 * stalls; Safari refuses to play). Spec: RFC 7233.
 *
 * Scope: single-range only (`bytes=START-END`, `bytes=-SUFFIX`,
 * `bytes=START-`). Multi-range (`bytes=0-100,200-300`) is RFC §4.1
 * but rarely used by browsers and would force multipart/byteranges
 * encoding — out of scope; we surface 416 for those.
 *
 * Wire shape contract:
 *  - `parseRange(header, totalSize)` returns `{ start, end }`
 *    (inclusive) on success, `"unsatisfiable"` for 416, or `null`
 *    when the header is absent / malformed (caller serves 200 with
 *    full body).
 *  - `rangeResponse(bytes, range, totalSize, headers)` builds the
 *    206 response with `Content-Range`, `Content-Length`, and
 *    `Accept-Ranges: bytes`.
 *  - `serveBytesWithRange(bytes, rangeHeader, headers)` is the
 *    one-stop wrapper: 200 + `Accept-Ranges` when no Range, 206
 *    when Range parses, 416 when unsatisfiable. Use this from
 *    routes that build their own bytes; use `parseRange` +
 *    `rangeResponse` when the caller needs finer control (e.g.
 *    sliced view of a cached response body).
 *
 * Cache semantics: a 206 response carries the same cache key as
 * the underlying 200 (the byte range is part of the request, not
 * the resource). Workers Cache + most CDNs handle this correctly
 * via `Vary: Range` — emit it on every Range-aware response.
 */

export interface ByteRange {
  /** Inclusive start byte offset. */
  start: number;
  /** Inclusive end byte offset. */
  end: number;
}

export type ParsedRange = ByteRange | "unsatisfiable" | null;

/**
 * Parse an HTTP `Range` header against a known resource size.
 *
 * Supported forms (single-range only):
 *   - `bytes=START-END`   — explicit window
 *   - `bytes=START-`      — open-ended, served as `[START, totalSize-1]`
 *   - `bytes=-SUFFIX`     — last `SUFFIX` bytes
 *
 * Returns:
 *   - `null` when the header is absent, malformed, or names a
 *     unit other than `bytes` (caller falls through to 200).
 *   - `"unsatisfiable"` when the parsed window starts at or
 *     beyond `totalSize`. Caller should respond 416 with
 *     `Content-Range: bytes <asterisk>/<totalSize>`.
 *   - `{start, end}` (inclusive) on success.
 *
 * `start === end` is legal (1-byte read). `end` is clamped to
 * `totalSize - 1` for open-ended forms.
 */
export function parseRange(
  header: string | null | undefined,
  totalSize: number
): ParsedRange {
  if (!header || typeof header !== "string") return null;
  const trimmed = header.trim();
  // Must start with "bytes=".
  if (!trimmed.toLowerCase().startsWith("bytes=")) return null;
  const spec = trimmed.slice(6).trim();
  // Multi-range requests contain a comma. Surface as null so the
  // caller serves 200 — strictly compliant alternative would be
  // 416 but we choose graceful degradation.
  if (spec.includes(",")) return null;
  // 0-byte resource: any range is unsatisfiable.
  if (totalSize === 0) return "unsatisfiable";

  const dash = spec.indexOf("-");
  if (dash < 0) return null;
  const startStr = spec.slice(0, dash).trim();
  const endStr = spec.slice(dash + 1).trim();

  // Strict-digit guards: parseInt is lenient and accepts trailing
  // junk (e.g. parseInt("10-100") === 10). Reject anything that
  // isn't a pure non-negative decimal so malformed inputs like
  // `bytes=-10-100` fall through to null.
  const isDigits = (s: string): boolean => /^[0-9]+$/.test(s);

  // Suffix form: `bytes=-N` → last N bytes.
  if (startStr === "" && endStr !== "") {
    if (!isDigits(endStr)) return null;
    const suffix = Number.parseInt(endStr, 10);
    if (!Number.isInteger(suffix) || suffix <= 0) return null;
    const start = Math.max(0, totalSize - suffix);
    return { start, end: totalSize - 1 };
  }

  // Open-ended form: `bytes=N-` → [N, totalSize-1].
  if (startStr !== "" && endStr === "") {
    if (!isDigits(startStr)) return null;
    const start = Number.parseInt(startStr, 10);
    if (!Number.isInteger(start) || start < 0) return null;
    if (start >= totalSize) return "unsatisfiable";
    return { start, end: totalSize - 1 };
  }

  // Explicit form: `bytes=START-END`.
  if (startStr !== "" && endStr !== "") {
    if (!isDigits(startStr) || !isDigits(endStr)) return null;
    const start = Number.parseInt(startStr, 10);
    const end = Number.parseInt(endStr, 10);
    if (!Number.isInteger(start) || start < 0) return null;
    if (!Number.isInteger(end) || end < start) return null;
    if (start >= totalSize) return "unsatisfiable";
    // Clamp end to last byte.
    return { start, end: Math.min(end, totalSize - 1) };
  }

  return null;
}

/**
 * Slice a Uint8Array to a parsed range and return a 206 Response.
 *
 * `headers` is an optional object whose entries are merged into
 * the response headers. The function always sets:
 *   - `Content-Range: bytes <start>-<end>/<totalSize>`
 *   - `Content-Length: <end - start + 1>`
 *   - `Accept-Ranges: bytes`
 *   - `Vary: Range` (appended if `headers` already has a `Vary`)
 *
 * `Content-Type` is NOT set — caller must supply it via `headers`.
 */
export function rangeResponse(
  fullBytes: Uint8Array,
  range: ByteRange,
  totalSize: number,
  headers: Record<string, string> = {}
): Response {
  // sliceBytes is `[start, end+1)` — Uint8Array.subarray is half-open.
  const slice = fullBytes.subarray(range.start, range.end + 1);
  const out = new Headers(headers);
  out.set(
    "Content-Range",
    `bytes ${range.start}-${range.end}/${totalSize}`
  );
  out.set("Content-Length", String(slice.byteLength));
  out.set("Accept-Ranges", "bytes");
  // Vary on Range so caches key entries by the byte window. If a
  // caller already set Vary (e.g. for Authorization), append.
  const existingVary = out.get("Vary");
  if (existingVary && existingVary.length > 0) {
    if (!/(^|,\s*)Range(\s*,|$)/i.test(existingVary)) {
      out.set("Vary", `${existingVary}, Range`);
    }
  } else {
    out.set("Vary", "Range");
  }
  return new Response(slice, { status: 206, headers: out });
}

/**
 * 416 Range Not Satisfiable response. Standard form is
 * `Content-Range: bytes <asterisk>/<totalSize>` so the client
 * knows the resource size and can re-issue with a valid range.
 */
export function rangeNotSatisfiableResponse(
  totalSize: number,
  contentType?: string
): Response {
  const headers: Record<string, string> = {
    "Content-Range": `bytes */${totalSize}`,
    "Accept-Ranges": "bytes",
  };
  if (contentType) headers["Content-Type"] = contentType;
  return new Response(null, { status: 416, headers });
}

/**
 * One-stop wrapper for "serve these bytes, honouring Range".
 *
 * Usage:
 *   const bytes = await assemble();
 *   return serveBytesWithRange(bytes, c.req.header("Range"), {
 *     "Content-Type": "video/mp4",
 *     "Cache-Control": "private, max-age=3600",
 *   });
 *
 * Returns:
 *   - 200 with full body + `Accept-Ranges: bytes` when no Range.
 *   - 206 with sliced body when Range parses successfully.
 *   - 416 when Range is unsatisfiable.
 */
export function serveBytesWithRange(
  fullBytes: Uint8Array,
  rangeHeader: string | null | undefined,
  headers: Record<string, string> = {}
): Response {
  const total = fullBytes.byteLength;
  const parsed = parseRange(rangeHeader, total);
  if (parsed === "unsatisfiable") {
    return rangeNotSatisfiableResponse(total, headers["Content-Type"]);
  }
  if (parsed === null) {
    // No range / malformed — full body with Accept-Ranges so the
    // client knows it CAN seek on a subsequent request.
    const out = new Headers(headers);
    out.set("Content-Length", String(total));
    out.set("Accept-Ranges", "bytes");
    return new Response(fullBytes, { status: 200, headers: out });
  }
  return rangeResponse(fullBytes, parsed, total, headers);
}
