export const SAFE_INLINE_IMAGE_MIME =
  /^image\/(?:jpeg|png|gif|webp|avif|bmp|heic|heif|tiff|svg\+xml)$/i;

export const SANDBOX_CSP = "sandbox; default-src 'none'";

export function buildImageResponseHeaders(
  sourceMimeType: string,
  cacheControl: string,
  contentLength?: number
): Record<string, string> {
  const allowInline = SAFE_INLINE_IMAGE_MIME.test(sourceMimeType);
  const headers: Record<string, string> = {
    "Content-Type": allowInline
      ? sourceMimeType
      : "application/octet-stream",
    "Cache-Control": cacheControl,
    "X-Content-Type-Options": "nosniff",
    "Content-Security-Policy": SANDBOX_CSP,
    // Range support is load-bearing for browser media seek/scrub
    // flows on gallery/shared endpoints.
    "Accept-Ranges": "bytes",
  };
  if (!allowInline) {
    headers["Content-Disposition"] = "attachment";
  }
  if (contentLength !== undefined) {
    headers["Content-Length"] = String(contentLength);
  }
  return headers;
}
