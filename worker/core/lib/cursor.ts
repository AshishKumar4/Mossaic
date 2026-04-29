/**
 * Phase 12: HMAC-signed opaque cursor codec for listFiles
 * pagination.
 *
 * Wire shape (post base64-url decode):
 *
 *   { v: 1, ob, d, ov, pid, sig }
 *
 *   v   — codec version. Bump on rotation; clients see EINVAL on
 *         mismatch and restart pagination. (No persistent cursor
 *         state on server.)
 *   ob  — orderBy: "mtime" | "name" | "size". Must match the next
 *         query's orderBy or we throw EINVAL.
 *   d   — direction: "asc" | "desc".
 *   ov  — orderbyValue (number | string) of the LAST item on the
 *         prior page. Used as the seek boundary for the next page.
 *   pid — file_id of the last item — disambiguator when multiple
 *         rows share the same orderbyValue (mtime tie-break).
 *   sig — HMAC-SHA256(secret, JSON({v, ob, d, ov, pid})), base64-url,
 *         truncated to 22 chars (~128 bits) — tamper detection.
 *
 * The DO is per-tenant, so cross-tenant cursor replay is structurally
 * impossible (each tenant's DO has its own SQLite). The HMAC guards
 * against per-tenant tampering — a client can't forge a cursor that
 * leaks rows it shouldn't see, because the cursor is just a seek
 * boundary into a query the client could have run anyway. We still
 * sign it so a) malformed payloads are rejected loudly, b) future
 * extensions (cross-DO cursors? federated listings?) work.
 */

import { VFSError } from "@shared/vfs-types";

export type OrderBy = "mtime" | "name" | "size";
export type Direction = "asc" | "desc";

/** Decoded cursor payload (without the signature). */
export interface CursorPayload {
  v: 1;
  ob: OrderBy;
  d: Direction;
  ov: number | string;
  pid: string;
}

const CURSOR_SIG_LEN = 22; // ~128 bits of base64-url

/**
 * Encode a cursor payload with an HMAC signature. Uses
 * `crypto.subtle.sign("HMAC", ...)` over the canonical JSON of the
 * payload (without the `sig` field).
 */
export async function encodeCursor(
  payload: CursorPayload,
  secret: string
): Promise<string> {
  const canon = canonical(payload);
  const sig = await hmacSha256B64Url(canon, secret);
  const wire = JSON.stringify({ ...payload, sig: sig.slice(0, CURSOR_SIG_LEN) });
  return b64UrlEncode(new TextEncoder().encode(wire));
}

/**
 * Decode and verify a cursor. Throws `VFSError("EINVAL", ...)` on
 * any of: malformed base64, malformed JSON, missing fields, version
 * mismatch, orderBy mismatch (caller-supplied), direction mismatch
 * (caller-supplied), HMAC verification failure.
 */
export async function decodeCursor(
  encoded: string,
  secret: string,
  expectedOb: OrderBy,
  expectedD: Direction
): Promise<CursorPayload> {
  let bytes: Uint8Array;
  try {
    bytes = b64UrlDecode(encoded);
  } catch {
    throw new VFSError("EINVAL", "listFiles: malformed cursor (base64)");
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(new TextDecoder().decode(bytes));
  } catch {
    throw new VFSError("EINVAL", "listFiles: malformed cursor (json)");
  }
  if (
    !parsed ||
    typeof parsed !== "object" ||
    (parsed as { v?: unknown }).v !== 1 ||
    typeof (parsed as { ob?: unknown }).ob !== "string" ||
    typeof (parsed as { d?: unknown }).d !== "string" ||
    typeof (parsed as { pid?: unknown }).pid !== "string" ||
    typeof (parsed as { sig?: unknown }).sig !== "string"
  ) {
    throw new VFSError("EINVAL", "listFiles: malformed cursor (shape)");
  }
  const p = parsed as {
    v: 1;
    ob: OrderBy;
    d: Direction;
    ov: number | string;
    pid: string;
    sig: string;
  };
  if (p.ob !== expectedOb) {
    throw new VFSError(
      "EINVAL",
      `listFiles: cursor orderBy mismatch (cursor=${p.ob}, query=${expectedOb})`
    );
  }
  if (p.d !== expectedD) {
    throw new VFSError(
      "EINVAL",
      `listFiles: cursor direction mismatch (cursor=${p.d}, query=${expectedD})`
    );
  }
  // Verify signature.
  const stripped: CursorPayload = {
    v: p.v,
    ob: p.ob,
    d: p.d,
    ov: p.ov,
    pid: p.pid,
  };
  const expectedSig = (await hmacSha256B64Url(canonical(stripped), secret)).slice(
    0,
    CURSOR_SIG_LEN
  );
  if (p.sig !== expectedSig) {
    throw new VFSError("EINVAL", "listFiles: invalid or tampered cursor");
  }
  return stripped;
}

/** Canonical JSON for HMAC input. Property order is fixed. */
function canonical(p: CursorPayload): string {
  return JSON.stringify({
    v: p.v,
    ob: p.ob,
    d: p.d,
    ov: p.ov,
    pid: p.pid,
  });
}

async function hmacSha256B64Url(
  payload: string,
  secret: string
): Promise<string> {
  const enc = new TextEncoder();
  const key = await crypto.subtle.importKey(
    "raw",
    enc.encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"]
  );
  const sigBytes = new Uint8Array(
    await crypto.subtle.sign("HMAC", key, enc.encode(payload))
  );
  return b64UrlEncode(sigBytes);
}

function b64UrlEncode(bytes: Uint8Array): string {
  let s = "";
  for (let i = 0; i < bytes.length; i++) s += String.fromCharCode(bytes[i]);
  return btoa(s).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

function b64UrlDecode(s: string): Uint8Array {
  const padded = s.replace(/-/g, "+").replace(/_/g, "/");
  // Pad to a multiple of 4.
  const padding = padded.length % 4 === 0 ? "" : "=".repeat(4 - (padded.length % 4));
  const decoded = atob(padded + padding);
  const out = new Uint8Array(decoded.length);
  for (let i = 0; i < decoded.length; i++) out[i] = decoded.charCodeAt(i);
  return out;
}
