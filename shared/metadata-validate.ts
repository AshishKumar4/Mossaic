/**
 * pure validators for metadata + tags.
 *
 * Both functions throw `VFSError("EINVAL", message)` on violation —
 * wired such that the SDK can pre-validate without a round-trip,
 * AND the worker re-validates so a malicious client cannot bypass.
 *
 * No I/O, no env access. Runs in both the SDK consumer's Worker
 * isolate and the per-tenant UserDO isolate.
 */

import { VFSError } from "./vfs-types";
import {
  METADATA_KEY_MAX_LEN,
  METADATA_MAX_ARRAY_LEN,
  METADATA_MAX_BYTES,
  METADATA_MAX_DEPTH,
  TAG_MAX_LEN,
  TAG_REGEX,
  TAGS_MAX_PER_FILE,
  VERSION_LABEL_MAX_LEN,
} from "./metadata-caps";

/**
 * Validate a metadata object. Throws on:
 *   - non-plain-object root (arrays, primitives, null are NOT valid
 *     at the root — the per-file metadata is conceptually a JSON
 *     object; nested arrays/primitives are fine).
 *   - JSON-encoded byte length > METADATA_MAX_BYTES.
 *   - any key length > METADATA_KEY_MAX_LEN.
 *   - object/array nesting depth > METADATA_MAX_DEPTH.
 *   - any array length > METADATA_MAX_ARRAY_LEN.
 *   - any non-finite number (NaN/Infinity — JSON.stringify maps to
 *     "null" silently, which would corrupt round-trips).
 *   - any function/symbol/undefined leaf (JSON-incompatible).
 *
 * Returns the JSON-encoded byte length (callers store via
 * `new TextEncoder().encode(JSON.stringify(meta))` so they can
 * pass the same blob to SQL without re-encoding).
 */
export function validateMetadata(
  metadata: unknown
): { encoded: Uint8Array } {
  if (
    metadata === null ||
    typeof metadata !== "object" ||
    Array.isArray(metadata)
  ) {
    throw new VFSError(
      "EINVAL",
      "metadata: root must be a plain object (use null to clear)"
    );
  }
  validateMetadataNode(metadata, 0);
  let encoded: Uint8Array;
  try {
    encoded = new TextEncoder().encode(JSON.stringify(metadata));
  } catch (err) {
    throw new VFSError(
      "EINVAL",
      `metadata: JSON.stringify failed: ${
        err instanceof Error ? err.message : String(err)
      }`
    );
  }
  if (encoded.byteLength > METADATA_MAX_BYTES) {
    throw new VFSError(
      "EINVAL",
      `metadata: ${encoded.byteLength} bytes > METADATA_MAX_BYTES (${METADATA_MAX_BYTES})`
    );
  }
  return { encoded };
}

function validateMetadataNode(node: unknown, depth: number): void {
  if (depth > METADATA_MAX_DEPTH) {
    throw new VFSError(
      "EINVAL",
      `metadata: nesting depth > ${METADATA_MAX_DEPTH}`
    );
  }
  if (node === null) return;
  const t = typeof node;
  if (t === "string" || t === "boolean") return;
  if (t === "number") {
    if (!Number.isFinite(node as number)) {
      throw new VFSError(
        "EINVAL",
        "metadata: numeric leaves must be finite (no NaN/Infinity)"
      );
    }
    return;
  }
  if (t === "function" || t === "symbol" || t === "undefined" || t === "bigint") {
    throw new VFSError(
      "EINVAL",
      `metadata: unsupported leaf type ${t}`
    );
  }
  if (Array.isArray(node)) {
    if (node.length > METADATA_MAX_ARRAY_LEN) {
      throw new VFSError(
        "EINVAL",
        `metadata: array length ${node.length} > ${METADATA_MAX_ARRAY_LEN}`
      );
    }
    for (const child of node) {
      validateMetadataNode(child, depth + 1);
    }
    return;
  }
  if (t === "object") {
    for (const key of Object.keys(node as Record<string, unknown>)) {
      if (key.length > METADATA_KEY_MAX_LEN) {
        throw new VFSError(
          "EINVAL",
          `metadata: key length ${key.length} > ${METADATA_KEY_MAX_LEN}`
        );
      }
      validateMetadataNode(
        (node as Record<string, unknown>)[key],
        depth + 1
      );
    }
    return;
  }
  // Defensive — shouldn't hit.
  throw new VFSError(
    "EINVAL",
    `metadata: unsupported leaf shape ${String(node)}`
  );
}

/**
 * Validate a list of tags. Throws on:
 *   - >TAGS_MAX_PER_FILE entries.
 *   - duplicates in the list.
 *   - any tag failing TAG_REGEX (charset + length).
 */
export function validateTags(tags: readonly string[]): void {
  if (tags.length > TAGS_MAX_PER_FILE) {
    throw new VFSError(
      "EINVAL",
      `tags: ${tags.length} > TAGS_MAX_PER_FILE (${TAGS_MAX_PER_FILE})`
    );
  }
  const seen = new Set<string>();
  for (const t of tags) {
    if (typeof t !== "string") {
      throw new VFSError("EINVAL", "tags: every entry must be a string");
    }
    if (t.length === 0 || t.length > TAG_MAX_LEN) {
      throw new VFSError(
        "EINVAL",
        `tags: length must be 1..${TAG_MAX_LEN}, got ${t.length}`
      );
    }
    if (!TAG_REGEX.test(t)) {
      throw new VFSError(
        "EINVAL",
        `tags: invalid charset in ${JSON.stringify(t)} (allowed: A-Z a-z 0-9 . _ : / -)`
      );
    }
    if (seen.has(t)) {
      throw new VFSError("EINVAL", `tags: duplicate tag ${JSON.stringify(t)}`);
    }
    seen.add(t);
  }
}

/** Validate a version label string. */
export function validateLabel(label: string): void {
  if (typeof label !== "string") {
    throw new VFSError("EINVAL", "version.label: must be a string");
  }
  if (label.length > VERSION_LABEL_MAX_LEN) {
    throw new VFSError(
      "EINVAL",
      `version.label: length ${label.length} > ${VERSION_LABEL_MAX_LEN}`
    );
  }
}
