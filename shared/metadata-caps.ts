/**
 * caps for the new metadata + tags surface.
 *
 * Shared between the SDK (for client-side pre-validation) and the
 * worker (for authoritative enforcement). Both layers throw
 * `VFSError("EINVAL", ...)` (worker) or `EINVAL` (SDK) on violation.
 *
 * The values here are deliberately conservative — DO SQLite has a
 * 1 MB row limit, so 64 KB of metadata + the rest of the row stays
 * comfortably within bounds. Tag-count + tag-len caps prevent a
 * pathological tenant from filling `file_tags` with millions of
 * rows for a single file.
 */

/** Maximum byte length of the JSON-stringified metadata blob. 64 KB. */
export const METADATA_MAX_BYTES = 64 * 1024;

/** Maximum number of tags attached to a single file. */
export const TAGS_MAX_PER_FILE = 32;

/** Maximum length of a single tag, in characters. */
export const TAG_MAX_LEN = 128;

/** Allowed tag charset: alphanumerics + `._:/-`. */
export const TAG_REGEX = /^[A-Za-z0-9._:/-]{1,128}$/;

/** Maximum length of a single metadata key, in characters. */
export const METADATA_KEY_MAX_LEN = 1024;

/** Maximum nesting depth for metadata objects/arrays. */
export const METADATA_MAX_DEPTH = 10;

/** Maximum length of any array stored inside metadata. */
export const METADATA_MAX_ARRAY_LEN = 1024;

/** Maximum number of tags accepted by a single listFiles query. */
export const TAGS_MAX_PER_LIST_QUERY = 8;

/** listFiles `limit` is clamped to this maximum. */
export const LIST_LIMIT_MAX = 1000;

/** Default `limit` when listFiles is called without one. */
export const LIST_LIMIT_DEFAULT = 50;

/** Maximum length of a version label. */
export const VERSION_LABEL_MAX_LEN = 128;
