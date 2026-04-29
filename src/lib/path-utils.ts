/**
 * Phase 17.6 — SPA path/parentId reconciliation helpers.
 *
 * `parallelUpload(client, path, source)` takes a `path`, but the
 * legacy App's UI is `parentId`-based — the photo-library models
 * folders by `parent_id` references and surfaces them via breadcrumb
 * UI. The new App-pinned multipart route accepts `path` as the
 * filename (it's an opaque label for the legacy `files.file_name`
 * column) and `parentId` via the `metadata.parentId` field on the
 * begin request.
 *
 * **Translation rules.**
 *  - For uploads: `path` = bare fileName; `parentId` is passed via
 *    `opts.metadata.parentId`. The App's `appBeginMultipart`
 *    extracts `parentId` and uses it for the legacy `files`
 *    INSERT.
 *  - For downloads: the SPA's `useDownload` already addresses files
 *    by `fileId` (legacy convention). The App's
 *    `/api/upload/multipart/download-token` route accepts a
 *    `fileId` in the `path` field (with optional leading `/`
 *    tolerated). `pathFromFileId(fileId) = fileId`.
 *
 * **Why the asymmetry?** The legacy schema doesn't have a UNIQUE
 * `(parent, name)` index — files are addressed by `fileId`. The
 * `path` column on the canonical multipart wire shape exists for
 * the canonical SDK path-resolution flow which the legacy App
 * doesn't use. We pass-through to satisfy the wire shape without
 * changing the legacy semantics.
 */

/**
 * Build the multipart upload `path` for a file in the legacy App.
 *
 * For the App's photo-library, the path is just the filename — the
 * folder hierarchy is encoded in the `parentId` passed via
 * `opts.metadata.parentId`. The App's `appBeginMultipart`
 * destructures both.
 *
 * @param parentId The destination folder's id, or `null` for root.
 * @param fileName The file's display name (last path segment).
 * @returns A path suitable for `parallelUpload(client, path, ...)`.
 */
export function pathFromParentId(
  parentId: string | null,
  fileName: string
): string {
  // The path is informational at the App layer — `appBeginMultipart`
  // uses parentId from metadata for legacy SQL routing. We construct
  // a `/`-prefixed path for cosmetic logging consistency with
  // canonical consumers.
  void parentId;
  return `/${fileName}`;
}

/**
 * Resolve a fileId to the multipart-route's `path` argument.
 *
 * The App-pinned multipart download-token route accepts a fileId
 * directly (it ignores leading slashes). `pathFromFileId` returns
 * the fileId unchanged so downstream callers don't have to think
 * about the `path` vs `fileId` mismatch.
 *
 * @param fileId The legacy `files.file_id` ULID-shaped string.
 * @returns A path suitable for
 *   `parallelDownload(client, path, ...)` against the App route.
 */
export function pathFromFileId(fileId: string): string {
  return fileId;
}
