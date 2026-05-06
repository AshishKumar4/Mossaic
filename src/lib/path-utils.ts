/**
 * SPA path/parentId reconciliation helpers.
 *
 * `parallelUpload(client, path, source)` and `parallelDownload(client,
 * path)` take a path. The App's UI is `parentId`-based — folders are
 * referenced via `parent_id` rows on the legacy `files` table. These
 * helpers reconcile the two address spaces.
 *
 * The path emitted is informational at the App layer for now; the
 * canonical pipeline ignores `parentId` and addresses files by full
 * path. The App's `/api/index/file` callback resolves the path back to
 * a `files.file_id` after upload-finalize.
 */

/**
 * Build the upload `path` for a file in the App.
 *
 * For root-level uploads we emit `/<fileName>`. Sub-folder uploads pass
 * a non-null `parentId`; the SPA UI hands the parentId in opaquely
 * (the user picked a folder in the breadcrumb). The path is constructed
 * with a leading slash for cosmetic consistency with canonical
 * consumers; canonical writes will key the row by (user_id, parent_id,
 * file_name) regardless.
 *
 * @param parentId The destination folder's id, or `null` for root.
 * @param fileName The file's display name (last path segment).
 */
export function pathFromParentId(
  parentId: string | null,
  fileName: string
): string {
  void parentId;
  return `/${fileName}`;
}

/**
 * Resolve a fileId to the download `path` argument.
 *
 * The canonical multipart download-token route accepts a fileId
 * directly (it's tolerant of leading slashes). `pathFromFileId`
 * returns the fileId unchanged so downstream callers don't have to
 * think about the path-vs-fileId mismatch.
 *
 * @param fileId The `files.file_id` ULID-shaped string.
 */
export function pathFromFileId(fileId: string): string {
  return fileId;
}
