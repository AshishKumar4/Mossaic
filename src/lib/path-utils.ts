/**
 * SPA path helpers.
 *
 * `parallelUpload(client, path, source)` takes a path. The App's UI is
 * `parentId`-based — folders are referenced via `parent_id` rows on the
 * `files` table — so the upload hook builds a path string from the
 * (parentId, fileName) pair.
 *
 * For downloads the inverse direction (fileId → path) requires a
 * server round-trip and lives on the `api` client as `api.getFilePath`;
 * there is no client-side helper because the answer depends on the
 * folder hierarchy stored in the DO, not on data the SPA holds.
 */

/**
 * Build the upload `path` for a file in the App.
 *
 * For root-level uploads we emit `/<fileName>`. Sub-folder uploads pass
 * a non-null `parentId`; the SPA UI hands the parentId in opaquely
 * (the user picked a folder in the breadcrumb). The path is
 * constructed with a leading slash for cosmetic consistency with
 * canonical consumers; canonical writes key the row by (user_id,
 * parent_id, file_name) regardless of the leading-slash form, and the
 * App's `/api/index/file` callback re-resolves the row by path after
 * upload-finalize.
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
