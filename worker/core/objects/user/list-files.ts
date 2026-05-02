/**
 * indexed listFiles primitive.
 *
 * Index selection:
 *   - prefix only → idx_files_parent_mtime (or _name / _size).
 *   - single tag → idx_file_tags_tag_mtime.
 *   - multiple tags (AND) → k-way intersect across per-tag iterators.
 *   - prefix + tags → drive from rarest dimension; post-filter the
 *     other.
 *   - metadata filter → post-filter only (no JSON index).
 *
 * Cursor stability: opaque HMAC-signed payload carrying the last
 * page's `(orderbyValue, file_id)` boundary. Strict `>` tie-break
 * on file_id ensures deterministic page boundaries even when many
 * rows share the same orderbyValue.
 */

import type { UserDOCore as UserDO } from "./user-do-core";
import { VFSError, type VFSScope } from "../../../../shared/vfs-types";
import {
  decodeCursor,
  encodeCursor,
  type CursorPayload,
  type Direction,
  type OrderBy,
} from "../../lib/cursor";
import { getCursorSecret } from "../../lib/auth";
import {
  LIST_LIMIT_DEFAULT,
  LIST_LIMIT_MAX,
  TAGS_MAX_PER_LIST_QUERY,
} from "../../../../shared/metadata-caps";

// Stat raw shape; mirrors VFSStatRaw fields we surface.
import type { VFSStatRaw } from "../../../../shared/vfs-types";
import { gidFromTenant, inoFromId, uidFromTenant } from "../../../../shared/ino";
import {
  readFolderRevision,
  userIdFor,
  FILE_HEAD_JOIN,
} from "./vfs/helpers";

export interface ListFilesItemRaw {
  path: string;
  pathId: string;
  stat?: VFSStatRaw;
  metadata?: Record<string, unknown> | null;
  tags: string[];
  /**
   * Opt-in via `includeContentHash: true`. Hex SHA-256 of the
   * file's contents, persisted in `files.file_hash`. Always defined
   * for completed file rows; absent when the caller did not request
   * it (kept off-by-default to keep listFiles wire payloads compact
   * for typical list-and-render UIs).
   */
  contentHash?: string;
}

export interface ListFilesResult {
  items: ListFilesItemRaw[];
  cursor?: string;
}

/**
 * Discriminated-union entry returned by `vfsListChildren`.
 *
 * - `kind: "file"` carries everything `ListFilesItemRaw` does
 *   (path, pathId, optional stat / metadata / contentHash, tags).
 * - `kind: "folder"` carries path / pathId / name / optional stat;
 *   no metadata or tags surface (folders don't have either).
 * - `kind: "symlink"` carries path / pathId / name / target / optional
 *   stat; readlink-equivalent surface so consumers can decide whether
 *   to follow without a follow-up RPC.
 *
 * `name` is the leaf segment (no leading `/`), pre-computed by the
 * server so SDK consumers don't re-parse `path`.
 */
export type VFSChildRaw =
  | {
      kind: "file";
      path: string;
      pathId: string;
      name: string;
      stat?: VFSStatRaw;
      metadata?: Record<string, unknown> | null;
      tags: string[];
      contentHash?: string;
    }
  | {
      kind: "folder";
      path: string;
      pathId: string;
      name: string;
      stat?: VFSStatRaw;
    }
  | {
      kind: "symlink";
      path: string;
      pathId: string;
      name: string;
      target: string;
      stat?: VFSStatRaw;
    };

export interface ListChildrenResult {
  /**
   * Monotonically-increasing per-folder counter. Bumped by every
   * mutation that affects this folder's direct children. Consumers
   * (Seal etc.) can use this as an ETag — when revision is unchanged
   * across two reads, the directory contents are guaranteed identical.
   * Strict-monotonic guarantee inside a DO turn (UPDATE
   * `revision = revision + 1` cannot lose updates).
   */
  revision: number;
  entries: VFSChildRaw[];
  cursor?: string;
}

export interface ListChildrenOpts {
  /** Absolute path of the folder to list. `/` lists the tenant root. */
  path: string;
  orderBy?: OrderBy;
  direction?: Direction;
  limit?: number;
  cursor?: string;
  includeStat?: boolean;
  includeMetadata?: boolean;
  /** Opt-in `contentHash` on file entries. */
  includeContentHash?: boolean;
  /** See `ListFilesOpts.includeTombstones`. Default false. */
  includeTombstones?: boolean;
  /** See `ListFilesOpts.includeArchived`. Default false. */
  includeArchived?: boolean;
}

interface ListFilesOpts {
  prefix?: string;
  tags?: readonly string[];
  metadata?: Record<string, unknown>;
  limit?: number;
  cursor?: string;
  orderBy?: OrderBy;
  direction?: Direction;
  includeStat?: boolean;
  includeMetadata?: boolean;
  /**
   * Tombstone-consistency.
   *
   * Default `false`: rows whose `files.head_version_id` points at a
   * `file_versions` row with `deleted=1` are EXCLUDED from results.
   * This is the user-visible default: a path that has been
   * unlinked under versioning-on (which leaves the `files` row
   * resolvable but the head version tombstoned) must NOT appear in
   * listings; otherwise the SDK's list→stat loop blows up at
   * `helpers.ts:245` for every result.
   *
   * Set to `true` only for admin / recovery surfaces that need to
   * see tombstoned heads (e.g. `adminReapTombstonedHeads` audit).
   * The CLI / SPA / typical SDK consumer should never pass this.
   */
  includeTombstones?: boolean;
  /**
   * Archive bit.
   *
   * Default `false`: rows where `files.archived = 1` are EXCLUDED
   * from results. Archive is the third tier of the delete API
   * (alongside unlink and purge); a "Hidden" / "Trash" UI surfaces
   * by passing `includeArchived: true` and either showing only
   * those rows or the full set.
   *
   * Read surfaces (`stat`, `readFile`, etc.) are NOT gated by this
   * — archived files remain readable by anyone who knows the path.
   */
  includeArchived?: boolean;
  /**
   * Opt-in inclusion of `contentHash` (hex SHA-256) on each file
   * row. Default false to keep wire payloads compact; passive
   * listings (typical UIs) don't need it.
   */
  includeContentHash?: boolean;
}

export interface FileInfoOpts {
  includeStat?: boolean;
  includeMetadata?: boolean;
  /** See `ListFilesOpts.includeContentHash`. */
  includeContentHash?: boolean;
  /**
   * Same semantics as `ListFilesOpts.includeTombstones`.
   * Default `false`: a path resolving to a row whose head version
   * is tombstoned throws ENOENT, matching `vfsStat`. Set to `true`
   * for admin/recovery surfaces.
   */
  includeTombstones?: boolean;
  /**
   * Archive filter.
   *
   * Default `false`: a path resolving to a row with `archived = 1`
   * throws ENOENT (matches the `listFiles` exclusion). Set to
   * `true` to surface archived files in admin/recovery surfaces.
   * Read surfaces are NOT gated by this — `vfs.stat` /
   * `vfs.readFile` always succeed on archived paths.
   */
  includeArchived?: boolean;
}

const FILE_NODE_KIND = "file";

export async function vfsListFiles(
  durableObject: UserDO,
  scope: VFSScope,
  opts: ListFilesOpts = {}
): Promise<ListFilesResult> {
  const userId = userIdFor(scope);
  const orderBy: OrderBy = opts.orderBy ?? "mtime";
  const direction: Direction =
    opts.direction ?? (orderBy === "name" ? "asc" : "desc");
  const limit = clampLimit(opts.limit);
  const includeStat = opts.includeStat !== false;
  const includeMetadata = opts.includeMetadata === true;
  const includeTombstones = opts.includeTombstones === true;
  const includeArchived = opts.includeArchived === true;
  const includeContentHash = opts.includeContentHash === true;

  if (opts.tags && opts.tags.length > TAGS_MAX_PER_LIST_QUERY) {
    throw new VFSError(
      "EINVAL",
      `listFiles: too many tags (max ${TAGS_MAX_PER_LIST_QUERY})`
    );
  }

  // B-1 (final-audit): NO dev-string fallback. If `JWT_SECRET` is
  // missing/empty, refuse to sign or verify cursors. Mirrors the C1
  // fix in `worker/core/lib/auth.ts:getSecret`. A hard-coded fallback
  // in source would let any reader of this open-source repo forge
  // cursors against any deployment that forgot to run
  // `wrangler secret put JWT_SECRET`. The thrown VFSConfigError
  // surfaces at the route layer as 503 (mirrors the JWT path).
  const secret = getCursorSecret(durableObject.envPublic);
  let cursor: CursorPayload | null = null;
  if (opts.cursor) {
    cursor = await decodeCursor(opts.cursor, secret, orderBy, direction);
  }

  // Resolve prefix → parent_id.
  let parentId: string | null | undefined; // undefined = no prefix filter
  if (opts.prefix !== undefined) {
    parentId = await resolvePrefixToParentId(durableObject, userId, opts.prefix);
  }

  // Choose the driving index. We intentionally do NOT let SQLite's
  // planner choose; we want predictable plans.
  const driver = chooseDriver(opts);

  let candidates: { pathId: string; orderValue: number | string }[];
  let sqlBoundary: { pathId: string; orderValue: number | string } | null;
  let sqlPageWasFull: boolean;
  if (driver === "tags") {
    const r = listByTags(
      durableObject,
      userId,
      opts.tags!,
      parentId,
      orderBy,
      direction,
      cursor,
      limit,
      includeTombstones,
      includeArchived
    );
    candidates = r.candidates;
    sqlBoundary = r.sqlBoundary;
    sqlPageWasFull = r.sqlPageWasFull;
  } else {
    // "prefix" or "none" — both use the files index.
    const r = listByFiles(
      durableObject,
      userId,
      parentId,
      orderBy,
      direction,
      cursor,
      limit,
      opts.tags,
      includeTombstones,
      includeArchived
    );
    candidates = r.candidates;
    sqlBoundary = r.sqlBoundary;
    sqlPageWasFull = r.sqlPageWasFull;
  }

  // Post-filter: metadata exact-match.
  if (opts.metadata && Object.keys(opts.metadata).length > 0) {
    candidates = postFilterMetadata(durableObject, candidates, opts.metadata);
  }

  // Hydrate items: stat + metadata + tags + path.
  const items: ListFilesItemRaw[] = [];
  for (const c of candidates) {
    const item = hydrateItem(
      durableObject,
      userId,
      scope,
      c.pathId,
      includeStat,
      includeMetadata,
      includeTombstones,
      includeArchived,
      includeContentHash
    );
    if (item) items.push(item);
  }

  // Pagination correctness: emit cursor on SQL boundary, not items
  // survivor.
  //
  // A naive `if (items.length === limit)` cursor emission would
  // miss pages: when `metadata` post-filter (or tag intersect, or
  // archive/tombstone hydration filter) shrinks the page below
  // `limit`, no cursor would be emitted EVEN THOUGH the underlying
  // SQL returned a full `sqlLimit` rows — meaning more matches
  // lived past the boundary. Callers iterating to enumerate the
  // dataset would stop early.
  //
  // Correct: emit cursor whenever the SQL fetched a full page,
  // anchored at the LAST row that came back from SQL
  // (`sqlBoundary`), not the last surviving item. Strict-monotonic
  // boundary on (orderValue, file_id) means the next page resumes
  // past every row SQL has already considered, so no row is
  // unreachable. When the SQL page was short (everything that
  // exists has been seen), no cursor is emitted and the caller
  // knows enumeration is complete.
  let nextCursor: string | undefined;
  if (sqlPageWasFull && sqlBoundary) {
    nextCursor = await encodeCursor(
      {
        v: 1,
        ob: orderBy,
        d: direction,
        ov: sqlBoundary.orderValue,
        pid: sqlBoundary.pathId,
      },
      secret
    );
  } else if (items.length === limit && candidates.length > 0) {
    // Belt-and-braces: when the SQL didn't fill `sqlLimit` but the
    // hydrated page nonetheless reached `limit` (impossible under
    // current semantics — sqlLimit >= limit — but kept as a no-op
    // safety net for future drivers).
    const last = candidates[candidates.length - 1];
    nextCursor = await encodeCursor(
      {
        v: 1,
        ob: orderBy,
        d: direction,
        ov: last.orderValue,
        pid: last.pathId,
      },
      secret
    );
  }
  return { items, cursor: nextCursor };
}

export async function vfsFileInfo(
  durableObject: UserDO,
  scope: VFSScope,
  path: string,
  opts: FileInfoOpts = {}
): Promise<ListFilesItemRaw> {
  const userId = userIdFor(scope);
  const pathMod = await import("./path-walk");
  const resolved = pathMod.resolvePathFollow(durableObject, userId, path);
  if (resolved.kind === "ENOENT") {
    throw new VFSError("ENOENT", `fileInfo: path not found: ${path}`);
  }
  if (resolved.kind === "ENOTDIR") {
    throw new VFSError("ENOTDIR", `fileInfo: path component is not a directory: ${path}`);
  }
  if (resolved.kind === "ELOOP") {
    throw new VFSError("ELOOP", `fileInfo: too many symbolic links: ${path}`);
  }
  if (resolved.kind === "dir") {
    throw new VFSError("EISDIR", `fileInfo: path is a directory: ${path}`);
  }
  const item = hydrateItem(
    durableObject,
    userId,
    scope,
    resolved.leafId,
    opts.includeStat !== false,
    opts.includeMetadata === true,
    // fileInfo is a strict-stat surface by default: a tombstoned
    // head IS ENOENT, matching `vfsStat`/`vfsReadFile`. Admin /
    // recovery callers can opt in via `includeTombstones: true`.
    opts.includeTombstones === true,
    // Archive default is also strict: archived files are ENOENT
    // to fileInfo unless the caller opts in. Note this is
    // STRICTER than `stat` / `readFile` (which never gate on
    // archived) — fileInfo is the listing-shape surface, and a UI
    // building a "Trash" view must opt in explicitly.
    opts.includeArchived === true,
    opts.includeContentHash === true
  );
  if (!item) throw new VFSError("ENOENT", `fileInfo: path not found: ${path}`);
  return item;
}

/**
 * Batched directory listing with hydrated stat / metadata / tags /
 * contentHash for every direct child (folders, files, symlinks).
 * Single round-trip replaces a naive `readdir + lstat × N` loop,
 * which incurs N+1 RPCs and cannot surface metadata or contentHash
 * in the same call.
 *
 * Wire shape: `{ revision, entries: VFSChildRaw[], cursor? }`.
 *   - `revision`: monotonically-increasing per-folder counter.
 *     Bumped by every mutation that affects this folder's direct
 *     children. Equality across two reads ⇒ contents identical
 *     (modulo concurrent in-flight mutations resolved by the DO's
 *     single-thread invariant).
 *   - `entries`: discriminated by `kind: "folder" | "file" | "symlink"`.
 *     Order: caller-supplied `orderBy` (default `mtime`) + `direction`
 *     (default `desc` for mtime/size, `asc` for name). Tie-break:
 *     kind enum (folder < symlink < file), then id ASC.
 *   - `cursor`: HMAC-signed, encodes `(ov, pid, k)` where `k` is the
 *     last entry's kind so the next page can resume each of the
 *     three streams (folders / files / symlinks) past the boundary
 *     correctly.
 *
 * Tombstone / archive gates match `vfsListFiles` (default exclude;
 * opt-in for admin/recovery surfaces). Folders are never tombstoned
 * at the schema level — they're hard-deleted by `vfsRmdir` — so
 * `includeTombstones` only affects file/symlink entries.
 *
 * No metadata / tag filter on this surface (deliberately narrow).
 * Use `vfsListFiles` with `prefix` for filtered queries; the
 * pagination-correctness logic elsewhere in this file ensures
 * those cursors enumerate completely.
 */
export async function vfsListChildren(
  durableObject: UserDO,
  scope: VFSScope,
  opts: ListChildrenOpts
): Promise<ListChildrenResult> {
  const userId = userIdFor(scope);
  const orderBy: OrderBy = opts.orderBy ?? "mtime";
  const direction: Direction =
    opts.direction ?? (orderBy === "name" ? "asc" : "desc");
  const limit = clampLimit(opts.limit);
  const includeStat = opts.includeStat !== false;
  const includeMetadata = opts.includeMetadata === true;
  const includeContentHash = opts.includeContentHash === true;
  const includeTombstones = opts.includeTombstones === true;
  const includeArchived = opts.includeArchived === true;

  if (typeof opts.path !== "string" || opts.path.length === 0) {
    throw new VFSError("EINVAL", "listChildren: path required");
  }

  const secret = getCursorSecret(durableObject.envPublic);
  let cursor: CursorPayload | null = null;
  if (opts.cursor) {
    cursor = await decodeCursor(opts.cursor, secret, orderBy, direction);
  }

  // Resolve the folder path. Reuse `resolvePrefixToParentId` semantics
  // (throws ENOENT / ENOTDIR) — listChildren on a file is ENOTDIR.
  const folderId = await resolvePrefixToParentId(
    durableObject,
    userId,
    opts.path
  );

  // Revision is read up-front so a concurrent mutation racing with
  // our SQL queries is reflected in the returned counter (the bump
  // happens AFTER the mutation's row writes commit; observers see the
  // new revision before they see the row mutation only if they read
  // revision after running their queries — we read it BEFORE so a
  // racing mutation manifests as `entries from new state, revision
  // from old state`, which is safe: caller will re-fetch on next
  // poll, see the bumped revision, and converge). Inside a single DO
  // turn (synchronous SQL) revision and entries are atomic.
  // P0 — read revision AFTER the queries to ensure caller sees a
  // revision >= the one that produced these entries (otherwise an
  // observer comparing cached revision = N with returned revision = N
  // could think nothing changed when in fact the entries reflect a
  // post-N state). The DO is single-threaded so this is straightforward.

  // Build streams: folders, files, symlinks. Each is ordered + cursor-
  // seeked the same way.
  const folderRows = listChildFolders(
    durableObject,
    userId,
    folderId,
    orderBy,
    direction,
    cursor,
    limit
  );
  const fileRows = listChildFiles(
    durableObject,
    userId,
    folderId,
    orderBy,
    direction,
    cursor,
    limit,
    includeTombstones,
    includeArchived
  );
  const symlinkRows = listChildSymlinks(
    durableObject,
    userId,
    folderId,
    orderBy,
    direction,
    cursor,
    limit
  );

  // Merge by (orderValue, kindRank, id). kindRank: folder=0, symlink=1,
  // file=2. Direction-aware comparison.
  const merged: MergedRow[] = mergeStreams(
    folderRows,
    symlinkRows,
    fileRows,
    direction
  );

  // Slice to limit. The merge sources each fetched up to `limit` rows,
  // so `merged.length` is at most `3 * limit` — slicing is cheap.
  const sliced = merged.slice(0, limit);
  const hasMore = merged.length > limit;

  // Hydrate.
  const entries: VFSChildRaw[] = [];
  for (const m of sliced) {
    if (m.kind === "folder") {
      const folderRow = durableObject.sql
        .exec(
          `SELECT folder_id, name, parent_id, updated_at, mode
             FROM folders
            WHERE folder_id = ? AND user_id = ?`,
          m.id,
          userId
        )
        .toArray()[0] as
        | {
            folder_id: string;
            name: string;
            parent_id: string | null;
            updated_at: number;
            mode: number;
          }
        | undefined;
      if (!folderRow) continue;
      const fpath = absolutePath(
        durableObject,
        userId,
        folderRow.parent_id,
        folderRow.name
      );
      const entry: VFSChildRaw = {
        kind: "folder",
        path: fpath,
        pathId: folderRow.folder_id,
        name: folderRow.name,
      };
      if (includeStat) {
        entry.stat = {
          type: "dir",
          mode: folderRow.mode ?? 0o755,
          size: 0,
          mtimeMs: folderRow.updated_at,
          uid: uidFromTenant(scope.tenant),
          gid: gidFromTenant(scope.tenant),
          ino: inoFromId(folderRow.folder_id),
        };
      }
      entries.push(entry);
    } else if (m.kind === "symlink") {
      const symRow = durableObject.sql
        .exec(
          `SELECT file_id, file_name, parent_id, symlink_target, updated_at, mode
             FROM files
            WHERE file_id = ? AND user_id = ? AND status = 'complete'`,
          m.id,
          userId
        )
        .toArray()[0] as
        | {
            file_id: string;
            file_name: string;
            parent_id: string | null;
            symlink_target: string | null;
            updated_at: number;
            mode: number;
          }
        | undefined;
      if (!symRow || symRow.symlink_target === null) continue;
      const spath = absolutePath(
        durableObject,
        userId,
        symRow.parent_id,
        symRow.file_name
      );
      const entry: VFSChildRaw = {
        kind: "symlink",
        path: spath,
        pathId: symRow.file_id,
        name: symRow.file_name,
        target: symRow.symlink_target,
      };
      if (includeStat) {
        entry.stat = {
          type: "symlink",
          mode: symRow.mode ?? 0o777,
          size: symRow.symlink_target.length,
          mtimeMs: symRow.updated_at,
          uid: uidFromTenant(scope.tenant),
          gid: gidFromTenant(scope.tenant),
          ino: inoFromId(symRow.file_id),
        };
      }
      entries.push(entry);
    } else {
      // file — reuse hydrateItem so file behaviour matches listFiles
      // exactly (head_version follow, archive/tombstone gating,
      // metadata + tags + contentHash hydration).
      const item = hydrateItem(
        durableObject,
        userId,
        scope,
        m.id,
        includeStat,
        includeMetadata,
        includeTombstones,
        includeArchived,
        includeContentHash
      );
      if (!item) continue;
      const fileEntry: VFSChildRaw = {
        kind: "file",
        path: item.path,
        pathId: item.pathId,
        name: leafName(item.path),
        tags: item.tags,
      };
      if (item.stat !== undefined) fileEntry.stat = item.stat;
      if (includeMetadata) fileEntry.metadata = item.metadata ?? null;
      if (includeContentHash && item.contentHash !== undefined) {
        fileEntry.contentHash = item.contentHash;
      }
      entries.push(fileEntry);
    }
  }

  // Build cursor from the LAST entry on the page when more exists.
  let nextCursor: string | undefined;
  if (hasMore && sliced.length > 0) {
    const last = sliced[sliced.length - 1];
    nextCursor = await encodeCursor(
      {
        v: 1,
        ob: orderBy,
        d: direction,
        ov: last.orderValue,
        pid: last.id,
        k: last.kind,
      },
      secret
    );
  }

  // Read revision AFTER queries (see comment above). Strict-monotonic
  // by DO single-thread invariant: any mutation that bumped revision
  // between query start and revision-read necessarily ran AFTER the
  // queries' SELECT (since the DO serializes turns), so the returned
  // entries are a snapshot from at-or-before the revision read here.
  const revision = readFolderRevision(durableObject, userId, folderId);

  return { revision, entries, cursor: nextCursor };
}

interface MergedRow {
  kind: "folder" | "file" | "symlink";
  id: string;
  orderValue: number | string;
}

function listChildFolders(
  durableObject: UserDO,
  userId: string,
  folderId: string | null,
  orderBy: OrderBy,
  direction: Direction,
  cursor: CursorPayload | null,
  limit: number
): MergedRow[] {
  // Folders don't have a `file_size` — size sort uses 0 as a stable
  // placeholder. mtime sort uses `updated_at`. Name sort uses `name`.
  const orderCol =
    orderBy === "name"
      ? "name"
      : orderBy === "size"
        ? "0"
        : "updated_at";
  const dirSql = direction === "asc" ? "ASC" : "DESC";

  const where: string[] = ["user_id = ?", "IFNULL(parent_id,'') = IFNULL(?,'')"];
  const args: (string | number)[] = [userId, folderId ?? ""];

  if (cursor) {
    // For folders, kindRank is 0 (smallest). When direction is desc,
    // resume rule: include rows where (ov < cursor.ov) OR
    // (ov === cursor.ov AND cursor.k === "folder" AND folder_id > pid).
    // When cursor.k is "symlink"|"file", folders at ov === cursor.ov
    // were ALL emitted on the prior page (folder kindRank=0 sorts
    // first when ascending, last when descending; but we're using a
    // canonical kind ordering folder<symlink<file regardless of
    // direction — see mergeStreams). So we exclude folders at that
    // ov entirely.
    const cmp = direction === "desc" ? "<" : ">";
    if (cursor.k === "folder") {
      where.push(
        `(${orderCol} ${cmp} ? OR (${orderCol} = ? AND folder_id > ?))`
      );
      args.push(
        cursor.ov as number | string,
        cursor.ov as number | string,
        cursor.pid
      );
    } else {
      // cursor anchored on a non-folder row at ov — folders at this
      // ov already emitted. Strict-greater on orderCol.
      where.push(`${orderCol} ${cmp} ?`);
      args.push(cursor.ov as number | string);
    }
  }
  args.push(limit + 1); // +1 so we can detect "more available"

  const sql = `
    SELECT folder_id AS id, ${orderCol} AS ov
      FROM folders
     WHERE ${where.join(" AND ")}
     ORDER BY ${orderCol} ${dirSql}, folder_id ASC
     LIMIT ?
  `;
  const rows = durableObject.sql
    .exec(sql, ...(args as [string, ...unknown[]]))
    .toArray() as { id: string; ov: number | string }[];
  return rows.map((r) => ({
    kind: "folder" as const,
    id: r.id,
    orderValue: r.ov,
  }));
}

function listChildFiles(
  durableObject: UserDO,
  userId: string,
  folderId: string | null,
  orderBy: OrderBy,
  direction: Direction,
  cursor: CursorPayload | null,
  limit: number,
  includeTombstones: boolean,
  includeArchived: boolean
): MergedRow[] {
  const orderCol =
    orderBy === "mtime"
      ? "f.updated_at"
      : orderBy === "name"
        ? "f.file_name"
        : "f.file_size";
  const dirSql = direction === "asc" ? "ASC" : "DESC";

  const where: string[] = [
    "f.user_id = ?",
    "f.status = 'complete'",
    "f.node_kind = 'file'",
    "IFNULL(f.parent_id,'') = IFNULL(?,'')",
  ];
  const args: (string | number)[] = [userId, folderId ?? ""];
  if (!includeTombstones) {
    where.push(
      "(f.head_version_id IS NULL OR fv.deleted IS NULL OR fv.deleted = 0)"
    );
  }
  if (!includeArchived) {
    where.push("f.archived = 0");
  }

  if (cursor) {
    const cmp = direction === "desc" ? "<" : ">";
    if (cursor.k === "file") {
      where.push(
        `(${orderCol} ${cmp} ? OR (${orderCol} = ? AND f.file_id > ?))`
      );
      args.push(
        cursor.ov as number | string,
        cursor.ov as number | string,
        cursor.pid
      );
    } else if (cursor.k === "folder" || cursor.k === "symlink") {
      // Files come AFTER folders+symlinks at the same ov in the
      // canonical (folder<symlink<file) tie-break. Resume:
      //   - boundary on folder: include files at cursor.ov (and past).
      //   - boundary on symlink: include files at cursor.ov (and past).
      // Both reduce to `${orderCol} ${cmp} ? OR ${orderCol} = ?`,
      // which simplifies to `${orderCol} ${cmp} ? OR ${orderCol} = ?`
      // — ambiguity-free because the inner OR covers ov === cursor.ov.
      where.push(
        `(${orderCol} ${cmp} ? OR ${orderCol} = ?)`
      );
      args.push(
        cursor.ov as number | string,
        cursor.ov as number | string
      );
    } else {
      // cursor.k undefined (legacy listFiles cursor passed by mistake)
      // — treat as "file" boundary, conservative.
      where.push(
        `(${orderCol} ${cmp} ? OR (${orderCol} = ? AND f.file_id > ?))`
      );
      args.push(
        cursor.ov as number | string,
        cursor.ov as number | string,
        cursor.pid
      );
    }
  }
  args.push(limit + 1);

  const sql = `
    SELECT f.file_id AS id, ${orderCol} AS ov
      FROM files f
      ${FILE_HEAD_JOIN}
     WHERE ${where.join(" AND ")}
     ORDER BY ${orderCol} ${dirSql}, f.file_id ASC
     LIMIT ?
  `;
  const rows = durableObject.sql
    .exec(sql, ...(args as [string, ...unknown[]]))
    .toArray() as { id: string; ov: number | string }[];
  return rows.map((r) => ({
    kind: "file" as const,
    id: r.id,
    orderValue: r.ov,
  }));
}

function listChildSymlinks(
  durableObject: UserDO,
  userId: string,
  folderId: string | null,
  orderBy: OrderBy,
  direction: Direction,
  cursor: CursorPayload | null,
  limit: number
): MergedRow[] {
  // Symlinks live in the `files` table with `node_kind='symlink'`.
  // They have no head_version_id (never versioned), no archive bit
  // semantics, no tombstones. Order columns mirror files.
  const orderCol =
    orderBy === "mtime"
      ? "updated_at"
      : orderBy === "name"
        ? "file_name"
        : "file_size";
  const dirSql = direction === "asc" ? "ASC" : "DESC";

  const where: string[] = [
    "user_id = ?",
    "status = 'complete'",
    "node_kind = 'symlink'",
    "IFNULL(parent_id,'') = IFNULL(?,'')",
  ];
  const args: (string | number)[] = [userId, folderId ?? ""];

  if (cursor) {
    const cmp = direction === "desc" ? "<" : ">";
    if (cursor.k === "symlink") {
      where.push(
        `(${orderCol} ${cmp} ? OR (${orderCol} = ? AND file_id > ?))`
      );
      args.push(
        cursor.ov as number | string,
        cursor.ov as number | string,
        cursor.pid
      );
    } else if (cursor.k === "folder") {
      // Symlinks come after folders at same ov. Include symlinks at
      // cursor.ov.
      where.push(`(${orderCol} ${cmp} ? OR ${orderCol} = ?)`);
      args.push(
        cursor.ov as number | string,
        cursor.ov as number | string
      );
    } else {
      // cursor.k === "file" — symlinks at cursor.ov already emitted
      // (symlink<file in tie-break). Strict-greater on orderCol.
      where.push(`${orderCol} ${cmp} ?`);
      args.push(cursor.ov as number | string);
    }
  }
  args.push(limit + 1);

  const sql = `
    SELECT file_id AS id, ${orderCol} AS ov
      FROM files
     WHERE ${where.join(" AND ")}
     ORDER BY ${orderCol} ${dirSql}, file_id ASC
     LIMIT ?
  `;
  const rows = durableObject.sql
    .exec(sql, ...(args as [string, ...unknown[]]))
    .toArray() as { id: string; ov: number | string }[];
  return rows.map((r) => ({
    kind: "symlink" as const,
    id: r.id,
    orderValue: r.ov,
  }));
}

/**
 * Merge folder / symlink / file streams in canonical order.
 *
 * Order key tuple: `(orderValue [direction-aware], kindRank, id ASC)`.
 * `kindRank: folder=0, symlink=1, file=2` regardless of direction —
 * keeping this fixed is what makes the cursor's `k` field unambiguous.
 *
 * Each input stream is already sorted by `(orderValue, id)` per its
 * SQL `ORDER BY`. We do a 3-way merge with one head pointer per
 * stream.
 */
function mergeStreams(
  folders: MergedRow[],
  symlinks: MergedRow[],
  files: MergedRow[],
  direction: Direction
): MergedRow[] {
  const out: MergedRow[] = [];
  let fi = 0;
  let si = 0;
  let xi = 0;
  const cmpOv = (
    a: number | string,
    b: number | string
  ): number => {
    if (typeof a === "number" && typeof b === "number") {
      return direction === "asc" ? a - b : b - a;
    }
    const sa = String(a);
    const sb = String(b);
    if (sa === sb) return 0;
    if (direction === "asc") return sa < sb ? -1 : 1;
    return sa < sb ? 1 : -1;
  };
  const kindRank = (k: "folder" | "symlink" | "file"): number =>
    k === "folder" ? 0 : k === "symlink" ? 1 : 2;
  const cmpRow = (a: MergedRow, b: MergedRow): number => {
    const c = cmpOv(a.orderValue, b.orderValue);
    if (c !== 0) return c;
    const kc = kindRank(a.kind) - kindRank(b.kind);
    if (kc !== 0) return kc;
    return a.id < b.id ? -1 : a.id > b.id ? 1 : 0;
  };
  while (fi < folders.length || si < symlinks.length || xi < files.length) {
    // Pick the smallest among current heads.
    const candidates: MergedRow[] = [];
    if (fi < folders.length) candidates.push(folders[fi]);
    if (si < symlinks.length) candidates.push(symlinks[si]);
    if (xi < files.length) candidates.push(files[xi]);
    let pick = candidates[0];
    for (let i = 1; i < candidates.length; i++) {
      if (cmpRow(candidates[i], pick) < 0) pick = candidates[i];
    }
    out.push(pick);
    if (pick.kind === "folder") fi++;
    else if (pick.kind === "symlink") si++;
    else xi++;
  }
  return out;
}

/** Extract the leaf segment from an absolute path. `/a/b/c` → `c`. */
function leafName(p: string): string {
  const i = p.lastIndexOf("/");
  return i < 0 ? p : p.slice(i + 1);
}

function clampLimit(n: number | undefined): number {
  if (n === undefined) return LIST_LIMIT_DEFAULT;
  if (!Number.isInteger(n) || n < 1 || n > LIST_LIMIT_MAX) {
    throw new VFSError(
      "EINVAL",
      `listFiles: limit must be an integer in 1..${LIST_LIMIT_MAX}`
    );
  }
  return n;
}

function chooseDriver(opts: ListFilesOpts): "tags" | "files" {
  if (opts.tags && opts.tags.length > 0) return "tags";
  return "files";
}

async function resolvePrefixToParentId(
  durableObject: UserDO,
  userId: string,
  prefix: string
): Promise<string | null> {
  const pathMod = await import("./path-walk");
  const r = pathMod.resolvePathFollow(durableObject, userId, prefix);
  if (r.kind === "ENOENT") {
    throw new VFSError("ENOENT", `listFiles: prefix not found: ${prefix}`);
  }
  if (r.kind !== "dir") {
    throw new VFSError(
      "ENOTDIR",
      `listFiles: prefix is not a directory: ${prefix}`
    );
  }
  return r.leafId === "" ? null : r.leafId;
}

interface DriverResult {
  candidates: { pathId: string; orderValue: number | string }[];
  /**
   * The last row that SQL returned (BEFORE post-filtering). When SQL
   * returned a full `sqlLimit` page this is the boundary the next
   * page must seek past. When the SQL page was short (rows < sqlLimit),
   * `sqlBoundary` is the last row and `sqlPageWasFull` is false —
   * caller does not emit a cursor.
   */
  sqlBoundary: { pathId: string; orderValue: number | string } | null;
  /** Whether SQL returned `sqlLimit` rows (i.e. there may be more). */
  sqlPageWasFull: boolean;
}

function listByFiles(
  durableObject: UserDO,
  userId: string,
  parentId: string | null | undefined,
  orderBy: OrderBy,
  direction: Direction,
  cursor: CursorPayload | null,
  limit: number,
  tagsFilter: readonly string[] | undefined,
  includeTombstones: boolean,
  includeArchived: boolean
): DriverResult {
  // Choose the SQL ordering column.
  const orderCol =
    orderBy === "mtime"
      ? "f.updated_at"
      : orderBy === "name"
        ? "f.file_name"
        : "f.file_size";
  const dirSql = direction === "asc" ? "ASC" : "DESC";
  const tieBreak = direction === "asc" ? ">" : ">"; // Always strict-greater on file_id for tie-break stability.

  // WHERE clauses.
  const where: string[] = [
    "f.user_id = ?",
    "f.status = 'complete'",
    "f.node_kind = ?",
  ];
  const args: (string | number)[] = [userId, FILE_NODE_KIND];
  if (parentId !== undefined) {
    where.push("IFNULL(f.parent_id,'') = IFNULL(?,'')");
    args.push(parentId ?? "");
  }
  // Tombstone-consistency. LEFT JOIN to file_versions on the head
  // pointer; require either no head (non-versioned tenants have
  // head_version_id IS NULL) or a non-tombstoned head. The
  // partial-NULL match is preserved by the LEFT JOIN — when
  // head_version_id IS NULL the join returns NULL columns and the
  // `fv.deleted IS NULL` predicate accepts it.
  if (!includeTombstones) {
    where.push(
      "(f.head_version_id IS NULL OR fv.deleted IS NULL OR fv.deleted = 0)"
    );
  }
  // Archive bit. Default-on filter so a tenant's "Hidden" /
  // "Trash" UI must explicitly opt in to see them.
  if (!includeArchived) {
    where.push("f.archived = 0");
  }
  if (cursor) {
    // Seek bound: (orderCol < ov) OR (orderCol = ov AND file_id > pid)
    // for direction=desc; flip for asc.
    const cmp = direction === "desc" ? "<" : ">";
    where.push(
      `(${orderCol} ${cmp} ? OR (${orderCol} = ? AND f.file_id ${tieBreak} ?))`
    );
    args.push(
      cursor.ov as number | string,
      cursor.ov as number | string,
      cursor.pid
    );
  }
  // Over-fetch when post-filtering for tags so the page after
  // intersection is closer to `limit`.
  const sqlLimit = tagsFilter && tagsFilter.length > 0 ? limit * 4 : limit;
  args.push(sqlLimit);

  const sql = `
    SELECT f.file_id AS file_id, ${orderCol} AS ov
      FROM files f
      ${FILE_HEAD_JOIN}
     WHERE ${where.join(" AND ")}
     ORDER BY ${orderCol} ${dirSql}, f.file_id ASC
     LIMIT ?
  `;
  const rows = durableObject.sql
    .exec(sql, ...(args as [string, ...unknown[]]))
    .toArray() as { file_id: string; ov: number | string }[];

  let pairs = rows.map((r) => ({
    pathId: r.file_id,
    orderValue: r.ov,
  }));
  const sqlBoundary =
    rows.length > 0
      ? {
          pathId: rows[rows.length - 1].file_id,
          orderValue: rows[rows.length - 1].ov,
        }
      : null;
  const sqlPageWasFull = rows.length === sqlLimit;

  // Tag AND-filter, if any.
  if (tagsFilter && tagsFilter.length > 0) {
    pairs = pairs.filter((p) =>
      hasAllTags(durableObject, p.pathId, tagsFilter)
    );
    pairs = pairs.slice(0, limit);
  }
  return { candidates: pairs, sqlBoundary, sqlPageWasFull };
}

function listByTags(
  durableObject: UserDO,
  userId: string,
  tags: readonly string[],
  parentId: string | null | undefined,
  orderBy: OrderBy,
  direction: Direction,
  cursor: CursorPayload | null,
  limit: number,
  includeTombstones: boolean,
  includeArchived: boolean
): DriverResult {
  // Single-tag fast path.
  if (tags.length === 1) {
    return listSingleTag(
      durableObject,
      userId,
      tags[0],
      parentId,
      orderBy,
      direction,
      cursor,
      limit,
      includeTombstones,
      includeArchived
    );
  }
  // Multi-tag intersect: drive from the rarest tag.
  const tagCounts = tags.map((t) => ({
    tag: t,
    n:
      (
        durableObject.sql
          .exec(
            "SELECT COUNT(*) AS n FROM file_tags WHERE tag = ?",
            t
          )
          .toArray()[0] as { n: number }
      ).n,
  }));
  tagCounts.sort((a, b) => a.n - b.n);
  const driver = tagCounts[0].tag;
  const otherTags = tags.filter((t) => t !== driver);

  // Drive from the rarest tag's index, then INTERSECT in app code.
  const driverResult = listSingleTag(
    durableObject,
    userId,
    driver,
    parentId,
    orderBy,
    direction,
    cursor,
    limit * 4, // over-fetch; intersect may drop rows.
    includeTombstones,
    includeArchived
  );
  const filtered = driverResult.candidates.filter((p) =>
    hasAllTags(durableObject, p.pathId, otherTags)
  );
  return {
    candidates: filtered.slice(0, limit),
    sqlBoundary: driverResult.sqlBoundary,
    sqlPageWasFull: driverResult.sqlPageWasFull,
  };
}

function listSingleTag(
  durableObject: UserDO,
  userId: string,
  tag: string,
  parentId: string | null | undefined,
  orderBy: OrderBy,
  direction: Direction,
  cursor: CursorPayload | null,
  limit: number,
  includeTombstones: boolean,
  includeArchived: boolean
): DriverResult {
  // The file_tags index covers (tag, mtime_ms DESC, path_id). For
  // orderBy 'mtime' we use it directly. For 'name' or 'size' we
  // resolve the path_ids via tag, then sort + filter via files.
  if (orderBy === "mtime") {
    const dirSql = direction === "asc" ? "ASC" : "DESC";
    const where: string[] = ["t.tag = ?", "t.user_id = ?"];
    const args: (string | number)[] = [tag, userId];
    if (cursor) {
      const cmp = direction === "desc" ? "<" : ">";
      where.push(
        `(t.mtime_ms ${cmp} ? OR (t.mtime_ms = ? AND t.path_id > ?))`
      );
      args.push(
        cursor.ov as number | string,
        cursor.ov as number | string,
        cursor.pid
      );
    }
    if (parentId !== undefined) {
      // Subquery: filter to files whose parent_id matches.
      where.push(
        `EXISTS (SELECT 1 FROM files f WHERE f.file_id = t.path_id
                  AND IFNULL(f.parent_id,'') = IFNULL(?,'')
                  AND f.status = 'complete')`
      );
      args.push(parentId ?? "");
    }
    // Tombstone filter on the path_id's head version.
    if (!includeTombstones) {
      where.push(
        `NOT EXISTS (
           SELECT 1 FROM files f
             JOIN file_versions fv
               ON fv.path_id = f.file_id
              AND fv.version_id = f.head_version_id
            WHERE f.file_id = t.path_id
              AND fv.deleted = 1
         )`
      );
    }
    // Archive filter on the path_id's files row.
    if (!includeArchived) {
      where.push(
        `EXISTS (
           SELECT 1 FROM files f
            WHERE f.file_id = t.path_id AND f.archived = 0
         )`
      );
    }
    args.push(limit);
    const sql = `
      SELECT t.path_id AS pid, t.mtime_ms AS ov
        FROM file_tags t
       WHERE ${where.join(" AND ")}
       ORDER BY t.mtime_ms ${dirSql}, t.path_id ASC
       LIMIT ?
    `;
    const rows = durableObject.sql
      .exec(sql, ...(args as [string, ...unknown[]]))
      .toArray() as { pid: string; ov: number }[];
    const candidates = rows.map((r) => ({
      pathId: r.pid,
      orderValue: r.ov as number | string,
    }));
    return {
      candidates,
      sqlBoundary:
        rows.length > 0
          ? {
              pathId: rows[rows.length - 1].pid,
              orderValue: rows[rows.length - 1].ov as number | string,
            }
          : null,
      sqlPageWasFull: rows.length === limit,
    };
  }
  // For non-mtime ordering, fetch tag matches then JOIN files for ordering.
  const orderCol = orderBy === "name" ? "f.file_name" : "f.file_size";
  const dirSql = direction === "asc" ? "ASC" : "DESC";
  const where: string[] = [
    "t.tag = ?",
    "t.user_id = ?",
    "f.status = 'complete'",
  ];
  const args: (string | number)[] = [tag, userId];
  if (parentId !== undefined) {
    where.push("IFNULL(f.parent_id,'') = IFNULL(?,'')");
    args.push(parentId ?? "");
  }
  if (!includeTombstones) {
    where.push(
      "(f.head_version_id IS NULL OR fv.deleted IS NULL OR fv.deleted = 0)"
    );
  }
  // Archive filter (non-mtime ordering branch).
  if (!includeArchived) {
    where.push("f.archived = 0");
  }
  if (cursor) {
    const cmp = direction === "desc" ? "<" : ">";
    where.push(
      `(${orderCol} ${cmp} ? OR (${orderCol} = ? AND f.file_id > ?))`
    );
    args.push(
      cursor.ov as number | string,
      cursor.ov as number | string,
      cursor.pid
    );
  }
  args.push(limit);
  const sql = `
    SELECT f.file_id AS pid, ${orderCol} AS ov
      FROM file_tags t
      JOIN files f ON f.file_id = t.path_id
      ${FILE_HEAD_JOIN}
     WHERE ${where.join(" AND ")}
     ORDER BY ${orderCol} ${dirSql}, f.file_id ASC
     LIMIT ?
  `;
  const rows = durableObject.sql
    .exec(sql, ...(args as [string, ...unknown[]]))
    .toArray() as { pid: string; ov: number | string }[];
  const candidates = rows.map((r) => ({
    pathId: r.pid,
    orderValue: r.ov,
  }));
  return {
    candidates,
    sqlBoundary:
      rows.length > 0
        ? {
            pathId: rows[rows.length - 1].pid,
            orderValue: rows[rows.length - 1].ov,
          }
        : null,
    sqlPageWasFull: rows.length === limit,
  };
}

function hasAllTags(
  durableObject: UserDO,
  pathId: string,
  tags: readonly string[]
): boolean {
  if (tags.length === 0) return true;
  const placeholders = tags.map(() => "?").join(",");
  const r = durableObject.sql
    .exec(
      `SELECT COUNT(*) AS n FROM file_tags WHERE path_id = ? AND tag IN (${placeholders})`,
      pathId,
      ...(tags as readonly string[])
    )
    .toArray()[0] as { n: number };
  return r.n === tags.length;
}

function postFilterMetadata(
  durableObject: UserDO,
  pairs: { pathId: string; orderValue: number | string }[],
  filter: Record<string, unknown>
): { pathId: string; orderValue: number | string }[] {
  if (pairs.length === 0) return pairs;
  // Read metadata blobs for the page candidates and post-filter.
  const placeholders = pairs.map(() => "?").join(",");
  const rows = durableObject.sql
    .exec(
      `SELECT file_id, metadata FROM files WHERE file_id IN (${placeholders})`,
      ...pairs.map((p) => p.pathId)
    )
    .toArray() as { file_id: string; metadata: ArrayBuffer | null }[];
  const byId = new Map<string, ArrayBuffer | null>();
  for (const r of rows) byId.set(r.file_id, r.metadata);
  return pairs.filter((p) => {
    const blob = byId.get(p.pathId);
    if (!blob) return false;
    let obj: Record<string, unknown>;
    try {
      obj = JSON.parse(
        new TextDecoder().decode(new Uint8Array(blob))
      ) as Record<string, unknown>;
    } catch {
      return false;
    }
    return matchesFilter(obj, filter);
  });
}

function matchesFilter(
  obj: Record<string, unknown>,
  filter: Record<string, unknown>
): boolean {
  for (const k of Object.keys(filter)) {
    const want = filter[k];
    const got = obj[k];
    if (typeof want === "object" && want !== null && !Array.isArray(want)) {
      if (
        typeof got !== "object" ||
        got === null ||
        Array.isArray(got) ||
        !matchesFilter(
          got as Record<string, unknown>,
          want as Record<string, unknown>
        )
      ) {
        return false;
      }
    } else if (Array.isArray(want)) {
      if (!Array.isArray(got)) return false;
      if (got.length !== want.length) return false;
      for (let i = 0; i < want.length; i++) {
        if (got[i] !== want[i]) return false;
      }
    } else if (got !== want) {
      return false;
    }
  }
  return true;
}

function hydrateItem(
  durableObject: UserDO,
  userId: string,
  scope: VFSScope,
  pathId: string,
  includeStat: boolean,
  includeMetadata: boolean,
  includeTombstones: boolean,
  includeArchived: boolean,
  includeContentHash: boolean = false
): ListFilesItemRaw | null {
  const f = durableObject.sql
    .exec(
      `SELECT file_id, file_name, parent_id, file_size, updated_at, mode,
              metadata, mode_yjs, head_version_id, archived, file_hash
         FROM files
        WHERE file_id = ? AND user_id = ? AND status = 'complete'`,
      pathId,
      userId
    )
    .toArray()[0] as
    | {
        file_id: string;
        file_name: string;
        parent_id: string | null;
        file_size: number;
        updated_at: number;
        mode: number;
        metadata: ArrayBuffer | null;
        mode_yjs: number;
        head_version_id: string | null;
        archived: number;
        file_hash: string;
      }
    | undefined;
  if (!f) return null;
  // Archive filter at the hydration boundary. Default listings
  // exclude archived rows. fileInfo (with default
  // `includeArchived: false`) returns null → caller raises ENOENT.
  if (!includeArchived && f.archived === 1) return null;

  // Tombstone-consistency. When the file row carries a versioned
  // head pointer, follow it; if the head is tombstoned we
  // skip this row (default) so the result is stat-able, mirroring
  // `statForResolved` (helpers.ts:244). We also use the head
  // version's `size` and `mtime_ms` so the surfaced stat matches
  // what `vfsStat` would return — closing the previous "approximate
  // for versioned tenants" gap.
  let headSize = f.file_size;
  let headMtime = f.updated_at;
  if (f.head_version_id !== null) {
    const head = durableObject.sql
      .exec(
        `SELECT size, mtime_ms, deleted, inline_data
           FROM file_versions
          WHERE path_id = ? AND version_id = ?`,
        f.file_id,
        f.head_version_id
      )
      .toArray()[0] as
      | {
          size: number;
          mtime_ms: number;
          deleted: number;
          inline_data: ArrayBuffer | null;
        }
      | undefined;
    if (head) {
      if (head.deleted === 1 && !includeTombstones) {
        return null;
      }
      headSize = head.inline_data
        ? head.inline_data.byteLength
        : head.size;
      headMtime = head.mtime_ms;
    }
    // If the version row is missing (orphan head), fall through to
    // the denormalized files columns — same fallback as
    // `statForResolved`.
  }

  const path = absolutePath(durableObject, userId, f.parent_id, f.file_name);
  const tags = (
    durableObject.sql
      .exec(
        "SELECT tag FROM file_tags WHERE path_id = ? ORDER BY tag",
        pathId
      )
      .toArray() as { tag: string }[]
  ).map((r) => r.tag);

  const out: ListFilesItemRaw = {
    path,
    pathId,
    tags,
  };

  if (includeStat) {
    // Build a VFSStatRaw aligned with `statForResolved`'s file
    // branch. Source of truth for size/mtime is the head version
    // row when present, falling back to the denormalized `files`
    // columns. mode_yjs is invariant across versions of the same
    // path (lives on `files`), so we still read it from the row.
    out.stat = {
      type: "file",
      mode: (f.mode ?? 0o644) | (f.mode_yjs === 1 ? 0o4000 : 0),
      size: headSize,
      mtimeMs: headMtime,
      uid: uidFromTenant(scope.tenant),
      gid: gidFromTenant(scope.tenant),
      ino: inoFromId(pathId),
    };
  }

  if (includeMetadata) {
    if (f.metadata) {
      try {
        out.metadata = JSON.parse(
          new TextDecoder().decode(new Uint8Array(f.metadata))
        ) as Record<string, unknown>;
      } catch {
        out.metadata = null;
      }
    } else {
      out.metadata = null;
    }
  }

  if (includeContentHash) {
    // Surface persisted SHA-256 hex. The column is NOT NULL but
    // legacy inline-tier writes set it to '' (no hash computed).
    // For backward compatibility we surface only non-empty values;
    // consumers who need an inline hash should compute it
    // client-side from the bytes.
    if (f.file_hash !== "") {
      out.contentHash = f.file_hash;
    }
  }

  return out;
}

function absolutePath(
  durableObject: UserDO,
  userId: string,
  parentId: string | null,
  leaf: string
): string {
  // Walk up the folder chain from parentId to root.
  const segs: string[] = [leaf];
  let cur: string | null = parentId;
  while (cur !== null) {
    const row = durableObject.sql
      .exec(
        "SELECT name, parent_id FROM folders WHERE folder_id = ? AND user_id = ?",
        cur,
        userId
      )
      .toArray()[0] as { name: string; parent_id: string | null } | undefined;
    if (!row) break; // should not happen for well-formed data
    segs.push(row.name);
    cur = row.parent_id;
  }
  segs.reverse();
  return "/" + segs.join("/");
}
