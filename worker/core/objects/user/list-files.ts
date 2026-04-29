/**
 * Phase 12: indexed listFiles primitive.
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
import { VFSError, type VFSScope } from "@shared/vfs-types";
import {
  decodeCursor,
  encodeCursor,
  type CursorPayload,
  type Direction,
  type OrderBy,
} from "../../lib/cursor";
import {
  LIST_LIMIT_DEFAULT,
  LIST_LIMIT_MAX,
  TAGS_MAX_PER_LIST_QUERY,
} from "@shared/metadata-caps";

// Stat raw shape; mirrors VFSStatRaw fields we surface.
import type { VFSStatRaw } from "@shared/vfs-types";
import { gidFromTenant, inoFromId, uidFromTenant } from "@shared/ino";

export interface ListFilesItemRaw {
  path: string;
  pathId: string;
  stat?: VFSStatRaw;
  metadata?: Record<string, unknown> | null;
  tags: string[];
}

export interface ListFilesResult {
  items: ListFilesItemRaw[];
  cursor?: string;
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

  if (opts.tags && opts.tags.length > TAGS_MAX_PER_LIST_QUERY) {
    throw new VFSError(
      "EINVAL",
      `listFiles: too many tags (max ${TAGS_MAX_PER_LIST_QUERY})`
    );
  }

  const secret = (durableObject.envPublic as { JWT_SECRET?: string }).JWT_SECRET ??
    "mossaic-cursor-dev-secret-do-not-use-in-prod";
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
  if (driver === "tags") {
    candidates = listByTags(
      durableObject,
      userId,
      opts.tags!,
      parentId,
      orderBy,
      direction,
      cursor,
      limit
    );
  } else {
    // "prefix" or "none" — both use the files index.
    candidates = listByFiles(
      durableObject,
      userId,
      parentId,
      orderBy,
      direction,
      cursor,
      limit,
      opts.tags
    );
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
      includeMetadata
    );
    if (item) items.push(item);
  }

  // Build next cursor if the page was full.
  let nextCursor: string | undefined;
  if (items.length === limit && candidates.length > 0) {
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

function userIdFor(scope: VFSScope): string {
  if (scope.sub !== undefined) return `${scope.tenant}::${scope.sub}`;
  return scope.tenant;
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

function listByFiles(
  durableObject: UserDO,
  userId: string,
  parentId: string | null | undefined,
  orderBy: OrderBy,
  direction: Direction,
  cursor: CursorPayload | null,
  limit: number,
  tagsFilter: readonly string[] | undefined
): { pathId: string; orderValue: number | string }[] {
  // Choose the SQL ordering column.
  const orderCol =
    orderBy === "mtime"
      ? "updated_at"
      : orderBy === "name"
        ? "file_name"
        : "file_size";
  const dirSql = direction === "asc" ? "ASC" : "DESC";
  const tieBreak = direction === "asc" ? ">" : ">"; // Always strict-greater on file_id for tie-break stability.

  // WHERE clauses.
  const where: string[] = [
    "user_id = ?",
    "status = 'complete'",
    "node_kind = ?",
  ];
  const args: (string | number)[] = [userId, FILE_NODE_KIND];
  if (parentId !== undefined) {
    where.push("IFNULL(parent_id,'') = IFNULL(?,'')");
    args.push(parentId ?? "");
  }
  if (cursor) {
    // Seek bound: (orderCol < ov) OR (orderCol = ov AND file_id > pid)
    // for direction=desc; flip for asc.
    const cmp = direction === "desc" ? "<" : ">";
    where.push(
      `(${orderCol} ${cmp} ? OR (${orderCol} = ? AND file_id ${tieBreak} ?))`
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
    SELECT file_id, ${orderCol} AS ov
      FROM files
     WHERE ${where.join(" AND ")}
     ORDER BY ${orderCol} ${dirSql}, file_id ASC
     LIMIT ?
  `;
  const rows = durableObject.sql
    .exec(sql, ...(args as [string, ...unknown[]]))
    .toArray() as { file_id: string; ov: number | string }[];

  let pairs = rows.map((r) => ({
    pathId: r.file_id,
    orderValue: r.ov,
  }));

  // Tag AND-filter, if any.
  if (tagsFilter && tagsFilter.length > 0) {
    pairs = pairs.filter((p) =>
      hasAllTags(durableObject, p.pathId, tagsFilter)
    );
    pairs = pairs.slice(0, limit);
  }
  return pairs;
}

function listByTags(
  durableObject: UserDO,
  userId: string,
  tags: readonly string[],
  parentId: string | null | undefined,
  orderBy: OrderBy,
  direction: Direction,
  cursor: CursorPayload | null,
  limit: number
): { pathId: string; orderValue: number | string }[] {
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
      limit
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
  const driverPairs = listSingleTag(
    durableObject,
    userId,
    driver,
    parentId,
    orderBy,
    direction,
    cursor,
    limit * 4 // over-fetch; intersect may drop rows.
  );
  const filtered = driverPairs.filter((p) =>
    hasAllTags(durableObject, p.pathId, otherTags)
  );
  return filtered.slice(0, limit);
}

function listSingleTag(
  durableObject: UserDO,
  userId: string,
  tag: string,
  parentId: string | null | undefined,
  orderBy: OrderBy,
  direction: Direction,
  cursor: CursorPayload | null,
  limit: number
): { pathId: string; orderValue: number | string }[] {
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
    return rows.map((r) => ({ pathId: r.pid, orderValue: r.ov }));
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
     WHERE ${where.join(" AND ")}
     ORDER BY ${orderCol} ${dirSql}, f.file_id ASC
     LIMIT ?
  `;
  const rows = durableObject.sql
    .exec(sql, ...(args as [string, ...unknown[]]))
    .toArray() as { pid: string; ov: number | string }[];
  return rows.map((r) => ({ pathId: r.pid, orderValue: r.ov }));
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
  includeMetadata: boolean
): ListFilesItemRaw | null {
  const f = durableObject.sql
    .exec(
      `SELECT file_id, file_name, parent_id, file_size, updated_at, mode,
              metadata, mode_yjs
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
      }
    | undefined;
  if (!f) return null;

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
    // Build a minimal VFSStatRaw. Mirrors statForResolved's file branch
    // closely; we don't follow versioning here (head-version size
    // already lives on file_versions, not files, but for listFiles we
    // use the file's denormalized fields which match for non-versioned
    // tenants and approximate for versioned ones — listFiles is not
    // a strict-stat surface).
    out.stat = {
      type: "file",
      mode: (f.mode ?? 0o644) | (f.mode_yjs === 1 ? 0o4000 : 0),
      size: f.file_size,
      mtimeMs: f.updated_at,
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
