/**
 * Admin tooling — operator-only helpers, not exposed via the public
 * VFS RPC surface or the legacy fetch routes.
 *
 * dedupePaths(userId): resolve legacy duplicate (parent_id, file_name)
 * rows that pre-date the Phase 1 UNIQUE partial index. The migration
 * tries to add the index lazily in ensureInit, but if existing data
 * has live duplicates the CREATE throws and is swallowed (Phase 1's
 * try/catch). This routine is the manual cleanup pass:
 *
 *   1. Identify groups of (user_id, IFNULL(parent_id,''), file_name)
 *      where >1 rows have status != 'deleted'.
 *   2. Within each group: keep the row with the largest updated_at
 *      (last writer wins; ties broken by file_id lexicographic, which
 *      is monotonic-ish since generateId is timestamp-prefixed).
 *   3. For each loser: hard-delete its files row + file_chunks rows,
 *      then dispatch ShardDO.deleteChunks(file_id) for every shard
 *      the file's chunks lived on. The shard sweeper takes it from
 *      there.
 *   4. Attempt to (re-)create the UNIQUE partial index. Returns the
 *      number of rows reconciled and whether the index was created.
 *
 * Concurrency: the routine runs as one DO RPC method. DOs serialise
 * synchronous code, but each `await this.shardStub.deleteChunks(...)`
 * temporarily releases the input lock, so a concurrent VFS write
 * CAN interleave with the dedupe pass. This is acceptable because:
 *   - In-flight writes use status='uploading' + a `_vfs_tmp_<id>`
 *     name; they never participate in an organic-name dupe group.
 *   - Newly-committed files have a fresher updated_at than anything
 *     dedupe is processing, so they would be the "winner" anyway.
 *   - The unique partial index, if successfully created at the end
 *     of the pass, then prevents future dupes.
 * If strict serialisation is required, wrap the call in
 * `this.ctx.blockConcurrencyWhile(...)` at the call site. Default
 * here is "eventually correct" — operator can re-run dedupePaths
 * if a race introduced a fresh dupe.
 *
 * Folders: the same shape applies. Different table, no soft-delete
 * column, but otherwise identical: pick newest, drop the rest, but
 * we ONLY drop a folder if it has no children (a folder with
 * children participating in the dupe is not safe to delete because
 * its file_id is referenced by file_chunks.file_id implicitly via
 * parent_id; we can't reparent without knowing intent). For the
 * MVP we report-but-skip duped folders that have children.
 */

import type { UserDOCore as UserDO } from "./user-do-core";
import type { ShardDO } from "../shard/shard-do";
import { vfsShardDOName } from "../../lib/utils";
import type { VFSScope } from "../../../../shared/vfs-types";

export interface DedupeResult {
  /** Number of duplicate file rows hard-deleted. */
  fileDupesResolved: number;
  /** Number of folder rows dropped. */
  folderDupesResolved: number;
  /** Number of folder dupe groups skipped because the loser had children. */
  folderDupesSkipped: number;
  /** Whether the UNIQUE partial index ended up in place (true if it now exists). */
  uniqFilesIndex: boolean;
  /** Whether the folders UNIQUE index ended up in place. */
  uniqFoldersIndex: boolean;
}

/**
 * Resolve legacy duplicate (parent_id, name) rows for a single user.
 *
 * `scope` carries the multi-tenant routing for ShardDO fan-out — the
 * deleted_chunks RPC needs (ns, tenant, sub) so it hits the right
 * vfs:${ns}:${tenant}[:${sub}]:s${idx} ShardDO instances. For legacy
 * data written through the user-facing app (NOT the VFS), the shard
 * names are the OLD pattern `shard:${userId}:${idx}` — those callers
 * should pass scope=null and let the routine use the legacy naming.
 */
export async function dedupePaths(
  durableObject: UserDO,
  userId: string,
  scope: VFSScope | null = null
): Promise<DedupeResult> {
  const result: DedupeResult = {
    fileDupesResolved: 0,
    folderDupesResolved: 0,
    folderDupesSkipped: 0,
    uniqFilesIndex: false,
    uniqFoldersIndex: false,
  };

  // ── Pass 1: file duplicates ────────────────────────────────────────────
  //
  // GROUP BY (parent_id, file_name) status!='deleted', count > 1.
  const fileGroups = durableObject.sql
    .exec(
      `SELECT IFNULL(parent_id, '') AS parent_key, file_name, COUNT(*) AS n
         FROM files
        WHERE user_id = ? AND status != 'deleted'
        GROUP BY parent_key, file_name
        HAVING n > 1`,
      userId
    )
    .toArray() as { parent_key: string; file_name: string; n: number }[];

  for (const grp of fileGroups) {
    // Pull the candidate rows. Order by updated_at DESC, file_id DESC
    // so the first row is the "winner" and the rest are losers.
    const rows = durableObject.sql
      .exec(
        `SELECT file_id, updated_at FROM files
          WHERE user_id = ?
            AND IFNULL(parent_id, '') = ?
            AND file_name = ?
            AND status != 'deleted'
          ORDER BY updated_at DESC, file_id DESC`,
        userId,
        grp.parent_key,
        grp.file_name
      )
      .toArray() as { file_id: string; updated_at: number }[];

    if (rows.length <= 1) continue; // race: another op resolved it
    const losers = rows.slice(1);

    for (const loser of losers) {
      await hardDeleteLoser(durableObject, userId, scope, loser.file_id);
      result.fileDupesResolved++;
    }
  }

  // ── Pass 2: folder duplicates ──────────────────────────────────────────
  //
  // Folders have no soft-delete state, so we look at every group with
  // count > 1. For each loser, only drop if it has no children
  // (subfolders + non-deleted files). Otherwise skip and surface a
  // count for the operator to investigate.
  const folderGroups = durableObject.sql
    .exec(
      `SELECT IFNULL(parent_id, '') AS parent_key, name, COUNT(*) AS n
         FROM folders
        WHERE user_id = ?
        GROUP BY parent_key, name
        HAVING n > 1`,
      userId
    )
    .toArray() as { parent_key: string; name: string; n: number }[];

  for (const grp of folderGroups) {
    const rows = durableObject.sql
      .exec(
        `SELECT folder_id, updated_at FROM folders
          WHERE user_id = ?
            AND IFNULL(parent_id, '') = ?
            AND name = ?
          ORDER BY updated_at DESC, folder_id DESC`,
        userId,
        grp.parent_key,
        grp.name
      )
      .toArray() as { folder_id: string; updated_at: number }[];
    if (rows.length <= 1) continue;
    const losers = rows.slice(1);
    for (const loser of losers) {
      const childFolder = durableObject.sql
        .exec(
          "SELECT 1 FROM folders WHERE user_id = ? AND parent_id = ? LIMIT 1",
          userId,
          loser.folder_id
        )
        .toArray();
      const childFile = durableObject.sql
        .exec(
          "SELECT 1 FROM files WHERE user_id = ? AND parent_id = ? AND status != 'deleted' LIMIT 1",
          userId,
          loser.folder_id
        )
        .toArray();
      if (childFolder.length > 0 || childFile.length > 0) {
        result.folderDupesSkipped++;
        continue;
      }
      durableObject.sql.exec(
        "DELETE FROM folders WHERE folder_id = ? AND user_id = ?",
        loser.folder_id,
        userId
      );
      result.folderDupesResolved++;
    }
  }

  // ── Pass 3: re-create the UNIQUE partial indexes ───────────────────────
  //
  // Phase 1's lazy ensureInit attempts these every cold start; if
  // duplicates blocked them then, they may now succeed. Both creations
  // are idempotent (IF NOT EXISTS). On still-duplicated state, a
  // second swallow happens here too — the result flags reflect what
  // we observe via sqlite_master after the attempt.
  try {
    durableObject.sql.exec(`
      CREATE UNIQUE INDEX IF NOT EXISTS uniq_files_parent_name
        ON files(user_id, IFNULL(parent_id, ''), file_name)
        WHERE status != 'deleted'
    `);
  } catch {
    // still blocked — operator must investigate
  }
  try {
    durableObject.sql.exec(`
      CREATE UNIQUE INDEX IF NOT EXISTS uniq_folders_parent_name
        ON folders(user_id, IFNULL(parent_id, ''), name)
    `);
  } catch {
    // still blocked
  }

  const idxRows = durableObject.sql
    .exec("SELECT name FROM sqlite_master WHERE type='index'")
    .toArray() as { name: string }[];
  const idxSet = new Set(idxRows.map((r) => r.name));
  result.uniqFilesIndex = idxSet.has("uniq_files_parent_name");
  result.uniqFoldersIndex = idxSet.has("uniq_folders_parent_name");

  return result;
}

/**
 * Drop a single losing file row + its file_chunks, then dispatch
 * deleteChunks RPC to each touched shard. Mirrors the inline-rename
 * GC path from Phase 3's commitRename, but here the file_id is the
 * old loser, not a freshly-superseded one.
 *
 * `scope` selects the shard name pattern:
 *   - scope null → legacy `shard:${userId}:${idx}` instances
 *     (rows written by the user-facing app pre-VFS)
 *   - scope set  → vfs:${ns}:${tenant}[:${sub}]:s${idx} instances
 *
 * Sequential RPCs (no parallel) so a transient shard error doesn't
 * fan out to all touched shards at once.
 */
async function hardDeleteLoser(
  durableObject: UserDO,
  userId: string,
  scope: VFSScope | null,
  fileId: string
): Promise<void> {
  const shardRows = durableObject.sql
    .exec(
      "SELECT DISTINCT shard_index FROM file_chunks WHERE file_id = ?",
      fileId
    )
    .toArray() as { shard_index: number }[];

  durableObject.sql.exec("DELETE FROM file_chunks WHERE file_id = ?", fileId);
  durableObject.sql.exec("DELETE FROM files WHERE file_id = ?", fileId);

  const env = durableObject.envPublic;
  const shardNs = env.MOSSAIC_SHARD as unknown as DurableObjectNamespace<ShardDO>;
  for (const { shard_index } of shardRows) {
    const shardName = scope
      ? vfsShardDOName(scope.ns, scope.tenant, scope.sub, shard_index)
      : `shard:${userId}:${shard_index}`;
    const stub = shardNs.get(shardNs.idFromName(shardName));
    await stub.deleteChunks(fileId);
  }
}
