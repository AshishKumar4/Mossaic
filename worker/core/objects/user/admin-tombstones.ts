/**
 * Tombstoned-head reaper (recovery primitive).
 *
 * Steady-state list/stat consistency filters tombstoned-head rows;
 * the throw at `helpers.ts:245` remains correct, matching
 * `readFile` / `exists` / S3 delete-marker semantics. But tenants
 * whose data already accumulated tombstoned heads BEFORE the
 * filter shipped need an explicit reaper to clean up.
 *
 * Two strategies, operator's choice per call:
 *
 *  - `mode: "hardDelete"` (DEFAULT and recommended). For each `files`
 *    row whose `head_version_id` points at a `deleted=1`
 *    `file_versions` row: drop EVERY version (live + tombstone)
 *    through the canonical `dropVersionRows` (which fans out
 *    `deleteChunks` to ShardDOs for refcount decrement), then drop
 *    the `files` row. Mirrors the non-versioning unlink semantics.
 *    Loses version history — appropriate when the user explicitly
 *    unlinked.
 *
 *  - `mode: "walkBack"`. For each tombstoned-head, find the newest
 *    `deleted=0` predecessor and repoint `head_version_id` at it.
 *    If no live predecessor exists, fall back to `hardDelete`
 *    behaviour for that path. Use when the user reports "I never
 *    unlinked these — recover the last live version."
 *
 * `dryRun: true` (DEFAULT) reports counts and a small sample without
 * writing. Run twice in operator runbook: first dry-run to confirm
 * the diagnosis matches expectations, then apply.
 *
 * SAFETY:
 *  - One affected tenant per RPC invocation (called against that
 *    tenant's UserDO).
 *  - No retention-policy interaction: this operates outside
 *    `dropVersions` and doesn't change retention semantics.
 *  - Idempotent: re-running on a clean tenant returns
 *    `scanned: 0`. Re-running with the same `mode` after a partial
 *    apply is safe — already-reaped paths are no longer in the
 *    candidate set.
 */

import type { UserDOCore as UserDO } from "./user-do-core";
import type { VFSScope } from "../../../../shared/vfs-types";
import { dropVersionRows } from "./vfs-versions";

export interface ReapMode {
  mode: "hardDelete" | "walkBack";
  dryRun?: boolean;
  /** Soft cap on number of paths processed in a single call. Default: 1000. */
  limit?: number;
}

export interface ReapResult {
  scanned: number;
  hardDeleted: number;
  walkedBack: number;
  /** Up to 10 sample paths (user-visible reconstruction skipped to keep this lean). */
  samplePathIds: string[];
  dryRun: boolean;
}

/**
 * Scan + (optionally) repair tombstoned-head rows in the current
 * UserDO. Returns counts + a small sample.
 */
export async function reapTombstonedHeads(
  durableObject: UserDO,
  userId: string,
  scope: VFSScope,
  opts: ReapMode
): Promise<ReapResult> {
  const limit = clampLimit(opts.limit);
  const dryRun = opts.dryRun !== false; // safe-by-default: dryRun on unless explicitly false

  // Find every files row whose head_version_id points at a deleted=1 row.
  const candidates = durableObject.sql
    .exec(
      `SELECT f.file_id AS file_id, fv.version_id AS version_id
         FROM files f
         JOIN file_versions fv
           ON fv.path_id = f.file_id AND fv.version_id = f.head_version_id
        WHERE f.user_id = ?
          AND f.status = 'complete'
          AND fv.deleted = 1
        ORDER BY f.created_at ASC
        LIMIT ?`,
      userId,
      limit
    )
    .toArray() as { file_id: string; version_id: string }[];

  const samplePathIds = candidates.slice(0, 10).map((c) => c.file_id);

  if (dryRun) {
    return {
      scanned: candidates.length,
      hardDeleted: 0,
      walkedBack: 0,
      samplePathIds,
      dryRun: true,
    };
  }

  let hardDeleted = 0;
  let walkedBack = 0;

  for (const c of candidates) {
    if (opts.mode === "walkBack") {
      // Find newest non-tombstoned predecessor.
      const prior = durableObject.sql
        .exec(
          `SELECT version_id FROM file_versions
            WHERE path_id = ? AND deleted = 0
            ORDER BY mtime_ms DESC
            LIMIT 1`,
          c.file_id
        )
        .toArray()[0] as { version_id: string } | undefined;
      if (prior) {
        durableObject.sql.exec(
          `UPDATE files SET head_version_id = ?, updated_at = ?
            WHERE file_id = ? AND user_id = ?`,
          prior.version_id,
          Date.now(),
          c.file_id,
          userId
        );
        walkedBack++;
        continue;
      }
      // No live predecessor — fall through to hardDelete for this row.
    }

    // hardDelete branch (also fallback when walkBack finds nothing).
    const allVersions = (
      durableObject.sql
        .exec(
          "SELECT version_id FROM file_versions WHERE path_id = ?",
          c.file_id
        )
        .toArray() as { version_id: string }[]
    ).map((r) => r.version_id);
    if (allVersions.length > 0) {
      // dropVersionRows handles ShardDO refcount fanout + deletes
      // the files row when no versions remain (vfs-versions.ts:626-631).
      await dropVersionRows(durableObject, scope, userId, c.file_id, allVersions);
    } else {
      // No version rows at all — just drop the orphan files row.
      durableObject.sql.exec(
        "DELETE FROM files WHERE file_id = ? AND user_id = ?",
        c.file_id,
        userId
      );
    }
    hardDeleted++;
  }

  return {
    scanned: candidates.length,
    hardDeleted,
    walkedBack,
    samplePathIds,
    dryRun: false,
  };
}

function clampLimit(n: number | undefined): number {
  if (n === undefined) return 1000;
  if (!Number.isInteger(n) || n < 1) return 1000;
  if (n > 10_000) return 10_000;
  return n;
}
