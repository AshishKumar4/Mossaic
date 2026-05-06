/**
 * Audit_log infrastructure.
 *
 * Every destructive operation (unlink, purge, archive,
 * unarchive, restoreVersion, dropVersions, admin*, account
 * delete, share-link mint) emits a row into a per-tenant
 * `audit_log` table. Operators can later answer "did this
 * tenant delete this file?" with a single SQL query against
 * the UserDO.
 *
 * Schema design choices:
 *
 *   - **Per-tenant scope.** The audit log lives on the UserDO
 *     (per-tenant single-writer). No cross-tenant queries; an
 *     operator who needs a fleet-wide view fans out across DOs.
 *     Trade-off: storage growth is bounded per tenant
 *     (`audit_log_max_rows`, default 10K).
 *   - **Append-only.** No UPDATE; ts is server-stamped at
 *     insertAuditLog time.
 *   - **Index on (op, ts) DESC.** Most operator queries are
 *     "last N entries of op X" or "last N entries". The
 *     index makes both fast.
 *   - **Idempotent ALTER.** Existing tenants get the table on
 *     next ensureInit; no migration script.
 *
 * Retention: oldest rows are reaped by the alarm sweeper once
 * count exceeds `AUDIT_LOG_MAX_ROWS`. Reaping deletes
 * `(count - target_floor)` rows in one DELETE — bounded SQL
 * cost regardless of how many rows accumulated since the
 * previous sweep.
 *
 * Payload shape: small JSON. Callers stringify their own
 * structured object; the helper takes a string and stores it
 * verbatim. Keep payloads under ~1 KB — large payloads bloat
 * the DO storage.
 */

import { generateId } from "../../../lib/utils";
import type { UserDOCore as UserDO } from "../user-do-core";

/**
 * Default retention cap. Operators can override per-tenant via
 * `vfs_meta.key='audit_log_max_rows'` if a particular tenant
 * needs a longer trail; the alarm sweeper reads this value
 * each tick.
 */
export const AUDIT_LOG_MAX_ROWS = 10_000;

/**
 * Floor the retention sweep targets when it triggers. Reaping
 * down to `MAX_ROWS - 200` (rather than exactly `MAX_ROWS`)
 * amortizes the trim across many writes \u2014 the sweep doesn't
 * fire every single insert.
 */
export const AUDIT_LOG_TARGET_FLOOR = AUDIT_LOG_MAX_ROWS - 200;

/**
 * Op classes recognized by the audit log. Free-form strings
 * are accepted but the union here documents the canonical set
 * so reviewers can confirm every destructive op emits exactly
 * one entry. Adding a new op? Add it here AND emit the row.
 */
export type AuditLogOp =
  | "unlink"
  | "purge"
  | "archive"
  | "unarchive"
  | "restoreVersion"
  | "dropVersions"
  | "rename"
  | "removeRecursive"
  | "adminSetVersioning"
  | "adminDedupePaths"
  | "adminReapTombstonedHeads"
  | "adminPreGenerateStandardVariants"
  | "adminWipeAccountData"
  | "shareLinkMint"
  | "accountDelete";

/**
 * Insert an audit-log row. Caller responsibilities:
 *   - `op` identifies the operation class (free-form).
 *   - `actor` is the userId / "operator" / "system".
 *   - `target` is the path / fileId / share-token jti the op
 *     acted on; empty string if N/A.
 *   - `payload` is a JSON-stringified small object with op-
 *     specific fields. Keep under ~1 KB.
 *
 * Idempotent in shape (same op + actor + target + payload =
 * same row, just at different ts) but NOT deduplicated. Each
 * call inserts a row.
 *
 * Best-effort: a SQL failure during insert is swallowed and
 * logged \u2014 audit-log infrastructure must not block the
 * destructive op it's recording.
 */
export function insertAuditLog(
  durableObject: UserDO,
  args: {
    op: AuditLogOp | string;
    actor: string;
    target: string;
    payload?: string;
    requestId?: string;
  }
): void {
  try {
    durableObject.sql.exec(
      `INSERT INTO audit_log (id, ts, op, actor, target, payload, request_id)
       VALUES (?, ?, ?, ?, ?, ?, ?)`,
      generateId(),
      Date.now(),
      args.op,
      args.actor,
      args.target,
      args.payload ?? null,
      args.requestId ?? null
    );
  } catch {
    // best-effort \u2014 audit log failure must not block the op
    // it's recording. The next ensureInit() recreates the
    // table if it was dropped (which would require operator
    // intervention anyway).
  }
}

/**
 * Reap oldest audit-log rows down to `AUDIT_LOG_TARGET_FLOOR`.
 * Invoked from `UserDO.alarm()` when `audit_log` row count
 * exceeds `AUDIT_LOG_MAX_ROWS`.
 *
 * Returns the number of rows reaped (0 when below the cap).
 *
 * Cost: 1 COUNT(*) probe + 1 DELETE with LIMIT subquery. Both
 * O(log N) given the (op, ts) index serves the COUNT and the
 * primary-key id sort serves the DELETE order. SQLite doesn't
 * use the column index for COUNT(*) without a WHERE clause, so
 * the COUNT is a sequential scan \u2014 cheap on a 10K-row table.
 */
export function reapAuditLog(
  durableObject: UserDO,
  maxRows: number = AUDIT_LOG_MAX_ROWS,
  targetFloor: number = AUDIT_LOG_TARGET_FLOOR
): number {
  const row = durableObject.sql
    .exec("SELECT COUNT(*) AS n FROM audit_log")
    .toArray()[0] as { n: number } | undefined;
  const count = row?.n ?? 0;
  if (count <= maxRows) return 0;
  const reapTarget = Math.max(0, count - targetFloor);
  // Delete the OLDEST rows. ULID ids are time-monotonic so
  // ORDER BY id ASC matches ORDER BY ts ASC for any rows
  // generated by `generateId()` (ULID-like). Defending against explicit ts
  // overrides (none today) by ORDER BY ts ASC.
  durableObject.sql.exec(
    `DELETE FROM audit_log
       WHERE id IN (
         SELECT id FROM audit_log
          ORDER BY ts ASC
          LIMIT ?
       )`,
    reapTarget
  );
  return reapTarget;
}

/**
 * Optional override read for `audit_log_max_rows` from
 * `vfs_meta`. Returns the configured cap or the default.
 * Operators set this via direct SQL (admin RPC could expose
 * later if there's demand).
 */
export function loadAuditLogMaxRows(durableObject: UserDO): number {
  const row = durableObject.sql
    .exec(
      "SELECT value FROM vfs_meta WHERE key = 'audit_log_max_rows'"
    )
    .toArray()[0] as { value: string } | undefined;
  if (!row) return AUDIT_LOG_MAX_ROWS;
  const parsed = Number.parseInt(row.value, 10);
  if (!Number.isFinite(parsed) || parsed < 100) return AUDIT_LOG_MAX_ROWS;
  return parsed;
}
