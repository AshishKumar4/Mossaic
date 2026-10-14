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
 * Retention: the insert that takes the log past its cap trims
 * the oldest rows in the same call. A tenant's log grows only
 * when it writes, so no alarm is needed to bound it, and a
 * write that settles the alarm cannot strand it over the cap.
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
 * needs a longer trail; every insert reads this value.
 */
export const AUDIT_LOG_MAX_ROWS = 10_000;

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
 * Best-effort: a SQL failure during insert or trim is
 * swallowed and logged \u2014 audit-log infrastructure must not
 * block the destructive op it's recording.
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
    reapAuditLog(durableObject, loadAuditLogMaxRows(durableObject));
  } catch {
    // best-effort \u2014 audit log failure must not block the op
    // it's recording. The next ensureInit() recreates the
    // table if it was dropped (which would require operator
    // intervention anyway).
  }
}

/**
 * Trim the oldest rows once the log holds more than `maxRows`,
 * down to a floor 2% under it (9,800 of the default 10,000), so
 * one DELETE serves the next 2% of inserts. Rows are only
 * appended, so rowid order is age order.
 *
 * The probe reads the rowid span, two rows, where COUNT(*) reads
 * every row; the span bounds the count from above and equals it
 * while this trim is the only delete.
 */
function reapAuditLog(durableObject: UserDO, maxRows: number): void {
  const span = durableObject.sql
    .exec(
      `SELECT (SELECT min(rowid) FROM audit_log) AS lo,
              (SELECT max(rowid) FROM audit_log) AS hi`
    )
    .toArray()[0] as { lo: number | null; hi: number | null };
  if (span.lo === null || span.hi === null) return;
  if (span.hi - span.lo + 1 <= maxRows) return;
  const floor = maxRows - Math.ceil(maxRows / 50);
  durableObject.sql.exec(
    "DELETE FROM audit_log WHERE rowid <= ?",
    span.hi - floor
  );
}

/**
 * Optional override read for `audit_log_max_rows` from
 * `vfs_meta`. Returns the configured cap or the default.
 * Operators set this via direct SQL (admin RPC could expose
 * later if there's demand).
 */
function loadAuditLogMaxRows(durableObject: UserDO): number {
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
