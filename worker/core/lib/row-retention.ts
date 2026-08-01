/**
 * Bounded retention prune for append-only and terminal rows.
 *
 * Durable Object storage is per-tenant and finite, so every table that
 * accumulates rows nobody will read again needs the same sweep: take the
 * oldest rows by some timestamp, cap how many one invocation touches, and
 * leave the rest for the next alarm. The retention *reason* differs — a
 * completed operation is kept only long enough for a late replay to observe
 * its result, an audit trail is kept until it exceeds its row cap — but the
 * SQL does not.
 *
 * Callers own the terminal predicate, because "finished" is domain knowledge:
 * an aborted or poisoned upload session, a retention operation whose status is
 * `done`, a cleanup page journal past its TTL.
 */

import type { DurableSqlStore } from "./paged-operation";
import { sqlRowsChanged } from "./paged-operation";

export interface RowRetentionSpec {
  readonly table: string;
  /** Identity column addressing the prune batch. */
  readonly keyColumn: string;
  /**
   * Column or expression the retention order is taken from — for example
   * `COALESCE(terminal_at, created_at)` when a row's terminal stamp is only
   * present on rows written after the column landed.
   */
  readonly timestamp: string;
  /** SQL predicate selecting rows that are finished. Everything else is kept. */
  readonly terminal?: string;
  /** Keep rows whose timestamp is newer than this. */
  readonly olderThan?: number;
  /** Rows one invocation may prune. */
  readonly limit: number;
}

/**
 * Prune the oldest finished rows, returning how many went away. Ties on the
 * timestamp break on the key so the batch is deterministic across replays.
 *
 * `cascade` runs for each pruned key in the same transaction as the delete,
 * for tables whose children are keyed by the same identity — a session's
 * staged chunk manifests, a journal entry's recorded pages.
 */
export function pruneOldestRows(
  store: DurableSqlStore,
  spec: RowRetentionSpec,
  cascade?: (key: SqlStorageValue) => void
): number {
  const predicates: string[] = [];
  const bindings: SqlStorageValue[] = [];
  if (spec.terminal !== undefined) predicates.push(`(${spec.terminal})`);
  if (spec.olderThan !== undefined) {
    predicates.push(`${spec.timestamp} <= ?`);
    bindings.push(spec.olderThan);
  }
  const where =
    predicates.length === 0 ? "" : `WHERE ${predicates.join(" AND ")}`;
  const oldestKeys = `SELECT ${spec.keyColumn} FROM ${spec.table}
       ${where}
       ORDER BY ${spec.timestamp}, ${spec.keyColumn}
       LIMIT ?`;

  return store.storage.transactionSync(() => {
    if (cascade !== undefined) {
      const keys = store.sql
        .exec<{ prune_key: SqlStorageValue }>(
          `SELECT ${spec.keyColumn} AS prune_key FROM ${spec.table}
             ${where}
             ORDER BY ${spec.timestamp}, ${spec.keyColumn}
             LIMIT ?`,
          ...bindings,
          spec.limit
        )
        .toArray();
      for (const key of keys) cascade(key.prune_key);
    }
    store.sql.exec(
      `DELETE FROM ${spec.table}
        WHERE ${spec.keyColumn} IN (${oldestKeys})`,
      ...bindings,
      spec.limit
    );
    return sqlRowsChanged(store.sql);
  });
}
