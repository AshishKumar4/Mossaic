/**
 * Schema-version registry for SQLite-backed Durable Objects.
 *
 * Replaces the `try { sql.exec("ALTER TABLE ...") } catch {}` pattern
 * (idempotent-on-failure idiom) with a tracked, named migration model:
 *
 *   - A `meta_schema(name TEXT PRIMARY KEY, applied_at INTEGER NOT NULL)`
 *     table records every applied migration by stable name.
 *   - `applyMigrationOnce(sql, name, fn)` runs `fn` only if the name
 *     is not yet recorded; success OR a "duplicate column" SQLite
 *     error both insert the name and return. Any other thrown error
 *     propagates — failed migrations become VISIBLE rather than
 *     silently swallowed.
 *
 * Bridge semantics for existing instances (DOs that already ran the
 * old idempotent-ALTER pattern): the columns exist; the first
 * post-upgrade boot finds an empty `meta_schema`; each migration
 * fn's ALTER throws SQLITE "duplicate column name"; we treat that
 * as "already applied" and record the name. Steady state after
 * the first boot is the same as a freshly-created instance.
 *
 * Steady-state cost for an N-migration registry on a re-init: one
 * SELECT per migration to check `meta_schema`. Cheap. The previous
 * pattern paid one SQLite ALTER attempt + one parse-error throw per
 * migration on every boot — strictly more expensive.
 */

import type { SqlStorage } from "@cloudflare/workers-types";

/**
 * Initialise the `meta_schema` registry table. Idempotent;
 * `CREATE TABLE IF NOT EXISTS` is a no-op when the table already
 * exists. Call once per DO `ensureInit()`, before any
 * `applyMigrationOnce` call.
 */
export function ensureMigrationsTable(sql: SqlStorage): void {
  sql.exec(
    `CREATE TABLE IF NOT EXISTS meta_schema (
       name TEXT PRIMARY KEY,
       applied_at INTEGER NOT NULL
     )`
  );
}

/**
 * Apply a named migration once.
 *
 * `name` is the stable identifier recorded in `meta_schema` —
 * choose `<table>_<column-or-purpose>` (e.g. `files_add_mode`,
 * `quota_versioning_enabled`, `chunks_deleted_at_idx`).
 *
 * `fn` is the migration body — typically a single `sql.exec("ALTER
 * TABLE ...")` call, but free-form SQL (CREATE INDEX, etc.) is also
 * supported.
 *
 * Failure semantics:
 *  - SQLITE "duplicate column name" → record as applied (existing
 *    DO bridge). The pre-registry ALTER+catch pattern relied on
 *    this exact SQLite error string; matching it here preserves
 *    safe upgrades on instances created before this registry
 *    landed.
 *  - Any other error → propagate to the caller. Migrations that
 *    fail for novel reasons (typos, missing parent table, etc.)
 *    must be visible.
 */
export function applyMigrationOnce(
  sql: SqlStorage,
  name: string,
  fn: () => void
): void {
  const existing = sql
    .exec(
      "SELECT 1 AS one FROM meta_schema WHERE name = ?",
      name
    )
    .toArray()[0] as { one: number } | undefined;
  if (existing !== undefined) return;

  try {
    fn();
  } catch (err) {
    // SQLite raises "duplicate column name: <name>" on a re-applied
    // ADD COLUMN; the rest of the migration text is unchanged. Any
    // other SQLite error (e.g. "no such table", "syntax error",
    // "UNIQUE constraint failed") indicates a genuinely failed
    // migration and must surface to the caller.
    const msg = err instanceof Error ? err.message : String(err);
    if (!/duplicate column name/i.test(msg)) {
      throw err;
    }
  }
  sql.exec(
    "INSERT INTO meta_schema (name, applied_at) VALUES (?, ?)",
    name,
    Date.now()
  );
}
