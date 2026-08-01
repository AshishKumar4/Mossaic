export function applyVfsMetaTable(sql: SqlStorage): void {
  // ── Audit H1: stale-upload sweeper bookkeeping ───────────────────────
  //
  // The UserDO alarm hard-deletes abandoned `_vfs_tmp_*` rows and replays
  // durable chunk-cleanup intents. It runs without an HTTP/RPC scope, so it
  // cannot synthesize (ns, tenant, sub) from a request. We persist the DO's
  // scope on every gated VFS call so the alarm can reconstruct routing.
  //
  // `vfs_meta` is a tiny key/value table that survives DO
  // hibernation. We store one row keyed `scope` whose value is a
  // JSON-encoded `{ ns, tenant, sub? }`. Writes are idempotent
  // (INSERT OR REPLACE) and hot-path-cheap (a single SQL UPSERT
  // bounded to one row). Pre-existing tenants without this row
  // keep working — the alarm becomes a no-op for them until the
  // first gated call records their scope.
  //
  // The same table also carries the H6 migration_state markers
  // ('files_unique_index', 'folders_unique_index') to surface a
  // failed CREATE UNIQUE INDEX rather than silently swallow it.
  sql.exec(`
      CREATE TABLE IF NOT EXISTS vfs_meta (
        key   TEXT PRIMARY KEY,
        value TEXT NOT NULL
      )
    `);
}
