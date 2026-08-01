export function applyShardStorageCache(sql: SqlStorage): void {
  // Skip-full-shard placement cache.
  //
  // Persistent per-shard byte-count cache. Refreshed every
  // ~30 min by `monitorShardCapacity` from the alarm path.
  // `placeChunk` reads this table to construct a `fullShards`
  // skip-set: rendezvous winners that are at-or-over the soft
  // cap fall over to the next-best score so writes never land
  // on a near-capacity shard. Backward-compat: empty cache
  // (no rows) → `placeChunk` is byte-equivalent to the
  // pure-rendezvous deterministic top-1 winner; the test pool +
  // brand-new tenants exhibit identical placement until the
  // first capacity poll runs.
  //
  // Cold-cache scenarios (no entry for a particular shard)
  // are treated as "not full" — better to write to an
  // un-measured shard than to refuse the write under-spec'd.
  sql.exec(`
      CREATE TABLE IF NOT EXISTS shard_storage_cache (
        shard_index   INTEGER PRIMARY KEY,
        bytes_stored  INTEGER NOT NULL,
        refreshed_at  INTEGER NOT NULL
      )
    `);
}
