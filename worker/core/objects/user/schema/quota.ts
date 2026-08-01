import { applyMigrationOnce } from "../../../lib/migrations";

export function applyQuotaTable(sql: SqlStorage): void {
  sql.exec(`
      CREATE TABLE IF NOT EXISTS quota (
        user_id       TEXT PRIMARY KEY,
        storage_used  INTEGER NOT NULL DEFAULT 0,
        storage_limit INTEGER NOT NULL DEFAULT 107374182400,
        file_count    INTEGER NOT NULL DEFAULT 0,
        pool_size     INTEGER NOT NULL DEFAULT 32
      )
    `);
}

export function applyQuotaRateLimit(sql: SqlStorage): void {
  // ── per-tenant rate-limit state (token bucket) ──────────────
  //
  // Token-bucket limiter applied to VFS RPC methods (not the legacy
  // fetch handler). State persists across DO hibernation so the
  // bucket survives cold starts. Defaults: 100 ops/sec refill, 200
  // burst capacity. Operators can override per-tenant via direct
  // SQL or admin tooling. NULL columns inherit defaults at runtime.
  applyMigrationOnce(sql, "quota_add_rate_limit_per_sec", () =>
    sql.exec("ALTER TABLE quota ADD COLUMN rate_limit_per_sec INTEGER")
  );
  applyMigrationOnce(sql, "quota_add_rate_limit_burst", () =>
    sql.exec("ALTER TABLE quota ADD COLUMN rate_limit_burst INTEGER")
  );
  applyMigrationOnce(sql, "quota_add_rl_tokens", () =>
    sql.exec("ALTER TABLE quota ADD COLUMN rl_tokens REAL")
  );
  applyMigrationOnce(sql, "quota_add_rl_updated_at", () =>
    sql.exec("ALTER TABLE quota ADD COLUMN rl_updated_at INTEGER")
  );
}

export function applyQuotaVersioningToggle(sql: SqlStorage): void {
  // ── per-tenant versioning toggle (S3-style, opt-in) ─────────
  // versioning_enabled: NULL/0 = disabled (byte-equivalent
  // behavior); 1 = every writeFile/unlink creates a `file_versions`
  // row, readFile resolves the head version, and historical
  // readFile(path, {version: id}) becomes available. The default is
  // off; tenants opt-in via setTenantVersioning().
  applyMigrationOnce(sql, "quota_add_versioning_enabled", () =>
    sql.exec(
      "ALTER TABLE quota ADD COLUMN versioning_enabled INTEGER NOT NULL DEFAULT 0"
    )
  );
}

export function applyQuotaInlineTierAccounting(sql: SqlStorage): void {
  // Inline-tier graceful migration.
  //
  // Tracks per-tenant cumulative bytes stored in the inline tier
  // (`files.inline_data` BLOBs). `vfsWriteFile` consults this on
  // every write ≤ INLINE_LIMIT and falls through to the chunked
  // tier once `inline_bytes_used >= INLINE_TIER_CAP` (1 GiB) — the
  // soft ceiling prevents the inline tier from monopolizing the
  // UserDO's ~10 GiB SQLite quota.
  //
  // Maintained by `recordWriteUsage`'s `deltaInlineBytes`
  // parameter, called from `commitInlineTier` (positive delta)
  // and `hardDeleteFileRow` (negative delta when the deleted
  // row had `inline_data IS NOT NULL`).
  //
  // Defaults to 0; legacy rows behave as if no inline bytes are
  // accounted, so the cap is effectively only enforced for
  // forward-going writes. That is correct behaviour: a tenant
  // already over the cap on legacy data continues to use inline
  // for the rows that already exist; new writes spill to chunked.
  applyMigrationOnce(sql, "quota_add_inline_bytes_used", () =>
    sql.exec(
      "ALTER TABLE quota ADD COLUMN inline_bytes_used INTEGER NOT NULL DEFAULT 0"
    )
  );
}
