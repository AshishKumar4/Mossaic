export function applyAuditLog(sql: SqlStorage): void {
  // ── audit_log table ──────────────────────────────────────────
  //
  // Per-tenant append-only audit trail of every destructive
  // operation. See `worker/core/objects/user/vfs/audit-log.ts`
  // for the helper API + retention policy. Idempotent CREATE —
  // existing tenants pick up the table on next ensureInit.
  //
  // Index on (op, ts DESC) supports the "last N entries of op X"
  // query without a full scan; primary key (id) supports point
  // lookups + retention sweeps that DELETE the oldest rows.
  sql.exec(`
      CREATE TABLE IF NOT EXISTS audit_log (
        id          TEXT PRIMARY KEY,
        ts          INTEGER NOT NULL,
        op          TEXT NOT NULL,
        actor       TEXT NOT NULL,
        target      TEXT NOT NULL,
        payload     TEXT,
        request_id  TEXT
      )
    `);
  sql.exec(`
      CREATE INDEX IF NOT EXISTS idx_audit_log_op_ts
        ON audit_log(op, ts DESC)
    `);
  sql.exec(`
      CREATE INDEX IF NOT EXISTS idx_audit_log_ts
        ON audit_log(ts DESC)
    `);
}
