import { applyMigrationOnce } from "../../../lib/migrations";

export function applyEncryptionColumns(sql: SqlStorage): void {
  // ── opt-in end-to-end encryption ───────────────────────────
  //
  // Two columns on `files` and two on `file_versions` carry the
  // per-file encryption mode + opaque keyId label. pre-encryption rows
  // get NULL by default — the SDK treats NULL as "plaintext" and
  // returns the bytes verbatim, preserving full backward compatibility.
  //
  // The server NEVER decrypts user data. These columns are pure
  // metadata used to (a) tell the SDK whether to attempt decryption
  // on read, and (b) reject mixed-mode writes within a path's
  // history with EBADF.
  //
  // No CHECK constraint — consistent with how `mode_yjs` was added
  // without one. The SDK validates the values it reads.
  //
  // No new wrangler migration tag — additive ALTERs are idempotent.
  applyMigrationOnce(sql, "files_add_encryption_mode", () =>
    sql.exec("ALTER TABLE files ADD COLUMN encryption_mode TEXT")
  );
  applyMigrationOnce(sql, "files_add_encryption_key_id", () =>
    sql.exec("ALTER TABLE files ADD COLUMN encryption_key_id TEXT")
  );
  applyMigrationOnce(sql, "file_versions_add_encryption_mode", () =>
    sql.exec(
      "ALTER TABLE file_versions ADD COLUMN encryption_mode TEXT"
    )
  );
  applyMigrationOnce(sql, "file_versions_add_encryption_key_id", () =>
    sql.exec(
      "ALTER TABLE file_versions ADD COLUMN encryption_key_id TEXT"
    )
  );
}
