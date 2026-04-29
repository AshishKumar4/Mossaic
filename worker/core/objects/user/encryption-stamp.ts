/**
 * Phase 15: server-side encryption metadata helpers.
 *
 * The Mossaic server NEVER decrypts user data. These helpers manage the
 * `encryption_mode` + `encryption_key_id` columns on `files` and
 * `file_versions` — pure metadata used to:
 *
 *  1. Tell the SDK on read whether to attempt decryption (`stat.encryption`).
 *  2. Refuse mixed-mode writes within a path's history with EBADF.
 *  3. Refuse plaintext writes to encrypted paths with EBADF.
 *  4. Preserve the encryption metadata across `copyFile` (which is a
 *     refcount-only operation; bytes never move).
 *
 * The actual envelope bytes the server stores in chunks/inline_data are
 * opaque to the worker — they're the SDK's responsibility.
 *
 * Plan reference: `local/phase-15-plan.md` §5.
 */

import type { UserDOCore as UserDO } from "./user-do-core";
import { VFSError } from "@shared/vfs-types";
import {
  KEY_ID_MAX_BYTES,
  type EncryptionMode,
} from "@shared/encryption-types";

/**
 * Wire-level encryption opts as accepted by writeFile / copyFile / etc.
 * Mirrors the SDK's `WriteFileOpts.encrypted` field after normalization.
 */
export interface EncryptionStampOpts {
  mode: EncryptionMode;
  keyId?: string;
}

/**
 * Per-row encryption columns, as read from the `files` table.
 * NULL `mode` means plaintext (Pre-Phase-15 default).
 */
export interface FileEncryptionRow {
  encryption_mode: string | null;
  encryption_key_id: string | null;
}

/**
 * Validate inbound encryption opts. Throws `VFSError("EINVAL", ...)` on
 * mode/keyId shape violations. Called BEFORE any SQL touches the row.
 */
export function validateEncryptionOpts(
  opts: EncryptionStampOpts | undefined
): void {
  if (!opts) return;
  if (opts.mode !== "convergent" && opts.mode !== "random") {
    throw new VFSError(
      "EINVAL",
      `encryption.mode must be 'convergent' or 'random' (got '${opts.mode}')`
    );
  }
  if (opts.keyId !== undefined) {
    const utf8Bytes = new TextEncoder().encode(opts.keyId).byteLength;
    if (utf8Bytes > KEY_ID_MAX_BYTES) {
      throw new VFSError(
        "EINVAL",
        `encryption.keyId UTF-8 length ${utf8Bytes} exceeds ${KEY_ID_MAX_BYTES}`
      );
    }
  }
}

/**
 * Read existing encryption columns for a file row. Returns NULL columns
 * when the file pre-dates Phase 15 (plaintext).
 */
export function readEncryptionRow(
  durableObject: UserDO,
  fileId: string
): FileEncryptionRow | undefined {
  const row = durableObject.sql
    .exec(
      `SELECT encryption_mode, encryption_key_id
         FROM files WHERE file_id = ?`,
      fileId
    )
    .toArray()[0] as
    | { encryption_mode: string | null; encryption_key_id: string | null }
    | undefined;
  return row;
}

/**
 * Look up `(encryption_mode, encryption_key_id)` for a path's CURRENT
 * head row. Returns undefined if no such file exists yet.
 *
 * Used by the writeFile mode-mismatch pre-flight: if a file at this
 * (parentId, leaf) is already encrypted with mode X, a write with
 * mode Y must be rejected EBADF.
 */
export function findHeadEncryption(
  durableObject: UserDO,
  userId: string,
  parentId: string | null,
  leaf: string
): FileEncryptionRow | undefined {
  const row = durableObject.sql
    .exec(
      `SELECT encryption_mode, encryption_key_id
         FROM files
        WHERE user_id = ?
          AND IFNULL(parent_id, '') = IFNULL(?, '')
          AND file_name = ?
          AND status = 'complete'`,
      userId,
      parentId,
      leaf
    )
    .toArray()[0] as
    | { encryption_mode: string | null; encryption_key_id: string | null }
    | undefined;
  return row;
}

/**
 * Pre-flight check enforced at the top of `vfsWriteFile`.
 *
 * Decision matrix:
 * ```
 *   existing encryption_mode | incoming opts.encryption | result
 *   -------------------------|--------------------------|--------
 *   NULL (plaintext)         | undefined                | OK (plaintext write)
 *   NULL (plaintext)         | { mode: X }              | OK (first encrypted write)
 *   'convergent'             | undefined                | EBADF — encrypted path requires opts
 *   'convergent'             | { mode: 'convergent' }   | OK
 *   'convergent'             | { mode: 'random' }       | EBADF — cannot mix modes
 *   'random'                 | { mode: 'convergent' }   | EBADF — cannot mix modes
 * ```
 *
 * The mode-history-monotonic rule prevents history-confusion attacks
 * where an attacker who briefly compromises a tenant could re-write a
 * file under a different mode and obscure the audit trail. By making
 * the mode immutable per path, we force overt unlink+create to change.
 */
export function enforceModeMonotonic(
  durableObject: UserDO,
  userId: string,
  parentId: string | null,
  leaf: string,
  incomingEncryption: EncryptionStampOpts | undefined
): void {
  const existing = findHeadEncryption(durableObject, userId, parentId, leaf);
  if (!existing) return; // no file yet → any mode acceptable
  const existingMode = existing.encryption_mode;
  if (existingMode === null) {
    // Plaintext file. Either incoming is plaintext (OK) or incoming
    // is encrypted (OK — first encrypted write supersedes plaintext).
    return;
  }
  // Existing is encrypted.
  if (!incomingEncryption) {
    throw new VFSError(
      "EBADF",
      `writeFile: path is encrypted (mode='${existingMode}'); writeFile requires encryption opts`
    );
  }
  if (incomingEncryption.mode !== existingMode) {
    throw new VFSError(
      "EBADF",
      `writeFile: cannot mix encryption modes within a path's history (existing='${existingMode}', incoming='${incomingEncryption.mode}')`
    );
  }
}

/**
 * Stamp `encryption_mode` + `encryption_key_id` onto an existing
 * `files` row. Idempotent under repeated identical calls.
 *
 * - opts === undefined → write NULL columns (clears any prior stamp;
 *   used by plaintext writes that supersede a pre-existing plaintext).
 * - opts !== undefined → write the values.
 */
export function stampFileEncryption(
  durableObject: UserDO,
  fileId: string,
  opts: EncryptionStampOpts | undefined
): void {
  const mode = opts?.mode ?? null;
  const keyId = opts?.keyId ?? null;
  durableObject.sql.exec(
    `UPDATE files SET encryption_mode = ?, encryption_key_id = ?
       WHERE file_id = ?`,
    mode,
    keyId,
    fileId
  );
}

/**
 * Stamp encryption columns onto a `file_versions` row. Mirrors
 * {@link stampFileEncryption} for the versioning code path. Called
 * after `INSERT INTO file_versions` from the versioned-write path.
 */
export function stampVersionEncryption(
  durableObject: UserDO,
  versionId: string,
  opts: EncryptionStampOpts | undefined
): void {
  const mode = opts?.mode ?? null;
  const keyId = opts?.keyId ?? null;
  durableObject.sql.exec(
    `UPDATE file_versions SET encryption_mode = ?, encryption_key_id = ?
       WHERE version_id = ?`,
    mode,
    keyId,
    versionId
  );
}

/**
 * Copy encryption columns from one `files` row to another. Used by
 * `copyFile`: when /a/ is encrypted with mode X, /b/ = copy(/a/) must
 * also surface as encrypted with mode X (the bytes are envelopes; the
 * dest must report the same mode so SDK readFile knows to decrypt).
 *
 * Atomic single SQL UPDATE. No-op if source has no encryption stamp.
 */
export function copyEncryptionStamp(
  durableObject: UserDO,
  srcFileId: string,
  destFileId: string
): void {
  durableObject.sql.exec(
    `UPDATE files
        SET encryption_mode = (SELECT encryption_mode FROM files WHERE file_id = ?),
            encryption_key_id = (SELECT encryption_key_id FROM files WHERE file_id = ?)
      WHERE file_id = ?`,
    srcFileId,
    srcFileId,
    destFileId
  );
}

/**
 * Convert a `FileEncryptionRow` into the SDK-facing object, or undefined
 * if the row is plaintext. Used by `vfsStat`, `vfsListFiles`, and the
 * `VersionRow` projection.
 */
export function projectEncryption(
  row: FileEncryptionRow | undefined | null
): EncryptionStampOpts | undefined {
  if (!row || row.encryption_mode === null) return undefined;
  if (row.encryption_mode !== "convergent" && row.encryption_mode !== "random") {
    // Defensive: reject any unexpected values rather than surfacing
    // an unknown mode. Should be impossible because writes go through
    // validateEncryptionOpts.
    return undefined;
  }
  const mode: EncryptionMode = row.encryption_mode;
  const out: EncryptionStampOpts = { mode };
  if (row.encryption_key_id !== null) {
    out.keyId = row.encryption_key_id;
  }
  return out;
}
