import type { SchemaStep } from "../../../lib/migrations";
import { applyAuditLog } from "./audit";
import { applyAuthTable } from "./auth";
import { applyChunkCleanupOutbox } from "./cleanup";
import { applyEncryptionColumns } from "./encryption";
import {
  applyFileArchivedColumn,
  applyFilePosixColumns,
  applyFileSearchIndexColumns,
  applyFileTables,
} from "./files";
import {
  applyFolderModeColumn,
  applyFolderRevision,
  applyFoldersTable,
} from "./folders";
import {
  applyMetadataColumns,
  applyTagsAndListingIndexes,
} from "./metadata-tags";
import {
  applyUploadSessionIndexes,
  applyUploadSessionsTable,
} from "./multipart";
import {
  applyPathIndexes,
  verifyPathUniquenessIndexes,
} from "./path-indexes";
import { applyPreviewVariants } from "./preview";
import {
  applyQuotaInlineTierAccounting,
  applyQuotaRateLimit,
  applyQuotaTable,
  applyQuotaVersioningToggle,
} from "./quota";
import { applyShardStorageCache } from "./shard-capacity";
import { applyVersionSchema, applyVersionShardRefColumn } from "./versions";
import { applyVfsMetaTable } from "./vfs-meta";
import { applyWriteStreamSessions } from "./write-streams";
import { applyYjsCompactionCounter, applyYjsSchema } from "./yjs";

/**
 * UserDO schema, in the order it must be applied.
 *
 * Migrations are recorded by stable name in `meta_schema`.
 * `applyMigrationOnce` runs the body only the first time it sees
 * a name; on existing instances whose columns already exist (from
 * the prior idempotent-ALTER pattern) the helper catches the
 * SQLite "duplicate column name" error and records the name as
 * applied — so the bridge from try/catch-ALTER to registry is
 * safe without a backfill pass.
 *
 * Order is load-bearing and must not be reshuffled:
 *
 *   - `ALTER TABLE` steps require their table's `CREATE TABLE` step,
 *     and `CREATE INDEX` steps require their indexed columns.
 *   - `verifyPathUniquenessIndexes` reads `sqlite_master` for the
 *     unique indexes created by `applyPathIndexes` and records its
 *     degraded marker in `vfs_meta`, so it must run after both
 *     `applyPathIndexes` and `applyVfsMetaTable`.
 *   - The sequence is otherwise append-only history, which is why a
 *     single feature's steps appear at several points in this list
 *     (e.g. `files`, `quota`, `versions`, `yjs`, `multipart`).
 *     Re-grouping them would change the DDL order that already-
 *     deployed instances applied, and the `sqlite_master` column
 *     order that `ALTER TABLE ADD COLUMN` bakes into every table.
 */
export const USER_SCHEMA_STEPS: readonly SchemaStep[] = [
  applyAuthTable,
  applyFileTables,
  applyFoldersTable,
  applyQuotaTable,
  applyFilePosixColumns,
  applyFolderModeColumn,
  applyFolderRevision,
  applyPathIndexes,
  applyQuotaRateLimit,
  applyQuotaVersioningToggle,
  applyQuotaInlineTierAccounting,
  applyVersionSchema,
  applyYjsSchema,
  applyVfsMetaTable,
  applyChunkCleanupOutbox,
  applyShardStorageCache,
  applyMetadataColumns,
  applyEncryptionColumns,
  applyVersionShardRefColumn,
  applyYjsCompactionCounter,
  applyFileSearchIndexColumns,
  applyFileArchivedColumn,
  applyTagsAndListingIndexes,
  applyUploadSessionsTable,
  applyWriteStreamSessions,
  applyUploadSessionIndexes,
  applyPreviewVariants,
  applyAuditLog,
  verifyPathUniquenessIndexes,
];
