import type { SchemaStep } from "../../../lib/migrations";
import { applyChunkGcIndexes, applyChunkTables } from "./chunks";
import { applyMultipartStaging } from "./multipart";

/**
 * ShardDO schema, in the order it must be applied. `applyChunkGcIndexes`
 * depends on the tables created by `applyChunkTables`.
 */
export const SHARD_SCHEMA_STEPS: readonly SchemaStep[] = [
  applyChunkTables,
  applyChunkGcIndexes,
  applyMultipartStaging,
];
