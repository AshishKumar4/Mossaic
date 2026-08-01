import { applyMigrationOnce } from "../../../lib/migrations";

export function applyPreviewVariants(sql: SqlStorage): void {
  // ── Universal preview pipeline ───────────────────────────────────────
  //
  // `file_variants` records pre-generated and on-demand-cached preview
  // bytes (thumb / medium / lightbox + custom dimensions). Variant
  // bytes live on a ShardDO under the same `chunks` / `chunk_refs`
  // refcount machinery as primary file chunks; this row maps
  // (file_id, variant_kind, renderer_kind) → (chunk_hash, shard_index).
  //
  // - Composite PK lets the same file carry multiple renderer
  //   strategies (e.g. a video could have both a "video-poster" thumb
  //   AND a "waveform" medium).
  // - `chunk_hash` is content-addressed (SHA-256 of variant bytes).
  //   Per-shard dedup fires inside `writeChunkInternal`
  //   (`shard-do.ts:457`) when two writes land on the same shard for
  //   the same hash. Cross-user/cross-file dedup is NOT achieved by
  //   construction: `placeChunk` keys on `(userId, fileId,
  //   chunkIndex)` not on hash, so identical bytes from different
  //   files generally route to different shards. Same-file replay
  //   (idempotent retry) is the realistic dedup path.
  // - `ON DELETE CASCADE` removes variant rows when the parent
  //   `files` row is hard-deleted; chunk_refs cleanup is dispatched
  //   by `vfsUnlink` (see worker/core/objects/user/vfs/write-commit.ts).
  //
  // Idempotent CREATE TABLE; no migration tag.
  //
  // @lean-invariant Mossaic.Vfs.Preview.stepVariant_preserves_validState
  //   All transitions in the abstract list model preserve modeled key
  //   uniqueness; the SQL schema is not refined by this theorem.
  // @lean-invariant Mossaic.Vfs.Preview.cascade_delete_drops_all
  //   The abstract cascade transition removes all modeled rows for a
  //   file id; SQLite cascade semantics are outside the proof.
  sql.exec(`
      CREATE TABLE IF NOT EXISTS file_variants (
        file_id        TEXT NOT NULL,
        variant_kind   TEXT NOT NULL,
        renderer_kind  TEXT NOT NULL,
        chunk_hash     TEXT NOT NULL,
        shard_index    INTEGER NOT NULL,
        mime_type      TEXT NOT NULL,
        width          INTEGER NOT NULL,
        height         INTEGER NOT NULL,
        byte_size      INTEGER NOT NULL,
        created_at     INTEGER NOT NULL,
        PRIMARY KEY (file_id, variant_kind, renderer_kind),
        FOREIGN KEY (file_id) REFERENCES files(file_id) ON DELETE CASCADE
      )
    `);
  sql.exec(`
      CREATE INDEX IF NOT EXISTS idx_file_variants_hash
        ON file_variants(chunk_hash)
    `);
  sql.exec(`
      CREATE INDEX IF NOT EXISTS idx_file_variants_file
        ON file_variants(file_id)
    `);

  // Version-aware variant cache.
  //
  // The cache key would otherwise be
  // `(file_id, variant_kind, renderer_kind)` — but after v2
  // supersedes v1 on a versioning-on tenant, the cache row for
  // v1 still matches a thumbnail lookup and the gallery would
  // serve STALE bytes for the file's HEAD until the row is
  // manually invalidated. The bug surfaces as "I edited the
  // photo but the thumbnail shows the old image."
  //
  // Fix: store the head_version_id at render time on the variant
  // row. The reader (`findVariantRow` in
  // `worker/core/objects/user/preview-variants.ts`) gates on a
  // match with the file's CURRENT head_version_id; mismatch →
  // cache miss → re-render against the new head. Existing
  // (legacy) rows have version_id IS NULL — they remain valid
  // for the versioning-OFF / no-head-version case (where
  // head_version_id is NULL on `files` too). The legacy
  // passthrough is a load-bearing equivalence: NULL == NULL is
  // the SQL convention we adopt explicitly (`IS NULL` predicate,
  // not `=`).
  applyMigrationOnce(sql, "file_variants_add_version_id", () =>
    sql.exec(
      "ALTER TABLE file_variants ADD COLUMN version_id TEXT"
    )
  );
}
