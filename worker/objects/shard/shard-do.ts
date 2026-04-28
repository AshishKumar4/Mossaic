import { DurableObject } from "cloudflare:workers";
import type { Env } from "@shared/types";

export class ShardDO extends DurableObject<Env> {
  sql: SqlStorage;
  private initialized = false;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.sql = ctx.storage.sql;
  }

  private ensureInit(): void {
    if (this.initialized) return;
    this.initialized = true;

    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS chunks (
        hash          TEXT PRIMARY KEY,
        data          BLOB NOT NULL,
        size          INTEGER NOT NULL,
        ref_count     INTEGER NOT NULL DEFAULT 1,
        created_at    INTEGER NOT NULL
      )
    `);

    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS chunk_refs (
        chunk_hash    TEXT NOT NULL,
        file_id       TEXT NOT NULL,
        chunk_index   INTEGER NOT NULL,
        user_id       TEXT NOT NULL,
        PRIMARY KEY (chunk_hash, file_id, chunk_index)
      )
    `);

    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS shard_meta (
        key           TEXT PRIMARY KEY,
        value         INTEGER NOT NULL
      )
    `);

    // ── VFS GC bookkeeping (sdk-impl-plan §3.2, §8.3) ──────────────────────
    // deleted_at marks chunks pending hard-delete. Set when ref_count first
    // hits 0; the alarm sweeper (Phase 3) hard-deletes after a grace
    // period. NULL = live. Idempotent ALTER guarded by try/catch like
    // search-do.ts:59-68.
    try {
      this.sql.exec("ALTER TABLE chunks ADD COLUMN deleted_at INTEGER");
    } catch {
      // column already exists
    }
    this.sql.exec(`
      CREATE INDEX IF NOT EXISTS idx_chunks_deleted
        ON chunks(deleted_at)
        WHERE deleted_at IS NOT NULL
    `);
    this.sql.exec(`
      CREATE INDEX IF NOT EXISTS idx_chunk_refs_file
        ON chunk_refs(file_id)
    `);
  }

  async fetch(request: Request): Promise<Response> {
    this.ensureInit();

    const url = new URL(request.url);
    const path = url.pathname;

    try {
      // Write a chunk: PUT /chunk
      if (path === "/chunk" && request.method === "PUT") {
        const chunkHash = request.headers.get("X-Chunk-Hash") || "";
        const fileId = request.headers.get("X-File-Id") || "";
        const chunkIndex = parseInt(
          request.headers.get("X-Chunk-Index") || "0"
        );
        const userId = request.headers.get("X-User-Id") || "";

        const data = await request.arrayBuffer();
        const result = this.writeChunkInternal(
          chunkHash,
          new Uint8Array(data),
          fileId,
          chunkIndex,
          userId
        );
        return Response.json(result);
      }

      // Read a chunk: GET /chunk/:hash
      if (path.startsWith("/chunk/") && request.method === "GET") {
        const hash = path.split("/")[2];
        const rows = this.sql
          .exec("SELECT data, size FROM chunks WHERE hash = ?", hash)
          .toArray();

        if (rows.length === 0) {
          return new Response("Chunk not found", { status: 404 });
        }

        const chunk = rows[0] as { data: ArrayBuffer; size: number };

        // Stream the chunk data
        return new Response(chunk.data, {
          headers: {
            "Content-Type": "application/octet-stream",
            "Content-Length": chunk.size.toString(),
            "Cache-Control": "public, max-age=31536000, immutable",
            ETag: `"${hash}"`,
          },
        });
      }

      // Shard stats: GET /stats
      if (path === "/stats" && request.method === "GET") {
        const stats = this.getStats();
        return Response.json(stats);
      }

      // Delete file refs: DELETE /refs/:fileId
      // Legacy HTTP shape — kept for back-compat. The body now reflects
      // soft-mark semantics: bytes are not freed synchronously, they are
      // marked for the alarm sweeper. No current route actually invokes
      // this endpoint (verified Phase 1); the public DO RPC is preferred.
      if (path.startsWith("/refs/") && request.method === "DELETE") {
        const fileId = path.split("/")[2];
        const result = await this.removeFileRefs(fileId);
        return Response.json({
          freedBytes: 0,
          markedChunks: result.marked,
        });
      }

      return new Response("Not found", { status: 404 });
    } catch (err) {
      const message = err instanceof Error ? err.message : "Internal error";
      return Response.json({ error: message }, { status: 500 });
    }
  }

  // ── VFS write RPC surface (sdk-impl-plan §7) ───────────────────────────
  //
  // putChunk is the typed RPC counterpart to the legacy
  // PUT /chunk HTTP route. UserDO's vfsWriteFile path calls this
  // directly via the typed DO stub: no JSON marshalling, no headers,
  // typed return.
  //
  // Refcount semantics are identical to the HTTP route — they share
  // writeChunkInternal so the dedup-drift fix (Phase 1) applies to
  // both paths.
  async putChunk(
    chunkHash: string,
    data: Uint8Array,
    fileId: string,
    chunkIndex: number,
    userId: string
  ): Promise<{ status: "created" | "deduplicated"; bytesStored: number }> {
    this.ensureInit();
    return this.writeChunkInternal(
      chunkHash,
      data,
      fileId,
      chunkIndex,
      userId
    );
  }

  /**
   * Shared write path used by both the legacy HTTP PUT /chunk route and
   * the new putChunk RPC. Implements:
   *   - dedup: existing hash → INSERT OR IGNORE chunk_refs, conditional
   *     ref_count++ via SELECT changes() (Phase 1 fix), clear deleted_at
   *     on resurrection
   *   - cold path: INSERT INTO chunks + chunk_refs, update capacity
   *
   * Returns the same shape as the HTTP route's JSON body.
   */
  private writeChunkInternal(
    chunkHash: string,
    data: Uint8Array,
    fileId: string,
    chunkIndex: number,
    userId: string
  ): { status: "created" | "deduplicated"; bytesStored: number } {
    const existing = this.sql
      .exec("SELECT hash FROM chunks WHERE hash = ?", chunkHash)
      .toArray();

    if (existing.length > 0) {
      this.sql.exec(
        `INSERT OR IGNORE INTO chunk_refs (chunk_hash, file_id, chunk_index, user_id)
         VALUES (?, ?, ?, ?)`,
        chunkHash,
        fileId,
        chunkIndex,
        userId
      );
      const inserted =
        (
          this.sql.exec("SELECT changes() AS n").toArray()[0] as {
            n: number;
          }
        ).n > 0;
      if (inserted) {
        this.sql.exec(
          "UPDATE chunks SET ref_count = ref_count + 1 WHERE hash = ?",
          chunkHash
        );
      }
      // Resurrection: a previous unlink may have soft-marked this chunk;
      // a fresh write/dedup cancels the GC.
      this.sql.exec(
        "UPDATE chunks SET deleted_at = NULL WHERE hash = ? AND deleted_at IS NOT NULL",
        chunkHash
      );
      return { status: "deduplicated", bytesStored: 0 };
    }

    this.sql.exec(
      `INSERT INTO chunks (hash, data, size, ref_count, created_at)
       VALUES (?, ?, ?, 1, ?)`,
      chunkHash,
      data,
      data.byteLength,
      Date.now()
    );
    this.sql.exec(
      `INSERT INTO chunk_refs (chunk_hash, file_id, chunk_index, user_id)
       VALUES (?, ?, ?, ?)`,
      chunkHash,
      fileId,
      chunkIndex,
      userId
    );
    this.updateCapacity(data.byteLength);
    return { status: "created", bytesStored: data.byteLength };
  }

  // ── VFS GC RPC surface (sdk-impl-plan §8.2) ────────────────────────────
  //
  // Public DO RPC. Called from UserDO's vfsUnlink / vfsCommitWrite /
  // vfsRename overwrite path. Drops one (chunk_hash, file_id) ref per
  // chunk_refs row; decrements `ref_count` on chunks; soft-marks any
  // chunk that hits ref_count=0 by setting `deleted_at`. The alarm
  // sweeper (alarm() handler) hard-deletes after a 30s grace window,
  // re-checking ref_count to absorb resurrection races where a
  // concurrent dedup PUT re-references the chunk.
  //
  // Returns `{ marked }` — number of chunks newly marked deleted_at on
  // this call. Callers don't need this for correctness; it's
  // observability for tests + future quota reconciliation.
  async deleteChunks(fileId: string): Promise<{ marked: number }> {
    this.ensureInit();
    return this.removeFileRefs(fileId);
  }

  /**
   * Drop refs for a fileId. For each ref: delete the chunk_refs row,
   * decrement ref_count, soft-mark with `deleted_at` if it hit 0. The
   * alarm handler does the actual hard-delete after a grace period.
   *
   * Per-call observable side-effects:
   *   - chunk_refs rows for this fileId removed
   *   - chunks.ref_count decremented
   *   - chunks.deleted_at set on rows that hit 0
   *   - alarm scheduled if any chunk was marked
   */
  private async removeFileRefs(fileId: string): Promise<{ marked: number }> {
    let marked = 0;

    const refs = this.sql
      .exec("SELECT chunk_hash FROM chunk_refs WHERE file_id = ?", fileId)
      .toArray() as { chunk_hash: string }[];

    for (const { chunk_hash } of refs) {
      this.sql.exec(
        "DELETE FROM chunk_refs WHERE chunk_hash = ? AND file_id = ?",
        chunk_hash,
        fileId
      );

      this.sql.exec(
        "UPDATE chunks SET ref_count = MAX(0, ref_count - 1) WHERE hash = ?",
        chunk_hash
      );

      const r = this.sql
        .exec("SELECT ref_count FROM chunks WHERE hash = ?", chunk_hash)
        .toArray()[0] as { ref_count: number } | undefined;

      if (r && r.ref_count === 0) {
        // First-to-zero wins the soft-mark. The `deleted_at IS NULL`
        // guard means a chunk already marked (e.g. in a prior unlink
        // that didn't fully sweep yet) keeps its earlier timestamp,
        // which is fine — the grace window is measured from that.
        this.sql.exec(
          "UPDATE chunks SET deleted_at = ? WHERE hash = ? AND deleted_at IS NULL",
          Date.now(),
          chunk_hash
        );
        marked++;
      }
    }

    if (marked > 0) {
      await this.scheduleSweep();
    }
    return { marked };
  }

  /**
   * Ensure an alarm is scheduled for the next sweep. If one is already
   * set sooner than our target, leave it alone — single alarm per DO.
   */
  private async scheduleSweep(): Promise<void> {
    const cur = await this.ctx.storage.getAlarm();
    const next = Date.now() + 5 * 60 * 1000; // 5 min default cadence
    if (cur === null || cur > next) {
      await this.ctx.storage.setAlarm(next);
    }
  }

  /**
   * Alarm handler — hard-deletes chunks soft-marked for >= 30s and
   * still at ref_count=0. Resurrected chunks (ref_count went back up
   * via a concurrent dedup PUT, with the deleted_at clear handled in
   * the PUT path) need the un-mark belt-and-suspenders here too.
   *
   * Reschedules itself if more rows remain past this batch's LIMIT 500.
   * Cloudflare alarms have at-least-once semantics with exponential
   * backoff retry on throw, so this handler is idempotent — re-running
   * the same batch is a no-op (the rows are already deleted).
   */
  async alarm(): Promise<void> {
    this.ensureInit();
    const cutoff = Date.now() - 30_000; // 30s grace
    const rows = this.sql
      .exec(
        "SELECT hash, size FROM chunks WHERE deleted_at IS NOT NULL AND deleted_at < ? LIMIT 500",
        cutoff
      )
      .toArray() as { hash: string; size: number }[];

    let freed = 0;
    for (const { hash, size } of rows) {
      // Re-check ref_count under the same single-threaded fetch to
      // catch a resurrection-by-PUT that happened after the
      // chunk_refs DELETE but before the sweeper saw it.
      const live = this.sql
        .exec("SELECT ref_count FROM chunks WHERE hash = ?", hash)
        .toArray()[0] as { ref_count: number } | undefined;

      if (!live || live.ref_count > 0) {
        // Resurrected; un-mark and skip the delete.
        this.sql.exec(
          "UPDATE chunks SET deleted_at = NULL WHERE hash = ?",
          hash
        );
        continue;
      }

      this.sql.exec("DELETE FROM chunks WHERE hash = ?", hash);
      freed += size;
    }
    if (freed > 0) this.updateCapacity(-freed);

    // Reschedule if the sweep was capped at LIMIT 500.
    const more = this.sql
      .exec("SELECT 1 FROM chunks WHERE deleted_at IS NOT NULL LIMIT 1")
      .toArray();
    if (more.length > 0) {
      await this.ctx.storage.setAlarm(Date.now() + 60_000);
    }
  }

  private getStats(): {
    totalChunks: number;
    totalBytes: number;
    uniqueChunks: number;
    totalRefs: number;
    capacityUsed: number;
  } {
    // Total chunks (including dedup references counted by ref_count)
    const chunkAgg = this.sql
      .exec(
        "SELECT COUNT(*) as unique_chunks, COALESCE(SUM(size), 0) as total_bytes, COALESCE(SUM(ref_count), 0) as total_refs FROM chunks"
      )
      .toArray();

    const row = chunkAgg[0] as {
      unique_chunks: number;
      total_bytes: number;
      total_refs: number;
    };

    // Capacity from shard_meta
    const metaRows = this.sql
      .exec(
        "SELECT value FROM shard_meta WHERE key = 'capacity_used_bytes'"
      )
      .toArray();
    const capacityUsed =
      metaRows.length > 0 ? (metaRows[0] as { value: number }).value : 0;

    return {
      totalChunks: row.total_refs,
      totalBytes: row.total_bytes,
      uniqueChunks: row.unique_chunks,
      totalRefs: row.total_refs,
      capacityUsed,
    };
  }

  private updateCapacity(deltaBytes: number): void {
    const existing = this.sql
      .exec(
        "SELECT value FROM shard_meta WHERE key = 'capacity_used_bytes'"
      )
      .toArray();

    if (existing.length === 0) {
      this.sql.exec(
        "INSERT INTO shard_meta (key, value) VALUES ('capacity_used_bytes', ?)",
        Math.max(0, deltaBytes)
      );
    } else {
      this.sql.exec(
        "UPDATE shard_meta SET value = MAX(0, value + ?) WHERE key = 'capacity_used_bytes'",
        deltaBytes
      );
    }
  }
}
